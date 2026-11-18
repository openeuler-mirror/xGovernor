use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use orchestration_contract::event::{EventPayload, EventSource, RunEvent};
use orchestration_contract::execution::ForkRequest;
use orchestration_contract::rpc::HostForkParams;
use orchestration_contract::run::*;
use rusqlite::{params, Connection, OpenFlags, Transaction};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use xgovernor_mcp::RunClient;

use crate::config::{select_backend, BackendRegistration};
use crate::Result;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub tokens: u64,
    pub cost_microunits: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ProcessIdentity {
    pid: u32,
    boot_id: String,
    start_ticks: String,
}

impl ProcessIdentity {
    fn read(pid: u32) -> Result<Self> {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
        // The command name may contain spaces and parentheses.
        let fields = stat.rsplit_once(')').ok_or("invalid process identity")?.1;
        let fields: Vec<_> = fields.split_whitespace().collect();
        if matches!(fields.first(), Some(&"Z") | Some(&"X")) {
            return Err("process has exited".into());
        }
        Ok(Self {
            pid,
            boot_id: fs::read_to_string("/proc/sys/kernel/random/boot_id")?,
            start_ticks: fields
                .get(19)
                .ok_or("missing process start time")?
                .to_string(),
        })
    }

    fn alive(&self) -> bool {
        Self::read(self.pid).is_ok_and(|current| current == *self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    request: StartRequest,
    summary: RunSummary,
    owner: Option<ProcessIdentity>,
    accepted_ms: u64,
    started_ms: Option<u64>,
    usage: Usage,
    result: Option<RunResult>,
    #[serde(default)]
    failure: Option<orchestration_contract::execution::Failure>,
}

/// SQLite is authoritative. Per-run JSON files are crash-repairable projections
/// rebuilt after committed mutations. Read-only queries never repair projections. Transactions
/// serialize multiple entry processes and worker writes without a daemon lock.
#[derive(Clone)]
pub struct RunManager {
    root: PathBuf,
    backends: Vec<BackendRegistration>,
}

impl RunManager {
    pub fn open(root: impl AsRef<Path>, backends: Vec<BackendRegistration>) -> Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        fs::set_permissions(root.as_ref(), fs::Permissions::from_mode(0o700))?;
        let root = fs::canonicalize(root)?;
        let manager = Self { root, backends };
        manager.connection()?.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
             CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY, start_key TEXT NOT NULL UNIQUE, record TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS events (
                run_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                source TEXT NOT NULL, source_sequence INTEGER NOT NULL, event TEXT NOT NULL,
                PRIMARY KEY(run_id, sequence), UNIQUE(run_id, source, source_sequence)
             );",
        )?;
        Ok(manager)
    }

    fn connection(&self) -> Result<Connection> {
        let connection = Connection::open(self.root.join("runs.sqlite"))?;
        connection.busy_timeout(Duration::from_secs(10))?;
        connection.execute_batch("PRAGMA synchronous=FULL;")?;
        Ok(connection)
    }

    /// Query clients neither initialize schemas nor require backend assembly.
    pub fn open_read_only(root: impl AsRef<Path>) -> Result<Self> {
        let manager = Self {
            root: fs::canonicalize(root)?,
            backends: Vec::new(),
        };
        manager.reader_connection()?;
        Ok(manager)
    }

    fn reader_connection(&self) -> Result<Connection> {
        let connection = Connection::open_with_flags(
            self.root.join("runs.sqlite"),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        connection.busy_timeout(Duration::from_secs(10))?;
        Ok(connection)
    }

    fn read<T>(
        &self,
        id: &str,
        query: impl FnOnce(&Transaction<'_>, &Record) -> Result<T>,
    ) -> Result<T> {
        let mut connection = self.reader_connection()?;
        let tx = connection.transaction()?;
        let mut record: Record = serde_json::from_str(&tx.query_row(
            "SELECT record FROM runs WHERE id=?1",
            [id],
            |r| r.get::<_, String>(0),
        )?)?;
        observe_interruption(&mut record);
        query(&tx, &record)
    }

    pub fn start_run(&self, request: StartRequest) -> std::result::Result<StartResponse, RunError> {
        self.start_checked(request).map_err(|error| {
            error
                .downcast_ref::<RunError>()
                .cloned()
                .unwrap_or_else(|| RunError::new(RunErrorCode::Internal, "run admission failed"))
        })
    }

    fn start_checked(&self, request: StartRequest) -> Result<StartResponse> {
        if request.start_key.trim().is_empty() || request.goal.trim().is_empty() {
            return Err(RunError::new(
                RunErrorCode::InvalidRequest,
                "start key and goal must not be empty",
            )
            .into());
        }
        if request.config.budget != request.approval.budget {
            return Err(RunError::new(
                RunErrorCode::BudgetApprovalMismatch,
                "budget approval does not match requested limits",
            )
            .into());
        }
        if let CodeSource::WorkspacePackage {
            digest,
            manifest_ref,
        } = &request.code_source
        {
            if !request.approval.workspace.as_ref().is_some_and(|approval| &approval.digest == digest && &approval.manifest_ref == manifest_ref && matches!(request.config.backend, BackendSelection::Explicit(kind) if kind == approval.backend)) {
                return Err(RunError::new(RunErrorCode::WorkspaceApprovalMismatch, "workspace approval does not match source and backend").into());
            }
            return Err(RunError::new(
                RunErrorCode::WorkspaceUnsupported,
                "workspace package upload is unsupported",
            )
            .into());
        }
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing = tx.query_row(
            "SELECT record FROM runs WHERE start_key=?1",
            [&request.start_key],
            |row| row.get::<_, String>(0),
        );
        match existing {
            Ok(json) => {
                let record: Record = serde_json::from_str(&json)?;
                if record.request != request {
                    return Err(RunError::new(
                        RunErrorCode::StartKeyConflict,
                        "start key conflicts with an existing request",
                    )
                    .into());
                }
                tx.commit()?;
                self.project(&record.summary.run_id)?;
                return Ok(StartResponse {
                    run_id: record.summary.run_id,
                    status: record.summary.status,
                });
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {}
            Err(error) => return Err(error.into()),
        }
        let backend = select_backend(&request, &self.backends)?;
        let record = Record {
            summary: RunSummary {
                run_id: Uuid::new_v4().to_string(),
                status: RunStatus::Accepted,
                summary: "accepted; awaiting worker".into(),
                backend: Some(backend),
                cleanup: CleanupStatus::Pending,
                source_session: request.source_session.clone(),
            },
            request,
            owner: None,
            accepted_ms: now_ms(),
            started_ms: None,
            usage: Usage::default(),
            result: None,
            failure: None,
        };
        tx.execute(
            "INSERT INTO runs(id,start_key,record) VALUES(?1,?2,?3)",
            params![
                record.summary.run_id,
                record.request.start_key,
                serde_json::to_string(&record)?
            ],
        )?;
        append_lifecycle(&tx, &record)?;
        tx.commit()?;
        self.project(&record.summary.run_id)?;
        Ok(StartResponse {
            run_id: record.summary.run_id,
            status: RunStatus::Accepted,
        })
    }

    /// Called by the actual worker before execution. Claiming does not itself
    /// create resources. An accepted run whose worker never claims is interrupted
    /// after the launch grace period; first implementation never resumes it.
    pub fn claim(&self, run_id: &str) -> Result<StartRequest> {
        self.update(run_id, |tx, record| {
            if !matches!(
                record.summary.status,
                RunStatus::Accepted | RunStatus::Cancelling
            ) {
                return Err("run cannot be claimed in its current state".into());
            }
            record.owner = Some(ProcessIdentity::read(std::process::id())?);
            record.started_ms = Some(now_ms());
            if record.summary.status != RunStatus::Cancelling {
                record.summary.status = RunStatus::Running;
            }
            record.summary.summary = "worker running".into();
            append_lifecycle(tx, record)?;
            Ok(record.request.clone())
        })
    }

    pub fn status_run(&self, request: StatusRequest) -> Result<StatusResponse> {
        self.read(&request.run_id, |tx, record| {
            let mut query = tx.prepare(
                "SELECT event FROM events WHERE run_id=?1 AND sequence>?2 ORDER BY sequence LIMIT ?3",
            )?;
            let events = query.query_map(
                params![request.run_id, request.after_sequence, u64::from(request.limit.min(1000)) + 1],
                |row| row.get::<_, String>(0),
            )?.map(|r| Ok(serde_json::from_str::<RunEvent>(&r?)?)).collect::<Result<Vec<_>>>()?;
            let has_more = events.len() > request.limit.min(1000) as usize;
            let events: Vec<_> = events.into_iter().take(request.limit.min(1000) as usize).collect();
            let next_sequence = events.last().map_or(request.after_sequence, |event| event.sequence);
            Ok(StatusResponse { run: record.summary.clone(), events, next_sequence, has_more })
        })
    }

    pub fn cancel_run(&self, request: CancelRequest) -> Result<CancelResponse> {
        self.update(&request.run_id, |tx, record| {
            if !terminal(record.summary.status) && record.summary.status != RunStatus::Cancelling {
                record.summary.status = RunStatus::Cancelling;
                record.summary.summary = request.reason;
                append_lifecycle(tx, record)?;
            }
            Ok(CancelResponse {
                run_id: request.run_id.clone(),
                status: record.summary.status,
                effective: record.summary.status == RunStatus::Cancelled,
                cleanup: record.summary.cleanup.clone(),
            })
        })
    }

    /// Worker only: record a terminal after execution has stopped, with an honest
    /// cleanup result. Completion cannot overwrite a previously recorded terminal.
    pub fn finish(
        &self,
        run_id: &str,
        status: RunStatus,
        cleanup: CleanupStatus,
        result: Option<RunResult>,
    ) -> Result<()> {
        if !terminal(status) || status == RunStatus::Interrupted {
            return Err("invalid worker terminal".into());
        }
        self.update(run_id, |tx, record| {
            require_owner(record)?;
            if terminal(record.summary.status) {
                return Ok(());
            }
            let status = if record.summary.status == RunStatus::Cancelling
                && status == RunStatus::Completed
            {
                if cleanup == CleanupStatus::Complete {
                    RunStatus::Cancelled
                } else {
                    RunStatus::Failed
                }
            } else {
                status
            };
            let result = if status == RunStatus::Cancelled {
                result.map(|mut value| {
                    value.stop_reason = "user_cancelled".into();
                    value
                })
            } else {
                result
            };
            record.summary.status = status;
            record.summary.cleanup = cleanup;
            record.summary.summary = result
                .as_ref()
                .map_or_else(|| format!("{status:?}"), |r| r.summary.clone());
            record.result = result;
            append_lifecycle(tx, record)
        })
    }

    pub fn result_run(&self, request: ResultRequest) -> Result<ResultResponse> {
        self.read(&request.run_id, |_, record| {
            Ok(ResultResponse {
                run: record.summary.clone(),
                result: record.result.clone(),
                failure: record.failure.clone(),
            })
        })
    }

    pub fn list_runs(&self, request: ListRequest) -> Result<ListResponse> {
        let connection = self.reader_connection()?;
        let mut query = connection.prepare("SELECT id FROM runs WHERE id>?1 ORDER BY id")?;
        let ids = query
            .query_map([request.cursor.as_deref().unwrap_or("")], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut runs = Vec::new();
        let mut cursor = None;
        for id in ids {
            let run = self.result_run(ResultRequest { run_id: id.clone() })?.run;
            if request.source_session_id.as_ref().is_some_and(|filter| {
                run.source_session.as_ref().map(|s| &s.session_id) != Some(filter)
            }) {
                continue;
            }
            if runs.len() >= request.limit.min(1000) as usize {
                cursor = runs.last().map(|r: &RunSummary| r.run_id.clone());
                break;
            }
            runs.push(run);
        }
        Ok(ListResponse {
            runs,
            next_cursor: cursor,
        })
    }

    pub fn record_event(
        &self,
        run_id: &str,
        source: EventSource,
        event: EventPayload,
    ) -> Result<()> {
        if source.source == "run-manager" || matches!(event, EventPayload::Lifecycle { .. }) {
            return Err("lifecycle events are reserved for run management".into());
        }
        self.update(run_id, |tx, record| {
            require_owner(record)?;
            if terminal(record.summary.status) {
                return Err("run has terminated".into());
            }
            append_event(tx, run_id, source, event)
        })
    }

    /// Cumulative, monotonic accounting avoids double counting repeated job
    /// notifications. In-flight tail usage remains recorded even beyond a limit.
    /// False means do not admit more work; the algorithm may still select/export.
    pub fn account_usage(&self, run_id: &str, usage: Usage) -> Result<bool> {
        self.update(run_id, |_, record| {
            require_owner(record)?;
            if usage.tokens < record.usage.tokens
                || usage.cost_microunits < record.usage.cost_microunits
            {
                return Err("usage cannot decrease".into());
            }
            record.usage = usage;
            let budget = &record.request.config.budget;
            Ok(record.summary.status == RunStatus::Running
                && record.usage.tokens < budget.max_tokens
                && now_ms().saturating_sub(record.started_ms.unwrap_or(record.accepted_ms))
                    < budget.max_time_ms
                && budget
                    .max_cost_microunits
                    .is_none_or(|limit| record.usage.cost_microunits < limit))
        })
    }

    /// Build the backend request from the authoritative run record, never from
    /// algorithm-supplied configuration. The owning worker performs the actual
    /// dispatch, including its resource ownership and capability checks.
    pub fn fork_request(&self, params: HostForkParams) -> Result<ForkRequest> {
        let id = params.context.call.run_id.clone();
        self.update(&id, |_, record| {
            require_owner(record)?;
            if record.summary.status != RunStatus::Running {
                return Err("run is not accepting execution requests".into());
            }
            Ok(params.into_backend_request(&record.request.config))
        })
    }

    pub fn request(&self, run_id: &str) -> Result<StartRequest> {
        self.read(run_id, |_, record| Ok(record.request.clone()))
    }

    pub fn run_directory(&self, run_id: &str) -> Result<PathBuf> {
        self.request(run_id)?;
        Ok(self.root.join(run_id))
    }

    pub fn fail(&self, run_id: &str, message: String) -> Result<()> {
        self.update(run_id, |_, record| {
            require_owner(record)?;
            record.failure = Some(orchestration_contract::execution::Failure {
                code: "execution_failed".into(),
                message,
            });
            Ok(())
        })
    }

    fn update<T>(
        &self,
        id: &str,
        change: impl FnOnce(&Transaction<'_>, &mut Record) -> Result<T>,
    ) -> Result<T> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut record: Record = serde_json::from_str(&tx.query_row(
            "SELECT record FROM runs WHERE id=?1",
            [id],
            |r| r.get::<_, String>(0),
        )?)?;
        if observe_interruption(&mut record) {
            append_lifecycle(&tx, &record)?;
        }
        let outcome = change(&tx, &mut record);
        tx.execute(
            "UPDATE runs SET record=?1 WHERE id=?2",
            params![serde_json::to_string(&record)?, id],
        )?;
        tx.commit()?;
        self.project(&record.summary.run_id)?;
        outcome
    }

    fn project(&self, id: &str) -> Result<()> {
        // Hold the writer lock while reloading and projecting the latest durable
        // record, so a slower entry process cannot overwrite a newer projection.
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let record: Record = serde_json::from_str(&tx.query_row(
            "SELECT record FROM runs WHERE id=?1",
            [id],
            |r| r.get::<_, String>(0),
        )?)?;
        let directory = self.root.join(&record.summary.run_id);
        fs::create_dir_all(&directory)?;
        atomic_json(&directory.join("config.json"), &record.request)?;
        atomic_json(&directory.join("state.json"), &record.summary)?;
        atomic_json(&directory.join("usage.json"), &record.usage)?;
        atomic_json(&directory.join("result.json"), &record.result)?;
        let mut query = tx.prepare("SELECT event FROM events WHERE run_id=?1 ORDER BY sequence")?;
        let mut events = Vec::new();
        for json in query.query_map([&record.summary.run_id], |r| r.get::<_, String>(0))? {
            events.extend_from_slice(json?.as_bytes());
            events.push(b'\n');
        }
        atomic_write(&directory.join("events.jsonl"), &events)?;
        drop(query);
        tx.commit()?;
        Ok(())
    }
}

// Query clients report a vanished owner without acquiring a writer lock. The
// next mutation persists that observation and its lifecycle event.
fn observe_interruption(record: &mut Record) -> bool {
    let vanished = record.owner.as_ref().is_some_and(|p| !p.alive())
        || (record.owner.is_none() && now_ms().saturating_sub(record.accepted_ms) > 30000);
    if !terminal(record.summary.status) && vanished {
        record.summary.status = RunStatus::Interrupted;
        record.summary.cleanup = CleanupStatus::Unknown;
        record.summary.summary = "worker disappeared; cleanup unconfirmed".into();
        true
    } else {
        false
    }
}

fn require_owner(record: &Record) -> Result<()> {
    if record.owner.as_ref() != Some(&ProcessIdentity::read(std::process::id())?) {
        return Err("only the owning worker may update execution state".into());
    }
    Ok(())
}

fn terminal(status: RunStatus) -> bool {
    matches!(
        status,
        RunStatus::Completed | RunStatus::Cancelled | RunStatus::Failed | RunStatus::Interrupted
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn append_lifecycle(tx: &Transaction<'_>, record: &Record) -> Result<()> {
    let sequence: u64 = tx.query_row(
        "SELECT COUNT(*) FROM events WHERE run_id=?1",
        [&record.summary.run_id],
        |r| r.get(0),
    )?;
    append_event(
        tx,
        &record.summary.run_id,
        EventSource {
            source: "run-manager".into(),
            source_sequence: sequence + 1,
        },
        EventPayload::Lifecycle {
            status: record.summary.status,
            summary: record.summary.summary.clone(),
            cleanup: record.summary.cleanup.clone(),
        },
    )
}

fn append_event(
    tx: &Transaction<'_>,
    id: &str,
    source: EventSource,
    payload: EventPayload,
) -> Result<()> {
    let prior: Option<String> = rusqlite::OptionalExtension::optional(tx.query_row(
        "SELECT event FROM events WHERE run_id=?1 AND source=?2 AND source_sequence=?3",
        params![id, source.source, source.source_sequence],
        |r| r.get(0),
    ))?;
    if let Some(json) = prior {
        if serde_json::from_str::<RunEvent>(&json)?.event != payload {
            return Err("event identity conflict".into());
        }
        return Ok(());
    }
    let sequence: u64 = tx.query_row(
        "SELECT COALESCE(MAX(sequence),0)+1 FROM events WHERE run_id=?1",
        [id],
        |r| r.get(0),
    )?;
    // SQLite supplies a UTC RFC3339 timestamp without another clock dependency.
    let timestamp: String =
        tx.query_row("SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')", [], |r| {
            r.get(0)
        })?;
    let event = RunEvent {
        run_id: id.into(),
        sequence,
        source,
        timestamp,
        event: payload,
    };
    tx.execute(
        "INSERT INTO events(run_id,sequence,source,source_sequence,event) VALUES(?1,?2,?3,?4,?5)",
        params![
            id,
            sequence,
            event.source.source,
            event.source.source_sequence,
            serde_json::to_string(&event)?
        ],
    )?;
    Ok(())
}

pub(crate) fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    atomic_write(path, &serde_json::to_vec_pretty(value)?)
}

pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    let operation = || -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(path.parent().ok_or("missing parent")?)?.sync_all()?;
        Ok(())
    };
    let result = operation();
    if temporary.exists() {
        fs::remove_file(temporary)?;
    }
    result
}

#[async_trait]
impl RunClient for RunManager {
    async fn start(&self, request: StartRequest) -> std::result::Result<StartResponse, String> {
        self.start_run(request).map_err(|e| e.json())
    }
    async fn status(&self, request: StatusRequest) -> std::result::Result<StatusResponse, String> {
        self.status_run(request).map_err(|e| e.to_string())
    }
    async fn cancel(&self, request: CancelRequest) -> std::result::Result<CancelResponse, String> {
        self.cancel_run(request).map_err(|e| e.to_string())
    }
    async fn result(&self, request: ResultRequest) -> std::result::Result<ResultResponse, String> {
        self.result_run(request).map_err(|e| e.to_string())
    }
    async fn list(&self, request: ListRequest) -> std::result::Result<ListResponse, String> {
        self.list_runs(request).map_err(|e| e.to_string())
    }
}
