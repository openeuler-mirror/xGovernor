//! Durable [`SessionRepository`] backed by SQLite.
//!
//! This module only opens whatever path its caller gives it — deciding
//! *where* that file lives (by convention, `~/.xgovernor/xgovernor.db`) is
//! the assembly layer's job, see `apps/server/src/main.rs`. Before this
//! module existed, `SessionRepository` was only ever implemented as a
//! `Mutex<HashMap<..>>` (`apps/server/src/main.rs`'s
//! `InMemorySessionRepository`): a daemon restart lost every session's
//! bookkeeping even though the underlying sandbox often kept running,
//! unreachable through the control plane (`docs/session_orchestration_
//! skeleton.md` §6). This is the fix for that specific gap — the paired
//! `provider_instances` ledger (`crates/backend/src/sqlite_ledger.rs`)
//! closes the analogous gap on the provider side.
//!
//! One row per [`SessionRecord::runtime_id`]. Scalar/queryable fields
//! (`status`, both timestamps, `tenant_id`, `created_by`) are their own
//! columns; the nested structs (`WorkspaceFacts`, `IsolationFacts`,
//! `EffectiveCapabilities`, `OpaqueRuntimeState`, `ResolvedLlm`,
//! `SessionLease`, `CheckpointLineage`) are stored as JSON blobs — they are
//! already `Serialize`/`Deserialize` and none of their fields need
//! independent SQL querying today. Should that change (e.g. a future
//! per-tenant session listing that filters on workspace kind), the
//! relevant field can be promoted to its own column without touching the
//! others.
//!
//! `rusqlite::Connection` is synchronous; every call below runs the actual
//! query inside `tokio::task::spawn_blocking` so `SessionRepository`'s
//! async trait methods never block the calling task's executor thread.
//! WAL mode is enabled on open, matching this codebase's single-writer
//! control-plane posture (`docs/tenancy_design.md` line 4: "单写者控制面").

use crate::application::{
    CheckpointListEntry, CheckpointListPage, SessionListPage, SessionRepository,
};
use crate::domain::{
    CheckpointLineage, EffectiveCapabilities, IsolationFacts, OpaqueRuntimeState, ResolvedLlm,
    SessionDomainError, SessionLease, SessionRecord, SessionStatus, WorkspaceFacts,
};
use async_trait::async_trait;
use rusqlite::{params, Connection, OptionalExtension, Row};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Durable, single-process SQLite-backed [`SessionRepository`].
pub struct SqliteSessionRepository {
    pub(crate) conn: Arc<Mutex<Connection>>,
    joint_transactions: bool,
    #[cfg(unix)]
    pub(crate) deployment: Option<Arc<crate::DeploymentGuard>>,
    pub(crate) joint_budget_policy: crate::JointBudgetPolicy,
}

impl SqliteSessionRepository {
    /// Opens (creating if absent, including parent directories) the SQLite
    /// file at `path`, creates the `sessions` table if it doesn't exist
    /// yet, and enables WAL mode.
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let mut conn = Connection::open(path)?;
        Self::init(&mut conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            joint_transactions: false,
            #[cfg(unix)]
            deployment: None,
            joint_budget_policy: Default::default(),
        })
    }

    /// In-memory database. For tests only — by definition there is nothing
    /// to survive a restart.
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let mut conn = Connection::open_in_memory()?;
        Self::init(&mut conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            joint_transactions: false,
            #[cfg(unix)]
            deployment: None,
            joint_budget_policy: Default::default(),
        })
    }

    /// Explicit assembly opt-in for a verified joint lifecycle owner. Defaults to disabled.
    pub fn with_joint_transactions(mut self) -> Self {
        self.joint_transactions = true;
        self
    }

    /// Sets admission capacity without enabling joint lifecycle assembly.
    /// Existing reservations remain charged when a new policy lowers capacity.
    pub fn with_joint_budget_policy(
        mut self,
        policy: crate::JointBudgetPolicy,
    ) -> Result<Self, SessionDomainError> {
        if policy.iter().any(|(kind, budget)| {
            ![
                "session",
                "root",
                "replica",
                "checkpoint",
                "repair",
                "disk",
                "operation",
                "record_bytes",
            ]
            .contains(&kind.as_str())
                || budget.recovery_reserve > budget.total
        }) {
            return Err(SessionDomainError::InvalidRequest {
                message: "invalid joint resource budget".into(),
            });
        }
        #[cfg(unix)]
        if let Some(deployment) = &self.deployment {
            deployment.check_policy(&policy)?;
        }
        self.joint_budget_policy = policy;
        Ok(self)
    }

    fn init(connection: &mut Connection) -> rusqlite::Result<()> {
        let _ = connection.pragma_update(None, "busy_timeout", 5_000i64);
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        // Enrollment and initialization serialize with business writers. A reader
        // must not migrate a database whose deployment holder it does not own.
        let conn =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if deployment_registered(&conn)? {
            crate::sqlite_host::validate_schema(&conn)?;
            crate::sqlite_cancel::validate_existing_schema(&conn)?;
            return Ok(());
        }
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS snapshot_operations (id TEXT PRIMARY KEY, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS sessions (
                runtime_id TEXT PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                sender_id TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                workspace_json TEXT NOT NULL,
                isolation_json TEXT NOT NULL,
                capabilities_json TEXT NOT NULL,
                runtime_json TEXT NOT NULL,
                llm_json TEXT,
                lease_json TEXT,
                lineage_json TEXT,
                last_error TEXT,
                tenant_id TEXT,
                created_by TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sessions_tenant_id ON sessions(tenant_id);
            CREATE TABLE IF NOT EXISTS checkpoints (
                checkpoint_id TEXT PRIMARY KEY,
                source_runtime_id TEXT NOT NULL,
                provider_snapshot_id TEXT NOT NULL,
                runtime_json TEXT NOT NULL,
                workspace_json TEXT NOT NULL,
                isolation_json TEXT NOT NULL,
                capabilities_json TEXT NOT NULL,
                owner_ref TEXT NOT NULL,
                tenant_id TEXT,
                created_by TEXT,
                created_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_checkpoints_tenant_runtime ON checkpoints(tenant_id, source_runtime_id);",
        )?;
        let has_execution = conn
            .prepare("PRAGMA table_info(sessions)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .iter()
            .any(|name| name == "execution_json");
        if !has_execution {
            conn.execute_batch("ALTER TABLE sessions ADD COLUMN execution_json TEXT NOT NULL DEFAULT '{\"kind\":\"split_v1\"}';")?;
        }
        crate::sqlite_joint::init(&conn)?;
        crate::sqlite_host::init(&conn)?;
        conn.commit()?;
        let _ = connection.pragma_update(None, "journal_mode", "WAL");
        Ok(())
    }
}

/// The durable deployment row is also a write-protection marker. Treat even an
/// incomplete registration as protected rather than guessing legacy ownership.
pub(crate) fn deployment_registered(conn: &Connection) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='local_deployment')",
        [],
        |row| row.get(0),
    )
}

pub(crate) fn verify_writer(
    conn: &Connection,
    #[cfg(unix)] deployment: Option<&crate::DeploymentGuard>,
) -> rusqlite::Result<()> {
    #[cfg(unix)]
    if let Some(holder) = deployment {
        return holder.validate().map_err(|_| rusqlite::Error::InvalidQuery);
    }
    if deployment_registered(conn)? {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(())
}

fn status_to_str(status: SessionStatus) -> &'static str {
    match status {
        SessionStatus::Opening => "opening",
        SessionStatus::Idle => "idle",
        SessionStatus::Running => "running",
        SessionStatus::Paused => "paused",
        SessionStatus::Failed => "failed",
        SessionStatus::Closed => "closed",
    }
}

fn str_to_status(value: &str) -> Option<SessionStatus> {
    Some(match value {
        "opening" => SessionStatus::Opening,
        "idle" => SessionStatus::Idle,
        "running" => SessionStatus::Running,
        "paused" => SessionStatus::Paused,
        "failed" => SessionStatus::Failed,
        "closed" => SessionStatus::Closed,
        _ => return None,
    })
}

/// Local error union for the blocking closures below — they can fail via
/// either `rusqlite` (storage) or `serde_json` (encode/decode of the JSON
/// columns). Collapsed to a single `String` before crossing the
/// `spawn_blocking` boundary since `SessionDomainError` is where it
/// ultimately needs to land, and that variant only carries a message.
fn json_encode<T: serde::Serialize>(value: &T) -> Result<String, String> {
    serde_json::to_string(value).map_err(|error| error.to_string())
}

fn json_decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, String> {
    serde_json::from_str(value).map_err(|error| error.to_string())
}

fn row_to_record(row: &Row) -> rusqlite::Result<Result<SessionRecord, String>> {
    // All raw column reads happen here, where `?` resolves against
    // `rusqlite::Error` (the function's own return type). JSON decoding
    // happens below in a separately-typed closure that returns `String`
    // errors instead — the two error types can't share one `?`-chain, so
    // the read step is deliberately finished before the decode step starts.
    let status_str: String = row.get("status")?;
    let workspace_json: String = row.get("workspace_json")?;
    let isolation_json: String = row.get("isolation_json")?;
    let capabilities_json: String = row.get("capabilities_json")?;
    let runtime_json: String = row.get("runtime_json")?;
    let execution_json: String = row.get("execution_json")?;
    let llm_json: Option<String> = row.get("llm_json")?;
    let lease_json: Option<String> = row.get("lease_json")?;
    let lineage_json: Option<String> = row.get("lineage_json")?;
    let runtime_id: String = row.get("runtime_id")?;
    let conversation_id: String = row.get("conversation_id")?;
    let sender_id: String = row.get("sender_id")?;
    let created_at_ms = row.get::<_, i64>("created_at_ms")? as u64;
    let updated_at_ms = row.get::<_, i64>("updated_at_ms")? as u64;
    let last_error: Option<String> = row.get("last_error")?;
    let tenant_id: Option<String> = row.get("tenant_id")?;
    let created_by: String = row.get("created_by")?;

    let decoded = (|| -> Result<SessionRecord, String> {
        let Some(status) = str_to_status(&status_str) else {
            return Err(format!("unknown session status in storage: {status_str}"));
        };
        let workspace: WorkspaceFacts = json_decode(&workspace_json)?;
        let isolation: IsolationFacts = json_decode(&isolation_json)?;
        let capabilities: EffectiveCapabilities = json_decode(&capabilities_json)?;
        let runtime: OpaqueRuntimeState = json_decode(&runtime_json)?;
        let llm: Option<ResolvedLlm> = llm_json.as_deref().map(json_decode).transpose()?;
        let lease: Option<SessionLease> = lease_json.as_deref().map(json_decode).transpose()?;
        let lineage: Option<CheckpointLineage> =
            lineage_json.as_deref().map(json_decode).transpose()?;

        Ok(SessionRecord {
            execution_backing: json_decode(&execution_json)?,
            runtime_id,
            conversation_id,
            sender_id,
            status,
            created_at_ms,
            updated_at_ms,
            workspace,
            isolation,
            capabilities,
            runtime,
            llm,
            lease,
            lineage,
            last_error,
            tenant_id,
            created_by,
        })
    })();

    Ok(decoded)
}

fn internal_error(message: impl Into<String>) -> SessionDomainError {
    SessionDomainError::Internal {
        message: message.into(),
        source: None,
    }
}

fn read_session(conn: &Connection, id: &str) -> Result<Option<SessionRecord>, String> {
    let body: Option<String> = conn
        .query_row(
            "SELECT body FROM joint_sessions WHERE runtime_id=?1",
            [id],
            |r| r.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    if let Some(body) = body {
        return json_decode(&body).map(Some);
    }
    let found = conn
        .query_row(
            "SELECT * FROM sessions WHERE runtime_id=?1",
            [id],
            row_to_record,
        )
        .optional()
        .map_err(|e| e.to_string())?;
    found.transpose()
}

#[async_trait]
impl SessionRepository for SqliteSessionRepository {
    fn host_repository(&self) -> Option<&dyn crate::HostRepository> {
        Some(self)
    }
    fn supports_joint_transactions(&self) -> bool {
        self.joint_transactions
    }
    async fn get_joint_binding(
        &self,
        binding: crate::JointBinding,
    ) -> Result<Option<crate::JointBindingState>, SessionDomainError> {
        self.read_joint_binding(binding).await
    }
    async fn get_joint_checkpoint(
        &self,
        id: &str,
    ) -> Result<Option<crate::JointCheckpointRecord>, SessionDomainError> {
        self.read_joint_checkpoint(id).await
    }
    async fn prepare_joint_operation(
        &self,
        request: crate::JointPrepareRequest,
    ) -> Result<crate::JointAdmission, SessionDomainError> {
        SqliteSessionRepository::prepare_joint_operation(self, request).await
    }
    async fn get_joint_turn(
        &self,
        runtime_id: String,
        client_request_id: String,
    ) -> Result<Option<crate::JointTurnRecord>, SessionDomainError> {
        SqliteSessionRepository::get_joint_turn(self, runtime_id, client_request_id).await
    }
    async fn joint_turn_result(
        &self,
        admission: crate::JointAdmission,
    ) -> Result<Option<crate::JointTurnTerminal>, SessionDomainError> {
        SqliteSessionRepository::joint_turn_result(self, admission).await
    }
    async fn finish_joint_turn(
        &self,
        admission: crate::JointAdmission,
        terminal: crate::JointTurnTerminal,
    ) -> Result<(), SessionDomainError> {
        SqliteSessionRepository::finish_joint_turn(self, admission, terminal).await
    }
    async fn prepare_joint_cancel(
        &self,
        context: crate::JointOperationContext,
        session: SessionRecord,
        turn_id: Option<String>,
    ) -> Result<Option<crate::JointTurnRecord>, SessionDomainError> {
        SqliteSessionRepository::prepare_joint_cancel(self, context, session, turn_id).await
    }
    async fn joint_cancel_control(
        &self,
        admission: crate::JointAdmission,
    ) -> Result<Option<crate::JointCancelControl>, SessionDomainError> {
        SqliteSessionRepository::joint_cancel_control(self, admission).await
    }
    async fn prepare_joint_cancel_native(
        &self,
        admission: crate::JointAdmission,
        native: crate::JointNativeIntent,
    ) -> Result<crate::JointCancelControl, SessionDomainError> {
        SqliteSessionRepository::prepare_joint_cancel_native(self, admission, native).await
    }
    async fn claim_joint_cancel_send(
        &self,
        admission: crate::JointAdmission,
    ) -> Result<bool, SessionDomainError> {
        SqliteSessionRepository::claim_joint_cancel_send(self, admission).await
    }
    async fn record_joint_cancel_result(
        &self,
        admission: crate::JointAdmission,
        native: crate::JointNativeIntent,
        outcome: crate::JointNativeOutcome,
    ) -> Result<(), SessionDomainError> {
        SqliteSessionRepository::record_joint_cancel_result(self, admission, native, outcome).await
    }
    async fn joint_workspace_boundary(
        &self,
        binding: crate::JointBinding,
    ) -> Result<crate::JointWorkspaceBoundary, SessionDomainError> {
        SqliteSessionRepository::joint_workspace_boundary(self, binding).await
    }
    async fn prepare_joint_checkpoint_boundary(
        &self,
        binding: crate::JointBinding,
        native: crate::JointNativeIntent,
    ) -> Result<crate::JointCheckpointBoundaryObservation, SessionDomainError> {
        SqliteSessionRepository::prepare_joint_checkpoint_boundary(self, binding, native).await
    }
    async fn confirm_joint_checkpoint_boundary(
        &self,
        ticket: crate::JointCheckpointBoundaryObservation,
        checkpoint_id: String,
    ) -> Result<bool, SessionDomainError> {
        SqliteSessionRepository::confirm_joint_checkpoint_boundary(self, ticket, checkpoint_id)
            .await
    }
    async fn invalidate_joint_turn(
        &self,
        admission: crate::JointAdmission,
    ) -> Result<(), SessionDomainError> {
        SqliteSessionRepository::invalidate_joint_turn(self, admission).await
    }
    async fn begin_joint_compensation(
        &self,
        admission: crate::JointAdmission,
    ) -> Result<(), SessionDomainError> {
        SqliteSessionRepository::begin_joint_compensation(self, admission).await
    }
    async fn claim_joint_native_send(
        &self,
        admission: crate::JointAdmission,
        step: String,
    ) -> Result<Option<crate::JointNativeIntent>, SessionDomainError> {
        SqliteSessionRepository::claim_joint_native_send(self, admission, step).await
    }
    async fn append_joint_native_step(
        &self,
        admission: crate::JointAdmission,
        native: crate::JointNativeIntent,
    ) -> Result<(), SessionDomainError> {
        SqliteSessionRepository::append_joint_native_step(self, admission, native).await
    }
    async fn record_joint_native_result(
        &self,
        admission: crate::JointAdmission,
        step: String,
        operation_id: String,
        digest: String,
        outcome: crate::JointNativeOutcome,
    ) -> Result<(), SessionDomainError> {
        SqliteSessionRepository::record_joint_native_result(
            self,
            admission,
            step,
            operation_id,
            digest,
            outcome,
        )
        .await
    }
    async fn publish_joint_session(
        &self,
        admission: crate::JointAdmission,
        record: SessionRecord,
    ) -> Result<SessionRecord, SessionDomainError> {
        SqliteSessionRepository::publish_joint_session(self, admission, record).await
    }
    async fn publish_joint_checkpoint(
        &self,
        admission: crate::JointAdmission,
        record: crate::JointCheckpointRecord,
    ) -> Result<crate::JointCheckpointRecord, SessionDomainError> {
        SqliteSessionRepository::publish_joint_checkpoint(self, admission, record).await
    }
    async fn finish_joint_release(
        &self,
        admission: crate::JointAdmission,
        reference_kind: String,
        public_id: String,
        updated_at_ms: u64,
    ) -> Result<(), SessionDomainError> {
        SqliteSessionRepository::finish_joint_release(
            self,
            admission,
            reference_kind,
            public_id,
            updated_at_ms,
        )
        .await
    }
    async fn finish_joint_compensation(
        &self,
        admission: crate::JointAdmission,
    ) -> Result<(), SessionDomainError> {
        SqliteSessionRepository::finish_joint_compensation(self, admission).await
    }
    async fn pending_joint_operations(
        &self,
    ) -> Result<Vec<crate::JointPendingOperation>, SessionDomainError> {
        SqliteSessionRepository::pending_joint_operations(self).await
    }
    async fn get(&self, runtime_id: &str) -> Result<Option<SessionRecord>, SessionDomainError> {
        let conn = self.conn.clone();
        let runtime_id = runtime_id.to_string();
        let result = tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|_| "sqlite session repository lock poisoned".to_string())?;
            read_session(&conn, &runtime_id)
        })
        .await
        .map_err(|error| internal_error(format!("sqlite get task panicked: {error}")))?;

        result.map_err(|error| internal_error(format!("sqlite get failed: {error}")))
    }

    async fn save(&self, record: SessionRecord) -> Result<(), SessionDomainError> {
        if record.execution_backing.joint().is_some() {
            return Err(crate::unsupported_joint("publication"));
        }
        let conn = self.conn.clone();
        #[cfg(unix)]
        let deployment = self.deployment.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<(), String> {
            let workspace_json = json_encode(&record.workspace)?;
            let isolation_json = json_encode(&record.isolation)?;
            let capabilities_json = json_encode(&record.capabilities)?;
            let runtime_json = json_encode(&record.runtime)?;
            let execution_json = json_encode(&record.execution_backing)?;
            let llm_json = record.llm.as_ref().map(json_encode).transpose()?;
            let lease_json = record.lease.as_ref().map(json_encode).transpose()?;
            let lineage_json = record.lineage.as_ref().map(json_encode).transpose()?;

            let mut conn = conn
                .lock()
                .map_err(|_| "sqlite session repository lock poisoned".to_string())?;
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(|e| e.to_string())?;
            verify_writer(
                &tx,
                #[cfg(unix)]
                deployment.as_deref(),
            )
            .map_err(|e| e.to_string())?;
            let changed = tx
                .execute(
                    "INSERT INTO sessions (
                    runtime_id, conversation_id, sender_id, status,
                    created_at_ms, updated_at_ms, workspace_json, isolation_json,
                    capabilities_json, runtime_json, llm_json, lease_json,
                    lineage_json, last_error, tenant_id, created_by, execution_json
                ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)
                ON CONFLICT(runtime_id) DO UPDATE SET
                    conversation_id = excluded.conversation_id,
                    sender_id = excluded.sender_id,
                    status = excluded.status,
                    updated_at_ms = excluded.updated_at_ms,
                    workspace_json = excluded.workspace_json,
                    isolation_json = excluded.isolation_json,
                    capabilities_json = excluded.capabilities_json,
                    runtime_json = excluded.runtime_json,
                    llm_json = excluded.llm_json,
                    lease_json = excluded.lease_json,
                    lineage_json = excluded.lineage_json,
                    last_error = excluded.last_error,
                    tenant_id = excluded.tenant_id,
                    created_by = excluded.created_by,
                    execution_json = excluded.execution_json
                WHERE sessions.execution_json = excluded.execution_json",
                    params![
                        record.runtime_id,
                        record.conversation_id,
                        record.sender_id,
                        status_to_str(record.status),
                        record.created_at_ms as i64,
                        record.updated_at_ms as i64,
                        workspace_json,
                        isolation_json,
                        capabilities_json,
                        runtime_json,
                        llm_json,
                        lease_json,
                        lineage_json,
                        record.last_error,
                        record.tenant_id,
                        record.created_by,
                        execution_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
            if changed != 1 {
                return Err("persisted execution route cannot be replaced".into());
            }
            tx.commit().map_err(|e| e.to_string())
        })
        .await
        .map_err(|error| internal_error(format!("sqlite save task panicked: {error}")))?;

        result.map_err(|error| internal_error(format!("sqlite save failed: {error}")))
    }

    async fn save_operation_intent(
        &self,
        id: &str,
        body: serde_json::Value,
    ) -> Result<(), SessionDomainError> {
        let conn = self.conn.clone();
        #[cfg(unix)]
        let deployment = self.deployment.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || { let mut conn = conn.lock().unwrap();
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate).map_err(|e| internal_error(e.to_string()))?;
            verify_writer(&tx, #[cfg(unix)] deployment.as_deref()).map_err(|e| internal_error(e.to_string()))?;
            tx.execute("INSERT INTO snapshot_operations VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET body=excluded.body",params![id,body.to_string()]).map_err(|e|internal_error(e.to_string()))?;
            tx.commit().map_err(|e|internal_error(e.to_string())) }).await.map_err(|e|internal_error(e.to_string()))?
    }
    async fn operation_intents(
        &self,
    ) -> Result<Vec<(String, serde_json::Value)>, SessionDomainError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let db = conn.lock().unwrap();
            let mut q = db
                .prepare("SELECT id,body FROM snapshot_operations ORDER BY id")
                .map_err(|e| internal_error(e.to_string()))?;
            let rows = q
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .map_err(|e| internal_error(e.to_string()))?;
            rows.map(|row| {
                let (id, body) = row.map_err(|e| internal_error(e.to_string()))?;
                Ok((
                    id,
                    serde_json::from_str(&body).map_err(|e| internal_error(e.to_string()))?,
                ))
            })
            .collect()
        })
        .await
        .map_err(|e| internal_error(e.to_string()))?
    }
    async fn delete_operation_intent(&self, id: &str) -> Result<(), SessionDomainError> {
        let conn = self.conn.clone();
        #[cfg(unix)]
        let deployment = self.deployment.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock().unwrap();
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(|e| internal_error(e.to_string()))?;
            verify_writer(
                &tx,
                #[cfg(unix)]
                deployment.as_deref(),
            )
            .map_err(|e| internal_error(e.to_string()))?;
            tx.execute("DELETE FROM snapshot_operations WHERE id=?1", [id])
                .map_err(|e| internal_error(e.to_string()))?;
            tx.commit().map_err(|e| internal_error(e.to_string()))
        })
        .await
        .map_err(|e| internal_error(e.to_string()))?
    }
    async fn save_checkpoint(
        &self,
        record: crate::CheckpointRecord,
    ) -> Result<(), SessionDomainError> {
        let conn = self.conn.clone();
        #[cfg(unix)]
        let deployment = self.deployment.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<(), String> {
            let runtime_json = json_encode(&record.runtime_state)?; let workspace_json = json_encode(&record.workspace)?; let isolation_json = json_encode(&record.isolation)?; let capabilities_json = json_encode(&record.capabilities)?;
            let mut conn = conn.lock().map_err(|_| "sqlite session repository lock poisoned".to_string())?;
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate).map_err(|e| e.to_string())?;
            verify_writer(&tx, #[cfg(unix)] deployment.as_deref()).map_err(|e| e.to_string())?;
            tx.execute("INSERT INTO checkpoints (checkpoint_id,source_runtime_id,provider_snapshot_id,runtime_json,workspace_json,isolation_json,capabilities_json,owner_ref,tenant_id,created_by,created_at_ms) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)", params![record.checkpoint_id,record.source_runtime_id,record.provider_snapshot_id,runtime_json,workspace_json,isolation_json,capabilities_json,record.owner_ref,record.tenant_id,record.created_by,record.created_at_ms as i64]).map_err(|e| e.to_string())?; tx.commit().map_err(|e| e.to_string())
        }).await.map_err(|e| internal_error(format!("sqlite checkpoint save task panicked: {e}")))?;
        result.map_err(|e| internal_error(format!("sqlite checkpoint save failed: {e}")))
    }

    async fn get_checkpoint(
        &self,
        checkpoint_id: &str,
    ) -> Result<Option<crate::CheckpointRecord>, SessionDomainError> {
        let conn = self.conn.clone();
        let checkpoint_id = checkpoint_id.to_string();
        let result = tokio::task::spawn_blocking(move || -> Result<Option<crate::CheckpointRecord>, String> {
            let conn = conn.lock().map_err(|_| "sqlite session repository lock poisoned".to_string())?;
            let mut stmt = conn.prepare("SELECT checkpoint_id,source_runtime_id,provider_snapshot_id,runtime_json,workspace_json,isolation_json,capabilities_json,owner_ref,tenant_id,created_by,created_at_ms FROM checkpoints WHERE checkpoint_id=?1 AND NOT EXISTS(SELECT 1 FROM snapshot_operations WHERE id='delete:'||checkpoint_id)").map_err(|e| e.to_string())?;
            let mut rows = stmt.query(params![checkpoint_id]).map_err(|e| e.to_string())?;
            let Some(row) = rows.next().map_err(|e| e.to_string())? else { return Ok(None); };
            let decode = |idx: usize| -> Result<String, String> { row.get(idx).map_err(|e| e.to_string()) };
            Ok(Some(crate::CheckpointRecord { checkpoint_id: decode(0)?, source_runtime_id: decode(1)?, provider_snapshot_id: decode(2)?, runtime_state: json_decode(&decode(3)?)?, workspace: json_decode(&decode(4)?)?, isolation: json_decode(&decode(5)?)?, capabilities: json_decode(&decode(6)?)?, owner_ref: decode(7)?, tenant_id: row.get(8).map_err(|e| e.to_string())?, created_by: row.get(9).map_err(|e| e.to_string())?, created_at_ms: row.get::<_, i64>(10).map_err(|e| e.to_string())? as u64 }))
        }).await.map_err(|e| internal_error(format!("sqlite checkpoint get task panicked: {e}")))?;
        result.map_err(|e| internal_error(format!("sqlite checkpoint get failed: {e}")))
    }

    async fn delete_checkpoint(&self, checkpoint_id: &str) -> Result<bool, SessionDomainError> {
        let conn = self.conn.clone();
        #[cfg(unix)]
        let deployment = self.deployment.clone();
        let checkpoint_id = checkpoint_id.to_string();
        let result = tokio::task::spawn_blocking(move || -> Result<bool, String> {
            let mut conn = conn
                .lock()
                .map_err(|_| "sqlite session repository lock poisoned".to_string())?;
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(|e| e.to_string())?;
            verify_writer(
                &tx,
                #[cfg(unix)]
                deployment.as_deref(),
            )
            .map_err(|e| e.to_string())?;
            let changed = tx
                .execute(
                    "DELETE FROM checkpoints WHERE checkpoint_id = ?1",
                    params![checkpoint_id],
                )
                .map_err(|error| error.to_string())?;
            tx.commit().map_err(|e| e.to_string())?;
            Ok(changed > 0)
        })
        .await
        .map_err(|error| {
            internal_error(format!("sqlite checkpoint delete task panicked: {error}"))
        })?;
        result.map_err(|error| internal_error(format!("sqlite checkpoint delete failed: {error}")))
    }

    async fn list_checkpoints(
        &self,
        tenant_id: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<CheckpointListPage, SessionDomainError> {
        let conn = self.conn.clone();
        let tenant_id = tenant_id.map(str::to_string);
        let limit = limit as i64;
        let offset = offset as i64;
        let result = tokio::task::spawn_blocking(move || -> Result<CheckpointListPage, String> {
            let conn = conn
                .lock()
                .map_err(|_| "sqlite session repository lock poisoned".to_string())?;
            let total: i64 = conn
                .query_row(
                    "SELECT count(*) FROM (SELECT checkpoint_id,tenant_id FROM checkpoints WHERE NOT EXISTS(SELECT 1 FROM snapshot_operations WHERE id='delete:'||checkpoint_id) UNION ALL SELECT checkpoint_id,tenant_id FROM joint_checkpoints) WHERE tenant_id=?1 OR ?1 IS NULL",
                    params![tenant_id],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())?;
            let mut stmt = conn
                .prepare(
                    "SELECT checkpoint_id, source_runtime_id, tenant_id, created_by, created_at_ms
                     FROM (SELECT checkpoint_id,source_runtime_id,tenant_id,created_by,created_at_ms FROM checkpoints
                       WHERE NOT EXISTS(SELECT 1 FROM snapshot_operations WHERE id='delete:'||checkpoint_id)
                       UNION ALL SELECT checkpoint_id,source_runtime_id,tenant_id,created_by,created_at_ms FROM joint_checkpoints)
                     WHERE tenant_id = ?1 OR ?1 IS NULL
                     ORDER BY created_at_ms DESC, checkpoint_id DESC
                     LIMIT ?2 OFFSET ?3",
                )
                .map_err(|error| error.to_string())?;
            let rows = stmt
                .query_map(params![tenant_id, limit, offset], |row| {
                    Ok(CheckpointListEntry {
                        checkpoint_id: row.get(0)?,
                        source_runtime_id: row.get(1)?,
                        tenant_id: row.get(2)?,
                        created_by: row.get(3)?,
                        created_at_ms: row.get::<_, i64>(4)? as u64,
                    })
                })
                .map_err(|error| error.to_string())?;
            let mut checkpoints = Vec::new();
            for row in rows {
                checkpoints.push(row.map_err(|error| error.to_string())?);
            }
            Ok(CheckpointListPage {
                checkpoints,
                total: total.max(0) as u64,
            })
        })
        .await
        .map_err(|error| {
            internal_error(format!("sqlite checkpoint list task panicked: {error}"))
        })?;
        result.map_err(|error| internal_error(format!("sqlite checkpoint list failed: {error}")))
    }

    async fn active_session_counts_by_tenant(
        &self,
    ) -> Result<HashMap<String, usize>, SessionDomainError> {
        let conn = self.conn.clone();
        let result =
            tokio::task::spawn_blocking(move || -> Result<HashMap<String, usize>, String> {
                let conn = conn
                    .lock()
                    .map_err(|_| "sqlite session repository lock poisoned".to_string())?;
                let mut stmt = conn
                    .prepare(
                        "SELECT tenant_id, COUNT(*) FROM (SELECT tenant_id,status FROM sessions UNION ALL SELECT tenant_id,status FROM joint_sessions) \
                 WHERE tenant_id IS NOT NULL AND status IN ('opening','idle','running','paused') \
                 GROUP BY tenant_id",
                    )
                    .map_err(|error| error.to_string())?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                    })
                    .map_err(|error| error.to_string())?;
                let mut counts = HashMap::new();
                for row in rows {
                    let (tenant_id, count) = row.map_err(|error| error.to_string())?;
                    counts.insert(tenant_id, count.max(0) as usize);
                }
                Ok(counts)
            })
            .await
            .map_err(|error| {
                internal_error(format!("sqlite quota restore task panicked: {error}"))
            })?;
        result.map_err(|error| internal_error(format!("sqlite quota restore failed: {error}")))
    }

    /// Two indexed queries (COUNT then SELECT...LIMIT) inside one
    /// `spawn_blocking` closure sharing the same locked connection — a read
    /// endpoint, so the extra round trip over a single combined query is a
    /// non-issue, and it keeps the "true total" and "capped page" logic each
    /// expressed as an ordinary SQL query rather than fetched-then-truncated
    /// in Rust (which would require pulling every matching row over just to
    /// count them).
    async fn list_active(
        &self,
        tenant_id: Option<&str>,
        limit: usize,
    ) -> Result<SessionListPage, SessionDomainError> {
        let conn = self.conn.clone();
        let tenant_id = tenant_id.map(str::to_string);
        let limit = limit as i64;
        let result = tokio::task::spawn_blocking(move || -> Result<SessionListPage, String> {
            const ACTIVE_STATUSES: &str = "status IN ('opening','idle','running','paused')";
            let conn = conn
                .lock()
                .map_err(|_| "sqlite session repository lock poisoned".to_string())?;

            let total_active: i64 = conn
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM (SELECT tenant_id,status FROM sessions UNION ALL SELECT tenant_id,status FROM joint_sessions) \
                         WHERE {ACTIVE_STATUSES} AND (tenant_id = ?1 OR ?1 IS NULL)"
                    ),
                    params![tenant_id],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())?;

            let mut stmt = conn
                .prepare(&format!(
                    "SELECT runtime_id FROM (SELECT runtime_id,tenant_id,status,updated_at_ms FROM sessions UNION ALL SELECT runtime_id,tenant_id,status,updated_at_ms FROM joint_sessions) \
                     WHERE {ACTIVE_STATUSES} AND (tenant_id = ?1 OR ?1 IS NULL) \
                     ORDER BY updated_at_ms DESC LIMIT ?2"
                ))
                .map_err(|error| error.to_string())?;
            let rows = stmt
                .query_map(params![tenant_id, limit], |r|r.get::<_,String>(0))
                .map_err(|error| error.to_string())?;
            let mut sessions = Vec::new();
            for row in rows {
                let id=row.map_err(|error|error.to_string())?;
                sessions.push(read_session(&conn,&id)?.ok_or("missing listed session")?);
            }

            Ok(SessionListPage {
                sessions,
                total_active: total_active.max(0) as u32,
            })
        })
        .await
        .map_err(|error| internal_error(format!("sqlite list_active task panicked: {error}")))?;

        result.map_err(|error| internal_error(format!("sqlite list_active failed: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{SandboxCapability, WorkspaceAccess};
    use std::collections::BTreeSet;

    fn sample_record(runtime_id: &str) -> SessionRecord {
        SessionRecord {
            execution_backing: Default::default(),
            runtime_id: runtime_id.to_string(),
            conversation_id: "conversation-1".to_string(),
            sender_id: "sender-1".to_string(),
            status: SessionStatus::Idle,
            created_at_ms: 1,
            updated_at_ms: 2,
            workspace: WorkspaceFacts {
                workspace_id: "workspace-1".to_string(),
                root: "/tmp/workspace-1".to_string(),
                access: WorkspaceAccess::ReadWrite,
                revision: Some("main".to_string()),
                metadata: serde_json::json!({"k": "v"}),
            },
            isolation: IsolationFacts {
                boundary: crate::domain::IsolationBoundary::Container,
                workspace_access: WorkspaceAccess::ReadWrite,
                network: crate::domain::NetworkIsolation::Restricted,
                metadata: serde_json::Value::Null,
            },
            capabilities: EffectiveCapabilities {
                sandbox: BTreeSet::from([SandboxCapability::Exec]),
                runtime: BTreeSet::new(),
            },
            runtime: OpaqueRuntimeState {
                runtime_kind: "local-mock".to_string(),
                schema_version: 1,
                state: serde_json::json!({"pid": 123}),
            },
            llm: Some(ResolvedLlm {
                provider: "anthropic".to_string(),
                model: "claude".to_string(),
                api_base: None,
                api: None,
                capabilities: None,
                credential_source: "env".to_string(),
            }),
            lease: Some(SessionLease {
                client_id: "client-1".to_string(),
                client_pid: Some(42),
                client_hostname: Some("host-1".to_string()),
                expires_at_ms: 999,
            }),
            lineage: Some(CheckpointLineage {
                parent_runtime_id: Some("runtime-0".to_string()),
                source_checkpoint_id: None,
            }),
            last_error: None,
            tenant_id: Some("tenant-1".to_string()),
            created_by: "principal-1".to_string(),
        }
    }

    #[tokio::test]
    async fn save_then_get_round_trips_the_full_record() {
        let repo = SqliteSessionRepository::open_in_memory().expect("open in-memory db");
        let record = sample_record("runtime-1");
        repo.save(record.clone()).await.expect("save must succeed");

        let loaded = repo
            .get("runtime-1")
            .await
            .expect("get must succeed")
            .expect("record must exist");
        assert_eq!(loaded, record);
    }

    #[tokio::test]
    async fn get_on_unknown_runtime_id_returns_none() {
        let repo = SqliteSessionRepository::open_in_memory().expect("open in-memory db");
        let loaded = repo.get("does-not-exist").await.expect("get must succeed");
        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn snapshot_intents_survive_reopen_and_hide_deleting_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.sqlite");
        let repo = SqliteSessionRepository::open(&path).unwrap();
        let session = sample_record("source");
        let cp = crate::CheckpointRecord {
            checkpoint_id: "cp".into(),
            source_runtime_id: "source".into(),
            provider_snapshot_id: "snap".into(),
            runtime_state: session.runtime,
            workspace: session.workspace,
            isolation: session.isolation,
            capabilities: session.capabilities,
            owner_ref: "admin".into(),
            tenant_id: None,
            created_by: None,
            created_at_ms: 1,
        };
        repo.save_checkpoint(cp).await.unwrap();
        repo.save_operation_intent(
            "delete:cp",
            serde_json::json!({"kind":"delete","checkpoint":"cp"}),
        )
        .await
        .unwrap();
        drop(repo);
        let repo = SqliteSessionRepository::open(&path).unwrap();
        assert_eq!(repo.operation_intents().await.unwrap().len(), 1);
        assert!(repo.get_checkpoint("cp").await.unwrap().is_none());
        assert_eq!(repo.list_checkpoints(None, 200, 0).await.unwrap().total, 0);
        assert!(repo.delete_checkpoint("cp").await.unwrap());
        repo.delete_operation_intent("delete:cp").await.unwrap();
        assert!(repo.operation_intents().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn checkpoint_round_trips() {
        let repo = SqliteSessionRepository::open_in_memory().unwrap();
        let session = sample_record("runtime-source");
        let checkpoint = crate::CheckpointRecord {
            checkpoint_id: "checkpoint-1".into(),
            source_runtime_id: session.runtime_id.clone(),
            provider_snapshot_id: "snapshot-1".into(),
            runtime_state: session.runtime.clone(),
            workspace: session.workspace.clone(),
            isolation: session.isolation.clone(),
            capabilities: session.capabilities.clone(),
            owner_ref: "tenant/tenant-1".into(),
            tenant_id: session.tenant_id.clone(),
            created_by: Some(session.created_by.clone()),
            created_at_ms: 42,
        };
        repo.save_checkpoint(checkpoint.clone()).await.unwrap();
        assert_eq!(
            repo.get_checkpoint("checkpoint-1").await.unwrap(),
            Some(checkpoint)
        );
        assert!(repo.delete_checkpoint("checkpoint-1").await.unwrap());
        assert_eq!(repo.get_checkpoint("checkpoint-1").await.unwrap(), None);
        assert!(!repo.delete_checkpoint("checkpoint-1").await.unwrap());
    }

    #[tokio::test]
    async fn checkpoint_list_is_tenant_scoped_and_paginated() {
        let repo = SqliteSessionRepository::open_in_memory().unwrap();
        let mut first = sample_record("runtime-a");
        first.tenant_id = Some("tenant-a".into());
        let mut second = sample_record("runtime-b");
        second.tenant_id = Some("tenant-b".into());
        for (record, id, created_at) in
            [(&first, "checkpoint-a", 20), (&second, "checkpoint-b", 10)]
        {
            repo.save_checkpoint(crate::CheckpointRecord {
                checkpoint_id: id.into(),
                source_runtime_id: record.runtime_id.clone(),
                provider_snapshot_id: format!("snapshot-{id}"),
                runtime_state: record.runtime.clone(),
                workspace: record.workspace.clone(),
                isolation: record.isolation.clone(),
                capabilities: record.capabilities.clone(),
                owner_ref: format!("tenant/{}", record.tenant_id.as_deref().unwrap()),
                tenant_id: record.tenant_id.clone(),
                created_by: Some("test".into()),
                created_at_ms: created_at,
            })
            .await
            .unwrap();
        }
        let tenant_page = repo.list_checkpoints(Some("tenant-a"), 1, 0).await.unwrap();
        assert_eq!(tenant_page.total, 1);
        assert_eq!(tenant_page.checkpoints[0].checkpoint_id, "checkpoint-a");

        let admin_page = repo.list_checkpoints(None, 1, 0).await.unwrap();
        assert_eq!(admin_page.total, 2);
        assert_eq!(admin_page.checkpoints[0].checkpoint_id, "checkpoint-a");
        let next_page = repo.list_checkpoints(None, 1, 1).await.unwrap();
        assert_eq!(next_page.checkpoints[0].checkpoint_id, "checkpoint-b");
    }

    #[tokio::test]
    async fn save_twice_upserts_rather_than_conflicting() {
        let repo = SqliteSessionRepository::open_in_memory().expect("open in-memory db");
        let mut record = sample_record("runtime-1");
        repo.save(record.clone())
            .await
            .expect("first save must succeed");

        record.status = SessionStatus::Closed;
        record.updated_at_ms = 3;
        repo.save(record.clone())
            .await
            .expect("second save must succeed");

        let loaded = repo
            .get("runtime-1")
            .await
            .expect("get must succeed")
            .expect("record must exist");
        assert_eq!(loaded.status, SessionStatus::Closed);
        assert_eq!(loaded.updated_at_ms, 3);
    }

    #[tokio::test]
    async fn survives_reopening_the_same_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("xgovernor.db");

        {
            let repo = SqliteSessionRepository::open(&path).expect("open must succeed");
            repo.save(sample_record("runtime-1"))
                .await
                .expect("save must succeed");
        }

        let repo = SqliteSessionRepository::open(&path).expect("reopen must succeed");
        let loaded = repo
            .get("runtime-1")
            .await
            .expect("get must succeed")
            .expect("record must have survived reopening the file");
        assert_eq!(loaded.runtime_id, "runtime-1");
    }

    #[tokio::test]
    async fn active_session_counts_by_tenant_ignore_closed_failed_and_admin_rows() {
        let repo = SqliteSessionRepository::open_in_memory().unwrap();
        let mut active = sample_record("active");
        active.tenant_id = Some("tenant-a".into());
        repo.save(active).await.unwrap();
        let mut paused = sample_record("paused");
        paused.status = SessionStatus::Paused;
        paused.tenant_id = Some("tenant-a".into());
        repo.save(paused).await.unwrap();
        let mut closed = sample_record("closed");
        closed.status = SessionStatus::Closed;
        closed.tenant_id = Some("tenant-a".into());
        repo.save(closed).await.unwrap();
        let mut failed = sample_record("failed");
        failed.status = SessionStatus::Failed;
        failed.tenant_id = Some("tenant-a".into());
        repo.save(failed).await.unwrap();
        let admin = sample_record("admin");
        repo.save(admin).await.unwrap();

        let counts = repo.active_session_counts_by_tenant().await.unwrap();
        assert_eq!(counts.get("tenant-a"), Some(&2));
        assert!(!counts.contains_key("admin"));
    }

    #[tokio::test]
    async fn legacy_database_migrates_without_changing_session_identity() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sessions.sqlite");
        let record = sample_record("legacy-session");
        let repo = SqliteSessionRepository::open(&path).unwrap();
        repo.save(record.clone()).await.unwrap();
        drop(repo);
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("ALTER TABLE sessions DROP COLUMN execution_json;")
            .unwrap();
        drop(conn);
        let reopened = SqliteSessionRepository::open(&path).unwrap();
        assert_eq!(reopened.get("legacy-session").await.unwrap(), Some(record));
    }

    #[tokio::test]
    async fn unknown_execution_backing_is_rejected_before_legacy_read() {
        let repo = SqliteSessionRepository::open_in_memory().unwrap();
        repo.save(sample_record("unknown-backing")).await.unwrap();
        repo.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE sessions SET execution_json=?1 WHERE runtime_id='unknown-backing'",
                [r#"{"kind":"joint_v99"}"#],
            )
            .unwrap();
        assert!(repo.get("unknown-backing").await.is_err());
        assert!(repo.save(sample_record("unknown-backing")).await.is_err());
        let binding = crate::SessionExecutionBacking::JointV1 {
            binding: crate::JointBinding {
                owner_kind: "joint-test".into(),
                binding_id: "binding-1".into(),
                configuration_revision: "revision-1".into(),
            },
        };
        repo.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE sessions SET execution_json=?1 WHERE runtime_id='unknown-backing'",
                [serde_json::to_string(&binding).unwrap()],
            )
            .unwrap();
        assert!(repo.save(sample_record("unknown-backing")).await.is_err());
        assert_eq!(
            repo.get("unknown-backing")
                .await
                .unwrap()
                .unwrap()
                .execution_backing,
            binding
        );
    }
}
