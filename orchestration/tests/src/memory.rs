//! Test-only backend for exercising the compatibility assertions.
use base64::Engine;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use orchestration_contract::execution::*;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

type Files = BTreeMap<String, String>;

#[derive(Default)]
struct Memory {
    states: BTreeMap<String, Files>,
    branches: BTreeMap<String, Files>,
    jobs: BTreeMap<String, JobState>,
    receipts: BTreeMap<(String, String), (Value, Value)>,
    next: u64,
    lease: Option<Instant>,
    reclaimed: bool,
    lose_fork_response: bool,
}

impl Memory {
    fn reclaim(&mut self) {
        for state in self.jobs.values_mut() {
            if matches!(state, JobState::Accepted | JobState::Running) {
                *state = JobState::Failed(JobFailed {
                    failure: Failure {
                        code: "cancelled".into(),
                        message: "run reclaimed".into(),
                    },
                    usage: BTreeMap::new(),
                });
            }
        }
        self.branches.clear();
        self.states.clear();
        self.reclaimed = true;
    }

    fn id(&mut self, prefix: &str) -> String {
        self.next += 1;
        format!("{prefix}-{}", self.next)
    }

    fn branch(&self, branch: &str) -> BackendResult<&Files> {
        self.branches
            .get(branch)
            .ok_or_else(|| rejected(ErrorCode::BranchNotFound))
    }

    fn mutation<T: Serialize, R: Serialize + DeserializeOwned>(
        &mut self,
        method: &str,
        context: &MutationContext,
        request: &T,
        execute: impl FnOnce(&mut Self) -> BackendResult<R>,
    ) -> BackendResult<R> {
        let key = (method.into(), context.idempotency_key.clone());
        let params = serde_json::to_value(request).unwrap();
        if let Some((original, result)) = self.receipts.get(&key) {
            if original != &params {
                return Err(rejected(ErrorCode::IdempotencyConflict));
            }
            return Ok(serde_json::from_value(result.clone()).unwrap());
        }
        let result = execute(self)?;
        self.receipts
            .insert(key, (params, serde_json::to_value(&result).unwrap()));
        Ok(result)
    }
}

fn rejected(code: ErrorCode) -> BackendError {
    BackendError::Rejected {
        code,
        message: "memory fixture rejection".into(),
    }
}

/// In-memory fixture only; never use as a production execution backend.
#[derive(Clone)]
pub struct MemoryBackend(Arc<Mutex<Memory>>);

impl MemoryBackend {
    pub fn new(lose_fork_response: bool) -> Self {
        Self(Arc::new(Mutex::new(Memory {
            states: [("root".into(), Files::new())].into_iter().collect(),
            lose_fork_response,
            ..Memory::default()
        })))
    }

    pub fn assert_no_branches(&self) {
        assert!(self.0.lock().unwrap().branches.is_empty());
    }
}

#[async_trait]
impl ExecutionBackend for MemoryBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            capabilities: [
                Capability::PrepareRoot,
                Capability::RenewRunLease,
                Capability::ReclaimRun,
                Capability::ExportWorkspace,
                Capability::Snapshot,
                Capability::Fork,
                Capability::AgentTurn,
                Capability::CancelJob,
                Capability::Inspect,
                Capability::Exec,
                Capability::FileRead,
                Capability::FileWrite,
                Capability::Diff,
                Capability::Release,
            ]
            .into_iter()
            .collect(),
            network_policies: [NetworkPolicy::Offline].into_iter().collect(),
            isolation: Isolation::None,
            joint_snapshots: false,
            independent_branches: true,
            can_reset_history: true,
        }
    }

    async fn prepare_root(&self, r: PrepareRootRequest) -> BackendResult<Snapshot> {
        self.0
            .lock()
            .unwrap()
            .mutation("host.prepare_root", &r.context, &r, |m| {
                let files = match &r.root.workspace {
                    WorkspaceSource::Snapshot { state_id } => m
                        .states
                        .get(state_id)
                        .ok_or_else(|| rejected(ErrorCode::StateExpired))?
                        .clone(),
                    WorkspaceSource::Git { .. } | WorkspaceSource::Package { .. } => {
                        return Err(rejected(ErrorCode::Unsupported))
                    }
                };
                let state_id = m.id("root");
                m.states.insert(state_id.clone(), files);
                Ok(Snapshot {
                    state_id,
                    expires_at: "2099-01-01T00:00:00Z".into(),
                })
            })
    }

    async fn renew_run_lease(&self, r: RenewRunLeaseRequest) -> BackendResult<RunLease> {
        let mut m = self.0.lock().unwrap();
        let result = m.mutation("host.renew_run_lease", &r.context, &r, |m| {
            if r.ttl_ms == 0 || m.reclaimed {
                return Err(rejected(ErrorCode::InvalidParams));
            }
            m.lease = Some(Instant::now() + Duration::from_millis(r.ttl_ms));
            // The fixture reports a stable RFC3339 value; expiry is driven by
            // its monotonic clock, independently of subsequent host requests.
            Ok(RunLease {
                expires_at: "2099-01-01T00:00:00Z".into(),
            })
        })?;
        let memory = self.0.clone();
        let deadline = m.lease;
        std::thread::spawn(move || {
            if let Some(deadline) = deadline {
                std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
                let mut m = memory.lock().unwrap();
                if m.lease == Some(deadline) {
                    m.reclaim();
                }
            }
        });
        Ok(result)
    }

    async fn reclaim_run(&self, r: ReclaimRunRequest) -> BackendResult<RunReclaimed> {
        self.0
            .lock()
            .unwrap()
            .mutation("host.reclaim_run", &r.context, &r, |m| {
                m.reclaim();
                Ok(RunReclaimed {
                    status: ReclaimStatus::Complete,
                    remaining: vec![],
                    message: "reclaimed".into(),
                })
            })
    }

    async fn export_workspace(
        &self,
        r: ExportWorkspaceRequest,
    ) -> BackendResult<WorkspaceExported> {
        self.0
            .lock()
            .unwrap()
            .mutation("host.export_workspace", &r.context, &r, |m| {
                let files = match &r.source {
                    WorkspaceRef::Snapshot(id) => m
                        .states
                        .get(id)
                        .ok_or_else(|| rejected(ErrorCode::StateExpired))?,
                    WorkspaceRef::Branch(id) => m.branch(id)?,
                };
                let directory = std::fs::canonicalize(&r.run_directory)
                    .map_err(|_| rejected(ErrorCode::InvalidParams))?;
                let path = std::path::Path::new(&r.path);
                if path
                    .components()
                    .any(|c| !matches!(c, std::path::Component::Normal(_)))
                    || path.components().count() != 1
                {
                    return Err(rejected(ErrorCode::InvalidParams));
                }
                let mut archive = tar::Builder::new(Vec::new());
                let mut file_count = 0;
                for (path, value) in files {
                    if !r.include_vcs_metadata && path.split('/').any(|part| part == ".git") {
                        continue;
                    }
                    let data = base64::engine::general_purpose::STANDARD
                        .decode(value)
                        .map_err(|_| rejected(ErrorCode::InvalidParams))?;
                    let mut header = tar::Header::new_gnu();
                    header.set_size(data.len() as u64);
                    header.set_mode(0o600);
                    header.set_cksum();
                    archive
                        .append_data(&mut header, path, data.as_slice())
                        .map_err(|_| rejected(ErrorCode::InvalidParams))?;
                    file_count += 1;
                }
                let data = archive
                    .into_inner()
                    .map_err(|_| rejected(ErrorCode::Internal))?;
                if data.len() as u64 > r.max_bytes {
                    return Err(rejected(ErrorCode::ExportTooLarge));
                }
                use std::io::Write;
                let temporary = directory.join(format!(".export-{}", r.context.idempotency_key));
                if r.context.idempotency_key.contains('/') {
                    return Err(rejected(ErrorCode::InvalidParams));
                }
                let publish = || -> std::io::Result<()> {
                    let mut output = std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&temporary)?;
                    output.write_all(&data)?;
                    output.sync_all()?;
                    std::fs::hard_link(&temporary, directory.join(path))?;
                    Ok(())
                };
                let outcome = publish();
                if temporary.exists() {
                    let _ = std::fs::remove_file(temporary);
                }
                outcome.map_err(|_| rejected(ErrorCode::Internal))?;
                Ok(WorkspaceExported {
                    size_bytes: data.len() as u64,
                    file_count,
                    sha256: format!("{:x}", Sha256::digest(&data)),
                })
            })
    }

    async fn snapshot(&self, r: SnapshotRequest) -> BackendResult<Snapshot> {
        self.0
            .lock()
            .unwrap()
            .mutation("host.snapshot", &r.context, &r, |m| {
                let files = m.branch(&r.branch_id)?.clone();
                let state_id = m.id("state");
                m.states.insert(state_id.clone(), files);
                Ok(Snapshot {
                    state_id,
                    expires_at: "2099-01-01T00:00:00Z".into(),
                })
            })
    }

    async fn fork(&self, r: ForkRequest) -> BackendResult<Branch> {
        let mut m = self.0.lock().unwrap();
        let result = m.mutation("host.fork", &r.context, &r, |m| {
            if r.network_policy != NetworkPolicy::Offline {
                return Err(rejected(ErrorCode::Unsupported));
            }
            let files = m
                .states
                .get(&r.parent_state_id)
                .ok_or_else(|| rejected(ErrorCode::StateExpired))?
                .clone();
            let branch_id = m.id("branch");
            m.branches.insert(branch_id.clone(), files);
            Ok(Branch { branch_id })
        })?;
        if m.lose_fork_response {
            m.lose_fork_response = false;
            return Err(BackendError::OutcomeUnknown {
                operation: OperationIdentity {
                    run_id: r.context.call.run_id,
                    method: "host.fork".into(),
                    idempotency_key: r.context.idempotency_key,
                },
                message: "injected response loss after fork".into(),
            });
        }
        Ok(result)
    }

    async fn agent_turn(&self, r: AgentTurnRequest) -> BackendResult<JobReceipt> {
        self.0
            .lock()
            .unwrap()
            .mutation("host.agent_turn", &r.context, &r, |m| {
                m.branch(&r.branch_id)?;
                let job_id = m.id("job");
                m.jobs.insert(
                    job_id.clone(),
                    JobState::Completed(JobCompleted {
                        output: "memory Agent output".into(),
                        usage: BTreeMap::new(),
                        exit_code: None,
                        truncated: false,
                    }),
                );
                Ok(JobReceipt {
                    job_id,
                    status: Accepted::Accepted,
                })
            })
    }

    async fn cancel_job(&self, r: CancelJobRequest) -> BackendResult<CancelJobResponse> {
        self.0
            .lock()
            .unwrap()
            .mutation("host.cancel_job", &r.context, &r, |m| {
                let job = m
                    .jobs
                    .get_mut(&r.job_id)
                    .ok_or_else(|| rejected(ErrorCode::JobNotFound))?;
                if matches!(job, JobState::Accepted | JobState::Running) {
                    *job = JobState::Failed(JobFailed {
                        failure: Failure {
                            code: "cancelled".into(),
                            message: r.reason.clone(),
                        },
                        usage: BTreeMap::new(),
                    });
                }
                Ok(CancelJobResponse {
                    job_id: r.job_id.clone(),
                    status: if matches!(job, JobState::Completed(_)) {
                        CancelJobStatus::Completed
                    } else {
                        CancelJobStatus::Stopped
                    },
                })
            })
    }

    async fn inspect(&self, r: InspectRequest) -> BackendResult<Content> {
        let m = self.0.lock().unwrap();
        Ok(Content {
            content: format!("{} files", m.branch(&r.branch_id)?.len()),
        })
    }

    async fn exec(&self, r: ExecRequest) -> BackendResult<JobReceipt> {
        self.0
            .lock()
            .unwrap()
            .mutation("host.exec", &r.context, &r, |m| {
                m.branch(&r.branch_id)?;
                let job_id = m.id("job");
                let state = if r.argv == ["blocking"] {
                    JobState::Running
                } else {
                    JobState::Completed(JobCompleted {
                        output: "compat-exec".into(),
                        usage: BTreeMap::new(),
                        exit_code: Some(7),
                        truncated: false,
                    })
                };
                m.jobs.insert(job_id.clone(), state);
                Ok(JobReceipt {
                    job_id,
                    status: Accepted::Accepted,
                })
            })
    }

    async fn file_read(&self, r: FileReadRequest) -> BackendResult<FileContent> {
        let m = self.0.lock().unwrap();
        Ok(FileContent {
            content_base64: m
                .branch(&r.branch_id)?
                .get(&r.path)
                .ok_or_else(|| rejected(ErrorCode::InvalidParams))?
                .clone(),
        })
    }

    async fn file_write(&self, r: FileWriteRequest) -> BackendResult<FileWritten> {
        self.0
            .lock()
            .unwrap()
            .mutation("host.file_write", &r.context, &r, |m| {
                m.branch(&r.branch_id)?;
                m.branches
                    .get_mut(&r.branch_id)
                    .unwrap()
                    .insert(r.path.clone(), r.content_base64.clone());
                // Fixtures use exactly three-byte values, without base64 padding.
                Ok(FileWritten { written_bytes: 3 })
            })
    }

    async fn diff(&self, r: DiffRequest) -> BackendResult<Content> {
        let m = self.0.lock().unwrap();
        let base = m
            .states
            .get(&r.base_state_id)
            .ok_or_else(|| rejected(ErrorCode::StateExpired))?;
        let mut content = String::new();
        for (path, value) in m.branch(&r.branch_id)? {
            if base.get(path) != Some(value) {
                content.push_str(&format!("--- a/{path}\n+++ b/{path}\n+one\n"));
            }
        }
        Ok(Content { content })
    }

    async fn release(&self, r: ReleaseRequest) -> BackendResult<Released> {
        self.0
            .lock()
            .unwrap()
            .mutation("host.release", &r.context, &r, |m| {
                m.branches.remove(&r.branch_id);
                Ok(Released { released: true })
            })
    }

    async fn wait_job(&self, _: CallContext, job_id: &str) -> BackendResult<JobState> {
        self.0
            .lock()
            .unwrap()
            .jobs
            .get(job_id)
            .cloned()
            .ok_or_else(|| rejected(ErrorCode::JobNotFound))
    }

    async fn reconcile(
        &self,
        _: CallContext,
        operation: OperationIdentity,
    ) -> BackendResult<OperationResolution> {
        let m = self.0.lock().unwrap();
        match m
            .receipts
            .get(&(operation.method, operation.idempotency_key))
        {
            Some((_, result)) => Ok(OperationResolution::Succeeded(MutationResult::Fork(
                serde_json::from_value(result.clone()).unwrap(),
            ))),
            None => Ok(OperationResolution::NotStarted),
        }
    }
}
