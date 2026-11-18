//! E2B execution backend using the existing joint-checkpoint coordinator.
mod host;
pub mod runtime;
mod store;

use async_trait::async_trait;
use backend::execution::{OperationExecutionExt, OPERATION_CONTEXT};
use orchestration_contract::execution::*;
use serde::{Deserialize, Serialize};
use session_protocol as session;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, RwLock};
use uuid::Uuid;
use xgovernor_core::{SecurityContext, SessionApplication, SessionRepository};
use xgovernor_manager::InstanceManager;

pub(crate) fn error(code: ErrorCode, message: impl Into<String>) -> BackendError {
    BackendError::Rejected {
        code,
        message: message.into(),
    }
}
pub(crate) fn internal(e: impl std::fmt::Display) -> BackendError {
    error(ErrorCode::Internal, e.to_string())
}
enum MutationFailure {
    Definite(BackendError),
    Uncertain(BackendError),
}
impl From<BackendError> for MutationFailure {
    fn from(error: BackendError) -> Self {
        match error {
            BackendError::Rejected {
                code: ErrorCode::Internal,
                ..
            } => Self::Uncertain(error),
            _ => Self::Definite(error),
        }
    }
}
impl MutationFailure {
    fn project(self, operation: &OperationIdentity) -> BackendError {
        match self {
            Self::Definite(error) => error,
            Self::Uncertain(BackendError::Rejected { message, .. }) => {
                BackendError::OutcomeUnknown {
                    operation: operation.clone(),
                    message,
                }
            }
            Self::Uncertain(error) => error,
        }
    }
}

fn classify_restore_error(
    error: xgovernor_core::SessionDomainError,
    identity_known: bool,
) -> MutationFailure {
    use xgovernor_core::SessionDomainError as E;
    let code = match &error {
        E::InvalidRequest { .. } => ErrorCode::InvalidParams,
        E::NotFound { .. } => ErrorCode::BranchNotFound,
        E::Conflict { .. } | E::LeaseRequired { .. } | E::LeaseConflict { .. } => {
            ErrorCode::BranchBusy
        }
        E::UnsupportedCapability { .. } => ErrorCode::Unsupported,
        E::Timeout { .. } => ErrorCode::DeadlineExceeded,
        E::QuotaExceeded { .. } => ErrorCode::BudgetExhausted,
        E::Unavailable { .. } | E::Internal { .. } => ErrorCode::Internal,
    };
    let uncertain = matches!(error, E::Timeout { .. }) || !identity_known;
    let error = crate::error(code, error.to_string());
    if uncertain {
        MutationFailure::Uncertain(error)
    } else {
        MutationFailure::Definite(error)
    }
}

fn unknown(op: &OperationIdentity) -> BackendError {
    BackendError::OutcomeUnknown {
        operation: op.clone(),
        message: "mutation is unresolved; reconcile this identity".into(),
    }
}
fn id() -> String {
    Uuid::new_v4().to_string()
}

#[derive(Clone, Serialize, Deserialize)]
struct BranchRecord {
    released: bool,
    output: String,
    #[serde(default)]
    root: bool,
}
#[derive(Clone, Serialize, Deserialize)]
struct StateRecord {
    checkpoint: String,
    expires_at: String,
}
#[derive(Clone, Serialize, Deserialize)]
struct JobRecord {
    branch: String,
    agent: bool,
    turn_id: String,
    state: JobState,
    #[serde(default)]
    cancelling: bool,
}

/// One backend per persisted run. The owner, grants, network policy and model
/// bootstrap are trusted inputs from run admission, never from algorithm args.
#[derive(Clone)]
pub struct OwnBackend(Arc<Inner>);
struct Inner {
    app: Arc<SessionApplication>,
    manager: Arc<InstanceManager>,
    repository: Arc<dyn SessionRepository>,
    owner: SecurityContext,
    context: CallContext,
    grants: BTreeSet<Capability>,
    policy: NetworkPolicy,
    llm: Option<session::LlmOverrideRequest>,
    bootstrap: session::SessionExtensions,
    store: store::Store,
    locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    lease: session::SessionLeaseClaim,
    run_gate: RwLock<()>,
    reclaim_gate: AsyncMutex<()>,
}

impl OwnBackend {
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        path: &Path,
        app: Arc<SessionApplication>,
        manager: Arc<InstanceManager>,
        repository: Arc<dyn SessionRepository>,
        owner: SecurityContext,
        context: CallContext,
        grants: BTreeSet<Capability>,
        policy: NetworkPolicy,
        llm: Option<session::LlmOverrideRequest>,
        bootstrap: session::SessionExtensions,
        lease: session::SessionLeaseClaim,
    ) -> BackendResult<Self> {
        if matches!(
            std::env::var("XGOVERNOR_E2B_LENIENT_CLEANUP").as_deref(),
            Ok("1") | Ok("true")
        ) {
            return Err(error(
                ErrorCode::Unsupported,
                "strict E2B cleanup is required",
            ));
        }
        if lease.client_id.as_deref().is_none_or(str::is_empty) {
            return Err(error(
                ErrorCode::InvalidParams,
                "run lease client identity is required",
            ));
        }
        if llm.as_ref().is_some_and(|llm| llm.api_key.is_some()) {
            return Err(error(
                ErrorCode::InvalidParams,
                "use host environment model credentials",
            ));
        }
        if context.run_id.is_empty() || context.plugin_id.is_empty() {
            return Err(error(
                ErrorCode::InvalidParams,
                "run and plugin identities are required",
            ));
        }
        let store = store::Store::open(path)?;
        let admission = serde_json::json!({"run":context.run_id,"plugin":context.plugin_id,"grants":grants,"network_policy":policy,"owner":owner.owner_ref()});
        if let Some(old) = store.get::<serde_json::Value>("admission", "run")? {
            if old != admission {
                return Err(error(
                    ErrorCode::CapabilityDenied,
                    "persisted run admission differs",
                ));
            }
        } else {
            store.put("admission", "run", &admission)?;
        }
        let backend = Self(Arc::new(Inner {
            app,
            manager,
            repository,
            owner,
            context,
            grants,
            policy,
            llm,
            bootstrap,
            store,
            locks: Mutex::new(HashMap::new()),
            lease,
            run_gate: RwLock::new(()),
            reclaim_gate: AsyncMutex::new(()),
        }));
        backend.start_reaper();
        Ok(backend)
    }
    fn check(&self, c: &CallContext, cap: Option<Capability>) -> BackendResult<()> {
        if c.run_id != self.0.context.run_id || c.plugin_id != self.0.context.plugin_id {
            return Err(error(
                ErrorCode::RunNotFound,
                "run is not owned by this caller",
            ));
        }
        if c.timeout_ms == 0 || c.timeout_ms > 3_600_000 {
            return Err(error(
                ErrorCode::InvalidParams,
                "timeout_ms must be between 1 and 3600000",
            ));
        }
        if cap.is_some_and(|cap| !self.0.grants.contains(&cap)) {
            return Err(error(ErrorCode::CapabilityDenied, "capability not granted"));
        }
        Ok(())
    }
    fn lock(&self, branch: &str) -> BackendResult<OwnedMutexGuard<()>> {
        if self
            .0
            .store
            .list::<JobRecord>("job")?
            .iter()
            .any(|(_, job)| {
                job.branch == branch
                    && !matches!(job.state, JobState::Completed(_) | JobState::Failed(_))
            })
        {
            return Err(error(ErrorCode::BranchBusy, "branch has an unresolved job"));
        }
        self.0
            .locks
            .lock()
            .unwrap()
            .entry(branch.into())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
            .try_lock_owned()
            .map_err(|_| error(ErrorCode::BranchBusy, "branch has an active mutation"))
    }
    fn branch(&self, branch: &str) -> BackendResult<BranchRecord> {
        if !self.run_live()? {
            return Err(error(
                ErrorCode::BranchNotFound,
                "run resources are reclaiming",
            ));
        }
        self.0
            .store
            .get::<BranchRecord>("branch", branch)?
            .filter(|b| !b.released && !b.root)
            .ok_or_else(|| error(ErrorCode::BranchNotFound, "branch not found"))
    }
    fn state(&self, state: &str) -> BackendResult<StateRecord> {
        let s = self
            .0
            .store
            .get::<StateRecord>("state", state)?
            .ok_or_else(|| error(ErrorCode::StateExpired, "snapshot not found"))?;
        if chrono::DateTime::parse_from_rfc3339(&s.expires_at).map_err(internal)?
            <= chrono::Utc::now()
        {
            return Err(error(ErrorCode::StateExpired, "snapshot expired"));
        }
        Ok(s)
    }
    async fn read<T>(
        &self,
        c: &CallContext,
        f: impl Future<Output = BackendResult<T>>,
    ) -> BackendResult<T> {
        tokio::time::timeout(Duration::from_millis(c.timeout_ms), f)
            .await
            .map_err(|_| error(ErrorCode::DeadlineExceeded, "read deadline expired"))?
    }
    async fn mutation<
        R: Serialize + Send,
        F: Future<Output = Result<MutationResult, MutationFailure>> + Send + 'static,
    >(
        &self,
        c: &MutationContext,
        method: &str,
        request: &R,
        f: F,
    ) -> BackendResult<MutationResult> {
        let op = OperationIdentity {
            run_id: c.call.run_id.clone(),
            method: method.into(),
            idempotency_key: c.idempotency_key.clone(),
        };
        if let Some(result) = self.0.store.begin(
            method,
            &c.idempotency_key,
            serde_json::to_value(request).map_err(internal)?,
        )? {
            return match self.0.store.resolution(&op)? {
                OperationResolution::Unknown => Err(unknown(&op)),
                _ => result,
            };
        }
        let backend = self.clone();
        let identity = op.clone();
        let method = method.to_string();
        let task = tokio::spawn(async move {
            let _gate = if method == "host.reclaim_run" {
                None
            } else {
                Some(backend.0.run_gate.read().await)
            };
            let admitted = matches!(method.as_str(), "host.renew_run_lease" | "host.reclaim_run")
                .then_some(Ok(()))
                .unwrap_or_else(|| backend.require_live());
            let result = async {
                admitted?;
                f.await
            }
            .await
            .map_err(|error: MutationFailure| error.project(&identity));
            backend.0.store.finish(&identity, &result)?;
            result
        });
        // Timeout drops only the join handle: the supervised mutation continues,
        // retaining its branch guard and original durable operation identity.
        match tokio::time::timeout(Duration::from_millis(c.call.timeout_ms), task).await {
            Ok(Ok(result)) => result,
            _ => Err(unknown(&op)),
        }
    }
    /// Register an already-open, run-owned E2B execution instance as the root.
    /// This is an admission/bootstrap action, outside the algorithm interface.
    pub async fn register_root(&self, runtime_id: &str) -> BackendResult<Snapshot> {
        self.check(&self.0.context, Some(Capability::Snapshot))?;
        self.require_live()?;
        let record = self
            .0
            .repository
            .get(runtime_id)
            .await
            .map_err(internal)?
            .ok_or_else(|| error(ErrorCode::BranchNotFound, "root execution instance missing"))?;
        if !self.0.owner.owns(record.tenant_id.as_deref())
            || record.created_by != self.0.owner.principal
        {
            return Err(error(ErrorCode::CapabilityDenied, "root owner mismatch"));
        }
        self.0
            .app
            .heartbeat(&self.0.owner, runtime_id, self.0.lease.clone())
            .await
            .map_err(internal)?;
        let backend = self.0.manager.backend_for(runtime_id).map_err(internal)?;
        if backend.backend_id() != "e2b" {
            return Err(error(ErrorCode::Unsupported, "own backend requires E2B"));
        }
        self.0.store.put(
            "branch",
            runtime_id,
            &BranchRecord {
                released: false,
                output: String::new(),
                root: true,
            },
        )?;
        let context = MutationContext {
            call: self.0.context.clone(),
            idempotency_key: format!("root:{runtime_id}"),
        };
        let backend = self.clone();
        let source = runtime_id.to_string();
        let result = self
            .mutation(
                &context,
                "host.snapshot",
                &serde_json::json!({"branch_id":source}),
                async move {
                    let _guard = backend.lock(&source)?;
                    backend
                        .save_snapshot(&source)
                        .await
                        .map(MutationResult::Snapshot)
                        .map_err(Into::into)
                },
            )
            .await?;
        match result {
            MutationResult::Snapshot(value) => Ok(value),
            _ => Err(internal("invalid root snapshot result")),
        }
    }
    async fn save_snapshot(&self, branch: &str) -> BackendResult<Snapshot> {
        self.0
            .app
            .heartbeat(&self.0.owner, branch, self.0.lease.clone())
            .await
            .map_err(internal)?;
        let r = self
            .0
            .app
            .checkpoint_filesystem(
                &self.0.owner,
                session::SessionCheckpointRequest {
                    runtime_id: branch.into(),
                    name: None,
                    requested_scope: Some(session::SessionCheckpointScope::Full),
                    lease: self.0.lease.clone(),
                },
            )
            .await
            .map_err(internal)?;
        let state = StateRecord {
            checkpoint: r.checkpoint_id.clone(),
            expires_at: (chrono::Utc::now() + chrono::Duration::hours(24)).to_rfc3339(),
        };
        self.0.store.put("state", &r.checkpoint_id, &state)?;
        Ok(Snapshot {
            state_id: r.checkpoint_id,
            expires_at: state.expires_at,
        })
    }
    async fn load(
        &self,
        state: StateRecord,
        branch: &str,
        inherit: bool,
    ) -> Result<(), MutationFailure> {
        self.0.store.put(
            "branch",
            branch,
            &BranchRecord {
                released: false,
                output: String::new(),
                root: false,
            },
        )?;
        let restored = self.0.app.load_checkpoint_with_fresh_runtime(&self.0.owner,session::SessionLoadRequest {
            checkpoint_id:state.checkpoint,runtime_id:Some(branch.into()),conversation_id:Some(branch.into()),sender_id:Some(self.0.owner.principal.clone()),requested_capabilities:Default::default(),
            deployment:session::DeploymentProfile{options:serde_json::json!({"provider_options":{"allow_internet_access":self.0.policy==NetworkPolicy::AllowInternet}}),..Default::default()},
            lease:self.0.lease.clone(),llm:self.0.llm.clone(),
        },(!inherit).then(||self.0.bootstrap.clone())).await;
        if let Err(error) = restored {
            // A ledger identity is written only after provider load and attach
            // complete. Agent failures additionally need confirmed termination.
            // An absent intent proves dispatch was rejected or never started.
            let known = self
                .0
                .manager
                .recorded_instance(branch)
                .await
                .is_ok_and(|instance| instance.is_some());
            let intents = self.0.repository.operation_intents().await;
            let confirmed = intents.is_ok_and(|intents| {
                match intents
                    .iter()
                    .find(|(id, _)| id == &format!("load:{branch}"))
                {
                    None => true,
                    Some((_, body)) => known && body["runtime_failure_confirmed"] == true,
                }
            });
            return Err(classify_restore_error(error, confirmed));
        }
        if let Some(mut record) = self.0.repository.get(branch).await.map_err(internal)? {
            if !record.isolation.metadata.is_object() {
                record.isolation.metadata = serde_json::json!({});
            }
            record.isolation.metadata["provider_options"] = serde_json::json!({"allow_internet_access": self.0.policy == NetworkPolicy::AllowInternet});
            record.isolation.network = if self.0.policy == NetworkPolicy::Offline {
                xgovernor_core::NetworkIsolation::Isolated
            } else {
                xgovernor_core::NetworkIsolation::None
            };
            self.0.repository.save(record).await.map_err(internal)?;
        }
        self.0
            .app
            .heartbeat(&self.0.owner, branch, self.0.lease.clone())
            .await
            .map_err(internal)?;
        self.0
            .store
            .put(
                "branch",
                branch,
                &BranchRecord {
                    released: false,
                    output: String::new(),
                    root: false,
                },
            )
            .map_err(Into::into)
    }
    async fn stop_branch(&self, branch: &str) -> BackendResult<()> {
        self.0
            .app
            .close(&self.0.owner, branch, self.0.lease.clone())
            .await
            .map_err(internal)?;
        self.0.store.put(
            "branch",
            branch,
            &BranchRecord {
                released: true,
                output: String::new(),
                root: false,
            },
        )
    }
    fn job(&self, job: &str) -> BackendResult<JobRecord> {
        self.0
            .store
            .get("job", job)?
            .ok_or_else(|| error(ErrorCode::JobNotFound, "job not found"))
    }
    fn terminal(&self, job: &str, state: JobState) -> BackendResult<()> {
        let r = self.0.store.update("job", job, |r: &mut JobRecord| {
            if !matches!(r.state, JobState::Completed(_) | JobState::Failed(_)) {
                r.state = state;
            }
        })?;
        if let JobState::Completed(result) = r.state {
            if let Some(mut branch) = self.0.store.get::<BranchRecord>("branch", &r.branch)? {
                branch.output = result.output;
                self.0.store.put("branch", &r.branch, &branch)?;
            }
        }
        Ok(())
    }
    async fn cancel_inner(&self, job: &str) -> BackendResult<CancelJobResponse> {
        self.job(job)?;
        let r = self.0.store.update("job", job, |r: &mut JobRecord| {
            if !matches!(r.state, JobState::Completed(_) | JobState::Failed(_)) {
                r.cancelling = true;
            }
        })?;
        if matches!(r.state, JobState::Completed(_) | JobState::Failed(_)) {
            return Ok(CancelJobResponse {
                job_id: job.into(),
                status: CancelJobStatus::Stopped,
            });
        }
        if r.agent {
            self.0
                .app
                .cancel(
                    &self.0.owner,
                    &r.branch,
                    (!r.turn_id.is_empty()).then_some(r.turn_id.as_str()),
                )
                .await
                .map_err(internal)?;
        } else {
            let backend = self.0.manager.backend_for(&r.branch).map_err(internal)?;
            backend.cancel_turn(job).await.map_err(internal)?;
        }
        // The worker that owns the execution guard publishes its terminal only
        // after execution control has confirmed all descendants stopped.
        Ok(CancelJobResponse {
            job_id: job.into(),
            status: CancelJobStatus::Cancelling,
        })
    }
    /// Renew the existing execution leases from the run supervisor's heartbeat.
    pub async fn heartbeat(&self) -> BackendResult<()> {
        for (branch, record) in self.0.store.list::<BranchRecord>("branch")? {
            if !record.released
                && self
                    .0
                    .repository
                    .get(&branch)
                    .await
                    .map_err(internal)?
                    .is_some()
            {
                self.0
                    .app
                    .heartbeat(&self.0.owner, &branch, self.0.lease.clone())
                    .await
                    .map_err(internal)?;
            }
        }
        self.0
            .app
            .reconcile_snapshot_intents()
            .await
            .map_err(internal)
    }

    /// Called by run reclamation after it has stopped admitting new calls.
    /// Leaves failed/unknown resources in durable records for the next pass.
    pub async fn cleanup(&self) -> BackendResult<()> {
        for (job, record) in self.0.store.list::<JobRecord>("job")? {
            if !matches!(record.state, JobState::Completed(_) | JobState::Failed(_)) {
                let _ = self.cancel_inner(&job).await;
            }
        }
        // Closing the whole execution instance is also the proof of cleanup
        // after a restart, when an old process supervisor cannot be reattached.
        for (branch, record) in self.0.store.list::<BranchRecord>("branch")? {
            if record.released {
                continue;
            }
            let lock = self
                .0
                .locks
                .lock()
                .unwrap()
                .entry(branch.clone())
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone();
            let _guard = lock
                .try_lock_owned()
                .map_err(|_| error(ErrorCode::BranchBusy, "execution has not stopped"))?;
            match self.0.repository.get(&branch).await.map_err(internal)? {
                Some(_) => self.stop_branch(&branch).await?,
                None => {
                    self.0
                        .app
                        .reconcile_snapshot_intents()
                        .await
                        .map_err(internal)?;
                    if self
                        .0
                        .repository
                        .operation_intents()
                        .await
                        .map_err(internal)?
                        .iter()
                        .any(|(_, body)| body["target"].as_str() == Some(branch.as_str()))
                    {
                        return Err(error(ErrorCode::BranchBusy, "restore remains unresolved"));
                    }
                    self.0.store.put(
                        "branch",
                        &branch,
                        &BranchRecord {
                            released: true,
                            output: String::new(),
                            root: false,
                        },
                    )?;
                }
            }
            for (job, record) in self.0.store.list::<JobRecord>("job")? {
                if record.branch == branch
                    && !matches!(record.state, JobState::Completed(_) | JobState::Failed(_))
                {
                    self.terminal(
                        &job,
                        failed(
                            "interrupted",
                            "execution instance reclaimed",
                            BTreeMap::new(),
                        ),
                    )?;
                }
            }
        }
        let branches: BTreeSet<String> = self
            .0
            .store
            .list::<BranchRecord>("branch")?
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let mut offset = 0;
        loop {
            let page = self
                .0
                .app
                .list_checkpoints(&self.0.owner, 100, offset)
                .await
                .map_err(internal)?;
            for checkpoint in &page.checkpoints {
                if branches.contains(&checkpoint.source_runtime_id) {
                    self.0
                        .app
                        .delete_checkpoint(
                            &self.0.owner,
                            session::SessionCheckpointDeleteRequest {
                                checkpoint_id: checkpoint.checkpoint_id.clone(),
                                lease: self.0.lease.clone(),
                            },
                        )
                        .await
                        .map_err(internal)?;
                }
            }
            if !page.has_more {
                break;
            }
            // Deleting this run's rows shifts the remaining page, so restart
            // at the same offset until no owned row remains in it.
            if !page
                .checkpoints
                .iter()
                .any(|c| branches.contains(&c.source_runtime_id))
            {
                offset += page.checkpoints.len();
            }
        }
        self.0
            .app
            .reconcile_snapshot_intents()
            .await
            .map_err(internal)
    }
}

#[async_trait]
impl ExecutionBackend for OwnBackend {
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
            .into(),
            network_policies: [NetworkPolicy::Offline, NetworkPolicy::AllowInternet].into(),
            isolation: Isolation::Sandbox,
            joint_snapshots: true,
            independent_branches: true,
            can_reset_history: true,
        }
    }
    async fn prepare_root(&self, r: PrepareRootRequest) -> BackendResult<Snapshot> {
        self.prepare_root_impl(r).await
    }
    async fn renew_run_lease(&self, r: RenewRunLeaseRequest) -> BackendResult<RunLease> {
        self.renew_run_lease_impl(r).await
    }
    async fn reclaim_run(&self, r: ReclaimRunRequest) -> BackendResult<RunReclaimed> {
        self.reclaim_run_impl(r).await
    }
    async fn export_workspace(
        &self,
        r: ExportWorkspaceRequest,
    ) -> BackendResult<WorkspaceExported> {
        self.export_workspace_impl(r).await
    }
    async fn snapshot(&self, r: SnapshotRequest) -> BackendResult<Snapshot> {
        self.check(&r.context.call, Some(Capability::Snapshot))?;
        let b = self.clone();
        let request = r.clone();
        let result = self
            .mutation(&r.context, "host.snapshot", &r, async move {
                b.branch(&request.branch_id)?;
                let _guard = b.lock(&request.branch_id)?;
                b.save_snapshot(&request.branch_id)
                    .await
                    .map(MutationResult::Snapshot)
                    .map_err(Into::into)
            })
            .await?;
        match result {
            MutationResult::Snapshot(v) => Ok(v),
            _ => Err(internal("invalid snapshot result")),
        }
    }
    async fn fork(&self, r: ForkRequest) -> BackendResult<Branch> {
        self.check(&r.context.call, Some(Capability::Fork))?;
        if r.network_policy != self.0.policy {
            return Err(error(
                ErrorCode::CapabilityDenied,
                "fork policy differs from admitted run policy",
            ));
        }
        let b = self.clone();
        let request = r.clone();
        let result = self
            .mutation(&r.context, "host.fork", &r, async move {
                let state = b.state(&request.parent_state_id)?;
                let branch = id();
                // Persist the target before provider dispatch for interrupted cleanup.
                b.0.store.put(
                    "branch",
                    &branch,
                    &BranchRecord {
                        released: false,
                        output: String::new(),
                        root: false,
                    },
                )?;
                b.load(state, &branch, request.inherit_history).await?;
                Ok(MutationResult::Fork(Branch { branch_id: branch }))
            })
            .await?;
        match result {
            MutationResult::Fork(v) => Ok(v),
            _ => Err(internal("invalid fork result")),
        }
    }
    async fn inspect(&self, r: InspectRequest) -> BackendResult<Content> {
        self.check(&r.context, Some(Capability::Inspect))?;
        let branch = self.branch(&r.branch_id)?;
        self.read(&r.context, async {
            Ok(Content {
                content: match r.view {
                    InspectView::Output => branch.output,
                    InspectView::Summary => {
                        serde_json::json!({"branch_id":r.branch_id,"network_policy":self.0.policy})
                            .to_string()
                    }
                },
            })
        })
        .await
    }
    async fn file_read(&self, r: FileReadRequest) -> BackendResult<FileContent> {
        self.check(&r.context, Some(Capability::FileRead))?;
        self.branch(&r.branch_id)?;
        self.read(&r.context, async {
            let v = self
                .0
                .app
                .read_file(
                    &self.0.owner,
                    session::SessionFileReadRequest {
                        runtime_id: r.branch_id.clone(),
                        path: r.path.clone(),
                        lease: self.0.lease.clone(),
                    },
                )
                .await
                .map_err(internal)?;
            Ok(FileContent {
                content_base64: v.content_base64,
            })
        })
        .await
    }
    async fn file_write(&self, r: FileWriteRequest) -> BackendResult<FileWritten> {
        self.check(&r.context.call, Some(Capability::FileWrite))?;
        let b = self.clone();
        let request = r.clone();
        let result = self
            .mutation(&r.context, "host.file_write", &r, async move {
                b.branch(&request.branch_id)?;
                let _guard = b.lock(&request.branch_id)?;
                let v =
                    b.0.app
                        .write_file(
                            &b.0.owner,
                            session::SessionFileWriteRequest {
                                runtime_id: request.branch_id,
                                path: request.path,
                                content_base64: request.content_base64,
                                create_parents: true,
                                lease: b.0.lease.clone(),
                            },
                        )
                        .await
                        .map_err(internal)?;
                Ok(MutationResult::FileWrite(FileWritten {
                    written_bytes: v.bytes_written,
                }))
            })
            .await?;
        match result {
            MutationResult::FileWrite(v) => Ok(v),
            _ => Err(internal("invalid file result")),
        }
    }
    async fn release(&self, r: ReleaseRequest) -> BackendResult<Released> {
        self.check(&r.context.call, Some(Capability::Release))?;
        let b = self.clone();
        let request = r.clone();
        let result = self
            .mutation(&r.context, "host.release", &r, async move {
                let branch =
                    b.0.store
                        .get::<BranchRecord>("branch", &request.branch_id)?
                        .ok_or_else(|| error(ErrorCode::BranchNotFound, "branch not found"))?;
                let _guard = b.lock(&request.branch_id)?;
                if !branch.released {
                    b.stop_branch(&request.branch_id).await?;
                }
                Ok(MutationResult::Release(Released { released: true }))
            })
            .await?;
        match result {
            MutationResult::Release(v) => Ok(v),
            _ => Err(internal("invalid release result")),
        }
    }
    async fn cancel_job(&self, r: CancelJobRequest) -> BackendResult<CancelJobResponse> {
        self.check(&r.context.call, Some(Capability::CancelJob))?;
        let b = self.clone();
        let request = r.clone();
        let result = self
            .mutation(&r.context, "host.cancel_job", &r, async move {
                b.cancel_inner(&request.job_id)
                    .await
                    .map(MutationResult::CancelJob)
                    .map_err(Into::into)
            })
            .await?;
        match result {
            MutationResult::CancelJob(v) => Ok(v),
            _ => Err(internal("invalid cancel result")),
        }
    }
    async fn wait_job(&self, c: CallContext, job: &str) -> BackendResult<JobState> {
        self.check(&c, None)?;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(c.timeout_ms);
        loop {
            let state = self.job(job)?.state;
            if matches!(state, JobState::Completed(_) | JobState::Failed(_)) {
                return Ok(state);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(state);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    async fn reconcile(
        &self,
        c: CallContext,
        op: OperationIdentity,
    ) -> BackendResult<OperationResolution> {
        self.check(&c, None)?;
        if op.run_id != c.run_id {
            return Err(error(
                ErrorCode::RunNotFound,
                "operation belongs to another run",
            ));
        }
        let resolution = self.0.store.resolution(&op)?;
        if matches!(
            resolution,
            OperationResolution::Unknown | OperationResolution::Pending
        ) {
            tokio::time::sleep(Duration::from_millis(c.timeout_ms.min(20))).await;
        }
        Ok(resolution)
    }
    async fn exec(&self, r: ExecRequest) -> BackendResult<JobReceipt> {
        self.check(&r.context.call, Some(Capability::Exec))?;
        if r.argv.first().is_none_or(|v| v.is_empty()) {
            return Err(error(
                ErrorCode::InvalidParams,
                "command executable is required",
            ));
        }
        let b = self.clone();
        let request = r.clone();
        let result = self
            .mutation(&r.context, "host.exec", &r, async move {
                b.branch(&request.branch_id)?;
                let guard = b.lock(&request.branch_id)?;
                let backend =
                    b.0.manager
                        .backend_for(&request.branch_id)
                        .map_err(internal)?;
                if backend.execution_control().is_none() {
                    return Err(error(
                        ErrorCode::Unsupported,
                        "execution cancellation unavailable",
                    )
                    .into());
                }
                let job = id();
                backend.begin_turn(&job).await.map_err(internal)?;
                b.0.store.put(
                    "job",
                    &job,
                    &JobRecord {
                        branch: request.branch_id.clone(),
                        agent: false,
                        turn_id: job.clone(),
                        state: JobState::Accepted,
                        cancelling: false,
                    },
                )?;
                let worker = b.clone();
                let job_id = job.clone();
                tokio::spawn(async move {
                    let _guard = guard;
                    let result = OPERATION_CONTEXT
                        .scope(
                            operation_protocol::OperationContext {
                                turn_id: job_id.clone(),
                                operation_id: job_id.clone(),
                            },
                            worker.0.app.exec(
                                &worker.0.owner,
                                session::SessionExecRequest {
                                    runtime_id: request.branch_id,
                                    command: request.argv,
                                    cwd: Some(request.cwd),
                                    env: Default::default(),
                                    timeout_ms: Some(request.context.call.timeout_ms),
                                    lease: worker.0.lease.clone(),
                                },
                            ),
                        )
                        .await;
                    let control = backend.finish_turn(&job_id).await;
                    if control.is_err() {
                        // Keep the job unresolved and the branch fenced until run
                        // reclamation confirms the provider instance has stopped.
                        return;
                    }
                    let cancelling = worker.job(&job_id).is_ok_and(|r| r.cancelling);
                    let state = match (result, control) {
                        _ if cancelling => {
                            failed("cancelled", "command cancelled", BTreeMap::new())
                        }
                        (Ok(v), Ok(())) if !v.timed_out => JobState::Completed(JobCompleted {
                            output: format!("{}{}", v.stdout, v.stderr),
                            usage: BTreeMap::new(),
                            exit_code: v.exit_code.map(i64::from),
                            truncated: false,
                        }),
                        (Ok(_), _) => {
                            failed("deadline_exceeded", "command timed out", BTreeMap::new())
                        }
                        (Err(e), _) => failed(
                            if e.to_string().to_ascii_lowercase().contains("cancel") {
                                "cancelled"
                            } else {
                                "execution_failed"
                            },
                            &e.to_string(),
                            BTreeMap::new(),
                        ),
                    };
                    let _ = worker.terminal(&job_id, state);
                });
                Ok(MutationResult::Exec(JobReceipt {
                    job_id: job,
                    status: Accepted::Accepted,
                }))
            })
            .await?;
        match result {
            MutationResult::Exec(v) => Ok(v),
            _ => Err(internal("invalid exec result")),
        }
    }
    async fn agent_turn(&self, r: AgentTurnRequest) -> BackendResult<JobReceipt> {
        self.check(&r.context.call, Some(Capability::AgentTurn))?;
        if r.profile.is_some() {
            return Err(error(
                ErrorCode::Unsupported,
                "runtime profiles are not configured",
            ));
        }
        let b = self.clone();
        let request = r.clone();
        let result=self.mutation(&r.context,"host.agent_turn",&r,async move {
            b.branch(&request.branch_id)?;let guard=b.lock(&request.branch_id)?;let job=id();
            b.0.store.put("job",&job,&JobRecord{branch:request.branch_id.clone(),agent:true,turn_id:String::new(),state:JobState::Accepted,cancelling:false})?;
            let submission=b.0.app.submit_turn(&b.0.owner,session::SessionTurnRequest{runtime_id:request.branch_id.clone(),text:request.input,entry:Default::default(),llm:None,reasoning_effort:None,client_request_id:Some(job.clone()),ext:Default::default(),lease:b.0.lease.clone()}).await.map_err(internal)?;
            let mut record=b.job(&job)?;record.turn_id=submission.receipt.turn_id.clone();record.state=JobState::Running;b.0.store.put("job",&job,&record)?;
            let mut events=submission.events.ok_or_else(||internal("new Agent turn has no event stream"))?;
            let worker=b.clone();let job_id=job.clone();
            tokio::spawn(async move {
                let _guard=guard;let mut output=String::new();let mut expired=false;
                let deadline=tokio::time::sleep(Duration::from_millis(request.context.call.timeout_ms));tokio::pin!(deadline);
                let state=loop {
                    tokio::select! {
                        _=&mut deadline, if !expired => {expired=true;let _=worker.cancel_inner(&job_id).await;}
                        event=events.recv()=>match event {
                            Some(session::SessionEvent::OutputDelta{delta,..})=>output.push_str(&delta),
                            Some(session::SessionEvent::TurnCompleted{outcome,usage,..})=>{
                                let usage=usage_counts(usage);
                                break if expired {failed("deadline_exceeded","Agent deadline expired",usage)}
                                else if outcome==session::SessionTurnOutcome::Cancelled {failed("cancelled","Agent turn cancelled",usage)}
                                else {JobState::Completed(JobCompleted{output,usage,exit_code:None,truncated:false})};
                            }
                            Some(session::SessionEvent::TurnFailed{error,usage,..})=>break failed(&error.code,&error.message,usage_counts(usage)),
                            None=>{
                                if worker.stop_branch(&request.branch_id).await.is_err() {return;}
                                break failed("stream_closed","Agent event stream ended before a terminal",BTreeMap::new());
                            },
                            _=>{}
                        }
                    }
                };let _=worker.terminal(&job_id,state);
            });
            Ok(MutationResult::AgentTurn(JobReceipt{job_id:job,status:Accepted::Accepted}))
        }).await?;
        match result {
            MutationResult::AgentTurn(v) => Ok(v),
            _ => Err(internal("invalid turn result")),
        }
    }
    async fn diff(&self, r: DiffRequest) -> BackendResult<Content> {
        self.check(&r.context, Some(Capability::Diff))?;
        self.branch(&r.branch_id)?;
        let state = self.state(&r.base_state_id)?;
        let _guard = self.lock(&r.branch_id)?;
        let backend = self.clone();
        let request = r.clone();
        let task = tokio::spawn(async move {
            let _guard = _guard;
            let r = request;
            let this = backend;
            let _run = this.0.run_gate.read().await;
            this.require_live()?;
            let baseline = id();
            this.load(state, &baseline, false).await.map_err(|e| {
                e.project(&OperationIdentity {
                    run_id: r.context.run_id.clone(),
                    method: "host.diff".into(),
                    idempotency_key: baseline.clone(),
                })
            })?;
            let result = async {
                let base = this.workspace_files(&baseline).await?;
                let current = this.workspace_files(&r.branch_id).await?;
                let paths: BTreeSet<_> = base.keys().chain(current.keys()).collect();
                let mut patch = String::new();
                for path in paths {
                    let old = base.get(path).map(Vec::as_slice).unwrap_or_default();
                    let new = current.get(path).map(Vec::as_slice).unwrap_or_default();
                    if old == new && base.contains_key(path) == current.contains_key(path) {
                        continue;
                    }
                    let from = if base.contains_key(path) {
                        format!("a/{path}")
                    } else {
                        "/dev/null".into()
                    };
                    let to = if current.contains_key(path) {
                        format!("b/{path}")
                    } else {
                        "/dev/null".into()
                    };
                    match (std::str::from_utf8(old), std::str::from_utf8(new)) {
                        (Ok(old), Ok(new)) => patch.push_str(
                            &similar::TextDiff::from_lines(old, new)
                                .unified_diff()
                                .header(&from, &to)
                                .to_string(),
                        ),
                        _ => patch.push_str(&format!("Binary files {from} and {to} differ\n")),
                    }
                }
                Ok(Content { content: patch })
            }
            .await;
            this.stop_branch(&baseline).await?;
            result
        });
        match tokio::time::timeout(Duration::from_millis(r.context.timeout_ms), task).await {
            Ok(result) => result.map_err(internal)?,
            Err(_) => Err(error(
                ErrorCode::DeadlineExceeded,
                "diff deadline expired; temporary branch remains tracked until cleanup",
            )),
        }
    }
}

impl OwnBackend {
    async fn workspace_files(&self, branch: &str) -> BackendResult<BTreeMap<String, Vec<u8>>> {
        // Enumerate actual filesystem contents, including untracked files. The
        // immutable base is restored separately; git HEAD is never a substitute.
        let backend = self.0.manager.backend_for(branch).map_err(internal)?;
        let result=backend.exec().exec(operation_protocol::capability::exec::ExecRequest{command:"python3".into(),args:vec!["-c".into(),r#"import os,json,base64,stat
root='.'; result={}; total=0
for parent,dirs,files in os.walk(root,followlinks=False):
 dirs[:]=[d for d in dirs if d!='.git']
 for name in files:
  p=os.path.join(parent,name); rel=os.path.relpath(p,root)
  if not stat.S_ISREG(os.lstat(p).st_mode): raise RuntimeError('diff requires regular files: '+rel)
  total+=os.path.getsize(p)
  if total>16777216 or len(result)>=10000: raise RuntimeError('diff workspace exceeds bounded export')
  with open(p,'rb') as f: result[rel]=base64.b64encode(f.read()).decode()
print(json.dumps(result))"#.into()],cwd:Some(backend.paths().workspace_root().clone()),timeout_ms:Some(30_000),..Default::default()}).await.map_err(internal)?;
        if result.exit_code != Some(0) || result.timed_out {
            return Err(error(ErrorCode::Internal, "workspace export failed"));
        }
        use base64::Engine;
        let values: BTreeMap<String, String> =
            serde_json::from_slice(&result.stdout).map_err(internal)?;
        values
            .into_iter()
            .map(|(p, v)| {
                Ok((
                    p,
                    base64::engine::general_purpose::STANDARD
                        .decode(v)
                        .map_err(internal)?,
                ))
            })
            .collect()
    }
}
fn failed(code: &str, message: &str, usage: BTreeMap<String, u64>) -> JobState {
    JobState::Failed(JobFailed {
        failure: Failure {
            code: code.into(),
            message: message.into(),
        },
        usage,
    })
}
fn usage_counts(usage: session::SessionUsageReport) -> BTreeMap<String, u64> {
    match usage {
        session::SessionUsageReport::Known(v) => [
            ("input_tokens".into(), v.input_tokens),
            ("output_tokens".into(), v.output_tokens),
            ("total_tokens".into(), v.total_tokens),
        ]
        .into(),
        session::SessionUsageReport::Unknown => BTreeMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xgovernor_core::SessionDomainError;

    #[tokio::test]
    async fn restore_failures_keep_definite_results_and_reconcile_uncertain_calls() {
        use backend::{e2b::E2bProvider, SqliteProviderInstanceLedger};
        struct Ids;
        impl xgovernor_core::Clock for Ids {
            fn now_ms(&self) -> u64 {
                1
            }
        }
        impl xgovernor_core::RuntimeIdGenerator for Ids {
            fn next_runtime_id(&self) -> String {
                id()
            }
        }
        impl xgovernor_core::TurnIdGenerator for Ids {
            fn next_turn_id(&self) -> String {
                id()
            }
        }
        let repo = Arc::new(xgovernor_core::SqliteSessionRepository::open_in_memory().unwrap());
        let provider = Arc::new(E2bProvider::new());
        let manager = Arc::new(InstanceManager::new(
            provider.clone(),
            provider,
            Arc::new(SqliteProviderInstanceLedger::open_in_memory().unwrap()),
            provider_protocol::ProviderKind("e2b".into()),
            xgovernor_manager::InstanceManagerConfig::new(1, 1),
        ));
        let app = Arc::new(
            SessionApplication::with_runtime_registry(
                "opencode",
                [runtime::opencode_registration("unused", manager.clone())],
                repo.clone(),
                Arc::new(Ids),
                Arc::new(Ids),
                Arc::new(Ids),
            )
            .unwrap(),
        );
        let context = CallContext {
            run_id: "run".into(),
            plugin_id: "plugin".into(),
            timeout_ms: 1000,
        };
        let backend = OwnBackend::open(
            Path::new(":memory:"),
            app,
            manager,
            repo,
            SecurityContext::admin("owner"),
            context.clone(),
            BTreeSet::new(),
            NetworkPolicy::Offline,
            None,
            Default::default(),
            serde_json::from_value(serde_json::json!({"client_id":"client"})).unwrap(),
        )
        .unwrap();
        backend.0.store.put("lease", "run", &serde_json::json!({"expires_at":(chrono::Utc::now()+chrono::Duration::minutes(1)).to_rfc3339(),"reclaiming":false})).unwrap();
        let request = MutationContext {
            call: context,
            idempotency_key: "restore".into(),
        };
        let result = backend
            .mutation(&request, "host.fork", &serde_json::json!({}), async {
                Err(classify_restore_error(
                    SessionDomainError::Unavailable {
                        message: "plugin activation failed".into(),
                    },
                    true,
                ))
            })
            .await;
        assert_eq!(
            result,
            Err(error(
                ErrorCode::Internal,
                "session unavailable: plugin activation failed"
            ))
        );
        assert_eq!(
            backend
                .mutation(&request, "host.fork", &serde_json::json!({}), async {
                    panic!("must replay")
                })
                .await,
            result
        );
        let store = &backend.0.store;
        let op = OperationIdentity {
            run_id: "run".into(),
            method: "host.fork".into(),
            idempotency_key: "restore".into(),
        };
        assert!(matches!(
            store.resolution(&op).unwrap(),
            OperationResolution::Rejected {
                code: ErrorCode::Internal,
                ..
            }
        ));
        assert!(store.unresolved().unwrap().is_empty());
        let unknown = classify_restore_error(
            SessionDomainError::Unavailable {
                message: "provider response lost".into(),
            },
            false,
        )
        .project(&op);
        assert!(matches!(unknown, BackendError::OutcomeUnknown { .. }));
        store.finish(&op, &Err(unknown)).unwrap();
        assert!(matches!(
            store.resolution(&op).unwrap(),
            OperationResolution::Unknown
        ));
        assert_eq!(store.unresolved().unwrap().len(), 1);
        assert!(matches!(
            classify_restore_error(
                SessionDomainError::Timeout {
                    operation: "restore".into(),
                    timeout_ms: 10,
                },
                true
            )
            .project(&op),
            BackendError::OutcomeUnknown { .. }
        ));
    }
}
