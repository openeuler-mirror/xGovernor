//! Backend-facing transcription of the host capabilities in `mcts-rpc-v1`.
use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Effective policy for every search, scoring and evaluation branch. A backend
/// must enforce the requested policy or reject the fork; never silently relax it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum NetworkPolicy {
    Offline,
    AllowInternet,
}

/// Isolation supplied by the selected deployment, not inferred from its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Isolation {
    None,
    /// Dedicated workspace only; no security isolation (including bare worktrees).
    Workspace,
    /// Container isolation sharing the host kernel, e.g. Docker.
    Container,
    /// Sandbox boundary with a separate kernel, e.g. E2B.
    Sandbox,
}

impl Isolation {
    pub fn satisfies(self, required: Self) -> bool {
        let rank = |value| match value {
            Self::None => 0,
            Self::Workspace => 1,
            Self::Container => 2,
            Self::Sandbox => 3,
        };
        rank(self) >= rank(required)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    PrepareRoot,
    RenewRunLease,
    ReclaimRun,
    ExportWorkspace,
    Snapshot,
    Fork,
    AgentTurn,
    CancelJob,
    Inspect,
    Exec,
    FileRead,
    FileWrite,
    Diff,
    Release,
}

/// Report only guarantees supplied by the provider and Agent combination.
/// Missing capabilities fail admission; this is not permission to emulate an
/// external backend's missing isolation, offline policy or consistency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendCapabilities {
    pub capabilities: BTreeSet<Capability>,
    pub network_policies: BTreeSet<NetworkPolicy>,
    pub isolation: Isolation,
    pub joint_snapshots: bool,
    pub independent_branches: bool,
    pub can_reset_history: bool,
}

/// IDs are opaque and scoped to this run. Every call checks ownership and
/// granted capabilities. `timeout_ms` is finite and positive; expiry is not proof
/// that a mutation stopped. Dropping a future is not cancellation confirmation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallContext {
    pub plugin_id: String,
    pub run_id: String,
    pub timeout_ms: u64,
}

/// Deduplicate by (run_id, host method, idempotency_key) until run reclamation.
/// Same key and arguments returns the original result, including its job ID;
/// changed arguments is IdempotencyConflict. Persist the intent before dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationContext {
    #[serde(flatten)]
    pub call: CallContext,
    pub idempotency_key: String,
}

/// Backend-neutral environment identity; references are resolved by the backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "reference", rename_all = "snake_case")]
pub enum EnvironmentRef {
    Image(String),
    Template(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceSource {
    Git {
        repository: String,
        commit: String,
    },
    Snapshot {
        state_id: String,
    },
    /// Reserved for workspace upload; implementations currently reject it.
    Package {
        digest: String,
        manifest_ref: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootDescription {
    pub environment: EnvironmentRef,
    pub workspace: WorkspaceSource,
    pub working_directory: String,
    /// Required effective policy loaded from the run record.
    pub network_policy: NetworkPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepareRootRequest {
    #[serde(flatten)]
    pub context: MutationContext,
    pub root: RootDescription,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewRunLeaseRequest {
    #[serde(flatten)]
    pub context: MutationContext,
    /// Positive duration from backend acceptance. Retrying the same key returns
    /// the original expiry, so each periodic renewal needs its own key.
    pub ttl_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLease {
    /// Backend clock, RFC 3339. Expiry triggers backend-owned orphan reclamation.
    pub expires_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReclaimRunRequest {
    #[serde(flatten)]
    pub context: MutationContext,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceIdentity {
    pub kind: String,
    pub id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReclaimStatus {
    Complete,
    Unknown,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunReclaimed {
    pub status: ReclaimStatus,
    /// Empty only when complete. Retain identities for unresolved resources,
    /// including in-flight operations whose resources are not yet known.
    pub remaining: Vec<ResourceIdentity>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum WorkspaceRef {
    Snapshot(String),
    Branch(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportWorkspaceRequest {
    #[serde(flatten)]
    pub context: MutationContext,
    pub source: WorkspaceRef,
    /// Authorized manager-owned run directory, never supplied by the algorithm.
    pub run_directory: String,
    /// Relative destination beneath run_directory. Reject traversal, symlink
    /// escape and existing destinations not owned by this operation.
    pub path: String,
    /// Maximum complete archive size, in bytes. Never return truncated success.
    pub max_bytes: u64,
    /// Default false: include tracked and new workspace files, exclude .git.
    #[serde(default)]
    pub include_vcs_metadata: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceExported {
    pub size_bytes: u64,
    pub file_count: u64,
    /// Lowercase SHA-256 of the complete tar archive written at the destination.
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRequest {
    #[serde(flatten)]
    pub context: MutationContext,
    pub branch_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkRequest {
    #[serde(flatten)]
    pub context: MutationContext,
    pub parent_state_id: String,
    /// Required effective policy from the persisted run configuration, possibly
    /// tightened by the RPC mapper. There is no backend default or wire boolean.
    pub network_policy: NetworkPolicy,
    /// False copies the workspace but starts an empty Agent conversation.
    pub inherit_history: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTurnRequest {
    #[serde(flatten)]
    pub context: MutationContext,
    pub branch_id: String,
    pub input: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelJobRequest {
    #[serde(flatten)]
    pub context: MutationContext,
    pub job_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectView {
    Summary,
    Output,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectRequest {
    #[serde(flatten)]
    pub context: CallContext,
    pub branch_id: String,
    pub view: InspectView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecRequest {
    #[serde(flatten)]
    pub context: MutationContext,
    pub branch_id: String,
    pub argv: Vec<String>,
    pub cwd: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileReadRequest {
    #[serde(flatten)]
    pub context: CallContext,
    pub branch_id: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileWriteRequest {
    #[serde(flatten)]
    pub context: MutationContext,
    pub branch_id: String,
    pub path: String,
    pub content_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffRequest {
    #[serde(flatten)]
    pub context: CallContext,
    pub branch_id: String,
    pub base_state_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseRequest {
    #[serde(flatten)]
    pub context: MutationContext,
    pub branch_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub state_id: String,
    /// RFC 3339 timestamp. Snapshot lifetime is independent of the live branch.
    pub expires_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Branch {
    pub branch_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Content {
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileContent {
    pub content_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileWritten {
    pub written_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Accepted {
    Accepted,
}

/// Acceptance is not completion. Publish terminal events only after delivering
/// this receipt. Retrying the initiating mutation must return this same job ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobReceipt {
    pub job_id: String,
    pub status: Accepted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Failure {
    pub code: String,
    pub message: String,
}

/// Empty usage means unavailable, never a measured zero. A completed command
/// includes exit_code (nonzero is still completion); an Agent turn omits it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCompleted {
    pub output: String,
    pub usage: BTreeMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i64>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobFailed {
    pub failure: Failure,
    pub usage: BTreeMap<String, u64>,
}

/// One immutable terminal per job. Failed includes execution timeout or
/// confirmed cancellation, never a fabricated score. EOF is not completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum JobState {
    Accepted,
    Running,
    Completed(JobCompleted),
    Failed(JobFailed),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelJobStatus {
    Cancelling,
    Stopped,
    /// Execution had already completed when cancellation arrived.
    Completed,
    /// Execution had already failed (or confirmed cancellation is reported as failure).
    Failed,
}

/// Stopped requires execution and all descendants to have stopped. It does not
/// undo side effects or overwrite a completion that won the terminal race.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelJobResponse {
    pub job_id: String,
    pub status: CancelJobStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Released {
    pub released: bool,
}

/// Same identity must be retained across timeout/disconnect and reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationIdentity {
    pub run_id: String,
    /// Full host method name, e.g. host.fork.
    pub method: String,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BackendError {
    #[error("{code:?}: {message}")]
    Rejected { code: ErrorCode, message: String },
    /// The operation may have taken effect. Do not retry under a fresh key or
    /// release dependent resources. Reconcile this identity; unknown is not clean.
    #[error("outcome unknown for {operation:?}: {message}")]
    OutcomeUnknown {
        operation: OperationIdentity,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidParams,
    Unsupported,
    CapabilityDenied,
    RunNotFound,
    BranchNotFound,
    JobNotFound,
    StateExpired,
    IdempotencyConflict,
    BranchBusy,
    DeadlineExceeded,
    Cancelled,
    BudgetExhausted,
    Internal,
    ExportTooLarge,
}

/// Typed cached mutation result, also used when an unknown operation resolves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", content = "result")]
pub enum MutationResult {
    #[serde(rename = "host.prepare_root")]
    PrepareRoot(Snapshot),
    #[serde(rename = "host.renew_run_lease")]
    RenewRunLease(RunLease),
    #[serde(rename = "host.reclaim_run")]
    ReclaimRun(RunReclaimed),
    #[serde(rename = "host.export_workspace")]
    ExportWorkspace(WorkspaceExported),
    #[serde(rename = "host.snapshot")]
    Snapshot(Snapshot),
    #[serde(rename = "host.fork")]
    Fork(Branch),
    #[serde(rename = "host.agent_turn")]
    AgentTurn(JobReceipt),
    #[serde(rename = "host.cancel_job")]
    CancelJob(CancelJobResponse),
    #[serde(rename = "host.exec")]
    Exec(JobReceipt),
    #[serde(rename = "host.file_write")]
    FileWrite(FileWritten),
    #[serde(rename = "host.release")]
    Release(Released),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "result", rename_all = "snake_case")]
pub enum OperationResolution {
    /// Authoritative proof it never started; retry only with the same identity.
    NotStarted,
    Pending,
    Unknown,
    Succeeded(MutationResult),
    Rejected {
        code: ErrorCode,
        message: String,
    },
}

pub type BackendResult<T> = Result<T, BackendError>;

/// Backend boundary; implementations enforce authorization, ownership, finite
/// deadlines and idempotency, including when used without a stdio dispatcher.
/// Per branch, mutations and snapshots are exclusive (BranchBusy on conflict).
/// Snapshots jointly capture Agent/environment state; forks have independent
/// mutable identities. Releasing a stopped live branch preserves its snapshots
/// and is repeatable. Run cleanup waits for jobs to stop before releasing them.
#[async_trait]
pub trait ExecutionBackend: Send + Sync {
    fn capabilities(&self) -> BackendCapabilities;
    /// Return an immutable snapshot owned by context.run_id, including when the
    /// source is an existing snapshot. Never transfer another run's ownership.
    async fn prepare_root(&self, _request: PrepareRootRequest) -> BackendResult<Snapshot> {
        Err(unsupported("prepare_root"))
    }
    /// Must establish/renew the run lease before provisioning resources. After
    /// expiry the backend stops jobs and reclaims all run resources itself.
    async fn renew_run_lease(&self, _request: RenewRunLeaseRequest) -> BackendResult<RunLease> {
        Err(unsupported("renew_run_lease"))
    }
    /// Wait for all in-flight work to stop before releasing run resources.
    /// Export selected artifacts before reclaiming; they survive reclamation.
    async fn reclaim_run(&self, _request: ReclaimRunRequest) -> BackendResult<RunReclaimed> {
        Err(unsupported("reclaim_run"))
    }
    /// Export a consistent complete tar archive, or reject without publishing a
    /// partial destination. Retain operation identity if publication is unknown.
    async fn export_workspace(
        &self,
        _request: ExportWorkspaceRequest,
    ) -> BackendResult<WorkspaceExported> {
        Err(unsupported("export_workspace"))
    }
    async fn snapshot(&self, request: SnapshotRequest) -> BackendResult<Snapshot>;
    async fn fork(&self, request: ForkRequest) -> BackendResult<Branch>;
    async fn agent_turn(&self, request: AgentTurnRequest) -> BackendResult<JobReceipt>;
    async fn cancel_job(&self, request: CancelJobRequest) -> BackendResult<CancelJobResponse>;
    async fn inspect(&self, request: InspectRequest) -> BackendResult<Content>;
    async fn exec(&self, request: ExecRequest) -> BackendResult<JobReceipt>;
    async fn file_read(&self, request: FileReadRequest) -> BackendResult<FileContent>;
    async fn file_write(&self, request: FileWriteRequest) -> BackendResult<FileWritten>;
    async fn diff(&self, request: DiffRequest) -> BackendResult<Content>;
    async fn release(&self, request: ReleaseRequest) -> BackendResult<Released>;
    /// Bounded wait/query. This is host-side; algorithms receive host.job.*.
    async fn wait_job(&self, context: CallContext, job_id: &str) -> BackendResult<JobState>;
    /// Read-only bounded lookup. Never substitute a fresh mutation to discover
    /// an outcome. Unknown/Pending must remain tracked until resolved/reclaimed.
    async fn reconcile(
        &self,
        context: CallContext,
        operation: OperationIdentity,
    ) -> BackendResult<OperationResolution>;
}

fn unsupported(method: &str) -> BackendError {
    BackendError::Rejected {
        code: ErrorCode::Unsupported,
        message: format!("backend does not support {method}"),
    }
}
