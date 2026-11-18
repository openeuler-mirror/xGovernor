//! Stdio mappings: UTF-8, one JSON-RPC 2.0 object per line; stderr is for logs.
//! The `plugin.*` and `host.*` spellings remain wire compatibility names only.
//! Request IDs correlate replies; they are never mutation idempotency keys.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::execution::*;
use crate::run::RunConfig;

pub const API_VERSION: &str = "mcts.rpc.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RpcId {
    String(String),
    Number(i64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request<T> {
    /// Must equal "2.0"; dispatchers reject null IDs and non-object params.
    pub jsonrpc: String,
    pub id: RpcId,
    #[serde(flatten)]
    pub call: T,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Notification<T> {
    pub jsonrpc: String,
    #[serde(flatten)]
    pub call: T,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response<T> {
    pub jsonrpc: String,
    /// None is reserved for parse/invalid-request errors without a known ID.
    pub id: Option<RpcId>,
    #[serde(flatten)]
    pub outcome: ResponseOutcome<T>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseOutcome<T> {
    Result(T),
    Error(RpcError),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    /// Known run/resource IDs and, for uncertainty, the stable operation identity.
    pub data: Value,
}

/// An unresolved mutation must terminate execution, never become a retryable
/// algorithm RPC error. Only authoritative backend rejections can be mapped.
impl TryFrom<BackendError> for RpcError {
    type Error = BackendError;
    fn try_from(error: BackendError) -> Result<Self, Self::Error> {
        if matches!(error, BackendError::OutcomeUnknown { .. }) {
            return Err(error);
        }
        let code = match &error {
            BackendError::Rejected { code, .. } => match code {
                ErrorCode::InvalidParams | ErrorCode::ExportTooLarge => -32602,
                ErrorCode::CapabilityDenied => -32001,
                ErrorCode::RunNotFound => -32002,
                ErrorCode::BranchNotFound => -32003,
                ErrorCode::DeadlineExceeded => -32004,
                ErrorCode::Cancelled => -32005,
                ErrorCode::StateExpired => -32006,
                ErrorCode::IdempotencyConflict => -32007,
                ErrorCode::Unsupported => -32008,
                ErrorCode::BranchBusy => -32009,
                ErrorCode::BudgetExhausted => -32010,
                ErrorCode::JobNotFound | ErrorCode::Internal => -32603,
            },
            BackendError::OutcomeUnknown { .. } => unreachable!(),
        };
        Ok(Self {
            code,
            message: error.to_string(),
            data: serde_json::to_value(error).expect("backend error serializes"),
        })
    }
}

/// Algorithm wire capabilities. Backend method declarations use execution::Capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireCapability {
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
    JobProgress,
}

impl WireCapability {
    pub fn from_backend(value: Capability) -> Option<Self> {
        Some(match value {
            Capability::Snapshot => Self::Snapshot,
            Capability::Fork => Self::Fork,
            Capability::AgentTurn => Self::AgentTurn,
            Capability::CancelJob => Self::CancelJob,
            Capability::Inspect => Self::Inspect,
            Capability::Exec => Self::Exec,
            Capability::FileRead => Self::FileRead,
            Capability::FileWrite => Self::FileWrite,
            Capability::Diff => Self::Diff,
            Capability::Release => Self::Release,
            Capability::PrepareRoot
            | Capability::RenewRunLease
            | Capability::ReclaimRun
            | Capability::ExportWorkspace => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireCancelJobStatus {
    Cancelling,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireCancelJobResponse {
    pub job_id: String,
    pub status: WireCancelJobStatus,
}

impl From<CancelJobResponse> for WireCancelJobResponse {
    fn from(value: CancelJobResponse) -> Self {
        Self {
            job_id: value.job_id,
            status: match value.status {
                CancelJobStatus::Cancelling => WireCancelJobStatus::Cancelling,
                CancelJobStatus::Completed => WireCancelJobStatus::Completed,
                CancelJobStatus::Stopped | CancelJobStatus::Failed => WireCancelJobStatus::Failed,
            },
        }
    }
}

/// Legacy algorithm wire input only. Never pass this boolean to a backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostForkParams {
    #[serde(flatten)]
    pub context: MutationContext,
    pub parent_state_id: String,
    #[serde(default)]
    pub offline: bool,
    #[serde(default = "inherit_history_default")]
    pub inherit_history: bool,
}

fn inherit_history_default() -> bool {
    true
}

impl HostForkParams {
    /// The caller loads the run record identified by context.call.run_id after
    /// authorization. Algorithm input can tighten, never relax, that policy.
    pub fn into_backend_request(self, recorded_config: &RunConfig) -> ForkRequest {
        let mut request =
            recorded_config.fork_request(self.context, self.parent_state_id, self.inherit_history);
        if self.offline {
            request.network_policy = NetworkPolicy::Offline;
        }
        request
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum HostCall {
    #[serde(rename = "host.snapshot")]
    Snapshot(SnapshotRequest),
    #[serde(rename = "host.fork")]
    Fork(HostForkParams),
    #[serde(rename = "host.agent_turn")]
    AgentTurn(AgentTurnRequest),
    #[serde(rename = "host.cancel_job")]
    CancelJob(CancelJobRequest),
    #[serde(rename = "host.inspect")]
    Inspect(InspectRequest),
    #[serde(rename = "host.exec")]
    Exec(ExecRequest),
    #[serde(rename = "host.file_read")]
    FileRead(FileReadRequest),
    #[serde(rename = "host.file_write")]
    FileWrite(FileWriteRequest),
    #[serde(rename = "host.diff")]
    Diff(DiffRequest),
    #[serde(rename = "host.release")]
    Release(ReleaseRequest),
}

/// Responses select their payload using the corresponding request's method.
/// No extra enum tag is added to the result object on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HostResult {
    Snapshot(Snapshot),
    Fork(Branch),
    Job(JobReceipt),
    CancelJob(WireCancelJobResponse),
    Content(Content),
    FileRead(FileContent),
    FileWrite(FileWritten),
    Release(Released),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobEvent<T> {
    pub run_id: String,
    pub job_id: String,
    pub sequence: u64,
    #[serde(flatten)]
    pub payload: T,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Empty {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputDelta {
    #[serde(
        deserialize_with = "nonempty_string",
        serialize_with = "serialize_nonempty_string"
    )]
    pub stream: String,
    pub delta: String,
}

fn serialize_nonempty_string<S: serde::Serializer>(
    value: &str,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    if value.is_empty() {
        return Err(serde::ser::Error::custom("stream must not be empty"));
    }
    serializer.serialize_str(value)
}

fn nonempty_string<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let value = String::deserialize(deserializer)?;
    if value.is_empty() {
        return Err(serde::de::Error::custom("string must not be empty"));
    }
    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobProgress {
    pub response_count: u64,
    pub head_input_tokens: u64,
    pub usage: BTreeMap<String, u64>,
    pub new_text: String,
    pub tools: Vec<BTreeMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_tokens: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolActivity {
    pub summary: String,
}

/// Publish receipt first, deduplicate by (job_id, sequence), exactly one terminal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum JobNotification {
    #[serde(rename = "host.job.progress")]
    Progress(JobEvent<JobProgress>),
    #[serde(rename = "host.job.started")]
    Started(JobEvent<Empty>),
    #[serde(rename = "host.job.output_delta")]
    OutputDelta(JobEvent<OutputDelta>),
    #[serde(rename = "host.job.tool_activity")]
    ToolActivity(JobEvent<ToolActivity>),
    #[serde(rename = "host.job.completed")]
    Completed(JobEvent<JobCompleted>),
    #[serde(rename = "host.job.failed")]
    Failed(JobEvent<JobFailed>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitializeParams {
    pub plugin_id: String,
    pub supported_api_versions: Vec<String>,
    pub host_version: String,
    pub available_capabilities: BTreeSet<WireCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitializeResult {
    pub api_version: String,
    pub plugin_version: String,
    pub required_host_capabilities: BTreeSet<WireCapability>,
    pub optional_host_capabilities: BTreeSet<WireCapability>,
}

/// Legacy start budget mapping. Run management combines the public limits with
/// the selected algorithm's parameters at this wire boundary. No algorithm
/// parameter names are part of the common run configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireBudget {
    pub max_iters: std::num::NonZeroU64,
    pub branching: std::num::NonZeroU64,
    pub max_time_ms: u64,
    pub max_tokens: u64,
}

impl WireBudget {
    pub fn from_config(config: &RunConfig) -> Result<Self, serde_json::Error> {
        let mut values = BTreeMap::new();
        for key in ["max_iters", "branching"] {
            if let Some(value) = config.algorithm.parameters.get(key) {
                values.insert(key.to_owned(), value.clone());
            }
        }
        values.insert("max_time_ms".into(), Value::from(config.budget.max_time_ms));
        values.insert("max_tokens".into(), Value::from(config.budget.max_tokens));
        serde_json::from_value(serde_json::to_value(values)?)
    }
}

/// Preserve the existing lifecycle payload. Session IDs describe optional
/// caller provenance only; execution is scoped by run_id and root_state_id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartParams {
    pub plugin_id: String,
    pub run_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub root_state_id: String,
    pub goal: String,
    pub content: String,
    pub budget: WireBudget,
    pub granted_capabilities: BTreeSet<WireCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitParams {
    pub plugin_id: String,
    pub run_id: String,
    pub after_sequence: u64,
    /// 0..=30000; timeout does not cancel the run.
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelParams {
    pub plugin_id: String,
    pub run_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownParams {
    pub plugin_id: String,
    pub grace_period_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum AlgorithmCall {
    #[serde(rename = "plugin.initialize")]
    Initialize(InitializeParams),
    #[serde(rename = "plugin.start")]
    Start(StartParams),
    #[serde(rename = "plugin.wait")]
    Wait(WaitParams),
    #[serde(rename = "plugin.cancel")]
    Cancel(CancelParams),
    #[serde(rename = "plugin.shutdown")]
    Shutdown(ShutdownParams),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmRunStatus {
    Accepted,
    Running,
    Cancelling,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartResult {
    pub run_id: String,
    pub status: Accepted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmCancelStatus {
    Cancelling,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelResult {
    pub run_id: String,
    pub status: AlgorithmCancelStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownStatus {
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownResult {
    pub plugin_id: String,
    pub status: ShutdownStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlgorithmStatus {
    pub run_id: String,
    pub status: AlgorithmRunStatus,
    pub sequence: u64,
    pub summary: String,
    /// Algorithm-owned completion payload, mapped to the shared RunResult by
    /// run management. Existing candidate/apply_plan fields remain intact.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<Failure>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub findings: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_input: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlgorithmEvent {
    pub plugin_id: String,
    pub run_id: String,
    pub sequence: u64,
    /// Existing progress/completion/failure fields, interpreted by the selected
    /// algorithm bridge. Common persisted events use event::RunEvent instead.
    #[serde(flatten)]
    pub payload: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum AlgorithmNotification {
    #[serde(rename = "plugin.started")]
    Started(AlgorithmEvent),
    #[serde(rename = "plugin.progress")]
    Progress(AlgorithmEvent),
    #[serde(rename = "plugin.completed")]
    Completed(AlgorithmEvent),
    #[serde(rename = "plugin.cancelled")]
    Cancelled(AlgorithmEvent),
    #[serde(rename = "plugin.failed")]
    Failed(AlgorithmEvent),
}
