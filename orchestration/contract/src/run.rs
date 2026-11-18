//! Shared application-layer messages for CLI, MCP and optional session callers.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::execution::{
    Capability, Failure, ForkRequest, Isolation, MutationContext, NetworkPolicy,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct BudgetLimits {
    /// Explicit approved limits; zero is a zero allowance, not an environment
    /// variable fallback. Stop admitting work at the limit and record tail usage.
    pub max_tokens: u64,
    pub max_time_ms: u64,
    pub max_cost_microunits: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    Thinkprocess,
    E2b,
    Docker,
    Worktree,
}

/// Auto chooses the first configured backend in ThinkProcess, E2B, Docker,
/// worktree order. Validate that selection; rejection never falls through.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(tag = "selection", content = "backend", rename_all = "snake_case")]
pub enum BackendSelection {
    Auto,
    Explicit(BackendKind),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct ExecutionRequirements {
    pub capabilities: BTreeSet<Capability>,
    pub isolation: Isolation,
    pub joint_snapshots: bool,
    pub independent_branches: bool,
    #[serde(default)]
    pub can_reset_history: bool,
}

/// Payload belongs to this namespace and schema, never to the common protocol.
/// For example, mcts max_iters and branching live only in `parameters` here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct AlgorithmConfig {
    pub namespace: String,
    pub version: String,
    pub schema: String,
    pub parameters: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct RunConfig {
    pub budget: BudgetLimits,
    /// Effective task default or explicit override, persisted with the run.
    /// SWE-bench defaults offline; DeepSWE defaults allow internet.
    pub network_policy: NetworkPolicy,
    pub backend: BackendSelection,
    pub requirements: ExecutionRequirements,
    pub algorithm: AlgorithmConfig,
}

impl RunConfig {
    /// Construct every host-initiated branch, including scoring and official
    /// evaluation, from this persisted effective configuration. Task defaults
    /// and user overrides must already have been resolved before persistence.
    pub fn fork_request(
        &self,
        context: MutationContext,
        parent_state_id: String,
        inherit_history: bool,
    ) -> ForkRequest {
        ForkRequest {
            context,
            parent_state_id,
            network_policy: self.network_policy,
            inherit_history,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CodeSource {
    Task {
        task: String,
        reference: String,
    },
    Snapshot {
        state_id: String,
    },
    WorkspacePackage {
        digest: String,
        manifest_ref: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct WorkspaceApproval {
    pub digest: String,
    pub manifest_ref: String,
    pub backend: BackendKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct StartApproval {
    /// Must exactly match the requested limits, checked by the start endpoint.
    pub budget: BudgetLimits,
    /// Required for package upload, bound to digest, manifest and selected target.
    pub workspace: Option<WorkspaceApproval>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct SourceSession {
    pub session_id: String,
    pub turn_id: Option<String>,
}

/// Persist before returning Accepted; packaging/provisioning runs in background.
/// Same start_key and same request returns the same run; changed request conflicts.
/// Validate budget approval and any workspace approval in the endpoint itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct StartRequest {
    pub start_key: String,
    pub goal: String,
    pub content: String,
    pub code_source: CodeSource,
    pub config: RunConfig,
    pub approval: StartApproval,
    pub source_session: Option<SourceSession>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Accepted,
    Running,
    Cancelling,
    Completed,
    Cancelled,
    Failed,
    /// Process disappeared. No crash resume in the first implementation.
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupStatus {
    Pending,
    Complete,
    /// Unconfirmed mutations/resources remain tracked by their identities.
    Unknown,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartResponse {
    pub run_id: String,
    pub status: RunStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusRequest {
    pub run_id: String,
    /// Return only events strictly after this run-local sequence.
    pub after_sequence: u64,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSummary {
    pub run_id: String,
    pub status: RunStatus,
    pub summary: String,
    pub backend: Option<BackendKind>,
    pub cleanup: CleanupStatus,
    pub source_session: Option<SourceSession>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusResponse {
    pub run: RunSummary,
    pub events: Vec<crate::event::RunEvent>,
    /// Last sequence returned, or the requested cursor if no events were returned.
    pub next_sequence: u64,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRequest {
    pub run_id: String,
    pub reason: String,
}

/// Repeatable. effective means Cancelled is durable and execution stopped.
/// A request that is merely accepted returns effective=false/Cancelling.
/// A previously recorded terminal wins; cancellation never overwrites it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelResponse {
    pub run_id: String,
    pub status: RunStatus,
    pub effective: bool,
    pub cleanup: CleanupStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultRequest {
    pub run_id: String,
}

/// Opaque, run-scoped reference resolved through controlled artifact access.
/// It is neither an arbitrary host path nor permission to fetch an external URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub artifact_id: String,
    pub kind: String,
    pub media_type: String,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Continuation {
    pub agent: String,
    pub reference: ArtifactRef,
}

/// Summary and controlled references, not unbounded logs or automatic patch apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunResult {
    pub summary: String,
    pub answer: String,
    pub stop_reason: String,
    pub selected_candidate_id: Option<String>,
    pub artifacts: Vec<ArtifactRef>,
    pub continuation: Option<Continuation>,
    pub limitations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultResponse {
    pub run: RunSummary,
    pub result: Option<RunResult>,
    pub failure: Option<Failure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListRequest {
    pub source_session_id: Option<String>,
    pub cursor: Option<String>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListResponse {
    pub runs: Vec<RunSummary>,
    pub next_cursor: Option<String>,
}

/// Shared application rejection, serialized identically by CLI, MCP and session
/// callers. A rejected start has no run_id and never creates execution resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunErrorCode {
    InvalidRequest,
    CapabilityMismatch,
    BackendUnavailable,
    BudgetApprovalMismatch,
    WorkspaceApprovalMismatch,
    WorkspaceUnsupported,
    StartKeyConflict,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct RunError {
    pub code: RunErrorCode,
    pub message: String,
}

impl RunError {
    pub fn new(code: RunErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
    pub fn json(&self) -> String {
        serde_json::to_string(self).expect("run error serializes")
    }
}
