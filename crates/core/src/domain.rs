use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use thiserror::Error;

pub use agent_runtime_protocol::{
    RuntimeCapability, RuntimeStateSnapshot as OpaqueRuntimeState,
    RuntimeWorkspace as WorkspaceFacts, RuntimeWorkspaceAccess as WorkspaceAccess,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Opening,
    Idle,
    Running,
    Paused,
    Failed,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxCapability {
    Exec,
    FileRead,
    FileWrite,
    Pause,
    Snapshot,
    Network,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub checkpoint_id: String,
    pub source_runtime_id: String,
    pub provider_snapshot_id: String,
    pub runtime_state: OpaqueRuntimeState,
    pub workspace: WorkspaceFacts,
    pub isolation: IsolationFacts,
    pub capabilities: EffectiveCapabilities,
    pub owner_ref: String,
    pub tenant_id: Option<String>,
    pub created_by: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveCapabilities {
    #[serde(default)]
    pub sandbox: BTreeSet<SandboxCapability>,
    #[serde(default)]
    pub runtime: BTreeSet<RuntimeCapability>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationBoundary {
    Host,
    Process,
    Container,
    VirtualMachine,
    Remote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkIsolation {
    None,
    Restricted,
    Isolated,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IsolationFacts {
    pub boundary: IsolationBoundary,
    pub workspace_access: WorkspaceAccess,
    pub network: NetworkIsolation,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedLlm {
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub api_base: Option<String>,
    pub credential_source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionLease {
    pub client_id: String,
    #[serde(default)]
    pub client_pid: Option<u32>,
    #[serde(default)]
    pub client_hostname: Option<String>,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointLineage {
    #[serde(default)]
    pub parent_runtime_id: Option<String>,
    #[serde(default)]
    pub source_checkpoint_id: Option<String>,
}

/// Persistent domain record. HTTP handlers must project it through
/// `projection::project_session` and must never serialize it directly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionRecord {
    pub runtime_id: String,
    pub conversation_id: String,
    pub sender_id: String,
    pub status: SessionStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub workspace: WorkspaceFacts,
    pub isolation: IsolationFacts,
    pub capabilities: EffectiveCapabilities,
    pub runtime: OpaqueRuntimeState,
    #[serde(default)]
    pub llm: Option<ResolvedLlm>,
    #[serde(default)]
    pub lease: Option<SessionLease>,
    #[serde(default)]
    pub lineage: Option<CheckpointLineage>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Error)]
pub enum SessionDomainError {
    #[error("invalid request: {message}")]
    InvalidRequest { message: String },
    #[error("session not found: {runtime_id}")]
    NotFound { runtime_id: String },
    #[error("session conflict: {message}")]
    Conflict { message: String },
    #[error("session lease required: {runtime_id}")]
    LeaseRequired { runtime_id: String },
    #[error("session lease conflict: {runtime_id}")]
    LeaseConflict {
        runtime_id: String,
        holder_client_id: Option<String>,
        holder_pid: Option<u32>,
        holder_hostname: Option<String>,
    },
    #[error("unsupported {family} capability: {capability}")]
    UnsupportedCapability {
        family: CapabilityFamily,
        capability: String,
    },
    #[error("operation {operation} timed out after {timeout_ms} ms")]
    Timeout { operation: String, timeout_ms: u64 },
    #[error("session unavailable: {message}")]
    Unavailable { message: String },
    #[error("tenant quota exceeded: {scope} (limit {limit})")]
    QuotaExceeded { scope: String, limit: u32 },
    #[error("internal session failure: {message}")]
    Internal {
        message: String,
        #[source]
        source: Option<Box<SessionDomainError>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityFamily {
    Sandbox,
    Runtime,
}

impl std::fmt::Display for CapabilityFamily {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sandbox => formatter.write_str("sandbox"),
            Self::Runtime => formatter.write_str("runtime"),
        }
    }
}
