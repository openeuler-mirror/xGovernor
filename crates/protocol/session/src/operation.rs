use crate::SessionLeaseClaim;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionExecRequest {
    pub runtime_id: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub lease: SessionLeaseClaim,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionExecResult {
    pub stdout: String,
    pub stderr: String,
    #[serde(default)]
    pub exit_code: Option<i32>,
    pub timed_out: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionFileReadRequest {
    pub runtime_id: String,
    pub path: String,
    #[serde(default)]
    pub lease: SessionLeaseClaim,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionFileReadResult {
    pub path: String,
    pub content_base64: String,
    #[serde(default)]
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionFileWriteRequest {
    pub runtime_id: String,
    pub path: String,
    pub content_base64: String,
    #[serde(default)]
    pub create_parents: bool,
    #[serde(default)]
    pub lease: SessionLeaseClaim,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionFileWriteResult {
    pub path: String,
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionCheckpointScope {
    Full,
    WorkspaceOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionCheckpointRequest {
    pub runtime_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub requested_scope: Option<SessionCheckpointScope>,
    #[serde(default)]
    pub lease: SessionLeaseClaim,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCheckpointResult {
    pub checkpoint_id: String,
    pub runtime_id: String,
    pub checkpoint_scope: SessionCheckpointScope,
    pub created_at_ms: u64,
}

/// Public metadata for a persisted checkpoint. Provider snapshot identifiers
/// and opaque runtime state remain server-side details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCheckpointSummary {
    pub checkpoint_id: String,
    pub source_runtime_id: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub created_by: Option<String>,
    pub created_at_ms: u64,
}

/// Paginated response for `GET /api/v1/checkpoints`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCheckpointListResponse {
    pub checkpoints: Vec<SessionCheckpointSummary>,
    pub total: u64,
    pub has_more: bool,
    #[serde(default)]
    pub next_offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionCheckoutRequest {
    pub checkpoint_id: String,
    #[serde(default)]
    pub runtime_id: Option<String>,
    #[serde(default)]
    pub lease: SessionLeaseClaim,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionPauseRequest {
    pub runtime_id: String,
    #[serde(default)]
    pub checkpoint_name: Option<String>,
    #[serde(default)]
    pub lease: SessionLeaseClaim,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionResumeRequest {
    pub runtime_id: String,
    #[serde(default)]
    pub lease: SessionLeaseClaim,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPauseResult {
    pub runtime_id: String,
    #[serde(default)]
    pub checkpoint: Option<SessionCheckpointResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionCheckpointDeleteRequest {
    pub checkpoint_id: String,
    #[serde(default)]
    pub lease: SessionLeaseClaim,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCheckpointDeleteResult {
    pub checkpoint_id: String,
    pub deleted: bool,
}
