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
