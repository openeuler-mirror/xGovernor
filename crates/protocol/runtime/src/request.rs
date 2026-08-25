use crate::RuntimeStateSnapshot;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use session_protocol::{LlmOverrideRequest, SessionExtensions, SessionInteractionAnswer};

/// Runtime-neutral workspace description supplied at process/session start.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeWorkspace {
    pub workspace_id: String,
    pub root: String,
    pub access: RuntimeWorkspaceAccess,
    #[serde(default)]
    pub revision: Option<String>,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeWorkspaceAccess {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeStartRequest {
    pub runtime_id: String,
    pub conversation_id: String,
    pub sender_id: String,
    pub workspace: RuntimeWorkspace,
    #[serde(default)]
    pub state: Option<RuntimeStateSnapshot>,
    #[serde(default)]
    pub llm: Option<LlmOverrideRequest>,
    pub owner_ref: String,
    #[serde(default)]
    pub ext: SessionExtensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeLoadRequest {
    pub new_runtime_id: String,
    pub owner_ref: String,
    pub provider_snapshot_id: String,
    pub runtime_state: RuntimeStateSnapshot,
    #[serde(default)]
    pub llm: Option<LlmOverrideRequest>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEntryContext {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub instance_id: Option<String>,
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub reply_to_message_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeTurnRequest {
    pub runtime_id: String,
    pub turn_id: String,
    pub text: String,
    #[serde(default)]
    pub entry: RuntimeEntryContext,
    #[serde(default)]
    pub llm: Option<LlmOverrideRequest>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub ext: SessionExtensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeInteractionRequest {
    pub runtime_id: String,
    pub turn_id: String,
    pub interaction_id: String,
    pub answer: SessionInteractionAnswer,
    #[serde(default)]
    pub ext: SessionExtensions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCancelRequest {
    pub runtime_id: String,
    #[serde(default)]
    pub turn_id: Option<String>,
}

/// Internal worker command envelope. This is deliberately separate from the
/// public HTTP session API and can be used by Pi, xiaoO, or another worker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkerRequest {
    SubmitTurn(RuntimeTurnRequest),
    AnswerInteraction(RuntimeInteractionRequest),
    Cancel(RuntimeCancelRequest),
    LoadState(RuntimeStateSnapshot),
    Shutdown,
}
