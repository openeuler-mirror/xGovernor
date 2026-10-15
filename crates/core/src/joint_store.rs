//! Durable admission and publication for joint execution owners.
use crate::{JointBinding, JointCheckpointRecord, JointOperationContext, SessionRecord};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeEffect {
    Create,
    Mutate,
    Remove,
    ObserveAbsence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointNativeObject {
    pub kind: String,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointNativeIntent {
    pub step: String,
    pub operation_id: String,
    pub digest: String,
    pub method: String,
    pub params: Value,
    pub effect: NativeEffect,
    #[serde(default, skip_serializing_if = "is_false")]
    pub compensation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeOperationState {
    NotSent,
    Accepted,
    Unknown,
    Completed,
    Failed,
    NeedsRecovery,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointNativeOutcome {
    pub state: NativeOperationState,
    pub resources: Vec<JointNativeObject>,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointResourceReservation {
    pub kind: String,
    pub units: u32,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointPrepareRequest {
    pub context: JointOperationContext,
    pub binding: JointBinding,
    pub root_id: String,
    pub source_id: String,
    pub exclusive_root: bool,
    pub parent_binding: Option<JointBinding>,
    pub reservations: Vec<JointResourceReservation>,
    pub native: Vec<JointNativeIntent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_input: Option<SessionRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn: Option<JointTurnIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointAdmission {
    pub intent_id: String,
    pub binding: JointBinding,
    pub generation: u64,
    pub root_epoch: u64,
    pub source_epoch: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointPendingOperation {
    pub request: JointPrepareRequest,
    pub admission: JointAdmission,
    pub stage: String,
    pub native: Vec<(JointNativeIntent, Option<JointNativeOutcome>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JointPublication {
    Session { record: SessionRecord },
    Checkpoint { record: JointCheckpointRecord },
}

/// Durable ownership lookup; native identities stay behind the application boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointBindingState {
    pub binding: JointBinding,
    pub generation: u64,
    pub stage: String,
    pub creation: JointPrepareRequest,
    pub objects: Vec<JointNativeObject>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Public correlation retained before the first native Message admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointTurnIdentity {
    pub turn_id: String,
    pub client_request_id: Option<String>,
    pub input_digest: String,
}

/// A complete native terminal, observed together with a quiescent owned Session.
/// Metering is intentionally absent; a Message reply does not establish token usage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointTurnTerminal {
    pub message_id: Option<String>,
    pub reply: Option<String>,
    pub failure: Option<String>,
    pub completed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointTurnRecord {
    pub runtime_id: String,
    pub identity: JointTurnIdentity,
    pub admission: JointAdmission,
    pub terminal: Option<JointTurnTerminal>,
}
