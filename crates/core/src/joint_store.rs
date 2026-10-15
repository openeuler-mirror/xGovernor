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
