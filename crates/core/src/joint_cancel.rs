//! Control requests retain the original turn admission instead of competing for its fence.
use crate::{JointAdmission, JointNativeIntent, JointNativeOutcome};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointCancelControl {
    pub admission: JointAdmission,
    pub native: Option<JointNativeIntent>,
    pub outcome: Option<JointNativeOutcome>,
}

/// Independent of per-operation dirty tracking; ordinary completion cannot clear it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointWorkspaceBoundary {
    pub binding: crate::JointBinding,
    pub generation: u64,
    pub cancel_epoch: u64,
    pub checkpoint_epoch: u64,
}
impl JointWorkspaceBoundary {
    pub fn unsettled(&self) -> bool {
        self.cancel_epoch > self.checkpoint_epoch
    }
}

/// Registered before an external SDK checkpoint request. This is an observation
/// ticket, not a public candidate checkpoint or a loadable Session resource.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointCheckpointBoundaryObservation {
    pub boundary: JointWorkspaceBoundary,
    pub native: JointNativeIntent,
}
