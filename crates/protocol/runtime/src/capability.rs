use serde::{Deserialize, Serialize};

/// Runtime features understood by the governor independently of any concrete
/// agent implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeCapability {
    Interaction,
    Steering,
    StateExport,
    ModelOverride,
    ReasoningControl,
}
