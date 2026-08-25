use crate::RuntimeFailure;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use session_protocol::{
    SessionExtensions, SessionInteractionOption, SessionToolActivityPhase,
    SessionToolActivityStatus, SessionTurnOutcome, SessionUsage,
};

/// Events produced by every agent runtime. A turn must end with exactly one
/// `Completed` or `Failed` event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RuntimeEvent {
    OutputDelta {
        stream_id: String,
        sequence: u64,
        delta: String,
    },
    ToolActivity {
        activity_id: String,
        phase: SessionToolActivityPhase,
        name: String,
        status: SessionToolActivityStatus,
        summary: Option<String>,
        ext: SessionExtensions,
    },
    InteractionRequested {
        interaction_id: String,
        interaction_kind: String,
        prompt: String,
        options: Vec<SessionInteractionOption>,
        ext: SessionExtensions,
    },
    Completed {
        outcome: SessionTurnOutcome,
        usage: SessionUsage,
    },
    Failed {
        error: RuntimeFailure,
        usage: SessionUsage,
    },
    Extension {
        namespace: String,
        payload: Value,
    },
    /// A newer worker event that this host does not understand yet.
    #[serde(other)]
    Unknown,
}

impl RuntimeEvent {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed { .. } | Self::Failed { .. })
    }
}
