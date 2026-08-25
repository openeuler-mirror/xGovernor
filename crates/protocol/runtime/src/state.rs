use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Runtime-owned state quarantined behind a runtime kind and schema version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeStateSnapshot {
    pub runtime_kind: String,
    pub schema_version: u32,
    #[serde(default)]
    pub state: Value,
}

/// State returned after coordinating a runtime and its optional provider
/// snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointPayload {
    pub checkpoint_id: String,
    pub runtime_state: RuntimeStateSnapshot,
    pub provider_snapshot_id: String,
}
