use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Runtime-side error taxonomy. The host maps this to its domain error type;
/// the protocol crate intentionally does not depend on xGovernor core.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RuntimeError {
    InvalidRequest { code: String, message: String },
    NotFound { runtime_id: String },
    Conflict { code: String, message: String },
    UnsupportedCapability { capability: String },
    WorkerUnavailable { message: String, retryable: bool },
    StateCorrupt { message: String },
    Internal { message: String },
}

/// A transportable failure emitted by an agent runtime or worker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeFailure {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default)]
    pub details: Value,
}
