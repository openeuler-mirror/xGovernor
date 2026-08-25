use crate::{RuntimeError, RuntimeEvent, RuntimeStateSnapshot};
use serde::{Deserialize, Serialize};

/// Internal worker response envelope shared by runtime worker supervisors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkerResponse {
    Ready,
    Event { event: RuntimeEvent },
    State { state: RuntimeStateSnapshot },
    Error { error: RuntimeError },
}
