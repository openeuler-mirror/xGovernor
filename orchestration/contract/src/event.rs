//! Persistent run events. Persist before publication; deduplicate producer events
//! by (run_id, source, source_sequence), then assign the run-local sequence.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::run::{ArtifactRef, CleanupStatus, RunStatus};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventSource {
    /// Stable producer instance ID, not just its component name.
    pub source: String,
    pub source_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunEvent {
    pub run_id: String,
    pub sequence: u64,
    pub source: EventSource,
    /// RFC 3339 UTC timestamp.
    pub timestamp: String,
    #[serde(flatten)]
    pub event: EventPayload,
}

/// Common payloads allow multiple parents, arbitrary roles and versioned scores.
/// Lifecycle events are emitted only by run management, the terminal authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum EventPayload {
    Lifecycle {
        status: RunStatus,
        summary: String,
        cleanup: CleanupStatus,
    },
    Node {
        node_id: String,
        parents: Vec<String>,
        role: String,
        status: String,
    },
    Score {
        node_id: String,
        /// Finite values; no common [0,1] scale or probability interpretation.
        metrics: BTreeMap<String, f64>,
        evaluation_version: String,
    },
    Selection {
        candidate_ids: Vec<String>,
        reason: String,
    },
    Artifact {
        reference: ArtifactRef,
    },
    Execution {
        method: String,
        branch_id: Option<String>,
        job_id: Option<String>,
        status: String,
    },
    /// Keep unknown extensions intact for generic display and export.
    Extension {
        namespace: String,
        version: String,
        schema: String,
        name: String,
        data: Value,
    },
}
