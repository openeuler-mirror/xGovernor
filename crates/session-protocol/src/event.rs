use crate::SessionExtensions;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionToolActivityPhase {
    Begin,
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionToolActivityStatus {
    Running,
    Succeeded,
    Failed,
    Denied,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionInteractionOption {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionTurnOutcome {
    Complete,
    MaxTurns,
    BudgetExhausted,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SessionUsage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
}

/// Failure reported after a turn was accepted. This is distinct from
/// [`crate::SessionWireError`], which describes rejection at the HTTP boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionTurnFailure {
    /// Stable machine-readable code owned by the runtime adapter/domain layer.
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default)]
    pub details: Value,
}

/// Well-known shape for the `"file_change"` key inside a
/// [`SessionEvent::ToolActivity`] event's `ext` bag, when the tool call
/// mutated a file. Not interpreted by this crate — `SessionEvent` still
/// carries `ext` as opaque JSON — this type exists purely so that runtime
/// adapters populating the key and clients reading it agree on a shape
/// without either side needing to know the tool's own argument schema
/// (e.g. `file_edit`'s `old_string`/`new_string` vs. `file_write`'s
/// `content`). The delta itself is computed by whoever has access to the
/// actual file bytes at write time (see `operation-protocol`'s
/// `FileChangeDelta`); this struct is just its wire representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionFileChangeExt {
    pub path: String,
    pub additions: u32,
    pub deletions: u32,
}

/// The complete normalized SSE event vocabulary. `Extension` is the sole
/// runtime-specific escape hatch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SessionEvent {
    OutputDelta {
        runtime_id: String,
        turn_id: String,
        stream_id: String,
        sequence: u64,
        delta: String,
    },
    ToolActivity {
        runtime_id: String,
        turn_id: String,
        activity_id: String,
        phase: SessionToolActivityPhase,
        name: String,
        status: SessionToolActivityStatus,
        #[serde(default)]
        summary: Option<String>,
        /// Runtime-specific tool metadata such as agent scope, argument/output
        /// previews, policy detail, file changes, or subagent information.
        #[serde(default)]
        ext: SessionExtensions,
    },
    InteractionRequested {
        runtime_id: String,
        turn_id: String,
        interaction_id: String,
        interaction_kind: String,
        prompt: String,
        /// Temporary wire-compatibility field. Runtime adapters do not model
        /// this value and the server currently always emits `false`.
        #[serde(default)]
        sensitive: bool,
        #[serde(default)]
        options: Vec<SessionInteractionOption>,
        #[serde(default)]
        ext: SessionExtensions,
    },
    TurnCompleted {
        runtime_id: String,
        turn_id: String,
        outcome: SessionTurnOutcome,
        #[serde(default)]
        usage: SessionUsage,
    },
    TurnFailed {
        runtime_id: String,
        turn_id: String,
        error: SessionTurnFailure,
        #[serde(default)]
        usage: SessionUsage,
    },
    Extension {
        runtime_id: String,
        #[serde(default)]
        turn_id: Option<String>,
        namespace: String,
        payload: Value,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_json_shape_is_stable() {
        let event = SessionEvent::OutputDelta {
            runtime_id: "runtime-1".into(),
            turn_id: "turn-1".into(),
            stream_id: "assistant".into(),
            sequence: 3,
            delta: "hi".into(),
        };
        assert_eq!(
            serde_json::to_value(event).unwrap(),
            json!({
                "kind": "output_delta", "runtime_id": "runtime-1", "turn_id": "turn-1",
                "stream_id": "assistant", "sequence": 3, "delta": "hi"
            })
        );
    }

    #[test]
    fn tool_activity_preserves_denied_status_and_namespaced_metadata() {
        let event = SessionEvent::ToolActivity {
            runtime_id: "runtime-1".into(),
            turn_id: "turn-1".into(),
            activity_id: "call-1".into(),
            phase: SessionToolActivityPhase::End,
            name: "exec".into(),
            status: SessionToolActivityStatus::Denied,
            summary: Some("policy denied".into()),
            ext: [("xiaoo".into(), json!({"detail": "blocked"}))]
                .into_iter()
                .collect(),
        };
        let wire = serde_json::to_value(event).unwrap();
        assert_eq!(wire["status"], "denied");
        assert_eq!(wire["ext"]["xiaoo"]["detail"], "blocked");
    }

    #[test]
    fn tool_activity_carries_file_change_ext_under_well_known_key() {
        let file_change = SessionFileChangeExt {
            path: "src/lib.rs".into(),
            additions: 3,
            deletions: 1,
        };
        let event = SessionEvent::ToolActivity {
            runtime_id: "runtime-1".into(),
            turn_id: "turn-1".into(),
            activity_id: "call-2".into(),
            phase: SessionToolActivityPhase::End,
            name: "file_edit".into(),
            status: SessionToolActivityStatus::Succeeded,
            summary: None,
            ext: [(
                "file_change".to_string(),
                serde_json::to_value(&file_change).unwrap(),
            )]
            .into_iter()
            .collect(),
        };
        let wire = serde_json::to_value(&event).unwrap();
        assert_eq!(wire["ext"]["file_change"]["path"], "src/lib.rs");
        assert_eq!(wire["ext"]["file_change"]["additions"], 3);
        assert_eq!(wire["ext"]["file_change"]["deletions"], 1);

        // Round-trips back to the typed struct for a reader that knows the
        // convention, without `SessionEvent` itself needing to know it.
        let SessionEvent::ToolActivity { ext, .. } = event else {
            unreachable!()
        };
        let round_tripped: SessionFileChangeExt =
            serde_json::from_value(ext["file_change"].clone()).unwrap();
        assert_eq!(round_tripped, file_change);
    }

    #[test]
    fn interaction_requested_defaults_sensitive_to_false() {
        let event = SessionEvent::InteractionRequested {
            runtime_id: "runtime-1".into(),
            turn_id: "turn-1".into(),
            interaction_id: "interaction-1".into(),
            interaction_kind: "text_input".into(),
            prompt: "API key".into(),
            sensitive: false,
            options: Vec::new(),
            ext: Default::default(),
        };
        let wire = serde_json::to_value(event).unwrap();
        assert_eq!(wire["sensitive"], false);
    }

    #[test]
    fn interaction_requested_accepts_legacy_wire_without_sensitive() {
        let event: SessionEvent = serde_json::from_value(json!({
            "kind": "interaction_requested",
            "runtime_id": "runtime-1",
            "turn_id": "turn-1",
            "interaction_id": "interaction-1",
            "interaction_kind": "text_input",
            "prompt": "input"
        }))
        .unwrap();
        match event {
            SessionEvent::InteractionRequested { sensitive, .. } => assert!(!sensitive),
            _ => panic!("expected interaction_requested"),
        }
    }
}
