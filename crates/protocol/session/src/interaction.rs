use crate::{LlmOverrideRequest, SessionExtensions, SessionLeaseClaim};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SessionEntryContext {
    #[serde(default)]
    pub entry_kind: Option<String>,
    #[serde(default)]
    pub instance_id: Option<String>,
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub reply_to_message_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionTurnRequest {
    pub runtime_id: String,
    pub text: String,
    #[serde(default)]
    pub entry: SessionEntryContext,
    #[serde(default)]
    pub llm: Option<LlmOverrideRequest>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    /// Client-chosen idempotency key, unique per session from the client's
    /// point of view. When a turn submission is retried with the same key
    /// (e.g. after a network timeout), the daemon replays the original
    /// receipt — same `turn_id` — instead of starting a second turn. The
    /// daemon compares only the key, not the rest of the payload; reusing a
    /// key with different text is a client bug. `None` opts out (every
    /// submission starts a new turn).
    #[serde(default)]
    pub client_request_id: Option<String>,
    #[serde(default)]
    pub ext: SessionExtensions,
    #[serde(default)]
    pub lease: SessionLeaseClaim,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionInteractionTextAnswer {
    /// Actual value delivered to the runtime. `None` represents no submitted
    /// value without conflating it with an empty string.
    #[serde(default)]
    pub value: Option<String>,
    /// Safe value suitable for display or transcript projection. Sensitive
    /// answers can use a redacted value such as `<SECRET>`.
    #[serde(default)]
    pub display_value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum SessionInteractionAnswer {
    Text(SessionInteractionTextAnswer),
    Selection(Vec<String>),
    Confirm(bool),
    Data(Value),
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionInteractionRequest {
    pub runtime_id: String,
    /// Turn that emitted the corresponding interaction request.
    pub turn_id: String,
    pub interaction_id: String,
    pub answer: SessionInteractionAnswer,
    #[serde(default)]
    pub ext: SessionExtensions,
    #[serde(default)]
    pub lease: SessionLeaseClaim,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionAcceptedInputKind {
    Turn,
    Interaction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSubmitReceipt {
    pub runtime_id: String,
    /// Server-assigned correlation id. The exact same value is carried by
    /// every SSE event produced while handling this accepted input.
    pub turn_id: String,
    pub accepted_kind: SessionAcceptedInputKind,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn namespaced_extensions_are_opaque_but_unknown_core_fields_are_rejected() {
        let value = json!({
            "runtime_id": "runtime-1",
            "text": "hello",
            "ext": {"xiaoo": {"skill_roots": ["/skills"], "anything": true}}
        });
        let request: SessionTurnRequest = serde_json::from_value(value).unwrap();
        assert_eq!(request.ext["xiaoo"]["anything"], true);

        let unknown = json!({"runtime_id": "runtime-1", "text": "hello", "skills": []});
        assert!(serde_json::from_value::<SessionTurnRequest>(unknown).is_err());
    }

    #[test]
    fn llm_descriptor_shape_cannot_echo_a_secret() {
        let request: SessionTurnRequest = serde_json::from_value(json!({
            "runtime_id": "runtime-1",
            "text": "hello",
            "llm": {"provider": "openai", "model": "gpt", "api_key": "secret"}
        }))
        .unwrap();
        assert_eq!(request.llm.unwrap().api_key.as_deref(), Some("secret"));

        let descriptor = crate::ResolvedLlmDescriptor {
            provider: "openai".into(),
            model: "gpt".into(),
            api_base: None,
            credential_source: "request".into(),
        };
        assert!(serde_json::to_value(descriptor)
            .unwrap()
            .get("api_key")
            .is_none());
    }

    #[test]
    fn text_answer_separates_value_from_display_value() {
        let answer = SessionInteractionAnswer::Text(SessionInteractionTextAnswer {
            value: Some("secret".into()),
            display_value: Some("<SECRET>".into()),
        });
        let wire = serde_json::to_value(answer).unwrap();
        assert_eq!(wire["kind"], "text");
        assert_eq!(wire["value"]["value"], "secret");
        assert_eq!(wire["value"]["display_value"], "<SECRET>");
    }
}
