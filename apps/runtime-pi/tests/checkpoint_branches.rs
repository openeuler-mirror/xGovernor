mod support;
use agent_runtime_protocol::RuntimeStartRequest;
use serde_json::{json};

fn request(
    id: &str,
    root: &str,
    state: Option<agent_runtime_protocol::RuntimeStateSnapshot>,
) -> RuntimeStartRequest {
    RuntimeStartRequest {
        runtime_id: id.into(),
        conversation_id: id.into(),
        sender_id: "test".into(),
        workspace: support::workspace_facts(root),
        state,
        llm: None,
        ext: support::pi_runtime_ext(),
    }
}
fn history(text: &str) -> String {
    format!(
        "{}\n",
        json!({"type":"message","message":{"role":"assistant","content":text,"stopReason":"stop"}})
    )
}
