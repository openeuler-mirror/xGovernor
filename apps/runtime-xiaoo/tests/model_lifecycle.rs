//! Uses the actual xiaoO worker and local HTTP endpoints, no paid API calls.
use agent_runtime_protocol::{
    AgentRuntime, RuntimeEvent, RuntimeExecutionContext, RuntimeStartRequest, RuntimeTurnRequest,
};
use serde_json::{json, Value};
use xgovernor_runtime_xiaoo::XiaooRuntime;
#[path = "../src/test_support.rs"]
mod support;
fn turn(id: &str, value: Value) -> RuntimeTurnRequest {
    let mut request =
        json!({"runtime_id":id,"turn_id":uuid::Uuid::new_v4().to_string(),"text":"hello"});
    for (key, value) in value.as_object().unwrap() {
        request[key] = value.clone();
    }
    serde_json::from_value(request).unwrap()
}
async fn run(runtime: &XiaooRuntime, request: RuntimeTurnRequest) {
    let mut events = runtime.submit_turn(request).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while let Some(event) = events.recv().await {
            match event {
                RuntimeEvent::Completed { .. } => return,
                RuntimeEvent::Failed { error, .. } => panic!("turn failed: {error:?}"),
                _ => {}
            }
        }
        panic!("missing terminal event");
    })
    .await
    .unwrap();
}
async fn endpoint() -> (String, tokio::task::JoinHandle<(String, Value)>) {
    let response = json!({"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"done"}]}],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}});
    support::mock(
        200,
        "text/event-stream",
        format!(
            "data: {}\n\n",
            json!({"type":"response.completed","response":response})
        ),
    )
    .await
}
#[tokio::test]
async fn real_worker_switch_checkpoint_restore_fork_and_rejection() {
    std::env::set_var("XGOVERNOR_XIAOO_WORKER", env!("CARGO_BIN_EXE_xiaoo-worker"));
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("AGENTS.md"), "UNWANTED_CONTEXT_MARKER").unwrap();
    let skill = root.path().join(".xiaoo/skills/unwanted");
    std::fs::create_dir_all(&skill).unwrap();
    std::fs::write(skill.join("SKILL.md"), "UNWANTED_SKILL_MARKER").unwrap();
    let backend = backend::local::local_backend(root.path().into(), None, None, None).unwrap();
    let runtime = XiaooRuntime::new();
    let start:RuntimeStartRequest=serde_json::from_value(json!({
        "runtime_id":"parent","conversation_id":"test","sender_id":"test",
        "workspace":{"workspace_id":"test","root":root.path(),"access":"read_write"},
        "llm":{"provider":"openai","model":"initial","api_key_env":"PATH"},
        "ext":{"xiaoo":{"backend_id":"local","tool_policy":{"allow":["file_read","grep"]},"discovery":{"extensions":false,"skills":false,"prompt_templates":false,"themes":false,"context_files":false}}}
    })).unwrap();
    runtime
        .start(
            start.clone(),
            RuntimeExecutionContext {
                operation_backend: backend.clone(),
            },
        )
        .await
        .unwrap();
    let (base, server) = endpoint().await;
    run(&runtime,turn("parent",json!({"llm":{"model":"switched","api":"openai-responses","api_base":base,"capabilities":{"reasoning":false,"input":["text"],"context_window":128000,"max_tokens":321}},"ext":{"xiaoo":{"tool_policy":{"deny":["grep"]}}}}))).await;
    let (_, request) = server.await.unwrap();
    assert!(!request.to_string().contains("UNWANTED_CONTEXT_MARKER"));
    assert!(!request.to_string().contains("UNWANTED_SKILL_MARKER"));
    assert_eq!(request["model"], "switched");
    assert_eq!(request["max_output_tokens"], 321);
    assert_eq!(request["tools"].as_array().unwrap().len(), 1);
    assert_eq!(request["tools"][0]["name"], "file_read");
    let frozen = runtime.export_state("parent").await.unwrap();
    let descriptor = runtime.describe_llm("parent").await.unwrap().unwrap();
    assert_eq!(descriptor.model, "switched");
    assert_eq!(
        descriptor.capabilities.as_ref().unwrap().max_tokens,
        Some(321)
    );
    assert_eq!(frozen.state["role_settings"]["discovery"]["skills"], false);
    for invalid in [
        json!({"llm":{"api_key_env":"XGOVERNOR_TEST_UNSET_CREDENTIAL"}}),
        json!({"llm":{"capabilities":{"context_window":1}}}),
        json!({"ext":{"xiaoo":{"discovery":{"skills":false}}}}),
    ] {
        assert!(runtime.submit_turn(turn("parent", invalid)).await.is_err());
        assert_eq!(
            runtime.describe_llm("parent").await.unwrap(),
            Some(descriptor.clone())
        );
        assert!(runtime.check_alive("parent").await.unwrap());
    }
    let mut child = start.clone();
    child.runtime_id = "child".into();
    child.llm = None;
    child.state = Some(frozen.clone());
    runtime
        .start(
            child,
            RuntimeExecutionContext {
                operation_backend: backend.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        runtime.describe_llm("child").await.unwrap(),
        Some(descriptor.clone())
    );
    let (base, server) = endpoint().await;
    run(
        &runtime,
        turn(
            "child",
            json!({"llm":{"api_base":base},"ext":{"xiaoo":{"tool_policy":{"allow":[]}}}}),
        ),
    )
    .await;
    let (_, request) = server.await.unwrap();
    assert!(request.get("tools").is_none());
    assert_eq!(request["model"], "switched");
    assert_eq!(runtime.export_state("parent").await.unwrap(), frozen);
    runtime.load_state("child", frozen.clone()).await.unwrap();
    assert_eq!(
        runtime.describe_llm("child").await.unwrap(),
        Some(descriptor)
    );
    let (base, server) = endpoint().await;
    run(&runtime, turn("child", json!({"llm":{"api_base":base}}))).await;
    let (_, request) = server.await.unwrap();
    assert_eq!(request["tools"][0]["name"], "file_read");
    runtime.stop("child").await.unwrap();
    runtime.stop("parent").await.unwrap();
    // Old snapshots have no policy, protocol, capabilities or Responses cache.
    let mut old = frozen;
    old.state["llm"].as_object_mut().unwrap().remove("api");
    old.state["llm"]
        .as_object_mut()
        .unwrap()
        .remove("capabilities");
    old.state.as_object_mut().unwrap().remove("responses_state");
    old.state.as_object_mut().unwrap().remove("role_settings");
    let mut restored = start;
    restored.runtime_id = "legacy".into();
    restored.llm = None;
    restored.state = Some(old);
    runtime
        .start(
            restored,
            RuntimeExecutionContext {
                operation_backend: backend,
            },
        )
        .await
        .unwrap();
    let state = runtime.export_state("legacy").await.unwrap();
    assert_eq!(state.state["role_settings"]["tools_enabled"], true);
    runtime.stop("legacy").await.unwrap();
}

struct Ids;
impl xgovernor_core::RuntimeIdGenerator for Ids {
    fn next_runtime_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
}
impl xgovernor_core::TurnIdGenerator for Ids {
    fn next_turn_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
}
impl xgovernor_core::Clock for Ids {
    fn now_ms(&self) -> u64 {
        1
    }
}

#[tokio::test]
async fn rejected_preflight_never_allocates_a_sandbox() {
    use std::sync::Arc;
    use xgovernor_core::{SecurityContext, SessionApplication, SqliteSessionRepository};
    use xgovernor_manager::{InstanceManager, InstanceManagerConfig};
    let root = tempfile::tempdir().unwrap();
    let provider = Arc::new(backend::local::LocalProvider::new());
    let manager = Arc::new(InstanceManager::new(
        provider.clone(),
        provider,
        Arc::new(backend::SqliteProviderInstanceLedger::open_in_memory().unwrap()),
        provider_protocol::ProviderKind("local".into()),
        InstanceManagerConfig::new(20, 1024),
    ));
    let app = SessionApplication::new(
        Arc::new(XiaooRuntime::new()),
        [("local".into(), manager.clone())].into_iter().collect(),
        Arc::new(SqliteSessionRepository::open_in_memory().unwrap()),
        Arc::new(Ids),
        Arc::new(Ids),
        Arc::new(xgovernor_runtime_xiaoo::XiaooSessionEnvironment::new(
            root.path().to_str().unwrap(),
            ["local".into()],
        )),
        Arc::new(Ids),
    );
    for api in [
        "openai-completions",
        "openai-responses",
        "anthropic-messages",
    ] {
        let (base, server) = support::mock(
            401,
            "application/json",
            json!({"error":{"message":"synthetic-secret expired"}}).to_string(),
        )
        .await;
        let request=serde_json::from_value(json!({"runtime_kind":"xiaoo","conversation_id":"test","sender_id":"test",
            "llm":{"provider":"gateway","model":"model","api":api,"api_base":base,"api_key_env":"PATH"},"ext":{"xiaoo":{"backend_id":"local"}}})).unwrap();
        let error = app
            .open(&SecurityContext::admin("test"), request)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("authentication"), "{error}");
        assert!(!error.contains("synthetic-secret"));
        assert_eq!(manager.global_active_count(), 0);
        server.await.unwrap();
    }
}
