//! Opt-in real Pi tests, using only a local mock provider. Run with --ignored.
#[allow(dead_code)]
mod support;
use agent_runtime_protocol::{AgentRuntime, RuntimeEvent, RuntimeStartRequest, RuntimeTurnRequest};
use axum::{routing::post, Router};
use serde_json::json;
use session_protocol::LlmOverrideRequest;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

fn model(base: &str) -> LlmOverrideRequest {
    serde_json::from_value(json!({"provider":"test-provider","model":"test-model","api_base":base,
        "api":"openai-completions","api_key":"test-secret",
        "capabilities":{"reasoning":false,"input":["text"],"context_window":8192,"max_tokens":1024}})).unwrap()
}
fn ext() -> session_protocol::SessionExtensions {
    [("runtime_pi".into(),json!({"backend_id":"local","executable":std::env::var("PI_TEST_EXECUTABLE").unwrap_or_else(|_|"pi".into()),
        "discovery":{"extensions":false,"skills":false,"context_files":false,"prompt_templates":false,"themes":false}}))].into_iter().collect()
}

#[tokio::test]
#[ignore = "requires Pi CLI"]
async fn real_model_probe_start_switch_checkpoint_and_failed_switch() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let router = Router::new().route("/v1/chat/completions",post(move |axum::Json(body): axum::Json<serde_json::Value>| { let count=count.clone(); async move {
        assert!(!body.to_string().contains("UNWANTED_CONTEXT_MARKER"));
        assert!(!body.to_string().contains("UNWANTED_SKILL_MARKER"));
        count.fetch_add(1,Ordering::SeqCst);
        ([ ("content-type","text/event-stream") ], concat!(
            "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"OK\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n",
            "data: [DONE]\n\n"))
    }}));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let workspace = tempfile::TempDir::new().unwrap();
    let root = workspace.path().to_str().unwrap();
    std::fs::write(
        workspace.path().join("AGENTS.md"),
        "UNWANTED_CONTEXT_MARKER",
    )
    .unwrap();
    let skill = workspace.path().join(".pi/skills/unwanted");
    std::fs::create_dir_all(&skill).unwrap();
    std::fs::write(
        skill.join("SKILL.md"),
        "---\nname: unwanted\ndescription: UNWANTED_SKILL_MARKER\n---\nUNWANTED_SKILL_MARKER",
    )
    .unwrap();
    let runtime = support::new_pi_runtime();
    let start = RuntimeStartRequest {
        runtime_id: "real-model".into(),
        conversation_id: "real".into(),
        sender_id: "test".into(),
        workspace: support::workspace_facts(root),
        state: None,
        llm: Some(model(&base)),
        ext: ext(),
    };
    runtime.probe_llm(&start).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    runtime
        .start(start, support::runtime_context(root).await)
        .await
        .unwrap();
    let state = runtime.export_state("real-model").await.unwrap();
    let dir = std::path::Path::new(state.state["pi_session_dir"].as_str().unwrap());
    let models: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join(".pi-agent/models.json")).unwrap()).unwrap();
    assert_eq!(
        models["providers"]["test-provider"]["models"][0]["contextWindow"],
        8192
    );
    assert!(!models.to_string().contains("test-secret"));
    let turn = |llm| RuntimeTurnRequest {
        runtime_id: "real-model".into(),
        turn_id: uuid::Uuid::new_v4().to_string(),
        text: "Reply OK".into(),
        entry: support::no_entry(),
        llm,
        reasoning_effort: None,
        ext: Default::default(),
    };
    let mut changed = model(&base);
    changed.capabilities.as_mut().unwrap().max_tokens = Some(512);
    let mut events = runtime.submit_turn(turn(Some(changed))).await.unwrap();
    let completed = tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(event) = events.recv().await {
            match event {
                RuntimeEvent::Completed { .. } => return true,
                RuntimeEvent::Failed { .. } => return false,
                _ => {}
            }
        }
        false
    })
    .await
    .unwrap();
    assert!(completed);
    let checkpoint = runtime.export_checkpoint_state("real-model").await.unwrap();
    let before = calls.load(Ordering::SeqCst);
    let bad: LlmOverrideRequest = serde_json::from_value(
        json!({"provider":"openai","model":"gpt-4o","api":"anthropic-messages"}),
    )
    .unwrap();
    let mut events = runtime.submit_turn(turn(Some(bad))).await.unwrap();
    let failed = tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(event) = events.recv().await {
            match event {
                RuntimeEvent::Failed { .. } => return true,
                RuntimeEvent::Completed { .. } => return false,
                _ => {}
            }
        }
        false
    })
    .await
    .unwrap();
    assert!(failed, "invalid switch must fail before prompting");
    assert_eq!(calls.load(Ordering::SeqCst), before);
    runtime.stop("real-model").await.unwrap();
    for id in ["real-fork", "real-restore"] {
        runtime
            .start(
                RuntimeStartRequest {
                    runtime_id: id.into(),
                    conversation_id: id.into(),
                    sender_id: "test".into(),
                    workspace: support::workspace_facts(root),
                    state: Some(checkpoint.clone()),
                    llm: None,
                    ext: ext(),
                },
                support::runtime_context(root).await,
            )
            .await
            .unwrap();
        let state = runtime.export_state(id).await.unwrap();
        let dir = std::path::Path::new(state.state["pi_session_dir"].as_str().unwrap());
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.join(".xgovernor-llm.json")).unwrap())
                .unwrap();
        assert_eq!(saved["api"], "openai-completions");
        assert_eq!(saved["capabilities"]["max_tokens"], 512);
        runtime.stop(id).await.unwrap();
    }
    server.abort();
}

#[tokio::test]
#[ignore = "requires Pi CLI"]
async fn rejected_probe_allocates_no_sandbox_and_redacts_provider_errors() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().fallback(|| async {
                (axum::http::StatusCode::UNAUTHORIZED, "test-secret expired")
            }),
        )
        .await
        .unwrap()
    });
    let workspace = tempfile::TempDir::new().unwrap();
    let managers = support::build_managers();
    let manager = managers["local"].clone();
    let app = support::application_with_runtime(
        support::new_pi_runtime(),
        managers,
        Arc::new(support::MemoryRepository::default()),
        workspace.path().to_str().unwrap().into(),
    );
    let request = serde_json::from_value(
        json!({"conversation_id":"test","sender_id":"test","llm":model(&base),"ext":ext()}),
    )
    .unwrap();
    let error = app
        .open(&xgovernor_core::SecurityContext::admin("test"), request)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("authentication"), "{error}");
    assert!(!error.to_string().contains("test-secret"));
    assert_eq!(manager.global_active_count(), 0);
    server.abort();
}
