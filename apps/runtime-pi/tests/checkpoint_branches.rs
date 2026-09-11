mod support;
use agent_runtime_protocol::{AgentRuntime, RuntimeStartRequest, RuntimeTurnRequest};
use serde_json::{json, Value};
use std::path::Path;

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

#[tokio::test]
async fn immutable_checkpoint_survives_parent_changes_and_close_and_gives_independent_branches() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_str().unwrap();
    let runtime = support::new_pi_runtime();
    runtime
        .start(
            request("parent", root, None),
            support::runtime_context(root).await,
        )
        .await
        .unwrap();
    let parent = runtime.export_state("parent").await.unwrap();
    let parent_dir = Path::new(parent.state["pi_session_dir"].as_str().unwrap());
    std::fs::write(parent_dir.join("original.jsonl"), history("before")).unwrap();
    let frozen = runtime.export_checkpoint_state("parent").await.unwrap();
    std::fs::write(parent_dir.join("original.jsonl"), history("after")).unwrap();
    runtime.stop("parent").await.unwrap();
    // The immutable state carries the data itself, no dependency on source files.
    std::fs::remove_dir_all(parent_dir).unwrap();
    for id in ["left", "right"] {
        runtime
            .start(
                request(id, root, Some(frozen.clone())),
                support::runtime_context(root).await,
            )
            .await
            .unwrap();
    }
    let left = runtime.export_state("left").await.unwrap();
    let right = runtime.export_state("right").await.unwrap();
    assert!(
        left.state.get("checkpoint").is_none(),
        "durable child state must reference its own live history"
    );
    let a = Path::new(left.state["pi_session_dir"].as_str().unwrap());
    let b = Path::new(right.state["pi_session_dir"].as_str().unwrap());
    assert_ne!(a, b);
    assert_eq!(
        std::fs::read_to_string(a.join("session.jsonl")).unwrap(),
        history("before")
    );
    std::fs::write(a.join("session.jsonl"), history("left only")).unwrap();
    assert_eq!(
        std::fs::read_to_string(b.join("session.jsonl")).unwrap(),
        history("before")
    );
    runtime.stop("left").await.unwrap();
    runtime.stop("right").await.unwrap();
}

#[tokio::test]
async fn role_overrides_are_persisted_and_frozen_with_checkpoint() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_str().unwrap();
    let runtime = support::new_pi_runtime();
    let mut start = request("role", root, None);
    start.ext.get_mut("runtime_pi").unwrap()["system_prompt"] = json!("author");
    start.ext.get_mut("runtime_pi").unwrap()["tools_enabled"] = json!(true);
    runtime
        .start(start, support::runtime_context(root).await)
        .await
        .unwrap();
    let mut events = runtime
        .submit_turn(RuntimeTurnRequest {
            runtime_id: "role".into(),
            turn_id: "turn-role".into(),
            text: "hello".into(),
            entry: Default::default(),
            llm: None,
            reasoning_effort: None,
            ext: [(
                "runtime_pi".into(),
                json!({"system_prompt":"selector","max_turns":2,"tools_enabled":false}),
            )]
            .into_iter()
            .collect(),
        })
        .await
        .unwrap();
    while events.recv().await.is_some() {}
    let current = runtime.export_state("role").await.unwrap();
    let current_dir = Path::new(current.state["pi_session_dir"].as_str().unwrap());
    std::fs::write(current_dir.join("session.jsonl"), history("role done")).unwrap();
    let frozen = runtime.export_checkpoint_state("role").await.unwrap();
    assert_eq!(
        frozen.state["checkpoint"]["role"]["system_prompt"],
        "selector"
    );
    assert_eq!(frozen.state["checkpoint"]["role"]["tools_enabled"], false);
    assert_eq!(frozen.state["checkpoint"]["role"]["max_turns"], 2);
    runtime.stop("role").await.unwrap();
    runtime
        .start(
            request("loaded-role", root, Some(frozen)),
            support::runtime_context(root).await,
        )
        .await
        .unwrap();
    let state = runtime.export_state("loaded-role").await.unwrap();
    let dir = Path::new(state.state["pi_session_dir"].as_str().unwrap());
    let role: Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("xgovernor-role.json")).unwrap())
            .unwrap();
    assert_eq!(role["system_prompt"], "selector");
    assert_eq!(role["tools_enabled"], false);
    runtime.stop("loaded-role").await.unwrap();
}

#[tokio::test]
async fn credentials_stay_out_of_checkpoint_json_and_are_reclaimed() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_str().unwrap();
    let runtime = support::new_pi_runtime();
    runtime
        .start(
            request("secret", root, None),
            support::runtime_context(root).await,
        )
        .await
        .unwrap();
    let source = runtime.export_state("secret").await.unwrap();
    let dir = Path::new(source.state["pi_session_dir"].as_str().unwrap());
    std::fs::write(dir.join(".xgovernor-llm.json"),json!({"provider":"openai","model":"fake","apiKey":"test-secret-do-not-persist","credentialSource":"request"}).to_string()).unwrap();
    let checkpoint = runtime.export_checkpoint_state("secret").await.unwrap();
    assert!(!serde_json::to_string(&checkpoint)
        .unwrap()
        .contains("test-secret-do-not-persist"));
    let reference = checkpoint.state["checkpoint"]["llm_config_ref"]
        .as_str()
        .unwrap();
    let credential_file = dir
        .parent()
        .unwrap()
        .join(".checkpoint-credentials")
        .join(reference);
    assert!(credential_file.exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&credential_file)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    runtime.stop("secret").await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
    runtime
        .start(
            request("secret-child", root, Some(checkpoint.clone())),
            support::runtime_context(root).await,
        )
        .await
        .unwrap();
    runtime.stop("secret-child").await.unwrap();
    runtime.delete_checkpoint_state(&checkpoint).await.unwrap();
    assert!(!credential_file.exists());
    runtime.delete_checkpoint_state(&checkpoint).await.unwrap();
}

#[tokio::test]
async fn live_state_cannot_be_mistaken_for_a_branch_checkpoint() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_str().unwrap();
    let runtime = support::new_pi_runtime();
    runtime
        .start(
            request("source", root, None),
            support::runtime_context(root).await,
        )
        .await
        .unwrap();
    let live = runtime.export_state("source").await.unwrap();
    assert!(runtime
        .start(
            request("other", root, Some(live)),
            support::runtime_context(root).await
        )
        .await
        .is_err());
    runtime.stop("source").await.unwrap();
}
