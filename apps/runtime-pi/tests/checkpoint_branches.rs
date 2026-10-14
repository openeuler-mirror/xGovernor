mod support;
use agent_runtime_protocol::RuntimeStartRequest;
use serde_json::json;

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
async fn policies_survive_checkpoint_fork_and_restore() {
    use agent_runtime_protocol::AgentRuntime;
    let workspace = tempfile::TempDir::new().unwrap();
    let root = workspace.path().to_str().unwrap();
    let runtime = support::new_pi_runtime();
    let mut start = request("policy-source", root, None);
    start.ext.get_mut("runtime_pi").unwrap()["tool_policy"] =
        json!({"allow":["read","grep"],"deny":["grep"]});
    start.ext.get_mut("runtime_pi").unwrap()["discovery"] =
        json!({"extensions":false,"skills":false,"context_files":false});
    runtime
        .start(start, support::runtime_context(root).await)
        .await
        .unwrap();
    let state = runtime.export_state("policy-source").await.unwrap();
    let dir = std::path::Path::new(state.state["pi_session_dir"].as_str().unwrap());
    std::fs::write(dir.join("history.jsonl"), history("source")).unwrap();
    let checkpoint = runtime
        .export_checkpoint_state("policy-source")
        .await
        .unwrap();
    assert_eq!(
        checkpoint.state["checkpoint"]["role"]["discovery"]["skills"],
        false
    );
    for id in ["policy-fork", "policy-restore"] {
        runtime
            .start(
                request(id, root, Some(checkpoint.clone())),
                support::runtime_context(root).await,
            )
            .await
            .unwrap();
        let branch = runtime.export_state(id).await.unwrap();
        let branch_dir = std::path::Path::new(branch.state["pi_session_dir"].as_str().unwrap());
        let role: serde_json::Value =
            serde_json::from_slice(&std::fs::read(branch_dir.join("xgovernor-role.json")).unwrap())
                .unwrap();
        assert_eq!(role, checkpoint.state["checkpoint"]["role"]);
        let args = std::fs::read_to_string(branch_dir.join("fake_pi_launch_args.json")).unwrap();
        assert!(
            args.contains("--no-extensions")
                && args.contains("--no-skills")
                && args.contains("--no-context-files")
        );
        runtime.stop(id).await.unwrap();
    }
    runtime.stop("policy-source").await.unwrap();
}
