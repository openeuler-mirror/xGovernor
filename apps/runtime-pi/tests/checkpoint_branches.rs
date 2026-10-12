//! Pi immutable history and checkpoint credential lifetime regressions.
//! The worker runs fake_pi; history is seeded, with no model or full provider snapshot.
mod support;
use agent_runtime_protocol::{
    AgentRuntime, RuntimeError, RuntimeStartRequest, RuntimeStateSnapshot,
};
use serde_json::json;
use std::path::PathBuf;
use tempfile::TempDir;
use xgovernor_runtime_pi::PiRuntime;

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

fn session_dir(state: &RuntimeStateSnapshot) -> PathBuf {
    state.state["pi_session_dir"].as_str().unwrap().into()
}

async fn start_branch(
    runtime: &PiRuntime,
    id: &str,
    root: &str,
    state: &RuntimeStateSnapshot,
) -> PathBuf {
    runtime
        .start(
            request(id, root, Some(state.clone())),
            support::runtime_context(root).await,
        )
        .await
        .unwrap();
    session_dir(&runtime.export_state(id).await.unwrap())
}

#[tokio::test]
async fn checkpoint_history_and_role_survive_source_removal_and_independent_cold_loads() {
    let workspace = TempDir::new().unwrap();
    let sessions = TempDir::new().unwrap();
    let root = workspace.path().to_str().unwrap();
    let runtime =
        PiRuntime::new_with_worker(sessions.path().into(), support::pi_worker_path()).unwrap();
    let role = json!({
        "system_prompt": "solver at K1",
        "max_turns": 3,
        "tools_enabled": false,
        "tool_policy": {"allow": null, "deny": null},
        "discovery": {
            "extensions": null,
            "skills": null,
            "prompt_templates": null,
            "themes": null,
            "context_files": null
        }
    });
    let mut open = request("source", root, None);
    for (key, value) in role.as_object().unwrap() {
        open.ext.get_mut("runtime_pi").unwrap()[key] = value.clone();
    }
    runtime
        .start(open, support::runtime_context(root).await)
        .await
        .unwrap();
    let source = session_dir(&runtime.export_state("source").await.unwrap());
    let original = history(&format!("K1:{}", "历史🙂".repeat(12_000)));
    assert!(original.len() > 64 * 1024);
    std::fs::write(source.join("session.jsonl"), &original).unwrap();
    let first = runtime.export_checkpoint_state("source").await.unwrap();
    let later = history("K2 has later committed history");
    std::fs::write(source.join("session.jsonl"), &later).unwrap();
    let later_role = json!({
        "system_prompt": "solver at K2",
        "max_turns": 7,
        "tools_enabled": true,
        "tool_policy": {"allow": null, "deny": null},
        "discovery": {
            "extensions": null,
            "skills": null,
            "prompt_templates": null,
            "themes": null,
            "context_files": null
        }
    });
    std::fs::write(source.join("xgovernor-role.json"), later_role.to_string()).unwrap();
    let second = runtime.export_checkpoint_state("source").await.unwrap();
    runtime.stop("source").await.unwrap();
    assert!(source.starts_with(sessions.path()));
    std::fs::remove_dir_all(&source).unwrap();
    drop(runtime);

    let runtime =
        PiRuntime::new_with_worker(sessions.path().into(), support::pi_worker_path()).unwrap();
    let a = start_branch(&runtime, "branch-a", root, &first).await;
    let b = start_branch(&runtime, "branch-b", root, &first).await;
    let later_branch = start_branch(&runtime, "branch-k2", root, &second).await;
    assert_ne!(a, b);
    assert_ne!(a, source);
    assert_ne!(b, source);
    assert_ne!(later_branch, a);
    for dir in [&a, &b] {
        assert_eq!(
            std::fs::read(dir.join("session.jsonl")).unwrap(),
            original.as_bytes()
        );
        let actual: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.join("xgovernor-role.json")).unwrap())
                .unwrap();
        assert_eq!(actual, role);
    }
    assert_eq!(
        std::fs::read(later_branch.join("session.jsonl")).unwrap(),
        later.as_bytes()
    );
    let actual: serde_json::Value =
        serde_json::from_slice(&std::fs::read(later_branch.join("xgovernor-role.json")).unwrap())
            .unwrap();
    assert_eq!(actual, later_role);
    std::fs::write(a.join("session.jsonl"), history("branch A mutated")).unwrap();
    assert_eq!(
        std::fs::read(b.join("session.jsonl")).unwrap(),
        original.as_bytes()
    );
    for id in ["branch-a", "branch-b", "branch-k2"] {
        runtime.stop(id).await.unwrap();
    }
    drop(runtime);
    let runtime =
        PiRuntime::new_with_worker(sessions.path().into(), support::pi_worker_path()).unwrap();
    let cold = start_branch(&runtime, "cold", root, &first).await;
    assert_ne!(cold, a);
    assert_ne!(cold, b);
    assert_eq!(
        std::fs::read(cold.join("session.jsonl")).unwrap(),
        original.as_bytes()
    );
    runtime.stop("cold").await.unwrap();
}

#[tokio::test]
async fn live_state_cannot_create_a_branch_and_incomplete_history_cannot_be_checkpointed() {
    let workspace = TempDir::new().unwrap();
    let sessions = TempDir::new().unwrap();
    let root = workspace.path().to_str().unwrap();
    let runtime =
        PiRuntime::new_with_worker(sessions.path().into(), support::pi_worker_path()).unwrap();
    runtime
        .start(
            request("source", root, None),
            support::runtime_context(root).await,
        )
        .await
        .unwrap();
    let state = runtime.export_state("source").await.unwrap();
    let source = session_dir(&state);
    assert!(matches!(
        runtime
            .start(
                request("foreign-branch", root, Some(state)),
                support::runtime_context(root).await
            )
            .await,
        Err(RuntimeError::InvalidRequest { .. })
    ));
    assert!(matches!(
        runtime.export_state("foreign-branch").await,
        Err(RuntimeError::NotFound { .. })
    ));
    let incomplete = format!(
        "{}\n",
        json!({"type":"message","message":{"role":"assistant","content":"unfinished","stopReason":"pending"}})
    );
    std::fs::write(source.join("session.jsonl"), &incomplete).unwrap();
    assert!(matches!(
        runtime.export_checkpoint_state("source").await,
        Err(RuntimeError::StateCorrupt { .. })
    ));
    assert_eq!(
        std::fs::read_to_string(source.join("session.jsonl")).unwrap(),
        incomplete
    );
    assert!(runtime.export_state("source").await.is_ok());
    runtime.stop("source").await.unwrap();
}

#[tokio::test]
async fn inline_checkpoint_credential_survives_source_removal_until_explicit_delete() {
    let workspace = TempDir::new().unwrap();
    let sessions = TempDir::new().unwrap();
    let root = workspace.path().to_str().unwrap();
    let runtime =
        PiRuntime::new_with_worker(sessions.path().into(), support::pi_worker_path()).unwrap();
    let mut open = request("source", root, None);
    open.llm = Some(session_protocol::LlmOverrideRequest {
        provider: Some("openai".into()),
        model: Some("checkpoint-test-model".into()),
        api_base: None,
        api: None,
        capabilities: None,
        api_key_env: None,
        api_key: Some("synthetic-checkpoint-key".into()),
    });
    runtime
        .start(open, support::runtime_context(root).await)
        .await
        .unwrap();
    let source = session_dir(&runtime.export_state("source").await.unwrap());
    std::fs::write(source.join("session.jsonl"), history("committed")).unwrap();
    let checkpoint = runtime.export_checkpoint_state("source").await.unwrap();
    assert!(!serde_json::to_string(&checkpoint)
        .unwrap()
        .contains("synthetic-checkpoint-key"));
    let reference = checkpoint.state["checkpoint"]["llm_config_ref"]
        .as_str()
        .unwrap();
    let sidecar = sessions
        .path()
        .join(".checkpoint-credentials")
        .join(reference);
    assert!(sidecar.is_file());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&sidecar).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(sidecar.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }
    runtime.stop("source").await.unwrap();
    assert!(source.starts_with(sessions.path()));
    std::fs::remove_dir_all(source).unwrap();
    drop(runtime);
    let runtime =
        PiRuntime::new_with_worker(sessions.path().into(), support::pi_worker_path()).unwrap();
    let branch = start_branch(&runtime, "branch", root, &checkpoint).await;
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(branch.join(".xgovernor-llm.json")).unwrap())
            .unwrap();
    assert_eq!(config["apiKey"], "synthetic-checkpoint-key");
    assert_eq!(config["model"], "checkpoint-test-model");
    runtime.delete_checkpoint_state(&checkpoint).await.unwrap();
    runtime.delete_checkpoint_state(&checkpoint).await.unwrap();
    assert!(!sidecar.exists());
    assert!(runtime.export_state("branch").await.is_ok());
    let before = std::fs::read_dir(sessions.path()).unwrap().count();
    assert!(runtime
        .start(
            request("deleted-checkpoint-load", root, Some(checkpoint)),
            support::runtime_context(root).await
        )
        .await
        .is_err());
    assert_eq!(
        std::fs::read_dir(sessions.path()).unwrap().count(),
        before,
        "failed load must clean its new branch directory"
    );
    assert!(runtime
        .export_state("deleted-checkpoint-load")
        .await
        .is_err());
    runtime.stop("branch").await.unwrap();
}
