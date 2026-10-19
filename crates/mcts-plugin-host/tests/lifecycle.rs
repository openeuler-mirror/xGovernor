use mcts_plugin_host::{manifest::PLUGIN_ID, PluginManifest, PluginSession};
use mcts_protocol::MethodName;
use serde_json::json;
use tokio::process::Command;

const MANIFEST_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/plugin_manifest.json"
);

#[test]
fn loads_mcts_manifest_fixture() {
    let manifest = PluginManifest::load(MANIFEST_FIXTURE).unwrap();
    assert_eq!(manifest.plugin_id, PLUGIN_ID);
    assert!(manifest.mode("zsoft").unwrap().profiles.contains_key("solver-step"));
    assert!(manifest.mode("sweverified").unwrap().profiles.contains_key("author-step"));
}

#[tokio::test]
async fn completes_initialize_start_wait_shutdown() {
    let manifest = PluginManifest::load(MANIFEST_FIXTURE).unwrap();
    let test_binary = std::env::current_exe().expect("current test binary");
    let stub_binary = test_binary
        .parent()
        .expect("target/deps directory")
        .parent()
        .expect("target directory")
        .join("stub-plugin");
    assert!(stub_binary.exists(), "stub-plugin binary was not built");
    let process = mcts_plugin_host::PluginProcess::spawn(Command::new(stub_binary), &[])
    .await
    .unwrap();
    let session = PluginSession::new(process);

    let initialize = session
        .initialize(json!({
            "plugin_id": "mcts",
            "supported_api_versions": ["mcts.rpc.v1"],
            "host_version": "xgovernor-dev",
            "available_capabilities": []
        }))
        .await
        .unwrap();
    assert!(initialize.result.is_some());

    let start = session
        .start(json!({
            "plugin_id": "mcts",
            "run_id": "run-day2",
            "session_id": "session-day2",
            "turn_id": "turn-day2",
            "root_state_id": "state-day2",
            "goal": "test",
            "content": "test",
            "budget": {"max_nodes": 1, "max_time_ms": 1000, "max_tokens": 1000},
            "granted_capabilities": []
        }))
        .await
        .unwrap();
    assert!(start.result.is_some());

    let wait = session
        .wait(json!({
            "plugin_id": "mcts",
            "run_id": "run-day2",
            "after_sequence": 0,
            "timeout_ms": 1000
        }))
        .await
        .unwrap();
    assert!(wait.result.is_some());

    let _ = session
        .request(
            "lifecycle-shutdown",
            MethodName::PluginShutdown,
            json!({"plugin_id": "mcts"}),
        )
        .await
        .unwrap();

    let _ = manifest;
}
