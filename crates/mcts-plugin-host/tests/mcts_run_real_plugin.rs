use mcts_plugin_host::{build_launch_command, FakeApplication, HostCapService, MctsRun};
use mcts_plugin_host::{MctsRunRequest};
use std::sync::Arc;

#[tokio::test]
async fn drives_real_mcts_mock_process_over_stdio() {
    let Ok(repo_root) = std::env::var("MCTS_REPO_ROOT") else {
        eprintln!("SKIP: MCTS_REPO_ROOT is not set");
        return;
    };
    let Ok(python_bin) = std::env::var("MCTS_PYTHON_BIN") else {
        eprintln!("SKIP: MCTS_PYTHON_BIN is not set");
        return;
    };

    let request: MctsRunRequest = serde_json::from_value(serde_json::json!({
        "plugin_id": "mcts",
        "mode": "mock",
        "run_id": format!("run-day4-real-{}", std::process::id()),
        "root_state_id": "state-root",
        "goal": "integration smoke test",
        "content": "drive the real MCTS stdio plugin",
        "budget": {
            "max_iters": 2,
            "branching": 1,
            "max_time_ms": 15000,
            "max_tokens": 1000
        }
    }))
    .unwrap();

    let run_map = mcts_plugin_host::RunMap::new();
    let host_service = Arc::new(HostCapService::new(FakeApplication::new(), run_map));
    let command = build_launch_command(python_bin, repo_root, &request);
    let run = MctsRun::new(command, vec![], request, host_service);
    let outcome = run.execute().await.unwrap();

    assert_eq!(
        outcome.status,
        mcts_plugin_host::MctsRunStatus::Completed
    );
    assert!(outcome.result.is_some(), "completed run must carry a result");
}
