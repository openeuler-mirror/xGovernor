use serde_json::{json, Value};
use std::process::Command;

#[test]
fn queries_read_committed_records_during_worker_write_without_backend_assembly() {
    let root = tempfile::tempdir().unwrap();
    let runs = root.path().join("runs");
    orchestration_run_manager::RunManager::open(&runs, vec![]).unwrap();
    let db = rusqlite::Connection::open(runs.join("runs.sqlite")).unwrap();
    let record = json!({
        "request": {"start_key":"key","goal":"goal","content":"", "code_source":{"kind":"snapshot","state_id":"state"},
            "config":{"budget":{"max_tokens":1,"max_time_ms":1000},"network_policy":"offline","backend":{"selection":"auto"},
                "requirements":{"capabilities":[],"isolation":"none","joint_snapshots":false,"independent_branches":false},
                "algorithm":{"namespace":"mcts","version":"1","schema":"v1","parameters":{}}},
            "approval":{"budget":{"max_tokens":1,"max_time_ms":1000}}},
        "summary":{"run_id":"run","status":"completed","summary":"done","backend":"e2b","cleanup":"complete"},
        "owner":null,"accepted_ms":0,"started_ms":null,"usage":{"tokens":0,"cost_microunits":0},"result":null
    });
    db.execute(
        "INSERT INTO runs VALUES('run','key',?1)",
        [record.to_string()],
    )
    .unwrap();
    let config = root.path().join("config.json");
    std::fs::write(
        &config,
        json!({"runs_dir":runs,"algorithms_dir":"missing","models":{},
        "backends":[{"kind":"e2b","driver":"unavailable"}]})
        .to_string(),
    )
    .unwrap();
    db.execute_batch("BEGIN IMMEDIATE; UPDATE runs SET start_key='uncommitted';")
        .unwrap();
    for args in [vec!["status", "run"], vec!["result", "run"], vec!["list"]] {
        let output = Command::new(env!("CARGO_BIN_EXE_mcts"))
            .arg("--config")
            .arg(&config)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(
            value.get("run").unwrap_or(&value["runs"][0])["status"],
            "completed"
        );
    }
    assert!(
        !runs.join("run/state.json").exists(),
        "queries must not write projections"
    );
    db.execute_batch("ROLLBACK").unwrap();
}
