use orchestration_compat::{memory::MemoryBackend, Fixture};
use orchestration_contract::execution::*;

macro_rules! case {
    ($name:ident, $lose_response:expr) => {
        #[tokio::test]
        async fn $name() {
            let backend = MemoryBackend::new($lose_response);
            let fixture = Fixture {
                backend: &backend,
                context: CallContext {
                    plugin_id: "algorithm".into(),
                    run_id: stringify!($name).into(),
                    timeout_ms: 1000,
                },
                root_state_id: "root".into(),
                network_policy: NetworkPolicy::Offline,
                test_path: "compat.txt".into(),
                exec_argv: vec!["complete".into()],
                cancellable_argv: vec!["blocking".into()],
                cwd: ".".into(),
                turn_input: "emit output".into(),
                lease_ttl_ms: 80,
                reclaim_wait_ms: 160,
            };
            orchestration_compat::$name(&fixture).await;
            backend.assert_no_branches();
        }
    };
}

case!(snapshot, false);
case!(fork, false);
case!(agent_turn, false);
case!(cancel_job, false);
case!(inspect, false);
case!(exec, false);
case!(file_read, false);
case!(file_write, false);
case!(diff, false);
case!(release, false);
case!(outcome_unknown, true);

case!(prepare_root, false);
case!(renew_run_lease, false);
case!(reclaim_run, false);
#[tokio::test]
async fn export_workspace() {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let backend = MemoryBackend::new(false);
    let directory = tempfile::tempdir().unwrap();
    let fixture = Fixture {
        backend: &backend,
        context: CallContext {
            plugin_id: "algorithm".into(),
            run_id: "export".into(),
            timeout_ms: 1000,
        },
        root_state_id: "root".into(),
        network_policy: NetworkPolicy::Offline,
        test_path: "new.txt".into(),
        exec_argv: vec![],
        cancellable_argv: vec![],
        cwd: ".".into(),
        turn_input: String::new(),
        lease_ttl_ms: 80,
        reclaim_wait_ms: 160,
    };
    let summary =
        orchestration_compat::export_workspace(&fixture, directory.path().to_str().unwrap()).await;
    let bytes = std::fs::read(directory.path().join("selected.tar")).unwrap();
    assert_eq!(summary.size_bytes, bytes.len() as u64);
    assert_eq!(summary.sha256, format!("{:x}", Sha256::digest(&bytes)));
    let mut archive = tar::Archive::new(bytes.as_slice());
    let mut entries = archive.entries().unwrap();
    let mut entry = entries.next().unwrap().unwrap();
    assert_eq!(entry.path().unwrap().to_str(), Some("new.txt"));
    let mut contents = String::new();
    entry.read_to_string(&mut contents).unwrap();
    assert_eq!(contents, "one");
    assert!(entries.next().is_none());
    backend.assert_no_branches();
}
