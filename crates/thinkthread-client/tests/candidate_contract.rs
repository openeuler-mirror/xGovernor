#![cfg(unix)]

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::{json, Value};
use tempfile::TempDir;
use xgovernor_thinkthread_client::{sdk, ManagementConfig};

struct Fixture {
    root: TempDir,
    config: ManagementConfig,
}
impl Fixture {
    fn new(scenario: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let host = root.path().join("host");
        std::fs::write(&host, include_bytes!("support/candidate_host.py")).unwrap();
        std::fs::set_permissions(&host, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = ManagementConfig {
            host_executable: host,
            state_root: root.path().join("state"),
            runtime_root: root.path().join("runtime"),
            config_root: root.path().join("config"),
            cache_root: root.path().join("cache"),
            working_directory: root.path().join("work"),
            fs_root: root.path().join("fs"),
            home_directory: root.path().join("home"),
            executable_search_path: vec![PathBuf::from("/usr/bin")],
            base_environment: BTreeMap::from([
                ("XG_TEST_SCENARIO".into(), scenario.into()),
                (
                    "XG_TEST_TRACE".into(),
                    root.path().join("trace").to_str().unwrap().into(),
                ),
            ]),
            connect_timeout: Duration::from_secs(2),
        };
        Self { root, config }
    }
    fn trace(&self) -> Vec<Value> {
        std::fs::read_to_string(self.root.path().join("trace"))
            .unwrap()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect()
    }
}
fn request() -> sdk::ThinkThreadForkRequest {
    serde_json::from_value(json!({"sourceThinkthreadId":"fixture-source-child"})).unwrap()
}

#[tokio::test]
async fn private_fork_keeps_derivation_source_separate_from_default_lifecycle_parent() {
    let f = Fixture::new("ok");
    let c = f.config.connect().await.unwrap();
    let r = c.thinkthreads().fork(request()).await.unwrap();
    assert_eq!(r.source_thinkthread_id, "fixture-source-child");
    assert_eq!(r.parent_thinkthread_id, "fixture-owning-root");
    assert_ne!(r.thinkthread_id, r.source_thinkthread_id);
    assert_eq!(r.fs, sdk::ThinkThreadForkFs::Private);
    assert_eq!(r.state, "ready");
    assert!(r.fs_branch_id.is_some());
    c.close().await.unwrap();
    let trace = f.trace();
    let p = &trace
        .iter()
        .find(|r| r["method"] == "thinkthread.fork")
        .unwrap()["params"];
    assert_eq!(p["fs"], "private");
    assert!(p.get("parentThinkthreadId").is_none());
}

#[tokio::test]
async fn explicit_parent_is_not_substituted_for_the_state_source() {
    let f = Fixture::new("ok");
    let c = f.config.connect().await.unwrap();
    let mut input = request();
    input.parent_thinkthread_id = Some("fixture-parent-child".into());
    let r = c.thinkthreads().fork(input).await.unwrap();
    assert_eq!(r.source_thinkthread_id, "fixture-source-child");
    assert_eq!(r.parent_thinkthread_id, "fixture-parent-child");
    c.close().await.unwrap();
}

#[tokio::test]
async fn fork_replay_retains_the_exact_uuid_and_private_attachment() {
    let f = Fixture::new("ok");
    let c = f.config.connect().await.unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let first = c
        .thinkthreads()
        .fork_with_operation(id.clone(), request())
        .await
        .unwrap();
    let replay = c
        .thinkthreads()
        .fork_with_operation(id.clone(), request())
        .await
        .unwrap();
    assert_eq!(first, replay);
    c.close().await.unwrap();
    let trace = f.trace();
    let rows: Vec<_> = trace
        .iter()
        .filter(|r| r["method"] == "thinkthread.fork")
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["params"], rows[1]["params"]);
}

#[tokio::test]
async fn inactive_source_rejection_does_not_start_restore_or_share_a_workspace() {
    let f = Fixture::new("source-not-ready");
    let c = f.config.connect().await.unwrap();
    let e = c.thinkthreads().fork(request()).await.unwrap_err();
    assert!(matches!(e,sdk::ManagementError::Rejected(body) if body.code=="SourceNotReady"));
    c.close().await.unwrap();
    let trace = f.trace();
    assert_eq!(
        trace
            .iter()
            .filter(|r| r["method"] == "thinkthread.fork")
            .count(),
        1
    );
    assert!(trace.iter().all(|r| matches!(
        r["method"].as_str().unwrap(),
        "handshake" | "context.bind" | "thinkthread.fork"
    )));
}

#[tokio::test]
async fn cross_runtime_parent_rejection_preserves_the_requested_source_and_parent() {
    let f = Fixture::new("cross-runtime");
    let c = f.config.connect().await.unwrap();
    let mut input = request();
    input.parent_thinkthread_id = Some("other-runtime-root".into());
    let e = c.thinkthreads().fork(input).await.unwrap_err();
    assert!(matches!(e,sdk::ManagementError::Rejected(body) if body.code=="CrossRuntimeParent"));
    c.close().await.unwrap();
    let trace = f.trace();
    let rows: Vec<_> = trace
        .iter()
        .filter(|r| r["method"] == "thinkthread.fork")
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]["params"]["sourceThinkthreadId"],
        "fixture-source-child"
    );
    assert_eq!(
        rows[0]["params"]["parentThinkthreadId"],
        "other-runtime-root"
    );
}

#[tokio::test]
async fn unknown_fork_never_falls_back_to_a_shared_or_new_root_operation() {
    let f = Fixture::new("drop-fork");
    let c = f.config.connect().await.unwrap();
    let e = c.thinkthreads().fork(request()).await.unwrap_err();
    assert_eq!(e.delivery(), Some(sdk::Delivery::SentUnknown));
    let _ = c.close().await;
    let trace = f.trace();
    assert_eq!(
        trace
            .iter()
            .filter(|r| r["method"] == "thinkthread.fork")
            .count(),
        1
    );
    assert!(trace.iter().all(|r| matches!(
        r["method"].as_str().unwrap(),
        "handshake" | "context.bind" | "thinkthread.fork"
    )));
}
