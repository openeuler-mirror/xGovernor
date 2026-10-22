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
    assert_eq!(e.delivery(), Some(sdk::Delivery::Accepted));
    assert!(matches!(e,sdk::ManagementError::Rejected(body) if body.code=="ThinkThreadNotReady"));
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
    assert_eq!(e.delivery(), Some(sdk::Delivery::Accepted));
    assert!(matches!(e,sdk::ManagementError::Rejected(body) if body.code=="ThinkThreadNotFound"));
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

#[tokio::test]
async fn distinct_fork_operations_publish_distinct_private_identities() {
    let f = Fixture::new("ok");
    let c = f.config.connect().await.unwrap();
    let first = c.thinkthreads().fork(request()).await.unwrap();
    let second = c.thinkthreads().fork(request()).await.unwrap();
    assert_ne!(first.operation.operation_id, second.operation.operation_id);
    assert_ne!(first.thinkthread_id, second.thinkthread_id);
    assert_ne!(first.fs_branch_id, second.fs_branch_id);
    assert_eq!(first.source_thinkthread_id, second.source_thinkthread_id);
    assert_eq!(first.fs, sdk::ThinkThreadForkFs::Private);
    assert_eq!(second.fs, sdk::ThinkThreadForkFs::Private);
    c.close().await.unwrap();
}

#[tokio::test]
async fn completed_fork_replay_after_reconnect_keeps_exact_identity_and_digest() {
    let f = Fixture::new("ok");
    let id = uuid::Uuid::new_v4().to_string();
    let c = f.config.connect().await.unwrap();
    let first = c
        .thinkthreads()
        .fork_with_operation(id.clone(), request())
        .await
        .unwrap();
    c.close().await.unwrap();
    let c = f.config.connect().await.unwrap();
    let replay = c
        .thinkthreads()
        .fork_with_operation(id, request())
        .await
        .unwrap();
    assert_eq!(first, replay);
    c.close().await.unwrap();
    let rows: Vec<_> = f
        .trace()
        .into_iter()
        .filter(|r| r["method"] == "thinkthread.fork")
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["params"], rows[1]["params"]);
}

#[tokio::test]
async fn reused_operation_with_changed_source_is_rejected_without_new_fork() {
    let f = Fixture::new("operation-conflict");
    let c = f.config.connect().await.unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    c.thinkthreads()
        .fork_with_operation(id.clone(), request())
        .await
        .unwrap();
    let mut changed = request();
    changed.source_thinkthread_id = "fixture-other-source".into();
    let e = c
        .thinkthreads()
        .fork_with_operation(id, changed)
        .await
        .unwrap_err();
    assert_eq!(e.delivery(), Some(sdk::Delivery::NotSent));
    assert!(matches!(e, sdk::ManagementError::Rejected(body) if body.code == "OperationConflict"));
    c.close().await.unwrap();
    let rows: Vec<_> = f
        .trace()
        .into_iter()
        .filter(|r| r["method"] == "thinkthread.fork")
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0]["params"]["operationId"],
        rows[1]["params"]["operationId"]
    );
    assert_ne!(
        rows[0]["params"]["operationDigest"],
        rows[1]["params"]["operationDigest"]
    );
    assert_eq!(
        rows[1]["params"]["sourceThinkthreadId"],
        "fixture-other-source"
    );
    assert_eq!(rows[1]["params"]["fs"], "private");
}

#[tokio::test]
async fn reused_operation_with_changed_fs_is_rejected_without_replacing_identity() {
    let f = Fixture::new("operation-conflict");
    let c = f.config.connect().await.unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    c.thinkthreads()
        .fork_with_operation(id.clone(), request())
        .await
        .unwrap();
    let mut changed = request();
    changed.fs = sdk::ThinkThreadForkFs::Shared;
    let e = c
        .thinkthreads()
        .fork_with_operation(id, changed)
        .await
        .unwrap_err();
    assert_eq!(e.delivery(), Some(sdk::Delivery::NotSent));
    assert!(matches!(e, sdk::ManagementError::Rejected(body) if body.code == "OperationConflict"));
    c.close().await.unwrap();
    let rows: Vec<_> = f
        .trace()
        .into_iter()
        .filter(|r| r["method"] == "thinkthread.fork")
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0]["params"]["operationId"],
        rows[1]["params"]["operationId"]
    );
    assert_ne!(
        rows[0]["params"]["operationDigest"],
        rows[1]["params"]["operationDigest"]
    );
    assert_eq!(rows[1]["params"]["fs"], "shared");
}

#[tokio::test]
async fn changed_lifecycle_parent_conflicts_without_losing_original_fork() {
    let f = Fixture::new("operation-conflict");
    let c = f.config.connect().await.unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let original = c
        .thinkthreads()
        .fork_with_operation(id.clone(), request())
        .await
        .unwrap();
    let mut changed = request();
    changed.parent_thinkthread_id = Some("other-runtime-root".into());
    let error = c
        .thinkthreads()
        .fork_with_operation(id.clone(), changed)
        .await
        .unwrap_err();
    assert_eq!(error.delivery(), Some(sdk::Delivery::NotSent));
    assert!(
        matches!(error, sdk::ManagementError::Rejected(body) if body.code == "OperationConflict")
    );
    let replay = c
        .thinkthreads()
        .fork_with_operation(id, request())
        .await
        .unwrap();
    assert_eq!(replay, original);
    c.close().await.unwrap();
    let rows: Vec<_> = f
        .trace()
        .into_iter()
        .filter(|row| row["method"] == "thinkthread.fork")
        .collect();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["params"], rows[2]["params"]);
    assert_eq!(
        rows[1]["params"]["parentThinkthreadId"],
        "other-runtime-root"
    );
    assert_eq!(
        rows[0]["params"]["operationId"],
        rows[1]["params"]["operationId"]
    );
    assert_ne!(
        rows[0]["params"]["operationDigest"],
        rows[1]["params"]["operationDigest"]
    );
}
