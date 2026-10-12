#![cfg(unix)]

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::Value;
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
        std::fs::write(&host, include_bytes!("support/checkpoint_host.py")).unwrap();
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
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}
fn restore() -> sdk::ThinkThreadRestoreRequest {
    sdk::ThinkThreadRestoreRequest {
        runtime_thinkthread_id: "fixture-owning-root".into(),
        thinkthread_id: "fixture-source".into(),
        checkpoint_id: "fixture-checkpoint".into(),
        if_content_revision: 41.into(),
        continuation: Some(sdk::RestoreContinuation {
            text: "continue exactly once".into(),
        }),
    }
}

#[tokio::test]
async fn accepted_capture_requires_domain_query_before_a_committed_proof() {
    let f = Fixture::new("ok");
    let c = f.config.connect().await.unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let response = c
        .checkpoints()
        .create_with_operation(
            id.clone(),
            sdk::CheckpointCreateRequest {
                runtime_thinkthread_id: "fixture-owning-root".into(),
                thinkthread_id: "fixture-source".into(),
                label: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(response.operation.operation_id, id);
    assert_eq!(
        response.request.state,
        sdk::CheckpointRequestState::Accepted
    );
    assert!(response.request.result.is_none());
    let status = c
        .checkpoints()
        .get_request(sdk::CheckpointRequestGetRequest {
            runtime_thinkthread_id: "fixture-owning-root".into(),
            request_id: response.request.request_id.clone(),
        })
        .await
        .unwrap();
    assert_eq!(status.request.state, sdk::CheckpointRequestState::Succeeded);
    assert!(matches!(
        status.request.result,
        Some(sdk::CheckpointMainResult::Created { .. })
    ));
    let record = c
        .checkpoints()
        .get(sdk::CheckpointTargetRequest {
            runtime_thinkthread_id: "fixture-owning-root".into(),
            checkpoint_id: "fixture-checkpoint".into(),
        })
        .await
        .unwrap();
    assert_eq!(record.boundary, sdk::CheckpointBoundary::Terminal);
    assert_eq!(record.source_thinkthread_id, "fixture-source");
    assert_eq!(record.source_content_revision.0, 41);
    assert_eq!(record.session_artifact_bytes.0, 8192);
    assert_eq!(record.session_artifact_digest, "a".repeat(64));
    assert_eq!(record.fs_snapshot_id, "fixture-fs");
    c.close().await.unwrap();
    let trace = f.trace();
    let mutation = trace
        .iter()
        .find(|v| v["method"] == "checkpoint.create")
        .unwrap();
    assert_eq!(
        mutation["params"]["runtimeThinkthreadId"],
        "fixture-owning-root"
    );
    assert_eq!(mutation["params"]["thinkthreadId"], "fixture-source");
}

#[tokio::test]
async fn restore_revision_and_continuation_remain_exact_on_wire() {
    let f = Fixture::new("ok");
    let c = f.config.connect().await.unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let r = c
        .thinkthreads()
        .restore_with_operation(id.clone(), restore())
        .await
        .unwrap();
    assert_eq!(r.operation.operation_id, id);
    c.close().await.unwrap();
    let trace = f.trace();
    let row = trace
        .iter()
        .find(|v| v["method"] == "thinkthread.restore")
        .unwrap();
    assert_eq!(row["params"]["ifContentRevision"]["kind"], "exact");
    assert_eq!(row["params"]["ifContentRevision"]["value"], "41");
    assert_eq!(
        row["params"]["continuation"]["text"],
        "continue exactly once"
    );
    assert_eq!(row["params"]["checkpointId"], "fixture-checkpoint");
    assert!(!trace
        .iter()
        .any(|v| v["method"] == "thinkthread.restore_current"));
}

#[tokio::test]
async fn failed_start_preserves_the_committed_restore_as_partial_failure() {
    let f = Fixture::new("partial-restore");
    let c = f.config.connect().await.unwrap();
    let r = c.thinkthreads().restore(restore()).await.unwrap();
    assert_eq!(r.request.state, sdk::CheckpointRequestState::Failed);
    assert_eq!(
        r.request.execution_start,
        Some(sdk::CheckpointExecutionStart::Failed)
    );
    assert_eq!(
        r.request.continuation,
        sdk::CheckpointContinuationState::Failed
    );
    assert!(matches!(
        r.request.result,
        Some(sdk::CheckpointMainResult::Restored {
            content_revision: sdk::DecimalU64(42),
            ..
        })
    ));
    assert_eq!(r.request.error.unwrap().code, "fixture_start_failed");
    c.close().await.unwrap();
}

#[tokio::test]
async fn exact_revision_conflict_does_not_fall_back_to_restore_current() {
    let f = Fixture::new("revision-conflict");
    let c = f.config.connect().await.unwrap();
    let error = c.thinkthreads().restore(restore()).await.unwrap_err();
    assert!(
        matches!(error,sdk::ManagementError::Rejected(body) if body.code=="ContentRevisionConflict")
    );
    c.close().await.unwrap();
    let trace = f.trace();
    assert_eq!(
        trace
            .iter()
            .filter(|v| v["method"] == "thinkthread.restore")
            .count(),
        1
    );
    assert!(!trace
        .iter()
        .any(|v| v["method"] == "thinkthread.restore_current"));
}

#[tokio::test]
async fn unknown_restore_reconnects_with_the_same_uuid_params_and_digest() {
    let mut f = Fixture::new("drop-restore");
    let c = f.config.connect().await.unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let error = c
        .thinkthreads()
        .restore_with_operation(id.clone(), restore())
        .await
        .unwrap_err();
    assert_eq!(error.delivery(), Some(sdk::Delivery::SentUnknown));
    let second = c
        .thinkthreads()
        .restore_with_operation(id.clone(), restore())
        .await
        .unwrap_err();
    assert_eq!(second.delivery(), Some(sdk::Delivery::NotSent));
    let _ = c.close().await;
    f.config
        .base_environment
        .insert("XG_TEST_SCENARIO".into(), "ok".into());
    let reconnect = f.config.connect().await.unwrap();
    reconnect
        .thinkthreads()
        .restore_with_operation(id.clone(), restore())
        .await
        .unwrap();
    reconnect.close().await.unwrap();
    let trace = f.trace();
    let requests: Vec<_> = trace
        .iter()
        .filter(|v| v["method"] == "thinkthread.restore")
        .collect();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["params"], requests[1]["params"]);
    assert_eq!(requests[0]["params"]["operationId"], id);
}

#[tokio::test]
async fn checkpoint_delete_uses_the_explicit_owner_and_retained_id() {
    let f = Fixture::new("ok");
    let c = f.config.connect().await.unwrap();
    c.checkpoints()
        .remove(sdk::CheckpointTargetRequest {
            runtime_thinkthread_id: "fixture-owning-root".into(),
            checkpoint_id: "fixture-checkpoint".into(),
        })
        .await
        .unwrap();
    c.close().await.unwrap();
    let trace = f.trace();
    let row = trace
        .iter()
        .find(|v| v["method"] == "checkpoint.remove")
        .unwrap();
    assert_eq!(row["params"]["runtimeThinkthreadId"], "fixture-owning-root");
    assert_eq!(row["params"]["checkpointId"], "fixture-checkpoint");
    assert!(!trace
        .iter()
        .any(|v| v["method"] == "checkpoint.list" || v["method"] == "thinkthread.remove"));
}
