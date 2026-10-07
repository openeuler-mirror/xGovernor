use backend::{
    docker::{DockerConfig, DockerProvider},
    OperationAttach,
};
use operation_protocol::capability::exec::ExecRequest;
use provider_protocol::*;
use serde_json::json;

#[tokio::test]
#[ignore = "requires dedicated Linux Docker Engine and prototype image"]
async fn idle_snapshots_forks_pause_and_reference_cleanup() {
    let state = tempfile::tempdir().unwrap();
    let mut config = DockerConfig::from_env().unwrap();
    config.state_dir = state.path().into();
    config.deployment = format!("snaptest-{}", uuid::Uuid::new_v4().simple());
    config.limits.snapshot_enabled = true;
    config.limits.max_containers = 3;
    config.limits.disk_bytes = 64 * 1024 * 1024;
    let provider = DockerProvider::new(config.clone()).unwrap();
    let create = ProviderCreateRequest {
        backend_id: BackendId("docker".into()),
        owner_ref: "admin".into(),
        reason: ProviderLifecycleReason::Acquire,
        resource_limits: Default::default(),
        provider_options: json!({"workspace_root":"/workspace"}),
        correlation: json!({"request_id":"parent"}),
    };
    let parent = provider.create(create).await.unwrap();
    let backend = provider.attach(&parent).await.unwrap();
    async fn cmd(b: &dyn operation_protocol::OperationBackend, command: &str) -> String {
        let r = b
            .exec()
            .exec(ExecRequest {
                command: command.into(),
                shell: Some("/bin/bash".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            r.exit_code,
            Some(0),
            "{}",
            String::from_utf8_lossy(&r.stderr)
        );
        String::from_utf8(r.stdout).unwrap()
    }
    let snapreq = |id: &str, request: &str| ProviderCheckpointRequest {
        backend_id: BackendId("docker".into()),
        instance_id: ProviderInstanceId(id.into()),
        reason: ProviderLifecycleReason::UserRequested,
        correlation: json!({"request_id":request}),
    };
    let load = |id: ProviderSnapshotId, request: &str| ProviderLoadRequest {
        backend_id: BackendId("docker".into()),
        owner_ref: "admin".into(),
        source: ProviderLoadSource::Snapshot(id),
        reason: ProviderLifecycleReason::Restore,
        resource_limits: Default::default(),
        provider_options: json!({"workspace_root":"/workspace"}),
        correlation: json!({"request_id":request}),
    };
    let delete = |instance: Option<ProviderInstanceId>, snapshot: Option<ProviderSnapshotId>| {
        ProviderDeleteRequest {
            backend_id: BackendId("docker".into()),
            instance_id: instance,
            snapshot_id: snapshot,
            reason: ProviderLifecycleReason::UserRequested,
            correlation: json!(null),
        }
    };
    cmd(backend.as_ref(),"printf before > /workspace/proof; printf config > /home/agent/config; printf '\\000\\377' > /workspace/binary; chmod 700 /workspace/proof; ln -s proof /workspace/link; setsid bash -c 'sleep 5; touch /workspace/leak' >/dev/null 2>&1 &").await;
    backend
        .execution_control()
        .unwrap()
        .begin_turn("busy")
        .await
        .unwrap();
    assert!(provider
        .checkpoint(snapreq(&parent.instance_id.0, "busy"))
        .await
        .is_err());
    backend
        .execution_control()
        .unwrap()
        .finish_turn("busy")
        .await
        .unwrap();
    let snap = provider
        .checkpoint(snapreq(&parent.instance_id.0, "one"))
        .await
        .unwrap();
    let again = provider
        .checkpoint(snapreq(&parent.instance_id.0, "one"))
        .await
        .unwrap();
    assert_eq!(again.snapshot_id, snap.snapshot_id);
    cmd(
        backend.as_ref(),
        "printf after > /workspace/proof; rm /home/agent/config",
    )
    .await;
    let child = provider
        .load(load(snap.snapshot_id.clone(), "child"))
        .await
        .unwrap();
    let child_backend = provider.attach(&child).await.unwrap();
    assert_eq!(
        cmd(
            child_backend.as_ref(),
            "cat /workspace/proof; cat /home/agent/config"
        )
        .await,
        "beforeconfig"
    );
    cmd(child_backend.as_ref(),"test $(stat -c %a /workspace/proof) = 700; test -L /workspace/link; test $(wc -c < /workspace/binary) = 2; printf child > /workspace/proof").await;
    assert_eq!(cmd(backend.as_ref(), "cat /workspace/proof").await, "after");
    provider
        .delete(delete(Some(parent.instance_id.clone()), None))
        .await
        .unwrap();
    let sibling = provider
        .load(load(snap.snapshot_id.clone(), "sibling"))
        .await
        .unwrap();
    let sb = provider.attach(&sibling).await.unwrap();
    assert_eq!(cmd(sb.as_ref(), "cat /workspace/proof").await, "before");
    provider
        .delete(delete(None, Some(snap.snapshot_id.clone())))
        .await
        .unwrap();
    assert!(provider
        .load(load(snap.snapshot_id.clone(), "retired"))
        .await
        .is_err());
    let restored = DockerProvider::new(config.clone()).unwrap();
    let cb = restored.attach(&child).await.unwrap();
    assert_eq!(cmd(cb.as_ref(), "cat /workspace/proof").await, "child");
    let paused = restored
        .pause(ProviderPauseRequest {
            backend_id: BackendId("docker".into()),
            instance_id: child.instance_id.clone(),
            mode: ProviderPauseMode::Snapshot,
            reason: ProviderLifecycleReason::UserRequested,
            correlation: json!({"request_id":"pause"}),
        })
        .await
        .unwrap();
    restored.maintenance().await.unwrap();
    assert!(restored.attach(&child).await.is_err());
    let mut resume = load(paused.snapshot_id.clone(), "resume");
    resume.source = ProviderLoadSource::Instance(child.instance_id.clone());
    restored.load(resume).await.unwrap();
    let cb = restored.attach(&child).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    cmd(cb.as_ref(), "test ! -e /workspace/leak").await;
    restored
        .delete(delete(None, Some(paused.snapshot_id)))
        .await
        .unwrap();
    restored
        .delete(delete(Some(child.instance_id), None))
        .await
        .unwrap();
    restored
        .delete(delete(Some(sibling.instance_id), None))
        .await
        .unwrap();
    for _ in 0..3 {
        restored.maintenance().await.unwrap();
    }
    let images = std::process::Command::new(&config.executable)
        .args([
            "--host",
            &format!("unix://{}", config.socket),
            "image",
            "ls",
            "-q",
            "--filter",
            &format!("label=io.xgovernor.docker-prototype={}", config.deployment),
        ])
        .output()
        .unwrap();
    assert!(images.status.success());
    assert!(
        images.stdout.is_empty(),
        "unreferenced snapshot images must be collected"
    );
}
