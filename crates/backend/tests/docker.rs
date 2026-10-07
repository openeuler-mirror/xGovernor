use backend::execution::OperationExecutionExt;
use backend::{
    docker::{DockerConfig, DockerProvider},
    OperationAttach,
};
use futures::FutureExt;
use operation_protocol::{
    capability::{
        exec::ExecRequest,
        export::ExportFileRequest,
        filesystem::{ReadBytesRequest, WriteBytesRequest, WriteMode},
        filesystem::{TempPathKind, TempPathRequest},
        path::{ResolveBase, ResolvePathRequest},
        search::{GlobRequest, GrepMode, GrepRequest},
    },
    BackendPath,
};
use provider_protocol::*;
use serde_json::json;
use tokio::io::AsyncReadExt;

#[tokio::test]
#[ignore = "requires local Linux Docker and the prototype image; run explicitly"]
async fn docker_lifecycle_operations_and_recovery() {
    let state = tempfile::tempdir().unwrap();
    let mut config = DockerConfig::from_env().unwrap();
    config.state_dir = state.path().into();
    config.deployment = format!("test-{}", uuid::Uuid::new_v4().simple());
    let provider = DockerProvider::new(config.clone()).unwrap();
    provider.preflight().await.unwrap();
    let request = ProviderCreateRequest {
        backend_id: BackendId("docker".into()),
        owner_ref: "admin".into(),
        reason: ProviderLifecycleReason::Acquire,
        resource_limits: Default::default(),
        provider_options: json!({"workspace_root":"/workspace"}),
        correlation: json!({"request_id":"stable-create"}),
    };
    let instance = provider.create(request.clone()).await.unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        assert_eq!(
            provider.create(request.clone()).await?.instance_id,
            instance.instance_id
        );
        assert_eq!(provider.list_instances().await?.len(), 1);
        let mut forbidden = request.clone();
        forbidden.provider_options = json!({"privileged":true});
        assert!(matches!(
            provider.create(forbidden).await,
            Err(ProviderControlError::InvalidRequest { .. })
        ));
        let backend = provider.attach(&instance).await?;
        let path = BackendPath("/workspace/bytes ; ' \".bin".into());
        backend
            .files()
            .write_bytes(WriteBytesRequest {
                path: path.clone(),
                content: vec![0, 255, 10],
                mode: WriteMode::AtomicOverwrite,
            })
            .await?;
        assert_eq!(
            backend
                .files()
                .read_bytes(ReadBytesRequest { path: path.clone() })
                .await?,
            vec![0, 255, 10]
        );
        let exec = backend
            .exec()
            .exec(ExecRequest {
                command: "printf out; printf err >&2; exit 7".into(),
                shell: Some("/bin/bash".into()),
                ..Default::default()
            })
            .await?;
        assert_eq!(exec.stdout, b"out");
        assert_eq!(exec.stderr, b"err");
        assert_eq!(exec.exit_code, Some(7));
        let text = BackendPath("/workspace/search.txt".into());
        backend
            .files()
            .write_bytes(WriteBytesRequest {
                path: text.clone(),
                content: b"alpha\nbeta\n".to_vec(),
                mode: WriteMode::Create,
            })
            .await?;
        let matches = backend
            .search()
            .glob(GlobRequest {
                pattern: "*.txt".into(),
                base_dir: None,
                limit: Some(10),
            })
            .await?;
        assert!(matches.contains(&text));
        let matches = backend
            .search()
            .grep(GrepRequest {
                query: "alpha".into(),
                base_dir: BackendPath("/workspace".into()),
                include: Some("*.txt".into()),
                mode: GrepMode::Content,
                head_limit: Some(10),
            })
            .await?;
        assert!(matches.entries.iter().any(|line| line.contains("alpha")));
        let resolved = backend
            .paths()
            .resolve_path(ResolvePathRequest {
                raw_path: "./search.txt".into(),
                base: ResolveBase::WorkspaceRoot,
            })
            .await?;
        assert_eq!(resolved, text);
        backend
            .files()
            .create_dir_all(&BackendPath("/workspace/sub/dir".into()))
            .await?;
        let temp = backend
            .files()
            .temp_path(TempPathRequest {
                kind: TempPathKind::File,
                preferred_parent: Some(BackendPath("/workspace/sub/dir".into())),
                prefix: None,
                suffix: None,
            })
            .await?;
        assert!(backend.files().stat(&temp).await?.exists);
        let export = backend
            .export()
            .export_file(ExportFileRequest {
                path: text,
                preferred_name: Some("proof.txt".into()),
            })
            .await?;
        let mut content = vec![];
        export.open_read().await?.read_to_end(&mut content).await?;
        assert_eq!(content, b"alpha\nbeta\n");
        let mut offline = config.clone();
        offline.socket = format!("/tmp/xg-no-daemon-{}.sock", uuid::Uuid::new_v4());
        assert!(matches!(
            DockerProvider::new(offline)?.attach(&instance).await,
            Err(ProviderControlError::Transport { .. })
        ));
        let mut missing_image = config.clone();
        missing_image.image = format!("xg-nonexistent-{}:test", uuid::Uuid::new_v4());
        assert!(DockerProvider::new(missing_image)?
            .preflight()
            .await
            .is_err());
        let timed = backend
            .exec()
            .exec(ExecRequest {
                command: "(sleep 1; touch /workspace/leaked) & wait".into(),
                shell: Some("/bin/bash".into()),
                timeout_ms: Some(100),
                ..Default::default()
            })
            .await?;
        assert!(timed.timed_out);
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        assert!(
            !backend
                .files()
                .stat(&BackendPath("/workspace/leaked".into()))
                .await?
                .exists
        );
        // A setsid child escapes process-group cleanup, so timeout must retire
        // the entire container while retaining its writable layer.
        let escaped=backend.exec().exec(ExecRequest {
            command:"setsid bash -c 'sleep 2; touch /workspace/escaped' >/dev/null 2>&1 & sleep 10".into(),
            shell:Some("/bin/bash".into()),timeout_ms:Some(100),..Default::default()
        }).await?;
        assert!(escaped.timed_out);
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        assert!(!backend.files().stat(&BackendPath("/workspace/escaped".into())).await?.exists);
        backend.begin_turn("cancelled-turn").await?;
        let running=backend.clone();
        let task=tokio::spawn(async move {
            backend::execution::OPERATION_CONTEXT.scope(operation_protocol::OperationContext {
                turn_id:"cancelled-turn".into(),operation_id:"cancel-op".into(),
            },running.exec().exec(ExecRequest {command:"touch /workspace/started; sleep 30; touch /workspace/cancel-leak".into(),
                shell:Some("/bin/bash".into()),..Default::default()})).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        backend.cancel_turn("old-turn").await?;
        assert!(!task.is_finished());
        backend.cancel_turn("cancelled-turn").await?;
        assert!(task.await?.is_err());
        backend.finish_turn("cancelled-turn").await?;
        let stale=backend::execution::OPERATION_CONTEXT.scope(operation_protocol::OperationContext {
            turn_id:"cancelled-turn".into(),operation_id:"late".into(),
        },backend.exec().exec(ExecRequest {command:"true".into(),..Default::default()})).await;
        assert!(stale.is_err());
        assert!(!backend.files().stat(&BackendPath("/workspace/cancel-leak".into())).await?.exists);
        let running=backend.clone();
        let dropped=tokio::spawn(async move {running.exec().exec(ExecRequest {
            command:"setsid bash -c 'sleep 3; touch /workspace/drop-leak' >/dev/null 2>&1 & sleep 30".into(),
            shell:Some("/bin/bash".into()),..Default::default()
        }).await});
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        dropped.abort(); let _=dropped.await;
        tokio::time::sleep(std::time::Duration::from_secs(4)).await;
        assert!(!backend.files().stat(&BackendPath("/workspace/drop-leak".into())).await?.exists);
        let stopped=tokio::process::Command::new(&config.executable).args(["--host",&format!("unix://{}",config.socket),"stop","--time","0",&instance.instance_id.0]).output().await?;
        assert!(stopped.status.success());
        let restored = DockerProvider::new(config.clone())?.attach(&instance).await?;
        assert_eq!(
            restored
                .files()
                .read_bytes(ReadBytesRequest { path })
                .await?,
            vec![0, 255, 10]
        );
        assert!(matches!(
            provider
                .checkpoint(ProviderCheckpointRequest {
                    backend_id: instance.backend_id.clone(),
                    instance_id: instance.instance_id.clone(),
                    reason: ProviderLifecycleReason::UserRequested,
                    correlation: json!(null)
                })
                .await,
            Err(ProviderControlError::UnsupportedCapability { .. })
        ));
        let mut second_request = request.clone();
        second_request.correlation = json!({"request_id":"second"});
        let second = provider.create(second_request).await?;
        let mut third_request = request.clone();
        third_request.correlation = json!({"request_id":"third"});
        assert!(matches!(
            provider.create(third_request).await,
            Err(ProviderControlError::ResourceLimitExceeded { max: 2, .. })
        ));
        assert_eq!(
            provider.create(request.clone()).await?.instance_id,
            instance.instance_id
        );
        provider
            .delete(ProviderDeleteRequest {
                backend_id: second.backend_id,
                instance_id: Some(second.instance_id),
                snapshot_id: None,
                reason: ProviderLifecycleReason::Release,
                correlation: json!(null),
            })
            .await?;
        // Inject loss of a successful create response, then a crash before
        // recording the container in the manager ledger.
        let wrapper=state.path().join("docker-lost-response");
        std::fs::write(&wrapper,r#"#!/usr/bin/env python3
import subprocess,sys
result=subprocess.run(['docker',*sys.argv[1:]],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
if len(sys.argv)>3 and sys.argv[3]=='create' and result.returncode==0:
 sys.stderr.write('synthetic lost response\n');sys.exit(17)
sys.stdout.buffer.write(result.stdout);sys.stderr.buffer.write(result.stderr);sys.exit(result.returncode)
"#)?;
        #[cfg(unix)] {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&wrapper,std::fs::Permissions::from_mode(0o700))?;
        }
        let mut lost_config=config.clone();lost_config.executable=wrapper.to_string_lossy().into_owned();
        let lost_provider=DockerProvider::new(lost_config)?;
        let mut lost_request=request.clone();lost_request.correlation=json!({"request_id":"lost-response"});
        let recovered=lost_provider.create(lost_request.clone()).await?;
        assert_eq!(lost_provider.create(lost_request).await?.instance_id,recovered.instance_id);
        assert_eq!(provider.list_instances().await?.len(),2);
        let journal=rusqlite::Connection::open(config.state_dir.join(format!("{}.sqlite",config.deployment)))?;
        journal.execute("UPDATE docker_control SET updated_ms=0 WHERE identity=?1",
            [recovered.metadata["container_name"].as_str().unwrap().trim_start_matches('/')])?;
        provider.cleanup_unbound(&[instance.instance_id.0.clone()]).await?;
        assert_eq!(provider.list_instances().await?.len(),1);
        assert!(matches!(provider.attach(&recovered).await,Err(ProviderControlError::NotFound {..})));
        let limits=backend.exec().exec(ExecRequest {command:"cat /sys/fs/cgroup/memory.max /sys/fs/cgroup/pids.max /sys/fs/cgroup/cpu.max".into(),shell:Some("/bin/bash".into()),..Default::default()}).await?;
        let limits=String::from_utf8(limits.stdout)?;
        assert!(limits.contains("1073741824") && limits.contains("128") && limits.contains("100000 100000"));
        let oom=backend.exec().exec(ExecRequest {command:"python3".into(),args:vec!["-c".into(),"x=bytearray(1200*1024*1024)".into()],timeout_ms:Some(15000),..Default::default()}).await?;
        assert_ne!(oom.exit_code,Some(0));
        let events=backend.exec().exec(ExecRequest {command:"cat".into(),args:vec!["/sys/fs/cgroup/memory.events".into()],..Default::default()}).await?;
        assert!(String::from_utf8_lossy(&events.stdout).lines().any(|line|line.starts_with("oom_kill ") && !line.ends_with(" 0")));
        let pids=backend.exec().exec(ExecRequest {command:"python3".into(),args:vec!["-c".into(),
            "import subprocess\np=[]\ntry:\n for i in range(160): p.append(subprocess.Popen(['sleep','10']))\nexcept OSError:\n print('pid-limit-enforced')".into()],timeout_ms:Some(15000),..Default::default()}).await?;
        assert!(String::from_utf8_lossy(&pids.stdout).contains("pid-limit-enforced"));
        let mut guarded=config.clone();
        guarded.limits.disk_bytes=1;
        let guarded=DockerProvider::new(guarded)?;
        guarded.maintenance().await?;
        assert!(provider.attach(&instance).await.is_err());
        assert!(backend.exec().exec(ExecRequest {command:"true".into(),..Default::default()}).await.is_err());
        assert!(provider.create(request.clone()).await.is_err());
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .catch_unwind()
    .await;
    let delete = ProviderDeleteRequest {
        backend_id: instance.backend_id.clone(),
        instance_id: Some(instance.instance_id.clone()),
        snapshot_id: None,
        reason: ProviderLifecycleReason::Release,
        correlation: json!(null),
    };
    assert!(provider.delete(delete.clone()).await.unwrap().deleted);
    assert!(!provider.delete(delete).await.unwrap().deleted);
    assert!(matches!(
        provider.attach(&instance).await,
        Err(ProviderControlError::NotFound { .. })
    ));
    for remaining in provider.list_instances().await.unwrap() {
        provider
            .delete(ProviderDeleteRequest {
                backend_id: remaining.backend_id,
                instance_id: Some(remaining.instance_id),
                snapshot_id: None,
                reason: ProviderLifecycleReason::Release,
                correlation: json!(null),
            })
            .await
            .unwrap();
    }
    result.unwrap().unwrap();
}
