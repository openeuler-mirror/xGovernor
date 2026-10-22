#![cfg(target_os = "linux")]
use std::{
    net::{TcpListener, TcpStream},
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use xgovernor_core::*;

struct Daemon(Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn spawn(data: &Path, address: &str) -> Daemon {
    Daemon(
        Command::new(env!("CARGO_BIN_EXE_xgovernor-server"))
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", data)
            .env("XGOVERNOR_DATA_DIR", data)
            .env("XGOVERNOR_BIND_ADDR", address)
            .env("XGOVERNOR_TENANT_BIND_ADDR", "127.0.0.1:0")
            .env("RUST_LOG", "error")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    )
}
fn wait_listening(daemon: &mut Daemon, address: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            daemon.0.try_wait().unwrap().is_none(),
            "daemon exited before serving"
        );
        if TcpStream::connect(address).is_ok() {
            break;
        }
        assert!(Instant::now() < deadline, "daemon did not start");
        std::thread::sleep(Duration::from_millis(25));
    }
}
#[tokio::test]
async fn legacy_daemon_ignores_deployment_lock_layout_without_a_profile() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    let database = data.join("xgovernor.db");
    let repo = SqliteSessionRepository::open(&database).unwrap();
    let record = SessionRecord {
        execution_backing: SessionExecutionBacking::SplitV1,
        runtime_id: "preserved-session".into(),
        conversation_id: "conversation".into(),
        sender_id: "sender".into(),
        status: SessionStatus::Running,
        created_at_ms: 1,
        updated_at_ms: 2,
        workspace: WorkspaceFacts {
            workspace_id: "workspace".into(),
            root: root.path().to_string_lossy().into(),
            access: WorkspaceAccess::ReadWrite,
            revision: None,
            metadata: serde_json::json!({}),
        },
        isolation: IsolationFacts {
            boundary: IsolationBoundary::Host,
            workspace_access: WorkspaceAccess::ReadWrite,
            network: NetworkIsolation::None,
            metadata: serde_json::json!({}),
        },
        capabilities: EffectiveCapabilities {
            sandbox: Default::default(),
            runtime: Default::default(),
        },
        runtime: OpaqueRuntimeState {
            runtime_kind: "pi".into(),
            schema_version: 1,
            state: serde_json::json!({"original":"preserved"}),
        },
        llm: None,
        lease: None,
        lineage: None,
        last_error: None,
        tenant_id: None,
        created_by: "operator".into(),
    };
    repo.save(record.clone()).await.unwrap();
    drop(repo);
    std::fs::write(data.join("sentinel"), b"unchanged workspace").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
    let mut first = spawn(&data, &address);
    wait_listening(&mut first, &address);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let duplicate_address = listener.local_addr().unwrap().to_string();
    drop(listener);
    let mut duplicate = spawn(&data, &duplicate_address);
    wait_listening(&mut duplicate, &duplicate_address);
    drop(duplicate);
    assert!(!data.join("deployment-locks").exists());
    drop(first);
    use std::os::unix::fs::PermissionsExt;
    let namespace = data.join("deployment-locks");
    std::fs::create_dir(&namespace).unwrap();
    std::fs::set_permissions(&namespace, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut first = spawn(&data, &address);
    wait_listening(&mut first, &address);
    drop(first);
    std::fs::remove_dir(&namespace).unwrap();
    // A procfs-backed would-be namespace is an unlisted-filesystem substitute.
    std::os::unix::fs::symlink("/proc/self", &namespace).unwrap();
    let mut first = spawn(&data, &address);
    wait_listening(&mut first, &address);
    let repo = SqliteSessionRepository::open(&database).unwrap();
    assert_eq!(
        repo.get("preserved-session").await.unwrap(),
        Some(record.clone())
    );
    assert!(!repo.supports_joint_transactions());
    drop(repo);
    assert_eq!(
        std::fs::read(data.join("sentinel")).unwrap(),
        b"unchanged workspace"
    );
    // Independent data directories remain separate deployments.
    let other = root.path().join("other");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let other_address = listener.local_addr().unwrap().to_string();
    drop(listener);
    let mut independent = spawn(&other, &other_address);
    wait_listening(&mut independent, &other_address);
    drop(independent);
    let status = Command::new("/bin/kill")
        .args(["-TERM", &first.0.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let deadline = Instant::now() + Duration::from_secs(10);
    while first.0.try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(25));
    }
    let mut restarted = spawn(&data, &address);
    wait_listening(&mut restarted, &address);
    let repo = SqliteSessionRepository::open(&database).unwrap();
    assert_eq!(repo.get("preserved-session").await.unwrap(), Some(record));
    assert_eq!(
        std::fs::read(data.join("sentinel")).unwrap(),
        b"unchanged workspace"
    );
}
