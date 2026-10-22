#![cfg(unix)]
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use xgovernor_core::*;
use xgovernor_thinkthread_client::{
    durable_operation::DurableManagementOperation, sdk, ManagementConfig,
};
fn repository(path: &Path) -> Arc<SqliteSessionRepository> {
    Arc::new(
        SqliteSessionRepository::open(path)
            .unwrap()
            .with_joint_budget_policy(JointBudgetPolicy::from([
                (
                    "session".into(),
                    JointResourceBudget {
                        total: 1,
                        ..Default::default()
                    },
                ),
                (
                    "root".into(),
                    JointResourceBudget {
                        total: 1,
                        ..Default::default()
                    },
                ),
            ]))
            .unwrap(),
    )
}

struct Fixture {
    root: TempDir,
    config: ManagementConfig,
}
impl Fixture {
    fn new(scenario: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let host = root.path().join("host");
        std::fs::write(&host, include_bytes!("support/lifecycle_host.py")).unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_senders_recover_original_uuid_after_claimant_disappears() {
    if let Some(path) = std::env::var_os("XG_TEST_PREPARE_CHILD") {
        let value: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let repo = repository(Path::new(value["db"].as_str().unwrap()));
        let request: JointPrepareRequest =
            serde_json::from_value(value["request"].clone()).unwrap();
        let admission = repo.prepare_joint_operation(request).await.unwrap();
        std::fs::write(
            value["admission_file"].as_str().unwrap(),
            serde_json::to_vec(&admission).unwrap(),
        )
        .unwrap();
        // Bypass connection destructors after the admission commit and before any send claim.
        std::process::exit(23);
    }
    let mut f = Fixture::new("durable-drop-create");
    let gate_path = f.root.path().join("operation.sock");
    let gate = tokio::net::UnixListener::bind(&gate_path).unwrap();
    f.config.base_environment.insert(
        "XG_TEST_OPERATION_GATE".into(),
        gate_path.to_string_lossy().into(),
    );
    let path = f.root.path().join("owner.db");
    let reopen = || repository(&path);
    let repo = reopen();
    let client = f.config.connect().await.unwrap();
    let create:sdk::ThinkThreadCreateRequest=serde_json::from_value(json!({"agent":{"kind":"profile","name":"fixture"},"fs":f.config.fs_root,"models":[],"allowSharedFs":false})).unwrap();
    let params = serde_json::to_value(create).unwrap();
    let native = JointNativeIntent {
        step: "create".into(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        digest: sdk::operation_digest(client.context(), "thinkthread.create", &params).unwrap(),
        method: "thinkthread.create".into(),
        params,
        effect: NativeEffect::Create,
        compensation: false,
    };
    let request = JointPrepareRequest {
        turn: None,
        context: JointOperationContext {
            intent_id: "opening".into(),
            operation: "open".into(),
            tenant_id: None,
            owner_ref: "admin".into(),
            principal: "admin".into(),
            actor_role: Role::Admin,
            quota: TenantQuota::default(),
            runtime_kind: "fixture-joint".into(),
            configuration_revision: "fixed-config".into(),
            source_runtime_id: None,
            target_runtime_id: Some("session".into()),
            checkpoint_id: None,
        },
        binding: JointBinding {
            owner_kind: "fixture-joint".into(),
            binding_id: "binding".into(),
            configuration_revision: "fixed-config".into(),
        },
        root_id: "root-owner".into(),
        source_id: "session".into(),
        exclusive_root: false,
        parent_binding: None,
        reservations: vec![
            JointResourceReservation {
                kind: "session".into(),
                units: 1,
                limit: 0,
            },
            JointResourceReservation {
                kind: "root".into(),
                units: 1,
                limit: 0,
            },
        ],
        publication_input: None,
        native: vec![native.clone()],
    };
    client.close().await.unwrap();
    let child_input = f.root.path().join("prepare-child.json");
    let admission_file = f.root.path().join("admission.json");
    std::fs::write(
        &child_input,
        serde_json::to_vec(&json!({"db":path,"request":request,"admission_file":admission_file}))
            .unwrap(),
    )
    .unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "competing_senders_recover_original_uuid_after_claimant_disappears",
            "--nocapture",
        ])
        .env("XG_TEST_PREPARE_CHILD", &child_input)
        .output()
        .unwrap();
    assert_eq!(
        child.status.code(),
        Some(23),
        "{}",
        String::from_utf8_lossy(&child.stderr)
    );
    let admission: JointAdmission =
        serde_json::from_slice(&std::fs::read(admission_file).unwrap()).unwrap();
    drop(repo);
    let repo = reopen();
    let durable = repo
        .pending_joint_operations()
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(durable.admission, admission);
    assert_eq!(durable.native[0].0.operation_id, native.operation_id);
    assert!(durable.native[0].1.is_none());
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let repository = repository(&path);
        let mut config = f.config.clone();
        config.base_environment.insert(
            "XG_TEST_OPERATION_GATE".into(),
            gate_path.to_string_lossy().into(),
        );
        let admission = admission.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            let client = config.connect().await.unwrap();
            barrier.wait().await;
            DurableManagementOperation::new(&client, repository)
                .execute(admission, "create")
                .await
        }));
    }
    let mut connections = Vec::new();
    let mut methods = Vec::new();
    for _ in 0..2 {
        use tokio::io::AsyncBufReadExt;
        let (stream, _) = tokio::time::timeout(Duration::from_secs(5), gate.accept())
            .await
            .unwrap()
            .unwrap();
        let mut reader = tokio::io::BufReader::new(stream);
        let mut method = String::new();
        tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut method))
            .await
            .unwrap()
            .unwrap();
        methods.push(method.trim().to_string());
        connections.push(reader);
    }
    methods.sort();
    assert_eq!(methods, ["operation.get", "thinkthread.create"]);
    let db = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT sum(units) FROM joint_reservations WHERE state='reserved'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.query_row(
            "SELECT holder FROM joint_fences WHERE kind='source'",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        admission.intent_id
    );
    let mut blocked = request.clone();
    blocked.context.intent_id = "blocked".into();
    blocked.context.target_runtime_id = Some("other".into());
    blocked.binding.binding_id = "other-binding".into();
    blocked.root_id = "other-root".into();
    blocked.source_id = "other".into();
    blocked.native[0].operation_id = uuid::Uuid::new_v4().to_string();
    assert!(matches!(
        repo.prepare_joint_operation(blocked).await,
        Err(SessionDomainError::QuotaExceeded { .. })
    ));
    assert!(repo
        .finish_joint_compensation(admission.clone())
        .await
        .is_err());
    for handle in &handles {
        handle.abort();
    }
    for handle in handles {
        assert!(handle.await.unwrap_err().is_cancelled());
    }
    drop(connections);
    drop(gate);
    std::fs::remove_file(&gate_path).unwrap();
    drop(db);
    assert_eq!(
        repo.pending_joint_operations().await.unwrap()[0].native[0]
            .1
            .as_ref()
            .unwrap()
            .state,
        NativeOperationState::Unknown
    );
    drop(repo);
    let repo = repository(&path);
    let db = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT sum(units) FROM joint_reservations WHERE state='reserved'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.query_row(
            "SELECT holder FROM joint_fences WHERE kind='source'",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        admission.intent_id
    );
    assert_eq!(
        repo.pending_joint_operations().await.unwrap()[0].native[0]
            .0
            .operation_id,
        native.operation_id
    );
    drop(db);
    let client = f.config.connect().await.unwrap();
    let bridge = DurableManagementOperation::new(&client, repo.clone());
    let recovered = bridge.execute(admission.clone(), "create").await.unwrap();
    assert_eq!(recovered.state, NativeOperationState::Completed);
    assert_eq!(recovered.resources[0].id, "fixture-owned-root");
    assert_eq!(
        bridge.execute(admission.clone(), "create").await.unwrap(),
        recovered
    );
    let params = json!({"thinkthreadId":"fixture-owned-root"});
    let start = JointNativeIntent {
        step: "start".into(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        digest: sdk::operation_digest(client.context(), "thinkthread.start", &params).unwrap(),
        method: "thinkthread.start".into(),
        params,
        effect: NativeEffect::Mutate,
        compensation: false,
    };
    repo.append_joint_native_step(admission.clone(), start)
        .await
        .unwrap();
    assert_eq!(
        bridge.execute(admission, "start").await.unwrap().state,
        NativeOperationState::Completed
    );
    client.close().await.unwrap();
    let trace = f.trace();
    let creates: Vec<_> = trace
        .iter()
        .filter(|r| r["method"] == "thinkthread.create")
        .collect();
    let queries: Vec<_> = trace
        .iter()
        .filter(|r| r["method"] == "operation.get")
        .collect();
    assert_eq!(creates.len(), 1);
    assert_eq!(queries.len(), 2);
    assert!(queries
        .iter()
        .all(|q| q["params"]["operationId"] == native.operation_id));
    assert_eq!(creates[0]["params"]["operationId"], native.operation_id);
    assert_eq!(queries[0]["params"]["operationId"], native.operation_id);
    assert_eq!(creates[0]["params"]["operationDigest"], native.digest);
}
