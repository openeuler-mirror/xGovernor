#![cfg(unix)]
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use xgovernor_core::*;
use xgovernor_thinkthread_client::{
    durable_operation::DurableManagementOperation, sdk, ManagementConfig,
};
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

#[tokio::test]
async fn lost_create_response_recovers_original_uuid_without_a_second_mutation() {
    let f = Fixture::new("durable-drop-create");
    let path = f.root.path().join("owner.db");
    let repo = Arc::new(SqliteSessionRepository::open(&path).unwrap());
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
                limit: 1,
            },
            JointResourceReservation {
                kind: "root".into(),
                units: 1,
                limit: 1,
            },
        ],
        publication_input: None,
        native: vec![native.clone()],
    };
    let admission = repo.prepare_joint_operation(request).await.unwrap();
    assert!(DurableManagementOperation::new(&client, repo.clone())
        .execute(admission.clone(), "create")
        .await
        .is_err());
    assert_eq!(
        repo.pending_joint_operations().await.unwrap()[0].native[0]
            .1
            .as_ref()
            .unwrap()
            .state,
        NativeOperationState::Unknown
    );
    let _ = client.close().await;
    drop(repo);
    let repo = Arc::new(SqliteSessionRepository::open(&path).unwrap());
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
    assert_eq!(queries.len(), 1);
    assert_eq!(creates[0]["params"]["operationId"], native.operation_id);
    assert_eq!(queries[0]["params"]["operationId"], native.operation_id);
    assert_eq!(creates[0]["params"]["operationDigest"], native.digest);
}
