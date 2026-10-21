#![cfg(unix)]
use base64::{engine::general_purpose::STANDARD, Engine};
use operation_protocol::{
    capability::{exec::*, filesystem::*},
    *,
};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use xgovernor_core::*;
use xgovernor_thinkthread_client::{
    host::{management_config_digest, CandidateHostOwner},
    sdk, ManagementConfig,
};

struct Fixture {
    root: TempDir,
    config: ManagementConfig,
    repo: Arc<SqliteSessionRepository>,
    record: SessionRecord,
    target: HostTargetV1,
    policy: HostCapabilityPolicyV1,
}
fn repository(path: &Path) -> Arc<SqliteSessionRepository> {
    Arc::new(
        SqliteSessionRepository::open(path)
            .unwrap()
            .with_joint_transactions()
            .with_joint_budget_policy(JointBudgetPolicy::from([
                (
                    "session".into(),
                    JointResourceBudget {
                        total: 4,
                        recovery_reserve: 0,
                    },
                ),
                (
                    "root".into(),
                    JointResourceBudget {
                        total: 2,
                        recovery_reserve: 0,
                    },
                ),
            ]))
            .unwrap(),
    )
}
impl Fixture {
    async fn new() -> Self {
        Self::seed(true).await
    }
    async fn seed(register: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let p = root.path();
        let host = p.join("host");
        std::fs::write(&host, include_bytes!("support/host_operations.py")).unwrap();
        std::fs::set_permissions(&host, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(p.join("state.json"), b"{}").unwrap();
        let config = ManagementConfig {
            host_executable: host,
            state_root: p.join("state"),
            runtime_root: p.join("runtime"),
            config_root: p.join("config"),
            cache_root: p.join("cache"),
            working_directory: p.join("work"),
            fs_root: p.join("fs"),
            home_directory: p.join("home"),
            executable_search_path: vec![PathBuf::from("/usr/bin")],
            base_environment: BTreeMap::from([
                ("XG_TEST_DB".into(), p.join("db").to_string_lossy().into()),
                (
                    "XG_TEST_TRACE".into(),
                    p.join("trace").to_string_lossy().into(),
                ),
                (
                    "XG_TEST_STATE".into(),
                    p.join("state.json").to_string_lossy().into(),
                ),
                ("XG_TEST_FS".into(), p.join("fs").to_string_lossy().into()),
            ]),
            connect_timeout: Duration::from_secs(2),
        };
        let repo = repository(&p.join("db"));
        let binding = JointBinding {
            owner_kind: "candidate-fixture".into(),
            binding_id: "live-binding".into(),
            configuration_revision: "fixed".into(),
        };
        let record = SessionRecord {
            execution_backing: SessionExecutionBacking::JointV1 {
                binding: binding.clone(),
            },
            runtime_id: "live".into(),
            conversation_id: "c".into(),
            sender_id: "s".into(),
            status: SessionStatus::Idle,
            created_at_ms: 1,
            updated_at_ms: 1,
            workspace: WorkspaceFacts {
                workspace_id: "workspace".into(),
                root: "/workspace".into(),
                access: WorkspaceAccess::ReadWrite,
                revision: None,
                metadata: Value::Null,
            },
            isolation: IsolationFacts {
                boundary: IsolationBoundary::Container,
                workspace_access: WorkspaceAccess::ReadWrite,
                network: NetworkIsolation::Restricted,
                metadata: Value::Null,
            },
            capabilities: EffectiveCapabilities {
                sandbox: BTreeSet::from([
                    SandboxCapability::Exec,
                    SandboxCapability::FileRead,
                    SandboxCapability::FileWrite,
                ]),
                runtime: BTreeSet::new(),
            },
            runtime: OpaqueRuntimeState {
                runtime_kind: "fixture".into(),
                schema_version: 1,
                state: Value::Null,
            },
            llm: None,
            lease: None,
            lineage: None,
            last_error: None,
            tenant_id: Some("a".into()),
            created_by: "fixture".into(),
        };
        let conn = Connection::open(p.join("db")).unwrap();
        conn.execute("INSERT INTO joint_bindings VALUES('root-binding','candidate-fixture','fixed','fixture','tenant/a','native-root',1,'active','root-open',NULL)",[]).unwrap();
        conn.execute("INSERT INTO joint_bindings VALUES('live-binding','candidate-fixture','fixed','fixture','tenant/a','native-root',1,'active','live-open','root-binding')",[]).unwrap();
        conn.execute("INSERT INTO joint_roots VALUES('native-root','candidate-fixture','fixed','tenant/a','root-binding')",[]).unwrap();
        conn.execute("INSERT INTO joint_objects VALUES('root-binding',1,'native-root','root','native-root',0)",[]).unwrap();
        conn.execute("INSERT INTO joint_objects VALUES('live-binding',1,'native-root','child','native-child',0)",[]).unwrap();
        conn.execute("INSERT INTO joint_objects VALUES('live-binding',1,'native-root','branch','private-branch',0)",[]).unwrap();
        conn.execute(
            "INSERT INTO joint_sessions VALUES('live','live-binding','a','idle',1,?1)",
            [serde_json::to_string(&record).unwrap()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO joint_refs VALUES('session','live','live-binding')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO joint_fences VALUES('root','native-root','native-root','tenant/a',0,NULL)",
            [],
        )
        .unwrap();
        let policy = HostCapabilityPolicyV1 {
            schema_version: 1,
            configuration_revision: "fixed".into(),
            deployment_id: "fixture".into(),
            backend_release_ref: "alpha11 fixture; no native evidence".into(),
            sdk_fingerprint: sdk::MANAGEMENT_CONTRACT_FINGERPRINT.into(),
            frozen_host_config_digest: management_config_digest(&config),
            evidence_refs: vec!["typed-transport-fixture".into()],
            paths: HostPathPolicyV1 {
                workspace_backend_root: "/workspace".into(),
                fs_relative_workspace: "work".into(),
                home_relative: None,
                mapping_version: 1,
            },
            exec: true,
            read: true,
            overwrite: true,
            max_timeout_ms: 300000,
            max_output_bytes: 65536,
            max_argv_items: 256,
            max_argv_bytes: 32768,
            max_file_bytes: 1_048_576,
            max_inflight: 16,
            max_reserved_bytes: 16_777_216,
            mkdir_timeout_ms: 30000,
            exited_settlement_declared: true,
            timeout_settlement_declared: false,
        };
        let target = HostTargetV1 {
            schema_version: 1,
            public_runtime_id: "live".into(),
            live_binding: binding,
            tenant_key: "tenant/a".into(),
            owner_ref: "tenant/a".into(),
            deployment_id: "fixture".into(),
            root_binding_id: "root-binding".into(),
            root_id: "native-root".into(),
            native_child_id: "native-child".into(),
            branch_id: "private-branch".into(),
            binding_generation: 1,
            root_runtime_generation: 1,
            workspace_mapping_digest: host_digest(&policy.paths),
            source_checkpoint_id: None,
            source_pair_ref: None,
        };
        if register {
            repo.register_host_target(target.clone(), policy.clone())
                .await
                .unwrap();
        }
        Self {
            root,
            config,
            repo,
            record,
            target,
            policy,
        }
    }
    fn owner(&self) -> CandidateHostOwner {
        CandidateHostOwner::new(
            self.config.clone(),
            "candidate-fixture".into(),
            "fixed".into(),
            Duration::from_millis(800),
        )
        .unwrap()
    }
    fn context(&self, id: &str) -> JointOperationContext {
        JointOperationContext {
            intent_id: id.into(),
            operation: "host".into(),
            tenant_id: Some("a".into()),
            owner_ref: "tenant/a".into(),
            principal: "fixture".into(),
            actor_role: Role::Tenant,
            quota: TenantQuota::default(),
            runtime_kind: "fixture".into(),
            configuration_revision: "fixed".into(),
            source_runtime_id: Some("live".into()),
            target_runtime_id: None,
            checkpoint_id: None,
        }
    }
    async fn access(
        &self,
        id: &str,
        r: JointHostRequest,
    ) -> Result<JointHostAccess, SessionDomainError> {
        self.owner()
            .prepare_host_operation(self.context(id), self.record.clone(), r, self.repo.clone())
            .await
    }
    fn scenario(&self, name: &str) {
        self.state(json!({"scenario":name}));
    }
    fn state(&self, patch: Value) {
        let p = self.root.path().join("state.json");
        let mut v: Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        for (k, val) in patch.as_object().unwrap() {
            v[k] = val.clone();
        }
        std::fs::write(p, serde_json::to_vec(&v).unwrap()).unwrap();
    }
    fn trace(&self) -> Vec<Value> {
        std::fs::read_to_string(self.root.path().join("trace"))
            .unwrap_or_default()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect()
    }
    fn count(&self, m: &str) -> usize {
        self.trace().iter().filter(|r| r["method"] == m).count()
    }
    fn conn(&self) -> Connection {
        Connection::open(self.root.path().join("db")).unwrap()
    }
    fn snapshot(&self) -> Vec<(String, Vec<Vec<String>>)> {
        let c = self.conn();
        [
            "joint_fences",
            "joint_host_allocations",
            "joint_host_workspace",
            "joint_refs",
            "joint_operations",
            "joint_host_cleanup",
            "joint_host_dependencies",
            "joint_reservations",
            "joint_native_steps",
            "joint_host_intents",
            "joint_host_targets",
            "joint_host_roots",
            "joint_bindings",
            "joint_sessions",
            "joint_objects",
        ]
        .iter()
        .map(|name| {
            let mut st = c
                .prepare(&format!("SELECT * FROM {name} ORDER BY 1"))
                .unwrap();
            let n = st.column_count();
            let rows = st
                .query_map([], |r| {
                    Ok((0..n)
                        .map(|i| format!("{:?}", r.get_ref(i).unwrap()))
                        .collect())
                })
                .unwrap()
                .map(Result::unwrap)
                .collect();
            (name.to_string(), rows)
        })
        .collect()
    }
    async fn intent(&self, id: &str) -> HostIntentV1 {
        self.repo.host_intent(id.into()).await.unwrap()
    }
}
fn read() -> JointHostRequest {
    JointHostRequest::Read {
        path: "artifact".into(),
    }
}
fn write(bytes: Vec<u8>) -> JointHostRequest {
    JointHostRequest::Write {
        path: "artifact".into(),
        content: bytes,
        mode: WriteMode::Overwrite,
        create_parents: false,
    }
}
fn exec() -> ExecRequest {
    ExecRequest {
        command: "program".into(),
        args: vec![
            "a b".into(),
            "$HOME".into(),
            "; value".into(),
            "".into(),
            "中文".into(),
        ],
        ..Default::default()
    }
}
fn execute(r: ExecRequest) -> JointHostRequest {
    JointHostRequest::Exec {
        request: r,
        required_output_bytes: None,
    }
}

#[tokio::test]
async fn exact_target_and_public_write_avoid_created_and_local_fallback() {
    let f = Fixture::new().await;
    let bytes = vec![0, 255, 1, 0];
    let r = write(bytes.clone());
    let a = f.access("write", r.clone()).await.unwrap();
    assert_eq!(a.backend.write_host(r).await.unwrap(), 4);
    assert!(a.target.source_checkpoint_id.is_none() && a.target.source_pair_ref.is_none());
    let h = f.intent("write").await;
    assert_eq!(h.state, "completed");
    assert!(h.prepared.target.source_checkpoint_id.is_none());
    assert!(h.prepared.target.source_pair_ref.is_none());
    let persisted: HostIntentV1 = repository(&f.root.path().join("db"))
        .host_intent("write".into())
        .await
        .unwrap();
    assert!(persisted.prepared.target.source_checkpoint_id.is_none());
    assert!(persisted.prepared.target.source_pair_ref.is_none());
    assert!(h.result.unwrap().get("created").is_none());
    assert!(matches!(
        a.backend
            .files()
            .write_bytes(WriteBytesRequest {
                path: BackendPath("artifact".into()),
                content: bytes.clone(),
                mode: WriteMode::Overwrite
            })
            .await,
        Err(OperationError::Unsupported { .. })
    ));
    assert_eq!(f.count("thinkthread.write"), 1);
    assert_eq!(f.count("thinkthread.read"), 0);
    let a = f.access("read", read()).await.unwrap();
    assert_eq!(
        a.backend
            .files()
            .read_bytes(ReadBytesRequest {
                path: BackendPath("artifact".into())
            })
            .await
            .unwrap(),
        bytes
    );
    assert!(f
        .trace()
        .iter()
        .filter(|r| r["method"].as_str().unwrap().starts_with("thinkthread."))
        .all(|r| r["params"]["thinkthreadId"] == "native-child"));
    for state in ["releasing", "removed"] {
        f.conn()
            .execute(
                "UPDATE joint_bindings SET state=?1 WHERE id='live-binding'",
                [state],
            )
            .unwrap();
        assert!(f.access("inactive", read()).await.is_err());
    }
}
#[tokio::test]
async fn identity_configuration_and_generation_rejections_are_side_effect_free() {
    for sql in ["UPDATE joint_host_targets SET target_json=json_set(target_json,'$.branch_id','other')","UPDATE joint_host_targets SET target_json=json_set(target_json,'$.root_id','other')","UPDATE joint_host_targets SET target_json=json_set(target_json,'$.owner_ref','tenant/b')","UPDATE joint_host_targets SET target_json=json_set(target_json,'$.schema_version',2)","UPDATE joint_host_targets SET target_json=json_set(target_json,'$.root_runtime_generation',2)","UPDATE joint_bindings SET generation=2 WHERE id='live-binding'","UPDATE joint_host_targets SET policy_json=json_set(policy_json,'$.configuration_revision','changed')","UPDATE joint_host_targets SET policy_json=json_set(policy_json,'$.frozen_host_config_digest','changed')"]{
        let f=Fixture::new().await;f.conn().execute(sql,[]).unwrap();let before=f.snapshot();assert!(f.access("wrong",read()).await.is_err(),"{sql}");assert_eq!(before,f.snapshot());assert_eq!(f.count("thinkthread.read"),0);
    }
    let f = Fixture::new().await;
    let before = f.snapshot();
    let mut c = f.context("cross");
    c.tenant_id = Some("b".into());
    c.owner_ref = "tenant/b".into();
    assert!(f
        .owner()
        .prepare_host_operation(c, f.record.clone(), read(), f.repo.clone())
        .await
        .is_err());
    assert_eq!(before, f.snapshot());
    assert_eq!(
        f.count("thinkthread.read") + f.count("thinkthread.write") + f.count("thinkthread.exec"),
        0
    );
    let f = Fixture::new().await;
    f.conn().execute("INSERT INTO joint_bindings VALUES('other-binding','candidate-fixture','fixed','fixture','tenant/a','native-root',1,'active','other-open','root-binding')",[]).unwrap();
    f.conn().execute("INSERT INTO joint_objects VALUES('other-binding',1,'native-root','branch','valid-other-branch',0)",[]).unwrap();
    f.conn().execute("UPDATE joint_host_targets SET target_json=json_set(target_json,'$.branch_id','valid-other-branch')",[]).unwrap();
    let before = f.snapshot();
    assert!(f.access("other-branch", write(vec![1])).await.is_err());
    assert_eq!(before, f.snapshot());
    assert_eq!(f.count("thinkthread.write"), 0);
}
#[tokio::test]
async fn complete_preflight_denies_paths_modes_env_extra_and_limits_without_mutation() {
    let f = Fixture::new().await;
    for path in ["/etc/passwd", "a/../..", "bad\0path", "~/x"] {
        let mut e = exec();
        e.cwd = Some(BackendPath(path.into()));
        for r in [
            JointHostRequest::Read { path: path.into() },
            JointHostRequest::Write {
                path: path.into(),
                content: vec![1],
                mode: WriteMode::Overwrite,
                create_parents: false,
            },
            execute(e),
        ] {
            let before = f.snapshot();
            assert!(f.access("invalid", r).await.is_err());
            assert_eq!(before, f.snapshot());
        }
    }
    let mut requests = vec![
        JointHostRequest::Export {
            path: "x".into(),
            preferred_name: None,
        },
        JointHostRequest::Write {
            path: "x".into(),
            content: vec![],
            mode: WriteMode::Create,
            create_parents: false,
        },
        JointHostRequest::Write {
            path: "x".into(),
            content: vec![],
            mode: WriteMode::AtomicOverwrite,
            create_parents: false,
        },
        write(vec![1; f.policy.max_file_bytes as usize + 1]),
    ];
    let mut e = exec();
    e.env = Some(vec![("A".into(), "B".into())]);
    requests.push(execute(e));
    let mut e = exec();
    e.extra = Some(json!({"unknown":true}));
    requests.push(execute(e));
    let mut e = exec();
    e.args = vec!["x".into(); 256];
    requests.push(execute(e));
    let mut e = exec();
    e.args = vec!["x".repeat(32769)];
    requests.push(execute(e));
    let mut e = exec();
    e.timeout_ms = Some(300001);
    requests.push(execute(e));
    requests.push(JointHostRequest::Exec {
        request: exec(),
        required_output_bytes: Some(65537),
    });
    for r in requests {
        let before = f.snapshot();
        assert!(f.access("invalid", r).await.is_err());
        assert_eq!(before, f.snapshot());
    }
    assert!(f.trace().is_empty());
    let mut e = exec();
    e.args = vec!["x".into(); 255];
    assert!(f.policy.preflight(&execute(e)).is_ok());
    let e = ExecRequest {
        command: "x".repeat(32768),
        ..Default::default()
    };
    assert!(f.policy.preflight(&execute(e)).is_ok());
}
#[tokio::test]
async fn read_bytes_framing_records_observed_hash_and_discards_failed_connections() {
    let f = Fixture::new().await;
    let expected = [
        (0u8..=255).collect::<Vec<_>>().repeat(513),
        vec![0, 255, b't', b'a', b'i', b'l'],
    ]
    .concat();
    for scenario in [
        "ok",
        "empty",
        "large-frame",
        "short-body",
        "wrong-length",
        "missing-end",
    ] {
        f.scenario(scenario);
        let id = format!("read-{scenario}");
        let a = f.access(&id, read()).await.unwrap();
        let result = a
            .backend
            .files()
            .read_bytes(ReadBytesRequest {
                path: BackendPath("artifact".into()),
            })
            .await;
        if scenario == "ok" || scenario == "empty" {
            let bytes = result.unwrap();
            assert_eq!(
                bytes,
                if scenario == "empty" {
                    vec![]
                } else {
                    expected.clone()
                }
            );
            let h = f.intent(&id).await;
            let t = h.observations[0]
                .as_ref()
                .unwrap()
                .transfer
                .as_ref()
                .unwrap();
            assert_eq!(t.observed_size, Some(bytes.len() as u64));
            assert_eq!(t.observed_sha256, Some(host_bytes_digest(&bytes)));
        } else {
            assert!(result.is_err());
            let h = f.intent(&id).await;
            assert_eq!(h.state, "failed");
            assert_eq!(
                h.observations[0]
                    .as_ref()
                    .unwrap()
                    .transfer
                    .as_ref()
                    .unwrap()
                    .state,
                "failed"
            );
        }
        f.repo.finish_host_cleanup(a.access).await.unwrap();
        f.scenario("ok");
        let a = f.access(&format!("next-{scenario}"), read()).await.unwrap();
        assert!(a
            .backend
            .files()
            .read_bytes(ReadBytesRequest {
                path: BackendPath("artifact".into())
            })
            .await
            .is_ok());
        f.repo.finish_host_cleanup(a.access).await.unwrap();
    }
    let trace: Vec<_> = f
        .trace()
        .into_iter()
        .filter(|r| r["method"] == "thinkthread.read")
        .collect();
    assert_eq!(trace.len(), 12);
    assert!(trace
        .windows(2)
        .all(|v| v[0]["connection"] != v[1]["connection"]));
}
#[tokio::test]
async fn write_rejections_preserve_expected_hash_and_reconnect() {
    let f = Fixture::new().await;
    for scenario in ["checksum-reject", "length-reject", "rejected-write"] {
        f.scenario(scenario);
        let id = format!("write-{scenario}");
        let r = write(vec![0, 255, 42]);
        let a = f.access(&id, r.clone()).await.unwrap();
        assert!(matches!(
            a.backend.write_host(r).await,
            Err(SessionDomainError::Unavailable { .. })
        ));
        let h = f.intent(&id).await;
        assert_eq!(h.state, "failed");
        let t = h.observations[0]
            .as_ref()
            .unwrap()
            .transfer
            .as_ref()
            .unwrap();
        assert_eq!(t.expected_size, Some(3));
        assert_eq!(t.expected_sha256, Some(host_bytes_digest(&[0, 255, 42])));
        assert_eq!(t.state, "failed");
        f.repo.finish_host_cleanup(a.access).await.unwrap();
        f.scenario("ok");
        let r = write(vec![1]);
        let a = f
            .access(&format!("next-{scenario}"), r.clone())
            .await
            .unwrap();
        assert_eq!(a.backend.write_host(r).await.unwrap(), 1);
        f.repo.finish_host_cleanup(a.access).await.unwrap();
    }
    assert_eq!(f.count("thinkthread.write"), 6);
    let connections: Vec<_> = f
        .trace()
        .into_iter()
        .filter(|r| r["method"] == "thinkthread.write")
        .map(|r| r["connection"].clone())
        .collect();
    assert!(connections.windows(2).all(|p| p[0] != p[1]));
}
#[tokio::test]
async fn exec_preserves_argv_cwd_timeout_and_native_reason_code() {
    for (reason, code, success) in [
        ("exited", Some(0), true),
        ("exited", Some(124), true),
        ("exited", Some(122), true),
        ("exited", Some(7), true),
        ("signaled", Some(9), false),
        ("timeout", None, false),
        ("output_limit", None, false),
        ("task_limit", None, false),
        ("cancelled", None, false),
    ] {
        let f = Fixture::new().await;
        f.state(json!({"reason":reason,"code":code}));
        let mut e = exec();
        e.cwd = Some(BackendPath("subdir".into()));
        e.timeout_ms = Some(300000);
        let a = f.access("exec", execute(e.clone())).await.unwrap();
        let result = a.backend.exec().exec(e.clone()).await;
        assert_eq!(result.is_ok(), success, "{reason}");
        if success {
            assert_eq!(result.unwrap().exit_code, code);
        }
        let h = f.intent("exec").await;
        let o = h.observations[0].as_ref().unwrap().exec.as_ref().unwrap();
        assert_eq!(o.reason, reason);
        assert_eq!(o.code, code);
        assert_eq!(o.stdout_len, if reason == "output_limit" { 7 } else { 3 });
        assert_eq!(o.stderr_len, 3);
        assert_eq!(o.timed_out, reason == "timeout");
        let snapshot = f.snapshot();
        let persisted = repository(&f.root.path().join("db"))
            .host_intent("exec".into())
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(&h).unwrap(),
            serde_json::to_value(persisted).unwrap()
        );
        assert_eq!(snapshot, f.snapshot());
        if reason == "cancelled" {
            let raw = h.observations[0]
                .as_ref()
                .unwrap()
                .native_outcome
                .as_ref()
                .unwrap();
            assert_eq!(raw["reason"], "cancelled");
            assert!(!o.timed_out);
            assert!(h.result.is_none());
        }
        if !success {
            assert_eq!(h.state, "unknown");
            assert!(h.blocker_id.is_some());
            assert!(f.access("blocked", write(vec![])).await.is_err());
            assert!(f.repo.finish_host(a.access, json!({})).await.is_err());
        }
        let trace = f.trace();
        let p = &trace
            .iter()
            .find(|r| r["method"] == "thinkthread.exec")
            .unwrap()["params"];
        let argv: Vec<_> = std::iter::once(e.command).chain(e.args).collect();
        assert_eq!(p["argv"], json!(argv));
        assert_eq!(p["cwd"], "work/subdir");
        assert_eq!(p["timeoutMs"], "300000");
        assert_eq!(f.count("thinkthread.exec"), 1);
    }
    let f = Fixture::new().await;
    let a = f.access("default", execute(exec())).await.unwrap();
    a.backend.exec().exec(exec()).await.unwrap();
    let trace = f.trace();
    assert_eq!(
        trace
            .iter()
            .find(|v| v["method"] == "thinkthread.exec")
            .unwrap()["params"]["timeoutMs"],
        "30000"
    );
}
#[tokio::test]
async fn unknown_mutations_query_only_original_uuid_and_retain_fences_until_evidence() {
    let f = Fixture::new().await;
    f.scenario("drop");
    let r = write(vec![0, 255, 3]);
    let a = f.access("lost", r.clone()).await.unwrap();
    assert!(a.backend.write_host(r).await.is_err());
    let h = f.intent("lost").await;
    assert_eq!(h.state, "unknown");
    assert_eq!(
        f.conn()
            .query_row(
                "SELECT dirty FROM joint_host_workspace WHERE binding_id='live-binding'",
                [],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
        1
    );
    assert!(h.blocker_id.is_some());
    let token = a.access.clone();
    let before = f.snapshot();
    drop(a);
    let reopened = repository(&f.root.path().join("db"));
    for scenario in [
        "needs-recovery",
        "not-found",
        "ttl",
        "wrong-digest",
        "wrong-handle",
        "wrong-method",
        "wrong-target",
        "missing-result",
    ] {
        f.scenario(scenario);
        assert!(
            f.owner()
                .recover_host("lost".into(), reopened.clone())
                .await
                .is_err(),
            "{scenario}"
        );
        assert!(reopened
            .finish_host(token.clone(), json!({}))
            .await
            .is_err());
        assert!(f.access("new", write(vec![])).await.is_err());
    }
    assert_eq!(f.count("thinkthread.write"), 1);
    let after = f.snapshot();
    // Recovery may append evidence and replace the original step observation only.
    for ((name, old), (after_name, new)) in before.iter().zip(&after) {
        assert_eq!(name, after_name);
        if name == "joint_host_intents" {
            continue;
        }
        if name == "joint_native_steps" {
            assert_eq!(old.len(), 1);
            assert_eq!(new.len(), 1);
            assert_eq!(
                old.iter().map(|r| &r[..5]).collect::<Vec<_>>(),
                new.iter().map(|r| &r[..5]).collect::<Vec<_>>()
            );
        } else {
            assert_eq!(old, new, "{name}");
        }
    }
    let recovered = f.intent("lost").await;
    assert!(recovered
        .recovery_evidence
        .starts_with(&h.recovery_evidence));
    let mut original = serde_json::to_value(h).unwrap();
    let mut recovered = serde_json::to_value(recovered).unwrap();
    for value in [&mut original, &mut recovered] {
        value.as_object_mut().unwrap().remove("recovery_evidence");
        value.as_object_mut().unwrap().remove("observations");
    }
    assert_eq!(original, recovered);
    f.scenario("ok");
    assert_eq!(
        f.owner()
            .recover_host("lost".into(), reopened.clone())
            .await
            .unwrap(),
        json!({"bytes_written":3})
    );
    let done = f.snapshot();
    assert_eq!(
        f.owner()
            .recover_host("lost".into(), reopened.clone())
            .await
            .unwrap(),
        json!({"bytes_written":3})
    );
    assert_eq!(done, f.snapshot());
    assert!(reopened
        .finish_host(token.clone(), json!({"bytes_written":4}))
        .await
        .is_err());
    assert_eq!(f.count("thinkthread.write"), 1);
    let original = f.intent("lost").await.prepared.native[0]
        .operation_id
        .clone();
    assert!(f
        .trace()
        .iter()
        .filter(|v| v["method"] == "operation.get")
        .all(|v| v["params"]["operationId"] == original));
    assert!(reopened
        .pending_hosts()
        .await
        .unwrap()
        .iter()
        .any(|h| h.result.is_some() && h.cleanup_state == "pending"));
    reopened.finish_host_cleanup(token.clone()).await.unwrap();
    let after = f.snapshot();
    reopened.finish_host_cleanup(token).await.unwrap();
    assert_eq!(after, f.snapshot());
}
#[tokio::test]
async fn observation_timeout_is_unknown_and_has_no_retry_or_cleanup() {
    for scenario in ["hang", "drop"] {
        let f = Fixture::new().await;
        f.scenario(scenario);
        let a = f.access("interrupted", execute(exec())).await.unwrap();
        assert!(matches!(
            a.backend.exec().exec(exec()).await,
            Err(OperationError::Transport { .. })
        ));
        let h = f.intent("interrupted").await;
        assert_eq!(h.state, "unknown");
        assert!(h
            .blocker_id
            .as_deref()
            .is_some_and(|id| id.contains("interrupted")));
        let reopened = repository(&f.root.path().join("db"))
            .host_intent("interrupted".into())
            .await
            .unwrap();
        assert_eq!(reopened.blocker_id, h.blocker_id);
        assert!(h.result.is_none());
        assert!(!h.observations[0].as_ref().unwrap().settled);
        assert!(f.repo.finish_host(a.access, json!({})).await.is_err());
        assert!(f.access("new", read()).await.is_err());
        assert_eq!(f.count("thinkthread.exec"), 1);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_connections_have_one_source_claim_and_guard_drop_keeps_it() {
    let f = Fixture::new().await;
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let mut tasks = vec![];
    for id in ["one", "two"] {
        let repo = repository(&f.root.path().join("db"));
        let owner = f.owner();
        let record = f.record.clone();
        let context = f.context(id);
        let gate = gate.clone();
        tasks.push(tokio::spawn(async move {
            gate.wait().await;
            owner
                .prepare_host_operation(context, record, write(vec![1]), repo)
                .await
        }));
    }
    let mut wins = vec![];
    for t in tasks {
        if let Ok(a) = t.await.unwrap() {
            wins.push(a);
        }
    }
    assert_eq!(wins.len(), 1);
    let token = wins.pop().unwrap().access;
    let before = f.snapshot();
    assert!(f.access("read", read()).await.is_err());
    assert!(f
        .access(
            "export",
            JointHostRequest::Export {
                path: "x".into(),
                preferred_name: None
            }
        )
        .await
        .is_err());
    assert_eq!(before, f.snapshot());
    let reopened = repository(&f.root.path().join("db"));
    assert_eq!(
        reopened
            .host_intent(token.admission.intent_id.clone())
            .await
            .unwrap()
            .access,
        token
    );
    assert_eq!(f.count("thinkthread.write"), 0);
    assert_eq!(
        f.conn()
            .query_row(
                "SELECT epoch FROM joint_fences WHERE kind='source'",
                [],
                |r| r.get::<_, u64>(0)
            )
            .unwrap(),
        1
    );
}
#[tokio::test]
async fn create_parents_preflights_both_steps_and_preserves_original_ids() {
    let f = Fixture::new().await;
    let r = JointHostRequest::Write {
        path: "dir/file".into(),
        content: vec![1],
        mode: WriteMode::Overwrite,
        create_parents: true,
    };
    let single = f.access("single", write(vec![1])).await.unwrap();
    let h = f.intent("single").await;
    assert_eq!(h.steps.len(), 1);
    assert_eq!(h.prepared.native.len(), 1);
    assert_eq!(h.steps[0].method, "thinkthread.write");
    assert_eq!(
        f.conn()
            .query_row(
                "SELECT count(*) FROM joint_native_steps WHERE intent_id='single'",
                [],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
        1
    );
    single.backend.write_host(write(vec![1])).await.unwrap();
    let writes_before = f.count("thinkthread.write");
    let a = f.access("parents", r.clone()).await.unwrap();
    let h = f.intent("parents").await;
    assert_eq!(h.prepared.native.len(), 2);
    assert_ne!(
        h.prepared.native[0].operation_id,
        h.prepared.native[1].operation_id
    );
    assert_eq!(
        h.prepared.native[0].params["argv"],
        json!(["mkdir", "-p", "--", "work/dir"])
    );
    assert_eq!(h.prepared.native[1].method, "thinkthread.write");
    a.backend.write_host(r.clone()).await.unwrap();
    let snapshot = f.snapshot();
    let reopened = repository(&f.root.path().join("db"));
    assert_eq!(
        f.owner()
            .recover_host("parents".into(), reopened)
            .await
            .unwrap()["bytes_written"],
        1
    );
    assert_eq!(snapshot, f.snapshot());
    let mut timeout = f.policy.clone();
    timeout.mkdir_timeout_ms = 300001;
    let mut items = f.policy.clone();
    items.max_argv_items = 3;
    let mut bytes = f.policy.clone();
    bytes.max_argv_bytes = 10;
    for policy in [timeout, items, bytes] {
        assert!(policy.preflight(&r).is_err());
    }
    for invalid in [
        JointHostRequest::Write {
            path: "dir/file".into(),
            content: vec![0; f.policy.max_file_bytes as usize + 1],
            mode: WriteMode::Overwrite,
            create_parents: true,
        },
        JointHostRequest::Write {
            path: "a/../../file".into(),
            content: vec![],
            mode: WriteMode::Overwrite,
            create_parents: true,
        },
    ] {
        assert!(f.access("bad-parent", invalid).await.is_err());
    }
    assert_eq!(h.steps.len(), 2);
    assert_ne!(
        h.steps[0].canonical_params_digest,
        h.steps[1].canonical_params_digest
    );
    assert!(h
        .steps
        .iter()
        .all(|s| s.original_uuid.is_some() && s.target_digest == a.access.target_digest));
    assert_eq!(f.count("thinkthread.exec"), 1);
    assert_eq!(f.count("thinkthread.write"), writes_before + 1);
}

fn settled_write() -> HostObservationV1 {
    HostObservationV1 {
        state: NativeOperationState::Completed,
        settled: true,
        result: json!({"bytes_written":1}),
        transfer: None,
        exec: None,
        error: None,
        native_outcome: None,
    }
}
#[tokio::test]
async fn cleanup_suspension_is_atomic_persistent_and_rejects_direct_legacy_entries() {
    let f = Fixture::new().await;
    let a = f.access("parent", write(vec![1])).await.unwrap();
    let token = a.access;
    assert!(f.repo.claim_host_step(token.clone(), 0).await.unwrap());
    assert_eq!(
        f.conn()
            .query_row(
                "SELECT dirty FROM joint_host_workspace WHERE binding_id='live-binding'",
                [],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
        1
    );
    f.repo
        .record_host_step(token.clone(), 0, settled_write())
        .await
        .unwrap();
    f.repo
        .prepare_host_cleanup(
            token.clone(),
            "child".into(),
            vec!["bytes-reference".into()],
        )
        .await
        .unwrap();
    let snapshot = f.snapshot();
    f.conn().execute_batch("CREATE TRIGGER fail_suspend BEFORE UPDATE ON joint_host_cleanup WHEN NEW.state='waiting_dependency' BEGIN SELECT RAISE(ABORT,'failpoint'); END").unwrap();
    assert!(f.repo.suspend_host_cleanup(token.clone()).await.is_err());
    assert_eq!(snapshot, f.snapshot());
    f.conn().execute_batch("DROP TRIGGER fail_suspend").unwrap();
    // These Host entries are valid before suspension; their rejection below is stage-specific.
    assert!(!f.repo.claim_host_step(token.clone(), 0).await.unwrap());
    f.repo
        .record_host_step(token.clone(), 0, settled_write())
        .await
        .unwrap();
    f.repo.suspend_host_cleanup(token.clone()).await.unwrap();
    let observer = f.conn();
    observer.execute_batch("BEGIN").unwrap();
    let stages: Vec<String> = observer
        .prepare("SELECT stage FROM joint_operations WHERE id IN ('parent','child') ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(stages, vec!["suspended_waiting_dependency"; 2]);
    let plan: (String, u32) = observer
        .query_row(
            "SELECT state,delegated FROM joint_host_cleanup WHERE parent_id='parent'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(plan, ("waiting_dependency".into(), 0));
    assert_eq!(
        observer
            .query_row(
                "SELECT holder FROM joint_fences WHERE kind='source' AND id='live'",
                [],
                |r| r.get::<_, Option<String>>(0)
            )
            .unwrap(),
        None
    );
    let body: String = observer
        .query_row(
            "SELECT body FROM joint_host_intents WHERE intent_id='parent'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let suspended: HostIntentV1 = serde_json::from_str(&body).unwrap();
    assert_eq!(
        suspended.suspended_cleanup_plan_id.as_deref(),
        Some("child")
    );
    assert_eq!(suspended.resume_joint_stage.as_deref(), Some("accepted"));
    assert!(suspended.blocker_id.is_some());
    observer.execute_batch("COMMIT").unwrap();
    let snapshot = f.snapshot();
    let repo = repository(&f.root.path().join("db"));
    assert_eq!(
        repo.host_intent("parent".into()).await.unwrap().state,
        "suspended_waiting_dependency"
    );
    let child_admission: JointAdmission = serde_json::from_str(
        &f.conn()
            .query_row(
                "SELECT admission_json FROM joint_operations WHERE id='child'",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap(),
    )
    .unwrap();
    for admission in [token.admission.clone(), child_admission] {
        let n = f.intent("parent").await.prepared.native[0].clone();
        assert!(matches!(
            repo.append_joint_native_step(admission.clone(), n.clone())
                .await,
            Err(SessionDomainError::Conflict { .. })
        ));
        assert!(matches!(
            repo.claim_joint_native_send(admission.clone(), n.step.clone())
                .await,
            Err(SessionDomainError::Conflict { .. })
        ));
        assert!(matches!(
            repo.record_joint_native_result(
                admission.clone(),
                n.step,
                n.operation_id,
                n.digest,
                JointNativeOutcome {
                    state: NativeOperationState::Completed,
                    resources: vec![],
                    payload: Value::Null
                }
            )
            .await,
            Err(SessionDomainError::Conflict { .. })
        ));
        assert!(matches!(
            repo.publish_joint_session(admission.clone(), f.record.clone())
                .await,
            Err(SessionDomainError::Conflict { .. })
        ));
        assert!(matches!(
            repo.finish_joint_release(admission, "session".into(), "live".into(), 1)
                .await,
            Err(SessionDomainError::Conflict { .. })
        ));
    }
    let error = repo.claim_host_step(token.clone(), 0).await.unwrap_err();
    assert!(
        error.to_string().contains("Host operation is not active"),
        "{error}"
    );
    let error = repo
        .record_host_step(token.clone(), 0, settled_write())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Host operation is not active"),
        "{error}"
    );
    assert!(repo
        .finish_host(token.clone(), json!({"bytes_written":1}))
        .await
        .is_err());
    assert!(repo.finish_host_cleanup(token.clone()).await.is_err());
    assert!(repo.resume_host_cleanup(token.clone()).await.is_err());
    let close = close_request(&f, "close");
    let error = repo
        .prepare_joint_operation(close.clone())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("suspended Host cleanup"),
        "{error}"
    );
    assert_eq!(snapshot, f.snapshot());
    repo.release_host_dependency(token.clone(), "bytes-reference".into())
        .await
        .unwrap();
    let released = f.snapshot();
    repo.release_host_dependency(token.clone(), "bytes-reference".into())
        .await
        .unwrap();
    assert_eq!(released, f.snapshot());
    let mut wrong = token.clone();
    wrong.admission.binding.owner_kind = "other".into();
    assert!(repo.resume_host_cleanup(wrong).await.is_err());
    f.conn()
        .execute(
            "UPDATE joint_host_allocations SET state='released' WHERE intent_id='parent'",
            [],
        )
        .unwrap();
    assert!(repo.resume_host_cleanup(token.clone()).await.is_err());
    f.conn()
        .execute(
            "UPDATE joint_host_allocations SET state='reserved' WHERE intent_id='parent'",
            [],
        )
        .unwrap();
    let before = f.snapshot();
    f.conn().execute_batch("CREATE TRIGGER fail_resume BEFORE UPDATE ON joint_host_cleanup WHEN NEW.state='prepared' BEGIN SELECT RAISE(ABORT,'failpoint'); END").unwrap();
    assert!(repo.resume_host_cleanup(token.clone()).await.is_err());
    assert_eq!(before, f.snapshot());
    f.conn().execute_batch("DROP TRIGGER fail_resume").unwrap();
    let new = repo.resume_host_cleanup(token.clone()).await.unwrap();
    assert!(new.admission.source_epoch > token.admission.source_epoch);
    assert!(repo.claim_host_step(token.clone(), 0).await.is_err());
    assert!(repo.resume_host_cleanup(token).await.is_err());
    repo.finish_host_cleanup(new.clone()).await.unwrap();
    repo.finish_host(new.clone(), json!({"bytes_written":1}))
        .await
        .unwrap();
    assert!(repo
        .pending_joint_operations()
        .await
        .unwrap()
        .iter()
        .any(|p| p.admission == new.admission));
    repo.finish_host_cleanup(new.clone()).await.unwrap();
    let done = f.snapshot();
    repo.finish_host_cleanup(new).await.unwrap();
    assert_eq!(done, f.snapshot());
    assert_eq!(
        f.conn()
            .query_row(
                "SELECT count(*) FROM joint_host_allocations WHERE state!='released'",
                [],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
        0
    );
    assert_eq!(
        f.conn()
            .query_row(
                "SELECT count(*) FROM joint_refs WHERE kind='host_hold'",
                [],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
        0
    );
    repo.prepare_joint_operation(close).await.unwrap();
}
#[tokio::test]
async fn unknown_and_capacity_cannot_be_released_or_suspended() {
    let f = Fixture::new().await;
    let a = f.access("unknown", write(vec![1])).await.unwrap();
    assert!(f.repo.claim_host_step(a.access.clone(), 0).await.unwrap());
    assert!(!f.repo.claim_host_step(a.access.clone(), 0).await.unwrap());
    let snapshot = f.snapshot();
    assert!(f
        .repo
        .prepare_host_cleanup(a.access.clone(), "child".into(), vec!["ref".into()])
        .await
        .is_err());
    assert!(f.repo.finish_host_cleanup(a.access.clone()).await.is_err());
    assert!(f
        .repo
        .finish_host(a.access, json!({"error":"assume absent"}))
        .await
        .is_err());
    assert_eq!(snapshot, f.snapshot());
    let f = Fixture::new().await;
    f.conn().execute("UPDATE joint_host_targets SET policy_json=json_set(policy_json,'$.max_reserved_bytes',1)",[]).unwrap();
    let p = f.repo.host_target("live".into()).await.unwrap().1;
    assert_eq!(p.max_reserved_bytes, 1);
    let before = f.snapshot();
    assert!(matches!(
        f.access("over", read()).await,
        Err(SessionDomainError::QuotaExceeded { .. })
    ));
    assert_eq!(before, f.snapshot());
    assert_eq!(f.count("thinkthread.read"), 0);
}

struct UnusedRuntime;
use xgovernor_core::runtime_protocol as rp;
#[async_trait::async_trait]
impl rp::AgentRuntime for UnusedRuntime {
    fn runtime_kind(&self) -> &str {
        "fixture"
    }
    fn capabilities(&self) -> BTreeSet<rp::RuntimeCapability> {
        BTreeSet::new()
    }
    async fn start(
        &self,
        _: rp::RuntimeStartRequest,
        _: rp::RuntimeExecutionContext,
    ) -> Result<(), rp::RuntimeError> {
        panic!("split runtime invoked")
    }
    async fn attach(
        &self,
        _: &str,
        _: rp::RuntimeExecutionContext,
    ) -> Result<(), rp::RuntimeError> {
        panic!("split runtime invoked")
    }
    async fn stop(&self, _: &str) -> Result<(), rp::RuntimeError> {
        panic!("split runtime invoked")
    }
    async fn check_alive(&self, _: &str) -> Result<bool, rp::RuntimeError> {
        panic!("split runtime invoked")
    }
    async fn submit_turn(
        &self,
        _: rp::RuntimeTurnRequest,
    ) -> Result<rp::RuntimeEventReceiver, rp::RuntimeError> {
        panic!("split runtime invoked")
    }
    async fn answer_interaction(
        &self,
        _: rp::RuntimeInteractionRequest,
    ) -> Result<(), rp::RuntimeError> {
        panic!("split runtime invoked")
    }
    async fn cancel(&self, _: rp::RuntimeCancelRequest) -> Result<(), rp::RuntimeError> {
        panic!("split runtime invoked")
    }
}
struct UnusedEnvironment;
#[async_trait::async_trait]
impl SessionEnvironmentNormalizer for UnusedEnvironment {
    async fn normalize(
        &self,
        _: &SecurityContext,
        _: &session_protocol::SessionOpenRequest,
    ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
        panic!("normalizer invoked for Host operation")
    }
}
struct Ids(std::sync::atomic::AtomicU64);
impl TurnIdGenerator for Ids {
    fn next_turn_id(&self) -> String {
        self.0
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .to_string()
    }
}
impl RuntimeIdGenerator for Ids {
    fn next_runtime_id(&self) -> String {
        self.0
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .to_string()
    }
}
impl Clock for Ids {
    fn now_ms(&self) -> u64 {
        100
    }
}
fn application(f: &Fixture) -> SessionApplication {
    let ids = Arc::new(Ids(std::sync::atomic::AtomicU64::new(1)));
    SessionApplication::with_runtime_registry(
        "fixture",
        [RuntimeRegistration::new(
            Arc::new(UnusedRuntime),
            HashMap::new(),
            Arc::new(UnusedEnvironment),
        )
        .with_joint_lifecycle(Arc::new(f.owner()))],
        f.repo.clone(),
        ids.clone(),
        ids.clone(),
        ids,
    )
    .unwrap()
}
#[tokio::test]
async fn public_session_routes_keep_authentication_workspace_and_wire_boundaries() {
    let f = Fixture::new().await;
    let app = application(&f);
    let ctx = SecurityContext::tenant("a", "principal");
    let request = session_protocol::SessionFileWriteRequest {
        runtime_id: "live".into(),
        path: "file".into(),
        content_base64: STANDARD.encode([0, 255, 42]),
        create_parents: false,
        lease: Default::default(),
    };
    let result = app.write_file(&ctx, request.clone()).await.unwrap();
    assert_eq!(
        serde_json::to_value(result).unwrap(),
        json!({"path":"/workspace/file","bytes_written":3})
    );
    assert_eq!(f.count("thinkthread.write"), 1);
    assert_eq!(f.count("thinkthread.read"), 0);
    let r = session_protocol::SessionFileReadRequest {
        runtime_id: "live".into(),
        path: "file".into(),
        lease: Default::default(),
    };
    let result = app.read_file(&ctx, r.clone()).await.unwrap();
    assert_eq!(
        STANDARD.decode(result.content_base64).unwrap(),
        vec![0, 255, 42]
    );
    let foreign = SecurityContext::tenant("b", "principal");
    assert!(matches!(
        app.read_file(&foreign, r).await,
        Err(SessionDomainError::NotFound { .. })
    ));
    for timeout in [0, 3_600_001, 300001] {
        let req = session_protocol::SessionExecRequest {
            runtime_id: "live".into(),
            command: vec!["program".into()],
            cwd: None,
            env: Default::default(),
            timeout_ms: Some(timeout),
            lease: Default::default(),
        };
        assert!(app.exec(&ctx, req).await.is_err());
    }
    assert_eq!(f.count("thinkthread.exec"), 0);
    let mut record = f.record.clone();
    record.workspace.access = WorkspaceAccess::ReadOnly;
    f.conn()
        .execute(
            "UPDATE joint_sessions SET body=?1 WHERE runtime_id='live'",
            [serde_json::to_string(&record).unwrap()],
        )
        .unwrap();
    assert!(app.write_file(&ctx, request).await.is_err());
    assert_eq!(f.count("thinkthread.write"), 1);
    assert!(
        serde_json::from_value::<session_protocol::SessionExecRequest>(
            json!({"runtime_id":"live","command":["program"],"unknown":true})
        )
        .is_err()
    );
}
#[tokio::test]
async fn durable_claim_survives_process_exit_and_fences_turn_and_capture() {
    if let Some(input) = std::env::var_os("XG_HOST_CLAIM_CHILD") {
        let v: Value = serde_json::from_slice(&std::fs::read(input).unwrap()).unwrap();
        let repo = repository(Path::new(v["db"].as_str().unwrap()));
        let token: HostAccessTokenV1 = serde_json::from_value(v["token"].clone()).unwrap();
        assert!(repo.claim_host_step(token, 0).await.unwrap());
        std::process::exit(23);
    }
    let f = Fixture::new().await;
    let a = f.access("crash", write(vec![1])).await.unwrap();
    let token = a.access;
    drop(a.backend);
    let input = f.root.path().join("crash.json");
    std::fs::write(
        &input,
        serde_json::to_vec(&json!({"db":f.root.path().join("db"),"token":token})).unwrap(),
    )
    .unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable_claim_survives_process_exit_and_fences_turn_and_capture",
            "--nocapture",
        ])
        .env("XG_HOST_CLAIM_CHILD", input)
        .output()
        .unwrap();
    assert_eq!(
        child.status.code(),
        Some(23),
        "{}",
        String::from_utf8_lossy(&child.stderr)
    );
    let repo = repository(&f.root.path().join("db"));
    let h = repo.host_intent("crash".into()).await.unwrap();
    assert_eq!(h.access, token);
    assert_eq!(
        h.observations[0].as_ref().unwrap().state,
        NativeOperationState::Unknown
    );
    let before = f.snapshot();
    for kind in ["turn", "checkpoint"] {
        let mut c = f.context(kind);
        c.operation = kind.into();
        if kind == "checkpoint" {
            c.checkpoint_id = Some("checkpoint".into());
        }
        let mut request = JointPrepareRequest {
            context: c,
            binding: f.target.live_binding.clone(),
            root_id: "native-root".into(),
            source_id: "live".into(),
            exclusive_root: false,
            parent_binding: None,
            reservations: vec![],
            native: vec![JointNativeIntent {
                step: "capture".into(),
                operation_id: uuid::Uuid::new_v4().to_string(),
                digest: "a".repeat(64),
                method: "message.send".into(),
                params: json!({}),
                effect: NativeEffect::Mutate,
                compensation: false,
            }],
            publication_input: None,
            turn: Some(JointTurnIdentity {
                turn_id: "turn".into(),
                client_request_id: None,
                input_digest: "a".repeat(64),
            }),
        };
        if kind == "checkpoint" {
            request.binding.binding_id = "capture-binding".into();
            request.parent_binding = Some(f.target.live_binding.clone());
            request.reservations = vec![JointResourceReservation {
                kind: "checkpoint".into(),
                units: 1,
                limit: 0,
            }];
            request.native[0].effect = NativeEffect::Create;
            request.native[0].method = "checkpoint.create".into();
            request.turn = None;
        }
        let error = repo.prepare_joint_operation(request).await.unwrap_err();
        assert!(error.to_string().contains("fenced"), "{error}");
    }
    assert_eq!(before, f.snapshot());
    assert!(f.access("new", read()).await.is_err());
    assert_eq!(f.count("thinkthread.write"), 0);
}

#[tokio::test]
async fn undeclared_host_methods_and_default_joint_owner_fail_closed() {
    use operation_protocol::capability::{
        export::ExportFileRequest,
        search::{GlobRequest, GrepMode, GrepRequest},
    };
    let f = Fixture::new().await;
    let a = f.access("methods", read()).await.unwrap();
    let caps = a.backend.capabilities();
    assert!(
        !caps.supports_atomic_write
            && !caps.supports_export_file
            && !caps.supports_grep
            && !caps.supports_lsp
    );
    let path = BackendPath("artifact".into());
    assert!(matches!(
        a.backend.files().stat(&path).await,
        Err(OperationError::Unsupported { .. })
    ));
    assert!(matches!(
        a.backend.files().create_dir_all(&path).await,
        Err(OperationError::Unsupported { .. })
    ));
    assert!(matches!(
        a.backend
            .files()
            .temp_path(TempPathRequest {
                kind: TempPathKind::File,
                preferred_parent: None,
                prefix: None,
                suffix: None
            })
            .await,
        Err(OperationError::Unsupported { .. })
    ));
    assert!(matches!(
        a.backend
            .search()
            .glob(GlobRequest {
                pattern: "*".into(),
                base_dir: None,
                limit: None
            })
            .await,
        Err(OperationError::Unsupported { .. })
    ));
    assert!(matches!(
        a.backend
            .search()
            .grep(GrepRequest {
                query: "x".into(),
                base_dir: path.clone(),
                include: None,
                mode: GrepMode::Content,
                head_limit: None
            })
            .await,
        Err(OperationError::Unsupported { .. })
    ));
    assert!(matches!(
        a.backend
            .export()
            .export_file(ExportFileRequest {
                path,
                preferred_name: None
            })
            .await,
        Err(OperationError::Unsupported { .. })
    ));
    struct Disabled;
    #[async_trait::async_trait]
    impl JointSessionLifecycle for Disabled {
        fn owner_kind(&self) -> &str {
            "disabled"
        }
        fn configuration_revision(&self) -> &str {
            "fixed"
        }
    }
    assert!(matches!(
        Disabled
            .prepare_host_operation(
                f.context("disabled"),
                f.record.clone(),
                read(),
                f.repo.clone()
            )
            .await,
        Err(SessionDomainError::UnsupportedCapability { .. })
    ));
    assert_eq!(f.count("thinkthread.read"), 0);
    assert_eq!(f.count("thinkthread.exec"), 0);
    assert_eq!(f.count("thinkthread.write"), 0);
}
#[tokio::test]
async fn schema_checksum_and_host_record_versions_are_not_downgraded() {
    let f = Fixture::new().await;
    let a = f.access("version", read()).await.unwrap();
    f.conn()
        .execute(
            "UPDATE joint_host_intents SET body=json_set(body,'$.schema_version',2)",
            [],
        )
        .unwrap();
    assert!(f.repo.claim_host_step(a.access, 0).await.is_err());
    assert_eq!(f.count("thinkthread.read"), 0);
    let f = Fixture::new().await;
    f.conn()
        .execute("UPDATE joint_host_schema SET checksum='other'", [])
        .unwrap();
    assert!(SqliteSessionRepository::open(&f.root.path().join("db")).is_err());
}

#[tokio::test]
async fn rejected_requests_preserve_existing_workspace_and_accounting() {
    let f = Fixture::new().await;
    let mut r = write(vec![0, 255, 11]);
    if let JointHostRequest::Write { path, .. } = &mut r {
        *path = "sibling".into();
    }
    let a = f.access("sibling", r.clone()).await.unwrap();
    a.backend.write_host(r).await.unwrap();
    let files = std::fs::read(f.root.path().join("state.json")).unwrap();
    let snapshot = f.snapshot();
    for path in ["/etc/passwd", "a/../../sibling", "~/sibling", "sibling\0"] {
        let r = JointHostRequest::Write {
            path: path.into(),
            content: vec![99],
            mode: WriteMode::Overwrite,
            create_parents: false,
        };
        assert!(f.access("bad", r).await.is_err());
        assert_eq!(snapshot, f.snapshot());
        assert_eq!(
            files,
            std::fs::read(f.root.path().join("state.json")).unwrap()
        );
    }
    assert_eq!(f.count("thinkthread.write"), 1);
    let a = f
        .access(
            "verify-sibling",
            JointHostRequest::Read {
                path: "sibling".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        a.backend
            .files()
            .read_bytes(ReadBytesRequest {
                path: BackendPath("sibling".into())
            })
            .await
            .unwrap(),
        vec![0, 255, 11]
    );
}

#[tokio::test]
async fn admitted_argv_boundaries_are_sent_unchanged() {
    let f = Fixture::new().await;
    let requests = [
        ExecRequest {
            command: "program".into(),
            args: vec!["x".into(); 255],
            ..Default::default()
        },
        ExecRequest {
            command: "x".repeat(32768),
            ..Default::default()
        },
    ];
    for (i, request) in requests.iter().enumerate() {
        let a = f
            .access(&format!("bound-{i}"), execute(request.clone()))
            .await
            .unwrap();
        a.backend.exec().exec(request.clone()).await.unwrap();
    }
    let trace = f.trace();
    let sent: Vec<_> = trace
        .iter()
        .filter(|r| r["method"] == "thinkthread.exec")
        .collect();
    assert_eq!(sent.len(), 2);
    for (r, s) in requests.iter().zip(sent) {
        let argv: Vec<_> = std::iter::once(r.command.clone())
            .chain(r.args.clone())
            .collect();
        assert_eq!(s["params"]["argv"], json!(argv));
        assert_eq!(s["params"]["cwd"], "work");
    }
}
#[tokio::test]
async fn committed_result_and_cleanup_survive_exit_before_acknowledgement() {
    if let Some(input) = std::env::var_os("XG_HOST_COMMIT_CHILD") {
        let v: Value = serde_json::from_slice(&std::fs::read(input).unwrap()).unwrap();
        let repo = repository(Path::new(v["db"].as_str().unwrap()));
        let token: HostAccessTokenV1 = serde_json::from_value(v["token"].clone()).unwrap();
        repo.finish_host(token, json!({"bytes_written":1}))
            .await
            .unwrap();
        std::process::exit(31);
    }
    let f = Fixture::new().await;
    let a = f.access("commit", write(vec![1])).await.unwrap();
    let token = a.access;
    f.repo.claim_host_step(token.clone(), 0).await.unwrap();
    f.repo
        .record_host_step(token.clone(), 0, settled_write())
        .await
        .unwrap();
    let input = f.root.path().join("commit.json");
    std::fs::write(
        &input,
        serde_json::to_vec(&json!({"db":f.root.path().join("db"),"token":token})).unwrap(),
    )
    .unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "committed_result_and_cleanup_survive_exit_before_acknowledgement",
            "--nocapture",
        ])
        .env("XG_HOST_COMMIT_CHILD", input)
        .output()
        .unwrap();
    assert_eq!(
        child.status.code(),
        Some(31),
        "{}",
        String::from_utf8_lossy(&child.stderr)
    );
    let repo = repository(&f.root.path().join("db"));
    let h = repo.host_intent("commit".into()).await.unwrap();
    assert_eq!(h.state, "completed");
    assert_eq!(h.cleanup_state, "pending");
    let snapshot = f.snapshot();
    let result = repo
        .finish_host(token.clone(), json!({"bytes_written":1}))
        .await
        .unwrap();
    assert_eq!(host_digest(&result), host_digest(&h.result.unwrap()));
    assert_eq!(snapshot, f.snapshot());
    assert!(repo
        .pending_joint_operations()
        .await
        .unwrap()
        .iter()
        .any(|p| p.admission == token.admission));
    assert!(repo
        .pending_hosts()
        .await
        .unwrap()
        .iter()
        .any(|h| h.access == token));
    repo.finish_host_cleanup(token.clone()).await.unwrap();
    let released = f.snapshot();
    repo.finish_host_cleanup(token).await.unwrap();
    assert_eq!(released, f.snapshot());
    assert_eq!(f.count("thinkthread.write"), 0);
}

fn close_request(f: &Fixture, id: &str) -> JointPrepareRequest {
    let mut context = f.context(id);
    context.operation = "close".into();
    JointPrepareRequest {
        context,
        binding: f.target.live_binding.clone(),
        root_id: f.target.root_id.clone(),
        source_id: f.record.runtime_id.clone(),
        exclusive_root: false,
        parent_binding: None,
        reservations: vec![],
        native: vec![JointNativeIntent {
            step: "delete-child".into(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            digest: "a".repeat(64),
            method: "thinkthread.delete".into(),
            params: json!({"thinkthreadId":"native-child"}),
            effect: NativeEffect::Remove,
            compensation: false,
        }],
        publication_input: None,
        turn: None,
    }
}

#[tokio::test]
async fn mutation_rejections_require_not_sent_and_no_recovery_blocker() {
    for method in ["write", "exec"] {
        for (code, delivery, settled) in [
            ("FileWriteRejected", "not_sent", true),
            ("FileWriteRejected", "accepted", false),
            ("ChecksumMismatch", "sent_unknown", false),
            ("NeedsRecovery", "not_sent", false),
            ("NeedsRecovery", "accepted", false),
            ("OperationInProgress", "not_sent", false),
            ("OperationInProgress", "sent_unknown", false),
        ] {
            let f = Fixture::new().await;
            f.state(json!({"scenario":"mutation-rejected","error_code":code,"delivery":delivery}));
            let request = if method == "write" {
                write(vec![1])
            } else {
                execute(exec())
            };
            let a = f.access("rejected", request.clone()).await.unwrap();
            if method == "write" {
                assert!(matches!(
                    a.backend.write_host(request).await,
                    Err(SessionDomainError::Unavailable { .. })
                ));
            } else {
                assert!(matches!(
                    a.backend.exec().exec(exec()).await,
                    Err(OperationError::Transport { .. })
                ));
            }
            let h = f.intent("rejected").await;
            assert_eq!(
                h.observations[0].as_ref().unwrap().settled,
                settled,
                "{method}/{code}/{delivery}"
            );
            assert_eq!(h.state, if settled { "failed" } else { "unknown" });
            let rejection = if delivery == "accepted"
                && code != "NeedsRecovery"
                && code != "OperationInProgress"
            {
                &h.recovery_evidence
                    .iter()
                    .find(|e| e["kind"] == "accepted_rejection")
                    .unwrap()["response"]
            } else {
                h.observations[0]
                    .as_ref()
                    .unwrap()
                    .native_outcome
                    .as_ref()
                    .unwrap()
            };
            assert_eq!(rejection["error"]["delivery"], delivery);
            let (dirty, holder):(u32, Option<String>)=f.conn().query_row("SELECT w.dirty,f.holder FROM joint_host_workspace w JOIN joint_fences f ON f.id='live' AND f.kind='source'",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
            assert_eq!(dirty, if settled { 0 } else { 1 });
            assert_eq!(holder.is_some(), !settled);
            let snapshot = f.snapshot();
            if settled {
                let repo = repository(&f.root.path().join("db"));
                assert!(matches!(
                    f.owner().recover_host("rejected".into(), repo).await,
                    Err(SessionDomainError::Unavailable { .. })
                ));
                assert_eq!(snapshot, f.snapshot());
                f.repo.finish_host_cleanup(a.access).await.unwrap();
            } else {
                assert!(h.blocker_id.is_some());
                assert!(f
                    .repo
                    .finish_host(a.access.clone(), json!({"error":"rejected"}))
                    .await
                    .is_err());
                assert!(f.repo.finish_host_cleanup(a.access.clone()).await.is_err());
                assert!(f.access("new", write(vec![])).await.is_err());
                assert_eq!(snapshot, f.snapshot());
                assert_eq!(
                    f.conn()
                        .query_row(
                            "SELECT state FROM joint_host_allocations WHERE intent_id='rejected'",
                            [],
                            |r| r.get::<_, String>(0)
                        )
                        .unwrap(),
                    "reserved"
                );
                assert_eq!(f.conn().query_row("SELECT count(*) FROM joint_refs WHERE kind='host_hold' AND public_id='rejected'",[],|r|r.get::<_,u32>(0)).unwrap(),1);
            }
            assert_eq!(f.count(&format!("thinkthread.{method}")), 1);
        }
    }
}

#[tokio::test]
async fn failures_before_claim_settle_without_payload_and_release_local_resources() {
    for scenario in [
        "get-rejected",
        "get-wrong-branch",
        "get-hang",
        "connect-failed",
        "no-payload",
    ] {
        let f = Fixture::new().await;
        let a = f.access("unsent", write(vec![1])).await.unwrap();
        f.scenario(scenario);
        let original = std::fs::read(&f.config.host_executable).unwrap();
        if scenario == "connect-failed" {
            std::fs::remove_file(&f.config.host_executable).unwrap();
        }
        let result = if scenario == "no-payload" {
            f.owner()
                .recover_host("unsent".into(), repository(&f.root.path().join("db")))
                .await
                .map(|_| 0)
        } else {
            a.backend.write_host(write(vec![1])).await
        };
        assert!(
            matches!(result, Err(SessionDomainError::Unavailable { .. })),
            "{scenario}: {result:?}"
        );
        let h = f.intent("unsent").await;
        assert_eq!(h.state, "failed", "{scenario}");
        assert_eq!(h.cleanup_state, "done");
        assert!(h.observations.iter().all(Option::is_none));
        assert!(h.blocker_id.is_none());
        assert_eq!(f.count("thinkthread.write"), 0);
        assert_eq!(
            f.conn()
                .query_row(
                    "SELECT state FROM joint_host_allocations WHERE intent_id='unsent'",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "released"
        );
        assert_eq!(
            f.conn()
                .query_row(
                    "SELECT count(*) FROM joint_refs WHERE kind='host_hold'",
                    [],
                    |r| r.get::<_, u32>(0)
                )
                .unwrap(),
            0
        );
        let (dirty, holder):(u32,Option<String>)=f.conn().query_row("SELECT w.dirty,f.holder FROM joint_host_workspace w JOIN joint_fences f ON f.id='live' AND f.kind='source'",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
        assert_eq!((dirty, holder), (0, None));
        let snapshot = f.snapshot();
        assert!(matches!(
            f.owner()
                .recover_host("unsent".into(), repository(&f.root.path().join("db")))
                .await,
            Err(SessionDomainError::Unavailable { .. })
        ));
        assert_eq!(snapshot, f.snapshot());
        if scenario == "connect-failed" {
            std::fs::write(&f.config.host_executable, original).unwrap();
            std::fs::set_permissions(
                &f.config.host_executable,
                std::fs::Permissions::from_mode(0o700),
            )
            .unwrap();
        }
        f.scenario("ok");
        let next = f.access("next", write(vec![2])).await.unwrap();
        assert_eq!(next.backend.write_host(write(vec![2])).await.unwrap(), 1);
    }
}

#[tokio::test]
async fn interrupted_exec_recovers_only_original_uuid_after_reopen() {
    let f = Fixture::new().await;
    f.scenario("drop");
    let a = f.access("lost-exec", execute(exec())).await.unwrap();
    assert!(matches!(
        a.backend.exec().exec(exec()).await,
        Err(OperationError::Transport { .. })
    ));
    let h = f.intent("lost-exec").await;
    assert_eq!(h.state, "unknown");
    let original = h.steps[0].original_uuid.clone().unwrap();
    let native_digest = h.steps[0].sdk_operation_digest.clone().unwrap();
    drop(a);
    let repo = repository(&f.root.path().join("db"));
    f.scenario("ok");
    let result = f
        .owner()
        .recover_host("lost-exec".into(), repo.clone())
        .await
        .unwrap();
    assert_eq!(result["exit_code"], 0);
    assert_eq!(result["reason"], "exited");
    let h = repo.host_intent("lost-exec".into()).await.unwrap();
    assert_eq!(h.state, "completed");
    assert_eq!(h.steps[0].original_uuid.as_deref(), Some(original.as_str()));
    assert_eq!(
        h.steps[0].sdk_operation_digest.as_deref(),
        Some(native_digest.as_str())
    );
    assert_eq!(f.count("thinkthread.exec"), 1);
    assert_eq!(f.count("operation.get"), 1);
    let trace = f.trace();
    let query = trace
        .iter()
        .find(|v| v["method"] == "operation.get")
        .unwrap();
    assert_eq!(query["params"]["operationId"], original);
    let before = f.snapshot();
    assert_eq!(
        f.owner()
            .recover_host("lost-exec".into(), repo)
            .await
            .unwrap(),
        result
    );
    assert_eq!(before, f.snapshot());
    assert_eq!(f.count("thinkthread.exec"), 1);
}

#[tokio::test]
async fn reopened_intents_reject_changed_configuration_owner_schema_and_generations() {
    for change in [
        "configuration",
        "owner",
        "schema",
        "root-generation",
        "binding-generation",
    ] {
        let f = Fixture::new().await;
        f.scenario("drop");
        let a = f.access("persisted", write(vec![1])).await.unwrap();
        assert!(a.backend.write_host(write(vec![1])).await.is_err());
        drop(a);
        let mut config = f.config.clone();
        let mut owner = "candidate-fixture";
        match change {
            "configuration" => config.fs_root = f.root.path().join("changed-fs"),
            "owner" => owner = "changed-owner",
            "schema" => {
                f.conn()
                    .execute(
                        "UPDATE joint_host_intents SET body=json_set(body,'$.schema_version',2)",
                        [],
                    )
                    .unwrap();
            }
            "root-generation" => {
                f.conn()
                    .execute("UPDATE joint_host_roots SET generation=2", [])
                    .unwrap();
            }
            "binding-generation" => {
                f.conn()
                    .execute(
                        "UPDATE joint_bindings SET generation=2 WHERE id='live-binding'",
                        [],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let owner = CandidateHostOwner::new(
            config,
            owner.into(),
            "fixed".into(),
            Duration::from_millis(800),
        )
        .unwrap();
        let repo = repository(&f.root.path().join("db"));
        let snapshot = f.snapshot();
        f.scenario("ok");
        assert!(
            owner.recover_host("persisted".into(), repo).await.is_err(),
            "{change}"
        );
        assert_eq!(snapshot, f.snapshot(), "{change}");
        assert_eq!(f.count("thinkthread.write"), 1);
        assert_eq!(f.count("thinkthread.exec"), 0);
        assert_eq!(f.count("operation.get"), 0);
    }
}

#[tokio::test]
async fn parent_creation_failures_never_send_write_and_keep_real_directory_effects() {
    for scenario in [
        "ok",
        "mkdir-failed",
        "mkdir-drop",
        "mkdir-needs-recovery",
        "rejected-write",
    ] {
        let f = Fixture::new().await;
        f.scenario(scenario);
        let r = JointHostRequest::Write {
            path: "dir/file".into(),
            content: vec![0, 255],
            mode: WriteMode::Overwrite,
            create_parents: true,
        };
        let a = f.access("parent", r.clone()).await.unwrap();
        let result = a.backend.write_host(r).await;
        let h = f.intent("parent").await;
        assert_eq!(f.count("thinkthread.exec"), 1, "{scenario}");
        let unknown = matches!(scenario, "mkdir-drop" | "mkdir-needs-recovery");
        let reached_write = matches!(scenario, "ok" | "rejected-write");
        assert_eq!(
            f.count("thinkthread.write"),
            usize::from(reached_write),
            "{scenario}"
        );
        assert_eq!(result.is_ok(), scenario == "ok", "{scenario}");
        assert_eq!(
            h.state,
            if unknown {
                "unknown"
            } else if scenario == "ok" {
                "completed"
            } else {
                "failed"
            }
        );
        assert_eq!(h.observations[1].is_some(), reached_write);
        let trace: Vec<_> = f
            .trace()
            .into_iter()
            .filter(|v| {
                ["thinkthread.exec", "thinkthread.write"].contains(&v["method"].as_str().unwrap())
            })
            .collect();
        for (step, item) in trace.iter().enumerate() {
            assert_eq!(
                item["params"]["operationId"],
                h.steps[step].original_uuid.as_ref().unwrap().as_str()
            );
            assert_eq!(
                item["params"]["thinkthreadId"],
                h.prepared.target.native_child_id
            );
        }
        if reached_write {
            let state: Value =
                serde_json::from_slice(&std::fs::read(f.root.path().join("state.json")).unwrap())
                    .unwrap();
            assert_eq!(state["directories"], json!(["work/dir"]));
            assert_eq!(
                h.observations[0]
                    .as_ref()
                    .unwrap()
                    .exec
                    .as_ref()
                    .unwrap()
                    .code,
                Some(0)
            );
        }
        if unknown {
            assert!(h.blocker_id.is_some());
            assert!(f.access("next", write(vec![1])).await.is_err());
            assert!(f
                .repo
                .finish_host(a.access, json!({"error":"unsafe"}))
                .await
                .is_err());
            let sends = f.count("thinkthread.exec");
            f.scenario("needs-recovery");
            assert!(f
                .owner()
                .recover_host("parent".into(), repository(&f.root.path().join("db")))
                .await
                .is_err());
            assert_eq!(f.count("thinkthread.exec"), sends);
            assert_eq!(f.count("thinkthread.write"), 0);
            assert!(f
                .trace()
                .iter()
                .filter(|v| v["method"] == "operation.get")
                .all(|v| v["params"]["operationId"]
                    == h.steps[0].original_uuid.as_ref().unwrap().as_str()));
        }
    }
}

#[tokio::test]
async fn interrupted_parent_write_queries_the_correct_original_step_without_payload_replay() {
    let f = Fixture::new().await;
    // The fixture completes mkdir before dropping the write response.
    f.scenario("drop-write");
    let r = JointHostRequest::Write {
        path: "dir/file".into(),
        content: vec![0, 255],
        mode: WriteMode::Overwrite,
        create_parents: true,
    };
    let a = f.access("parent", r.clone()).await.unwrap();
    assert!(a.backend.write_host(r).await.is_err());
    let h = f.intent("parent").await;
    assert!(h.observations[0].as_ref().unwrap().settled);
    assert!(!h.observations[1].as_ref().unwrap().settled);
    f.scenario("ok");
    assert_eq!(
        f.owner()
            .recover_host("parent".into(), repository(&f.root.path().join("db")))
            .await
            .unwrap()["bytes_written"],
        2
    );
    assert_eq!(f.count("thinkthread.exec"), 1);
    assert_eq!(f.count("thinkthread.write"), 1);
    let queries: Vec<_> = f
        .trace()
        .into_iter()
        .filter(|v| v["method"] == "operation.get")
        .collect();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0]["params"]["operationId"],
        h.steps[1].original_uuid.as_ref().unwrap().as_str()
    );
}

#[tokio::test]
async fn artifact_bytes_and_provenance_commit_together_and_reject_changed_revisions() {
    for scenario in ["ok", "changed-revision", "changed-branch", "commit-failure"] {
        let f = Fixture::new().await;
        f.scenario(scenario);
        if scenario == "commit-failure" {
            f.conn().execute_batch("CREATE TRIGGER reject_artifact BEFORE UPDATE ON joint_host_intents WHEN json_extract(NEW.body,'$.artifact_evidence') IS NOT NULL BEGIN SELECT RAISE(ABORT,'artifact failpoint'); END;").unwrap();
        }
        let a = f.access("artifact", read()).await.unwrap();
        let result = a
            .backend
            .files()
            .read_bytes(ReadBytesRequest {
                path: BackendPath("/workspace/artifact".into()),
            })
            .await;
        let h = f.intent("artifact").await;
        if scenario == "ok" {
            let bytes = result.unwrap();
            assert!(bytes.len() > 65536);
            assert_eq!(&bytes[bytes.len() - 6..], b"\x00\xfftail");
            let e = h.artifact_evidence.as_ref().unwrap();
            assert_eq!(e.byte_length, bytes.len() as u64);
            assert_eq!(e.sha256, host_bytes_digest(&bytes));
            assert_eq!(e.native_child_id, f.target.native_child_id);
            assert_eq!(e.branch_id, f.target.branch_id);
            assert_eq!(e.live_binding_id, f.target.live_binding.binding_id);
            assert_eq!(e.binding_generation, f.target.binding_generation);
            assert_eq!(e.root_runtime_generation, f.target.root_runtime_generation);
            assert_eq!(e.configuration_revision, f.policy.configuration_revision);
            assert_eq!(e.relative_path, "work/artifact");
            assert_eq!(e.content_revision_before, "1");
            assert_eq!(e.content_revision_after, "1");
            assert!(e.completeness);
            assert!(e.bytes_ref.is_none());
            assert_eq!(
                STANDARD
                    .decode(h.result.as_ref().unwrap()["data_base64"].as_str().unwrap())
                    .unwrap(),
                bytes
            );
            let reopened = repository(&f.root.path().join("db"));
            assert_eq!(
                reopened
                    .host_intent("artifact".into())
                    .await
                    .unwrap()
                    .artifact_evidence,
                h.artifact_evidence
            );
        } else {
            assert!(result.is_err(), "{scenario}");
            assert!(h.artifact_evidence.is_none());
            if scenario == "commit-failure" {
                assert!(h.result.is_none());
                assert!(f.access("next", write(vec![1])).await.is_err());
            } else {
                assert_eq!(h.state, "failed");
                assert!(h.result.as_ref().unwrap().get("error").is_some());
            }
        }
    }
}

#[path = "support/native_host.rs"]
mod native_host;

#[tokio::test]
#[ignore = "requires explicitly provisioned matching native installation and model credentials"]
async fn native_current_view_host_bridge() {
    native_host::bridge().await;
}

#[tokio::test]
async fn accepted_mutation_rejections_query_only_original_identity_and_require_postconditions() {
    for method in ["write", "exec"] {
        for case in [
            "failed-old",
            "failed-new",
            "failed-directory",
            "failed-missing-file",
            "failed-read-error",
            "needs_recovery",
            "in_progress",
            "accepted",
            "not-found",
            "bad-digest",
            "bad-handle",
            "bad-target",
            "missing-error",
            "missing-completed",
        ] {
            let f = Fixture::new().await;
            let bytes = vec![0, 255, 7];
            let state = match case {
                "in_progress" => "running",
                "needs_recovery" | "accepted" | "not-found" => case,
                _ => "failed",
            };
            let mut patch = json!({"scenario":"accepted-error","error_code":"FileOperationFailed","delivery":"accepted","query_state":state,"files":{"work/artifact":STANDARD.encode(if case=="failed-new" {bytes.clone()}else{vec![2]})}});
            if case == "failed-directory" {
                patch["postcondition_read_error"] = json!("FileOperationFailed");
                patch["postcondition_read_message"] = json!(
                    "filesystem branch: invalid execution policy: target is not a regular file"
                );
            }
            if case == "failed-missing-file" {
                patch["postcondition_read_error"] = json!("FileOperationFailed");
                patch["postcondition_read_message"] =
                    json!("filesystem branch: read failed: No such file or directory (os error 2)");
            }
            if case == "failed-read-error" {
                patch["postcondition_read_error"] = json!("ThinkThreadNotFound");
            }
            for (name, field) in [
                ("bad-digest", "query_bad_digest"),
                ("bad-handle", "query_bad_handle"),
                ("bad-target", "query_error_target"),
                ("missing-error", "query_missing_error"),
                ("missing-completed", "query_missing_completed"),
            ] {
                if case == name {
                    patch[field] = json!(true);
                }
            }
            f.state(patch);
            let request = if method == "write" {
                write(bytes)
            } else {
                execute(exec())
            };
            let a = f.access("accepted", request.clone()).await.unwrap();
            if method == "write" {
                assert!(a.backend.write_host(request).await.is_err());
            } else {
                assert!(a.backend.exec().exec(exec()).await.is_err());
            }
            let h = f.intent("accepted").await;
            let settled = method == "write"
                && matches!(
                    case,
                    "failed-old" | "failed-directory" | "failed-missing-file"
                );
            assert_eq!(
                h.state,
                if settled { "failed" } else { "unknown" },
                "{method}/{case}"
            );
            assert_eq!(h.result.is_some(), settled, "{method}/{case}");
            assert_eq!(f.count(&format!("thinkthread.{method}")), 1);
            assert_eq!(f.count("operation.get"), 1);
            let query = f
                .trace()
                .into_iter()
                .find(|v| v["method"] == "operation.get")
                .unwrap();
            assert_eq!(
                query["params"]["operationId"],
                h.prepared.native[0].operation_id
            );
            assert!(!h.recovery_evidence.is_empty());
            if case != "not-found" {
                assert!(
                    h.recovery_evidence.iter().any(|e| e["state"] == state),
                    "{method}/{case}: no parsed original query view"
                );
            }
            let (dirty,holder):(u32,Option<String>)=f.conn().query_row("SELECT w.dirty,f.holder FROM joint_host_workspace w JOIN joint_fences f ON f.id='live' AND f.kind='source'",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
            assert_eq!(dirty, if settled { 0 } else { 1 });
            assert_eq!(holder.is_some(), !settled);
            let allocation: String = f
                .conn()
                .query_row(
                    "SELECT state FROM joint_host_allocations WHERE intent_id='accepted'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(allocation, if settled { "released" } else { "reserved" });
            let hold:u32=f.conn().query_row("SELECT count(*) FROM joint_refs WHERE kind='host_hold' AND public_id='accepted'",[],|r|r.get(0)).unwrap();
            assert_eq!(hold, u32::from(!settled));
            if settled {
                assert_eq!(h.cleanup_state, "done");
                assert!(h.blocker_id.is_none());
                let before = f.snapshot();
                assert!(f
                    .owner()
                    .recover_host("accepted".into(), repository(&f.root.path().join("db")))
                    .await
                    .is_err());
                assert_eq!(before, f.snapshot());
                assert!(f.access("next", write(vec![1])).await.is_ok());
            } else {
                assert!(h.blocker_id.is_some());
                let before = f.snapshot();
                assert!(f
                    .repo
                    .finish_host(a.access.clone(), json!({"error":"unsafe"}))
                    .await
                    .is_err());
                assert!(f.repo.finish_host_cleanup(a.access).await.is_err());
                assert!(f.access("next", write(vec![1])).await.is_err());
                assert_eq!(before, f.snapshot());
            }
        }
    }
}

#[tokio::test]
async fn read_generation_changes_before_observation_or_finish_persist_failure_without_bytes() {
    for before_finish in [false, true] {
        let f = Fixture::new().await;
        if before_finish {
            f.conn().execute_batch("CREATE TRIGGER change_generation AFTER UPDATE ON joint_host_intents WHEN json_extract(NEW.body,'$.observations[0].result.data_base64') IS NOT NULL AND json_extract(NEW.body,'$.result') IS NULL BEGIN UPDATE joint_bindings SET generation=generation+1 WHERE id='live-binding'; END;").unwrap();
        } else {
            f.scenario("local-generation-change");
        }
        let a = f.access("stale-read", read()).await.unwrap();
        assert!(a
            .backend
            .files()
            .read_bytes(ReadBytesRequest {
                path: BackendPath("artifact".into())
            })
            .await
            .is_err());
        let h = f.intent("stale-read").await;
        assert_eq!(h.state, "failed");
        assert_eq!(h.cleanup_state, "done");
        assert!(h.artifact_evidence.is_none());
        assert!(h.result.as_ref().unwrap()["error"]
            .as_str()
            .unwrap()
            .contains("generation changed"));
        assert!(h.result.as_ref().unwrap().get("data_base64").is_none());
        assert!(h.observations[0].as_ref().unwrap().result.is_null());
        assert_eq!(f.count("thinkthread.read"), 1);
        assert_eq!(f.count("thinkthread.write"), 0);
        let generation: u64 = f
            .conn()
            .query_row(
                "SELECT generation FROM joint_bindings WHERE id='live-binding'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(generation, 2);
        let (dirty,holder):(u32,Option<String>)=f.conn().query_row("SELECT w.dirty,f.holder FROM joint_host_workspace w JOIN joint_fences f ON f.id='live' AND f.kind='source'",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
        assert_eq!(dirty, 0);
        assert!(holder.is_none());
        assert_eq!(
            f.conn()
                .query_row(
                    "SELECT state FROM joint_host_allocations WHERE intent_id='stale-read'",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "released"
        );
        let before = f.snapshot();
        assert!(f
            .owner()
            .recover_host("stale-read".into(), repository(&f.root.path().join("db")))
            .await
            .is_err());
        assert_eq!(before, f.snapshot());
    }
}

#[tokio::test]
async fn stale_read_failure_cannot_release_a_replaced_writer_fence() {
    let f = Fixture::new().await;
    let a = f.access("stale", read()).await.unwrap();
    f.repo.claim_host_step(a.access.clone(), 0).await.unwrap();
    f.conn().execute_batch("UPDATE joint_bindings SET generation=2 WHERE id='live-binding'; UPDATE joint_fences SET epoch=epoch+1,holder='another-owner' WHERE kind='source' AND id='live';").unwrap();
    let before = f.snapshot();
    assert!(f.repo.fail_stale_host_read(a.access).await.is_err());
    assert_eq!(before, f.snapshot());
}
