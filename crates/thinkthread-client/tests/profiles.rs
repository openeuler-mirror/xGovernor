#![cfg(unix)]
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tempfile::TempDir;
use xgovernor_core::*;
use xgovernor_thinkthread_client::{
    host::management_config_digest, profile::*, root_owner::*, ManagementConfig,
};

struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
fn budget() -> JointBudgetPolicy {
    [
        ("session", 4, 1),
        ("root", 4, 0),
        ("operation", 4, 1),
        ("record_bytes", 4_000_000, 100_000),
    ]
    .into_iter()
    .map(|(kind, total, recovery_reserve)| {
        (
            kind.into(),
            JointResourceBudget {
                total,
                recovery_reserve,
            },
        )
    })
    .collect()
}
struct Fixture {
    root: TempDir,
    guard: Arc<DeploymentGuard>,
    clock: Arc<TestClock>,
    configurations: BTreeMap<String, ManagementConfig>,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let p = root.path();
        let host = p.join("host");
        let source = include_str!("support/lifecycle_host.py")
            .replace(
                "scenario == \"secret-reply\"",
                "scenario == \"owner-secret-reply\"",
            )
            .replace(
                "root = \"fixture-owned-root\"",
                "root = os.environ.get(\"XG_TEST_ROOT\", \"fixture-owned-root\")",
            );
        std::fs::write(&host, source).unwrap();
        std::fs::set_permissions(&host, std::fs::Permissions::from_mode(0o700)).unwrap();
        let namespace = p.join("locks");
        std::fs::create_dir(&namespace).unwrap();
        std::fs::set_permissions(&namespace, std::fs::Permissions::from_mode(0o700)).unwrap();
        let database = p.join("db");
        drop(SqliteSessionRepository::open(&database).unwrap());
        let mut configurations = BTreeMap::new();
        let mut resources = Vec::new();
        for name in ["A", "B"] {
            let base = p.join(name);
            std::fs::create_dir(&base).unwrap();
            resources.push(base.clone());
            for dir in ["state", "runtime", "config", "cache", "work", "fs", "home"] {
                std::fs::create_dir(base.join(dir)).unwrap();
            }
            std::fs::write(base.join("fs/sentinel"), format!("workspace-{name}")).unwrap();
            configurations.insert(
                name.into(),
                ManagementConfig {
                    host_executable: host.clone(),
                    state_root: base.join("state"),
                    runtime_root: base.join("runtime"),
                    config_root: base.join("config"),
                    cache_root: base.join("cache"),
                    working_directory: base.join("work"),
                    fs_root: base.join("fs"),
                    home_directory: base.join("home"),
                    executable_search_path: vec![PathBuf::from("/usr/bin")],
                    base_environment: BTreeMap::from([
                        ("XG_TEST_SCENARIO".into(), "owner".into()),
                        ("XG_TEST_ROOT".into(), format!("native-{name}")),
                        (
                            "XG_TEST_FS".into(),
                            base.join("fs").to_string_lossy().into(),
                        ),
                        (
                            "XG_TEST_TRACE".into(),
                            base.join("trace").to_string_lossy().into(),
                        ),
                        (
                            "XG_TEST_STATE".into(),
                            base.join("state.json").to_string_lossy().into(),
                        ),
                        (
                            "MODEL_API_KEY".into(),
                            "synthetic-private-model-value".into(),
                        ),
                    ]),
                    connect_timeout: Duration::from_secs(2),
                },
            );
        }
        let guard =
            DeploymentGuard::acquire(&database, "deployment", &namespace, &resources, &budget())
                .unwrap();
        guard
            .protect_server_values([
                "synthetic-admin-bearer".into(),
                "synthetic-tenant-bearer".into(),
                "synthetic-database-password".into(),
                "synthetic-operation-password".into(),
            ])
            .unwrap();
        Self {
            root,
            guard,
            clock: Arc::new(TestClock(AtomicU64::new(100))),
            configurations,
        }
    }
    fn profile(&self, name: &str) -> ExecutionProfile {
        let operations = BTreeSet::from([
            Capability::Message,
            Capability::HostRead,
            Capability::HostWrite,
            Capability::HostExec,
            Capability::OrdinaryCancel,
        ]);
        let release = ReleaseBinding {
            public_sdk_commit: "881ff5d49395bfe53904c2da2f8d6403db1b42fd".into(),
            release_version: "alpha11/0.1.0-25".into(),
            architecture: "aarch64-unknown-linux-gnu".into(),
            rpm_sha256: "5e9347d0ba04a6ee1781cf0e05e4fc396e1401d4e11904ca050993779b549d59".into(),
            management_fingerprint:
                "8e65d3658ebc674bf3f263b93c813e630e991bf870bc2118535ab0f2313d9fb6".into(),
            builtin_extension_version_ref: "builtin-xiaoo/v7".into(),
            endside_version_ref: "deployment/endside-sha256".into(),
            session_format_ref: "deployment/session-format".into(),
            compatibility_evidence_ref: "alpha11/compatibility".into(),
        };
        ExecutionProfile {
            profile_id: "research".into(),
            revision: name.into(),
            owner_kind: ROOT_OWNER_KIND.into(),
            runtime_kind: ROOT_RUNTIME_KIND.into(),
            backing_schema: "joint_v1".into(),
            deployment_id: "deployment".into(),
            context_ref: format!("context/{name}"),
            context_digest: management_config_digest(&self.configurations[name]),
            tt_profile_ref: "fixture".into(),
            model_refs: BTreeSet::from(["models/fixed-revision".into()]),
            secret_version_refs: BTreeMap::from([(
                "MODEL_API_KEY".into(),
                "model-secret/v1".into(),
            )]),
            release_ref: "alpha11".into(),
            release,
            declaration: CapabilityDeclaration {
                declaration_id: "alpha11/public-mappings".into(),
                schema_version: 1,
                release_ref: "alpha11".into(),
                public_contract_refs: BTreeSet::from(["management/v2".into()]),
                operation_subset: operations.clone(),
                usage_mode: UsageMode::Unknown,
                policy_subset: PolicySubset::Empty,
                cancel_mode: CancelMode::OrdinaryCheckpointRequired,
                observation_mode: ObservationMode::TerminalOnly,
                evidence_refs: operations
                    .iter()
                    .map(|c| (c.clone(), "alpha11/matching-evidence".into()))
                    .collect(),
            },
            workload: Workload::Research,
            role: ExecutionRole::Research,
            policy: RolePolicy::default(),
            required_capabilities: BTreeSet::from([Capability::Message]),
            research_limits: ResearchLimits {
                token_fuse_enabled: false,
                wall_timeout_ms: 1000,
                expansion_bound: 10,
            },
            enabled: true,
            admission_mode: AdmissionMode::Experiment,
            experiment_allowlist: vec![ExperimentPermit {
                approval_ref: "research/approval".into(),
                principal: "operator".into(),
                tenant_key: ADMIN_OWNER_REF.into(),
                profile_revision: name.into(),
                release_ref: "alpha11".into(),
                workload: Workload::Research,
                capability_subset: operations,
                expires_at_ms: 1000,
            }],
        }
    }
    fn manifest(&self, name: &str) -> DeploymentManifest {
        let p = self.profile(name);
        DeploymentManifest {
            host_sha256: host_bytes_digest(
                &std::fs::read(&self.configurations[name].host_executable).unwrap(),
            ),
            owner_kind: p.owner_kind,
            runtime_kind: p.runtime_kind,
            backing_schema: p.backing_schema,
            deployment_id: p.deployment_id,
            context_ref: p.context_ref,
            context_digest: p.context_digest,
            tt_profile_ref: p.tt_profile_ref,
            model_refs: p.model_refs,
            secret_version_refs: p.secret_version_refs,
            release_ref: p.release_ref,
            release: p.release,
            declaration: p.declaration,
        }
    }
    fn revision(&self, p: ExecutionProfile) -> Result<ProfileRevision, SessionDomainError> {
        let name = p.revision.split('.').next().unwrap().to_owned();
        let create=serde_json::from_value(json!({"agent":{"kind":"profile","name":"fixture"},"fs":self.configurations[&name].fs_root,"models":[],"allowSharedFs":false})).unwrap();
        ProfileRevision::root(
            p,
            &self.manifest(&name),
            self.configurations[&name].clone(),
            create,
            2000,
        )
    }
    fn router(
        &self,
        p: ExecutionProfile,
        joint: bool,
    ) -> Result<Arc<ProfileRouter>, SessionDomainError> {
        let name = p.revision.clone();
        ProfileRouter::assemble(
            self.guard.clone(),
            budget(),
            vec![self.revision(p)?],
            name,
            joint,
            self.clock.clone(),
        )
    }
    fn ctx(&self, revision: &str, operation: &str, id: &str) -> JointOperationContext {
        JointOperationContext {
            intent_id: format!("{operation}-{id}"),
            operation: operation.into(),
            tenant_id: None,
            owner_ref: ADMIN_OWNER_REF.into(),
            principal: "operator".into(),
            actor_role: Role::Admin,
            quota: TenantQuota::default(),
            runtime_kind: ROOT_RUNTIME_KIND.into(),
            configuration_revision: revision.into(),
            source_runtime_id: (operation != "open").then(|| id.into()),
            target_runtime_id: (operation == "open").then(|| id.into()),
            checkpoint_id: None,
        }
    }
    fn request(&self) -> session_protocol::SessionOpenRequest {
        serde_json::from_value(json!({"conversation_id":"conversation","sender_id":"sender"}))
            .unwrap()
    }
    async fn open(
        &self,
        router: &Arc<ProfileRouter>,
        name: &str,
        id: &str,
    ) -> Result<SessionRecord, SessionDomainError> {
        let request = self.request();
        let normalized = router
            .normalize(&SecurityContext::admin("operator"), &request)
            .await?;
        router
            .open(
                self.ctx(name, "open", id),
                JointOpenInput {
                    request,
                    normalized,
                    capabilities: EffectiveCapabilities {
                        runtime: BTreeSet::new(),
                        sandbox: BTreeSet::new(),
                    },
                },
                router.repository(),
            )
            .await
    }
    fn snapshot(&self) -> Value {
        use rusqlite::types::ValueRef;
        let db = rusqlite::Connection::open(self.root.path().join("db")).unwrap();
        let tables:Vec<String>=db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT IN ('local_deployment','execution_configurations','execution_defaults') ORDER BY name").unwrap().query_map([],|r|r.get(0)).unwrap().collect::<Result<_,_>>().unwrap();
        let mut result = BTreeMap::new();
        for table in tables {
            let mut stmt = db
                .prepare(&format!("SELECT * FROM \"{table}\" ORDER BY rowid"))
                .unwrap();
            let cols = stmt.column_count();
            let rows: Vec<Vec<Value>> = stmt
                .query_map([], |r| {
                    Ok((0..cols)
                        .map(|i| match r.get_ref_unwrap(i) {
                            ValueRef::Null => Value::Null,
                            ValueRef::Integer(n) => json!(n),
                            ValueRef::Real(n) => json!(n),
                            ValueRef::Text(t) => json!(String::from_utf8_lossy(t)),
                            ValueRef::Blob(b) => json!(b),
                        })
                        .collect())
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            result.insert(table, json!(rows));
        }
        for name in ["A", "B"] {
            result.insert(
                format!("workspace-{name}"),
                json!(std::fs::read(self.configurations[name].fs_root.join("sentinel")).unwrap()),
            );
        }
        json!(result)
    }
    fn trace(&self) -> Vec<Value> {
        ["A", "B"]
            .into_iter()
            .flat_map(|name| {
                std::fs::read_to_string(self.root.path().join(name).join("trace"))
                    .unwrap_or_default()
                    .lines()
                    .map(|s| serde_json::from_str(s).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }
    async fn stored(&self) -> Arc<ProfileRouter> {
        let r = self.router(self.profile("A"), true).unwrap();
        self.open(&r, "A", "existing").await.unwrap();
        r
    }
    async fn stored_with_pending(&self) -> Arc<ProfileRouter> {
        let router = self.stored().await;
        let owner=RootSessionOwner::new(self.configurations["A"].clone(),serde_json::from_value(json!({"agent":{"kind":"profile","name":"fixture"},"fs":self.configurations["A"].fs_root,"models":[],"allowSharedFs":false})).unwrap(),"A".into(),0,0,2000).unwrap();
        let session = router.repository().get("existing").await.unwrap().unwrap();
        let request=serde_json::from_value(json!({"runtime_id":"existing","text":"retained original input","client_request_id":"retained-original"})).unwrap();
        let turn = owner
            .admit_message(
                self.ctx("A", "turn", "existing"),
                session,
                request,
                "retained-turn".into(),
                router.repository(),
            )
            .await
            .unwrap();
        assert!(router
            .repository()
            .claim_joint_native_send(turn.admission, "message".into())
            .await
            .unwrap()
            .is_some());
        router
    }
    async fn reject(&self, mut p: ExecutionProfile, label: &str) {
        let original = p.revision.clone();
        p.revision = format!("{}.{}", original, uuid::Uuid::new_v4());
        for permit in &mut p.experiment_allowlist {
            if permit.profile_revision == original {
                permit.profile_revision = p.revision.clone();
            }
        }
        let before = self.snapshot();
        let trace = self.trace();
        match self.router(p, true) {
            Err(_) => {}
            Ok(router) => assert!(
                self.open(&router, router.configuration_revision(), "rejected")
                    .await
                    .is_err(),
                "{label}"
            ),
        }
        assert_eq!(self.snapshot(), before, "business state changed: {label}");
        assert_eq!(self.trace(), trace, "transport called: {label}");
        println!("sample {label}: rejected with unchanged business rows and workspace sentinels");
    }
}

#[tokio::test]
async fn disabled_profile_and_transaction_opt_in_are_independent() {
    for joint in [false, true] {
        for mode in [AdmissionMode::Disabled, AdmissionMode::Experiment] {
            let f = Fixture::new();
            let old = f.stored_with_pending().await;
            let mut p = f.profile("B");
            p.enabled = false;
            p.admission_mode = mode;
            let before = f.snapshot();
            let trace = f.trace();
            let router = f.router(p, joint).unwrap();
            assert!(f.open(&router, "B", "new").await.is_err());
            assert_eq!(f.snapshot(), before);
            assert_eq!(f.trace(), trace);
            assert!(old.repository().get("existing").await.unwrap().is_some());
        }
    }
    let f = Fixture::new();
    let r = f.router(f.profile("B"), true).unwrap();
    assert!(f.open(&r, "B", "positive").await.is_ok());
    let mut value = serde_json::to_value(f.profile("B")).unwrap();
    value.as_object_mut().unwrap().remove("enabled");
    value.as_object_mut().unwrap().remove("admission_mode");
    let p = ExecutionProfile::parse(&value.to_string()).unwrap();
    assert!(!p.enabled);
    assert_eq!(p.admission_mode, AdmissionMode::Disabled);
}
#[tokio::test]
async fn principal_tenant_revision_and_workload_mismatches_are_side_effect_free() {
    let f = Fixture::new();
    let old = f.stored_with_pending().await;
    for field in ["tenant_key", "principal", "profile_revision", "release_ref"] {
        let mut p = f.profile("B");
        match field {
            "tenant_key" => p.experiment_allowlist[0].tenant_key = "tenant/other".into(),
            "principal" => p.experiment_allowlist[0].principal = "other".into(),
            "profile_revision" => p.experiment_allowlist[0].profile_revision = "A".into(),
            _ => p.experiment_allowlist[0].release_ref = "other".into(),
        };
        f.reject(p, field).await;
    }
    let mut p = f.profile("B");
    p.workload = Workload::DefaultMcts;
    f.reject(p, "workload").await;
    let router = f.router(f.profile("B"), true).unwrap();
    let before = f.snapshot();
    for c in [
        SecurityContext::tenant("other", "operator"),
        SecurityContext::admin("other"),
    ] {
        assert!(router.normalize(&c, &f.request()).await.is_err());
    }
    assert_eq!(f.snapshot(), before);
    assert!(old.repository().get("existing").await.unwrap().is_some());
}
#[tokio::test]
async fn permit_expiry_missing_approval_and_missing_entry_reject_new_opens() {
    for (label, expiry, approval, missing) in [
        ("at-expiry", 100, "research/approval", false),
        ("expired", 99, "research/approval", false),
        ("no-approval", 1000, "", false),
        ("no-permit", 1000, "research/approval", true),
    ] {
        let f = Fixture::new();
        let _old = f.stored_with_pending().await;
        let mut p = f.profile("B");
        p.experiment_allowlist[0].expires_at_ms = expiry;
        p.experiment_allowlist[0].approval_ref = approval.into();
        if missing {
            p.experiment_allowlist.clear();
        }
        f.reject(p, label).await;
    }
    let f = Fixture::new();
    let r = f.router(f.profile("B"), true).unwrap();
    f.clock.0.store(999, Ordering::SeqCst);
    assert!(r
        .normalize(&SecurityContext::admin("operator"), &f.request())
        .await
        .is_ok());
    f.clock.0.store(1000, Ordering::SeqCst);
    assert!(r
        .normalize(&SecurityContext::admin("operator"), &f.request())
        .await
        .is_err());
}
#[tokio::test]
async fn strict_schema_rejects_unknown_nested_configuration() {
    let f = Fixture::new();
    let _old = f.stored_with_pending().await;
    let before = f.snapshot();
    let trace = f.trace();
    for path in [
        "unknown",
        "declaration",
        "release",
        "policy",
        "research_limits",
    ] {
        let mut v = serde_json::to_value(f.profile("B")).unwrap();
        if path == "unknown" {
            v["launcher"] = json!("synthetic-admin-bearer");
        } else {
            v[path]["unknown"] = json!({"nested":["synthetic-tenant-bearer"]});
        }
        let error = ExecutionProfile::parse(&v.to_string()).err().unwrap();
        assert!(!error.to_string().contains("synthetic-"));
    }
    for (key, value) in [
        ("backing_schema", json!("unknown")),
        ("context_digest", json!("unmatched")),
        ("release_ref", json!("other")),
    ] {
        let mut v = serde_json::to_value(f.profile("B")).unwrap();
        v[key] = value;
        f.reject(ExecutionProfile::parse(&v.to_string()).unwrap(), key)
            .await;
    }
    let mut v = serde_json::to_value(f.profile("B")).unwrap();
    v["declaration"]["operation_subset"] = json!(["unknown_capability"]);
    assert!(ExecutionProfile::parse(&v.to_string()).is_err());
    assert_eq!(f.snapshot(), before);
    assert_eq!(f.trace(), trace);
}
#[tokio::test]
async fn fixed_release_checks_every_field_and_missing_value() {
    let f = Fixture::new();
    let _old = f.stored_with_pending().await;
    let base = serde_json::to_value(f.profile("B")).unwrap();
    let keys: Vec<_> = base["release"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    for key in keys {
        for missing in [false, true] {
            let mut v = base.clone();
            if missing {
                v["release"].as_object_mut().unwrap().remove(&key);
                assert!(ExecutionProfile::parse(&v.to_string()).is_err());
            } else {
                v["release"][&key] = json!("unmatched");
                f.reject(
                    ExecutionProfile::parse(&v.to_string()).unwrap(),
                    &format!("release/{key}"),
                )
                .await;
            }
        }
    }
}
#[tokio::test]
async fn context_model_and_secret_versions_are_frozen_and_required() {
    let f = Fixture::new();
    let _old = f.stored_with_pending().await;
    for field in ["context_ref", "deployment_id", "tt_profile_ref"] {
        let mut v = serde_json::to_value(f.profile("B")).unwrap();
        v[field] = json!("unmatched");
        f.reject(ExecutionProfile::parse(&v.to_string()).unwrap(), field)
            .await;
    }
    let mut p = f.profile("B");
    p.model_refs.clear();
    f.reject(p, "missing-model").await;
    let mut p = f.profile("B");
    p.secret_version_refs
        .insert("MODEL_API_KEY".into(), "v2".into());
    f.reject(p, "secret-version").await;
    let mut p = f.profile("B");
    p.secret_version_refs.clear();
    f.reject(p, "missing-secret-reference").await;
    assert_ne!(
        f.configurations["A"].state_root,
        f.configurations["A"].config_root
    );
    let references = rusqlite::Connection::open(f.root.path().join("db"))
        .unwrap()
        .query_row(
            "SELECT references_json FROM execution_configurations WHERE revision='A'",
            [],
            |r| r.get::<_, String>(0),
        )
        .unwrap();
    assert!(!references.contains("synthetic-private-model-value"));
    assert!(references.contains("model-secret/v1"));
}
#[tokio::test]
async fn hard_policies_reject_all_roles_before_creation_and_turn() {
    for role in [
        ExecutionRole::Author,
        ExecutionRole::Solver,
        ExecutionRole::Research,
    ] {
        for field in ["init_limits", "step_limits", "tools_disabled", "unattended"] {
            let f = Fixture::new();
            let old = f.stored_with_pending().await;
            let mut p = f.profile("B");
            p.role = role.clone();
            match field {
                "init_limits" => {
                    p.policy.init_limits = Some(PolicyLimits {
                        max_tokens: 1,
                        timeout_ms: 1,
                    })
                }
                "step_limits" => {
                    p.policy.step_limits = Some(PolicyLimits {
                        max_tokens: 1,
                        timeout_ms: 1,
                    })
                }
                "tools_disabled" => p.policy.tools_disabled = Some(true),
                _ => p.policy.unattended = Some(true),
            };
            f.reject(p, field).await;
            let before = f.snapshot();
            let trace = f.trace();
            let session = old.repository().get("existing").await.unwrap().unwrap();
            let request=serde_json::from_value(json!({"runtime_id":"existing","text":"unchanged text","ext":{"policy":{field:true}}})).unwrap();
            assert!(old
                .submit_turn(
                    f.ctx("A", "turn", "existing"),
                    session,
                    request,
                    "denied-turn".into(),
                    old.repository()
                )
                .await
                .is_err());
            assert_eq!(f.snapshot(), before);
            assert_eq!(f.trace(), trace);
        }
    }
}
#[tokio::test]
async fn role_and_policy_schema_do_not_drop_unknown_fields() {
    let f = Fixture::new();
    let p = serde_json::to_value(f.profile("B")).unwrap();
    for (key, value) in [
        ("role", json!("unknown")),
        ("policy", json!({"step_override":{"nested":true}})),
        (
            "policy",
            json!({"step_limits":{"max_tokens":1,"timeout_ms":1,"unknown":2}}),
        ),
        ("policy", json!({"unattended":"true"})),
    ] {
        let mut v = p.clone();
        v[key] = value;
        assert!(ExecutionProfile::parse(&v.to_string()).is_err());
    }
}
#[tokio::test]
async fn supported_message_retains_text_and_reports_unknown_usage() {
    let f = Fixture::new();
    let router = f.stored().await;
    let session = router.repository().get("existing").await.unwrap().unwrap();
    let request=serde_json::from_value(json!({"runtime_id":"existing","text":"return exactly kept-text.","client_request_id":"kept-id"})).unwrap();
    let submission = router
        .submit_turn(
            f.ctx("A", "turn", "existing"),
            session,
            request,
            "turn".into(),
            router.repository(),
        )
        .await
        .unwrap();
    let terminal = submission.terminal.unwrap().await.unwrap();
    assert_eq!(terminal.reply.as_deref(), Some("kept-text"));
    let result = router
        .repository()
        .get_joint_turn("existing".into(), "kept-id".into())
        .await
        .unwrap()
        .unwrap();
    assert!(result.terminal.is_some());
    let encoded = serde_json::to_value(result).unwrap();
    assert!(encoded.get("usage").is_none());
    let trace = f.trace();
    let send = trace
        .iter()
        .find(|v| v["method"] == "message.send")
        .unwrap();
    assert_eq!(send["params"]["text"], "return exactly kept-text.");
    assert_ne!(
        serde_json::to_value(session_protocol::SessionUsageReport::Unknown).unwrap(),
        serde_json::to_value(session_protocol::SessionUsageReport::Known(
            session_protocol::SessionUsage::default()
        ))
        .unwrap()
    );
}
#[tokio::test]
async fn unsupported_requirements_reject_individually_and_in_combinations() {
    let f = Fixture::new();
    let _old = f.stored_with_pending().await;
    for caps in [
        vec![Capability::TrustedUsage],
        vec![Capability::TokenFuse],
        vec![Capability::TrustedUsage, Capability::TokenFuse],
        vec![Capability::HardPolicy],
        vec![Capability::Isolation],
        vec![Capability::ControlProtection],
        vec![Capability::TaskImage],
        vec![Capability::StructuredEvents],
        vec![Capability::NewIdentity],
        vec![Capability::DurableStorage],
        vec![Capability::StorageUsage],
        vec![Capability::StorageLimit],
        vec![Capability::CancelContinuation],
        vec![Capability::CompleteReply],
    ] {
        let mut p = f.profile("B");
        p.required_capabilities = caps.iter().cloned().collect();
        f.reject(p, &format!("{caps:?}")).await;
    }
}
#[tokio::test]
async fn research_exception_requires_explicit_fuse_and_finite_bounds() {
    let f = Fixture::new();
    let _old = f.stored_with_pending().await;
    for (fuse, wall, bound) in [
        (true, 1000, 1),
        (false, 0, 1),
        (false, 300001, 1),
        (false, 1, 0),
        (false, u64::MAX, 1),
        (false, 1, u64::MAX),
    ] {
        let mut p = f.profile("B");
        p.research_limits = ResearchLimits {
            token_fuse_enabled: fuse,
            wall_timeout_ms: wall,
            expansion_bound: bound,
        };
        f.reject(p, &format!("limits/{fuse}/{wall}/{bound}")).await;
    }
    for field in ["token_fuse_enabled", "wall_timeout_ms", "expansion_bound"] {
        let mut v = serde_json::to_value(f.profile("B")).unwrap();
        v["research_limits"].as_object_mut().unwrap().remove(field);
        assert!(ExecutionProfile::parse(&v.to_string()).is_err());
    }
}
#[tokio::test]
async fn new_default_routes_existing_records_to_the_original_revision() {
    let f = Fixture::new();
    let first = f.stored_with_pending().await;
    let original = first.repository().get("existing").await.unwrap().unwrap();
    let old_snapshot = f.snapshot();
    let router = ProfileRouter::assemble(
        f.guard.clone(),
        budget(),
        vec![
            f.revision(f.profile("A")).unwrap(),
            f.revision(f.profile("B")).unwrap(),
        ],
        "B".into(),
        true,
        f.clock.clone(),
    )
    .unwrap();
    assert!(f.open(&router, "B", "new-default").await.is_ok());
    let read_trace = |name: &str| {
        std::fs::read_to_string(f.root.path().join(name).join("trace"))
            .unwrap_or_default()
            .lines()
            .map(|s| serde_json::from_str::<Value>(s).unwrap())
            .collect::<Vec<_>>()
    };
    let before_a = read_trace("A");
    let before_b = read_trace("B");
    router
        .ensure_attached(&original, router.repository())
        .await
        .unwrap();
    let after_a = read_trace("A");
    let after_b = read_trace("B");
    assert!(after_a[before_a.len()..]
        .iter()
        .any(|v| v["params"]["thinkthreadId"] == "native-A"));
    assert_eq!(after_b, before_b);
    assert_eq!(
        router.repository().get("existing").await.unwrap().unwrap(),
        original
    );
    assert_eq!(f.snapshot()["workspace-A"], old_snapshot["workspace-A"]);
}
#[tokio::test]
async fn disabled_configuration_preserves_original_readers_and_owner() {
    let f = Fixture::new();
    let old = f.stored_with_pending().await;
    let original = old.repository().get("existing").await.unwrap().unwrap();
    let before = f.snapshot();
    let trace = f.trace();
    let mut p = f.profile("A");
    p.enabled = false;
    let disabled = f.router(p, true).unwrap();
    assert!(f.open(&disabled, "A", "new").await.is_err());
    assert_eq!(f.snapshot(), before);
    assert_eq!(f.trace(), trace);
    disabled
        .ensure_attached(&original, disabled.repository())
        .await
        .unwrap();
    assert_eq!(
        disabled
            .repository()
            .get("existing")
            .await
            .unwrap()
            .unwrap(),
        original
    );
}
#[tokio::test]
async fn missing_original_revision_fails_without_using_new_defaults() {
    let f = Fixture::new();
    let old = f.stored_with_pending().await;
    let original = old.repository().get("existing").await.unwrap().unwrap();
    let router = f.router(f.profile("B"), true).unwrap();
    let before = f.snapshot();
    let trace = f.trace();
    assert!(router
        .ensure_attached(&original, router.repository())
        .await
        .is_err());
    assert_eq!(f.snapshot(), before);
    assert_eq!(f.trace(), trace);
}
#[tokio::test]
async fn unavailable_original_dependencies_do_not_fall_back_to_new_defaults() {
    for missing in ["reader", "configuration", "secret_version", "release"] {
        let mut f = Fixture::new();
        let old = f.stored_with_pending().await;
        let original = old.repository().get("existing").await.unwrap().unwrap();
        let b = f.revision(f.profile("B")).unwrap();
        let mut revisions = vec![b];
        match missing {
            "reader" => {}
            "configuration" => {
                revisions.push(f.revision(f.profile("A")).unwrap());
            }
            "secret_version" => {
                f.configurations
                    .get_mut("A")
                    .unwrap()
                    .base_environment
                    .remove("MODEL_API_KEY");
                assert!(f.revision(f.profile("A")).is_err());
            }
            "release" => {
                let mut a = f.profile("A");
                a.release.endside_version_ref = "unavailable/endside".into();
                assert!(f.revision(a).is_err());
            }
            _ => unreachable!(),
        }
        let router = ProfileRouter::assemble(
            f.guard.clone(),
            budget(),
            revisions,
            "B".into(),
            true,
            f.clock.clone(),
        )
        .unwrap();
        if missing == "configuration" {
            let db = rusqlite::Connection::open(f.root.path().join("db")).unwrap();
            db.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
            db.execute(
                "DELETE FROM execution_configurations WHERE revision='A'",
                [],
            )
            .unwrap();
        }
        let before = f.snapshot();
        let trace = f.trace();
        assert!(router
            .ensure_attached(&original, router.repository())
            .await
            .is_err());
        let pending = router
            .repository()
            .pending_joint_operations()
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert!(router
            .reconcile(pending[0].clone(), router.repository())
            .await
            .is_err());
        assert_eq!(f.snapshot(), before);
        assert_eq!(f.trace(), trace);
        assert_eq!(
            router.repository().get("existing").await.unwrap(),
            Some(original)
        );
        eprintln!("sample missing original {missing}: unchanged records, UUID and transport");
    }
}
#[tokio::test]
async fn normalizer_rejects_llm_extensions_provider_options_and_policies() {
    let f = Fixture::new();
    let _old = f.stored_with_pending().await;
    let router = f.router(f.profile("B"), true).unwrap();
    for (field, value) in [
        (
            "llm",
            json!({"model":"other","api_key":"synthetic-private-model-value"}),
        ),
        ("ext", json!({"runtime_pi":{"backend_id":"local"}})),
        ("ext", json!({"runtime_xiaoo":{"backend_id":"e2b"}})),
        ("ext", json!({"policy":{"tools_disabled":true}})),
        (
            "deployment",
            json!({"options":{"provider_options":{"nested":"synthetic-admin-bearer"}}}),
        ),
        ("deployment", json!({"profile":"inline-selection"})),
    ] {
        let before = f.snapshot();
        let trace = f.trace();
        let mut v = serde_json::to_value(f.request()).unwrap();
        v[field] = value;
        let req = serde_json::from_value(v).unwrap();
        let error = router
            .normalize(&SecurityContext::admin("operator"), &req)
            .await
            .err()
            .unwrap();
        assert!(!error.to_string().contains("synthetic-"));
        assert_eq!(f.snapshot(), before);
        assert_eq!(f.trace(), trace);
    }
}
#[tokio::test]
async fn public_projections_do_not_contain_backend_or_context_identities() {
    let f = Fixture::new();
    let r = f.stored_with_pending().await;
    let session = r.repository().get("existing").await.unwrap().unwrap();
    let value = serde_json::to_string(&project_session(&session)).unwrap();
    for private in [
        "native-A",
        "root-binding-existing",
        "model-secret/v1",
        "synthetic-private-model-value",
        f.configurations["A"].state_root.to_str().unwrap(),
    ] {
        assert!(!value.contains(private), "public identity leak: {private}");
    }
}
#[tokio::test]
async fn invalid_agent_paths_and_unapproved_extensions_are_rejected() {
    let f = Fixture::new();
    let _old = f.stored_with_pending().await;
    let before = f.snapshot();
    for field in [
        "launcher",
        "extension",
        "permit",
        "tool_entry",
        "agent_control",
    ] {
        let mut v = serde_json::to_value(f.profile("B")).unwrap();
        v[field] = json!({"nested":{"enabled":true}});
        assert!(ExecutionProfile::parse(&v.to_string()).is_err());
    }
    let mut p = f.profile("B");
    p.tt_profile_ref = "old-poc-profile".into();
    f.reject(p, "old-poc-profile").await;
    let mut p = f.profile("B");
    p.declaration
        .evidence_refs
        .insert(Capability::Message, "old-poc/evidence".into());
    f.reject(p, "old-poc-evidence").await;
    assert_eq!(f.snapshot(), before);
}
#[tokio::test]
async fn admission_modes_cannot_enable_default_workloads_or_production() {
    let f = Fixture::new();
    let _old = f.stored_with_pending().await;
    for mode in [
        AdmissionMode::Disabled,
        AdmissionMode::Experiment,
        AdmissionMode::ProductionCandidate,
    ] {
        for workload in [Workload::Research, Workload::DefaultMcts] {
            if mode == AdmissionMode::Experiment && workload == Workload::Research {
                continue;
            }
            let mut p = f.profile("B");
            p.admission_mode = mode;
            p.workload = workload;
            f.reject(p, "mode/workload").await;
        }
    }
    let mut v = serde_json::to_value(f.profile("B")).unwrap();
    v["admission_mode"] = json!("unknown");
    assert!(ExecutionProfile::parse(&v.to_string()).is_err());
}
#[tokio::test]
async fn permits_cannot_expand_backend_capabilities_or_trust_request_identity() {
    let f = Fixture::new();
    let _old = f.stored_with_pending().await;
    let mut p = f.profile("B");
    p.experiment_allowlist[0]
        .capability_subset
        .insert(Capability::Isolation);
    f.reject(p, "expanded-permit").await;
    let router = f.router(f.profile("B"), true).unwrap();
    for field in ["sender_id", "ext"] {
        let before = f.snapshot();
        let mut v = serde_json::to_value(f.request()).unwrap();
        v[field] = if field == "sender_id" {
            json!("operator")
        } else {
            json!({"principal":"operator","tenant_key":"admin"})
        };
        let req = serde_json::from_value(v).unwrap();
        assert!(router
            .normalize(&SecurityContext::admin("unapproved"), &req)
            .await
            .is_err());
        assert_eq!(f.snapshot(), before);
    }
}
#[tokio::test]
async fn declarations_do_not_promote_isolation_events_or_cancellation_continuation() {
    let f = Fixture::new();
    let _old = f.stored_with_pending().await;
    let p = f.profile("B");
    assert_eq!(p.declaration.usage_mode, UsageMode::Unknown);
    assert_eq!(
        p.declaration.observation_mode,
        ObservationMode::TerminalOnly
    );
    assert_eq!(
        p.declaration.cancel_mode,
        CancelMode::OrdinaryCheckpointRequired
    );
    for cap in [
        Capability::Isolation,
        Capability::StructuredEvents,
        Capability::CancelContinuation,
        Capability::DurableStorage,
    ] {
        let mut p = f.profile("B");
        p.declaration.operation_subset.insert(cap.clone());
        p.declaration
            .evidence_refs
            .insert(cap, "dedicated-server/ownership".into());
        f.reject(p, "unverified-declaration").await;
    }
    for cap in [
        Capability::Message,
        Capability::HostRead,
        Capability::HostWrite,
        Capability::HostExec,
        Capability::OrdinaryCancel,
    ] {
        let mut p = f.profile("B");
        p.declaration.evidence_refs.remove(&cap);
        f.reject(p, "missing-mapping-evidence").await;
    }
}

use xgovernor_core::runtime_protocol as rt;
struct BoundaryRuntime;
#[async_trait::async_trait]
impl rt::AgentRuntime for BoundaryRuntime {
    fn runtime_kind(&self) -> &str {
        ROOT_RUNTIME_KIND
    }
    fn capabilities(&self) -> BTreeSet<rt::RuntimeCapability> {
        BTreeSet::new()
    }
    async fn start(
        &self,
        _: rt::RuntimeStartRequest,
        _: rt::RuntimeExecutionContext,
    ) -> Result<(), rt::RuntimeError> {
        panic!("split start invoked")
    }
    async fn attach(
        &self,
        _: &str,
        _: rt::RuntimeExecutionContext,
    ) -> Result<(), rt::RuntimeError> {
        panic!("split attach invoked")
    }
    async fn stop(&self, _: &str) -> Result<(), rt::RuntimeError> {
        panic!("split stop invoked")
    }
    async fn check_alive(&self, _: &str) -> Result<bool, rt::RuntimeError> {
        panic!("split health invoked")
    }
    async fn submit_turn(
        &self,
        _: rt::RuntimeTurnRequest,
    ) -> Result<rt::RuntimeEventReceiver, rt::RuntimeError> {
        panic!("split turn invoked")
    }
    async fn answer_interaction(
        &self,
        _: rt::RuntimeInteractionRequest,
    ) -> Result<(), rt::RuntimeError> {
        panic!("split interaction invoked")
    }
    async fn cancel(&self, _: rt::RuntimeCancelRequest) -> Result<(), rt::RuntimeError> {
        panic!("split cancel invoked")
    }
}
struct Ids(AtomicU64);
impl RuntimeIdGenerator for Ids {
    fn next_runtime_id(&self) -> String {
        format!("public-{}", self.0.fetch_add(1, Ordering::SeqCst))
    }
}
impl TurnIdGenerator for Ids {
    fn next_turn_id(&self) -> String {
        format!("turn-{}", self.0.fetch_add(1, Ordering::SeqCst))
    }
}
fn application(f: &Fixture, router: &Arc<ProfileRouter>) -> SessionApplication {
    let ids = Arc::new(Ids(AtomicU64::new(1)));
    SessionApplication::with_runtime_registry(
        ROOT_RUNTIME_KIND,
        [RuntimeRegistration::new(
            Arc::new(BoundaryRuntime),
            Default::default(),
            router.clone(),
        )
        .with_joint_lifecycle(router.clone())],
        router.repository(),
        ids.clone(),
        ids,
        f.clock.clone(),
    )
    .unwrap()
}
fn http(f: &Fixture, router: &Arc<ProfileRouter>) -> axum::Router {
    use xgovernor_server::{
        httpserver::auth::security_layer, session_router, SessionHttpState, TokenTable,
    };
    let tokens = TokenTable::new(std::collections::HashMap::from([
        (
            "synthetic-admin-bearer".into(),
            SecurityContext::admin("operator"),
        ),
        (
            "synthetic-other-admin".into(),
            SecurityContext::admin("other"),
        ),
        (
            "synthetic-tenant-bearer".into(),
            SecurityContext::tenant("tenant-A", "operator"),
        ),
    ]));
    security_layer(
        session_router(Arc::new(SessionHttpState::new(application(f, router)))),
        Some(tokens),
    )
}
async fn call(
    router: &axum::Router,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Value,
) -> (axum::http::StatusCode, String) {
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let mut request = axum::http::Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = router
        .clone()
        .oneshot(
            request
                .body(axum::body::Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = tokio::time::timeout(Duration::from_secs(8), response.into_body().collect())
        .await
        .unwrap()
        .unwrap()
        .to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}
fn assert_safe(value: &str) {
    for secret in [
        "synthetic-admin-bearer",
        "synthetic-tenant-bearer",
        "synthetic-private-model-value",
        "synthetic-database-password",
        "synthetic-operation-password",
    ] {
        assert!(!value.contains(secret), "protected value exposed");
    }
}
#[tokio::test]
async fn authenticated_http_rejects_forged_identity_and_preserves_original_queries() {
    let f = Fixture::new();
    let _old = f.stored_with_pending().await;
    let router = ProfileRouter::assemble(
        f.guard.clone(),
        budget(),
        vec![
            f.revision(f.profile("A")).unwrap(),
            f.revision(f.profile("B")).unwrap(),
        ],
        "B".into(),
        true,
        f.clock.clone(),
    )
    .unwrap();
    let transport = http(&f, &router);
    for token in [
        None,
        Some("invalid-token"),
        Some("synthetic-other-admin"),
        Some("synthetic-tenant-bearer"),
    ] {
        for forged in [false, true] {
            let before = f.snapshot();
            let trace = f.trace();
            let mut request =
                json!({"runtime_id":"new","sender_id":"operator","conversation_id":"conversation"});
            if forged {
                request["ext"] = json!({"principal":"operator","tenant_key":"admin"});
            }
            let (status, body) =
                call(&transport, "POST", "/api/v1/sessions/open", token, request).await;
            assert!(!status.is_success());
            if token.is_none() || token == Some("invalid-token") {
                assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
            }
            assert_safe(&body);
            assert_eq!(f.snapshot(), before);
            assert_eq!(f.trace(), trace);
        }
    }
    let (status, body) = call(
        &transport,
        "POST",
        "/api/v1/sessions/open",
        Some("synthetic-tenant-bearer"),
        json!({"runtime_id":"existing","conversation_id":"conversation","sender_id":"sender"}),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
    assert_safe(&body);
    for (method, path, body) in [
        (
            "POST",
            "/api/v1/sessions/open",
            json!({"runtime_id":"existing","conversation_id":"conversation","sender_id":"sender"}),
        ),
        ("GET", "/api/v1/sessions", Value::Null),
    ] {
        let (status, body) = call(
            &transport,
            method,
            path,
            Some("synthetic-admin-bearer"),
            body,
        )
        .await;
        assert!(status.is_success(), "{body}");
        assert_safe(&body);
        for private in [
            "native-A",
            "native-B",
            "root-binding-existing",
            f.configurations["A"].state_root.to_str().unwrap(),
            f.configurations["A"].config_root.to_str().unwrap(),
        ] {
            assert!(!body.contains(private));
        }
    }
    let before = f.snapshot();
    let trace = f.trace();
    for path in ["/api/v1/sessions/close", "/api/v1/sessions/cancel"] {
        let (status, body) = call(
            &transport,
            "POST",
            path,
            Some("synthetic-admin-bearer"),
            json!({"runtime_id":"existing","conversation_id":"conversation","sender_id":"sender"}),
        )
        .await;
        assert!(!status.is_success());
        assert_safe(&body);
    }
    assert_eq!(f.snapshot(), before);
    assert_eq!(f.trace(), trace);
}
#[tokio::test]
async fn public_receipt_and_terminal_keep_unknown_distinct_from_trusted_zero() {
    let f = Fixture::new();
    let router = f.stored().await;
    let transport = http(&f, &router);
    let (status,body)=call(&transport,"POST","/api/v1/sessions/turns",Some("synthetic-admin-bearer"),json!({"runtime_id":"existing","text":"return exactly public-text.","client_request_id":"public-key"})).await;
    assert!(status.is_success(), "{body}");
    assert_safe(&body);
    let receipt: session_protocol::SessionSubmitReceipt = serde_json::from_str(&body).unwrap();
    let (status, sse) = call(
        &transport,
        "GET",
        &format!("/api/v1/sessions/existing/turns/{}/events", receipt.turn_id),
        Some("synthetic-admin-bearer"),
        Value::Null,
    )
    .await;
    assert!(status.is_success());
    assert_safe(&sse);
    assert!(sse.contains("public-text"));
    assert!(sse.contains("\"usage\":\"unknown\""));
    assert!(!sse.contains("thinking_delta"));
    assert!(!sse.contains("tool_activity"));
    let public = serde_json::to_value(session_protocol::SessionUsageReport::Unknown).unwrap();
    let zero = serde_json::to_value(session_protocol::SessionUsageReport::Known(
        Default::default(),
    ))
    .unwrap();
    assert_ne!(public, zero);
    assert_eq!(public, json!("unknown"));
    let reopened = f.guard.repository(budget()).unwrap();
    let turn = reopened
        .get_joint_turn("existing".into(), "public-key".into())
        .await
        .unwrap()
        .unwrap();
    assert!(turn.terminal.is_some());
    let encoded = serde_json::to_string(&turn).unwrap();
    assert!(!encoded.contains("input_tokens"));
    assert!(!encoded.contains("output_tokens"));
}
#[tokio::test]
async fn unsupported_turn_fields_and_checkpoint_entries_are_side_effect_free() {
    let f = Fixture::new();
    let router = f.stored_with_pending().await;
    let transport = http(&f, &router);
    for extra in [
        json!({"usage_requirement":"trusted"}),
        json!({"token_fuse":true}),
        json!({"usage_requirement":"trusted","token_fuse":true}),
        json!({"policy":{"init_limits":{"max_tokens":1},"step_limits":{"max_tokens":1}}}),
        json!({"policy":{"tools_disabled":true}}),
        json!({"policy":{"unattended":true}}),
        json!({"isolation":"required"}),
        json!({"task_image":"required"}),
        json!({"events":"full_trace"}),
    ] {
        let before = f.snapshot();
        let trace = f.trace();
        let (status, body) = call(
            &transport,
            "POST",
            "/api/v1/sessions/turns",
            Some("synthetic-admin-bearer"),
            json!({"runtime_id":"existing","text":"unchanged","ext":extra}),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::UNPROCESSABLE_ENTITY);
        assert_safe(&body);
        assert_eq!(f.snapshot(), before);
        assert_eq!(f.trace(), trace);
        println!("sample unsupported turn {extra}: zero business changes");
    }
    let before = f.snapshot();
    let trace = f.trace();
    let (status, body) = call(
        &transport,
        "POST",
        "/api/v1/sessions/checkpoint",
        Some("synthetic-admin-bearer"),
        json!({"runtime_id":"existing","conversation_id":"conversation","sender_id":"sender"}),
    )
    .await;
    assert!(!status.is_success());
    assert_safe(&body);
    assert_eq!(f.snapshot(), before);
    assert_eq!(f.trace(), trace);
}
#[tokio::test]
async fn pending_intent_and_checkpoint_keep_original_references_and_uuid() {
    let mut f = Fixture::new();
    f.configurations
        .get_mut("A")
        .unwrap()
        .base_environment
        .insert("XG_TEST_SCENARIO".into(), "owner-message-eof".into());
    let old = f.stored().await;
    let session = old.repository().get("existing").await.unwrap().unwrap();
    let request=serde_json::from_value(json!({"runtime_id":"existing","text":"return exactly pending-text.","client_request_id":"original-key"})).unwrap();
    let submitted = old
        .submit_turn(
            f.ctx("A", "turn", "existing"),
            session.clone(),
            request,
            "original-turn".into(),
            old.repository(),
        )
        .await
        .unwrap();
    assert!(submitted.terminal.unwrap().await.is_err());
    let pending = old.repository().pending_joint_operations().await.unwrap();
    assert_eq!(pending.len(), 1);
    let original = serde_json::to_value(&pending[0]).unwrap();
    let uuid = pending[0].native[0].0.operation_id.clone();
    let checkpoint = JointCheckpointRecord {
        checkpoint_id: "retained-reference".into(),
        source_runtime_id: "existing".into(),
        runtime_kind: ROOT_RUNTIME_KIND.into(),
        binding: session.execution_backing.joint().unwrap().clone(),
        workspace: session.workspace.clone(),
        isolation: session.isolation.clone(),
        capabilities: session.capabilities.clone(),
        tenant_id: None,
        owner_ref: ADMIN_OWNER_REF.into(),
        created_by: Some("operator".into()),
        created_at_ms: 100,
    };
    let db = rusqlite::Connection::open(f.root.path().join("db")).unwrap();
    db.execute(
        "INSERT INTO joint_checkpoints VALUES(?1,?2,NULL,?3,?4,?5,?6)",
        rusqlite::params![
            checkpoint.checkpoint_id,
            checkpoint.binding.binding_id,
            checkpoint.source_runtime_id,
            checkpoint.created_by,
            checkpoint.created_at_ms,
            serde_json::to_string(&checkpoint).unwrap()
        ],
    )
    .unwrap();
    drop(db);
    let router = ProfileRouter::assemble(
        f.guard.clone(),
        budget(),
        vec![
            f.revision(f.profile("A")).unwrap(),
            f.revision(f.profile("B")).unwrap(),
        ],
        "B".into(),
        true,
        f.clock.clone(),
    )
    .unwrap();
    let before = f.snapshot();
    let before_mutations = f
        .trace()
        .into_iter()
        .filter(|v| is_mutation(v))
        .collect::<Vec<_>>();
    assert!(router
        .reconcile(pending.into_iter().next().unwrap(), router.repository())
        .await
        .is_err());
    assert_eq!(f.snapshot(), before);
    let pending = router
        .repository()
        .pending_joint_operations()
        .await
        .unwrap();
    assert_eq!(serde_json::to_value(&pending[0]).unwrap(), original);
    assert_eq!(pending[0].native[0].0.operation_id, uuid);
    let saved = router
        .repository()
        .get_joint_checkpoint("retained-reference")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved, checkpoint);
    let request = serde_json::from_value(json!({"checkpoint_id":"retained-reference"})).unwrap();
    assert!(router
        .load(
            f.ctx("A", "load", "new-load"),
            saved,
            request,
            router.repository()
        )
        .await
        .is_err());
    assert_eq!(f.snapshot(), before);
    assert_eq!(
        f.trace()
            .into_iter()
            .filter(|v| is_mutation(v))
            .collect::<Vec<_>>(),
        before_mutations
    );
    let reopened = f.guard.repository(budget()).unwrap();
    assert_eq!(
        reopened
            .get_joint_checkpoint("retained-reference")
            .await
            .unwrap()
            .unwrap(),
        checkpoint
    );
    assert_eq!(
        reopened.pending_joint_operations().await.unwrap()[0].native[0]
            .0
            .operation_id,
        uuid
    );
}
fn is_mutation(v: &Value) -> bool {
    matches!(
        v["method"].as_str().unwrap_or(""),
        "thinkthread.create"
            | "thinkthread.start"
            | "thinkthread.stop"
            | "thinkthread.remove"
            | "message.send"
            | "message.cancel"
            | "host.exec"
            | "file.write"
    )
}
#[tokio::test]
async fn known_secrets_cannot_enter_nested_rows_or_encoded_host_results() {
    use base64::Engine;
    let f = Fixture::new();
    let router = f.stored().await;
    let before = f.snapshot();
    for secret in [
        "synthetic-admin-bearer",
        "synthetic-tenant-bearer",
        "synthetic-private-model-value",
        "synthetic-database-password",
        "synthetic-operation-password",
    ] {
        for value in [
            json!({"note":[{"nested":format!("prefix {secret} suffix")}]}),
            json!({"bytes":base64::engine::general_purpose::STANDARD.encode(secret)}),
            json!({"arbitrary":format!("\"{secret}\"")}),
        ] {
            let session = router.repository().get("existing").await.unwrap().unwrap();
            let request =
                serde_json::from_value(json!({"runtime_id":"existing","text":value.to_string()}))
                    .unwrap();
            assert!(router
                .submit_turn(
                    f.ctx("A", "turn", "existing"),
                    session,
                    request,
                    "denied-secret".into(),
                    router.repository()
                )
                .await
                .is_err());
            assert_eq!(f.snapshot(), before);
        }
    }
    let db = rusqlite::Connection::open(f.root.path().join("db")).unwrap();
    let records: String = db
        .query_row(
            "SELECT group_concat(references_json,'') FROM execution_configurations",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_safe(&records);
    drop(db);
    for name in ["db", "db-wal"] {
        let path = f.root.path().join(name);
        if path.exists() {
            assert_safe(&String::from_utf8_lossy(&std::fs::read(path).unwrap()));
        }
    }
}
#[tokio::test]
async fn protected_reply_fails_without_publishing_secrets_or_releasing_fences() {
    let mut f = Fixture::new();
    f.configurations
        .get_mut("A")
        .unwrap()
        .base_environment
        .insert("XG_TEST_SCENARIO".into(), "owner-secret-reply".into());
    let router = f.stored().await;
    let transport = http(&f, &router);
    let (status,body)=call(&transport,"POST","/api/v1/sessions/turns",Some("synthetic-admin-bearer"),json!({"runtime_id":"existing","text":"return exactly safe-input.","client_request_id":"secret-result"})).await;
    assert!(status.is_success(), "{body}");
    let receipt: session_protocol::SessionSubmitReceipt = serde_json::from_str(&body).unwrap();
    let (_, sse) = call(
        &transport,
        "GET",
        &format!("/api/v1/sessions/existing/turns/{}/events", receipt.turn_id),
        Some("synthetic-admin-bearer"),
        Value::Null,
    )
    .await;
    assert_safe(&sse);
    assert!(sse.contains("turn_failed"));
    assert!(!sse.contains("turn_completed"));
    assert!(sse.contains("\"usage\":\"unknown\""));
    let pending = router
        .repository()
        .pending_joint_operations()
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].request.context.configuration_revision, "A");
    assert_eq!(
        f.trace()
            .iter()
            .filter(|v| v["method"] == "message.send")
            .count(),
        1
    );
    assert!(router
        .repository()
        .get_joint_turn("existing".into(), "secret-result".into())
        .await
        .unwrap()
        .unwrap()
        .terminal
        .is_none());
    for name in ["db", "db-wal"] {
        let path = f.root.path().join(name);
        if path.exists() {
            assert_safe(&String::from_utf8_lossy(&std::fs::read(path).unwrap()));
        }
    }
}

#[tokio::test]
async fn assembling_profiles_does_not_change_existing_runtime_normalization() {
    let f = Fixture::new();
    let normalizers: Vec<(&str, Box<dyn SessionEnvironmentNormalizer>)> = vec![
        (
            "pi",
            Box::new(xgovernor_runtime_pi::PiSessionEnvironment::new(
                "/fixed/local",
                ["local".into(), "e2b".into()],
            )),
        ),
        (
            "xiaoo",
            Box::new(xgovernor_runtime_xiaoo::XiaooSessionEnvironment::new(
                "/fixed/local",
                ["local".into(), "e2b".into()],
            )),
        ),
    ];
    for (runtime, normalizer) in normalizers {
        for backend in ["local", "e2b", "missing"] {
            let request = serde_json::from_value(json!({"conversation_id":"conversation","sender_id":"sender", "ext":{runtime:{"backend_id":backend}}})).unwrap();
            let before = normalizer
                .normalize(&SecurityContext::admin("operator"), &request)
                .await;
            for enabled in [false, true] {
                let mut profile = f.profile("B");
                profile.enabled = enabled;
                let _router = f.router(profile, true).unwrap();
                let trace = f.trace();
                let after = normalizer
                    .normalize(&SecurityContext::admin("operator"), &request)
                    .await;
                match (&before, after) {
                    (Ok(expected), Ok(actual)) => {
                        assert_eq!(actual.workspace, expected.workspace);
                        assert_eq!(actual.isolation, expected.isolation);
                        assert_eq!(actual.sandbox_capabilities, expected.sandbox_capabilities);
                        assert_eq!(actual.llm, expected.llm);
                        assert_eq!(actual.lease, expected.lease);
                        assert_eq!(
                            actual.workspace.root,
                            if backend == "local" {
                                "/fixed/local"
                            } else {
                                "/home/user/workspace"
                            }
                        );
                    }
                    (Err(expected), Err(actual)) => {
                        assert_eq!(actual.to_string(), expected.to_string())
                    }
                    _ => panic!("existing {runtime}/{backend} normalization changed"),
                }
                assert_eq!(f.trace(), trace);
                eprintln!("sample existing {runtime}/{backend} profile_enabled={enabled}: unchanged normalized facts and errors");
            }
        }
    }
}
