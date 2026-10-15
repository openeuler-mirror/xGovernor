#![cfg(unix)]
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use xgovernor_core::*;
use xgovernor_thinkthread_client::{root_owner::*, sdk, ManagementConfig};
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
                    "XG_TEST_FS".into(),
                    root.path().join("fs").to_string_lossy().into(),
                ),
                (
                    "XG_TEST_STATE".into(),
                    root.path().join("state.json").to_string_lossy().into(),
                ),
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
            .unwrap_or_default()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect()
    }
}

impl Fixture {
    fn owner(&self) -> RootSessionOwner {
        let create:sdk::ThinkThreadCreateRequest=serde_json::from_value(json!({"agent":{"kind":"profile","name":"fixture"},"fs":self.config.fs_root,"models":[],"allowSharedFs":false})).unwrap();
        RootSessionOwner::new(
            self.config.clone(),
            create,
            "fixed-revision".into(),
            2,
            2,
            3000,
        )
        .unwrap()
    }
    fn context(&self) -> JointOperationContext {
        JointOperationContext {
            intent_id: "opening".into(),
            operation: "open".into(),
            tenant_id: None,
            owner_ref: "admin".into(),
            principal: "admin".into(),
            actor_role: Role::Admin,
            quota: TenantQuota::default(),
            runtime_kind: ROOT_RUNTIME_KIND.into(),
            configuration_revision: "fixed-revision".into(),
            source_runtime_id: None,
            target_runtime_id: Some("public-session".into()),
            checkpoint_id: None,
        }
    }
    fn input(&self) -> JointOpenInput {
        JointOpenInput {
            request: serde_json::from_value(
                json!({"conversation_id":"conversation","sender_id":"sender"}),
            )
            .unwrap(),
            normalized: NormalizedSessionEnvironment {
                workspace: WorkspaceFacts {
                    workspace_id: "workspace".into(),
                    root: self.config.fs_root.to_string_lossy().into(),
                    access: WorkspaceAccess::ReadWrite,
                    revision: None,
                    metadata: json!({}),
                },
                isolation: IsolationFacts {
                    boundary: IsolationBoundary::Host,
                    workspace_access: WorkspaceAccess::ReadWrite,
                    network: NetworkIsolation::None,
                    metadata: json!({}),
                },
                sandbox_capabilities: BTreeSet::new(),
                llm: None,
                lease: None,
            },
            capabilities: EffectiveCapabilities {
                sandbox: BTreeSet::new(),
                runtime: BTreeSet::new(),
            },
        }
    }
}
#[tokio::test]
async fn owner_restarts_after_publication_failure_without_recreating_the_root() {
    let f = Fixture::new("owner");
    let path = f.root.path().join("owner.db");
    let repo = Arc::new(
        SqliteSessionRepository::open(&path)
            .unwrap()
            .with_joint_transactions(),
    );
    assert!(repo.supports_joint_transactions());
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TRIGGER fail_publication BEFORE INSERT ON joint_sessions BEGIN SELECT RAISE(ABORT,'injected publication failure'); END;").unwrap();
    let mut tenant = f.context();
    tenant.actor_role = Role::Tenant;
    tenant.tenant_id = Some("tenant".into());
    assert!(f
        .owner()
        .open(tenant, f.input(), repo.clone())
        .await
        .is_err());
    assert!(f.trace().is_empty());
    assert!(f
        .owner()
        .open(f.context(), f.input(), repo.clone())
        .await
        .is_err());
    assert!(repo.get("public-session").await.unwrap().is_none());
    let pending = repo
        .pending_joint_operations()
        .await
        .unwrap()
        .pop()
        .unwrap();
    let binding = pending.admission.binding.clone();
    assert!(pending.request.publication_input.is_some());
    db.execute_batch("DROP TRIGGER fail_publication;").unwrap();
    drop(db);
    drop(repo);
    let repo = Arc::new(
        SqliteSessionRepository::open(&path)
            .unwrap()
            .with_joint_transactions(),
    );
    let owner = f.owner();
    owner.reconcile(pending, repo.clone()).await.unwrap();
    let record = repo.get("public-session").await.unwrap().unwrap();
    assert_eq!(record.execution_backing.joint(), Some(&binding));
    assert_eq!(record.status, SessionStatus::Idle);
    assert!(owner.check_alive(&record, repo.clone()).await.unwrap());
    owner.ensure_attached(&record, repo.clone()).await.unwrap();
    assert_eq!(
        owner
            .open(f.context(), f.input(), repo.clone())
            .await
            .unwrap(),
        record
    );
    let mut second = f.context();
    second.intent_id = "other".into();
    second.target_runtime_id = Some("other-public".into());
    assert!(owner.open(second, f.input(), repo.clone()).await.is_err());
    assert!(owner
        .close(f.context(), record, None, repo.clone())
        .await
        .is_err());
    let trace = f.trace();
    assert_eq!(
        trace
            .iter()
            .filter(|r| r["method"] == "thinkthread.create")
            .count(),
        1
    );
    assert_eq!(
        trace
            .iter()
            .filter(|r| r["method"] == "thinkthread.start")
            .count(),
        1
    );
    assert!(repo.pending_joint_operations().await.unwrap().is_empty());
}
#[tokio::test]
async fn known_start_failure_is_reconciled_before_exact_unpublished_compensation() {
    let f = Fixture::new("owner-start-reject");
    let path = f.root.path().join("owner.db");
    let repo = Arc::new(SqliteSessionRepository::open(&path).unwrap());
    let owner = f.owner();
    assert!(owner
        .open(f.context(), f.input(), repo.clone())
        .await
        .is_err());
    let pending = repo
        .pending_joint_operations()
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(owner.reconcile(pending, repo.clone()).await.is_err());
    let pending = repo
        .pending_joint_operations()
        .await
        .unwrap()
        .pop()
        .unwrap();
    let binding = pending.admission.binding.clone();
    owner.reconcile(pending, repo.clone()).await.unwrap();
    assert!(repo.get("public-session").await.unwrap().is_none());
    assert!(repo.pending_joint_operations().await.unwrap().is_empty());
    let state = repo.get_joint_binding(binding).await.unwrap().unwrap();
    assert_eq!(state.stage, "removed");
    assert!(state.objects.is_empty());
    let trace = f.trace();
    assert_eq!(
        trace
            .iter()
            .filter(|r| r["method"] == "thinkthread.create")
            .count(),
        1
    );
    assert_eq!(
        trace
            .iter()
            .filter(|r| r["method"] == "thinkthread.start")
            .count(),
        1
    );
    assert_eq!(
        trace
            .iter()
            .filter(|r| r["method"] == "thinkthread.stop")
            .count(),
        1
    );
    assert_eq!(
        trace
            .iter()
            .filter(|r| r["method"] == "thinkthread.remove")
            .count(),
        1
    );
    for row in trace.iter().filter(|r| {
        ["thinkthread.stop", "thinkthread.remove"].contains(&r["method"].as_str().unwrap_or(""))
    }) {
        assert_eq!(
            row["params"]["thinkthreadIds"],
            json!(["fixture-owned-root"])
        );
        assert_eq!(row["params"]["force"], false);
    }
}

#[tokio::test]
async fn message_journal_recovers_and_settles_without_repeating_admission() {
    for scenario in [
        "owner-message-ok",
        "owner-message-drop",
        "owner-message-partial",
        "owner-message-unproven",
        "owner-message-truncated",
        "owner-message-failed",
        "owner-message-eof",
        "owner-message-silent",
    ] {
        let f = Fixture::new(scenario);
        let path = f.root.path().join("messages.db");
        let repo = Arc::new(
            SqliteSessionRepository::open(&path)
                .unwrap()
                .with_joint_transactions(),
        );
        let owner = f.owner();
        let record = owner
            .open(f.context(), f.input(), repo.clone())
            .await
            .unwrap();
        let public_id = record.runtime_id.clone();
        let context = |id: &str| {
            let mut c = f.context();
            c.intent_id = id.into();
            c.operation = "turn".into();
            c.source_runtime_id = Some(public_id.clone());
            c.target_runtime_id = None;
            c
        };
        let request: session_protocol::SessionTurnRequest = serde_json::from_value(
            json!({"runtime_id":record.runtime_id,"text":"hello","client_request_id":"request"}),
        )
        .unwrap();
        let mut unsupported = request.clone();
        unsupported.ext.insert("token_budget".into(), json!(10));
        assert!(owner
            .submit_turn(
                context("rejected"),
                record.clone(),
                unsupported,
                "rejected-turn".into(),
                repo.clone()
            )
            .await
            .is_err());
        assert!(!f.trace().iter().any(|r| r["method"] == "message.send"));
        let submitted = owner
            .submit_turn(
                context("turn-intent"),
                record,
                request.clone(),
                "public-turn".into(),
                repo.clone(),
            )
            .await
            .unwrap();
        if scenario == "owner-message-ok" {
            let pending = repo
                .pending_joint_operations()
                .await
                .unwrap()
                .pop()
                .unwrap();
            owner.reconcile(pending, repo.clone()).await.unwrap();
        }
        let mut result = submitted.terminal.unwrap().await;
        if [
            "owner-message-drop",
            "owner-message-partial",
            "owner-message-unproven",
        ]
        .contains(&scenario)
        {
            assert!(result.is_err());
            let pending = repo
                .pending_joint_operations()
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(
                pending.native[0].1.as_ref().unwrap().state,
                NativeOperationState::Unknown
            );
            drop(repo);
            let reopened = Arc::new(
                SqliteSessionRepository::open(&path)
                    .unwrap()
                    .with_joint_transactions(),
            );
            result = owner.resume_message(pending, reopened).await;
            assert!(f.trace().iter().any(|r| r["method"] == "operation.get"));
        }
        let repo = Arc::new(
            SqliteSessionRepository::open(&path)
                .unwrap()
                .with_joint_transactions(),
        );
        let record = repo.get("public-session").await.unwrap().unwrap();
        if [
            "owner-message-eof",
            "owner-message-silent",
            "owner-message-unproven",
        ]
        .contains(&scenario)
        {
            assert!(result.is_err());
            assert_eq!(record.status, SessionStatus::Running);
            assert_eq!(repo.pending_joint_operations().await.unwrap().len(), 1);
            let mut next = request.clone();
            next.client_request_id = Some("next".into());
            assert!(owner
                .submit_turn(
                    context("blocked-next"),
                    record,
                    next,
                    "next-turn".into(),
                    repo.clone()
                )
                .await
                .is_err());
        } else {
            let terminal = result.unwrap();
            assert_eq!(
                terminal.failure.is_some(),
                [
                    "owner-message-truncated",
                    "owner-message-failed",
                    "owner-message-partial"
                ]
                .contains(&scenario)
            );
            assert_eq!(
                terminal.reply.as_deref(),
                if terminal.failure.is_none() {
                    Some("complete reply")
                } else {
                    None
                }
            );
            assert!(repo.pending_joint_operations().await.unwrap().is_empty());
            let replay = owner
                .submit_turn(
                    context("replay"),
                    record.clone(),
                    request.clone(),
                    "different-turn".into(),
                    repo.clone(),
                )
                .await
                .unwrap();
            assert_eq!(replay.turn_id, "public-turn");
            assert!(replay.terminal.is_none());
            let old = repo
                .get_joint_turn("public-session".into(), "request".into())
                .await
                .unwrap()
                .unwrap();
            let mut duplicate = terminal.clone();
            duplicate.completed_at_ms += 1;
            repo.finish_joint_turn(old.admission.clone(), duplicate)
                .await
                .unwrap();
            let mut changed = terminal;
            changed.failure = Some("changed".into());
            changed.reply = None;
            assert!(repo
                .finish_joint_turn(old.admission, changed)
                .await
                .is_err());
            let mut different = request;
            different.text = "changed input".into();
            assert!(owner
                .submit_turn(
                    context("different"),
                    record,
                    different,
                    "different".into(),
                    repo.clone()
                )
                .await
                .is_err());
        }
        assert_eq!(
            f.trace()
                .iter()
                .filter(|r| r["method"] == "message.send")
                .count(),
            1,
            "{scenario}"
        );
    }
}
