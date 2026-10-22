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
fn repository(path: &std::path::Path) -> SqliteSessionRepository {
    SqliteSessionRepository::open(path)
        .unwrap()
        .with_joint_budget_policy(JointBudgetPolicy::from([
            (
                "session".into(),
                JointResourceBudget {
                    total: 2,
                    recovery_reserve: 1,
                },
            ),
            (
                "root".into(),
                JointResourceBudget {
                    total: 2,
                    ..Default::default()
                },
            ),
        ]))
        .unwrap()
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
                (
                    "HOME".into(),
                    root.path().join("home").to_string_lossy().into(),
                ),
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
    let repo = Arc::new(repository(&path).with_joint_transactions());
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
    let repo = Arc::new(repository(&path).with_joint_transactions());
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
    let repo = Arc::new(repository(&path));
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
    let original = pending.admission.clone();
    let start_id = pending
        .native
        .iter()
        .find(|(n, _)| n.step == "start")
        .unwrap()
        .0
        .operation_id
        .clone();
    assert_eq!(
        pending
            .native
            .iter()
            .find(|(n, _)| n.step == "start")
            .unwrap()
            .1
            .as_ref()
            .unwrap()
            .state,
        NativeOperationState::Unknown
    );
    drop(repo);
    let repo = Arc::new(repository(&path));
    let pending = repo
        .pending_joint_operations()
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(pending.admission, original);
    let next = Fixture::new("owner");
    let mut next_context = next.context();
    next_context.intent_id = "next-open".into();
    next_context.target_runtime_id = Some("next-session".into());
    assert!(matches!(
        next.owner()
            .open(next_context.clone(), next.input(), repo.clone())
            .await,
        Err(SessionDomainError::QuotaExceeded { limit: 1, .. })
    ));
    assert!(next
        .trace()
        .iter()
        .all(|r| ["handshake", "context.bind"].contains(&r["method"].as_str().unwrap())));
    assert!(owner
        .compensate_unpublished(pending.clone(), repo.clone())
        .await
        .is_err());
    let db = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT sum(units) FROM joint_reservations WHERE kind='session' AND state='reserved'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    assert!(owner.reconcile(pending, repo.clone()).await.is_err());
    let queries: Vec<_> = f
        .trace()
        .into_iter()
        .filter(|r| r["method"] == "operation.get")
        .collect();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0]["params"]["operationId"], start_id);
    let pending = repo
        .pending_joint_operations()
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        pending
            .native
            .iter()
            .find(|(n, _)| n.step == "start")
            .unwrap()
            .1
            .as_ref()
            .unwrap()
            .state,
        NativeOperationState::Failed
    );
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
    assert_eq!(
        db.query_row(
            "SELECT sum(units) FROM joint_reservations WHERE kind='session' AND state='released'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    let admitted = next
        .owner()
        .open(next_context, next.input(), repo.clone())
        .await
        .unwrap();
    assert_eq!(admitted.runtime_id, "next-session");
    repo.finish_joint_compensation(original.clone())
        .await
        .unwrap();
    repo.finish_joint_compensation(original).await.unwrap();
    assert_eq!(
        db.query_row(
            "SELECT sum(units) FROM joint_reservations WHERE kind='session' AND state='committed'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    let third = Fixture::new("owner");
    let mut context = third.context();
    context.intent_id = "third-open".into();
    context.target_runtime_id = Some("third-session".into());
    assert!(matches!(
        third
            .owner()
            .open(context, third.input(), repo.clone())
            .await,
        Err(SessionDomainError::QuotaExceeded { limit: 1, .. })
    ));
    assert!(third
        .trace()
        .iter()
        .all(|r| ["handshake", "context.bind"].contains(&r["method"].as_str().unwrap())));
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
        let repo = Arc::new(repository(&path).with_joint_transactions());
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
            let reopened = Arc::new(repository(&path).with_joint_transactions());
            result = owner.resume_message(pending, reopened).await;
            assert!(f.trace().iter().any(|r| r["method"] == "operation.get"));
        }
        let repo = Arc::new(repository(&path).with_joint_transactions());
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
            let trace_before_replay = f.trace();
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
            assert_eq!(replay.terminal.unwrap().await.unwrap(), terminal);
            let trace_after_replay = f.trace();
            assert_eq!(
                &trace_after_replay[..trace_before_replay.len()],
                trace_before_replay.as_slice()
            );
            assert!(trace_after_replay[trace_before_replay.len()..]
                .iter()
                .all(|r| r["method"] == "handshake" || r["method"] == "context.bind"));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_open_reuses_the_winning_durable_request() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let mut f = Fixture::new("owner-concurrent");
    let path = f.root.path().join("owner.db");
    let gate_path = f.root.path().join("preflight.sock");
    let gate = tokio::net::UnixListener::bind(&gate_path).unwrap();
    f.config.base_environment.insert(
        "XG_TEST_PREFLIGHT_GATE".into(),
        gate_path.to_string_lossy().into(),
    );
    let left = Arc::new(repository(&path));
    let right = Arc::new(repository(&path));
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TRIGGER fail_publication BEFORE INSERT ON joint_sessions BEGIN SELECT RAISE(ABORT,'injected publication failure'); END;").unwrap();
    let release = async {
        let mut streams = Vec::new();
        for _ in 0..2 {
            let (stream, _) = gate.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert_eq!(line, "preflight\n");
            streams.push(reader);
        }
        for stream in &mut streams {
            stream.get_mut().write_all(b"x").await.unwrap();
        }
    };
    let owner = f.owner();
    let (one, two, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            owner.open(f.context(), f.input(), left.clone()),
            owner.open(f.context(), f.input(), right.clone()),
            release
        )
    })
    .await
    .unwrap();
    for result in [one, two] {
        let error = result.unwrap_err().to_string();
        assert!(
            !error.contains("joint operation identity conflict"),
            "{error}"
        );
    }
    let pending = left.pending_joint_operations().await.unwrap();
    assert_eq!(pending.len(), 1);
    let original = pending[0].clone();
    assert_eq!(
        db.query_row("SELECT count(*) FROM joint_reservations", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        2
    );
    let mut changed = f.input();
    changed.request.conversation_id = "changed".into();
    let before = f.trace().len();
    assert!(owner
        .open(f.context(), changed, right.clone())
        .await
        .is_err());
    assert_eq!(f.trace().len(), before);
    db.execute_batch("DROP TRIGGER fail_publication").unwrap();
    drop(left);
    drop(right);
    drop(db);
    let reopened = Arc::new(repository(&path));
    owner
        .reconcile(original.clone(), reopened.clone())
        .await
        .unwrap();
    let record = owner
        .open(f.context(), f.input(), reopened.clone())
        .await
        .unwrap();
    assert_eq!(
        record.execution_backing.joint(),
        Some(&original.admission.binding)
    );
    assert!(reopened
        .pending_joint_operations()
        .await
        .unwrap()
        .is_empty());
    let trace = f.trace();
    for method in ["thinkthread.create", "thinkthread.start"] {
        assert_eq!(trace.iter().filter(|r| r["method"] == method).count(), 1);
    }
    assert_eq!(
        trace
            .iter()
            .find(|r| r["method"] == "thinkthread.create")
            .unwrap()["params"]["operationId"],
        original.native[0].0.operation_id
    );
}

#[tokio::test]
async fn partial_create_identity_survives_empty_outcomes_and_restart_compensation() {
    for (known, erased_legacy_outcome) in [(false, false), (true, false), (true, true)] {
        let f = Fixture::new("owner-create-drop");
        let path = f.root.path().join("owner.db");
        let repo = Arc::new(repository(&path));
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
        let a = pending.admission.clone();
        let native = pending.native[0].0.clone();
        let root = JointNativeObject {
            kind: "root".into(),
            id: "fixture-owned-root".into(),
        };
        if known {
            repo.record_joint_native_result(
                a.clone(),
                native.step.clone(),
                native.operation_id.clone(),
                native.digest.clone(),
                JointNativeOutcome {
                    state: NativeOperationState::Accepted,
                    resources: vec![root.clone()],
                    payload: json!({"partial":"first"}),
                },
            )
            .await
            .unwrap();
            repo.record_joint_native_result(
                a.clone(),
                native.step.clone(),
                native.operation_id.clone(),
                native.digest.clone(),
                JointNativeOutcome {
                    state: NativeOperationState::Unknown,
                    resources: vec![],
                    payload: json!({"result":null}),
                },
            )
            .await
            .unwrap();
            let current = repo
                .pending_joint_operations()
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(
                current.native[0].1.as_ref().unwrap().resources,
                vec![root.clone()]
            );
            assert_eq!(
                current.native[0].1.as_ref().unwrap().payload,
                json!({"result":null})
            );
        }
        repo.record_joint_native_result(
            a.clone(),
            native.step.clone(),
            native.operation_id.clone(),
            native.digest.clone(),
            JointNativeOutcome {
                state: NativeOperationState::Failed,
                resources: vec![],
                payload: json!({"terminal":"failed"}),
            },
        )
        .await
        .unwrap();
        let current = repo
            .pending_joint_operations()
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            current.native[0].1.as_ref().unwrap().resources,
            if known { vec![root.clone()] } else { vec![] }
        );
        assert!(repo
            .record_joint_native_result(
                a.clone(),
                native.step.clone(),
                native.operation_id.clone(),
                native.digest.clone(),
                JointNativeOutcome {
                    state: NativeOperationState::Unknown,
                    resources: vec![],
                    payload: json!({})
                }
            )
            .await
            .is_err());
        if erased_legacy_outcome {
            // Reproduce a journal row written before resource identities were retained monotonically.
            let db = rusqlite::Connection::open(&path).unwrap();
            let mut outcome = current.native[0].1.clone().unwrap();
            outcome.resources.clear();
            db.execute(
                "UPDATE joint_native_steps SET outcome_json=?1 WHERE intent_id=?2 AND step=?3",
                rusqlite::params![
                    serde_json::to_string(&outcome).unwrap(),
                    a.intent_id,
                    native.step
                ],
            )
            .unwrap();
        }
        drop(repo);
        let repo = Arc::new(repository(&path));
        let pending = repo
            .pending_joint_operations()
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(pending.native[0].0.operation_id, native.operation_id);
        if !known {
            assert!(owner
                .compensate_unpublished(pending, repo.clone())
                .await
                .is_err());
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
                a.intent_id
            );
            repo.begin_joint_compensation(a.clone()).await.unwrap();
            let unknown = repo
                .pending_joint_operations()
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(unknown.stage, "compensating");
            assert!(owner
                .compensate_unpublished(unknown, repo.clone())
                .await
                .is_err());
            assert!(!f
                .trace()
                .iter()
                .any(|r| ["thinkthread.stop", "thinkthread.remove"]
                    .contains(&r["method"].as_str().unwrap())));
            continue;
        }
        assert!(repo.finish_joint_compensation(a.clone()).await.is_err());
        let (repo, pending) = if erased_legacy_outcome {
            let db = rusqlite::Connection::open(&path).unwrap();
            db.execute_batch("CREATE TRIGGER interrupt_finish BEFORE UPDATE ON joint_operations WHEN NEW.stage='compensated' BEGIN SELECT RAISE(ABORT,'injected finish interruption'); END;").unwrap();
            assert!(owner
                .compensate_unpublished(pending, repo.clone())
                .await
                .is_err());
            assert!(repo
                .get_joint_binding(a.binding.clone())
                .await
                .unwrap()
                .unwrap()
                .objects
                .is_empty());
            assert_eq!(
                db.query_row(
                    "SELECT count(*) FROM joint_objects WHERE binding_id=?1 AND generation=?2 AND kind='root' AND native_id=?3 AND absent=1",
                    rusqlite::params![a.binding.binding_id, a.generation, root.id],
                    |r| r.get::<_, i64>(0)
                ).unwrap(),
                1
            );
            assert_eq!(
                db.query_row(
                    "SELECT stage FROM joint_operations WHERE id=?1",
                    [&a.intent_id],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
                "compensating"
            );
            for (table, condition, expected) in [
                ("joint_reservations", "state='reserved'", 2),
                ("joint_public_claims", "1=1", 1),
                ("joint_workspace_claims", "1=1", 1),
                ("joint_fences", "holder IS NOT NULL", 1),
            ] {
                assert_eq!(
                    db.query_row(
                        &format!("SELECT count(*) FROM {table} WHERE {condition}"),
                        [],
                        |r| r.get::<_, i64>(0)
                    )
                    .unwrap(),
                    expected
                );
            }
            let mutations_before = f
                .trace()
                .into_iter()
                .filter(|r| {
                    ["thinkthread.stop", "thinkthread.remove"]
                        .contains(&r["method"].as_str().unwrap())
                })
                .collect::<Vec<_>>();
            assert_eq!(mutations_before.len(), 2);
            drop(db);
            drop(repo);
            let repo = Arc::new(repository(&path));
            let db = rusqlite::Connection::open(&path).unwrap();
            db.execute_batch("DROP TRIGGER interrupt_finish").unwrap();
            drop(db);
            let pending = repo
                .pending_joint_operations()
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(pending.admission, a);
            assert_eq!(pending.stage, "compensating");
            let mut stale = pending.clone();
            stale.admission.generation += 1;
            assert!(owner
                .compensate_unpublished(stale, repo.clone())
                .await
                .is_err());
            assert_eq!(
                f.trace()
                    .into_iter()
                    .filter(|r| ["thinkthread.stop", "thinkthread.remove"]
                        .contains(&r["method"].as_str().unwrap()))
                    .collect::<Vec<_>>(),
                mutations_before
            );
            (repo, pending)
        } else {
            (repo, pending)
        };
        owner
            .compensate_unpublished(pending, repo.clone())
            .await
            .unwrap();
        assert!(repo.pending_joint_operations().await.unwrap().is_empty());
        let binding = repo
            .get_joint_binding(a.binding.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(binding.stage, "removed");
        assert!(binding.objects.is_empty());
        let db = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            db.query_row(
                "SELECT sum(units) FROM joint_reservations WHERE state='released'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            2
        );
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM joint_fences WHERE holder IS NOT NULL",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
        for table in ["joint_public_claims", "joint_workspace_claims"] {
            assert_eq!(
                db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
        let trace = f.trace();
        for method in [
            "thinkthread.create",
            "thinkthread.stop",
            "thinkthread.remove",
        ] {
            assert_eq!(trace.iter().filter(|r| r["method"] == method).count(), 1);
        }
        for row in trace.iter().filter(|r| {
            ["thinkthread.stop", "thinkthread.remove"].contains(&r["method"].as_str().unwrap())
        }) {
            assert_eq!(row["params"]["thinkthreadIds"], json!([root.id]));
            assert_eq!(row["params"]["force"], false);
        }
        assert!(trace
            .iter()
            .any(|r| r["method"] == "thinkthread.get" && r["params"]["thinkthreadId"] == root.id));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paused_open_returns_the_exact_publication_committed_by_another_connection() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    for gate_at in [1, 2] {
        let mut f = Fixture::new("owner-concurrent");
        f.config
            .base_environment
            .insert("XG_TEST_OPEN_GATE_AT".into(), gate_at.to_string());
        let path = f.root.path().join("owner.db");
        let gate_path = f.root.path().join("resume.sock");
        let gate = tokio::net::UnixListener::bind(&gate_path).unwrap();
        f.config.base_environment.insert(
            "XG_TEST_RESUME_GATE".into(),
            gate_path.to_string_lossy().into(),
        );
        let left = Arc::new(repository(&path));
        let right = Arc::new(repository(&path));
        let owner = f.owner();
        let delayed = {
            let owner = owner.clone();
            let context = f.context();
            let input = f.input();
            let right = right.clone();
            tokio::spawn(async move { owner.open(context, input, right).await })
        };
        let (stream, _) = tokio::time::timeout(Duration::from_secs(5), gate.accept())
            .await
            .unwrap()
            .unwrap();
        let mut stream = tokio::io::BufReader::new(stream);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(line, "resume\n");
        let snapshot = left.pending_joint_operations().await.unwrap().pop();
        assert_eq!(snapshot.is_some(), gate_at == 2);
        if let Some(original) = &snapshot {
            assert!(original.native.iter().all(|(_, o)| o.is_none()));
        }
        let committed = owner
            .open(f.context(), f.input(), left.clone())
            .await
            .unwrap();
        let original = if let Some(original) = snapshot {
            original
        } else {
            let creation = left
                .get_joint_binding(committed.execution_backing.joint().unwrap().clone())
                .await
                .unwrap()
                .unwrap()
                .creation;
            let admission = left
                .prepare_joint_operation(creation.clone())
                .await
                .unwrap();
            JointPendingOperation {
                admission,
                native: creation.native.iter().cloned().map(|n| (n, None)).collect(),
                request: creation,
                stage: "prepared".into(),
            }
        };
        assert!(left.pending_joint_operations().await.unwrap().is_empty());
        stream.get_mut().write_all(b"x").await.unwrap();
        let replayed = tokio::time::timeout(Duration::from_secs(5), delayed)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(replayed, committed);
        for variant in 0..5 {
            let mut changed = original.clone();
            match variant {
                0 => changed.admission.generation += 1,
                1 => changed.admission.intent_id = "foreign-intent".into(),
                2 => changed.admission.binding.binding_id = "foreign-binding".into(),
                3 => changed.admission.root_epoch += 1,
                _ => changed.request.context.principal = "changed-principal".into(),
            }
            assert!(owner.reconcile(changed, right.clone()).await.is_err());
        }
        assert_eq!(left.get("public-session").await.unwrap(), Some(committed));
        let trace = f.trace();
        for method in ["thinkthread.create", "thinkthread.start"] {
            assert_eq!(trace.iter().filter(|r| r["method"] == method).count(), 1);
        }
        assert_eq!(
            trace
                .iter()
                .find(|r| r["method"] == "thinkthread.create")
                .unwrap()["params"]["operationId"],
            original.native[0].0.operation_id
        );
    }
}

fn crash_cancel_config(path: &std::path::Path) -> ManagementConfig {
    ManagementConfig {
        host_executable: path.join("host"),
        state_root: path.join("state"),
        runtime_root: path.join("runtime"),
        config_root: path.join("config"),
        cache_root: path.join("cache"),
        working_directory: path.join("work"),
        fs_root: path.join("fs"),
        home_directory: path.join("home"),
        executable_search_path: vec![PathBuf::from("/usr/bin")],
        base_environment: BTreeMap::from([
            ("HOME".into(), path.join("home").to_string_lossy().into()),
            ("XG_TEST_SCENARIO".into(), "owner-cancel".into()),
            (
                "XG_TEST_FS".into(),
                path.join("fs").to_string_lossy().into(),
            ),
            (
                "XG_TEST_STATE".into(),
                path.join("state.json").to_string_lossy().into(),
            ),
            (
                "XG_TEST_TRACE".into(),
                path.join("trace").to_string_lossy().into(),
            ),
        ]),
        connect_timeout: Duration::from_secs(2),
    }
}
#[tokio::test]
async fn ordinary_cancel_crash_child() {
    let Ok(root) = std::env::var("XGOVERNOR_CANCEL_CHILD_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let config = crash_cancel_config(&root);
    let repo = Arc::new(repository(&root.join("owner.db")).with_joint_transactions());
    let pending = repo
        .pending_joint_operations()
        .await
        .unwrap()
        .pop()
        .unwrap();
    let mut context = pending.request.context.clone();
    context.operation = "cancel".into();
    context.intent_id = "child-cancel".into();
    let record = repo.get("public-session").await.unwrap().unwrap();
    let owner=RootSessionOwner::new(config.clone(),serde_json::from_value(json!({"agent":{"kind":"profile","name":"fixture"},"fs":config.fs_root,"models":[],"allowSharedFs":false})).unwrap(),"fixed-revision".into(),0,0,3000).unwrap();
    if std::env::var("XGOVERNOR_CANCEL_CHILD_PHASE").unwrap() == "prepared" {
        repo.prepare_joint_cancel(context, record, None)
            .await
            .unwrap();
    } else {
        owner
            .cancel_turn(context, record, None, repo)
            .await
            .unwrap();
    }
    // The parent owns all paths and checks recovery after this process exits.
    std::process::exit(0);
}
#[tokio::test]
async fn cancellation_survives_process_exit_before_send_and_after_acknowledgement() {
    for phase in ["prepared", "ack"] {
        let f = Fixture::new("owner-cancel");
        let repo = Arc::new(repository(&f.root.path().join("owner.db")).with_joint_transactions());
        let owner = f.owner();
        let record = owner
            .open(f.context(), f.input(), repo.clone())
            .await
            .unwrap();
        let mut context = f.context();
        context.operation = "turn".into();
        context.intent_id = "original-turn".into();
        context.source_runtime_id = Some(record.runtime_id.clone());
        context.target_runtime_id = None;
        let turn=owner.admit_message(context,record,serde_json::from_value(json!({"runtime_id":"public-session","text":"active","client_request_id":"key"})).unwrap(),"original-turn-id".into(),repo.clone()).await.unwrap();
        let client = f.config.connect().await.unwrap();
        xgovernor_thinkthread_client::durable_operation::DurableManagementOperation::new(
            &client,
            repo.clone(),
        )
        .execute(turn.admission.clone(), "message")
        .await
        .unwrap();
        client.close().await.unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "ordinary_cancel_crash_child", "--nocapture"])
            .env("XGOVERNOR_CANCEL_CHILD_ROOT", f.root.path())
            .env("XGOVERNOR_CANCEL_CHILD_PHASE", phase)
            .status()
            .unwrap();
        assert!(status.success());
        assert!(repo
            .joint_turn_result(turn.admission.clone())
            .await
            .unwrap()
            .is_none());
        let control = repo
            .joint_cancel_control(turn.admission.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(control.native.is_none(), phase == "prepared");
        drop(repo);
        let reopened =
            Arc::new(repository(&f.root.path().join("owner.db")).with_joint_transactions());
        let pending = reopened
            .pending_joint_operations()
            .await
            .unwrap()
            .pop()
            .unwrap();
        let terminal = owner
            .resume_message(pending, reopened.clone())
            .await
            .unwrap();
        assert!(terminal.cancelled);
        assert!(reopened
            .joint_workspace_boundary(turn.admission.binding)
            .await
            .unwrap()
            .unsettled());
        assert_eq!(
            f.trace()
                .iter()
                .filter(|r| r["method"] == "message.cancel")
                .count(),
            1
        );
        assert_eq!(
            f.trace()
                .iter()
                .filter(|r| r["method"] == "message.send")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn profile_home_is_explicit_and_preserved_across_owner_connections() {
    let f = Fixture::new("owner-explicit-home");
    let repo = Arc::new(repository(&f.root.path().join("owner.db")).with_joint_transactions());
    let owner = f.owner();
    let record = owner
        .open(f.context(), f.input(), repo.clone())
        .await
        .unwrap();
    owner.ensure_attached(&record, repo).await.unwrap();
    let trace = f.trace();
    let bindings: Vec<_> = trace
        .iter()
        .filter(|r| r["method"] == "context.bind")
        .collect();
    assert!(bindings.len() >= 3);
    for binding in bindings {
        assert_eq!(
            binding["host_home"],
            f.config.home_directory.to_string_lossy().as_ref()
        );
        assert_eq!(binding["params"]["homeDirectory"], binding["host_home"]);
        let keys = binding["params"]["environmentKeys"].as_array().unwrap();
        assert!(keys.contains(&json!("HOME")));
    }
    assert_eq!(
        trace
            .iter()
            .filter(|r| r["method"] == "thinkthread.create")
            .count(),
        1
    );
}
