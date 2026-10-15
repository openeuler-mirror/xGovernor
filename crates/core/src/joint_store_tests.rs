use super::*;
use crate::domain::{SandboxCapability, WorkspaceAccess};
use crate::*;
use std::collections::BTreeSet;
fn sample_record(runtime_id: &str) -> SessionRecord {
    SessionRecord {
        execution_backing: Default::default(),
        runtime_id: runtime_id.to_string(),
        conversation_id: "conversation-1".to_string(),
        sender_id: "sender-1".to_string(),
        status: SessionStatus::Idle,
        created_at_ms: 1,
        updated_at_ms: 2,
        workspace: WorkspaceFacts {
            workspace_id: "workspace-1".to_string(),
            root: "/tmp/workspace-1".to_string(),
            access: WorkspaceAccess::ReadWrite,
            revision: Some("main".to_string()),
            metadata: serde_json::json!({"k": "v"}),
        },
        isolation: IsolationFacts {
            boundary: crate::domain::IsolationBoundary::Container,
            workspace_access: WorkspaceAccess::ReadWrite,
            network: crate::domain::NetworkIsolation::Restricted,
            metadata: serde_json::Value::Null,
        },
        capabilities: EffectiveCapabilities {
            sandbox: BTreeSet::from([SandboxCapability::Exec]),
            runtime: BTreeSet::new(),
        },
        runtime: OpaqueRuntimeState {
            runtime_kind: "local-mock".to_string(),
            schema_version: 1,
            state: serde_json::json!({"pid": 123}),
        },
        llm: Some(ResolvedLlm {
            provider: "anthropic".to_string(),
            model: "claude".to_string(),
            api_base: None,
            api: None,
            capabilities: None,
            credential_source: "env".to_string(),
        }),
        lease: Some(SessionLease {
            client_id: "client-1".to_string(),
            client_pid: Some(42),
            client_hostname: Some("host-1".to_string()),
            expires_at_ms: 999,
        }),
        lineage: Some(CheckpointLineage {
            parent_runtime_id: Some("runtime-0".to_string()),
            source_checkpoint_id: None,
        }),
        last_error: None,
        tenant_id: Some("tenant-1".to_string()),
        created_by: "principal-1".to_string(),
    }
}

fn request(
    id: &str,
    operation: &str,
    source: &str,
    target: &str,
    checkpoint: &str,
    root: &str,
    parent: Option<JointBinding>,
) -> JointPrepareRequest {
    let kind = if operation == "checkpoint" {
        "checkpoint"
    } else {
        "session"
    };
    let creating = ["open", "load", "checkpoint"].contains(&operation);
    let mut reservations = if creating {
        vec![JointResourceReservation {
            kind: kind.into(),
            units: 1,
            limit: 3,
        }]
    } else {
        vec![]
    };
    if operation == "open" {
        reservations.push(JointResourceReservation {
            kind: "root".into(),
            units: 1,
            limit: 3,
        });
    }
    JointPrepareRequest {
        turn: None,
        context: JointOperationContext {
            intent_id: id.into(),
            operation: operation.into(),
            tenant_id: Some("tenant-1".into()),
            owner_ref: "tenant/tenant-1".into(),
            principal: "principal-1".into(),
            actor_role: Role::Tenant,
            quota: TenantQuota {
                max_sessions: Some(3),
                max_requests_per_minute: None,
            },
            runtime_kind: "local-mock".into(),
            configuration_revision: "revision-1".into(),
            source_runtime_id: (!source.is_empty()).then(|| source.into()),
            target_runtime_id: (!target.is_empty()).then(|| target.into()),
            checkpoint_id: (!checkpoint.is_empty()).then(|| checkpoint.into()),
        },
        binding: JointBinding {
            owner_kind: "joint-test".into(),
            binding_id: format!("binding-{id}"),
            configuration_revision: "revision-1".into(),
        },
        root_id: root.into(),
        source_id: match operation {
            "open" => target,
            "load" | "delete_checkpoint" => checkpoint,
            _ => source,
        }
        .into(),
        exclusive_root: false,
        parent_binding: parent,
        reservations,
        publication_input: None,
        native: vec![JointNativeIntent {
            step: "native".into(),
            operation_id: format!(
                "00000000-0000-4000-8000-{:012x}",
                id.bytes()
                    .fold(0u64, |a, b| a.wrapping_mul(31).wrapping_add(b as u64))
                    & 0xffffffffffff
            ),
            digest: "a".repeat(64),
            method: if creating { "create" } else { "inspect" }.into(),
            params: serde_json::json!({"source":source}),
            compensation: false,
            effect: if creating {
                NativeEffect::Create
            } else {
                NativeEffect::ObserveAbsence
            },
        }],
    }
}
fn object(binding: &JointBinding) -> JointNativeObject {
    JointNativeObject {
        kind: "child".into(),
        id: format!("native-{}", binding.binding_id),
    }
}
async fn result(
    repo: &SqliteSessionRepository,
    a: &JointAdmission,
    n: &JointNativeIntent,
    state: NativeOperationState,
    resources: Vec<JointNativeObject>,
) {
    let mut resources = resources;
    if a.binding.binding_id == "binding-source" || a.binding.binding_id == "binding-create" {
        resources.push(JointNativeObject {
            kind: "root".into(),
            id: format!("root-{}", a.binding.binding_id),
        });
    }
    repo.record_joint_native_result(
        a.clone(),
        n.step.clone(),
        n.operation_id.clone(),
        n.digest.clone(),
        JointNativeOutcome {
            state,
            resources,
            payload: serde_json::Value::Null,
        },
    )
    .await
    .unwrap();
}
async fn open(
    repo: &SqliteSessionRepository,
    id: &str,
    runtime: &str,
    root: &str,
) -> (JointPrepareRequest, SessionRecord) {
    let req = request(id, "open", "", runtime, "", root, None);
    let a = repo.prepare_joint_operation(req.clone()).await.unwrap();
    result(
        repo,
        &a,
        &req.native[0],
        NativeOperationState::Completed,
        vec![object(&req.binding)],
    )
    .await;
    let mut record = sample_record(runtime);
    record.execution_backing = SessionExecutionBacking::JointV1 {
        binding: req.binding.clone(),
    };
    repo.publish_joint_session(a, record.clone()).await.unwrap();
    (req, record)
}
async fn checkpoint(
    repo: &SqliteSessionRepository,
    id: &str,
    session: &JointPrepareRequest,
) -> JointPrepareRequest {
    let req = request(
        id,
        "checkpoint",
        session.context.target_runtime_id.as_deref().unwrap(),
        "",
        id,
        &session.root_id,
        Some(session.binding.clone()),
    );
    let a = repo.prepare_joint_operation(req.clone()).await.unwrap();
    result(
        repo,
        &a,
        &req.native[0],
        NativeOperationState::Completed,
        vec![object(&req.binding)],
    )
    .await;
    let record = sample_record("ignored");
    repo.publish_joint_checkpoint(
        a,
        JointCheckpointRecord {
            checkpoint_id: id.into(),
            source_runtime_id: req.context.source_runtime_id.clone().unwrap(),
            runtime_kind: req.context.runtime_kind.clone(),
            binding: req.binding.clone(),
            workspace: record.workspace,
            isolation: record.isolation,
            capabilities: record.capabilities,
            tenant_id: req.context.tenant_id.clone(),
            owner_ref: req.context.owner_ref.clone(),
            created_by: Some("principal-1".into()),
            created_at_ms: 3,
        },
    )
    .await
    .unwrap();
    req
}

#[tokio::test]
async fn durable_admission_preserves_unknown_quota_and_atomic_publication() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let repo = SqliteSessionRepository::open(&path).unwrap();
    let mut legacy = sample_record("legacy");
    repo.save(legacy.clone()).await.unwrap();
    let mut req = request("create", "open", "", "joint", "", "root", None);
    req.context.quota.max_sessions = Some(1);
    assert!(matches!(
        repo.prepare_joint_operation(req.clone()).await,
        Err(SessionDomainError::QuotaExceeded { .. })
    ));
    assert!(repo.pending_joint_operations().await.unwrap().is_empty());
    legacy.status = SessionStatus::Closed;
    repo.save(legacy).await.unwrap();
    let a = repo.prepare_joint_operation(req.clone()).await.unwrap();
    assert_eq!(a, repo.prepare_joint_operation(req.clone()).await.unwrap());
    let mut conflict = req.clone();
    conflict.native[0].digest = "b".repeat(64);
    assert!(repo.prepare_joint_operation(conflict).await.is_err());
    result(
        &repo,
        &a,
        &req.native[0],
        NativeOperationState::Unknown,
        vec![object(&req.binding)],
    )
    .await;
    drop(repo);
    let repo = SqliteSessionRepository::open(&path).unwrap();
    let pending = repo.pending_joint_operations().await.unwrap();
    assert_eq!(pending[0].admission, a);
    assert_eq!(
        pending[0].native[0].0.operation_id,
        req.native[0].operation_id
    );
    let mut other = request("other", "open", "", "other", "", "other-root", None);
    other.context.quota.max_sessions = Some(1);
    assert!(matches!(
        repo.prepare_joint_operation(other).await,
        Err(SessionDomainError::QuotaExceeded { .. })
    ));
    let mut record = sample_record("joint");
    record.execution_backing = SessionExecutionBacking::JointV1 {
        binding: req.binding.clone(),
    };
    assert!(repo
        .publish_joint_session(a.clone(), record.clone())
        .await
        .is_err());
    result(
        &repo,
        &a,
        &req.native[0],
        NativeOperationState::Completed,
        vec![object(&req.binding)],
    )
    .await;
    repo.conn.lock().unwrap().execute_batch("CREATE TRIGGER inject_failure BEFORE INSERT ON joint_refs BEGIN SELECT RAISE(ABORT,'rollback'); END;").unwrap();
    assert!(repo
        .publish_joint_session(a.clone(), record.clone())
        .await
        .is_err());
    assert!(repo.get("joint").await.unwrap().is_none());
    repo.conn
        .lock()
        .unwrap()
        .execute_batch("DROP TRIGGER inject_failure;")
        .unwrap();
    assert_eq!(
        repo.publish_joint_session(a.clone(), record.clone())
            .await
            .unwrap(),
        record
    );
    assert_eq!(
        repo.publish_joint_session(a, record.clone()).await.unwrap(),
        record
    );
    assert_eq!(repo.get("joint").await.unwrap(), Some(record.clone()));
    let mut split = record.clone();
    split.execution_backing = Default::default();
    assert!(repo.save(split).await.is_err());
    let db = repo.conn.lock().unwrap();
    let legacy_count: i64 = db
        .query_row(
            "SELECT count(*) FROM sessions WHERE runtime_id='joint'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(legacy_count, 0);
    drop(db);
    assert_eq!(
        repo.list_active(Some("tenant-1"), 100)
            .await
            .unwrap()
            .total_active,
        1
    );
    assert_eq!(
        repo.active_session_counts_by_tenant().await.unwrap()["tenant-1"],
        1
    );
}

#[tokio::test]
async fn source_fences_and_partial_creation_compensation_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let repo = SqliteSessionRepository::open(&path).unwrap();
    let (source, _) = open(&repo, "source", "source", "root").await;
    let req = request(
        "capture",
        "checkpoint",
        "source",
        "",
        "capture",
        "root",
        Some(source.binding.clone()),
    );
    let a = repo.prepare_joint_operation(req.clone()).await.unwrap();
    result(
        &repo,
        &a,
        &req.native[0],
        NativeOperationState::Unknown,
        vec![object(&req.binding)],
    )
    .await;
    let other = SqliteSessionRepository::open(&path).unwrap();
    let mut same = req.clone();
    same.context.intent_id = "racer".into();
    same.binding.binding_id = "racer".into();
    assert!(other.prepare_joint_operation(same).await.is_err());
    assert!(repo.finish_joint_compensation(a.clone()).await.is_err());
    let mut stale = a.clone();
    stale.root_epoch += 1;
    assert!(repo
        .record_joint_native_result(
            stale,
            req.native[0].step.clone(),
            req.native[0].operation_id.clone(),
            req.native[0].digest.clone(),
            JointNativeOutcome {
                state: NativeOperationState::Failed,
                resources: vec![object(&req.binding)],
                payload: serde_json::Value::Null
            }
        )
        .await
        .is_err());
    result(
        &repo,
        &a,
        &req.native[0],
        NativeOperationState::Failed,
        vec![object(&req.binding)],
    )
    .await;
    repo.begin_joint_compensation(a.clone()).await.unwrap();
    assert!(repo
        .claim_joint_native_send(a.clone(), req.native[0].step.clone())
        .await
        .is_err());
    let mut cleanup = req.native[0].clone();
    cleanup.step = "absence".into();
    cleanup.operation_id = "11111111-1111-4111-8111-111111111111".into();
    cleanup.effect = NativeEffect::ObserveAbsence;
    cleanup.compensation = true;
    repo.append_joint_native_step(a.clone(), cleanup.clone())
        .await
        .unwrap();
    result(
        &repo,
        &a,
        &cleanup,
        NativeOperationState::Completed,
        vec![object(&req.binding)],
    )
    .await;
    repo.finish_joint_compensation(a.clone()).await.unwrap();
    repo.finish_joint_compensation(a).await.unwrap();
    assert!(other.pending_joint_operations().await.unwrap().is_empty());
    // The source can capture again after compensation; the failed binding cannot publish.
    checkpoint(&other, "next", &source).await;
}

#[tokio::test]
async fn retained_references_prevent_close_until_exact_absence_is_committed() {
    let repo = SqliteSessionRepository::open_in_memory().unwrap();
    let (source, _) = open(&repo, "source", "source", "root").await;
    let mut foreign = request(
        "foreign",
        "checkpoint",
        "source",
        "",
        "foreign-checkpoint",
        "root",
        Some(source.binding.clone()),
    );
    foreign.context.tenant_id = Some("another-tenant".into());
    foreign.context.owner_ref = "tenant/another-tenant".into();
    assert!(repo.prepare_joint_operation(foreign).await.is_err());
    assert!(repo.pending_joint_operations().await.unwrap().is_empty());
    let cp = checkpoint(&repo, "checkpoint", &source).await;
    let mut close = request("close", "close", "source", "", "", "root", None);
    close.binding = source.binding.clone();
    assert!(repo.prepare_joint_operation(close.clone()).await.is_err());
    let mut deletion = request(
        "delete",
        "delete_checkpoint",
        "",
        "",
        "checkpoint",
        "root",
        None,
    );
    deletion.binding = cp.binding.clone();
    let a = repo
        .prepare_joint_operation(deletion.clone())
        .await
        .unwrap();
    assert!(repo
        .finish_joint_release(a.clone(), "checkpoint".into(), "checkpoint".into(), 9)
        .await
        .is_err());
    // A child cannot retain a parent that has entered releasing.
    let load = request(
        "load",
        "load",
        "",
        "new",
        "checkpoint",
        "root",
        Some(cp.binding.clone()),
    );
    assert!(repo.prepare_joint_operation(load).await.is_err());
    let n = &deletion.native[0];
    assert!(repo
        .record_joint_native_result(
            a.clone(),
            n.step.clone(),
            n.operation_id.clone(),
            n.digest.clone(),
            JointNativeOutcome {
                state: NativeOperationState::Completed,
                resources: vec![object(&source.binding)],
                payload: serde_json::Value::Null
            }
        )
        .await
        .is_err());
    result(
        &repo,
        &a,
        n,
        NativeOperationState::Completed,
        vec![object(&cp.binding)],
    )
    .await;
    repo.finish_joint_release(a.clone(), "checkpoint".into(), "checkpoint".into(), 9)
        .await
        .unwrap();
    repo.finish_joint_release(a, "checkpoint".into(), "checkpoint".into(), 9)
        .await
        .unwrap();
    assert!(repo
        .get_joint_checkpoint("checkpoint")
        .await
        .unwrap()
        .is_none());
    let a = repo.prepare_joint_operation(close.clone()).await.unwrap();
    result(
        &repo,
        &a,
        &close.native[0],
        NativeOperationState::Completed,
        vec![object(&source.binding)],
    )
    .await;
    repo.finish_joint_release(a, "session".into(), "source".into(), 10)
        .await
        .unwrap();
    assert_eq!(
        repo.get("source").await.unwrap().unwrap().status,
        SessionStatus::Closed
    );
    assert!(repo
        .active_session_counts_by_tenant()
        .await
        .unwrap()
        .is_empty());
    assert!(repo.pending_joint_operations().await.unwrap().is_empty());
}

#[tokio::test]
async fn independent_connections_serialize_replays_conflicts_and_source_fences() {
    async fn race(
        left: &SqliteSessionRepository,
        right: &SqliteSessionRepository,
        a: JointPrepareRequest,
        b: JointPrepareRequest,
    ) -> (
        std::result::Result<JointAdmission, SessionDomainError>,
        std::result::Result<JointAdmission, SessionDomainError>,
    ) {
        let barrier = tokio::sync::Barrier::new(2);
        tokio::join!(
            async {
                barrier.wait().await;
                left.prepare_joint_operation(a).await
            },
            async {
                barrier.wait().await;
                right.prepare_joint_operation(b).await
            },
        )
    }
    for different_digest in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let left = SqliteSessionRepository::open(&path).unwrap();
        let right = SqliteSessionRepository::open(&path).unwrap();
        let a = request("concurrent", "open", "", "target", "", "root", None);
        let mut b = a.clone();
        if different_digest {
            b.native[0].digest = "b".repeat(64);
        }
        let (one, two) = race(&left, &right, a.clone(), b).await;
        if different_digest {
            let (winner, loser) = if one.is_ok() { (one, two) } else { (two, one) };
            winner.unwrap();
            assert!(matches!(loser, Err(SessionDomainError::Conflict { .. })));
        } else {
            assert_eq!(one.unwrap(), two.unwrap());
        }
        let db = left.conn.lock().unwrap();
        for (table, count) in [
            ("joint_operations", 1),
            ("joint_bindings", 1),
            ("joint_roots", 1),
            ("joint_native_steps", 1),
            ("joint_public_claims", 1),
            ("joint_reservations", 2),
            ("joint_fences", 2),
        ] {
            assert_eq!(
                db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                count
            );
        }
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
                "SELECT epoch FROM joint_fences WHERE kind='source'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        drop(db);
        assert_eq!(right.pending_joint_operations().await.unwrap().len(), 1);
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let left = SqliteSessionRepository::open(&path).unwrap();
    let right = SqliteSessionRepository::open(&path).unwrap();
    let (source, _) = open(&left, "source", "source", "root").await;
    let a = request(
        "capture-a",
        "checkpoint",
        "source",
        "",
        "cp-a",
        "root",
        Some(source.binding.clone()),
    );
    let b = request(
        "capture-b",
        "checkpoint",
        "source",
        "",
        "cp-b",
        "root",
        Some(source.binding),
    );
    let (one, two) = race(&left, &right, a, b).await;
    let (winner, loser) = if one.is_ok() { (one, two) } else { (two, one) };
    let winner = winner.unwrap();
    assert!(matches!(loser, Err(SessionDomainError::Conflict { .. })));
    let pending = right.pending_joint_operations().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].admission, winner);
    assert!(pending[0]
        .native
        .iter()
        .all(|(_, outcome)| outcome.is_none()));
    let db = left.conn.lock().unwrap();
    assert_eq!(
        db.query_row(
            "SELECT holder FROM joint_fences WHERE kind='source' AND id='source'",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        winner.intent_id
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM joint_public_claims WHERE kind='checkpoint'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM joint_reservations WHERE kind='checkpoint'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    // Force lock contention independently of all identity and quota checks.
    right
        .conn
        .lock()
        .unwrap()
        .busy_timeout(std::time::Duration::ZERO)
        .unwrap();
    db.execute_batch("BEGIN IMMEDIATE").unwrap();
    let busy = right
        .prepare_joint_operation(pending[0].request.clone())
        .await
        .unwrap_err();
    match busy {
        SessionDomainError::Internal { message, .. } => {
            assert!(message.contains("database is locked"), "{message}")
        }
        other => panic!("lock contention changed domain meaning: {other}"),
    }
    db.execute_batch("ROLLBACK").unwrap();
    drop(db);
    assert_eq!(
        right
            .prepare_joint_operation(pending[0].request.clone())
            .await
            .unwrap(),
        winner
    );
    assert_eq!(
        right.pending_joint_operations().await.unwrap()[0].native[0]
            .0
            .operation_id,
        pending[0].native[0].0.operation_id
    );
}
