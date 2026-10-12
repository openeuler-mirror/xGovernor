//! Durable snapshot intent recovery using real SQLite reopen boundaries.
//! Snapshot and archive deletion are recorded fixtures, not full content recovery.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

struct RecoveryClock(AtomicU64);

impl Clock for RecoveryClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct RecoveryRuntime {
    inner: CompletingRuntime,
    stops: Mutex<Vec<String>>,
    archive_deletes: Mutex<Vec<agent_runtime_protocol::RuntimeStateSnapshot>>,
    fail_archive_delete: AtomicBool,
}

#[async_trait]
impl AgentRuntime for RecoveryRuntime {
    fn runtime_kind(&self) -> &str {
        "test"
    }

    fn capabilities(&self) -> BTreeSet<agent_runtime_protocol::RuntimeCapability> {
        self.inner.capabilities()
    }

    async fn start(
        &self,
        request: RuntimeStartRequest,
        context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        self.inner.start(request, context).await
    }

    async fn attach(
        &self,
        runtime_id: &str,
        context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        self.inner.attach(runtime_id, context).await
    }

    async fn stop(&self, runtime_id: &str) -> Result<(), RuntimeError> {
        self.stops.lock().await.push(runtime_id.into());
        self.inner.stop(runtime_id).await
    }

    async fn check_alive(&self, runtime_id: &str) -> Result<bool, RuntimeError> {
        self.inner.check_alive(runtime_id).await
    }

    async fn submit_turn(
        &self,
        request: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, RuntimeError> {
        self.inner.submit_turn(request).await
    }

    async fn answer_interaction(
        &self,
        request: RuntimeInteractionInput,
    ) -> Result<(), RuntimeError> {
        self.inner.answer_interaction(request).await
    }

    async fn cancel(&self, request: RuntimeCancelRequest) -> Result<(), RuntimeError> {
        self.inner.cancel(request).await
    }

    async fn delete_checkpoint_state(
        &self,
        state: &agent_runtime_protocol::RuntimeStateSnapshot,
    ) -> Result<(), RuntimeError> {
        self.archive_deletes.lock().await.push(state.clone());
        if self.fail_archive_delete.load(Ordering::SeqCst) {
            return Err(RuntimeError::WorkerUnavailable {
                message: "archive delete failed".into(),
                retryable: true,
            });
        }
        Ok(())
    }
}

fn recovery_application(
    records: Arc<SqliteSessionRepository>,
    runtime: Arc<RecoveryRuntime>,
    providers: HashMap<String, Arc<InstanceManager>>,
    clock: Arc<RecoveryClock>,
) -> SessionApplication {
    SessionApplication::new(
        runtime,
        providers,
        records,
        Arc::new(FixedTurnId),
        Arc::new(FixedTurnId),
        Arc::new(UnusedEnvironment),
        clock,
    )
}

fn checkpoint_intent(checkpoint: &CheckpointRecord) -> serde_json::Value {
    serde_json::json!({
        "kind": "checkpoint", "runtime_kind": "test", "provider": "local",
        "source": checkpoint.source_runtime_id, "checkpoint": checkpoint.checkpoint_id,
        "snapshot": checkpoint.provider_snapshot_id, "history": checkpoint.runtime_state,
    })
}

#[tokio::test]
async fn failed_delete_stays_hidden_and_retry_deadline_survives_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("sessions.db");
    let records = Arc::new(SqliteSessionRepository::open(&database).unwrap());
    let checkpoint = checkpoint_manager_record();
    records.save_checkpoint(checkpoint.clone()).await.unwrap();
    let clock = Arc::new(RecoveryClock(AtomicU64::new(100)));
    let runtime = Arc::new(RecoveryRuntime::default());
    let deletes = Arc::new(Mutex::new(Vec::new()));
    let app = recovery_application(
        records.clone(),
        runtime.clone(),
        snapshot_provider_managers_with_deletes(true, Some(deletes.clone())),
        clock.clone(),
    );
    assert!(matches!(
        app.delete_checkpoint(
            &tenant_ctx(),
            SessionCheckpointDeleteRequest {
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                lease: Default::default(),
            }
        )
        .await,
        Err(SessionDomainError::Unavailable { .. })
    ));
    assert!(app.reconcile_snapshot_intents().await.is_err());
    let pending = records.operation_intents().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].0, "delete:checkpoint-1");
    assert_eq!(pending[0].1["snapshot"], checkpoint.provider_snapshot_id);
    assert_eq!(
        pending[0].1["history"],
        serde_json::to_value(&checkpoint.runtime_state).unwrap()
    );
    assert_eq!(pending[0].1["retry_count"], 1);
    let retry_at = pending[0].1["retry_at_ms"].as_u64().unwrap();
    assert!(retry_at > clock.now_ms());
    assert!(pending[0].1["last_error"].as_str().is_some());
    assert!(runtime.archive_deletes.lock().await.is_empty());
    drop(app);
    drop(records);

    let records = Arc::new(SqliteSessionRepository::open(&database).unwrap());
    assert_eq!(records.operation_intents().await.unwrap(), pending);
    assert!(records
        .get_checkpoint("checkpoint-1")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        records
            .list_checkpoints(Some("tenant-a"), 10, 0)
            .await
            .unwrap()
            .total,
        0
    );
    let app = recovery_application(
        records.clone(),
        runtime.clone(),
        snapshot_provider_managers_with_deletes(false, Some(deletes.clone())),
        clock.clone(),
    );
    assert!(matches!(
        app.load_checkpoint(
            &tenant_ctx(),
            SessionLoadRequest {
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                runtime_id: Some("rejected-load".into()),
                conversation_id: None,
                sender_id: None,
                requested_capabilities: Default::default(),
                deployment: Default::default(),
                lease: Default::default(),
                llm: None,
            }
        )
        .await,
        Err(SessionDomainError::NotFound { .. })
    ));
    assert!(runtime.inner.started.lock().await.is_none());
    let attempts = deletes.lock().await.len();
    clock.0.store(retry_at - 1, Ordering::SeqCst);
    app.reconcile_snapshot_intents().await.unwrap();
    assert_eq!(
        deletes.lock().await.len(),
        attempts,
        "reopen must not bypass backoff"
    );
    assert_eq!(records.operation_intents().await.unwrap(), pending);
    clock.0.store(retry_at, Ordering::SeqCst);
    app.reconcile_snapshot_intents().await.unwrap();
    assert_eq!(deletes.lock().await.len(), attempts + 1);
    assert_eq!(
        *runtime.archive_deletes.lock().await,
        vec![checkpoint.runtime_state.clone()]
    );
    assert!(records.operation_intents().await.unwrap().is_empty());
    app.reconcile_snapshot_intents().await.unwrap();
    assert_eq!(deletes.lock().await.len(), attempts + 1);
    drop(app);
    drop(records);
    let records = SqliteSessionRepository::open(&database).unwrap();
    assert!(records.operation_intents().await.unwrap().is_empty());
    assert!(records
        .get_checkpoint("checkpoint-1")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn published_checkpoint_is_preserved_when_recovering_a_stale_publication_intent() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("sessions.db");
    let checkpoint = checkpoint_manager_record();
    {
        let records = SqliteSessionRepository::open(&database).unwrap();
        records.save_checkpoint(checkpoint.clone()).await.unwrap();
        // Simulate interruption after publication but before intent retirement.
        records
            .save_operation_intent(&checkpoint.checkpoint_id, checkpoint_intent(&checkpoint))
            .await
            .unwrap();
    }
    let records = Arc::new(SqliteSessionRepository::open(&database).unwrap());
    let runtime = Arc::new(RecoveryRuntime::default());
    let deletes = Arc::new(Mutex::new(Vec::new()));
    let app = recovery_application(
        records.clone(),
        runtime.clone(),
        snapshot_provider_managers_with_deletes(true, Some(deletes.clone())),
        Arc::new(RecoveryClock(AtomicU64::new(100))),
    );
    app.reconcile_snapshot_intents().await.unwrap();
    assert_eq!(
        records.get_checkpoint("checkpoint-1").await.unwrap(),
        Some(checkpoint.clone())
    );
    assert_eq!(
        records
            .list_checkpoints(Some("tenant-a"), 10, 0)
            .await
            .unwrap()
            .total,
        1
    );
    assert!(records.operation_intents().await.unwrap().is_empty());
    assert!(deletes.lock().await.is_empty());
    assert!(runtime.archive_deletes.lock().await.is_empty());
    assert!(runtime.stops.lock().await.is_empty());
    drop(app);
    drop(records);
    assert_eq!(
        SqliteSessionRepository::open(&database)
            .unwrap()
            .get_checkpoint("checkpoint-1")
            .await
            .unwrap(),
        Some(checkpoint)
    );
}

#[tokio::test]
async fn abandoned_checkpoint_cleanup_preserves_published_sibling_and_source() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("sessions.db");
    let source = tenant_test_record();
    let sibling = checkpoint_manager_record();
    let mut abandoned = sibling.clone();
    abandoned.checkpoint_id = "checkpoint-abandoned".into();
    abandoned.provider_snapshot_id = "snapshot-abandoned".into();
    abandoned.runtime_state.state = serde_json::json!({"archive": "abandoned"});
    {
        let records = SqliteSessionRepository::open(&database).unwrap();
        records.save(source.clone()).await.unwrap();
        records.save_checkpoint(sibling.clone()).await.unwrap();
        // Simulate interruption after external capture but before publication.
        records
            .save_operation_intent(&abandoned.checkpoint_id, checkpoint_intent(&abandoned))
            .await
            .unwrap();
    }
    let records = Arc::new(SqliteSessionRepository::open(&database).unwrap());
    let runtime = Arc::new(RecoveryRuntime::default());
    let deletes = Arc::new(Mutex::new(Vec::new()));
    let providers = snapshot_provider_managers_with_deletes(false, Some(deletes.clone()));
    start_test_provider(&providers, "runtime-1").await;
    let app = recovery_application(
        records.clone(),
        runtime.clone(),
        providers.clone(),
        Arc::new(RecoveryClock(AtomicU64::new(100))),
    );
    app.reconcile_snapshot_intents().await.unwrap();
    assert_eq!(*deletes.lock().await, vec!["snapshot-abandoned"]);
    assert_eq!(
        *runtime.archive_deletes.lock().await,
        vec![abandoned.runtime_state]
    );
    assert!(runtime.stops.lock().await.is_empty());
    assert!(records.operation_intents().await.unwrap().is_empty());
    assert_eq!(records.get("runtime-1").await.unwrap(), Some(source));
    assert_eq!(
        records.get_checkpoint("checkpoint-1").await.unwrap(),
        Some(sibling)
    );
    assert_eq!(
        providers["local"]
            .inspect_instance("runtime-1")
            .await
            .unwrap()
            .state,
        provider_protocol::ProviderLifecycleState::Active
    );
}

#[tokio::test]
async fn load_recovery_reclaims_only_the_unpublished_target() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("sessions.db");
    let checkpoint = checkpoint_manager_record();
    let source = tenant_test_record();
    let mut sibling = source.clone();
    sibling.runtime_id = "sibling".into();
    {
        let records = SqliteSessionRepository::open(&database).unwrap();
        records.save(source.clone()).await.unwrap();
        records.save(sibling.clone()).await.unwrap();
        records.save_checkpoint(checkpoint.clone()).await.unwrap();
        for target in ["abandoned", "sibling"] {
            records
                .save_operation_intent(
                    &format!("load:{target}"),
                    serde_json::json!({
                        "kind": "load", "provider": "local", "runtime_kind": "test",
                        "target": target, "checkpoint": checkpoint.checkpoint_id,
                        "snapshot": checkpoint.provider_snapshot_id,
                    }),
                )
                .await
                .unwrap();
        }
    }
    let records = Arc::new(SqliteSessionRepository::open(&database).unwrap());
    let runtime = Arc::new(RecoveryRuntime::default());
    let deletes = Arc::new(Mutex::new(Vec::new()));
    let providers = snapshot_provider_managers_with_deletes(true, Some(deletes.clone()));
    for id in ["runtime-1", "sibling", "abandoned"] {
        start_test_provider(&providers, id).await;
    }
    let app = recovery_application(
        records.clone(),
        runtime.clone(),
        providers.clone(),
        Arc::new(RecoveryClock(AtomicU64::new(100))),
    );
    app.reconcile_snapshot_intents().await.unwrap();
    assert_eq!(*runtime.stops.lock().await, vec!["abandoned"]);
    assert!(providers["local"]
        .inspect_instance("abandoned")
        .await
        .is_err());
    for id in ["runtime-1", "sibling"] {
        assert_eq!(
            providers["local"].inspect_instance(id).await.unwrap().state,
            provider_protocol::ProviderLifecycleState::Active
        );
    }
    assert_eq!(records.get("runtime-1").await.unwrap(), Some(source));
    assert_eq!(records.get("sibling").await.unwrap(), Some(sibling));
    assert_eq!(
        records.get_checkpoint("checkpoint-1").await.unwrap(),
        Some(checkpoint)
    );
    assert!(records.operation_intents().await.unwrap().is_empty());
    assert!(deletes.lock().await.is_empty());
    assert!(runtime.archive_deletes.lock().await.is_empty());
    app.reconcile_snapshot_intents().await.unwrap();
    assert_eq!(*runtime.stops.lock().await, vec!["abandoned"]);
}

#[tokio::test]
async fn archive_cleanup_failure_remains_durable_after_provider_delete_succeeds() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("sessions.db");
    let records = Arc::new(SqliteSessionRepository::open(&database).unwrap());
    let checkpoint = checkpoint_manager_record();
    records.save_checkpoint(checkpoint.clone()).await.unwrap();
    let runtime = Arc::new(RecoveryRuntime::default());
    runtime.fail_archive_delete.store(true, Ordering::SeqCst);
    let deletes = Arc::new(Mutex::new(Vec::new()));
    let clock = Arc::new(RecoveryClock(AtomicU64::new(100)));
    let app = recovery_application(
        records.clone(),
        runtime.clone(),
        snapshot_provider_managers_with_deletes(false, Some(deletes.clone())),
        clock.clone(),
    );
    assert!(app
        .delete_checkpoint(
            &tenant_ctx(),
            SessionCheckpointDeleteRequest {
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                lease: Default::default(),
            }
        )
        .await
        .is_err());
    let pending = records.operation_intents().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(*deletes.lock().await, vec!["snapshot-1"]);
    assert_eq!(
        *runtime.archive_deletes.lock().await,
        vec![checkpoint.runtime_state.clone()]
    );
    drop(app);
    drop(records);
    let records = Arc::new(SqliteSessionRepository::open(&database).unwrap());
    assert_eq!(records.operation_intents().await.unwrap(), pending);
    assert!(records
        .get_checkpoint("checkpoint-1")
        .await
        .unwrap()
        .is_none());
    runtime.fail_archive_delete.store(false, Ordering::SeqCst);
    let app = recovery_application(
        records.clone(),
        runtime.clone(),
        snapshot_provider_managers_with_deletes(false, Some(deletes.clone())),
        clock,
    );
    app.reconcile_snapshot_intents().await.unwrap();
    assert!(records.operation_intents().await.unwrap().is_empty());
    assert!(records
        .get_checkpoint("checkpoint-1")
        .await
        .unwrap()
        .is_none());
    assert_eq!(*deletes.lock().await, vec!["snapshot-1", "snapshot-1"]);
    assert_eq!(
        *runtime.archive_deletes.lock().await,
        vec![checkpoint.runtime_state.clone(), checkpoint.runtime_state]
    );
}

#[tokio::test]
async fn failed_recovery_retains_its_intent_without_blocking_an_independent_publication() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("sessions.db");
    let published = checkpoint_manager_record();
    let mut abandoned = published.clone();
    abandoned.checkpoint_id = "checkpoint-0-abandoned".into();
    abandoned.provider_snapshot_id = "snapshot-abandoned".into();
    {
        let records = SqliteSessionRepository::open(&database).unwrap();
        records.save_checkpoint(published.clone()).await.unwrap();
        for checkpoint in [&abandoned, &published] {
            records
                .save_operation_intent(&checkpoint.checkpoint_id, checkpoint_intent(checkpoint))
                .await
                .unwrap();
        }
    }
    let records = Arc::new(SqliteSessionRepository::open(&database).unwrap());
    let runtime = Arc::new(RecoveryRuntime::default());
    let deletes = Arc::new(Mutex::new(Vec::new()));
    let app = recovery_application(
        records.clone(),
        runtime.clone(),
        snapshot_provider_managers_with_deletes(true, Some(deletes.clone())),
        Arc::new(RecoveryClock(AtomicU64::new(100))),
    );
    assert!(app.reconcile_snapshot_intents().await.is_err());
    assert_eq!(*deletes.lock().await, vec!["snapshot-abandoned"]);
    let pending = records.operation_intents().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].0, abandoned.checkpoint_id);
    assert_eq!(pending[0].1["retry_count"], 1);
    assert_eq!(
        records.get_checkpoint("checkpoint-1").await.unwrap(),
        Some(published.clone())
    );
    assert!(runtime.archive_deletes.lock().await.is_empty());
    drop(app);
    drop(records);
    let records = SqliteSessionRepository::open(&database).unwrap();
    assert_eq!(records.operation_intents().await.unwrap(), pending);
    assert_eq!(
        records.get_checkpoint("checkpoint-1").await.unwrap(),
        Some(published)
    );
}
