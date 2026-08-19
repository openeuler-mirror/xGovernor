use async_trait::async_trait;
use backend::{ActiveLedgerEntry, OperationAttach, ProviderInstanceLedger};
use backend::local::LocalProvider;
use operation_protocol::capability::exec::ExecRequest;
use operation_protocol::OperationBackend;
use provider_protocol::*;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use xgovernor_manager::{InstanceManager, InstanceManagerConfig, ADMIN_OWNER_REF};

struct CreateGate {
    entered: tokio::sync::Notify,
    proceed: tokio::sync::Notify,
}

/// Fake `ProviderLifecycle` with independently controllable create/
/// delete failure injection, used to exercise quota, retry, and
/// pending-release logic without a real provider.
struct FakeLifecycle {
    next_id: AtomicU64,
    fail_create_remaining: AtomicU32,
    fail_create_permanently: AtomicBool,
    fail_delete_remaining: AtomicU32,
    live_instances: Mutex<Vec<ProviderInstance>>,
    create_calls: AtomicU32,
    delete_calls: AtomicU32,
    gate: Mutex<Option<Arc<CreateGate>>>,
}

impl FakeLifecycle {
    fn new() -> Self {
        Self {
            next_id: AtomicU64::new(0),
            fail_create_remaining: AtomicU32::new(0),
            fail_create_permanently: AtomicBool::new(false),
            fail_delete_remaining: AtomicU32::new(0),
            live_instances: Mutex::new(Vec::new()),
            create_calls: AtomicU32::new(0),
            delete_calls: AtomicU32::new(0),
            gate: Mutex::new(None),
        }
    }

    fn set_gate(&self, gate: Arc<CreateGate>) {
        *self.gate.lock().unwrap() = Some(gate);
    }

    fn instance(&self, instance_id: String) -> ProviderInstance {
        ProviderInstance {
            backend_id: BackendId("fake".to_string()),
            provider: ProviderKind("fake".to_string()),
            instance_id: ProviderInstanceId(instance_id),
            state: ProviderLifecycleState::Active,
            endpoint: None,
            snapshot: None,
            capabilities: Default::default(),
            resources: Default::default(),
            metadata: Value::Null,
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }

    fn next_instance_id(&self) -> String {
        format!("fake-{}", self.next_id.fetch_add(1, Ordering::Relaxed))
    }
}

fn should_fail_and_decrement(counter: &AtomicU32) -> bool {
    loop {
        let current = counter.load(Ordering::SeqCst);
        if current == 0 {
            return false;
        }
        if counter
            .compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return true;
        }
    }
}

#[async_trait]
impl ProviderLifecycle for FakeLifecycle {
    async fn create(
        &self,
        _request: ProviderCreateRequest,
    ) -> Result<ProviderInstance, ProviderControlError> {
        self.create_calls.fetch_add(1, Ordering::SeqCst);

        let gate = self.gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.proceed.notified().await;
        }

        if self.fail_create_permanently.load(Ordering::SeqCst) {
            return Err(ProviderControlError::InvalidRequest {
                message: "permanent fake create failure".to_string(),
            });
        }
        if should_fail_and_decrement(&self.fail_create_remaining) {
            return Err(ProviderControlError::Transport {
                message: "transient fake create failure".to_string(),
            });
        }
        let instance = self.instance(self.next_instance_id());
        self.live_instances.lock().unwrap().push(instance.clone());
        Ok(instance)
    }

    async fn load(
        &self,
        _request: ProviderLoadRequest,
    ) -> Result<ProviderInstance, ProviderControlError> {
        unimplemented!("not exercised: InstanceManager never calls load()")
    }

    async fn pause(
        &self,
        _request: ProviderPauseRequest,
    ) -> Result<ProviderSnapshot, ProviderControlError> {
        unimplemented!("not exercised: InstanceManager never calls pause()")
    }

    async fn checkpoint(
        &self,
        _request: provider_protocol::ProviderCheckpointRequest,
    ) -> Result<ProviderSnapshot, ProviderControlError> {
        unimplemented!("not exercised: checkpoint tests")
    }

    async fn delete(
        &self,
        request: ProviderDeleteRequest,
    ) -> Result<ProviderDeleteOutcome, ProviderControlError> {
        self.delete_calls.fetch_add(1, Ordering::SeqCst);
        if should_fail_and_decrement(&self.fail_delete_remaining) {
            return Err(ProviderControlError::Transport {
                message: "transient fake delete failure".to_string(),
            });
        }
        if let Some(id) = &request.instance_id {
            self.live_instances
                .lock()
                .unwrap()
                .retain(|instance| instance.instance_id != *id);
        }
        Ok(ProviderDeleteOutcome {
            backend_id: request.backend_id,
            provider: ProviderKind("fake".to_string()),
            instance_id: request.instance_id,
            deleted: true,
            retained_snapshots: Vec::new(),
            deleted_snapshots: Vec::new(),
            correlation: request.correlation,
        })
    }

    async fn inspect(
        &self,
        _request: ProviderInspectRequest,
    ) -> Result<ProviderInstanceStatus, ProviderControlError> {
        unimplemented!("not exercised by these tests")
    }

    async fn list_instances(&self) -> Result<Vec<ProviderInstance>, ProviderControlError> {
        Ok(self.live_instances.lock().unwrap().clone())
    }
}

struct FakeBackend {
    id: String,
}

#[async_trait]
impl OperationBackend for FakeBackend {
    fn backend_id(&self) -> &str {
        &self.id
    }
    fn capabilities(&self) -> operation_protocol::OperationBackendCapabilities {
        unimplemented!("not exercised by these tests")
    }
    fn paths(&self) -> &dyn operation_protocol::capability::OperationPathResolver {
        unimplemented!("not exercised by these tests")
    }
    fn files(&self) -> &dyn operation_protocol::capability::OperationFileSystem {
        unimplemented!("not exercised by these tests")
    }
    fn search(&self) -> &dyn operation_protocol::capability::OperationSearch {
        unimplemented!("not exercised by these tests")
    }
    fn exec(&self) -> &dyn operation_protocol::capability::OperationExec {
        unimplemented!("not exercised by these tests")
    }
    fn export(&self) -> &dyn operation_protocol::capability::OperationExport {
        unimplemented!("not exercised by these tests")
    }
    async fn shutdown(&self) -> Result<(), operation_protocol::OperationError> {
        Ok(())
    }
}

struct FakeAttach {
    fail: AtomicBool,
}

impl FakeAttach {
    fn new() -> Self {
        Self {
            fail: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl OperationAttach for FakeAttach {
    async fn attach(
        &self,
        instance: &ProviderInstance,
    ) -> Result<Arc<dyn OperationBackend>, ProviderControlError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(ProviderControlError::Transport {
                message: "fake attach failure".to_string(),
            });
        }
        Ok(Arc::new(FakeBackend {
            id: instance.instance_id.0.clone(),
        }))
    }
}

#[derive(Default)]
struct FakeLedger {
    rows: Mutex<HashMap<String, ActiveLedgerEntry>>,
    fail_record_created: AtomicBool,
}

#[async_trait]
impl ProviderInstanceLedger for FakeLedger {
    async fn record_created(
        &self,
        runtime_id: &str,
        owner_ref: &str,
        instance: &ProviderInstance,
    ) -> Result<(), ProviderControlError> {
        if self.fail_record_created.load(Ordering::SeqCst) {
            return Err(ProviderControlError::Transport {
                message: "fake ledger write failure".to_string(),
            });
        }
        self.rows.lock().unwrap().insert(
            runtime_id.to_string(),
            ActiveLedgerEntry {
                runtime_id: runtime_id.to_string(),
                owner_ref: owner_ref.to_string(),
                instance: instance.clone(),
            },
        );
        Ok(())
    }

    async fn record_deleted(&self, runtime_id: &str) -> Result<(), ProviderControlError> {
        self.rows.lock().unwrap().remove(runtime_id);
        Ok(())
    }

    async fn list_active(
        &self,
        provider: &ProviderKind,
    ) -> Result<Vec<ActiveLedgerEntry>, ProviderControlError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|entry| entry.instance.provider == *provider)
            .cloned()
            .collect())
    }
}

fn manager_with(
    max_per_owner: usize,
    max_global: usize,
) -> (Arc<InstanceManager>, Arc<FakeLifecycle>, Arc<FakeLedger>) {
    let lifecycle = Arc::new(FakeLifecycle::new());
    let attach = Arc::new(FakeAttach::new());
    let ledger = Arc::new(FakeLedger::default());
    let manager = Arc::new(InstanceManager::new(
        lifecycle.clone() as Arc<dyn ProviderLifecycle>,
        attach as Arc<dyn OperationAttach>,
        ledger.clone() as Arc<dyn ProviderInstanceLedger>,
        ProviderKind("fake".to_string()),
        InstanceManagerConfig::new(max_per_owner, max_global),
    ));
    (manager, lifecycle, ledger)
}

async fn start(
    manager: &InstanceManager,
    runtime_id: &str,
    owner_ref: &str,
) -> Result<Arc<dyn OperationBackend>, ProviderControlError> {
    manager
        .start_instance(
            runtime_id.to_string(),
            BackendId("fake".to_string()),
            owner_ref.to_string(),
            Value::Null,
        )
        .await
}

/// `Result<Arc<dyn OperationBackend>, _>::unwrap_err()` doesn't compile
/// (`dyn OperationBackend` isn't `Debug`, which `unwrap_err` requires
/// purely to format the *Ok* value in its panic message). This sidesteps
/// that without adding a `Debug` impl to the trait just for tests.
fn expect_err<T>(result: Result<T, ProviderControlError>) -> ProviderControlError {
    match result {
        Ok(_) => panic!("expected Err, got Ok"),
        Err(error) => error,
    }
}

#[tokio::test]
async fn allows_up_to_owner_cap_then_rejects() {
    let (manager, _lifecycle, _ledger) = manager_with(2, 100);
    start(&manager, "r1", "owner-a").await.unwrap();
    start(&manager, "r2", "owner-a").await.unwrap();
    assert_eq!(manager.active_count("owner-a"), 2);

    let error = expect_err(start(&manager, "r3", "owner-a").await);
    assert!(matches!(
        error,
        ProviderControlError::ResourceLimitExceeded {
            current: 2,
            max: 2,
            ..
        }
    ));
}

#[tokio::test]
async fn global_cap_rejects_across_different_owners() {
    let (manager, _lifecycle, _ledger) = manager_with(10, 2);
    start(&manager, "r1", "owner-a").await.unwrap();
    start(&manager, "r2", "owner-b").await.unwrap();
    assert_eq!(manager.global_active_count(), 2);

    // owner-c has plenty of per-owner headroom (cap 10) but the global
    // ceiling (2) is already exhausted by owner-a + owner-b.
    let error = expect_err(start(&manager, "r3", "owner-c").await);
    assert!(matches!(
        error,
        ProviderControlError::ResourceLimitExceeded {
            current: 2,
            max: 2,
            ..
        }
    ));
}

#[tokio::test]
async fn admin_owner_ref_is_exempt_from_both_caps() {
    let (manager, _lifecycle, _ledger) = manager_with(1, 1);
    start(&manager, "r1", ADMIN_OWNER_REF).await.unwrap();
    start(&manager, "r2", ADMIN_OWNER_REF).await.unwrap();
    start(&manager, "r3", ADMIN_OWNER_REF).await.unwrap();
    assert_eq!(
        manager.active_count(ADMIN_OWNER_REF),
        3,
        "still counted for diagnostics"
    );
    assert_eq!(manager.global_active_count(), 3);

    // A regular tenant at the same caps is still capped, proving this is
    // an identity-specific exemption, not a global bypass.
    expect_err(start(&manager, "r4", "tenant/tenant-a").await);
}

#[tokio::test]
async fn stop_instance_releases_the_slot_and_removes_the_registry_entry() {
    let (manager, _lifecycle, _ledger) = manager_with(1, 10);
    start(&manager, "r1", "owner-a").await.unwrap();
    assert_eq!(manager.active_count("owner-a"), 1);

    manager.stop_instance("r1").await.unwrap();
    assert_eq!(manager.active_count("owner-a"), 0);
    assert!(matches!(
        expect_err(manager.backend_for("r1")),
        ProviderControlError::NotFound { .. }
    ));

    // The freed slot can be reused under the same runtime_id.
    start(&manager, "r1", "owner-a").await.unwrap();
    assert_eq!(manager.active_count("owner-a"), 1);
}

#[tokio::test]
async fn duplicate_start_instance_on_an_already_registered_runtime_id_is_conflict() {
    let (manager, _lifecycle, _ledger) = manager_with(10, 10);
    start(&manager, "r1", "owner-a").await.unwrap();
    let error = expect_err(start(&manager, "r1", "owner-a").await);
    assert!(matches!(error, ProviderControlError::Conflict { .. }));
    // The original registration must be untouched.
    assert_eq!(manager.active_count("owner-a"), 1);
}

/// The regression test for the bug `ProviderBoundRuntime`'s own module
/// doc flagged as deliberately unfixed: two genuinely concurrent
/// `start_instance` calls for the same `runtime_id` must not both
/// proceed to create a provider instance. Uses a `CreateGate` to force
/// call A to sit *inside* `create()` (holding the runtime_id lock) while
/// call B is spawned and attempts to acquire that same lock — proving B
/// blocks on the lock (not on a data race that happens to usually work)
/// before A is allowed to finish.
#[tokio::test]
async fn concurrent_start_instance_for_the_same_runtime_id_is_serialized() {
    let (manager, lifecycle, _ledger) = manager_with(10, 10);
    let gate = Arc::new(CreateGate {
        entered: tokio::sync::Notify::new(),
        proceed: tokio::sync::Notify::new(),
    });
    lifecycle.set_gate(gate.clone());

    let task_a = {
        let manager = Arc::clone(&manager);
        tokio::spawn(async move { start(&manager, "r1", "owner-a").await })
    };

    // Wait until call A is inside create(), blocked on gate.proceed —
    // it is holding the runtime_id lock the whole time.
    gate.entered.notified().await;

    let task_b = {
        let manager = Arc::clone(&manager);
        tokio::spawn(async move { start(&manager, "r1", "owner-a").await })
    };

    // Give B a chance to actually reach and start waiting on the lock
    // (cooperative yields are sufficient/deterministic on a
    // current-thread runtime; no real preemption needed).
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    // create_calls must still be 1: B cannot have reached create() yet,
    // since it is blocked on the same runtime_id lock A is holding.
    assert_eq!(lifecycle.create_calls.load(Ordering::SeqCst), 1);

    gate.proceed.notify_one();

    let result_a = task_a.await.unwrap();
    let result_b = task_b.await.unwrap();

    let outcomes = [result_a.is_ok(), result_b.is_ok()];
    assert_eq!(
        outcomes.iter().filter(|ok| **ok).count(),
        1,
        "exactly one of the two concurrent start_instance calls must succeed"
    );
    let error = if result_a.is_err() {
        expect_err(result_a)
    } else {
        expect_err(result_b)
    };
    assert!(matches!(error, ProviderControlError::Conflict { .. }));
    assert_eq!(manager.active_count("owner-a"), 1);
}

#[tokio::test]
async fn attach_failure_triggers_compensating_delete_and_quota_rollback() {
    let lifecycle = Arc::new(FakeLifecycle::new());
    let attach = Arc::new(FakeAttach::new());
    attach.fail.store(true, Ordering::SeqCst);
    let ledger = Arc::new(FakeLedger::default());
    let manager = InstanceManager::new(
        lifecycle.clone() as Arc<dyn ProviderLifecycle>,
        attach as Arc<dyn OperationAttach>,
        ledger as Arc<dyn ProviderInstanceLedger>,
        ProviderKind("fake".to_string()),
        InstanceManagerConfig::new(10, 10),
    );

    let error = expect_err(start(&manager, "r1", "owner-a").await);
    assert!(matches!(error, ProviderControlError::Transport { .. }));
    assert_eq!(
        manager.active_count("owner-a"),
        0,
        "quota reservation must be rolled back"
    );
    assert_eq!(
        lifecycle.delete_calls.load(Ordering::SeqCst),
        1,
        "the instance create() succeeded on must be compensating-deleted"
    );
    assert!(manager.backend_for("r1").is_err());
}

#[tokio::test]
async fn ledger_write_failure_triggers_compensating_delete_and_quota_rollback() {
    let lifecycle = Arc::new(FakeLifecycle::new());
    let attach = Arc::new(FakeAttach::new());
    let ledger = Arc::new(FakeLedger::default());
    ledger.fail_record_created.store(true, Ordering::SeqCst);
    let manager = InstanceManager::new(
        lifecycle.clone() as Arc<dyn ProviderLifecycle>,
        attach as Arc<dyn OperationAttach>,
        ledger as Arc<dyn ProviderInstanceLedger>,
        ProviderKind("fake".to_string()),
        InstanceManagerConfig::new(10, 10),
    );

    let error = expect_err(start(&manager, "r1", "owner-a").await);
    assert!(matches!(error, ProviderControlError::Transport { .. }));
    assert_eq!(manager.active_count("owner-a"), 0);
    assert_eq!(lifecycle.delete_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn create_retries_a_transient_error_then_succeeds() {
    let (manager, lifecycle, _ledger) = manager_with(10, 10);
    lifecycle.fail_create_remaining.store(2, Ordering::SeqCst);

    let backend = start(&manager, "r1", "owner-a").await.unwrap();
    assert_eq!(backend.backend_id(), "fake-0");
    assert_eq!(lifecycle.create_calls.load(Ordering::SeqCst), 3);
    assert_eq!(manager.active_count("owner-a"), 1);
}

#[tokio::test]
async fn create_exhausts_retries_and_rolls_back_quota() {
    let (manager, lifecycle, _ledger) = manager_with(10, 10);
    lifecycle.fail_create_remaining.store(100, Ordering::SeqCst);

    let error = expect_err(start(&manager, "r1", "owner-a").await);
    assert!(matches!(error, ProviderControlError::Transport { .. }));
    // Default RetryPolicy::max_attempts is 3.
    assert_eq!(lifecycle.create_calls.load(Ordering::SeqCst), 3);
    assert_eq!(manager.active_count("owner-a"), 0);
}

#[tokio::test]
async fn create_does_not_retry_non_retryable_errors() {
    let (manager, lifecycle, _ledger) = manager_with(10, 10);
    lifecycle
        .fail_create_permanently
        .store(true, Ordering::SeqCst);

    let error = expect_err(start(&manager, "r1", "owner-a").await);
    assert!(matches!(error, ProviderControlError::InvalidRequest { .. }));
    assert_eq!(
        lifecycle.create_calls.load(Ordering::SeqCst),
        1,
        "a non-retryable error must not be retried even though max_attempts > 1"
    );
}

// `start_paused = true` + explicit `time::advance` makes the backoff
// wait deterministic instead of racing a real 200ms sleep against
// however long test setup happens to take.
#[tokio::test(start_paused = true)]
async fn delete_failure_is_queued_and_a_manual_retry_sweep_eventually_cleans_up() {
    let (manager, lifecycle, ledger) = manager_with(10, 10);
    start(&manager, "r1", "owner-a").await.unwrap();

    lifecycle.fail_delete_remaining.store(1, Ordering::SeqCst);
    let error = expect_err(manager.stop_instance("r1").await);
    assert!(matches!(error, ProviderControlError::Transport { .. }));
    assert_eq!(manager.pending_release_count(), 1);
    // Registry entry and quota reservation both survive a failed delete.
    assert_eq!(manager.active_count("owner-a"), 1);
    assert!(manager.backend_for("r1").is_ok());
    assert_eq!(ledger.rows.lock().unwrap().len(), 1);

    // fail_delete_remaining is now 0 — the next attempt (once its
    // backoff has actually elapsed) succeeds.
    tokio::time::advance(Duration::from_millis(250)).await;
    manager.retry_pending_releases_once().await;

    assert_eq!(manager.pending_release_count(), 0);
    assert_eq!(manager.active_count("owner-a"), 0);
    assert!(manager.backend_for("r1").is_err());
    assert_eq!(ledger.rows.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn retry_sweep_before_backoff_elapses_is_a_noop() {
    let (manager, lifecycle, _ledger) = manager_with(10, 10);
    start(&manager, "r1", "owner-a").await.unwrap();

    lifecycle.fail_delete_remaining.store(100, Ordering::SeqCst);
    manager.stop_instance("r1").await.unwrap_err();
    assert_eq!(manager.pending_release_count(), 1);

    // Backoff has not elapsed yet (base_delay defaults to 200ms) — an
    // immediate sweep must leave the entry queued, not spin it away.
    manager.retry_pending_releases_once().await;
    assert_eq!(
        manager.pending_release_count(),
        1,
        "an immediate re-sweep before backoff elapses must not drop the entry"
    );
}

#[tokio::test]
async fn reconcile_rehydrates_registry_and_quota_from_the_ledger() {
    let lifecycle = Arc::new(FakeLifecycle::new());
    let attach = Arc::new(FakeAttach::new());
    let ledger = Arc::new(FakeLedger::default());

    // Simulate state that survived a restart: the provider still
    // reports the instance live, and the ledger still has the row, but
    // this fresh InstanceManager's in-memory registry/quota knows
    // nothing about it yet.
    let instance = lifecycle.instance("fake-restart-survivor".to_string());
    lifecycle
        .live_instances
        .lock()
        .unwrap()
        .push(instance.clone());
    ledger.rows.lock().unwrap().insert(
        "runtime-restart-1".to_string(),
        ActiveLedgerEntry {
            runtime_id: "runtime-restart-1".to_string(),
            owner_ref: "owner-a".to_string(),
            instance,
        },
    );

    let manager = InstanceManager::new(
        lifecycle as Arc<dyn ProviderLifecycle>,
        attach as Arc<dyn OperationAttach>,
        ledger as Arc<dyn ProviderInstanceLedger>,
        ProviderKind("fake".to_string()),
        InstanceManagerConfig::new(10, 10),
    );

    assert_eq!(manager.active_count("owner-a"), 0);
    let outcome = manager.reconcile().await.unwrap();
    assert_eq!(outcome.confirmed.len(), 1);
    assert!(outcome.orphaned.is_empty());
    assert_eq!(manager.active_count("owner-a"), 1);
    assert_eq!(manager.global_active_count(), 1);
    assert!(manager.backend_for("runtime-restart-1").is_ok());
}

/// Regression test for the real E2E finding (`docs/pi_session_restore_
/// plan.md` §1.4, F11 leak): a `runtime_id` whose in-memory registry
/// entry is gone (simulating a fresh post-restart manager that has not
/// run `reconcile`/`attach`) but whose ledger row is still `Active`
/// must still get its provider instance destroyed — `destroy_by_
/// runtime_id` must fall back to a direct ledger-driven delete rather
/// than reporting `NotFound` and leaving the sandbox running.
#[tokio::test]
async fn destroy_by_runtime_id_falls_back_to_a_ledger_driven_delete_when_the_registry_is_empty()
{
    let lifecycle = Arc::new(FakeLifecycle::new());
    let attach = Arc::new(FakeAttach::new());
    let ledger = Arc::new(FakeLedger::default());

    let instance = lifecycle.instance("fake-post-restart".to_string());
    lifecycle
        .live_instances
        .lock()
        .unwrap()
        .push(instance.clone());
    ledger.rows.lock().unwrap().insert(
        "runtime-post-restart-1".to_string(),
        ActiveLedgerEntry {
            runtime_id: "runtime-post-restart-1".to_string(),
            owner_ref: "owner-a".to_string(),
            instance,
        },
    );

    let manager = InstanceManager::new(
        lifecycle.clone() as Arc<dyn ProviderLifecycle>,
        attach as Arc<dyn OperationAttach>,
        ledger.clone() as Arc<dyn ProviderInstanceLedger>,
        ProviderKind("fake".to_string()),
        InstanceManagerConfig::new(10, 10),
    );

    // Deliberately no `reconcile()` call here: the in-memory registry
    // knows nothing about "runtime-post-restart-1", exactly like a
    // freshly-started daemon that closes a session before anything else
    // has touched it.
    assert!(manager.backend_for("runtime-post-restart-1").is_err());

    manager
        .destroy_by_runtime_id("runtime-post-restart-1")
        .await
        .expect("ledger-driven fallback delete must succeed");

    assert_eq!(
        lifecycle.delete_calls.load(Ordering::SeqCst),
        1,
        "the provider instance must actually be deleted, not silently skipped"
    );
    assert!(
        ledger
            .rows
            .lock()
            .unwrap()
            .get("runtime-post-restart-1")
            .is_none()
            || !lifecycle
                .live_instances
                .lock()
                .unwrap()
                .iter()
                .any(|i| i.instance_id.0 == "fake-post-restart"),
        "the underlying provider instance must no longer be live"
    );

    // A second call (nothing left, ledger row now soft-deleted) is still
    // a clean no-op success, not an error.
    manager
        .destroy_by_runtime_id("runtime-post-restart-1")
        .await
        .expect("destroying an already-gone runtime_id must be a no-op success");
}

#[tokio::test]
async fn reconcile_orphans_a_ledger_row_the_provider_no_longer_reports() {
    let lifecycle = Arc::new(FakeLifecycle::new());
    let attach = Arc::new(FakeAttach::new());
    let ledger = Arc::new(FakeLedger::default());

    // Ledger believes it's active, but re-attaching disagrees — e.g. it
    // was deleted out-of-band. `attach()` is now the sole liveness
    // oracle reconcile relies on (see reconcile's doc comment), so this
    // is modeled by making `FakeAttach` fail, not by leaving
    // `lifecycle`'s `list_instances()` empty (which no longer has any
    // bearing on reconcile's outcome).
    attach.fail.store(true, Ordering::SeqCst);
    ledger.rows.lock().unwrap().insert(
        "runtime-orphan-1".to_string(),
        ActiveLedgerEntry {
            runtime_id: "runtime-orphan-1".to_string(),
            owner_ref: "owner-a".to_string(),
            instance: lifecycle.instance("fake-gone".to_string()),
        },
    );

    let manager = InstanceManager::new(
        lifecycle as Arc<dyn ProviderLifecycle>,
        attach as Arc<dyn OperationAttach>,
        ledger.clone() as Arc<dyn ProviderInstanceLedger>,
        ProviderKind("fake".to_string()),
        InstanceManagerConfig::new(10, 10),
    );

    let outcome = manager.reconcile().await.unwrap();
    assert!(outcome.confirmed.is_empty());
    assert_eq!(outcome.orphaned.len(), 1);
    assert_eq!(manager.active_count("owner-a"), 0);
    assert!(
        ledger.rows.lock().unwrap().is_empty(),
        "orphan must be soft-deleted from the ledger"
    );
}

/// End-to-end confidence check against a real provider (not a fake),
/// mirroring `backend::binding::ProviderBoundRuntime`'s original test of
/// the same name/shape — proves the new crate's wiring still drives a
/// real local sandbox exec, not just the fakes above.
#[tokio::test]
async fn start_instance_then_backend_for_returns_a_working_backend_against_a_real_provider() {
    let workspace = std::env::temp_dir().join(format!(
        "xgovernor-manager-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&workspace).unwrap();

    let local = Arc::new(LocalProvider::new());
    let lifecycle: Arc<dyn ProviderLifecycle> = local.clone();
    let attach: Arc<dyn OperationAttach> = local;
    let ledger: Arc<dyn ProviderInstanceLedger> = Arc::new(
        backend::SqliteProviderInstanceLedger::open_in_memory().expect("open in-memory ledger"),
    );
    let manager = InstanceManager::new(
        lifecycle,
        attach,
        ledger,
        ProviderKind("local".to_string()),
        InstanceManagerConfig::new(4, 100),
    );

    let backend = manager
        .start_instance(
            "runtime-1".to_string(),
            BackendId("local".to_string()),
            "test-owner".to_string(),
            serde_json::json!({ "workspace_root": workspace.to_string_lossy() }),
        )
        .await
        .expect("start_instance should succeed");

    let result = backend
        .exec()
        .exec(ExecRequest {
            command: "echo".to_string(),
            args: vec!["hello".to_string()],
            shell: None,
            cwd: None,
            timeout_ms: Some(5_000),
            env: None,
        })
        .await
        .expect("exec should succeed through the attached backend");
    assert_eq!(result.exit_code, Some(0));

    manager
        .stop_instance("runtime-1")
        .await
        .expect("stop_instance should succeed");

    let _ = std::fs::remove_dir_all(&workspace);
}

/// Real E2E regression test for Fix B's local half (Finding 1, root
/// cause B): a `LocalProvider` started, persisted to a real (file-
/// backed, not in-memory) SQLite ledger, then "restarted" by
/// constructing a brand-new `LocalProvider` (empty in-memory registry,
/// exactly what happens on every daemon restart) plus a brand-new
/// `InstanceManager` pointed at the same ledger file. Before Fix B,
/// `reconcile()` on the restarted manager would have orphaned this row
/// unconditionally — first because `list_instances()` on a fresh
/// `LocalProvider` is always empty (the old gate this test would have
/// tripped), and second because `LocalProvider::attach()` was a
/// registry-only lookup that could never succeed cold. This proves both
/// halves of the fix together: `reconcile()` no longer gates on
/// `list_instances()`, and `attach()` can rebuild a working backend from
/// `provider_options` persisted on `ProviderInstance.metadata` alone.
#[tokio::test]
async fn reconcile_after_restart_rebuilds_a_local_instance_from_the_ledger_alone() {
    let workspace = std::env::temp_dir().join(format!(
        "xgovernor-manager-restart-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&workspace).unwrap();
    let ledger_path = std::env::temp_dir().join(format!(
        "xgovernor-manager-restart-test-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    {
        // "Before restart": start an instance and let it persist to the
        // real ledger file, then drop everything (provider included) —
        // nothing here survives into the next block except the ledger
        // file on disk and the workspace directory.
        let local = Arc::new(LocalProvider::new());
        let lifecycle: Arc<dyn ProviderLifecycle> = local.clone();
        let attach: Arc<dyn OperationAttach> = local;
        let ledger: Arc<dyn ProviderInstanceLedger> = Arc::new(
            backend::SqliteProviderInstanceLedger::open(&ledger_path)
                .expect("open file-backed ledger"),
        );
        let manager = InstanceManager::new(
            lifecycle,
            attach,
            ledger,
            ProviderKind("local".to_string()),
            InstanceManagerConfig::new(4, 100),
        );

        manager
            .start_instance(
                "runtime-restart-e2e".to_string(),
                BackendId("local".to_string()),
                "test-owner".to_string(),
                serde_json::json!({ "workspace_root": workspace.to_string_lossy() }),
            )
            .await
            .expect("start_instance should succeed");
    }

    // "After restart": brand-new provider (empty registry), brand-new
    // manager, same ledger file.
    let restarted_local = Arc::new(LocalProvider::new());
    let restarted_lifecycle: Arc<dyn ProviderLifecycle> = restarted_local.clone();
    let restarted_attach: Arc<dyn OperationAttach> = restarted_local;
    let restarted_ledger: Arc<dyn ProviderInstanceLedger> = Arc::new(
        backend::SqliteProviderInstanceLedger::open(&ledger_path)
            .expect("reopen file-backed ledger"),
    );
    let restarted_manager = InstanceManager::new(
        restarted_lifecycle,
        restarted_attach,
        restarted_ledger,
        ProviderKind("local".to_string()),
        InstanceManagerConfig::new(4, 100),
    );

    let outcome = restarted_manager
        .reconcile()
        .await
        .expect("reconcile should succeed");
    assert_eq!(
        outcome.confirmed.len(),
        1,
        "the ledger row must be confirmed live, not orphaned, after restart"
    );
    assert!(outcome.orphaned.is_empty());

    let backend = restarted_manager
        .backend_for("runtime-restart-e2e")
        .expect("backend_for should find the reconciled instance");
    let result = backend
        .exec()
        .exec(ExecRequest {
            command: "echo".to_string(),
            args: vec!["reattached-after-restart".to_string()],
            shell: None,
            cwd: None,
            timeout_ms: Some(5_000),
            env: None,
        })
        .await
        .expect("exec through the reattached backend should succeed");
    assert_eq!(result.exit_code, Some(0));
    assert!(String::from_utf8_lossy(&result.stdout).contains("reattached-after-restart"));

    let _ = std::fs::remove_dir_all(&workspace);
    let _ = std::fs::remove_file(&ledger_path);
}
