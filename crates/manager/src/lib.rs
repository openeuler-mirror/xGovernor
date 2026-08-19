use backend::{ActiveLedgerEntry, OperationAttach, ProviderInstanceLedger};
use operation_protocol::OperationBackend;
use provider_protocol::{
    BackendId, ProviderCapability, ProviderControlError, ProviderCreateRequest,
    ProviderDeleteRequest, ProviderInstance, ProviderKind, ProviderLifecycle,
    ProviderLifecycleReason, ProviderLoadRequest, ProviderLoadSource, ProviderResourceLimits,
    ProviderSnapshot,
};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::Instant;

/// Sentinel `owner_ref` exempt from both `max_sandboxes_per_owner` and
/// `max_sandboxes_global` (`docs/tenancy_design.md` §7 step 4: admin is
/// "百无禁忌"). Must stay in sync with `xgovernor_core::security::
/// ADMIN_OWNER_REF` and `backend::quota`'s (now-deleted) copy of the same
/// literal — duplicated here rather than adding a dependency edge from this
/// crate to `xgovernor-core` purely to share one constant, following the
/// precedent already set for this exact constant.
pub const ADMIN_OWNER_REF: &str = "admin";

const DEFAULT_MAX_CONCURRENT_CREATES: usize = 16;

/// Backoff policy shared by the create-path retry
/// ([`InstanceManager::create_with_retry`]) and the pending-release retry
/// loop ([`InstanceManager::spawn_retry_loop`]) — same shape, different
/// give-up semantics: the create path gives up after `max_attempts` and
/// surfaces the error to the caller (an in-flight `start_instance` request
/// cannot wait forever); the pending-release queue ignores `max_attempts`
/// entirely and retries indefinitely (giving up there would permanently
/// leak a provider instance, which is strictly worse than an unbounded but
/// capped-backoff retry).
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, Clone)]
pub struct InstanceManagerConfig {
    pub max_sandboxes_per_owner: usize,
    pub max_sandboxes_global: usize,
    pub max_concurrent_creates: usize,
    pub retry: RetryPolicy,
}

impl InstanceManagerConfig {
    /// Owner/global caps are required inputs (no sensible universal
    /// default); admission concurrency and retry policy get house defaults,
    /// overridable via the struct's public fields.
    pub fn new(max_sandboxes_per_owner: usize, max_sandboxes_global: usize) -> Self {
        Self {
            max_sandboxes_per_owner,
            max_sandboxes_global,
            max_concurrent_creates: DEFAULT_MAX_CONCURRENT_CREATES,
            retry: RetryPolicy::default(),
        }
    }
}

/// Classifies which `ProviderControlError` variants are worth retrying.
/// `Transport`/`Timeout` are infrastructure blips; `ProviderFailure` is the
/// provider's own "something went wrong on my end" bucket — all three are
/// plausibly transient. Everything else (`InvalidRequest`, `NotFound`,
/// `Conflict`, `UnsupportedCapability`, `InvalidState`,
/// `ResourceLimitExceeded`) describes a request that will fail identically
/// on retry, so retrying it would only add latency for a guaranteed-same
/// outcome.
pub fn is_retryable(error: &ProviderControlError) -> bool {
    matches!(
        error,
        ProviderControlError::Transport { .. }
            | ProviderControlError::Timeout { .. }
            | ProviderControlError::ProviderFailure { .. }
    )
}

/// Same shape as the `ReconcileOutcome` this crate's `reconcile` replaces
/// (formerly on `backend::binding::ProviderBoundRuntime`), redefined here
/// since that type is deleted alongside the file it lived in.
#[derive(Debug)]
pub struct ReconcileOutcome {
    pub confirmed: Vec<ActiveLedgerEntry>,
    pub orphaned: Vec<ActiveLedgerEntry>,
}

#[derive(Clone)]
struct BoundInstance {
    instance: ProviderInstance,
    backend: Arc<dyn OperationBackend>,
}

#[derive(Default)]
struct QuotaState {
    /// owner_ref -> count of instances currently tracked for that owner.
    counts: HashMap<String, usize>,
    /// instance_id -> owner_ref, so a `delete()` (which carries no
    /// owner_ref of its own) can find whose quota to release.
    owners_by_instance: HashMap<String, String>,
    /// Total instances tracked across every owner — the global cap's
    /// counter. Kept in the same lock as `counts` so a `reserve()` check
    /// against both caps is a single atomic check-and-increment.
    total: usize,
}

struct PendingRelease {
    runtime_id: String,
    record: BoundInstance,
    attempt: u32,
    next_retry_at: Instant,
}

/// The unified orchestration component `docs/protocol_boundaries.md`
/// describes as "manager" — see the module doc for what it absorbs and adds.
pub struct InstanceManager {
    lifecycle: Arc<dyn ProviderLifecycle>,
    attach: Arc<dyn OperationAttach>,
    ledger: Arc<dyn ProviderInstanceLedger>,
    kind: ProviderKind,
    config: InstanceManagerConfig,
    instances: Mutex<HashMap<String, BoundInstance>>,
    quota: Mutex<QuotaState>,
    /// Striped per-`runtime_id` locks. Lazily created, opportunistically
    /// removed once unreferenced (see `release_runtime_lock`) so this map
    /// does not grow without bound across a long-running daemon's lifetime
    /// — `runtime_id` is a fresh UUID per session (`docs/tenancy_design.md`
    /// §3.3), so without cleanup this would be an unbounded leak, not just
    /// untidy bookkeeping.
    runtime_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    admission: Semaphore,
    pending_release: Mutex<VecDeque<PendingRelease>>,
}

impl InstanceManager {
    pub fn new(
        lifecycle: Arc<dyn ProviderLifecycle>,
        attach: Arc<dyn OperationAttach>,
        ledger: Arc<dyn ProviderInstanceLedger>,
        kind: ProviderKind,
        config: InstanceManagerConfig,
    ) -> Self {
        let admission = Semaphore::new(config.max_concurrent_creates);
        Self {
            lifecycle,
            attach,
            ledger,
            kind,
            config,
            instances: Mutex::new(HashMap::new()),
            quota: Mutex::new(QuotaState::default()),
            runtime_locks: Mutex::new(HashMap::new()),
            admission,
            pending_release: Mutex::new(VecDeque::new()),
        }
    }

    pub fn max_sandboxes_per_owner(&self) -> usize {
        self.config.max_sandboxes_per_owner
    }

    pub fn max_sandboxes_global(&self) -> usize {
        self.config.max_sandboxes_global
    }

    /// Number of instances currently counted against `owner_ref`. Exposed
    /// mainly for tests and diagnostics.
    pub fn active_count(&self, owner_ref: &str) -> usize {
        self.quota_state()
            .map(|state| state.counts.get(owner_ref).copied().unwrap_or(0))
            .unwrap_or(0)
    }

    /// Total instances currently tracked across every owner (the global cap's
    /// live counter). Exposed mainly for tests and diagnostics.
    pub fn global_active_count(&self) -> usize {
        self.quota_state().map(|state| state.total).unwrap_or(0)
    }

    /// Number of provider instances currently awaiting a background delete
    /// retry (see [`Self::spawn_retry_loop`]). Exposed for tests and
    /// diagnostics — a persistently nonzero count is a signal worth
    /// alerting on in a real deployment (the underlying provider is failing
    /// deletes repeatedly).
    pub fn pending_release_count(&self) -> usize {
        self.pending_release.lock().unwrap().len()
    }

    fn quota_state(&self) -> Result<std::sync::MutexGuard<'_, QuotaState>, ProviderControlError> {
        self.quota
            .lock()
            .map_err(|_| ProviderControlError::Transport {
                message: "instance manager quota lock poisoned".to_string(),
            })
    }

    fn get_runtime_lock(&self, runtime_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.runtime_locks
            .lock()
            .expect("InstanceManager runtime_locks lock poisoned")
            .entry(runtime_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Drops `lock` (the caller's own clone) and, if nothing else is holding
    /// a reference to the map's clone afterward, removes the map entry.
    /// Racing with another caller that grabbed a clone in between just skips
    /// the removal this time — the entry gets cleaned up on a later call
    /// instead, never a correctness issue, only a (bounded, self-correcting)
    /// delay in reclaiming memory.
    fn release_runtime_lock(&self, runtime_id: &str, lock: Arc<tokio::sync::Mutex<()>>) {
        drop(lock);
        let mut locks = self
            .runtime_locks
            .lock()
            .expect("InstanceManager runtime_locks lock poisoned");
        if let Some(existing) = locks.get(runtime_id) {
            if Arc::strong_count(existing) == 1 {
                locks.remove(runtime_id);
            }
        }
    }

    /// Reserve a slot for `owner_ref` if under both the per-owner and global
    /// caps. `owner_ref == ADMIN_OWNER_REF` bypasses both checks entirely
    /// (still counted, for diagnostics, but never rejected).
    ///
    /// A global-cap rejection reuses `ProviderControlError::
    /// ResourceLimitExceeded` (no new error variant) rather than touching
    /// `provider-protocol` for a single extra field — `current`/`max` in
    /// that case describe the *global* count, and `owner_ref` carries a
    /// `" (global cap)"` suffix so the message stays distinguishable from an
    /// owner-cap rejection without widening the protocol crate's error
    /// surface for this one crate's benefit. See
    /// `docs/protocol_boundaries.md`'s updated architecture note for this
    /// tradeoff spelled out explicitly.
    fn reserve(&self, owner_ref: &str) -> Result<(), ProviderControlError> {
        let mut state = self.quota_state()?;
        let current = state.counts.get(owner_ref).copied().unwrap_or(0);
        if owner_ref != ADMIN_OWNER_REF {
            if current >= self.config.max_sandboxes_per_owner {
                return Err(ProviderControlError::ResourceLimitExceeded {
                    provider: self.kind.clone(),
                    owner_ref: owner_ref.to_string(),
                    current,
                    max: self.config.max_sandboxes_per_owner,
                });
            }
            if state.total >= self.config.max_sandboxes_global {
                return Err(ProviderControlError::ResourceLimitExceeded {
                    provider: self.kind.clone(),
                    owner_ref: format!("{owner_ref} (global cap)"),
                    current: state.total,
                    max: self.config.max_sandboxes_global,
                });
            }
        }
        state.counts.insert(owner_ref.to_string(), current + 1);
        state.total += 1;
        Ok(())
    }

    /// Undo a `reserve()` whose subsequent step failed.
    fn rollback_reservation(&self, owner_ref: &str) {
        if let Ok(mut state) = self.quota_state() {
            decrement(&mut state.counts, owner_ref);
            state.total = state.total.saturating_sub(1);
        }
    }

    fn track_instance(&self, instance_id: &str, owner_ref: &str) {
        if let Ok(mut state) = self.quota_state() {
            state
                .owners_by_instance
                .insert(instance_id.to_string(), owner_ref.to_string());
        }
    }

    /// Release the slot held by `instance_id`, if any. No-op if the
    /// instance was never tracked.
    fn release_instance(&self, instance_id: &str) {
        if let Ok(mut state) = self.quota_state() {
            if let Some(owner_ref) = state.owners_by_instance.remove(instance_id) {
                decrement(&mut state.counts, owner_ref.as_str());
                state.total = state.total.saturating_sub(1);
            }
        }
    }

    /// Combined counts-bump + tracking used by `reconcile` — unlike
    /// `reserve()` this does not cap-check (a restart rehydrating already-live
    /// instances must never be rejected by the very cap that governs new
    /// admissions).
    fn rehydrate_one(&self, instance_id: &str, owner_ref: &str) {
        if let Ok(mut state) = self.quota_state() {
            *state.counts.entry(owner_ref.to_string()).or_insert(0) += 1;
            state.total += 1;
            state
                .owners_by_instance
                .insert(instance_id.to_string(), owner_ref.to_string());
        }
    }

    async fn rollback_create(&self, instance: &ProviderInstance) {
        let outcome = self
            .lifecycle
            .delete(ProviderDeleteRequest {
                backend_id: instance.backend_id.clone(),
                instance_id: Some(instance.instance_id.clone()),
                snapshot_id: None,
                reason: ProviderLifecycleReason::ErrorCleanup,
                correlation: Value::Null,
            })
            .await;
        if let Err(error) = outcome {
            tracing::warn!(
                target: "instance_manager",
                instance_id = %instance.instance_id,
                %error,
                "rollback delete after a failed start_instance step also failed; instance may be leaked"
            );
        }
    }

    async fn create_with_retry(
        &self,
        backend_id: BackendId,
        owner_ref: String,
        provider_options: Value,
    ) -> Result<ProviderInstance, ProviderControlError> {
        let mut attempt = 0u32;
        let mut delay = self.config.retry.base_delay;
        loop {
            attempt += 1;
            let permit = self
                .admission
                .acquire()
                .await
                .expect("admission semaphore is never closed");
            let outcome = self
                .lifecycle
                .create(ProviderCreateRequest {
                    backend_id: backend_id.clone(),
                    owner_ref: owner_ref.clone(),
                    reason: ProviderLifecycleReason::Acquire,
                    resource_limits: ProviderResourceLimits::default(),
                    provider_options: provider_options.clone(),
                    correlation: Value::Null,
                })
                .await;
            drop(permit);

            match outcome {
                Ok(instance) => return Ok(instance),
                Err(error) => {
                    if attempt >= self.config.retry.max_attempts || !is_retryable(&error) {
                        return Err(error);
                    }
                    tracing::warn!(
                        target: "instance_manager",
                        attempt,
                        %error,
                        "create() failed with a retryable error; backing off before retry"
                    );
                    tokio::time::sleep(delay).await;
                    delay = std::cmp::min(delay * 2, self.config.retry.max_delay);
                }
            }
        }
    }

    /// Create a provider instance for `runtime_id`, attach its operation
    /// backend, durably record it in the ledger, and register it in-memory.
    ///
    /// Concurrent calls for the *same* `runtime_id` are serialized by a
    /// per-`runtime_id` lock held for this call's full duration. Once the
    /// lock is acquired, if `runtime_id` is already registered (i.e. a
    /// concurrent or duplicate call already won), this returns
    /// `ProviderControlError::Conflict` immediately rather than blocking
    /// further or silently overwriting the winner's registry entry —
    /// `backend::binding::ProviderBoundRuntime`'s module doc had explicitly
    /// posed "reject the second call? queue it?" as an open design question;
    /// reject is the answer, because a `runtime_id` is minted fresh per
    /// session (never reused for two different logical sessions), so a
    /// second concurrent `start_instance` for the same id is definitionally
    /// a bug in the caller, not a legitimate retry to coalesce.
    pub async fn start_instance(
        &self,
        runtime_id: String,
        backend_id: BackendId,
        owner_ref: String,
        provider_options: Value,
    ) -> Result<Arc<dyn OperationBackend>, ProviderControlError> {
        let lock = self.get_runtime_lock(&runtime_id);
        let guard = lock.clone().lock_owned().await;
        let result = self
            .start_instance_locked(&runtime_id, backend_id, owner_ref, provider_options)
            .await;
        drop(guard);
        self.release_runtime_lock(&runtime_id, lock);
        result
    }

    pub async fn checkpoint_instance(
        &self,
        runtime_id: &str,
    ) -> Result<ProviderSnapshot, ProviderControlError> {
        let record = self
            .instances
            .lock()
            .expect("InstanceManager registry lock poisoned")
            .get(runtime_id)
            .cloned()
            .ok_or_else(|| ProviderControlError::NotFound {
                resource_ref: runtime_id.to_string(),
            })?;
        if !record
            .instance
            .capabilities
            .lifecycle
            .contains(&ProviderCapability::Snapshot)
        {
            return Err(ProviderControlError::UnsupportedCapability {
                provider: self.kind.clone(),
                capability: "checkpoint".to_string(),
            });
        }
        self.lifecycle
            .checkpoint(provider_protocol::ProviderCheckpointRequest {
                backend_id: record.instance.backend_id.clone(),
                instance_id: record.instance.instance_id.clone(),
                reason: ProviderLifecycleReason::UserRequested,
                correlation: Value::Null,
            })
            .await
    }

    pub async fn delete_snapshot(
        &self,
        backend_id: BackendId,
        snapshot_id: provider_protocol::ProviderSnapshotId,
    ) -> Result<(), ProviderControlError> {
        self.lifecycle
            .delete(ProviderDeleteRequest {
                backend_id,
                instance_id: None,
                snapshot_id: Some(snapshot_id),
                reason: ProviderLifecycleReason::UserRequested,
                correlation: Value::Null,
            })
            .await
            .map(|_| ())
    }

    pub async fn load_instance_from_snapshot(
        &self,
        runtime_id: String,
        backend_id: BackendId,
        owner_ref: String,
        snapshot_id: provider_protocol::ProviderSnapshotId,
        provider_options: Value,
    ) -> Result<Arc<dyn OperationBackend>, ProviderControlError> {
        let lock = self.get_runtime_lock(&runtime_id);
        let guard = lock.clone().lock_owned().await;
        let result = self
            .load_instance_from_snapshot_locked(
                &runtime_id,
                backend_id,
                owner_ref,
                snapshot_id,
                provider_options,
            )
            .await;
        drop(guard);
        self.release_runtime_lock(&runtime_id, lock);
        result
    }

    async fn load_instance_from_snapshot_locked(
        &self,
        runtime_id: &str,
        backend_id: BackendId,
        owner_ref: String,
        snapshot_id: provider_protocol::ProviderSnapshotId,
        provider_options: Value,
    ) -> Result<Arc<dyn OperationBackend>, ProviderControlError> {
        if self
            .instances
            .lock()
            .expect("InstanceManager registry lock poisoned")
            .contains_key(runtime_id)
        {
            return Err(ProviderControlError::Conflict {
                message: format!(
                    "runtime_id '{runtime_id}' already has an active provider instance"
                ),
            });
        }
        self.reserve(&owner_ref)?;
        let instance = match self
            .lifecycle
            .load(ProviderLoadRequest {
                backend_id: backend_id.clone(),
                owner_ref: owner_ref.clone(),
                source: ProviderLoadSource::Snapshot(snapshot_id),
                reason: ProviderLifecycleReason::Restore,
                resource_limits: ProviderResourceLimits::default(),
                provider_options,
                correlation: Value::Null,
            })
            .await
        {
            Ok(v) => v,
            Err(e) => {
                self.rollback_reservation(&owner_ref);
                return Err(e);
            }
        };
        let backend = match self.attach.attach(&instance).await {
            Ok(v) => v,
            Err(e) => {
                self.rollback_create(&instance).await;
                self.rollback_reservation(&owner_ref);
                return Err(e);
            }
        };
        if let Err(e) = self
            .ledger
            .record_created(runtime_id, &owner_ref, &instance)
            .await
        {
            self.rollback_create(&instance).await;
            self.rollback_reservation(&owner_ref);
            return Err(e);
        }
        self.track_instance(instance.instance_id.0.as_str(), &owner_ref);
        self.instances
            .lock()
            .expect("InstanceManager registry lock poisoned")
            .insert(
                runtime_id.to_string(),
                BoundInstance {
                    instance,
                    backend: backend.clone(),
                },
            );
        Ok(backend)
    }

    async fn start_instance_locked(
        &self,
        runtime_id: &str,
        backend_id: BackendId,
        owner_ref: String,
        provider_options: Value,
    ) -> Result<Arc<dyn OperationBackend>, ProviderControlError> {
        if self
            .instances
            .lock()
            .expect("InstanceManager registry lock poisoned")
            .contains_key(runtime_id)
        {
            return Err(ProviderControlError::Conflict {
                message: format!(
                    "runtime_id '{runtime_id}' already has an active provider instance \
                     (concurrent or duplicate start_instance rejected)"
                ),
            });
        }

        self.reserve(&owner_ref)?;

        let instance = match self
            .create_with_retry(backend_id, owner_ref.clone(), provider_options)
            .await
        {
            Ok(instance) => instance,
            Err(error) => {
                self.rollback_reservation(&owner_ref);
                return Err(error);
            }
        };

        let backend = match self.attach.attach(&instance).await {
            Ok(backend) => backend,
            Err(error) => {
                self.rollback_create(&instance).await;
                self.rollback_reservation(&owner_ref);
                return Err(error);
            }
        };

        if let Err(error) = self
            .ledger
            .record_created(runtime_id, &owner_ref, &instance)
            .await
        {
            self.rollback_create(&instance).await;
            self.rollback_reservation(&owner_ref);
            return Err(error);
        }

        self.track_instance(instance.instance_id.0.as_str(), &owner_ref);
        self.instances
            .lock()
            .expect("InstanceManager registry lock poisoned")
            .insert(
                runtime_id.to_string(),
                BoundInstance {
                    instance,
                    backend: backend.clone(),
                },
            );

        Ok(backend)
    }

    /// Delete the provider instance registered for `runtime_id`, record the
    /// deletion in the ledger, release its quota slot, and drop it from the
    /// registry. `Err(ProviderControlError::NotFound)` if `runtime_id` was
    /// never started or was already stopped.
    ///
    /// If the underlying `delete()` call fails, the registry entry and
    /// quota reservation are both left in place (so a caller-initiated retry
    /// can still find and retry it) *and* the instance is enqueued for
    /// automatic background retry — see [`Self::spawn_retry_loop`]. Either
    /// path can win the race to clean it up; both are serialized against
    /// each other by the same per-`runtime_id` lock `start_instance` uses.
    pub async fn stop_instance(&self, runtime_id: &str) -> Result<(), ProviderControlError> {
        let lock = self.get_runtime_lock(runtime_id);
        let guard = lock.clone().lock_owned().await;
        let result = self.stop_instance_locked(runtime_id).await;
        drop(guard);
        self.release_runtime_lock(runtime_id, lock);
        result
    }

    async fn stop_instance_locked(&self, runtime_id: &str) -> Result<(), ProviderControlError> {
        let record = self
            .instances
            .lock()
            .expect("InstanceManager registry lock poisoned")
            .get(runtime_id)
            .cloned()
            .ok_or_else(|| ProviderControlError::NotFound {
                resource_ref: runtime_id.to_string(),
            })?;

        match self.delete_instance(&record).await {
            Ok(()) => {
                self.finish_release(runtime_id, &record).await;
                Ok(())
            }
            Err(error) => {
                tracing::warn!(
                    target: "instance_manager",
                    runtime_id,
                    %error,
                    "stop_instance: delete failed; queued for background pending-release retry \
                     (registry entry retained for a possible caller-initiated retry too)"
                );
                self.pending_release
                    .lock()
                    .unwrap()
                    .push_back(PendingRelease {
                        runtime_id: runtime_id.to_string(),
                        record,
                        attempt: 0,
                        next_retry_at: Instant::now() + self.config.retry.base_delay,
                    });
                Err(error)
            }
        }
    }

    async fn delete_instance(&self, record: &BoundInstance) -> Result<(), ProviderControlError> {
        self.lifecycle
            .delete(ProviderDeleteRequest {
                backend_id: record.instance.backend_id.clone(),
                instance_id: Some(record.instance.instance_id.clone()),
                snapshot_id: None,
                reason: ProviderLifecycleReason::Release,
                correlation: Value::Null,
            })
            .await
            .map(|_| ())
    }

    /// Common "delete already succeeded" cleanup shared by
    /// `stop_instance_locked`'s direct path and the pending-release retry
    /// loop's eventual-success path: record the deletion durably (best
    /// effort — a ledger write failure here is logged, not propagated,
    /// matching the ledger's existing "lags reality" tolerance), release the
    /// quota slot, and drop the registry entry.
    async fn finish_release(&self, runtime_id: &str, record: &BoundInstance) {
        if let Err(error) = self.ledger.record_deleted(runtime_id).await {
            tracing::warn!(
                target: "instance_manager",
                runtime_id,
                %error,
                "provider instance deleted but ledger record_deleted failed; ledger row will lag reality"
            );
        }
        self.release_instance(record.instance.instance_id.0.as_str());
        self.instances
            .lock()
            .expect("InstanceManager registry lock poisoned")
            .remove(runtime_id);
    }

    /// Runs one sweep of the pending-release queue: every entry whose
    /// backoff has elapsed gets a fresh `delete()` attempt. Success cleans
    /// the instance up fully (same as a direct `stop_instance` success);
    /// failure re-queues with the backoff doubled (capped at
    /// `retry.max_delay`) — there is no attempt ceiling here (see
    /// [`RetryPolicy`]'s doc for why giving up is worse than retrying
    /// forever). Each due entry re-acquires that `runtime_id`'s lock before
    /// touching the registry, so this can never race a concurrent
    /// caller-initiated `start_instance`/`stop_instance` for the same id —
    /// if a caller already cleaned it up first, the entry is silently
    /// dropped (nothing left to do) instead of erroring.
    ///
    /// Public so a caller can flush deterministically (as this crate's own
    /// tests do) instead of waiting on [`Self::spawn_retry_loop`]'s sleep
    /// tick, and so an application can force a drain before shutdown if it
    /// wants to.
    pub async fn retry_pending_releases_once(&self) {
        let due: Vec<PendingRelease> = {
            let now = Instant::now();
            let mut queue = self.pending_release.lock().unwrap();
            let mut due = Vec::new();
            let mut remaining = VecDeque::with_capacity(queue.len());
            while let Some(item) = queue.pop_front() {
                if item.next_retry_at <= now {
                    due.push(item);
                } else {
                    remaining.push_back(item);
                }
            }
            *queue = remaining;
            due
        };

        for mut item in due {
            // Captured up front: `item` may be moved into the pending queue
            // again inside the `Err` arm below, but we still need the id
            // afterward to release the per-runtime_id lock.
            let runtime_id = item.runtime_id.clone();
            let lock = self.get_runtime_lock(&runtime_id);
            let guard = lock.clone().lock_owned().await;

            let still_pending = self
                .instances
                .lock()
                .expect("InstanceManager registry lock poisoned")
                .contains_key(&runtime_id);
            if !still_pending {
                // A concurrent caller-initiated stop_instance already won
                // this race and cleaned it up (or start_instance's Conflict
                // path proves it was never re-registered) — nothing left to
                // retry.
                drop(guard);
                self.release_runtime_lock(&runtime_id, lock);
                continue;
            }

            match self.delete_instance(&item.record).await {
                Ok(()) => {
                    self.finish_release(&runtime_id, &item.record).await;
                    tracing::info!(
                        target: "instance_manager",
                        runtime_id = %runtime_id,
                        attempts = item.attempt + 1,
                        "pending-release retry succeeded; instance cleaned up"
                    );
                }
                Err(error) => {
                    item.attempt += 1;
                    let delay = std::cmp::min(
                        self.config.retry.base_delay * 2u32.saturating_pow(item.attempt.min(16)),
                        self.config.retry.max_delay,
                    );
                    item.next_retry_at = Instant::now() + delay;
                    tracing::warn!(
                        target: "instance_manager",
                        runtime_id = %runtime_id,
                        attempt = item.attempt,
                        %error,
                        "pending-release retry failed again; re-queued"
                    );
                    self.pending_release.lock().unwrap().push_back(item);
                }
            }

            drop(guard);
            self.release_runtime_lock(&runtime_id, lock);
        }
    }

    /// Spawns a detached background task that calls
    /// [`Self::retry_pending_releases_once`] on a fixed 1-second tick for
    /// the life of the process. Dropping the returned `JoinHandle` does not
    /// stop it — same "runs for the life of this process, not aborted on
    /// graceful shutdown" convention as `xgovernor_core::
    /// spawn_orphan_reaper` (a stateless periodic sweep with nothing to hand
    /// back to a client, so there is no drain-completeness reason to stop it
    /// early).
    pub fn spawn_retry_loop(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                self.retry_pending_releases_once().await;
            }
        })
    }

    pub async fn reconcile(&self) -> Result<ReconcileOutcome, ProviderControlError> {
        let active = self.ledger.list_active(&self.kind).await?;

        let mut confirmed = Vec::new();
        let mut orphaned = Vec::new();

        for entry in active {
            match self.attach.attach(&entry.instance).await {
                Ok(backend) => {
                    self.instances
                        .lock()
                        .expect("InstanceManager registry lock poisoned")
                        .insert(
                            entry.runtime_id.clone(),
                            BoundInstance {
                                instance: entry.instance.clone(),
                                backend,
                            },
                        );
                    self.rehydrate_one(entry.instance.instance_id.0.as_str(), &entry.owner_ref);
                    confirmed.push(entry);
                }
                Err(error) => {
                    tracing::warn!(
                        target: "instance_manager",
                        runtime_id = %entry.runtime_id,
                        instance_id = %entry.instance.instance_id,
                        %error,
                        "reconcile: re-attach failed; treating as orphaned"
                    );
                    self.soft_delete_orphan(&entry, "re-attach failed").await;
                    orphaned.push(entry);
                }
            }
        }

        Ok(ReconcileOutcome {
            confirmed,
            orphaned,
        })
    }

    async fn soft_delete_orphan(&self, entry: &ActiveLedgerEntry, reason: &str) {
        if let Err(error) = self.ledger.record_deleted(&entry.runtime_id).await {
            tracing::warn!(
                target: "instance_manager",
                runtime_id = %entry.runtime_id,
                reason,
                %error,
                "reconcile: record_deleted failed for an orphaned ledger row; ledger row will lag reality"
            );
        }
    }

    pub async fn destroy_by_runtime_id(
        &self,
        runtime_id: &str,
    ) -> Result<(), ProviderControlError> {
        match self.stop_instance(runtime_id).await {
            Ok(()) => return Ok(()),
            Err(ProviderControlError::NotFound { .. }) => {}
            Err(error) => return Err(error),
        }

        let lock = self.get_runtime_lock(runtime_id);
        let guard = lock.clone().lock_owned().await;
        let result = self.destroy_by_ledger_locked(runtime_id).await;
        drop(guard);
        self.release_runtime_lock(runtime_id, lock);
        result
    }

    async fn destroy_by_ledger_locked(&self, runtime_id: &str) -> Result<(), ProviderControlError> {
        let active = self.ledger.list_active(&self.kind).await?;
        let Some(entry) = active
            .into_iter()
            .find(|entry| entry.runtime_id == runtime_id)
        else {
            // Nothing this manager's ledger believes is active under this
            // runtime_id — already gone (or never existed under this
            // backend), which is the success case here, not an error.
            return Ok(());
        };

        self.lifecycle
            .delete(ProviderDeleteRequest {
                backend_id: entry.instance.backend_id.clone(),
                instance_id: Some(entry.instance.instance_id.clone()),
                snapshot_id: None,
                reason: ProviderLifecycleReason::Release,
                correlation: Value::Null,
            })
            .await?;

        if let Err(error) = self.ledger.record_deleted(runtime_id).await {
            tracing::warn!(
                target: "instance_manager",
                runtime_id,
                %error,
                "destroy_by_runtime_id: delete succeeded but ledger record_deleted failed; ledger row will lag reality"
            );
        }
        Ok(())
    }

    /// Look up the attached operation backend for `runtime_id`.
    /// `Err(ProviderControlError::NotFound)` if it was never started.
    pub fn backend_for(
        &self,
        runtime_id: &str,
    ) -> Result<Arc<dyn OperationBackend>, ProviderControlError> {
        self.instances
            .lock()
            .expect("InstanceManager registry lock poisoned")
            .get(runtime_id)
            .map(|record| record.backend.clone())
            .ok_or_else(|| ProviderControlError::NotFound {
                resource_ref: runtime_id.to_string(),
            })
    }
}

fn decrement(counts: &mut HashMap<String, usize>, owner_ref: &str) {
    if let Some(count) = counts.get_mut(owner_ref) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            counts.remove(owner_ref);
        }
    }
}
