// Only used by `#[cfg(test)] mod tests`'s fake `ProviderLifecycle`/
// `OperationAttach` impls (via `use super::*`) — gated the same way so a
// non-test build doesn't warn about it being unused.
#[cfg(test)]
use async_trait::async_trait;
use backend::{ActiveLedgerEntry, OperationAttach, ProviderInstanceLedger};
use operation_protocol::OperationBackend;
use provider_protocol::{
    BackendId, ProviderControlError, ProviderCreateRequest, ProviderDeleteRequest,
    ProviderInstance, ProviderKind, ProviderLifecycle, ProviderLifecycleReason,
    ProviderResourceLimits,
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

#[cfg(test)]
mod tests {
    use super::*;
    use backend::local::LocalProvider;
    use operation_protocol::capability::exec::ExecRequest;
    use provider_protocol::{
        ProviderDeleteOutcome, ProviderInspectRequest, ProviderInstanceId, ProviderInstanceStatus,
        ProviderLifecycleState, ProviderLoadRequest, ProviderPauseRequest, ProviderSnapshot,
    };
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

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
}
