//! Orphan session reaper (Component C, `docs/session_orchestration_skeleton.md`).
//!
//! A session's [`crate::session_lease::SessionLeaseTable`] entry ages once its
//! holder stops heartbeating (crash, network partition, a client that never
//! calls `detach`/`close`). Nothing else in the control plane notices this on
//! its own — the runtime keeps running, leaking sandbox/provider resources,
//! until something force-closes it. This module is that something: a
//! background sweep that force-closes any session whose lease has carried no
//! live heartbeat for longer than [`ORPHAN_SESSION_THRESHOLD_MS`] (2h,
//! conservative on purpose — see that constant's own doc comment).
//!
//! Deliberately reads its candidate set from `SessionLeaseTable::snapshot()`,
//! not `SessionRepository` — the lease table already carries the "who last
//! heartbeated, and when" fact this sweep cares about, and every session with
//! lease enforcement on gets a table entry (acquired on `open`'s first
//! `heartbeat` — see `SessionApplication::heartbeat`'s auto-acquire path). One
//! consequence worth knowing: a session that was `detach()`ed (which removes
//! its lease-table entry outright, by design — see `SessionApplication::
//! detach`) will not be swept here even if left running indefinitely. That is
//! intentional, not a gap: `detach` means "leave the runtime warm for the next
//! `open`", which is a different lifecycle state than "orphaned".

use crate::session_lease::current_time_ms;
use crate::{
    SecurityContext, SessionApplication, SessionDomainError, SessionLeaseTable,
    ORPHAN_SESSION_THRESHOLD_MS, REAPER_INTERVAL,
};
use session_protocol::SessionLeaseClaim;
use std::sync::Arc;

/// Outcome of a single candidate's reap attempt. Kept as an explicit enum
/// (rather than logging inline from `reap_one_record`) so `sweep_once`'s
/// tests can assert on it per-record instead of only on aggregate side
/// effects.
#[derive(Debug)]
enum ReapOutcome {
    /// The TOCTOU re-check (`SessionLeaseTable::has_live_lease`) found the
    /// lease live again — a heartbeat landed between `snapshot()` and now.
    /// Left alone.
    StillLive,
    /// Confirmed stale past `ORPHAN_SESSION_THRESHOLD_MS` at the re-check;
    /// `SessionApplication::close` was called and returned `Ok`.
    Closed,
    /// `close` was attempted but returned an error (e.g. the session was
    /// already closed through another path between the snapshot and this
    /// sweep, so `require_session` inside `close` returned `NotFound`). Not a
    /// bug — logged and skipped so one bad record doesn't stall the sweep.
    CloseFailed(SessionDomainError),
}

/// TOCTOU-guarded reap of a single `session_id` already known (from
/// `snapshot()`) to be past `ORPHAN_SESSION_THRESHOLD_MS` as of the sweep's
/// start. Re-checks the table's *current* state under its own lock before
/// acting — `SessionLeaseTable::has_live_lease`'s doc comment is written
/// explicitly for this call site.
///
/// The close is issued with an anonymous [`SessionLeaseClaim`] (`client_id:
/// None`). `SessionApplication::close` gates through
/// `SessionLeaseTable::check_holder`, whose stale-passthrough arm lets an
/// anonymous caller through once the recorded holder's heartbeat is older
/// than `STALE_LEASE_THRESHOLD_MS` (45s) — and anything reaching this
/// function already failed a stricter 2h bar, so the 45s check always passes
/// too.
async fn reap_one_record(
    app: &SessionApplication,
    lease_table: &SessionLeaseTable,
    session_id: &str,
) -> ReapOutcome {
    if lease_table
        .has_live_lease(session_id, ORPHAN_SESSION_THRESHOLD_MS)
        .await
    {
        return ReapOutcome::StillLive;
    }
    // The reaper is server-internal trusted code, not a caller acting on
    // behalf of a tenant — same World-A trust posture as any other
    // daemon-owned background job (`docs/tenancy_design.md` §0). An admin
    // `SecurityContext` makes `close`'s ownership check a no-op here, which
    // is correct: whoever the session belongs to, the reaper must be able to
    // force-close it once it is confirmed orphaned.
    let reaper_ctx = SecurityContext::admin("system:orphan-reaper");
    match app
        .close(&reaper_ctx, session_id, SessionLeaseClaim::default())
        .await
    {
        Ok(_) => ReapOutcome::Closed,
        Err(error) => ReapOutcome::CloseFailed(error),
    }
}

/// Sweep every entry currently in `lease_table`, force-closing the ones past
/// `ORPHAN_SESSION_THRESHOLD_MS`. Split out from [`spawn_orphan_reaper`] so a
/// test can drive exactly one sweep synchronously instead of waiting on
/// `REAPER_INTERVAL` (10 min) in real time.
async fn sweep_once(app: &SessionApplication, lease_table: &SessionLeaseTable) {
    let now = match current_time_ms() {
        Ok(now) => now,
        Err(_) => {
            tracing::error!("orphan reaper sweep skipped: daemon wall clock is before UNIX_EPOCH");
            return;
        }
    };
    for (session_id, holder_client_id, last_heartbeat_ms) in lease_table.snapshot().await {
        if now.saturating_sub(last_heartbeat_ms) <= ORPHAN_SESSION_THRESHOLD_MS {
            continue;
        }
        match reap_one_record(app, lease_table, &session_id).await {
            ReapOutcome::Closed => tracing::warn!(
                session_id = %session_id,
                holder_client_id = %holder_client_id,
                last_heartbeat_ms,
                threshold_ms = ORPHAN_SESSION_THRESHOLD_MS,
                "orphan reaper force-closed a session with no live lease past the threshold"
            ),
            ReapOutcome::StillLive => tracing::debug!(
                session_id = %session_id,
                "orphan reaper TOCTOU re-check found a live lease; left alone"
            ),
            ReapOutcome::CloseFailed(error) => tracing::warn!(
                session_id = %session_id,
                error = %error,
                "orphan reaper close attempt failed; skipped"
            ),
        }
    }
}

/// Spawn a background task that calls [`sweep_once`] every `REAPER_INTERVAL`
/// (10 min) for the lifetime of the returned task. The first sweep runs one
/// interval after this is called, not immediately — this avoids racing a
/// session that is still mid-`open` at daemon startup.
///
/// The caller owns the returned `JoinHandle`. Per `tokio::spawn` semantics,
/// dropping it does NOT stop the sweep — call `.abort()` explicitly if the
/// process ever needs to stop it before exit (today no caller does; the sweep
/// simply stops when the process does).
pub fn spawn_orphan_reaper(
    app: SessionApplication,
    lease_table: Arc<SessionLeaseTable>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(REAPER_INTERVAL);
        ticker.tick().await; // first tick fires immediately; skip it
        loop {
            ticker.tick().await;
            sweep_once(&app, &lease_table).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Clock, EffectiveCapabilities, IsolationBoundary, IsolationFacts, NetworkIsolation,
        NormalizedSessionEnvironment, OpaqueRuntimeState, RuntimeIdGenerator,
        RuntimeInteractionInput, RuntimeStartRequest, RuntimeTurnInput,
        SessionEnvironmentNormalizer, SessionRecord, SessionRepository, SessionStatus,
        TurnIdGenerator, WorkspaceAccess, WorkspaceFacts,
    };
    use async_trait::async_trait;
    use session_protocol::SessionOpenRequest;
    use std::collections::{BTreeSet, HashMap};
    use tokio::sync::Mutex;

    fn make_record(runtime_id: &str) -> SessionRecord {
        SessionRecord {
            runtime_id: runtime_id.to_string(),
            conversation_id: "conversation-1".into(),
            sender_id: "sender-1".into(),
            status: SessionStatus::Idle,
            created_at_ms: 1,
            updated_at_ms: 1,
            workspace: WorkspaceFacts {
                workspace_id: "workspace-1".into(),
                root: ".".into(),
                access: WorkspaceAccess::ReadWrite,
                revision: None,
                metadata: serde_json::Value::Null,
            },
            isolation: IsolationFacts {
                boundary: IsolationBoundary::Host,
                workspace_access: WorkspaceAccess::ReadWrite,
                network: NetworkIsolation::None,
                metadata: serde_json::Value::Null,
            },
            capabilities: EffectiveCapabilities {
                sandbox: Default::default(),
                runtime: Default::default(),
            },
            runtime: OpaqueRuntimeState {
                runtime_kind: "test".into(),
                schema_version: 1,
                state: serde_json::Value::Null,
            },
            llm: None,
            lease: None,
            lineage: None,
            last_error: None,
            tenant_id: None,
            created_by: "test".to_string(),
        }
    }

    /// Keyed by `runtime_id`, unlike `application.rs`'s single-slot test
    /// repository — the reaper sweeps multiple sessions at once, so its tests
    /// need to tell them apart.
    #[derive(Default)]
    struct MultiRecordRepository(Mutex<HashMap<String, SessionRecord>>);

    #[async_trait]
    impl SessionRepository for MultiRecordRepository {
        async fn get(&self, runtime_id: &str) -> Result<Option<SessionRecord>, SessionDomainError> {
            Ok(self.0.lock().await.get(runtime_id).cloned())
        }
        async fn save(&self, record: SessionRecord) -> Result<(), SessionDomainError> {
            self.0
                .lock()
                .await
                .insert(record.runtime_id.clone(), record);
            Ok(())
        }
    }

    /// Records every `runtime_id` passed to `stop()`, so tests can assert the
    /// reaper actually force-stopped (not just tombstoned) an orphan.
    #[derive(Default)]
    struct TrackingRuntime {
        stopped: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl crate::RuntimeAdapter for TrackingRuntime {
        fn kind(&self) -> &str {
            "tracking"
        }

        fn capabilities(&self) -> BTreeSet<session_protocol::SessionRuntimeCapability> {
            BTreeSet::new()
        }

        async fn start(&self, _request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
            unreachable!("reaper tests never call open()/start()")
        }

        async fn stop(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
            self.stopped.lock().await.push(runtime_id.to_string());
            Ok(())
        }

        async fn attach(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn submit_turn(
            &self,
            _input: RuntimeTurnInput,
        ) -> Result<crate::RuntimeEventReceiver, SessionDomainError> {
            unreachable!("reaper tests never submit a turn")
        }

        async fn answer_interaction(
            &self,
            _input: RuntimeInteractionInput,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn cancel(
            &self,
            _runtime_id: &str,
            _turn_id: Option<&str>,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }
    }

    struct FixedIds;
    impl TurnIdGenerator for FixedIds {
        fn next_turn_id(&self) -> String {
            "turn-fixed".into()
        }
    }
    impl RuntimeIdGenerator for FixedIds {
        fn next_runtime_id(&self) -> String {
            "runtime-fixed".into()
        }
    }
    impl Clock for FixedIds {
        fn now_ms(&self) -> u64 {
            1
        }
    }

    struct UnusedEnvironment;
    #[async_trait]
    impl SessionEnvironmentNormalizer for UnusedEnvironment {
        async fn normalize(
            &self,
            _ctx: &SecurityContext,
            _request: &SessionOpenRequest,
        ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
            unreachable!("reaper tests never call open()")
        }
    }

    fn test_application(
        runtime: Arc<TrackingRuntime>,
        repository: Arc<MultiRecordRepository>,
        lease_table: Arc<SessionLeaseTable>,
    ) -> SessionApplication {
        SessionApplication::new(
            runtime,
            repository,
            Arc::new(FixedIds),
            Arc::new(FixedIds),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedIds),
        )
        .with_lease_table(lease_table)
    }

    #[tokio::test]
    async fn orphan_past_threshold_is_force_closed_and_stops_the_runtime() {
        let repository = Arc::new(MultiRecordRepository::default());
        repository.save(make_record("runtime-1")).await.unwrap();
        let lease_table = Arc::new(SessionLeaseTable::new());
        lease_table
            .acquire("runtime-1", "client-a", None, None)
            .await;
        lease_table
            .set_last_heartbeat_ms_for_test(
                "runtime-1",
                current_time_ms()
                    .expect("wall clock")
                    .saturating_sub(ORPHAN_SESSION_THRESHOLD_MS + 1_000),
            )
            .await;
        let runtime = Arc::new(TrackingRuntime::default());
        let application =
            test_application(runtime.clone(), repository.clone(), lease_table.clone());

        sweep_once(&application, &lease_table).await;

        assert_eq!(
            runtime.stopped.lock().await.as_slice(),
            ["runtime-1".to_string()],
            "reaper must call runtime.stop() on the orphaned session"
        );
        let stored = repository.get("runtime-1").await.unwrap().unwrap();
        assert_eq!(stored.status, SessionStatus::Closed);
        assert!(
            lease_table
                .snapshot()
                .await
                .iter()
                .all(|(sid, _, _)| sid != "runtime-1"),
            "close() must remove the lease-table entry"
        );
    }

    #[tokio::test]
    async fn session_within_threshold_is_left_alone() {
        let repository = Arc::new(MultiRecordRepository::default());
        repository.save(make_record("runtime-1")).await.unwrap();
        let lease_table = Arc::new(SessionLeaseTable::new());
        lease_table
            .acquire("runtime-1", "client-a", None, None)
            .await;
        // Stale enough to lose the write lock (past the 45s threshold) but
        // nowhere near the 2h orphan threshold.
        lease_table
            .set_last_heartbeat_ms_for_test(
                "runtime-1",
                current_time_ms()
                    .expect("wall clock")
                    .saturating_sub(3_600_000),
            )
            .await;
        let runtime = Arc::new(TrackingRuntime::default());
        let application =
            test_application(runtime.clone(), repository.clone(), lease_table.clone());

        sweep_once(&application, &lease_table).await;

        assert!(
            runtime.stopped.lock().await.is_empty(),
            "a session within the orphan threshold must not be stopped"
        );
        let stored = repository.get("runtime-1").await.unwrap().unwrap();
        assert_eq!(stored.status, SessionStatus::Idle);
    }

    #[tokio::test]
    async fn toctou_recheck_skips_a_session_that_became_live_again() {
        let repository = Arc::new(MultiRecordRepository::default());
        repository.save(make_record("runtime-1")).await.unwrap();
        let lease_table = Arc::new(SessionLeaseTable::new());
        lease_table
            .acquire("runtime-1", "client-a", None, None)
            .await;
        lease_table
            .set_last_heartbeat_ms_for_test(
                "runtime-1",
                current_time_ms()
                    .expect("wall clock")
                    .saturating_sub(ORPHAN_SESSION_THRESHOLD_MS + 1_000),
            )
            .await;
        let runtime = Arc::new(TrackingRuntime::default());
        let application =
            test_application(runtime.clone(), repository.clone(), lease_table.clone());

        // Simulate a heartbeat landing between `snapshot()` and the reap
        // (e.g. a client that was merely slow, not actually gone).
        lease_table
            .heartbeat("runtime-1", "client-a", None, None)
            .await
            .expect("client-a is still the holder");

        let outcome = reap_one_record(&application, &lease_table, "runtime-1").await;
        assert!(
            matches!(outcome, ReapOutcome::StillLive),
            "expected StillLive, got {outcome:?}"
        );
        assert!(
            runtime.stopped.lock().await.is_empty(),
            "TOCTOU re-check must prevent closing a session that heartbeated again"
        );
        let stored = repository.get("runtime-1").await.unwrap().unwrap();
        assert_eq!(stored.status, SessionStatus::Idle);
    }

    #[tokio::test]
    async fn one_orphans_close_failure_does_not_block_the_rest_of_the_sweep() {
        let repository = Arc::new(MultiRecordRepository::default());
        // "runtime-ghost" has a stale lease-table entry but no matching
        // repository record (e.g. already cleaned up through another path) —
        // `close()` will fail with `NotFound`. "runtime-1" is a normal
        // orphan and must still be reaped in the same sweep.
        repository.save(make_record("runtime-1")).await.unwrap();
        let lease_table = Arc::new(SessionLeaseTable::new());
        for session_id in ["runtime-ghost", "runtime-1"] {
            lease_table
                .acquire(session_id, "client-a", None, None)
                .await;
            lease_table
                .set_last_heartbeat_ms_for_test(
                    session_id,
                    current_time_ms()
                        .expect("wall clock")
                        .saturating_sub(ORPHAN_SESSION_THRESHOLD_MS + 1_000),
                )
                .await;
        }
        let runtime = Arc::new(TrackingRuntime::default());
        let application =
            test_application(runtime.clone(), repository.clone(), lease_table.clone());

        sweep_once(&application, &lease_table).await;

        assert_eq!(
            runtime.stopped.lock().await.as_slice(),
            ["runtime-1".to_string()],
            "the ghost record's close failure must not stop runtime-1 from being reaped"
        );
        let stored = repository.get("runtime-1").await.unwrap().unwrap();
        assert_eq!(stored.status, SessionStatus::Closed);
    }
}
