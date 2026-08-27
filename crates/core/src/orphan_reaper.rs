use crate::session_lease::current_time_ms;
use crate::{
    SecurityContext, SessionApplication, SessionDomainError, SessionLeaseTable,
    ORPHAN_SESSION_THRESHOLD_MS, REAPER_INTERVAL,
};
use session_protocol::SessionLeaseClaim;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct OrphanReaperConfig {
    pub threshold: Duration,
    pub interval: Duration,
}

impl Default for OrphanReaperConfig {
    fn default() -> Self {
        Self {
            threshold: Duration::from_millis(ORPHAN_SESSION_THRESHOLD_MS),
            interval: REAPER_INTERVAL,
        }
    }
}

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
/// function already failed the configured orphan threshold, so production
/// startup clamps the stale lease window to no greater than that threshold.
async fn reap_one_record_with_threshold(
    app: &SessionApplication,
    lease_table: &SessionLeaseTable,
    session_id: &str,
    threshold_ms: u64,
) -> ReapOutcome {
    if lease_table.has_live_lease(session_id, threshold_ms).await {
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

#[cfg(test)]
async fn reap_one_record(
    app: &SessionApplication,
    lease_table: &SessionLeaseTable,
    session_id: &str,
) -> ReapOutcome {
    reap_one_record_with_threshold(app, lease_table, session_id, ORPHAN_SESSION_THRESHOLD_MS).await
}

/// Sweep every entry currently in `lease_table`, force-closing the ones past
/// `ORPHAN_SESSION_THRESHOLD_MS`. Split out from [`spawn_orphan_reaper`] so a
/// test can drive exactly one sweep synchronously instead of waiting on
/// `REAPER_INTERVAL` (10 min by default) in real time.
async fn sweep_once_with_config(
    app: &SessionApplication,
    lease_table: &SessionLeaseTable,
    config: OrphanReaperConfig,
) {
    let threshold_ms = config.threshold.as_millis().min(u64::MAX as u128) as u64;
    let now = match current_time_ms() {
        Ok(now) => now,
        Err(_) => {
            tracing::error!("orphan reaper sweep skipped: daemon wall clock is before UNIX_EPOCH");
            return;
        }
    };
    for (session_id, holder_client_id, last_heartbeat_ms) in lease_table.snapshot().await {
        if now.saturating_sub(last_heartbeat_ms) <= threshold_ms {
            continue;
        }
        match reap_one_record_with_threshold(app, lease_table, &session_id, threshold_ms).await {
            ReapOutcome::Closed => tracing::warn!(
                session_id = %session_id,
                holder_client_id = %holder_client_id,
                last_heartbeat_ms,
                threshold_ms,
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

#[cfg(test)]
async fn sweep_once(app: &SessionApplication, lease_table: &SessionLeaseTable) {
    sweep_once_with_config(app, lease_table, OrphanReaperConfig::default()).await;
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
    spawn_orphan_reaper_with_config(app, lease_table, OrphanReaperConfig::default())
}

pub fn spawn_orphan_reaper_with_config(
    app: SessionApplication,
    lease_table: Arc<SessionLeaseTable>,
    config: OrphanReaperConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(config.interval);
        ticker.tick().await; // first tick fires immediately; skip it
        loop {
            ticker.tick().await;
            sweep_once_with_config(&app, &lease_table, config).await;
        }
    })
}
