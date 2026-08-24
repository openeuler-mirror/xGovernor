use crate::{SecurityContext, SessionApplication, SessionDomainError};
use std::time::Duration;

pub const RECLAIM_SWEEP_INTERVAL: Duration = Duration::from_secs(300);
const RECLAIM_SWEEP_MAX_CANDIDATES: usize = 10_000;

#[derive(Debug, Clone, Copy)]
pub struct ReclaimSweeperConfig {
    pub interval: Duration,
}

impl Default for ReclaimSweeperConfig {
    fn default() -> Self {
        Self {
            interval: RECLAIM_SWEEP_INTERVAL,
        }
    }
}

#[derive(Debug)]
enum SweepOutcome {
    Alive,
    Closed,
    CheckFailed(SessionDomainError),
    ReclaimFailed(SessionDomainError),
}

/// Probe one `runtime_id` and reclaim it if the provider confirms it is
/// gone. See [`SweepOutcome`] for what each result means.
async fn sweep_one_record(
    app: &SessionApplication,
    ctx: &SecurityContext,
    runtime_id: &str,
) -> SweepOutcome {
    match app.check_alive(ctx, runtime_id).await {
        Ok(true) => SweepOutcome::Alive,
        Ok(false) => match app.reclaim(ctx, runtime_id).await {
            Ok(_) => SweepOutcome::Closed,
            Err(error) => SweepOutcome::ReclaimFailed(error),
        },
        Err(error) => SweepOutcome::CheckFailed(error),
    }
}

async fn sweep_once_with_config(app: &SessionApplication, _config: ReclaimSweeperConfig) {
    let sweeper_ctx = SecurityContext::admin("system:reclaim-sweeper");
    let page = match app
        .list_active_sessions_for_reclaim_sweep(RECLAIM_SWEEP_MAX_CANDIDATES)
        .await
    {
        Ok(page) => page,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "reclaim sweep: failed to list active sessions; skipped this tick"
            );
            return;
        }
    };
    if page.total_active as usize > page.sessions.len() {
        tracing::warn!(
            total_active = page.total_active,
            fetched = page.sessions.len(),
            limit = RECLAIM_SWEEP_MAX_CANDIDATES,
            "reclaim sweep: active-session count exceeds the per-sweep candidate cap; \
             some sessions were not checked this tick"
        );
    }
    for record in page.sessions {
        let runtime_id = record.runtime_id.as_str();
        match sweep_one_record(app, &sweeper_ctx, runtime_id).await {
            SweepOutcome::Alive => {
                tracing::debug!(runtime_id = %runtime_id, "reclaim sweep: provider confirms alive")
            }
            SweepOutcome::Closed => tracing::warn!(
                runtime_id = %runtime_id,
                "reclaim sweep: provider sandbox is gone; session force-closed"
            ),
            SweepOutcome::CheckFailed(error) => tracing::debug!(
                runtime_id = %runtime_id,
                error = %error,
                "reclaim sweep: liveness check failed (transient/unknown); left alone"
            ),
            SweepOutcome::ReclaimFailed(error) => tracing::warn!(
                runtime_id = %runtime_id,
                error = %error,
                "reclaim sweep: reclaim attempt failed; skipped"
            ),
        }
    }
}

pub fn spawn_reclaim_sweeper(app: SessionApplication) -> tokio::task::JoinHandle<()> {
    spawn_reclaim_sweeper_with_config(app, ReclaimSweeperConfig::default())
}

pub fn spawn_reclaim_sweeper_with_config(
    app: SessionApplication,
    config: ReclaimSweeperConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(config.interval);
        ticker.tick().await; // first tick fires immediately; skip it
        loop {
            ticker.tick().await;
            sweep_once_with_config(&app, config).await;
        }
    })
}
