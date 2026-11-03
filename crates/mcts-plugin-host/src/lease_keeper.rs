//! Background heartbeat for MCTS-owned live branches.
//!
//! A single run owns many short-lived sandbox branches. The plugin process
//! does not implement xg's session lease protocol, so the host-side
//! :class:`MctsRun` periodically renews every branch for the duration of the
//! run. The same run terminates all heartbeats before releasing branches in
//! its cleanup path.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::host_cap_service::HostApplication;
use crate::run_map::RunMap;

pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Handle returned by [`spawn_lease_keeper`]. Dropping it does not stop the
/// heartbeat task; call [`LeaseKeeperGuard::shutdown`] to terminate cleanly.
pub struct LeaseKeeperGuard {
    shutdown_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl LeaseKeeperGuard {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.task.await;
    }
}

pub fn spawn_lease_keeper<A>(
    run_id: String,
    run_map: Arc<RunMap>,
    app: Arc<A>,
    client_id: String,
    interval: Duration,
) -> LeaseKeeperGuard
where
    A: HostApplication + 'static,
{
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    for branch_id in run_map.list_run_branches(&run_id) {
                        if let Err(error) = app.heartbeat(&run_id, &branch_id, &client_id).await {
                            tracing::warn!(
                                run_id,
                                branch_id,
                                error = %error,
                                "MCTS branch heartbeat failed"
                            );
                        }
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() && *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });

    LeaseKeeperGuard { shutdown_tx, task }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_cap_service::FakeApplication;
    use crate::run_map::RunMap;

    #[tokio::test]
    async fn heartbeats_owned_branch_until_shutdown() {
        let map = Arc::new(RunMap::new());
        map.register_run("run-1").unwrap();
        map.register_root_state("run-1", "state-root").unwrap();
        map.add_branch("run-1", "branch-1", "state-root").unwrap();
        let app = Arc::new(FakeApplication::new());

        let guard = spawn_lease_keeper(
            "run-1".to_string(),
            map.clone(),
            app.clone(),
            "mcts-run:run-1".to_string(),
            Duration::from_millis(5),
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        guard.shutdown().await;

        let calls = app.calls();
        assert!(calls
            .iter()
            .any(|call| call == "heartbeat:branch-1:mcts-run:run-1"));
    }
}
