use backend::e2b::E2bProvider;
use backend::local::LocalProvider;
use backend::{OperationAttach, ProviderInstanceLedger, SqliteProviderInstanceLedger};
use provider_protocol::{ProviderKind, ProviderLifecycle};
use std::collections::HashMap;
use std::io::IsTerminal;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use xgovernor_core::{
    Clock, Role, RuntimeIdGenerator, SessionApplication, SessionLeaseTable,
    SqliteSessionRepository, TurnIdGenerator,
};
use xgovernor_manager::{InstanceManager, InstanceManagerConfig};
use xgovernor_runtime_pi::{PiRuntime, PiSessionEnvironment};
use xgovernor_server::{create_router, SessionHttpState, TokenTable};

const FORCED_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

const DEFAULT_MAX_SANDBOXES_PER_OWNER: usize = 20;
const DEFAULT_MAX_SANDBOXES_GLOBAL: usize = 1024;

/// Environment variable gating whether the `e2b` `InstanceManager` is
/// constructed at all (see [`build_e2b_instance_manager`] and its call site in
/// [`main`]). Same variable `backend::e2b::provider::resolve_api_key` reads by
/// default when a session's `provider_options` doesn't override
/// `api_key_env`/`api_key` — kept in sync deliberately: this is a startup
/// pre-check, not an independent config surface, so it must ask the identical
/// question the provider itself will ask again on every `create` call.
const E2B_API_KEY_ENV: &str = "E2B_API_KEY";

/// Builds the `InstanceManager` wrapping a real [`LocalProvider`], replicating
/// `apps/runtime-mock`'s private `in_memory_local_manager` construction shape
/// (see that crate for the canonical version — this binary needs the bare
/// `Arc<InstanceManager>` itself to hand to [`PiRuntime::new`], not a
/// `MockRuntime` wrapper, since `PiRuntime` composes `InstanceManager`s
/// directly rather than delegating to another `RuntimeAdapter`). `db_path`
/// points at the same physical SQLite file `SqliteSessionRepository` above
/// already opened; a second, independent `rusqlite::Connection` onto that file
/// in WAL mode is exactly the convention `SqliteProviderInstanceLedger`'s
/// module doc describes, and [`build_e2b_instance_manager`] opens a third one
/// onto the same file the same way.
fn build_local_instance_manager(db_path: &Path) -> Arc<InstanceManager> {
    let ledger: Arc<dyn ProviderInstanceLedger> =
        Arc::new(SqliteProviderInstanceLedger::open(db_path).unwrap_or_else(|error| {
            eprintln!(
                "refusing to start: failed to open local provider-instance ledger at {}: {error}",
                db_path.display()
            );
            std::process::exit(1);
        }));
    let local = Arc::new(LocalProvider::new());
    let lifecycle: Arc<dyn ProviderLifecycle> = local.clone();
    let attach: Arc<dyn OperationAttach> = local;
    Arc::new(InstanceManager::new(
        lifecycle,
        attach,
        ledger,
        ProviderKind("local".to_string()),
        InstanceManagerConfig::new(DEFAULT_MAX_SANDBOXES_PER_OWNER, DEFAULT_MAX_SANDBOXES_GLOBAL),
    ))
}

/// Builds the `InstanceManager` wrapping a real [`E2bProvider`] — same
/// rationale and construction shape as [`build_local_instance_manager`],
/// mirroring `apps/runtime-mock`'s private `in_memory_e2b_manager`
/// construction shape. Only called when [`E2B_API_KEY_ENV`] is set (see [`main`]):
/// `E2bProvider::new()` itself never fails (it takes no config), but every
/// sandbox it would actually create needs that env var at `create` time
/// (`resolve_api_key`), so gating construction on it here turns a
/// per-session `InvalidRequest` failure at first use into a clear
/// startup-time signal that this backend isn't configured.
fn build_e2b_instance_manager(db_path: &Path) -> Arc<InstanceManager> {
    let ledger: Arc<dyn ProviderInstanceLedger> =
        Arc::new(SqliteProviderInstanceLedger::open(db_path).unwrap_or_else(|error| {
            eprintln!(
                "refusing to start: failed to open e2b provider-instance ledger at {}: {error}",
                db_path.display()
            );
            std::process::exit(1);
        }));
    let e2b = Arc::new(E2bProvider::new());
    let lifecycle: Arc<dyn ProviderLifecycle> = e2b.clone();
    let attach: Arc<dyn OperationAttach> = e2b;
    Arc::new(InstanceManager::new(
        lifecycle,
        attach,
        ledger,
        ProviderKind("e2b".to_string()),
        InstanceManagerConfig::new(DEFAULT_MAX_SANDBOXES_PER_OWNER, DEFAULT_MAX_SANDBOXES_GLOBAL),
    ))
}

/// Reconciles `manager` against its ledger (`InstanceManager::reconcile` —
/// see `apps/runtime-mock`'s own startup-reconcile usage for why this must
/// run once before a manager serves real traffic) and spawns
/// its pending-release retry loop. Exits the process on a reconcile failure,
/// matching the fail-closed treatment `SqliteSessionRepository::open` above
/// already gets: a `InstanceManager` that can't be trusted to agree with its
/// own ledger at startup must not silently serve traffic with stale
/// quota/registry state. Returns the retry loop's `JoinHandle` so `main` can
/// keep it alive for the process's lifetime (dropping it does not stop the
/// loop, but the binding must still outlive `main`'s local scope — same
/// convention as `_orphan_reaper`/`_admin_stream_sweeper` below).
async fn reconcile_and_spawn_retry_loop(
    label: &str,
    manager: &Arc<InstanceManager>,
) -> tokio::task::JoinHandle<()> {
    if let Err(error) = manager.reconcile().await {
        eprintln!(
            "refusing to start: failed to reconcile '{label}' provider-instance ledger: {error}"
        );
        std::process::exit(1);
    }
    Arc::clone(manager).spawn_retry_loop()
}

fn xgovernor_data_dir() -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("XGOVERNOR_DATA_DIR") {
        return std::path::PathBuf::from(dir);
    }
    dirs::home_dir()
        .map(|home| home.join(".xgovernor"))
        .unwrap_or_else(|| std::env::temp_dir().join(".xgovernor"))
}

/// Single SQLite file shared (in WAL mode) by `SqliteSessionRepository` and
/// `SqliteProviderInstanceLedger` — see this module's doc comment.
fn xgovernor_db_path() -> std::path::PathBuf {
    xgovernor_data_dir().join("xgovernor.db")
}

/// Root directory `PiRuntime` creates one per-`runtime_id` `--session-dir`
/// subdirectory under (`docs/pi_session_restore_plan.md` §1.1/decision 1).
/// Same root as `xgovernor_db_path()` so both live under one
/// `XGOVERNOR_DATA_DIR`, deliberately not its own separate env var — a single
/// configuration knob, not another toggle.
fn xgovernor_pi_session_root() -> std::path::PathBuf {
    xgovernor_data_dir().join("pi-sessions")
}

fn install_logging(data_dir: &Path) -> Option<tracing_appender::non_blocking::WorkerGuard> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // Default `SystemTime` timer (RFC3339 UTC) instead of the chrono-backed
    // `ChronoLocal`: keeping chrono in the tree dragged in
    // iana-time-zone → android_system_properties, which the local crates.io
    // mirror fails to serve. Timestamps are now UTC rather than local-time
    // with offset; the format is otherwise equivalent.
    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal());

    let log_dir = data_dir.join("logs");
    let mut file_guard = None;
    let file_layer = match std::fs::create_dir_all(&log_dir) {
        Ok(()) => {
            let file_appender = tracing_appender::rolling::daily(&log_dir, "xgovernor-server.log");
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            file_guard = Some(guard);
            Some(tracing_subscriber::fmt::layer().with_writer(non_blocking).with_ansi(false))
        }
        Err(error) => {
            eprintln!(
                "xgovernor-server: cannot create log directory {} ({error}); logging to stderr only",
                log_dir.display()
            );
            None
        }
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(stderr_layer)
        .with(file_layer)
        .init();

    if file_guard.is_some() {
        tracing::info!(
            dir = %log_dir.display(),
            "file logging enabled (xgovernor-server.log.<date>, daily rolling)"
        );
    }

    file_guard
}

/// Wall-clock `Clock` plus UUID-backed id generators, bundled behind one
/// type since none of them carry state beyond `std`/`uuid`.
struct SystemClockAndUuidIds;

impl TurnIdGenerator for SystemClockAndUuidIds {
    fn next_turn_id(&self) -> String {
        format!("turn-{}", uuid::Uuid::new_v4())
    }
}

impl RuntimeIdGenerator for SystemClockAndUuidIds {
    fn next_runtime_id(&self) -> String {
        format!("runtime-{}", uuid::Uuid::new_v4())
    }
}

impl Clock for SystemClockAndUuidIds {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is before the unix epoch")
            .as_millis() as u64
    }
}

/// Resolves `SessionOpenRequest.workspace` and isolation facts for PI
/// sessions, delegated entirely to `xgovernor_runtime_pi::PiSessionEnvironment`
/// (replacing the old hardcoded host-only `LocalWorkspaceEnvironment`): the
/// selected `ext.runtime_pi.backend_id` now drives both admission
/// (`enforce_workspace_axiom` with `provider_is_sandbox` per backend) and the
/// `isolation.boundary` reported on `open` — `"local"` stays host-boundary,
/// `"e2b"` is declared `remote`, so tenant + git + e2b sessions can be
/// admitted instead of being rejected as if there were no sandbox.

/// `docs/tenancy_design.md` §3.1: "admin 面只绑 loopback / unix socket，不随
/// 租户 API 暴露在网络上" — recognizes loopback host literals in a
/// `host:port` (or bare host) bind-address string. Deliberately string-based
/// rather than a real DNS/`ToSocketAddrs` resolution: this is a startup
/// fail-closed guard, not a general-purpose address validator, and it must
/// not depend on network access to decide whether to refuse to start.
fn is_loopback_bind_addr(addr: &str) -> bool {
    let host = if let Some(rest) = addr.strip_prefix('[') {
        // "[::1]:8787" or a bare "[::1]".
        rest.split(']').next().unwrap_or(rest)
    } else {
        match addr.rsplit_once(':') {
            Some((host, port)) if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => {
                host
            }
            _ => addr,
        }
    };
    host == "localhost" || host == "::1" || host.starts_with("127.")
}

/// Resolves once an OS shutdown signal (`Ctrl+C`/`SIGINT`, or `SIGTERM` — the
/// signal `systemd`/`docker stop`/most process supervisors send) is
/// received. Fed to both listeners' `with_graceful_shutdown` via a shared
/// `broadcast` channel in `main` (a plain `tokio::select!` between the two
/// `serve()` futures and this signal would *cancel* whichever server hadn't
/// finished yet instead of letting it drain — `with_graceful_shutdown` is
/// what makes `axum::serve` keep running to let in-flight requests complete
/// after this future resolves, rather than being torn down immediately).
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install the ctrl_c signal handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install the SIGTERM signal handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

/// Outcome of [`wait_for_shutdown`]. Kept as data rather than having that
/// function call `tracing`/`std::process::exit` directly, so the shutdown
/// orchestration itself stays testable — `main` is the only caller that
/// turns this into logging and (for [`ShutdownOutcome::TimedOut`]) a forced
/// process exit.
enum ShutdownOutcome {
    /// Both listeners stopped accepting new connections and finished
    /// draining every in-flight request before the forced-exit deadline.
    Drained,
    /// One of the two `axum::serve(..)` futures itself returned an error
    /// (e.g. the underlying `TcpListener::accept` failed).
    ServerError(std::io::Error),
    /// The task driving both servers panicked.
    ServerTaskPanicked(tokio::task::JoinError),
    /// [`FORCED_SHUTDOWN_TIMEOUT`] elapsed before the servers finished
    /// draining — `main` force-exits on this outcome.
    TimedOut,
}

/// Waits for `shutdown_signal` to resolve, fans that out to both listeners
/// via `shutdown_tx` (see the doc comment on `main`'s broadcast channel setup
/// for why a `broadcast` channel rather than a plain `select!` against the
/// serve futures themselves), then gives `serve_both` up to `forced_timeout`
/// to finish draining before reporting [`ShutdownOutcome::TimedOut`].
///
/// Generic over `shutdown_signal` (rather than always being the real
/// [`shutdown_signal`] function) so tests can trigger shutdown deterministically
/// instead of sending the process a real OS signal.
async fn wait_for_shutdown<S>(
    shutdown_signal: S,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    serve_both: tokio::task::JoinHandle<Result<((), ()), std::io::Error>>,
    forced_timeout: Duration,
) -> ShutdownOutcome
where
    S: std::future::Future<Output = ()>,
{
    shutdown_signal.await;
    tracing::info!(
        drain_timeout_secs = forced_timeout.as_secs(),
        "shutdown signal received; no longer accepting new connections, draining in-flight requests"
    );
    let _ = shutdown_tx.send(());

    match tokio::time::timeout(forced_timeout, serve_both).await {
        Ok(Ok(Ok(((), ())))) => ShutdownOutcome::Drained,
        Ok(Ok(Err(error))) => ShutdownOutcome::ServerError(error),
        Ok(Err(join_error)) => ShutdownOutcome::ServerTaskPanicked(join_error),
        Err(_) => ShutdownOutcome::TimedOut,
    }
}

#[tokio::main]
async fn main() {
    let _log_guard = install_logging(&xgovernor_data_dir());

    let admin_bind_addr =
        std::env::var("XGOVERNOR_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:8787".to_string());
    let Ok(tenant_bind_addr) = std::env::var("XGOVERNOR_TENANT_BIND_ADDR") else {
        eprintln!(
            "refusing to start: XGOVERNOR_TENANT_BIND_ADDR is not set. xgovernor-server always \
             runs two listeners — an admin-only surface (XGOVERNOR_BIND_ADDR, must be loopback) \
             and a tenant-only surface (XGOVERNOR_TENANT_BIND_ADDR, may be public) — per \
             docs/tenancy_design.md §3.1. Set XGOVERNOR_TENANT_BIND_ADDR to the address tenant \
             traffic should reach (e.g. 0.0.0.0:8788)."
        );
        std::process::exit(1);
    };
    if !is_loopback_bind_addr(&admin_bind_addr) {
        eprintln!(
            "refusing to start: XGOVERNOR_BIND_ADDR={admin_bind_addr} is not loopback. It is the \
             admin-only surface (docs/tenancy_design.md §3.1) and must always bind \
             127.0.0.1/::1/localhost."
        );
        std::process::exit(1);
    }
    let Some(token_table) = TokenTable::from_env() else {
        eprintln!(
            "refusing to start: no XGOVERNOR_BEARER_TOKEN/XGOVERNOR_TENANT_TOKENS_JSON is \
             configured. Without a token table every request resolves to implicit admin, which \
             the tenant listener would then reject outright, silently bricking it. Configure \
             XGOVERNOR_TENANT_TOKENS_JSON (tenant entries) and, if the admin surface should be \
             reachable, XGOVERNOR_BEARER_TOKEN."
        );
        std::process::exit(1);
    };
    let default_workspace_root = std::env::var("XGOVERNOR_DEFAULT_WORKSPACE_ROOT")
        .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());

    let db_path = xgovernor_db_path();
    let session_repository = SqliteSessionRepository::open(&db_path).unwrap_or_else(|error| {
        eprintln!(
            "refusing to start: failed to open session repository at {}: {error}",
            db_path.display()
        );
        std::process::exit(1);
    });
    // `PiRuntime` now composes `xgovernor_manager::InstanceManager` the same
    // way `MockRuntime` (`apps/runtime-mock`) does — real provider-instance
    // ledger, startup reconciliation, quota enforcement, pending-release
    // retry loop, one `InstanceManager` per provider `backend_id` it is
    // willing to route to. `main.rs` builds those `InstanceManager`s itself
    // (`build_local_instance_manager`/`build_e2b_instance_manager` above)
    // rather than composing `MockRuntime`, since `PiRuntime::new` needs the
    // bare `Arc<InstanceManager>`s, not another `RuntimeAdapter` layered on
    // top of them.
    //
    // What actually changed, and what didn't: Pi's tool *execution* (file
    // read/write/edit, exec, grep/glob) no longer touches this host's
    // filesystem directly — it is routed through the selected
    // `InstanceManager`'s attached `OperationBackend` via a small local HTTP
    // bridge (`apps/runtime-pi/src/bridge.rs`) that a TypeScript Pi
    // extension calls instead of Pi's built-in tools reaching the host fs.
    // Each session's `ext.runtime_pi.backend_id` (`"local"` or `"e2b"`)
    // picks which `InstanceManager` in the map below provisions its sandbox.
    // The one remaining, narrower deviation from
    // `docs/runtime_adapter_guide.md` §2/§5 and `docs/protocol_boundaries.md`
    // §5 point 4 is the RPC *control channel* itself: the long-lived,
    // interactive, line-delimited JSON dialog with the `pi` process's
    // stdin/stdout still goes through `tokio::process` directly on this
    // host, not through `operation-protocol` (which is a one-shot,
    // fully-buffered request/response contract and cannot drive a
    // persistent bidirectional stdio conversation). See
    // `apps/runtime-pi/src/lib.rs`'s module doc and
    // `apps/runtime-pi/demo/easydemo.md` for the full picture.
    //
    // `e2b` support is optional at startup: constructing an `InstanceManager`
    // over `E2bProvider` never itself fails (it takes no config), but every
    // sandbox it would create needs `E2B_API_KEY` at `create` time, so this
    // binary checks for it up front and simply omits `"e2b"` from the map
    // (logging why) rather than failing the whole server to start — the
    // `local` backend must keep working for `apps/runtime-pi/demo/easydemo.md`'s existing
    // demo flow regardless of whether e2b is configured.
    let local_manager = build_local_instance_manager(&db_path);
    let _local_retry_loop = reconcile_and_spawn_retry_loop("local", &local_manager).await;

    let mut runtime_managers: HashMap<String, Arc<InstanceManager>> = HashMap::new();
    runtime_managers.insert("local".to_string(), local_manager);

    let e2b_configured = std::env::var(E2B_API_KEY_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .is_some();
    let _e2b_retry_loop = if e2b_configured {
        let e2b_manager = build_e2b_instance_manager(&db_path);
        let retry_loop = reconcile_and_spawn_retry_loop("e2b", &e2b_manager).await;
        runtime_managers.insert("e2b".to_string(), e2b_manager);
        Some(retry_loop)
    } else {
        tracing::info!(
            env_var = E2B_API_KEY_ENV,
            "not set; Pi sessions requesting ext.runtime_pi.backend_id \"e2b\" will be \
             rejected (local-only mode)"
        );
        None
    };

    // Snapshot the configured `backend_id`s before `PiRuntime::new` moves
    // the map — `PiSessionEnvironment` must admit exactly the backends the
    // runtime can actually provision (and no others), so the two stay in
    // lockstep by construction.
    let configured_backend_ids: Vec<String> = runtime_managers.keys().cloned().collect();
    let runtime = PiRuntime::new(runtime_managers, xgovernor_pi_session_root()).unwrap_or_else(
        |error| {
            eprintln!("refusing to start: failed to initialize PiRuntime bridge: {error}");
            std::process::exit(1);
        },
    );

    let lease_table = Arc::new(SessionLeaseTable::new());
    let application = SessionApplication::new(
        Arc::new(runtime),
        Arc::new(session_repository),
        Arc::new(SystemClockAndUuidIds),
        Arc::new(SystemClockAndUuidIds),
        Arc::new(PiSessionEnvironment::new(
            default_workspace_root,
            configured_backend_ids,
        )),
        Arc::new(SystemClockAndUuidIds),
    )
    .with_lease_table(lease_table.clone());

    // Component C (docs/session_orchestration_skeleton.md): force-close any
    // session whose lease has carried no live heartbeat for over 2h, so a
    // client that crashes/disappears without detach/close doesn't leak a
    // sandbox forever. Detached from the request path on purpose — dropping
    // the returned JoinHandle does not stop it (see xgovernor_core::
    // spawn_orphan_reaper's doc comment); it runs for the life of this
    // process. Deliberately NOT aborted during graceful shutdown below: it's
    // a stateless periodic sweep with nothing to hand back to a client
    // (unlike an in-flight HTTP request), so there is no drain-completeness
    // reason to stop it early — it simply stops existing when the process
    // exits, same as before this task added shutdown handling at all.
    let _orphan_reaper = xgovernor_core::spawn_orphan_reaper(application.clone(), lease_table);

    // Each listener gets its own `SessionHttpState` (and therefore its own,
    // independent `streams` table of pending turn-event subscriptions) — see
    // `SessionHttpState::spawn_stream_sweeper`'s doc comment. Same
    // dropped-handle convention as `_orphan_reaper` above: the sweepers run
    // for the life of this process.
    let admin_state = SessionHttpState::new(application.clone());
    let _admin_stream_sweeper = admin_state.spawn_stream_sweeper();
    let admin_router = create_router(admin_state, Some(token_table.clone()), Some(Role::Admin));

    let tenant_state = SessionHttpState::new(application);
    let _tenant_stream_sweeper = tenant_state.spawn_stream_sweeper();
    let tenant_router = create_router(tenant_state, Some(token_table), Some(Role::Tenant));

    let admin_listener = tokio::net::TcpListener::bind(&admin_bind_addr)
        .await
        .unwrap_or_else(|error| panic!("failed to bind admin listener {admin_bind_addr}: {error}"));
    let tenant_listener = tokio::net::TcpListener::bind(&tenant_bind_addr)
        .await
        .unwrap_or_else(|error| {
            panic!("failed to bind tenant listener {tenant_bind_addr}: {error}")
        });
    tracing::info!(addr = %admin_bind_addr, "admin surface listening (loopback-only)");
    tracing::info!(addr = %tenant_bind_addr, "tenant surface listening");

    // One shared shutdown signal fans out to both listeners via a broadcast
    // channel (each `serve()` needs its own `Future`, and `Future`s aren't
    // `Clone`) — see `shutdown_signal`'s doc comment for why this must not
    // be a plain `tokio::select!` between the two `serve()` futures and the
    // signal itself.
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let admin_shutdown = {
        let mut rx = shutdown_tx.subscribe();
        async move {
            let _ = rx.recv().await;
        }
    };
    let tenant_shutdown = {
        let mut rx = shutdown_tx.subscribe();
        async move {
            let _ = rx.recv().await;
        }
    };

    let admin_serve =
        axum::serve(admin_listener, admin_router).with_graceful_shutdown(admin_shutdown);
    let tenant_serve =
        axum::serve(tenant_listener, tenant_router).with_graceful_shutdown(tenant_shutdown);

    // Spawned (not awaited inline) so the servers keep draining independently
    // of `wait_for_shutdown`'s `shutdown_signal.await` — awaiting them
    // directly in the same `select!`/`join!` as the signal would race the
    // signal against the servers themselves rather than against a bounded
    // drain window.
    let serve_both = tokio::spawn(async move { tokio::try_join!(admin_serve, tenant_serve) });

    match wait_for_shutdown(
        shutdown_signal(),
        shutdown_tx,
        serve_both,
        FORCED_SHUTDOWN_TIMEOUT,
    )
    .await
    {
        ShutdownOutcome::Drained => {
            tracing::info!("graceful shutdown complete; all listeners drained")
        }
        ShutdownOutcome::ServerError(error) => {
            tracing::error!(%error, "http server exited with an error during shutdown")
        }
        ShutdownOutcome::ServerTaskPanicked(join_error) => {
            tracing::error!(%join_error, "server task panicked during shutdown")
        }
        ShutdownOutcome::TimedOut => {
            tracing::error!(
                "graceful shutdown did not finish draining within {}s; forcing exit",
                FORCED_SHUTDOWN_TIMEOUT.as_secs()
            );
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is_loopback_bind_addr;

    #[test]
    fn recognizes_loopback_forms() {
        for addr in [
            "127.0.0.1:8787",
            "127.0.0.1",
            "localhost:8787",
            "127.5.5.5:9999",
            "[::1]:8787",
            "[::1]",
        ] {
            assert!(
                is_loopback_bind_addr(addr),
                "expected {addr} to be loopback"
            );
        }
    }

    #[test]
    fn rejects_non_loopback_forms() {
        for addr in [
            "0.0.0.0:8787",
            "192.168.1.5:8787",
            "[::]:8787",
            "example.com:8787",
        ] {
            assert!(
                !is_loopback_bind_addr(addr),
                "expected {addr} to not be loopback"
            );
        }
    }

    mod shutdown {
        use super::super::{wait_for_shutdown, ShutdownOutcome};
        use std::time::Duration;

        #[tokio::test]
        async fn drained_once_the_signal_fires_and_the_server_task_finishes_in_time() {
            let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
            let mut rx = shutdown_tx.subscribe();
            // Mirrors the real `serve_both` shape: a task that only resolves
            // once it has observed the fanned-out shutdown signal, standing
            // in for "the server finished draining."
            let serve_both = tokio::spawn(async move {
                let _ = rx.recv().await;
                Ok::<((), ()), std::io::Error>(((), ()))
            });

            let outcome = wait_for_shutdown(
                std::future::ready(()),
                shutdown_tx,
                serve_both,
                Duration::from_secs(5),
            )
            .await;
            assert!(matches!(outcome, ShutdownOutcome::Drained));
        }

        #[tokio::test]
        async fn server_error_surfaces_as_its_own_outcome_not_a_panic() {
            let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
            let mut rx = shutdown_tx.subscribe();
            let serve_both = tokio::spawn(async move {
                let _ = rx.recv().await;
                Err::<((), ()), std::io::Error>(std::io::Error::other("accept loop failed"))
            });

            let outcome = wait_for_shutdown(
                std::future::ready(()),
                shutdown_tx,
                serve_both,
                Duration::from_secs(5),
            )
            .await;
            assert!(matches!(outcome, ShutdownOutcome::ServerError(_)));
        }

        #[tokio::test]
        async fn a_panicking_server_task_surfaces_as_its_own_outcome() {
            let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
            let mut rx = shutdown_tx.subscribe();
            let serve_both = tokio::spawn(async move {
                let _ = rx.recv().await;
                panic!("simulated server task panic");
                #[allow(unreachable_code)]
                Ok::<((), ()), std::io::Error>(((), ()))
            });

            let outcome = wait_for_shutdown(
                std::future::ready(()),
                shutdown_tx,
                serve_both,
                Duration::from_secs(5),
            )
            .await;
            assert!(matches!(outcome, ShutdownOutcome::ServerTaskPanicked(_)));
        }

        #[tokio::test(start_paused = true)]
        async fn forces_a_timed_out_outcome_when_draining_outlasts_the_deadline() {
            // Paused clock: the spawned task's `sleep` and the `timeout`'s
            // internal timer both resolve near-instantly in wall-clock test
            // time (see the equivalent pattern/rationale in
            // httpserver::router::tests::slow_requests_are_cut_off_by_the_timeout_layer),
            // while still exercising the real race between "server still
            // draining" and "forced-exit deadline."
            let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
            let mut rx = shutdown_tx.subscribe();
            let forced_timeout = Duration::from_secs(1);
            let serve_both = tokio::spawn(async move {
                let _ = rx.recv().await;
                // Never finishes within forced_timeout -- simulates a stuck
                // drain (e.g. a client holding an SSE connection open
                // forever).
                tokio::time::sleep(forced_timeout + Duration::from_secs(30)).await;
                Ok::<((), ()), std::io::Error>(((), ()))
            });

            let outcome = wait_for_shutdown(
                std::future::ready(()),
                shutdown_tx,
                serve_both,
                forced_timeout,
            )
            .await;
            assert!(matches!(outcome, ShutdownOutcome::TimedOut));
        }

        /// Integration check: a *real* `axum::serve(..).with_graceful_shutdown(..)`
        /// wired to the same broadcast-channel pattern `main` uses (not just
        /// the synthetic `JoinHandle`s the tests above use), proving the
        /// actual plumbing -- not only `wait_for_shutdown`'s own
        /// orchestration logic -- behaves as intended: the server keeps
        /// accepting/serving until the shutdown signal is sent, and only
        /// then begins draining.
        #[tokio::test]
        async fn a_real_axum_serve_stays_up_until_the_broadcast_signal_and_then_drains() {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let router =
                axum::Router::new().route("/probe", axum::routing::get(|| async { "ok" }));

            let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
            let shutdown_rx = shutdown_tx.subscribe();
            let serve = axum::serve(listener, router).with_graceful_shutdown(async move {
                let mut rx = shutdown_rx;
                let _ = rx.recv().await;
            });
            let serve_handle = tokio::spawn(async move { serve.await.map(|()| ((), ())) });

            // No shutdown signal has been sent yet -- the server must still
            // be up.
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(
                !serve_handle.is_finished(),
                "server must stay up before shutdown is signaled"
            );

            let outcome = wait_for_shutdown(
                std::future::ready(()),
                shutdown_tx,
                serve_handle,
                Duration::from_secs(5),
            )
            .await;
            assert!(matches!(outcome, ShutdownOutcome::Drained));
        }
    }
}
