use backend::e2b::E2bProvider;
use backend::local::LocalProvider;
use backend::{ProviderInstanceLedger, SqliteProviderInstanceLedger};
use provider_protocol::ProviderKind;
use std::collections::HashMap;
use std::io::IsTerminal;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use xgovernor_core::{
    Clock, Role, RuntimeIdGenerator, RuntimeRegistration, SessionApplication, SessionLeaseTable,
    SqliteSessionRepository, TurnIdGenerator,
};
use xgovernor_manager::{InstanceManager, InstanceManagerConfig};
use xgovernor_runtime_pi::{PiRuntime, PiSessionEnvironment};
use xgovernor_runtime_xiaoo::{XiaooRuntime, XiaooSessionEnvironment};
use xgovernor_server::{
    create_router, load_tenants_file, SessionHttpState, TenantAdminState, TenantConfigError,
    TokenTable,
};

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

fn open_provider_ledger(db_path: &Path, backend_id: &str) -> Arc<dyn ProviderInstanceLedger> {
    Arc::new(SqliteProviderInstanceLedger::open(db_path).unwrap_or_else(|error| {
        eprintln!(
            "refusing to start: failed to open {backend_id} provider-instance ledger at {}: {error}",
            db_path.display()
        );
        std::process::exit(1);
    }))
}

fn build_instance_manager(db_path: &Path, backend_id: &str) -> Arc<InstanceManager> {
    let ledger = open_provider_ledger(db_path, backend_id);
    let config = InstanceManagerConfig::new(
        DEFAULT_MAX_SANDBOXES_PER_OWNER,
        DEFAULT_MAX_SANDBOXES_GLOBAL,
    );
    match backend_id {
        "local" => {
            let provider = Arc::new(LocalProvider::new());
            Arc::new(InstanceManager::new(
                provider.clone(),
                provider,
                ledger,
                ProviderKind("local".into()),
                config,
            ))
        }
        "e2b" => {
            let provider = Arc::new(E2bProvider::new());
            Arc::new(InstanceManager::new(
                provider.clone(),
                provider,
                ledger,
                ProviderKind("e2b".into()),
                config,
            ))
        }
        _ => panic!("unknown backend id: {backend_id}"),
    }
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

fn xgovernor_db_path() -> std::path::PathBuf {
    xgovernor_data_dir().join("xgovernor.db")
}

/// Resolves the `tenants.toml` path (`docs/tenancy_design.md` §4) and
/// whether it was explicitly requested via `XGOVERNOR_TENANTS_CONFIG_PATH`
/// (as opposed to falling back to the default `$XGOVERNOR_DATA_DIR/tenants.toml`).
/// The distinction matters to [`load_token_table_at_startup`]: a *default*
/// path that doesn't exist means "auth not configured, run in dev mode"; an
/// *explicit* path that doesn't exist means "operator asked for a specific
/// file and it isn't there" — a fail-closed startup error, not a silent
/// downgrade to dev mode.
fn xgovernor_tenants_config_path() -> (std::path::PathBuf, bool) {
    if let Ok(path) = std::env::var("XGOVERNOR_TENANTS_CONFIG_PATH") {
        (std::path::PathBuf::from(path), true)
    } else {
        (xgovernor_data_dir().join("tenants.toml"), false)
    }
}

fn load_token_table_at_startup(path: &Path, path_is_explicit: bool) -> Option<TokenTable> {
    match load_tenants_file(path) {
        Ok(entries) if entries.is_empty() => {
            tracing::warn!(
                path = %path.display(),
                "tenants config file parsed but contains zero admin/tenant tokens; running in \
                 dev mode (every request resolves to implicit admin) — if this wasn't \
                 intentional, check for a stray empty [admin]/[[tenant]] block"
            );
            None
        }
        Ok(entries) => {
            tracing::info!(
                path = %path.display(),
                tokens = entries.len(),
                "loaded tenants config"
            );
            Some(TokenTable::new(entries))
        }
        Err(TenantConfigError::Read(io_error))
            if io_error.kind() == std::io::ErrorKind::NotFound && !path_is_explicit =>
        {
            tracing::info!(
                path = %path.display(),
                "no tenants config file at the default path; running in dev mode (every request \
                 resolves to implicit admin). Write a tenants.toml there, or set \
                 XGOVERNOR_TENANTS_CONFIG_PATH, to enable auth."
            );
            None
        }
        Err(error) => {
            eprintln!(
                "refusing to start: failed to load tenants config file {}: {error}",
                path.display()
            );
            std::process::exit(1);
        }
    }
}

#[cfg(unix)]
fn spawn_tenants_reload_task(
    table: TokenTable,
    path: std::path::PathBuf,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut signal = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        {
            Ok(signal) => signal,
            Err(error) => {
                tracing::error!(
                    %error,
                    "failed to install the SIGHUP signal handler; tenants config hot-reload \
                     is disabled for this run"
                );
                return;
            }
        };
        loop {
            signal.recv().await;
            match load_tenants_file(&path) {
                Ok(entries) if entries.is_empty() => {
                    tracing::error!(
                        path = %path.display(),
                        "SIGHUP reload produced zero tokens; refusing to apply it and keeping \
                         the last-good tenants config (to intentionally lock everyone out, stop \
                         the server rather than emptying this file)"
                    );
                }
                Ok(entries) => {
                    let tokens = entries.len();
                    table.reload(entries);
                    tracing::info!(
                        path = %path.display(),
                        tokens,
                        "reloaded tenants config on SIGHUP"
                    );
                }
                Err(error) => {
                    tracing::error!(
                        path = %path.display(),
                        %error,
                        "SIGHUP reload failed to read/parse/validate tenants config; keeping the \
                         last-good config"
                    );
                }
            }
        }
    })
}

#[cfg(not(unix))]
fn spawn_tenants_reload_task(
    _table: TokenTable,
    _path: std::path::PathBuf,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(std::future::pending())
}

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
            Some(
                tracing_subscriber::fmt::layer()
                    .with_writer(non_blocking)
                    .with_ansi(false),
            )
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
    if std::env::args_os().any(|arg| arg == "--worker") {
        if let Err(error) = xgovernor_runtime_xiaoo::worker::run_worker_from_env().await {
            eprintln!("xiaoo worker failed: {error}");
            std::process::exit(1);
        }
        return;
    }
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
    let (tenants_config_path, tenants_config_path_is_explicit) = xgovernor_tenants_config_path();
    let token_table =
        load_token_table_at_startup(&tenants_config_path, tenants_config_path_is_explicit);
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

    let local_manager = build_instance_manager(&db_path, "local");
    let _local_retry_loop = reconcile_and_spawn_retry_loop("local", &local_manager).await;

    let mut runtime_managers: HashMap<String, Arc<InstanceManager>> = HashMap::new();
    runtime_managers.insert("local".to_string(), local_manager);

    let e2b_configured = std::env::var(E2B_API_KEY_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .is_some();
    let _e2b_retry_loop = if e2b_configured {
        let e2b_manager = build_instance_manager(&db_path, "e2b");
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
    let pi_runtime = PiRuntime::new(runtime_managers.clone(), xgovernor_pi_session_root())
        .unwrap_or_else(|error| {
            eprintln!("refusing to start: failed to initialize PiRuntime bridge: {error}");
            std::process::exit(1);
        });
    let xiaoo_runtime = XiaooRuntime::new(runtime_managers);

    let lease_table = Arc::new(SessionLeaseTable::new());
    let application = SessionApplication::with_runtime_registry(
        "pi",
        [
            RuntimeRegistration::new(
                Arc::new(pi_runtime),
                Arc::new(PiSessionEnvironment::new(
                    default_workspace_root.clone(),
                    configured_backend_ids.clone(),
                )),
            ),
            RuntimeRegistration::new(
                Arc::new(xiaoo_runtime),
                Arc::new(XiaooSessionEnvironment::new(
                    default_workspace_root,
                    configured_backend_ids,
                )),
            ),
        ],
        Arc::new(session_repository),
        Arc::new(SystemClockAndUuidIds),
        Arc::new(SystemClockAndUuidIds),
        Arc::new(SystemClockAndUuidIds),
    )
    .unwrap_or_else(|error| {
        eprintln!("refusing to start: invalid runtime registry: {error}");
        std::process::exit(1);
    })
    .with_lease_table(lease_table.clone());
    application
        .restore_tenant_session_counts()
        .await
        .unwrap_or_else(|error| {
            eprintln!("refusing to start: failed to restore tenant session quotas: {error}");
            std::process::exit(1);
        });

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

    // Providers with a client-set idle timeout (e.g. E2B) can kill a sandbox
    // with no push notification to xGovernor — the platform just stops
    // answering. This sweep periodically re-verifies every active session's
    // runtime against its provider and force-closes the ones confirmed gone
    // as `SessionStatus::Closed`, so they don't sit stuck showing
    // idle/running forever (see `xgovernor_core::spawn_reclaim_sweeper`'s doc
    // comment). Same dropped-handle convention as `_orphan_reaper` above.
    let _reclaim_sweeper = xgovernor_core::spawn_reclaim_sweeper(application.clone());

    // Each listener gets its own `SessionHttpState` (and therefore its own,
    // independent `streams` table of pending turn-event subscriptions) — see
    // `SessionHttpState::spawn_stream_sweeper`'s doc comment. Same
    // dropped-handle convention as `_orphan_reaper` above: the sweepers run
    // for the life of this process.
    let tenant_admin_state = token_table.clone().map(|table| {
        TenantAdminState::new(table, tenants_config_path.clone(), application.clone())
    });

    let admin_state = SessionHttpState::new(application.clone());
    let _admin_stream_sweeper = admin_state.spawn_stream_sweeper();
    let admin_router = create_router(
        admin_state,
        token_table.clone(),
        Some(Role::Admin),
        tenant_admin_state,
    );

    let tenant_state = SessionHttpState::new(application);
    let _tenant_stream_sweeper = tenant_state.spawn_stream_sweeper();
    let tenant_router = create_router(tenant_state, token_table.clone(), Some(Role::Tenant), None);

    // SIGHUP-triggered tenants.toml hot reload — only wired up when the
    // server actually started with a real token table (see
    // spawn_tenants_reload_task's doc comment for why a dev-mode server gets
    // no reload capability).
    let _tenants_reload_task =
        token_table.map(|table| spawn_tenants_reload_task(table, tenants_config_path));

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
