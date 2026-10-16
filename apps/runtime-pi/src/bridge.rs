//! Local HTTP bridge that lets the Pi extension (TypeScript, running inside
//! the `pi` process itself) reach the real `operation_protocol::OperationBackend`
//! attached to a session's sandbox, instead of Pi's built-in tools touching
//! the daemon host's filesystem directly.
//!
//! One `Bridge` is shared across every `runtime_id` a `PiRuntime` manages
//! (see `Bridge::spawn`, called once per `PiRuntime`): it binds a single
//! `127.0.0.1:<ephemeral>` listener and multiplexes all sessions behind a
//! per-session bearer token (`Bridge::register`/`unregister`). The extension
//! learns `<base_url>`/`<token>` for its session via the
//! `XGOVERNOR_BRIDGE_URL`/`XGOVERNOR_BRIDGE_TOKEN` environment variables
//! `PiRuntime::start` sets on the spawned `pi` child process.
//!
//! Wire contract: project-internal JSON-over-HTTP, not `session-protocol` or
//! `operation-protocol`'s own wire types — this is this bridge's own local
//! shape, documented endpoint-by-endpoint below. All binary content is
//! base64-encoded.

use axum::extract::rejection::JsonRejection;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{async_trait, Json, Router};
use base64::Engine;
use operation_protocol::capability::exec::ExecRequest;
use operation_protocol::capability::filesystem::{ReadBytesRequest, WriteBytesRequest, WriteMode};
use operation_protocol::capability::search::{GlobRequest, GrepMode, GrepRequest};
use operation_protocol::{BackendPath, OperationBackend, OperationError, PathKind};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

/// What `Bridge::register` associates with a per-session bearer token: the
/// real backend to proxy operations to, and that session's workspace root
/// (returned verbatim by `POST /v1/workspace-root`).
struct SessionEntry {
    backend: Arc<dyn OperationBackend>,
    workspace_root: BackendPath,
    activity: Arc<tokio::sync::RwLock<()>>,
}

/// Shared local HTTP server proxying Pi's tool calls to a session's real
/// `OperationBackend`. See the module doc for the full picture.
pub struct Bridge {
    port: u16,
    sessions: RwLock<HashMap<String, SessionEntry>>,
}

impl Bridge {
    /// Binds `127.0.0.1:0` synchronously (so the assigned port is known
    /// immediately, without an async constructor) and spawns the axum server
    /// in the background. Meant to be called once per `PiRuntime`.
    pub fn spawn() -> std::io::Result<Arc<Self>> {
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        std_listener.set_nonblocking(true)?;
        let port = std_listener.local_addr()?.port();
        let listener = tokio::net::TcpListener::from_std(std_listener)?;

        let bridge = Arc::new(Bridge {
            port,
            sessions: RwLock::new(HashMap::new()),
        });

        let app = router(Arc::clone(&bridge));
        tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                tracing::error!(%error, "pi bridge http server exited unexpectedly");
            }
        });

        Ok(bridge)
    }

    /// Registers a new session's backend under `token`. Any previous entry
    /// for the same token is silently replaced (tokens are freshly generated
    /// per `start()` call, so a collision would only happen if a caller
    /// reused one, which is a caller bug — not this method's concern to
    /// detect).
    pub fn register(
        &self,
        token: String,
        backend: Arc<dyn OperationBackend>,
        workspace_root: BackendPath,
        activity: Arc<tokio::sync::RwLock<()>>,
    ) {
        self.sessions
            .write()
            .expect("Bridge sessions lock poisoned")
            .insert(
                token,
                SessionEntry {
                    backend,
                    workspace_root,
                    activity,
                },
            );
    }

    /// Removes a session's registration. No-op if `token` is unknown (e.g.
    /// already unregistered, or `start()` never got far enough to register
    /// one).
    pub fn unregister(&self, token: &str) {
        self.sessions
            .write()
            .expect("Bridge sessions lock poisoned")
            .remove(token);
    }

    /// Base URL the bridge is listening on, e.g. `http://127.0.0.1:54321`.
    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

fn router(bridge: Arc<Bridge>) -> Router {
    Router::new()
        .route("/v1/workspace-root", post(workspace_root_handler))
        .route("/v1/resolve", post(resolve_handler))
        .route("/v1/stat", post(stat_handler))
        .route("/v1/read", post(read_handler))
        .route("/v1/write", post(write_handler))
        .route("/v1/mkdir", post(mkdir_handler))
        .route("/v1/temp", post(temp_handler))
        .route("/v1/exec", post(exec_handler))
        .route("/v1/glob", post(glob_handler))
        .route("/v1/grep", post(grep_handler))
        .layer(axum::extract::DefaultBodyLimit::max(129 * 1024 * 1024))
        .layer(axum::middleware::from_fn(operation_context))
        .with_state(bridge)
}

async fn operation_context(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let turn = request
        .headers()
        .get("x-xgovernor-turn")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let operation = request
        .headers()
        .get("x-xgovernor-operation")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    match (turn, operation) {
        (Some(turn_id), Some(operation_id)) if !turn_id.is_empty() && !operation_id.is_empty() => {
            backend::execution::OPERATION_CONTEXT
                .scope(
                    operation_protocol::OperationContext {
                        turn_id,
                        operation_id,
                    },
                    next.run(request),
                )
                .await
        }
        _ => next.run(request).await,
    }
}

/// Extractor pulling `Authorization: Bearer <token>` out of the request and
/// resolving it against `Bridge`'s session map. Any failure here short
/// circuits the handler with a `401` in the same `{"error": {...}}` shape
/// every other error path on this bridge uses.
struct AuthedSession {
    backend: Arc<dyn OperationBackend>,
    workspace_root: BackendPath,
    activity: Arc<tokio::sync::RwLock<()>>,
}

#[async_trait]
impl FromRequestParts<Arc<Bridge>> for AuthedSession {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<Bridge>,
    ) -> Result<Self, Self::Rejection> {
        let token = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| unauthorized_response("missing or malformed Authorization header"))?;

        let sessions = state
            .sessions
            .read()
            .expect("Bridge sessions lock poisoned");
        let entry = sessions
            .get(token)
            .ok_or_else(|| unauthorized_response("unknown bearer token"))?;
        if entry.backend.backend_id() == "docker"
            && parts.uri.path() != "/v1/workspace-root"
            && backend::execution::OPERATION_CONTEXT
                .try_with(|_| ())
                .is_err()
        {
            return Err(bad_request_response(
                "missing Docker tool execution context".into(),
            ));
        }
        Ok(AuthedSession {
            backend: Arc::clone(&entry.backend),
            workspace_root: BackendPath(entry.workspace_root.0.clone()),
            activity: Arc::clone(&entry.activity),
        })
    }
}

fn unauthorized_response(message: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({ "error": { "kind": "unauthorized", "message": message } })),
    )
        .into_response()
}

fn bad_request_response(message: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": { "kind": "invalid_request", "message": message } })),
    )
        .into_response()
}

/// Maps a rejected `Json<T>` extraction (malformed body) to this bridge's
/// error shape instead of axum's default plaintext rejection body.
fn json_rejection_response(rejection: JsonRejection) -> Response {
    bad_request_response(rejection.to_string())
}

fn error_kind(error: &OperationError) -> &'static str {
    match error {
        OperationError::InvalidPath { .. } => "invalid_path",
        OperationError::NotFound { .. } => "not_found",
        OperationError::AlreadyExists { .. } => "already_exists",
        OperationError::NotDirectory { .. } => "not_directory",
        OperationError::NotFile { .. } => "not_file",
        OperationError::PermissionDenied { .. } => "permission_denied",
        OperationError::SandboxPolicyDenied { .. } => "sandbox_policy_denied",
        OperationError::Unsupported { .. } => "unsupported",
        OperationError::ExecutionFailed { .. } => "execution_failed",
        OperationError::ExecutionInterrupted { .. } => "execution_interrupted",
        OperationError::Transport { .. } => "transport",
    }
}

fn status_for_kind(kind: &str) -> StatusCode {
    match kind {
        "not_found" => StatusCode::NOT_FOUND,
        "already_exists" => StatusCode::CONFLICT,
        "invalid_path" | "not_directory" | "not_file" => StatusCode::BAD_REQUEST,
        "permission_denied" | "sandbox_policy_denied" => StatusCode::FORBIDDEN,
        "unsupported" => StatusCode::NOT_IMPLEMENTED,
        "execution_failed" | "execution_interrupted" => StatusCode::INTERNAL_SERVER_ERROR,
        "transport" => StatusCode::BAD_GATEWAY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Maps any `OperationError` to this bridge's `{"error": {"kind", "message"}}`
/// response shape, with the status mapping specified on the bridge's wire
/// contract.
fn operation_error_response(error: OperationError) -> Response {
    let kind = error_kind(&error);
    let status = status_for_kind(kind);
    let message = error.to_string();
    (
        status,
        Json(json!({ "error": { "kind": kind, "message": message } })),
    )
        .into_response()
}

// ---- POST /v1/workspace-root ----

async fn workspace_root_handler(session: AuthedSession) -> Response {
    Json(json!({ "path": session.workspace_root.0 })).into_response()
}

#[derive(Debug, Deserialize)]
struct ResolveRequestBody {
    raw_path: String,
    base: String,
    explicit_base: Option<String>,
}

async fn resolve_handler(
    session: AuthedSession,
    body: Result<Json<ResolveRequestBody>, JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(body) => body,
        Err(rejection) => return json_rejection_response(rejection),
    };
    let base = match body.base.as_str() {
        "workspace_root" => operation_protocol::capability::path::ResolveBase::WorkspaceRoot,
        "home_dir" => operation_protocol::capability::path::ResolveBase::HomeDir,
        "explicit" => match body.explicit_base {
            Some(path) => {
                operation_protocol::capability::path::ResolveBase::Explicit(BackendPath(path))
            }
            None => return bad_request_response("explicit_base is required".into()),
        },
        other => return bad_request_response(format!("unknown resolve base '{other}'")),
    };
    let _activity = session.activity.read().await;
    match session
        .backend
        .paths()
        .resolve_path(operation_protocol::capability::path::ResolvePathRequest {
            raw_path: body.raw_path,
            base,
        })
        .await
    {
        Ok(path) => Json(json!({ "path": path.0 })).into_response(),
        Err(error) => operation_error_response(error),
    }
}

// ---- POST /v1/stat ----

#[derive(Debug, Deserialize)]
struct StatRequestBody {
    path: String,
}

#[derive(Debug, Serialize)]
struct StatResponseBody {
    exists: bool,
    kind: Option<&'static str>,
    size_bytes: Option<u64>,
    modified_at_ms: Option<u128>,
}

fn path_kind_str(kind: PathKind) -> &'static str {
    match kind {
        PathKind::File => "file",
        PathKind::Directory => "directory",
        PathKind::Symlink => "symlink",
        PathKind::Other => "other",
    }
}

async fn stat_handler(
    session: AuthedSession,
    body: Result<Json<StatRequestBody>, JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(body) => body,
        Err(rejection) => return json_rejection_response(rejection),
    };
    let _activity = session.activity.read().await;
    match session.backend.files().stat(&BackendPath(body.path)).await {
        Ok(stat) => Json(StatResponseBody {
            exists: stat.exists,
            kind: stat.kind.map(path_kind_str),
            size_bytes: stat.size_bytes,
            modified_at_ms: stat.modified_at.map(|time| {
                time.duration_since(std::time::UNIX_EPOCH)
                    .map(|duration| duration.as_millis())
                    .unwrap_or(0)
            }),
        })
        .into_response(),
        Err(error) => operation_error_response(error),
    }
}

// ---- POST /v1/read ----

#[derive(Debug, Deserialize)]
struct ReadRequestBody {
    path: String,
}

async fn read_handler(
    session: AuthedSession,
    body: Result<Json<ReadRequestBody>, JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(body) => body,
        Err(rejection) => return json_rejection_response(rejection),
    };
    let _activity = session.activity.read().await;
    match session
        .backend
        .files()
        .read_bytes(ReadBytesRequest {
            path: BackendPath(body.path),
        })
        .await
    {
        Ok(content) => Json(json!({ "content_base64": B64.encode(content) })).into_response(),
        Err(error) => operation_error_response(error),
    }
}

// ---- POST /v1/write ----

#[derive(Debug, Deserialize)]
struct WriteRequestBody {
    path: String,
    content_base64: String,
    mode: String,
}

fn parse_write_mode(mode: &str) -> Result<WriteMode, String> {
    match mode {
        "create" => Ok(WriteMode::Create),
        "overwrite" => Ok(WriteMode::Overwrite),
        "atomic_overwrite" => Ok(WriteMode::AtomicOverwrite),
        other => Err(format!("unknown write mode '{other}'")),
    }
}

async fn write_handler(
    session: AuthedSession,
    body: Result<Json<WriteRequestBody>, JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(body) => body,
        Err(rejection) => return json_rejection_response(rejection),
    };
    let mode = match parse_write_mode(&body.mode) {
        Ok(mode) => mode,
        Err(message) => return bad_request_response(message),
    };
    let content = match B64.decode(body.content_base64) {
        Ok(content) => content,
        Err(error) => return bad_request_response(format!("invalid content_base64: {error}")),
    };
    let _activity = session.activity.read().await;
    match session
        .backend
        .files()
        .write_bytes(WriteBytesRequest {
            path: BackendPath(body.path),
            content,
            mode,
        })
        .await
    {
        Ok(outcome) => {
            Json(json!({ "path": outcome.path.0, "created": outcome.created })).into_response()
        }
        Err(error) => operation_error_response(error),
    }
}

#[derive(Debug, Deserialize)]
struct TempRequestBody {
    kind: String,
    preferred_parent: Option<String>,
    prefix: Option<String>,
    suffix: Option<String>,
}

async fn temp_handler(
    session: AuthedSession,
    body: Result<Json<TempRequestBody>, JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(body) => body,
        Err(rejection) => return json_rejection_response(rejection),
    };
    let kind = match body.kind.as_str() {
        "file" => operation_protocol::capability::filesystem::TempPathKind::File,
        "directory" => operation_protocol::capability::filesystem::TempPathKind::Directory,
        other => return bad_request_response(format!("unknown temp path kind '{other}'")),
    };
    let _activity = session.activity.read().await;
    match session
        .backend
        .files()
        .temp_path(
            operation_protocol::capability::filesystem::TempPathRequest {
                kind,
                preferred_parent: body.preferred_parent.map(BackendPath),
                prefix: body.prefix,
                suffix: body.suffix,
            },
        )
        .await
    {
        Ok(path) => Json(json!({ "path": path.0 })).into_response(),
        Err(error) => operation_error_response(error),
    }
}

// ---- POST /v1/mkdir ----

#[derive(Debug, Deserialize)]
struct MkdirRequestBody {
    path: String,
}

async fn mkdir_handler(
    session: AuthedSession,
    body: Result<Json<MkdirRequestBody>, JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(body) => body,
        Err(rejection) => return json_rejection_response(rejection),
    };
    let _activity = session.activity.read().await;
    match session
        .backend
        .files()
        .create_dir_all(&BackendPath(body.path))
        .await
    {
        Ok(()) => Json(json!({})).into_response(),
        Err(error) => operation_error_response(error),
    }
}

// ---- POST /v1/exec ----

#[derive(Debug, Deserialize)]
struct ExecRequestBody {
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    env: Option<HashMap<String, String>>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    shell: Option<String>,
    #[serde(default)]
    extra: Option<serde_json::Value>,
}

async fn exec_handler(
    session: AuthedSession,
    body: Result<Json<ExecRequestBody>, JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(body) => body,
        Err(rejection) => return json_rejection_response(rejection),
    };
    let _activity = session.activity.read().await;
    match session
        .backend
        .exec()
        .exec(ExecRequest {
            command: body.command,
            args: body.args,
            shell: body.shell,
            cwd: body.cwd.map(BackendPath),
            timeout_ms: body.timeout_ms,
            env: body.env.map(|map| map.into_iter().collect()),
            extra: body.extra,
        })
        .await
    {
        Ok(result) => Json(json!({
            "stdout_base64": B64.encode(result.stdout),
            "stderr_base64": B64.encode(result.stderr),
            "exit_code": result.exit_code,
            "timed_out": result.timed_out,
        }))
        .into_response(),
        Err(error) => operation_error_response(error),
    }
}

// ---- POST /v1/glob ----

#[derive(Debug, Deserialize)]
struct GlobRequestBody {
    pattern: String,
    #[serde(default)]
    base_dir: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

async fn glob_handler(
    session: AuthedSession,
    body: Result<Json<GlobRequestBody>, JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(body) => body,
        Err(rejection) => return json_rejection_response(rejection),
    };
    let _activity = session.activity.read().await;
    match session
        .backend
        .search()
        .glob(GlobRequest {
            pattern: body.pattern,
            base_dir: body.base_dir.map(BackendPath),
            limit: body.limit,
        })
        .await
    {
        Ok(paths) => Json(json!({
            "paths": paths.into_iter().map(|path| path.0).collect::<Vec<_>>(),
        }))
        .into_response(),
        Err(error) => operation_error_response(error),
    }
}

// ---- POST /v1/grep ----

#[derive(Debug, Deserialize)]
struct GrepRequestBody {
    query: String,
    base_dir: String,
    #[serde(default)]
    include: Option<String>,
    mode: String,
    #[serde(default)]
    head_limit: Option<usize>,
}

fn parse_grep_mode(mode: &str) -> Result<GrepMode, String> {
    match mode {
        "files_with_matches" => Ok(GrepMode::FilesWithMatches),
        "content" => Ok(GrepMode::Content),
        "count" => Ok(GrepMode::Count),
        other => Err(format!("unknown grep mode '{other}'")),
    }
}

async fn grep_handler(
    session: AuthedSession,
    body: Result<Json<GrepRequestBody>, JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(body) => body,
        Err(rejection) => return json_rejection_response(rejection),
    };
    let mode = match parse_grep_mode(&body.mode) {
        Ok(mode) => mode,
        Err(message) => return bad_request_response(message),
    };
    let _activity = session.activity.read().await;
    match session
        .backend
        .search()
        .grep(GrepRequest {
            query: body.query,
            base_dir: BackendPath(body.base_dir),
            include: body.include,
            mode,
            head_limit: body.head_limit,
        })
        .await
    {
        Ok(result) => Json(json!({ "entries": result.entries })).into_response(),
        Err(error) => operation_error_response(error),
    }
}
