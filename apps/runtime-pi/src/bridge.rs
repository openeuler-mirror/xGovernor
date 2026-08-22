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
        .with_state(bridge)
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

#[cfg(test)]
mod tests {
    use super::*;
    use operation_protocol::capability::exec::ExecResult;
    use operation_protocol::capability::export::ExportFileRequest;
    use operation_protocol::capability::filesystem::{
        TempPathKind, TempPathRequest, WriteBytesOutcome,
    };
    use operation_protocol::capability::path::{ResolveBase, ResolvePathRequest};
    use operation_protocol::capability::{
        OperationExec, OperationExport, OperationFileSystem, OperationPathResolver, OperationSearch,
    };
    use operation_protocol::{OperationBackendCapabilities, PathStat};
    use std::sync::Mutex as StdMutex;

    /// Minimal in-memory `OperationBackend` test double: enough to exercise
    /// every bridge endpoint without a real sandbox. `files`/`base_dir` map
    /// is keyed by the raw path string the test sends, no real filesystem
    /// involved.
    struct FakeBackend {
        workspace_root: BackendPath,
        files: StdMutex<HashMap<String, Vec<u8>>>,
    }

    impl FakeBackend {
        fn new(workspace_root: &str) -> Self {
            Self {
                workspace_root: BackendPath(workspace_root.to_string()),
                files: StdMutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl OperationPathResolver for FakeBackend {
        fn workspace_root(&self) -> &BackendPath {
            &self.workspace_root
        }
        fn home_dir(&self) -> Option<&BackendPath> {
            None
        }
        async fn resolve_path(
            &self,
            request: ResolvePathRequest,
        ) -> Result<BackendPath, OperationError> {
            let _ = request;
            unimplemented!("not exercised by bridge tests")
        }
    }
    // Silence unused-import warning for ResolveBase (kept for readability of
    // the trait signature above / potential future test use).
    #[allow(unused_imports)]
    use self::ResolveBase as _UnusedResolveBase;

    #[async_trait]
    impl OperationFileSystem for FakeBackend {
        async fn stat(&self, path: &BackendPath) -> Result<PathStat, OperationError> {
            let files = self.files.lock().unwrap();
            match files.get(&path.0) {
                Some(content) => Ok(PathStat {
                    exists: true,
                    kind: Some(PathKind::File),
                    size_bytes: Some(content.len() as u64),
                    modified_at: Some(std::time::SystemTime::UNIX_EPOCH),
                }),
                None => Ok(PathStat {
                    exists: false,
                    kind: None,
                    size_bytes: None,
                    modified_at: None,
                }),
            }
        }

        async fn read_bytes(&self, request: ReadBytesRequest) -> Result<Vec<u8>, OperationError> {
            self.files
                .lock()
                .unwrap()
                .get(&request.path.0)
                .cloned()
                .ok_or_else(|| OperationError::NotFound {
                    path: request.path.0.clone(),
                })
        }

        async fn write_bytes(
            &self,
            request: WriteBytesRequest,
        ) -> Result<WriteBytesOutcome, OperationError> {
            let mut files = self.files.lock().unwrap();
            let created = !files.contains_key(&request.path.0);
            if request.mode == WriteMode::Create && !created {
                return Err(OperationError::AlreadyExists {
                    path: request.path.0.clone(),
                });
            }
            files.insert(request.path.0.clone(), request.content);
            Ok(WriteBytesOutcome {
                path: request.path,
                created,
            })
        }

        async fn create_dir_all(&self, _path: &BackendPath) -> Result<(), OperationError> {
            Ok(())
        }

        async fn temp_path(&self, request: TempPathRequest) -> Result<BackendPath, OperationError> {
            let _ = request;
            match request.kind {
                TempPathKind::File => Ok(BackendPath("/tmp/fake-file".to_string())),
                TempPathKind::Directory => Ok(BackendPath("/tmp/fake-dir".to_string())),
            }
        }
    }

    #[async_trait]
    impl OperationSearch for FakeBackend {
        async fn glob(&self, request: GlobRequest) -> Result<Vec<BackendPath>, OperationError> {
            let files = self.files.lock().unwrap();
            Ok(files
                .keys()
                .filter(|path| path.contains(&request.pattern))
                .map(|path| BackendPath(path.clone()))
                .collect())
        }

        async fn grep(
            &self,
            request: GrepRequest,
        ) -> Result<operation_protocol::capability::search::GrepResult, OperationError> {
            let files = self.files.lock().unwrap();
            let entries = files
                .iter()
                .filter(|(_, content)| String::from_utf8_lossy(content).contains(&request.query))
                .map(|(path, _)| path.clone())
                .collect();
            Ok(operation_protocol::capability::search::GrepResult { entries })
        }
    }

    #[async_trait]
    impl OperationExec for FakeBackend {
        async fn exec(&self, request: ExecRequest) -> Result<ExecResult, OperationError> {
            if request.command == "fail" {
                return Err(OperationError::ExecutionFailed {
                    message: "forced failure".to_string(),
                });
            }
            Ok(ExecResult {
                stdout: format!("ran {} {:?}", request.command, request.args).into_bytes(),
                stderr: Vec::new(),
                exit_code: Some(0),
                timed_out: false,
            })
        }
    }

    #[async_trait]
    impl OperationExport for FakeBackend {
        async fn export_file(
            &self,
            request: ExportFileRequest,
        ) -> Result<operation_protocol::SharedExportedFileHandle, OperationError> {
            let _ = request;
            unimplemented!("not exercised by bridge tests")
        }
    }

    #[async_trait]
    impl OperationBackend for FakeBackend {
        fn backend_id(&self) -> &str {
            "fake-bridge-backend"
        }
        fn capabilities(&self) -> OperationBackendCapabilities {
            OperationBackendCapabilities {
                supports_atomic_write: true,
                supports_grep: true,
                supports_export_file: false,
                supports_lsp: false,
            }
        }
        fn paths(&self) -> &dyn OperationPathResolver {
            self
        }
        fn files(&self) -> &dyn OperationFileSystem {
            self
        }
        fn search(&self) -> &dyn OperationSearch {
            self
        }
        fn exec(&self) -> &dyn OperationExec {
            self
        }
        fn export(&self) -> &dyn OperationExport {
            self
        }
        async fn shutdown(&self) -> Result<(), OperationError> {
            Ok(())
        }
    }

    async fn spawn_test_bridge() -> (Arc<Bridge>, String) {
        let bridge = Bridge::spawn().expect("bridge must spawn");
        let backend: Arc<dyn OperationBackend> = Arc::new(FakeBackend::new("/workspace"));
        let token = "test-token".to_string();
        bridge.register(
            token.clone(),
            backend,
            BackendPath("/workspace".to_string()),
            Arc::new(tokio::sync::RwLock::new(())),
        );
        (bridge, token)
    }

    fn client() -> reqwest::Client {
        reqwest::Client::new()
    }

    #[tokio::test]
    async fn workspace_root_returns_the_registered_path() {
        let (bridge, token) = spawn_test_bridge().await;
        let response = client()
            .post(format!("{}/v1/workspace-root", bridge.base_url()))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["path"], "/workspace");
    }

    #[tokio::test]
    async fn unknown_token_is_rejected_with_401() {
        let (bridge, _token) = spawn_test_bridge().await;
        let response = client()
            .post(format!("{}/v1/workspace-root", bridge.base_url()))
            .bearer_auth("not-a-real-token")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 401);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["error"]["kind"], "unauthorized");
    }

    #[tokio::test]
    async fn missing_authorization_header_is_rejected_with_401() {
        let (bridge, _token) = spawn_test_bridge().await;
        let response = client()
            .post(format!("{}/v1/workspace-root", bridge.base_url()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 401);
    }

    #[tokio::test]
    async fn write_then_read_round_trips_base64_content() {
        let (bridge, token) = spawn_test_bridge().await;
        let write_response = client()
            .post(format!("{}/v1/write", bridge.base_url()))
            .bearer_auth(&token)
            .json(&json!({
                "path": "/workspace/hello.txt",
                "content_base64": B64.encode(b"hello world"),
                "mode": "create",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(write_response.status(), 200);

        let read_response = client()
            .post(format!("{}/v1/read", bridge.base_url()))
            .bearer_auth(&token)
            .json(&json!({ "path": "/workspace/hello.txt" }))
            .send()
            .await
            .unwrap();
        assert_eq!(read_response.status(), 200);
        let body: serde_json::Value = read_response.json().await.unwrap();
        let content = B64
            .decode(body["content_base64"].as_str().unwrap())
            .unwrap();
        assert_eq!(content, b"hello world");
    }

    #[tokio::test]
    async fn stat_reports_exists_false_for_a_missing_path() {
        let (bridge, token) = spawn_test_bridge().await;
        let response = client()
            .post(format!("{}/v1/stat", bridge.base_url()))
            .bearer_auth(&token)
            .json(&json!({ "path": "/workspace/does-not-exist.txt" }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["exists"], false);
    }

    #[tokio::test]
    async fn read_of_a_missing_path_returns_404_not_found() {
        let (bridge, token) = spawn_test_bridge().await;
        let response = client()
            .post(format!("{}/v1/read", bridge.base_url()))
            .bearer_auth(&token)
            .json(&json!({ "path": "/workspace/does-not-exist.txt" }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 404);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["error"]["kind"], "not_found");
    }

    #[tokio::test]
    async fn mkdir_succeeds() {
        let (bridge, token) = spawn_test_bridge().await;
        let response = client()
            .post(format!("{}/v1/mkdir", bridge.base_url()))
            .bearer_auth(&token)
            .json(&json!({ "path": "/workspace/subdir" }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn exec_runs_and_returns_base64_stdout() {
        let (bridge, token) = spawn_test_bridge().await;
        let response = client()
            .post(format!("{}/v1/exec", bridge.base_url()))
            .bearer_auth(&token)
            .json(&json!({ "command": "echo", "args": ["hi"] }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let body: serde_json::Value = response.json().await.unwrap();
        let stdout = B64.decode(body["stdout_base64"].as_str().unwrap()).unwrap();
        assert!(String::from_utf8_lossy(&stdout).contains("echo"));
        assert_eq!(body["exit_code"], 0);
    }

    #[tokio::test]
    async fn exec_failure_maps_to_500_execution_failed() {
        let (bridge, token) = spawn_test_bridge().await;
        let response = client()
            .post(format!("{}/v1/exec", bridge.base_url()))
            .bearer_auth(&token)
            .json(&json!({ "command": "fail", "args": [] }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 500);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["error"]["kind"], "execution_failed");
    }

    #[tokio::test]
    async fn glob_and_grep_find_a_written_file() {
        let (bridge, token) = spawn_test_bridge().await;
        client()
            .post(format!("{}/v1/write", bridge.base_url()))
            .bearer_auth(&token)
            .json(&json!({
                "path": "/workspace/needle.txt",
                "content_base64": B64.encode(b"the needle is here"),
                "mode": "create",
            }))
            .send()
            .await
            .unwrap();

        let glob_response = client()
            .post(format!("{}/v1/glob", bridge.base_url()))
            .bearer_auth(&token)
            .json(&json!({ "pattern": "needle" }))
            .send()
            .await
            .unwrap();
        assert_eq!(glob_response.status(), 200);
        let glob_body: serde_json::Value = glob_response.json().await.unwrap();
        assert_eq!(glob_body["paths"].as_array().unwrap().len(), 1);

        let grep_response = client()
            .post(format!("{}/v1/grep", bridge.base_url()))
            .bearer_auth(&token)
            .json(&json!({
                "query": "needle is here",
                "base_dir": "/workspace",
                "mode": "files_with_matches",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(grep_response.status(), 200);
        let grep_body: serde_json::Value = grep_response.json().await.unwrap();
        assert_eq!(grep_body["entries"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn unregister_makes_the_token_unauthorized() {
        let (bridge, token) = spawn_test_bridge().await;
        bridge.unregister(&token);
        let response = client()
            .post(format!("{}/v1/workspace-root", bridge.base_url()))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 401);
    }
}
