use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{path::PathBuf, process::Stdio};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use xgovernor_core::{RuntimeEvent, SessionDomainError};
use xiaoo_core::LoopStateSnapshot;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedLlm {
    pub(crate) provider: String,
    pub(crate) model: String,
    pub(crate) api_key_env: String,
    #[serde(default)]
    pub(crate) api_base: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkerResponse {
    Event { event: RuntimeEvent },
    State { loop_state: LoopStateSnapshot },
    Ready,
    Error { message: String },
}

#[derive(Serialize)]
struct PathBody {
    path: String,
}
#[derive(Deserialize)]
struct PathReply {
    path: String,
}
#[derive(Serialize)]
struct ResolveBody {
    raw_path: String,
    base: String,
    explicit_base: Option<String>,
}
#[derive(Serialize)]
struct TempBody {
    kind: String,
    preferred_parent: Option<String>,
    prefix: Option<String>,
    suffix: Option<String>,
}
#[derive(Deserialize)]
struct ContentReply {
    content_base64: String,
}
#[derive(Serialize)]
struct WriteBody {
    path: String,
    content_base64: String,
    mode: String,
}
#[derive(Deserialize)]
struct WriteReply {
    path: String,
    created: bool,
}
#[derive(Serialize)]
struct ExecBody {
    command: String,
    args: Vec<String>,
    shell: Option<String>,
    cwd: Option<String>,
    timeout_ms: Option<u64>,
    env: Option<Vec<(String, String)>>,
    extra: Option<Value>,
}
#[derive(Deserialize)]
struct ExecReply {
    stdout_base64: String,
    stderr_base64: String,
    exit_code: Option<i32>,
    timed_out: bool,
}
#[derive(Serialize)]
struct GlobBody {
    pattern: String,
    base_dir: Option<String>,
    limit: Option<usize>,
}
#[derive(Deserialize)]
struct PathsReply {
    paths: Vec<String>,
}
#[derive(Serialize)]
struct GrepBody {
    query: String,
    base_dir: String,
    include: Option<String>,
    mode: String,
    head_limit: Option<usize>,
}
#[derive(Deserialize)]
struct GrepReply {
    entries: Vec<String>,
}

fn transport_error(error: impl ToString) -> xiaoo_api::backend::OperationError {
    xiaoo_api::backend::OperationError::Transport {
        message: error.to_string(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub(crate) llm: PersistedLlm,
    pub(crate) loop_state: LoopStateSnapshot,
    pub(crate) bridge_url: String,
    pub(crate) bridge_token: String,
    pub(crate) backend_id: String,
    pub(crate) workspace_root: String,
    pub(crate) home_dir: Option<String>,
    pub(crate) supports_atomic_write: bool,
    pub(crate) supports_grep: bool,
}

pub struct HttpOperationBackend {
    client: reqwest::Client,
    base: String,
    token: String,
    backend_id: String,
    workspace_root: xiaoo_api::backend::BackendPath,
    home_dir: Option<xiaoo_api::backend::BackendPath>,
    capabilities: xiaoo_api::backend::OperationBackendCapabilities,
}

impl HttpOperationBackend {
    pub fn new(config: &WorkerConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            base: config.bridge_url.trim_end_matches('/').into(),
            token: config.bridge_token.clone(),
            backend_id: config.backend_id.clone(),
            workspace_root: xiaoo_api::backend::BackendPath(config.workspace_root.clone()),
            home_dir: config.home_dir.clone().map(xiaoo_api::backend::BackendPath),
            capabilities: xiaoo_api::backend::OperationBackendCapabilities {
                supports_atomic_write: config.supports_atomic_write,
                supports_grep: config.supports_grep,
                supports_export_file: false,
                supports_lsp: false,
            },
        }
    }
    async fn post<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<R, xiaoo_api::backend::OperationError> {
        let response = self
            .client
            .post(format!("{}{}", self.base, path))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(|e| xiaoo_api::backend::OperationError::Transport {
                message: e.to_string(),
            })?;
        if !response.status().is_success() {
            return Err(xiaoo_api::backend::OperationError::Transport {
                message: response.text().await.unwrap_or_default(),
            });
        }
        response
            .json()
            .await
            .map_err(|e| xiaoo_api::backend::OperationError::Transport {
                message: e.to_string(),
            })
    }
}

#[async_trait]
impl xiaoo_api::backend::OperationBackend for HttpOperationBackend {
    fn backend_id(&self) -> &str {
        &self.backend_id
    }
    fn capabilities(&self) -> xiaoo_api::backend::OperationBackendCapabilities {
        self.capabilities
    }
    fn paths(&self) -> &dyn xiaoo_api::backend::OperationPathResolver {
        self
    }
    fn files(&self) -> &dyn xiaoo_api::backend::OperationFileSystem {
        self
    }
    fn search(&self) -> &dyn xiaoo_api::backend::OperationSearch {
        self
    }
    fn exec(&self) -> &dyn xiaoo_api::backend::OperationExec {
        self
    }
    fn export(&self) -> &dyn xiaoo_api::backend::OperationExport {
        self
    }
    async fn shutdown(&self) -> Result<(), xiaoo_api::backend::OperationError> {
        Ok(())
    }
}

#[async_trait]
impl xiaoo_api::backend::OperationPathResolver for HttpOperationBackend {
    fn workspace_root(&self) -> &xiaoo_api::backend::BackendPath {
        &self.workspace_root
    }
    fn home_dir(&self) -> Option<&xiaoo_api::backend::BackendPath> {
        self.home_dir.as_ref()
    }
    async fn resolve_path(
        &self,
        request: xiaoo_api::backend::ResolvePathRequest,
    ) -> Result<xiaoo_api::backend::BackendPath, xiaoo_api::backend::OperationError> {
        let (base, explicit_base) = match request.base {
            xiaoo_api::backend::ResolveBase::WorkspaceRoot => ("workspace_root".into(), None),
            xiaoo_api::backend::ResolveBase::HomeDir => ("home_dir".into(), None),
            xiaoo_api::backend::ResolveBase::Explicit(path) => ("explicit".into(), Some(path.0)),
        };
        self.post(
            "/v1/resolve",
            &ResolveBody {
                raw_path: request.raw_path,
                base,
                explicit_base,
            },
        )
        .await
        .map(|r: PathReply| xiaoo_api::backend::BackendPath(r.path))
    }
}

#[async_trait]
impl xiaoo_api::backend::OperationFileSystem for HttpOperationBackend {
    async fn stat(
        &self,
        path: &xiaoo_api::backend::BackendPath,
    ) -> Result<xiaoo_api::backend::PathStat, xiaoo_api::backend::OperationError> {
        let r: Value = self
            .post(
                "/v1/stat",
                &PathBody {
                    path: path.0.clone(),
                },
            )
            .await?;
        Ok(xiaoo_api::backend::PathStat {
            exists: r.get("exists").and_then(Value::as_bool).unwrap_or(false),
            kind: None,
            size_bytes: r.get("size_bytes").and_then(Value::as_u64),
            modified_at: None,
        })
    }
    async fn read_bytes(
        &self,
        request: xiaoo_api::backend::ReadBytesRequest,
    ) -> Result<Vec<u8>, xiaoo_api::backend::OperationError> {
        let r: ContentReply = self
            .post(
                "/v1/read",
                &PathBody {
                    path: request.path.0,
                },
            )
            .await?;
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, r.content_base64)
            .map_err(transport_error)
    }
    async fn write_bytes(
        &self,
        request: xiaoo_api::backend::WriteBytesRequest,
    ) -> Result<xiaoo_api::backend::WriteBytesOutcome, xiaoo_api::backend::OperationError> {
        let mode = match request.mode {
            xiaoo_api::backend::WriteMode::Create => "create",
            xiaoo_api::backend::WriteMode::Overwrite => "overwrite",
            xiaoo_api::backend::WriteMode::AtomicOverwrite => "atomic_overwrite",
        };
        let r: WriteReply = self
            .post(
                "/v1/write",
                &WriteBody {
                    path: request.path.0,
                    content_base64: base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        request.content,
                    ),
                    mode: mode.into(),
                },
            )
            .await?;
        Ok(xiaoo_api::backend::WriteBytesOutcome {
            path: xiaoo_api::backend::BackendPath(r.path),
            created: r.created,
        })
    }
    async fn create_dir_all(
        &self,
        path: &xiaoo_api::backend::BackendPath,
    ) -> Result<(), xiaoo_api::backend::OperationError> {
        let _: Value = self
            .post(
                "/v1/mkdir",
                &PathBody {
                    path: path.0.clone(),
                },
            )
            .await?;
        Ok(())
    }
    async fn temp_path(
        &self,
        request: xiaoo_api::backend::TempPathRequest,
    ) -> Result<xiaoo_api::backend::BackendPath, xiaoo_api::backend::OperationError> {
        let kind = match request.kind {
            xiaoo_api::backend::TempPathKind::File => "file",
            xiaoo_api::backend::TempPathKind::Directory => "directory",
        };
        self.post(
            "/v1/temp",
            &TempBody {
                kind: kind.into(),
                preferred_parent: request.preferred_parent.map(|p| p.0),
                prefix: request.prefix,
                suffix: request.suffix,
            },
        )
        .await
        .map(|r: PathReply| xiaoo_api::backend::BackendPath(r.path))
    }
}

#[async_trait]
impl xiaoo_api::backend::OperationExec for HttpOperationBackend {
    fn default_shell(&self) -> Option<&str> {
        None
    }
    async fn exec(
        &self,
        request: xiaoo_api::backend::ExecRequest,
    ) -> Result<xiaoo_api::backend::ExecResult, xiaoo_api::backend::OperationError> {
        let r: ExecReply = self
            .post(
                "/v1/exec",
                &ExecBody {
                    command: request.command,
                    args: request.args,
                    shell: request.shell,
                    cwd: request.cwd.map(|p| p.0),
                    timeout_ms: request.timeout_ms,
                    env: request.env,
                    extra: request.extra,
                },
            )
            .await?;
        Ok(xiaoo_api::backend::ExecResult {
            stdout: base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                r.stdout_base64,
            )
            .map_err(transport_error)?,
            stderr: base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                r.stderr_base64,
            )
            .map_err(transport_error)?,
            exit_code: r.exit_code,
            timed_out: r.timed_out,
        })
    }
}

#[async_trait]
impl xiaoo_api::backend::OperationSearch for HttpOperationBackend {
    async fn glob(
        &self,
        request: xiaoo_api::backend::GlobRequest,
    ) -> Result<Vec<xiaoo_api::backend::BackendPath>, xiaoo_api::backend::OperationError> {
        self.post(
            "/v1/glob",
            &GlobBody {
                pattern: request.pattern,
                base_dir: request.base_dir.map(|p| p.0),
                limit: request.limit,
            },
        )
        .await
        .map(|r: PathsReply| {
            r.paths
                .into_iter()
                .map(xiaoo_api::backend::BackendPath)
                .collect()
        })
    }
    async fn grep(
        &self,
        request: xiaoo_api::backend::GrepRequest,
    ) -> Result<xiaoo_api::backend::GrepResult, xiaoo_api::backend::OperationError> {
        let mode = match request.mode {
            xiaoo_api::backend::GrepMode::FilesWithMatches => "files_with_matches",
            xiaoo_api::backend::GrepMode::Content => "content",
            xiaoo_api::backend::GrepMode::Count => "count",
        };
        self.post(
            "/v1/grep",
            &GrepBody {
                query: request.query,
                base_dir: request.base_dir.0,
                include: request.include,
                mode: mode.into(),
                head_limit: request.head_limit,
            },
        )
        .await
        .map(|r: GrepReply| xiaoo_api::backend::GrepResult { entries: r.entries })
    }
}

#[async_trait]
impl xiaoo_api::backend::OperationExport for HttpOperationBackend {
    async fn export_file(
        &self,
        _request: xiaoo_api::backend::ExportFileRequest,
    ) -> Result<xiaoo_api::backend::SharedExportedFileHandle, xiaoo_api::backend::OperationError>
    {
        Err(xiaoo_api::backend::OperationError::Unsupported {
            message: "export unavailable in worker bridge".into(),
        })
    }
}

pub async fn spawn_worker_process(
    executable: &PathBuf,
    config: &WorkerConfig,
) -> Result<(Child, ChildStdin, BufReader<ChildStdout>), SessionDomainError> {
    let config_json =
        serde_json::to_string(config).map_err(|error| SessionDomainError::Internal {
            message: format!("failed to serialize xiaoO worker config: {error}"),
            source: None,
        })?;
    let mut command = Command::new(executable);
    command
        .arg("--worker")
        .env("XGOVERNOR_XIAOO_WORKER_CONFIG", config_json)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|error| SessionDomainError::Unavailable {
            message: format!(
                "failed to spawn xiaoO worker '{}': {error}",
                executable.display()
            ),
        })?;
    let stdin = child.stdin.take().expect("xiaoO worker stdin was piped");
    let stdout = child.stdout.take().expect("xiaoO worker stdout was piped");
    let mut stdout = BufReader::new(stdout);
    let mut ready = String::new();
    stdout
        .read_line(&mut ready)
        .await
        .map_err(|error| SessionDomainError::Unavailable {
            message: format!("failed to read xiaoO worker readiness: {error}"),
        })?;
    match serde_json::from_str::<WorkerResponse>(ready.trim()) {
        Ok(WorkerResponse::Ready) => Ok((child, stdin, stdout)),
        Ok(WorkerResponse::Error { message }) => Err(SessionDomainError::Unavailable { message }),
        Ok(other) => Err(SessionDomainError::Unavailable {
            message: format!("xiaoO worker sent unexpected readiness response: {other:?}"),
        }),
        Err(error) => Err(SessionDomainError::Unavailable {
            message: format!("invalid xiaoO worker readiness response: {error}"),
        }),
    }
}
