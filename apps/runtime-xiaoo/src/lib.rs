//! xiaoO runtime adapter for xGovernor.
//!
//! This first integration intentionally keeps xGovernor's operation-plane
//! contracts separate from xiaoO's current contracts. The adapter owns that
//! compatibility boundary so a future shared crate does not leak into the
//! session protocol or application router.

use agent_contracts::interaction::InteractionHandle;
use agent_contracts::tool::{DiscoveredTool, ToolRegistryBuilder, ToolSource};
use agent_types::common::ids::{AgentId, ToolName};
use agent_types::context::TokenBudgetConfig;
use agent_types::interaction::{InteractionRequest, InteractionResponse};
use agent_types::outcome::AgentOutcome;
use agent_types::tool::{ToolRegistryConfig, ToolVisibilityConfig};
use async_trait::async_trait;
use compact::{build_context_manager, CompactionPolicy};
use operation_protocol::capability::exec::ExecRequest;
use provider_protocol::{BackendId, ProviderControlError};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use session_protocol::{
    SessionInteractionAnswer, SessionInteractionOption, SessionRuntimeCapability,
    SessionToolActivityPhase, SessionToolActivityStatus, SessionTurnOutcome, SessionUsage,
};
use std::collections::{BTreeSet, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use xgovernor_core::{
    enforce_workspace_axiom, CapabilityFamily, CheckpointPayload, IsolationBoundary,
    IsolationFacts, NetworkIsolation, NormalizedSessionEnvironment, OpaqueRuntimeState,
    ResolvedLlm, RuntimeAdapter, RuntimeEvent, RuntimeEventReceiver, RuntimeFailure,
    RuntimeInteractionInput, RuntimeLoadRequest, RuntimeStartRequest, RuntimeTurnInput,
    SandboxCapability, SecurityContext, SessionDomainError, SessionEnvironmentNormalizer,
    WorkspaceAccess,
};
use xgovernor_manager::InstanceManager;
use xiaoo_api::events::{LoopEndSummary, LoopEventSink, ToolResultEvent};
use xiaoo_api::llm::{resolve_config, resolve_model_context_length, ResolveInput};
use xiaoo_api::runtime::{Runtime, RuntimeInput, RuntimeOutput, RuntimeState};
use xiaoo_core::{EmptySkillRegistry, LoopStateSnapshot};

pub const EXT_NAMESPACE: &str = "xiaoo";
pub const LOCAL_BACKEND_ID: &str = "local";
pub const E2B_BACKEND_ID: &str = "e2b";
pub const STATE_SCHEMA_VERSION: u32 = 1;
const E2B_WORKSPACE_ROOT: &str = "/home/user/workspace";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct XiaooRuntimeExt {
    backend_id: String,
    provider: String,
    model: String,
    api_key_env: String,
    #[serde(default)]
    api_base: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedLlm {
    provider: String,
    model: String,
    api_key_env: String,
    #[serde(default)]
    api_base: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct XiaooPersistedState {
    backend_id: String,
    owner_ref: String,
    workspace_root: String,
    provider_options: Value,
    llm: PersistedLlm,
    loop_state: LoopStateSnapshot,
}

struct XiaooInstance {
    manager: Arc<InstanceManager>,
    backend: Arc<dyn xiaoo_api::backend::OperationBackend>,
    persisted: Mutex<XiaooPersistedState>,
    state: Mutex<Option<RuntimeState>>,
    active_turn: Mutex<Option<(String, CancellationToken)>>,
    pending_interactions: Arc<Mutex<HashMap<String, PendingInteraction>>>,
}

struct PendingInteraction {
    turn_id: String,
    response: oneshot::Sender<InteractionResponse>,
}

pub struct XiaooRuntime {
    managers: HashMap<String, Arc<InstanceManager>>,
    instances: RwLock<HashMap<String, Arc<XiaooInstance>>>,
}

impl XiaooRuntime {
    pub fn new(managers: HashMap<String, Arc<InstanceManager>>) -> Self {
        Self {
            managers,
            instances: RwLock::new(HashMap::new()),
        }
    }

    fn manager_for(&self, backend_id: &str) -> Result<Arc<InstanceManager>, SessionDomainError> {
        self.managers
            .get(backend_id)
            .cloned()
            .ok_or_else(|| SessionDomainError::InvalidRequest {
                message: format!("xiaoo backend_id '{backend_id}' is not configured"),
            })
    }

    async fn instance_for(
        &self,
        runtime_id: &str,
    ) -> Result<Arc<XiaooInstance>, SessionDomainError> {
        self.instances
            .read()
            .await
            .get(runtime_id)
            .cloned()
            .ok_or_else(|| SessionDomainError::NotFound {
                runtime_id: runtime_id.to_string(),
            })
    }
}

pub struct XiaooSessionEnvironment {
    default_local_root: String,
    configured_backends: BTreeSet<String>,
}

impl XiaooSessionEnvironment {
    pub fn new(
        default_local_root: impl Into<String>,
        configured_backends: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            default_local_root: default_local_root.into(),
            configured_backends: configured_backends.into_iter().collect(),
        }
    }
}

fn read_ext(
    ext: &session_protocol::SessionExtensions,
) -> Result<XiaooRuntimeExt, SessionDomainError> {
    let value = ext
        .get(EXT_NAMESPACE)
        .ok_or_else(|| SessionDomainError::InvalidRequest {
            message: "missing required 'ext.xiaoo' configuration".into(),
        })?;
    let parsed: XiaooRuntimeExt = serde_json::from_value(value.clone()).map_err(|error| {
        SessionDomainError::InvalidRequest {
            message: format!("invalid 'ext.xiaoo' configuration: {error}"),
        }
    })?;
    for (name, value) in [
        ("backend_id", parsed.backend_id.as_str()),
        ("provider", parsed.provider.as_str()),
        ("model", parsed.model.as_str()),
        ("api_key_env", parsed.api_key_env.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(SessionDomainError::InvalidRequest {
                message: format!("'ext.xiaoo.{name}' must not be empty"),
            });
        }
    }
    // Resolve now so open fails before a sandbox is provisioned. Only the
    // environment variable name is retained after this point.
    std::env::var(&parsed.api_key_env)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| SessionDomainError::InvalidRequest {
            message: format!(
                "environment variable '{}' required by xiaoO is missing or empty",
                parsed.api_key_env
            ),
        })?;
    resolve_config(ResolveInput {
        provider: Some(parsed.provider.clone()),
        api_key_env: Some(parsed.api_key_env.clone()),
        base_url: parsed.api_base.clone(),
        ..Default::default()
    })
    .map_err(|error| SessionDomainError::InvalidRequest {
        message: format!("invalid xiaoO LLM configuration: {error}"),
    })?;
    Ok(parsed)
}

#[async_trait]
impl SessionEnvironmentNormalizer for XiaooSessionEnvironment {
    async fn normalize(
        &self,
        ctx: &SecurityContext,
        request: &session_protocol::SessionOpenRequest,
    ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
        if request
            .llm
            .as_ref()
            .and_then(|llm| llm.api_key.as_ref())
            .is_some()
        {
            return Err(SessionDomainError::InvalidRequest {
                message: "xiaoO rejects inline api_key; use ext.xiaoo.api_key_env".into(),
            });
        }
        let ext = read_ext(&request.ext)?;
        if !self.configured_backends.contains(&ext.backend_id) {
            return Err(SessionDomainError::InvalidRequest {
                message: format!("xiaoo backend_id '{}' is not configured", ext.backend_id),
            });
        }
        let sandboxed = ext.backend_id == E2B_BACKEND_ID;
        enforce_workspace_axiom(ctx, &request.workspace, sandboxed)?;
        let (root, boundary, network, metadata) =
            match (&request.workspace, ext.backend_id.as_str()) {
                (session_protocol::WorkspaceSpec::DaemonDefault, LOCAL_BACKEND_ID) => (
                    self.default_local_root.clone(),
                    IsolationBoundary::Host,
                    NetworkIsolation::None,
                    Value::Null,
                ),
                (session_protocol::WorkspaceSpec::LocalPath { path }, LOCAL_BACKEND_ID) => (
                    path.clone(),
                    IsolationBoundary::Host,
                    NetworkIsolation::None,
                    Value::Null,
                ),
                (session_protocol::WorkspaceSpec::DaemonDefault, E2B_BACKEND_ID) => (
                    E2B_WORKSPACE_ROOT.into(),
                    IsolationBoundary::Remote,
                    NetworkIsolation::Restricted,
                    Value::Null,
                ),
                (
                    session_protocol::WorkspaceSpec::Git {
                        url,
                        reference,
                        subdirectory,
                    },
                    E2B_BACKEND_ID,
                ) => (
                    E2B_WORKSPACE_ROOT.into(),
                    IsolationBoundary::Remote,
                    NetworkIsolation::Restricted,
                    json!({"url": url, "reference": reference, "subdirectory": subdirectory}),
                ),
                (workspace, backend_id) => {
                    return Err(SessionDomainError::InvalidRequest {
                        message: format!(
                        "workspace {workspace:?} is not supported by xiaoO backend '{backend_id}'"
                    ),
                    })
                }
            };
        let mut sandbox = BTreeSet::from([
            SandboxCapability::Exec,
            SandboxCapability::FileRead,
            SandboxCapability::FileWrite,
        ]);
        if ext.backend_id == E2B_BACKEND_ID {
            sandbox.extend([SandboxCapability::Snapshot, SandboxCapability::Network]);
        }
        Ok(NormalizedSessionEnvironment {
            workspace: xgovernor_core::WorkspaceFacts {
                workspace_id: format!("xiaoo:{}", ext.backend_id),
                root,
                access: WorkspaceAccess::ReadWrite,
                revision: None,
                metadata,
            },
            isolation: IsolationFacts {
                boundary,
                workspace_access: WorkspaceAccess::ReadWrite,
                network,
                metadata: json!({"backend_id": ext.backend_id}),
            },
            sandbox_capabilities: sandbox,
            llm: Some(ResolvedLlm {
                provider: ext.provider,
                model: ext.model,
                api_base: ext.api_base,
                credential_source: format!("env:{}", ext.api_key_env),
            }),
            lease: None,
        })
    }
}

fn map_provider_error(error: ProviderControlError) -> SessionDomainError {
    match error {
        ProviderControlError::NotFound { resource_ref } => SessionDomainError::NotFound {
            runtime_id: resource_ref,
        },
        ProviderControlError::InvalidRequest { message } => {
            SessionDomainError::InvalidRequest { message }
        }
        other => SessionDomainError::Unavailable {
            message: other.to_string(),
        },
    }
}

fn map_operation_error(
    error: operation_protocol::OperationError,
) -> xiaoo_api::backend::OperationError {
    xiaoo_api::backend::OperationError::Transport {
        message: error.to_string(),
    }
}

struct OperationBridge {
    inner: Arc<dyn operation_protocol::OperationBackend>,
    workspace_root: xiaoo_api::backend::BackendPath,
    home_dir: Option<xiaoo_api::backend::BackendPath>,
}

impl OperationBridge {
    fn new(inner: Arc<dyn operation_protocol::OperationBackend>) -> Self {
        Self {
            workspace_root: xiaoo_api::backend::BackendPath(
                inner.paths().workspace_root().0.clone(),
            ),
            home_dir: inner
                .paths()
                .home_dir()
                .map(|path| xiaoo_api::backend::BackendPath(path.0.clone())),
            inner,
        }
    }
}

fn bridge_path(path: xiaoo_api::backend::BackendPath) -> operation_protocol::BackendPath {
    operation_protocol::BackendPath(path.0)
}

fn xiaoo_path(path: operation_protocol::BackendPath) -> xiaoo_api::backend::BackendPath {
    xiaoo_api::backend::BackendPath(path.0)
}

fn xiaoo_stat(stat: operation_protocol::PathStat) -> xiaoo_api::backend::PathStat {
    xiaoo_api::backend::PathStat {
        exists: stat.exists,
        kind: stat.kind.map(|kind| match kind {
            operation_protocol::PathKind::File => xiaoo_api::backend::PathKind::File,
            operation_protocol::PathKind::Directory => xiaoo_api::backend::PathKind::Directory,
            operation_protocol::PathKind::Symlink => xiaoo_api::backend::PathKind::Symlink,
            operation_protocol::PathKind::Other => xiaoo_api::backend::PathKind::Other,
        }),
        size_bytes: stat.size_bytes,
        modified_at: stat.modified_at,
    }
}

#[async_trait]
impl xiaoo_api::backend::OperationBackend for OperationBridge {
    fn backend_id(&self) -> &str {
        self.inner.backend_id()
    }
    fn capabilities(&self) -> xiaoo_api::backend::OperationBackendCapabilities {
        let caps = self.inner.capabilities();
        xiaoo_api::backend::OperationBackendCapabilities {
            supports_atomic_write: caps.supports_atomic_write,
            supports_grep: caps.supports_grep,
            supports_export_file: caps.supports_export_file,
            supports_lsp: false,
        }
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
impl xiaoo_api::backend::OperationPathResolver for OperationBridge {
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
        let base = match request.base {
            xiaoo_api::backend::ResolveBase::WorkspaceRoot => {
                operation_protocol::capability::path::ResolveBase::WorkspaceRoot
            }
            xiaoo_api::backend::ResolveBase::HomeDir => {
                operation_protocol::capability::path::ResolveBase::HomeDir
            }
            xiaoo_api::backend::ResolveBase::Explicit(path) => {
                operation_protocol::capability::path::ResolveBase::Explicit(bridge_path(path))
            }
        };
        self.inner
            .paths()
            .resolve_path(operation_protocol::capability::path::ResolvePathRequest {
                raw_path: request.raw_path,
                base,
            })
            .await
            .map(xiaoo_path)
            .map_err(map_operation_error)
    }
}

#[async_trait]
impl xiaoo_api::backend::OperationFileSystem for OperationBridge {
    async fn stat(
        &self,
        path: &xiaoo_api::backend::BackendPath,
    ) -> Result<xiaoo_api::backend::PathStat, xiaoo_api::backend::OperationError> {
        self.inner
            .files()
            .stat(&operation_protocol::BackendPath(path.0.clone()))
            .await
            .map(xiaoo_stat)
            .map_err(map_operation_error)
    }
    async fn read_bytes(
        &self,
        request: xiaoo_api::backend::ReadBytesRequest,
    ) -> Result<Vec<u8>, xiaoo_api::backend::OperationError> {
        self.inner
            .files()
            .read_bytes(
                operation_protocol::capability::filesystem::ReadBytesRequest {
                    path: bridge_path(request.path),
                },
            )
            .await
            .map_err(map_operation_error)
    }
    async fn write_bytes(
        &self,
        request: xiaoo_api::backend::WriteBytesRequest,
    ) -> Result<xiaoo_api::backend::WriteBytesOutcome, xiaoo_api::backend::OperationError> {
        let mode = match request.mode {
            xiaoo_api::backend::WriteMode::Create => {
                operation_protocol::capability::filesystem::WriteMode::Create
            }
            xiaoo_api::backend::WriteMode::Overwrite => {
                operation_protocol::capability::filesystem::WriteMode::Overwrite
            }
            xiaoo_api::backend::WriteMode::AtomicOverwrite => {
                operation_protocol::capability::filesystem::WriteMode::AtomicOverwrite
            }
        };
        self.inner
            .files()
            .write_bytes(
                operation_protocol::capability::filesystem::WriteBytesRequest {
                    path: bridge_path(request.path),
                    content: request.content,
                    mode,
                },
            )
            .await
            .map(|result| xiaoo_api::backend::WriteBytesOutcome {
                path: xiaoo_path(result.path),
                created: result.created,
            })
            .map_err(map_operation_error)
    }
    async fn create_dir_all(
        &self,
        path: &xiaoo_api::backend::BackendPath,
    ) -> Result<(), xiaoo_api::backend::OperationError> {
        self.inner
            .files()
            .create_dir_all(&operation_protocol::BackendPath(path.0.clone()))
            .await
            .map_err(map_operation_error)
    }
    async fn temp_path(
        &self,
        request: xiaoo_api::backend::TempPathRequest,
    ) -> Result<xiaoo_api::backend::BackendPath, xiaoo_api::backend::OperationError> {
        let kind = match request.kind {
            xiaoo_api::backend::TempPathKind::File => {
                operation_protocol::capability::filesystem::TempPathKind::File
            }
            xiaoo_api::backend::TempPathKind::Directory => {
                operation_protocol::capability::filesystem::TempPathKind::Directory
            }
        };
        self.inner
            .files()
            .temp_path(
                operation_protocol::capability::filesystem::TempPathRequest {
                    kind,
                    preferred_parent: request.preferred_parent.map(bridge_path),
                    prefix: request.prefix,
                    suffix: request.suffix,
                },
            )
            .await
            .map(xiaoo_path)
            .map_err(map_operation_error)
    }
}

#[async_trait]
impl xiaoo_api::backend::OperationExec for OperationBridge {
    fn default_shell(&self) -> Option<&str> {
        self.inner.exec().default_shell()
    }
    async fn exec(
        &self,
        request: xiaoo_api::backend::ExecRequest,
    ) -> Result<xiaoo_api::backend::ExecResult, xiaoo_api::backend::OperationError> {
        self.inner
            .exec()
            .exec(operation_protocol::capability::exec::ExecRequest {
                command: request.command,
                args: request.args,
                shell: request.shell,
                cwd: request.cwd.map(bridge_path),
                timeout_ms: request.timeout_ms,
                env: request.env,
                extra: request.extra,
            })
            .await
            .map(|result| xiaoo_api::backend::ExecResult {
                stdout: result.stdout,
                stderr: result.stderr,
                exit_code: result.exit_code,
                timed_out: result.timed_out,
            })
            .map_err(map_operation_error)
    }
}

#[async_trait]
impl xiaoo_api::backend::OperationSearch for OperationBridge {
    async fn glob(
        &self,
        request: xiaoo_api::backend::GlobRequest,
    ) -> Result<Vec<xiaoo_api::backend::BackendPath>, xiaoo_api::backend::OperationError> {
        self.inner
            .search()
            .glob(operation_protocol::capability::search::GlobRequest {
                pattern: request.pattern,
                base_dir: request.base_dir.map(bridge_path),
                limit: request.limit,
            })
            .await
            .map(|paths| paths.into_iter().map(xiaoo_path).collect())
            .map_err(map_operation_error)
    }
    async fn grep(
        &self,
        request: xiaoo_api::backend::GrepRequest,
    ) -> Result<xiaoo_api::backend::GrepResult, xiaoo_api::backend::OperationError> {
        let mode = match request.mode {
            xiaoo_api::backend::GrepMode::FilesWithMatches => {
                operation_protocol::capability::search::GrepMode::FilesWithMatches
            }
            xiaoo_api::backend::GrepMode::Content => {
                operation_protocol::capability::search::GrepMode::Content
            }
            xiaoo_api::backend::GrepMode::Count => {
                operation_protocol::capability::search::GrepMode::Count
            }
        };
        self.inner
            .search()
            .grep(operation_protocol::capability::search::GrepRequest {
                query: request.query,
                base_dir: bridge_path(request.base_dir),
                include: request.include,
                mode,
                head_limit: request.head_limit,
            })
            .await
            .map(|result| xiaoo_api::backend::GrepResult {
                entries: result.entries,
            })
            .map_err(map_operation_error)
    }
}

struct ExportHandleBridge {
    inner: operation_protocol::SharedExportedFileHandle,
    metadata: xiaoo_api::backend::ExportedFileMeta,
}

#[async_trait]
impl xiaoo_api::backend::ExportedFileHandle for ExportHandleBridge {
    fn metadata(&self) -> &xiaoo_api::backend::ExportedFileMeta {
        &self.metadata
    }
    async fn open_read(
        &self,
    ) -> Result<xiaoo_api::backend::ExportedFileReader, xiaoo_api::backend::OperationError> {
        self.inner.open_read().await.map_err(map_operation_error)
    }
}

#[async_trait]
impl xiaoo_api::backend::OperationExport for OperationBridge {
    async fn export_file(
        &self,
        request: xiaoo_api::backend::ExportFileRequest,
    ) -> Result<xiaoo_api::backend::SharedExportedFileHandle, xiaoo_api::backend::OperationError>
    {
        let handle = self
            .inner
            .export()
            .export_file(operation_protocol::capability::export::ExportFileRequest {
                path: bridge_path(request.path),
                preferred_name: request.preferred_name,
            })
            .await
            .map_err(map_operation_error)?;
        let meta = handle.metadata();
        Ok(Arc::new(ExportHandleBridge {
            metadata: xiaoo_api::backend::ExportedFileMeta {
                file_name: meta.file_name.clone(),
                size_bytes: meta.size_bytes,
                media_type: meta.media_type.clone(),
            },
            inner: handle,
        }))
    }
}

struct CoreToolSource {
    discovered: Vec<DiscoveredTool>,
}

impl CoreToolSource {
    fn new() -> Self {
        const ALLOWED: &[&str] = &[
            "bash",
            "file_read",
            "file_write",
            "file_edit",
            "glob",
            "grep",
            "ask_user_question",
        ];
        let discovered = tool::load_tool_sources_with_services(tool::ToolRuntimeServices {
            disable_plugin_tools: true,
            ..Default::default()
        })
        .into_iter()
        .flat_map(|source| source.discover())
        .filter(|tool| ALLOWED.contains(&tool.spec.name().0.as_str()))
        .collect();
        Self { discovered }
    }
}

impl ToolSource for CoreToolSource {
    fn discover(&self) -> Vec<DiscoveredTool> {
        self.discovered
            .iter()
            .map(|tool| DiscoveredTool {
                spec: Arc::clone(&tool.spec),
                executor: Arc::clone(&tool.executor),
            })
            .collect()
    }
}

struct GovernorInteractionHandle {
    turn_id: String,
    tx: mpsc::UnboundedSender<RuntimeEvent>,
    pending: Arc<Mutex<HashMap<String, PendingInteraction>>>,
}

#[async_trait]
impl InteractionHandle for GovernorInteractionHandle {
    async fn ask(&self, request: &InteractionRequest) -> InteractionResponse {
        let interaction_id = format!("interaction-{}", uuid::Uuid::new_v4());
        let (prompt, kind, options) = match request {
            InteractionRequest::Confirm { prompt, .. } => {
                (prompt.clone(), "confirm".to_string(), Vec::new())
            }
            InteractionRequest::TextInput { prompt, .. } => {
                (prompt.clone(), "text_input".to_string(), Vec::new())
            }
            InteractionRequest::Choice {
                prompt, options, ..
            } => (
                prompt.clone(),
                "choice".to_string(),
                options
                    .iter()
                    .map(|value| SessionInteractionOption {
                        id: value.clone(),
                        label: value.clone(),
                        description: None,
                        value: Value::String(value.clone()),
                    })
                    .collect(),
            ),
        };
        let (response_tx, response_rx) = oneshot::channel();
        self.pending.lock().await.insert(
            interaction_id.clone(),
            PendingInteraction {
                turn_id: self.turn_id.clone(),
                response: response_tx,
            },
        );
        if self
            .tx
            .send(RuntimeEvent::InteractionRequested {
                interaction_id: interaction_id.clone(),
                interaction_kind: kind,
                prompt,
                options,
                ext: Default::default(),
            })
            .is_err()
        {
            self.pending.lock().await.remove(&interaction_id);
            return cancelled_interaction_response(request);
        }
        match response_rx.await {
            Ok(response) => response,
            Err(_) => cancelled_interaction_response(request),
        }
    }
}

fn cancelled_interaction_response(request: &InteractionRequest) -> InteractionResponse {
    match request {
        InteractionRequest::Confirm { .. } => InteractionResponse::Confirmed { allowed: false },
        InteractionRequest::TextInput { .. } => InteractionResponse::Text {
            value: None,
            display_value: None,
        },
        InteractionRequest::Choice { .. } => InteractionResponse::Choice { value: None },
    }
}

async fn clone_git_workspace(
    backend: &dyn operation_protocol::OperationBackend,
    metadata: &Value,
    workspace_root: &str,
) -> Result<(), SessionDomainError> {
    let url = metadata.get("url").and_then(Value::as_str).ok_or_else(|| {
        SessionDomainError::InvalidRequest {
            message: "xiaoO git workspace metadata has no url".into(),
        }
    })?;
    let mut args = vec!["clone".to_string()];
    if let Some(reference) = metadata.get("reference").and_then(Value::as_str) {
        args.extend(["--branch".to_string(), reference.to_string()]);
    }
    args.extend([url.to_string(), workspace_root.to_string()]);
    let result = backend
        .exec()
        .exec(ExecRequest {
            command: "git".into(),
            args,
            timeout_ms: Some(120_000),
            ..Default::default()
        })
        .await
        .map_err(|error| SessionDomainError::Unavailable {
            message: format!("xiaoO git clone failed: {error}"),
        })?;
    if result.exit_code != Some(0) {
        return Err(SessionDomainError::Unavailable {
            message: format!(
                "xiaoO git clone exited {:?}: {}",
                result.exit_code,
                String::from_utf8_lossy(&result.stderr)
            ),
        });
    }
    Ok(())
}

fn state_to_opaque(state: &XiaooPersistedState) -> Result<OpaqueRuntimeState, SessionDomainError> {
    Ok(OpaqueRuntimeState {
        runtime_kind: "xiaoo".into(),
        schema_version: STATE_SCHEMA_VERSION,
        state: serde_json::to_value(state).map_err(|error| SessionDomainError::Internal {
            message: format!("failed to serialize xiaoO runtime state: {error}"),
            source: None,
        })?,
    })
}

fn state_from_opaque(
    state: &OpaqueRuntimeState,
) -> Result<XiaooPersistedState, SessionDomainError> {
    if state.runtime_kind != "xiaoo" || state.schema_version != STATE_SCHEMA_VERSION {
        return Err(SessionDomainError::InvalidRequest {
            message: format!(
                "unsupported xiaoO state kind/version: {}/{}",
                state.runtime_kind, state.schema_version
            ),
        });
    }
    serde_json::from_value(state.state.clone()).map_err(|error| {
        SessionDomainError::InvalidRequest {
            message: format!("invalid xiaoO runtime state: {error}"),
        }
    })
}

fn validate_persisted_llm(llm: &PersistedLlm) -> Result<(), SessionDomainError> {
    std::env::var(&llm.api_key_env)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| SessionDomainError::InvalidRequest {
            message: format!(
                "environment variable '{}' required by xiaoO is missing or empty",
                llm.api_key_env
            ),
        })?;
    resolve_config(ResolveInput {
        provider: Some(llm.provider.clone()),
        api_key_env: Some(llm.api_key_env.clone()),
        base_url: llm.api_base.clone(),
        ..Default::default()
    })
    .map(|_| ())
    .map_err(|error| SessionDomainError::InvalidRequest {
        message: format!("invalid persisted xiaoO LLM configuration: {error}"),
    })
}

async fn build_runtime(
    llm: &PersistedLlm,
    model_override: Option<&str>,
    backend: Arc<dyn xiaoo_api::backend::OperationBackend>,
) -> Result<Runtime, SessionDomainError> {
    let model = model_override.unwrap_or(&llm.model);
    let resolved = resolve_config(ResolveInput {
        provider: Some(llm.provider.clone()),
        api_key_env: Some(llm.api_key_env.clone()),
        base_url: llm.api_base.clone(),
        ..Default::default()
    })
    .map_err(|error| SessionDomainError::InvalidRequest {
        message: error.to_string(),
    })?;
    let context = resolve_model_context_length(&resolved, model)
        .await
        .map_err(|error| SessionDomainError::Unavailable {
            message: error.to_string(),
        })?
        .unwrap_or(128_000) as usize;
    let budget = TokenBudgetConfig {
        total_budget: context,
        reserved_for_output: context / 10,
        reserved_for_system: context / 20,
        hard_limit_ratio: 0.9,
    };
    let provider = Arc::new(
        llm_client::create_llm_provider_from_resolved(
            &resolved,
            model.to_string(),
            Some("xgovernor".into()),
            None,
        )
        .map_err(|error| SessionDomainError::InvalidRequest {
            message: error.to_string(),
        })?,
    );
    let compression = build_context_manager(None, Arc::clone(&provider)).map_err(|error| {
        SessionDomainError::Internal {
            message: error.to_string(),
            source: None,
        }
    })?;
    let anonymous = AgentId("anonymous".into());
    let allowed = [
        "bash",
        "file_read",
        "file_write",
        "file_edit",
        "glob",
        "grep",
        "ask_user_question",
    ]
    .into_iter()
    .map(|name| ToolName(name.into()))
    .collect();
    let registry = tool::ToolRegistryBuilderImpl::new()
        .with_sources(vec![Box::new(CoreToolSource::new())])
        .with_config(ToolRegistryConfig {
            visibility: ToolVisibilityConfig {
                per_agent_allowed_tools: [(anonymous, allowed)].into_iter().collect(),
            },
        })
        .build()
        .map_err(|error| SessionDomainError::Internal {
            message: format!("failed to build xiaoO core tool registry: {error}"),
            source: None,
        })?;
    Runtime::builder()
        .llm_provider(provider)
        .compression_pipeline(compression)
        .prompt_builder(Arc::new(prompt::PromptBuilderImpl::new()))
        .system_prompt("You are xiaoO, a coding assistant managed by xGovernor.")
        .tool_registry(Arc::from(registry))
        .skill_registry(Arc::new(EmptySkillRegistry::new()))
        .operation_backend(backend)
        .token_budget_config(budget.clone())
        .token_budget_policy(Arc::new(CompactionPolicy::from_budget(&budget)))
        .build()
        .map_err(|error| SessionDomainError::Internal {
            message: error.to_string(),
            source: None,
        })
}

struct GovernorEventSink {
    tx: mpsc::UnboundedSender<RuntimeEvent>,
    output: std::sync::Mutex<(String, u64)>,
    reasoning: std::sync::Mutex<String>,
}

impl GovernorEventSink {
    fn new(tx: mpsc::UnboundedSender<RuntimeEvent>) -> Self {
        Self {
            tx,
            output: std::sync::Mutex::new((String::new(), 0)),
            reasoning: std::sync::Mutex::new(String::new()),
        }
    }

    fn try_send(&self, event: RuntimeEvent) {
        let _ = self.tx.send(event);
    }
}

impl LoopEventSink for GovernorEventSink {
    fn on_turn_start(&self, _agent_id: &agent_types::common::ids::AgentId, _turn: u32) {}

    fn on_assistant_message(&self, _agent_id: &agent_types::common::ids::AgentId, text: &str) {
        let mut output = self.output.lock().expect("output snapshot lock poisoned");
        let delta = text.strip_prefix(&output.0).unwrap_or(text).to_string();
        output.0 = text.to_string();
        if !delta.is_empty() {
            output.1 += 1;
            self.try_send(RuntimeEvent::OutputDelta {
                stream_id: "assistant".into(),
                sequence: output.1,
                delta,
            });
        }
    }

    fn on_assistant_reasoning(&self, _agent_id: &agent_types::common::ids::AgentId, text: &str) {
        let mut previous = self
            .reasoning
            .lock()
            .expect("reasoning snapshot lock poisoned");
        let delta = text
            .strip_prefix(previous.as_str())
            .unwrap_or(text)
            .to_string();
        *previous = text.to_string();
        if !delta.is_empty() {
            self.try_send(RuntimeEvent::Extension {
                namespace: "xiaoo".into(),
                payload: json!({"kind": "reasoning_delta", "delta": delta}),
            });
        }
    }

    fn on_tool_result(
        &self,
        _agent_id: &agent_types::common::ids::AgentId,
        event: &ToolResultEvent,
    ) {
        self.try_send(RuntimeEvent::ToolActivity {
            activity_id: event.call_id.clone(),
            phase: SessionToolActivityPhase::End,
            name: event.tool_name.clone(),
            status: if event.is_error {
                SessionToolActivityStatus::Failed
            } else {
                SessionToolActivityStatus::Succeeded
            },
            summary: Some(event.output_preview.clone()),
            ext: [("xiaoo".into(), json!({"args_preview": event.args_preview}))]
                .into_iter()
                .collect(),
        });
    }

    fn on_loop_end(
        &self,
        _agent_id: &agent_types::common::ids::AgentId,
        _summary: &LoopEndSummary,
    ) {
    }
}

fn usage_from_outcome(outcome: &AgentOutcome) -> SessionUsage {
    let usage = match outcome {
        AgentOutcome::Complete { token_usage, .. }
        | AgentOutcome::MaxTurnsReached { token_usage, .. }
        | AgentOutcome::BudgetExhausted { token_usage, .. }
        | AgentOutcome::Cancelled { token_usage, .. } => token_usage,
    };
    SessionUsage {
        input_tokens: usage.prompt_tokens as u64,
        output_tokens: usage.completion_tokens as u64,
        total_tokens: usage.total_tokens as u64,
    }
}

#[async_trait]
impl RuntimeAdapter for XiaooRuntime {
    fn kind(&self) -> &str {
        "xiaoo"
    }

    fn capabilities(&self) -> BTreeSet<SessionRuntimeCapability> {
        let mut values = BTreeSet::from([
            SessionRuntimeCapability::Interaction,
            SessionRuntimeCapability::StateExport,
            SessionRuntimeCapability::ModelOverride,
            SessionRuntimeCapability::ReasoningControl,
        ]);
        if self.managers.contains_key(E2B_BACKEND_ID) {
            values.insert(SessionRuntimeCapability::Checkpoint);
        }
        values
    }

    fn capabilities_for_request(
        &self,
        request: &session_protocol::SessionOpenRequest,
    ) -> BTreeSet<SessionRuntimeCapability> {
        let mut values = self.capabilities();
        let backend_id = request
            .ext
            .get(EXT_NAMESPACE)
            .and_then(|value| value.get("backend_id"))
            .and_then(Value::as_str);
        if backend_id != Some(E2B_BACKEND_ID) {
            values.remove(&SessionRuntimeCapability::Checkpoint);
        }
        values
    }

    async fn start(&self, request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
        if self
            .instances
            .read()
            .await
            .contains_key(&request.runtime_id)
        {
            return Err(SessionDomainError::Conflict {
                message: format!("runtime '{}' is already started", request.runtime_id),
            });
        }
        let (persisted, manager, backend) = if let Some(state) = request.state.as_ref() {
            let persisted = state_from_opaque(state)?;
            validate_persisted_llm(&persisted.llm)?;
            let manager = self.manager_for(&persisted.backend_id)?;
            let backend = manager
                .backend_for(&request.runtime_id)
                .map_err(map_provider_error)?;
            (persisted, manager, backend)
        } else {
            if request
                .llm
                .as_ref()
                .and_then(|llm| llm.api_key.as_ref())
                .is_some()
            {
                return Err(SessionDomainError::InvalidRequest {
                    message: "xiaoO rejects inline api_key".into(),
                });
            }
            let ext = read_ext(&request.ext)?;
            let manager = self.manager_for(&ext.backend_id)?;
            let provider_options = json!({
                "workspace_root": request.workspace.root,
                "allow_internet_access": ext.backend_id == E2B_BACKEND_ID,
            });
            let backend = manager
                .start_instance(
                    request.runtime_id.clone(),
                    BackendId(ext.backend_id.clone()),
                    request.owner_ref.clone(),
                    provider_options.clone(),
                )
                .await
                .map_err(map_provider_error)?;
            if request.workspace.metadata != Value::Null {
                if let Err(error) = clone_git_workspace(
                    backend.as_ref(),
                    &request.workspace.metadata,
                    &request.workspace.root,
                )
                .await
                {
                    let _ = manager.stop_instance(&request.runtime_id).await;
                    return Err(error);
                }
            }
            let initial = RuntimeState::new(request.conversation_id.clone()).to_snapshot();
            let backend = manager
                .backend_for(&request.runtime_id)
                .map_err(map_provider_error)?;
            (
                XiaooPersistedState {
                    backend_id: ext.backend_id,
                    owner_ref: request.owner_ref,
                    workspace_root: request.workspace.root,
                    provider_options,
                    llm: PersistedLlm {
                        provider: ext.provider,
                        model: ext.model,
                        api_key_env: ext.api_key_env,
                        api_base: ext.api_base,
                    },
                    loop_state: initial,
                },
                manager,
                backend,
            )
        };
        let backend: Arc<dyn xiaoo_api::backend::OperationBackend> =
            Arc::new(OperationBridge::new(backend));
        let state =
            RuntimeState::from_snapshot(persisted.loop_state.clone(), CancellationToken::new());
        self.instances.write().await.insert(
            request.runtime_id,
            Arc::new(XiaooInstance {
                manager,
                backend,
                persisted: Mutex::new(persisted),
                state: Mutex::new(Some(state)),
                active_turn: Mutex::new(None),
                pending_interactions: Arc::new(Mutex::new(HashMap::new())),
            }),
        );
        Ok(())
    }

    async fn stop(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
        let instance = self
            .instances
            .write()
            .await
            .remove(runtime_id)
            .ok_or_else(|| SessionDomainError::NotFound {
                runtime_id: runtime_id.into(),
            })?;
        if let Some((_, cancel)) = instance.active_turn.lock().await.take() {
            cancel.cancel();
        }
        instance.pending_interactions.lock().await.clear();
        instance
            .manager
            .stop_instance(runtime_id)
            .await
            .map_err(map_provider_error)
    }

    async fn attach(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
        self.instance_for(runtime_id).await.map(|_| ())
    }

    async fn submit_turn(
        &self,
        input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, SessionDomainError> {
        if input
            .llm
            .as_ref()
            .and_then(|llm| llm.api_key.as_ref())
            .is_some()
        {
            return Err(SessionDomainError::InvalidRequest {
                message: "xiaoO rejects inline api_key".into(),
            });
        }
        if let Some(llm) = input.llm.as_ref() {
            if llm.provider.is_some() || llm.api_base.is_some() || llm.api_key_env.is_some() {
                return Err(SessionDomainError::InvalidRequest {
                    message: "xiaoO turn overrides may set only 'model'".into(),
                });
            }
        }
        let reasoning = input.reasoning_effort.as_deref().unwrap_or("off");
        let effort = agent_types::ReasoningEffort::from_str(reasoning)
            .map_err(|error| SessionDomainError::InvalidRequest { message: error })?;
        let instance = self.instance_for(&input.runtime_id).await?;
        let cancel = CancellationToken::new();
        {
            let mut active = instance.active_turn.lock().await;
            if let Some((turn, _)) = active.as_ref() {
                return Err(SessionDomainError::Conflict {
                    message: format!("xiaoO turn '{turn}' is already active"),
                });
            }
            *active = Some((input.turn_id.clone(), cancel.clone()));
        }
        let persisted = instance.persisted.lock().await.clone();
        let model_override = input.llm.as_ref().and_then(|llm| llm.model.as_deref());
        let runtime = match build_runtime(
            &persisted.llm,
            model_override,
            Arc::clone(&instance.backend),
        )
        .await
        {
            Ok(runtime) => runtime,
            Err(error) => {
                instance.active_turn.lock().await.take();
                return Err(error);
            }
        };
        let mut state =
            instance
                .state
                .lock()
                .await
                .take()
                .ok_or_else(|| SessionDomainError::Conflict {
                    message: "xiaoO runtime state is busy".into(),
                })?;
        state.cancel = cancel;
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let (tx, rx) = mpsc::channel(64);
        let sink = Arc::new(GovernorEventSink::new(event_tx.clone()));
        let interaction = Arc::new(GovernorInteractionHandle {
            turn_id: input.turn_id.clone(),
            tx: event_tx.clone(),
            pending: Arc::clone(&instance.pending_interactions),
        });
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
        tokio::spawn(async move {
            let result = runtime
                .run(
                    &mut state,
                    RuntimeInput::new(input.text)
                        .with_event_sink(sink)
                        .with_interaction(interaction)
                        .with_reasoning_effort(effort),
                )
                .await;
            let event = match result {
                Ok(RuntimeOutput::Complete(outcome)) => {
                    let kind = match &outcome {
                        AgentOutcome::Complete { .. } => SessionTurnOutcome::Complete,
                        AgentOutcome::MaxTurnsReached { .. } => SessionTurnOutcome::MaxTurns,
                        AgentOutcome::BudgetExhausted { .. } => SessionTurnOutcome::BudgetExhausted,
                        AgentOutcome::Cancelled { .. } => SessionTurnOutcome::Cancelled,
                    };
                    RuntimeEvent::Completed {
                        outcome: kind,
                        usage: usage_from_outcome(&outcome),
                    }
                }
                Ok(RuntimeOutput::Suspended(calls)) => RuntimeEvent::Failed {
                    error: RuntimeFailure {
                        code: "xiaoo_unexpected_suspension".into(),
                        message: format!(
                            "xiaoO suspended with {} pending tool call(s)",
                            calls.len()
                        ),
                        retryable: false,
                        details: Value::Null,
                    },
                    usage: SessionUsage::default(),
                },
                Err(error) => RuntimeEvent::Failed {
                    error: RuntimeFailure {
                        code: "xiaoo_runtime_error".into(),
                        message: error.to_string(),
                        retryable: false,
                        details: Value::Null,
                    },
                    usage: SessionUsage::default(),
                },
            };
            {
                let mut persisted = instance.persisted.lock().await;
                persisted.loop_state = state.to_snapshot();
            }
            *instance.state.lock().await = Some(state);
            instance.active_turn.lock().await.take();
            let _ = event_tx.send(event);
        });
        Ok(rx)
    }

    async fn answer_interaction(
        &self,
        input: RuntimeInteractionInput,
    ) -> Result<(), SessionDomainError> {
        let instance = self.instance_for(&input.runtime_id).await?;
        let mut pending_interactions = instance.pending_interactions.lock().await;
        let pending_turn_id = pending_interactions
            .get(&input.interaction_id)
            .map(|pending| pending.turn_id.clone())
            .ok_or_else(|| SessionDomainError::Conflict {
                message: format!("interaction '{}' is not pending", input.interaction_id),
            })?;
        if pending_turn_id != input.turn_id {
            return Err(SessionDomainError::Conflict {
                message: format!(
                    "interaction '{}' belongs to turn '{}'",
                    input.interaction_id, pending_turn_id
                ),
            });
        }
        let response = match input.answer {
            SessionInteractionAnswer::Confirm(allowed) => {
                InteractionResponse::Confirmed { allowed }
            }
            SessionInteractionAnswer::Text(answer) => InteractionResponse::Text {
                value: answer.value,
                display_value: answer.display_value,
            },
            SessionInteractionAnswer::Selection(values) => InteractionResponse::Choice {
                value: values.into_iter().next(),
            },
            SessionInteractionAnswer::Cancelled => InteractionResponse::Choice { value: None },
            SessionInteractionAnswer::Data(_) => {
                return Err(SessionDomainError::InvalidRequest {
                    message: "xiaoO interaction does not accept data answers".into(),
                })
            }
        };
        let pending = pending_interactions
            .remove(&input.interaction_id)
            .expect("pending interaction was validated under the same lock");
        drop(pending_interactions);
        pending
            .response
            .send(response)
            .map_err(|_| SessionDomainError::Conflict {
                message: format!(
                    "interaction '{}' is no longer waiting",
                    input.interaction_id
                ),
            })
    }

    async fn cancel(
        &self,
        runtime_id: &str,
        turn_id: Option<&str>,
    ) -> Result<(), SessionDomainError> {
        let instance = self.instance_for(runtime_id).await?;
        let active = instance.active_turn.lock().await;
        if let Some((active_turn, cancel)) = active.as_ref() {
            if turn_id.is_none() || turn_id == Some(active_turn.as_str()) {
                cancel.cancel();
                instance.pending_interactions.lock().await.clear();
            }
        }
        Ok(())
    }

    async fn export_state(
        &self,
        runtime_id: &str,
    ) -> Result<OpaqueRuntimeState, SessionDomainError> {
        let instance = self.instance_for(runtime_id).await?;
        if instance.active_turn.lock().await.is_some() {
            return Err(SessionDomainError::Conflict {
                message: "cannot export xiaoO state during an active turn".into(),
            });
        }
        let persisted = instance.persisted.lock().await;
        state_to_opaque(&persisted)
    }

    async fn load_state(
        &self,
        runtime_id: &str,
        state: OpaqueRuntimeState,
    ) -> Result<(), SessionDomainError> {
        let instance = self.instance_for(runtime_id).await?;
        let persisted = state_from_opaque(&state)?;
        validate_persisted_llm(&persisted.llm)?;
        *instance.state.lock().await = Some(RuntimeState::from_snapshot(
            persisted.loop_state.clone(),
            CancellationToken::new(),
        ));
        *instance.persisted.lock().await = persisted;
        Ok(())
    }

    async fn checkpoint(&self, runtime_id: &str) -> Result<CheckpointPayload, SessionDomainError> {
        let instance = self.instance_for(runtime_id).await?;
        if instance.active_turn.lock().await.is_some() {
            return Err(SessionDomainError::Conflict {
                message: "cannot checkpoint an active xiaoO turn".into(),
            });
        }
        let persisted = instance.persisted.lock().await.clone();
        if persisted.backend_id != E2B_BACKEND_ID {
            return Err(SessionDomainError::UnsupportedCapability {
                family: CapabilityFamily::Runtime,
                capability: "checkpoint".into(),
            });
        }
        let snapshot = instance
            .manager
            .checkpoint_instance(runtime_id)
            .await
            .map_err(map_provider_error)?;
        Ok(CheckpointPayload {
            checkpoint_id: format!("checkpoint-{}", uuid::Uuid::new_v4()),
            runtime_state: state_to_opaque(&persisted)?,
            provider_snapshot_id: snapshot.snapshot_id.0,
        })
    }

    async fn load_from_checkpoint(
        &self,
        request: RuntimeLoadRequest,
    ) -> Result<(), SessionDomainError> {
        if request
            .llm
            .as_ref()
            .and_then(|llm| llm.api_key.as_ref())
            .is_some()
        {
            return Err(SessionDomainError::InvalidRequest {
                message: "xiaoO rejects inline api_key".into(),
            });
        }
        let persisted = state_from_opaque(&request.runtime_state)?;
        validate_persisted_llm(&persisted.llm)?;
        if persisted.backend_id != E2B_BACKEND_ID {
            return Err(SessionDomainError::UnsupportedCapability {
                family: CapabilityFamily::Runtime,
                capability: "checkpoint".into(),
            });
        }
        let manager = self.manager_for(&persisted.backend_id)?;
        let raw_backend = manager
            .load_instance_from_snapshot(
                request.new_runtime_id.clone(),
                BackendId(persisted.backend_id.clone()),
                request.owner_ref,
                provider_protocol::ProviderSnapshotId(request.provider_snapshot_id),
                persisted.provider_options.clone(),
            )
            .await
            .map_err(map_provider_error)?;
        let backend: Arc<dyn xiaoo_api::backend::OperationBackend> =
            Arc::new(OperationBridge::new(raw_backend));
        let state =
            RuntimeState::from_snapshot(persisted.loop_state.clone(), CancellationToken::new());
        self.instances.write().await.insert(
            request.new_runtime_id,
            Arc::new(XiaooInstance {
                manager,
                backend,
                persisted: Mutex::new(persisted),
                state: Mutex::new(Some(state)),
                active_turn: Mutex::new(None),
                pending_interactions: Arc::new(Mutex::new(HashMap::new())),
            }),
        );
        Ok(())
    }

    async fn delete_checkpoint(
        &self,
        state: OpaqueRuntimeState,
        provider_snapshot_id: String,
    ) -> Result<(), SessionDomainError> {
        let persisted = state_from_opaque(&state)?;
        self.manager_for(&persisted.backend_id)?
            .delete_snapshot(
                BackendId(persisted.backend_id),
                provider_protocol::ProviderSnapshotId(provider_snapshot_id),
            )
            .await
            .map_err(map_provider_error)
    }

    async fn cleanup_from_state(
        &self,
        runtime_id: &str,
        state: &OpaqueRuntimeState,
    ) -> Result<(), SessionDomainError> {
        let persisted = state_from_opaque(state)?;
        self.manager_for(&persisted.backend_id)?
            .destroy_by_runtime_id(runtime_id)
            .await
            .map_err(map_provider_error)
    }
}
