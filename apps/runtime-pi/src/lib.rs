pub mod bridge;
mod session_file;
pub mod worker;

pub use worker::run_worker_from_env;

use agent_runtime_protocol::{
    decode_worker_response, encode_worker_request, worker_error_event, RuntimeCancelRequest,
    RuntimeError, WorkerRequest, WorkerResponse,
};
use agent_runtime_protocol::{
    AgentRuntime, RuntimeCapability, RuntimeCapabilityContext, RuntimeEvent, RuntimeEventReceiver,
    RuntimeExecutionContext, RuntimeInteractionRequest as RuntimeInteractionInput,
    RuntimeStartRequest, RuntimeTurnRequest as RuntimeTurnInput,
};
use async_trait::async_trait;
use bridge::Bridge;
use llm_client::ResolveInput;
use operation_protocol::capability::exec::ExecRequest;
use operation_protocol::OperationBackend;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use session_protocol::{
    LlmOverrideRequest, SessionExtensions, SessionInteractionAnswer, SessionUsage,
};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, Mutex, RwLock};
use uuid::Uuid;
use xgovernor_core::{
    enforce_workspace_axiom, IsolationBoundary, IsolationFacts, NetworkIsolation,
    NormalizedSessionEnvironment, OpaqueRuntimeState, ResolvedLlm, SandboxCapability,
    SecurityContext, SessionDomainError, SessionEnvironmentNormalizer, WorkspaceAccess,
    WorkspaceFacts,
};

/// Namespaced `ext` key this adapter reads its bootstrap config from,
/// matching the "runtime-specific input belongs in namespaced ext bags"
/// convention documented on `session_protocol::SessionExtensions`. As of the
/// bridge-backed architecture (see the module doc), presence of this
/// namespace is now **mandatory** — `backend_id` names which of this
/// `PiRuntime`'s composed `InstanceManager`s should provision the sandbox
/// Pi's tools will be routed to, mirroring `runtime-local`'s
/// `read_local_runtime_ext` fail-closed pattern exactly (a missing/empty
/// `backend_id` is meaningless: there is no sandbox to attach the bridge to
/// without it).
pub const EXT_NAMESPACE: &str = "runtime_pi";

/// Environment variable consulted when `ext.runtime_pi.executable` is absent.
pub const PI_EXECUTABLE_ENV: &str = "XGOVERNOR_PI_EXECUTABLE";

const DEFAULT_PI_EXECUTABLE: &str = "pi";

/// Pi RPC dialog methods that expect an `extension_ui_response` reply. Every
/// other `extension_ui_request` method (`notify`, `setStatus`, `setWidget`,
/// `setTitle`, `set_editor_text`, ...) is fire-and-forget and is surfaced as
/// an `Extension` event instead of an `InteractionRequested` one, since there
/// is nothing for `answer_interaction` to reply to.
const DIALOG_METHODS: &[&str] = &["select", "confirm", "input", "editor"];

#[derive(Debug, Clone, Deserialize)]
struct PiRuntimeExt {
    /// Which of this `PiRuntime`'s composed `InstanceManager`s should
    /// provision the sandbox backing this session. Mandatory: see
    /// [`EXT_NAMESPACE`]'s doc for why a default/fallback would be
    /// meaningless here.
    backend_id: String,
    #[serde(default)]
    executable: Option<String>,
    /// Overrides the directory containing the TypeScript Pi extension
    /// (passed to `pi --mode rpc` via `-e <extension_dir>`). Defaults to
    /// `<this crate's manifest dir>/extension` — see
    /// [`resolve_extension_dir`].
    #[serde(default)]
    extension_dir: Option<String>,
    #[serde(flatten)]
    role: PiRoleConfig,
}

/// Per-session defaults and per-turn overrides. Persisted separately from Pi history.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct PiRoleConfig {
    #[serde(default)]
    system_prompt: Option<String>,
    #[serde(default)]
    max_turns: Option<u32>,
    #[serde(default)]
    tools_enabled: Option<bool>,
}
const PI_ROLE_FILE: &str = "xgovernor-role.json";
impl PiRoleConfig {
    fn validate(&self) -> Result<(), SessionDomainError> {
        if self.max_turns == Some(0) {
            return Err(SessionDomainError::InvalidRequest {
                message: "max_turns must be positive".into(),
            });
        }
        Ok(())
    }
    fn merge(&mut self, next: Self) {
        if next.system_prompt.is_some() {
            self.system_prompt = next.system_prompt;
        }
        if next.max_turns.is_some() {
            self.max_turns = next.max_turns;
        }
        if next.tools_enabled.is_some() {
            self.tools_enabled = next.tools_enabled;
        }
    }
}
async fn read_role(dir: &std::path::Path) -> Result<PiRoleConfig, SessionDomainError> {
    match tokio::fs::read(dir.join(PI_ROLE_FILE)).await {
        Ok(bytes) => {
            serde_json::from_slice(&bytes).map_err(|e| SessionDomainError::InvalidRequest {
                message: format!("invalid Pi role state: {e}"),
            })
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(PiRoleConfig::default()),
        Err(e) => Err(SessionDomainError::Unavailable {
            message: e.to_string(),
        }),
    }
}
async fn write_role(dir: &std::path::Path, role: &PiRoleConfig) -> Result<(), SessionDomainError> {
    role.validate()?;
    tokio::fs::write(
        dir.join(PI_ROLE_FILE),
        serde_json::to_vec(role).expect("role is serializable"),
    )
    .await
    .map_err(|e| SessionDomainError::Unavailable {
        message: e.to_string(),
    })
}

/// Default location of the bundled Pi extension, resolved at compile time
/// against this crate's own manifest directory so it works regardless of the
/// daemon's current working directory at runtime.
const DEFAULT_EXTENSION_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/extension");

fn resolve_extension_dir(ext: &PiRuntimeExt) -> String {
    ext.extension_dir
        .clone()
        .unwrap_or_else(|| DEFAULT_EXTENSION_DIR.to_string())
}

const LOCAL_BACKEND_ID: &str = "local";
const E2B_BACKEND_ID: &str = "e2b";
const E2B_WORKSPACE_ROOT: &str = "/home/user/workspace";
const PI_LLM_STATE_FILE: &str = ".xgovernor-llm.json";
const PI_MODEL_COMMAND: &str = "xgovernor-model";
pub const PI_WORKER_ENV: &str = "XGOVERNOR_PI_WORKER";

/// Fully resolved, per-session Pi model configuration. This is deliberately
/// provider-neutral: `provider` and `model` are passed through exactly as the
/// caller selected them. When `api_base` is present, the bundled extension
/// registers that endpoint as an OpenAI-compatible provider before selecting
/// the model; otherwise Pi's built-in provider catalogue is used.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PiLlmConfig {
    provider: String,
    model: String,
    #[serde(default)]
    api_base: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
    credential_source: String,
}

impl PiLlmConfig {
    fn resolve(request: &LlmOverrideRequest) -> Result<Self, SessionDomainError> {
        let required = |name: &str, value: &Option<String>| {
            value
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| SessionDomainError::InvalidRequest {
                    message: format!("pi llm.{name} is required when an llm override is supplied"),
                })
        };
        let provider = required("provider", &request.provider)?;
        let model = required("model", &request.model)?;
        let api_base = request
            .api_base
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        let (api_key, credential_source) = if let Some(key) = request
            .api_key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            (Some(key.to_string()), "request".to_string())
        } else if let Some(name) = request
            .api_key_env
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let key = std::env::var(name).map_err(|_| SessionDomainError::InvalidRequest {
                message: format!(
                    "pi llm.api_key_env references unset environment variable '{name}'"
                ),
            })?;
            if key.trim().is_empty() {
                return Err(SessionDomainError::InvalidRequest {
                    message: format!(
                        "pi llm.api_key_env references empty environment variable '{name}'"
                    ),
                });
            }
            (Some(key), format!("env:{name}"))
        } else {
            (None, "runtime_default".to_string())
        };

        Ok(Self {
            provider,
            model,
            api_base,
            api_key,
            credential_source,
        })
    }

    fn descriptor(&self) -> ResolvedLlm {
        ResolvedLlm {
            provider: self.provider.clone(),
            model: self.model.clone(),
            api_base: self.api_base.clone(),
            credential_source: self.credential_source.clone(),
        }
    }
}

fn resolve_llm(
    request: Option<&LlmOverrideRequest>,
) -> Result<Option<PiLlmConfig>, SessionDomainError> {
    request.map(PiLlmConfig::resolve).transpose()
}

/// Open-time LLM connectivity probe. Sends a minimal chat-completions
/// request (max_tokens: 1) to verify the endpoint is reachable and the
/// key/model are valid. Any failure aborts the session open before the
/// sandbox is provisioned.
async fn probe_llm_endpoint(
    api_base: &str,
    api_key: Option<&str>,
    model: &str,
) -> Result<(), SessionDomainError> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| SessionDomainError::InvalidRequest {
            message: format!("LLM probe: failed to build HTTP client: {e}"),
        })?;
    let url = format!("{}/chat/completions", api_base.trim_end_matches('/'));
    let mut req = client.post(&url).json(&json!({
        "model": model,
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 1
    }));
    if let Some(key) = api_key {
        req = req.header("Authorization", format!("Bearer {key}"));
    }
    let resp = req
        .send()
        .await
        .map_err(|e| SessionDomainError::InvalidRequest {
            message: format!("LLM probe to '{url}': {e}"),
        })?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(SessionDomainError::InvalidRequest {
            message: format!(
                "LLM probe to '{url}' returned {status}: {}",
                body.chars().take(500).collect::<String>()
            ),
        });
    }
    Ok(())
}

async fn persist_llm_config(
    session_dir: &std::path::Path,
    config: &PiLlmConfig,
) -> Result<(), SessionDomainError> {
    let bytes = serde_json::to_vec(config).expect("PiLlmConfig always serializes");
    tokio::fs::write(session_dir.join(PI_LLM_STATE_FILE), bytes)
        .await
        .map_err(|error| SessionDomainError::Unavailable {
            message: format!("failed to persist pi session llm configuration: {error}"),
        })
}

async fn load_llm_config(
    session_dir: &std::path::Path,
) -> Result<Option<PiLlmConfig>, SessionDomainError> {
    let path = session_dir.join(PI_LLM_STATE_FILE);
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(SessionDomainError::Unavailable {
                message: format!("failed to read pi session llm configuration: {error}"),
            })
        }
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| SessionDomainError::Unavailable {
            message: format!(
                "pi session llm configuration '{}' is corrupt: {error}",
                path.display()
            ),
        })
}

async fn configure_pi_launch(
    command: &mut Command,
    session_dir: &std::path::Path,
    config: Option<&PiLlmConfig>,
) -> Result<(), SessionDomainError> {
    let Some(config) = config else { return Ok(()) };

    command
        .arg("--provider")
        .arg(&config.provider)
        .arg("--model")
        .arg(&config.model);
    if let Some(api_key) = &config.api_key {
        command.arg("--api-key").arg(api_key);
    }

    // Pi has no generic `--base-url` flag. A request carrying api_base gets
    // an isolated models.json, scoped to this one child process. The wire
    // protocol currently has no provider API-kind field, so api_base means
    // OpenAI Chat Completions compatible until that contract is extended.
    if let Some(api_base) = &config.api_base {
        let agent_dir = session_dir.join(".pi-agent");
        tokio::fs::create_dir_all(&agent_dir)
            .await
            .map_err(|error| SessionDomainError::Unavailable {
                message: format!("failed to create isolated pi agent directory: {error}"),
            })?;
        let mut provider = json!({
            "baseUrl": api_base,
            "api": "openai-completions",
            "models": [{ "id": config.model }],
        });
        provider["apiKey"] = json!(config.api_key.as_deref().unwrap_or("xgovernor-keyless"));
        let models = json!({ "providers": { config.provider.clone(): provider } });
        tokio::fs::write(
            agent_dir.join("models.json"),
            serde_json::to_vec_pretty(&models).expect("models.json always serializes"),
        )
        .await
        .map_err(|error| SessionDomainError::Unavailable {
            message: format!("failed to write isolated pi models.json: {error}"),
        })?;
        command.env("PI_CODING_AGENT_DIR", agent_dir);
        if config.api_key.is_none() {
            command.arg("--api-key").arg("xgovernor-keyless");
        }
    }
    persist_llm_config(session_dir, config).await
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct GitWorkspaceMetadata {
    url: String,
    #[serde(default)]
    reference: Option<String>,
    #[serde(default)]
    subdirectory: Option<String>,
}

/// `PiRuntime`'s private shape for the `state` blob it writes into
/// `SessionRecord.runtime` (`OpaqueRuntimeState.state`) — see
/// `docs/pi_session_restore_plan.md` §1.1. Nothing outside this crate ever
/// interprets this shape; `application.rs` only ever handles the opaque
/// `OpaqueRuntimeState` wrapper produced by [`PiPersistedState::into_opaque`].
///
/// `executable`/`extension_dir` mirror [`PiRuntimeExt`]'s own optionality:
/// only the caller's explicit override (if any) is stored, not the resolved
/// default/env-var value, so a restore re-derives the same fallback chain
/// `start()`'s cold path already uses (env var read at restore time, not
/// frozen at the original start time).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct PiPersistedState {
    backend_id: String,
    #[serde(default)]
    executable: Option<String>,
    #[serde(default)]
    extension_dir: Option<String>,
    /// Absolute path of the per-`runtime_id` `--session-dir` this instance's
    /// `pi` process was (or, on restore, will be) spawned with.
    pi_session_dir: String,
    #[serde(default)]
    runtime_id: Option<String>,
    /// The `WorkspaceFacts.metadata` blob this session opened with (e.g. git
    /// clone parameters), stashed verbatim for a future explicit-rebuild path
    /// (`docs/pi_session_restore_plan.md` §4.3 / risk #3). Phase 1 only
    /// stores this; nothing reads it back yet.
    #[serde(default)]
    workspace_metadata: Value,
    /// Embedded immutable history only for checkpoint exports; never a live path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint: Option<PiCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct PiCheckpoint {
    session_jsonl: Option<String>,
    llm_config_ref: Option<String>,
    llm_config: Option<PiLlmConfig>,
    role: PiRoleConfig,
}

/// Bump whenever [`PiPersistedState`]'s shape changes. `from_opaque` fails
/// closed on any version it doesn't recognize (`docs/pi_session_restore_plan.md`
/// §1.1) rather than guessing at a shape it was never told about.
const PI_PERSISTED_STATE_SCHEMA_VERSION: u32 = 2;

impl PiPersistedState {
    fn into_opaque(self) -> OpaqueRuntimeState {
        OpaqueRuntimeState {
            runtime_kind: "pi".to_string(),
            schema_version: PI_PERSISTED_STATE_SCHEMA_VERSION,
            state: serde_json::to_value(self)
                .expect("PiPersistedState is plain data and always serializes"),
        }
    }

    fn from_opaque(opaque: &OpaqueRuntimeState) -> Result<Self, SessionDomainError> {
        if opaque.runtime_kind != "pi" {
            return Err(SessionDomainError::Internal {
                message: format!(
                    "PiRuntime received persisted state for runtime_kind {:?}, not \"pi\"; \
                     refusing to interpret another runtime's state",
                    opaque.runtime_kind
                ),
                source: None,
            });
        }
        if opaque.schema_version != 1 && opaque.schema_version != PI_PERSISTED_STATE_SCHEMA_VERSION
        {
            return Err(SessionDomainError::Internal {
                message: format!(
                    "PiRuntime does not recognize persisted state schema_version {} (expected \
                     {}); refusing to guess at its shape rather than risk misreading it (see \
                     docs/pi_session_restore_plan.md §1.1)",
                    opaque.schema_version, PI_PERSISTED_STATE_SCHEMA_VERSION
                ),
                source: None,
            });
        }
        serde_json::from_value(opaque.state.clone()).map_err(|error| SessionDomainError::Internal {
            message: format!("PiRuntime persisted state failed to parse: {error}"),
            source: None,
        })
    }
}

fn read_pi_runtime_ext(ext: &SessionExtensions) -> Result<PiRuntimeExt, SessionDomainError> {
    let value = ext
        .get(EXT_NAMESPACE)
        .ok_or_else(|| SessionDomainError::InvalidRequest {
            message: format!("missing required '{EXT_NAMESPACE}' ext payload (backend_id)"),
        })?;
    let parsed: PiRuntimeExt = serde_json::from_value(value.clone()).map_err(|error| {
        SessionDomainError::InvalidRequest {
            message: format!("invalid '{EXT_NAMESPACE}' ext payload: {error}"),
        }
    })?;
    parsed.role.validate()?;
    if parsed.backend_id.trim().is_empty() {
        return Err(SessionDomainError::InvalidRequest {
            message: format!("'{EXT_NAMESPACE}.backend_id' must not be empty"),
        });
    }
    Ok(parsed)
}

fn to_runtime_error(error: SessionDomainError) -> RuntimeError {
    match error {
        SessionDomainError::InvalidRequest { message } => RuntimeError::InvalidRequest {
            code: "invalid_request".into(),
            message,
        },
        SessionDomainError::NotFound { runtime_id } => RuntimeError::NotFound { runtime_id },
        SessionDomainError::Conflict { message } => RuntimeError::Conflict {
            code: "conflict".into(),
            message,
        },
        SessionDomainError::UnsupportedCapability { capability, .. } => {
            RuntimeError::UnsupportedCapability { capability }
        }
        SessionDomainError::Unavailable { message } => RuntimeError::WorkerUnavailable {
            message,
            retryable: true,
        },
        error => RuntimeError::Internal {
            message: error.to_string(),
        },
    }
}

/// Environment facts for PI sessions. The selected tool backend is part of
/// PI's namespaced open extension, so the same `backend_id` that `PiRuntime`
/// later uses to provision the sandbox must also drive admission and the
/// isolation facts returned by `open`.
pub struct PiSessionEnvironment {
    default_local_root: String,
    configured_backends: BTreeSet<String>,
}

impl PiSessionEnvironment {
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

#[async_trait]
impl SessionEnvironmentNormalizer for PiSessionEnvironment {
    async fn normalize(
        &self,
        ctx: &SecurityContext,
        request: &session_protocol::SessionOpenRequest,
    ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
        let ext = read_pi_runtime_ext(&request.ext)?;
        if !self.configured_backends.contains(&ext.backend_id) {
            return Err(SessionDomainError::InvalidRequest {
                message: format!(
                    "no PI backend configured for backend_id '{}'; configured backends: {:?}",
                    ext.backend_id, self.configured_backends
                ),
            });
        }

        let provider_is_sandbox = match ext.backend_id.as_str() {
            LOCAL_BACKEND_ID => false,
            E2B_BACKEND_ID => true,
            other => {
                return Err(SessionDomainError::InvalidRequest {
                    message: format!("backend_id '{other}' has no declared PI isolation profile"),
                })
            }
        };
        enforce_workspace_axiom(ctx, &request.workspace, provider_is_sandbox)?;

        let (root, revision, metadata) = match (&request.workspace, ext.backend_id.as_str()) {
            (session_protocol::WorkspaceSpec::DaemonDefault, LOCAL_BACKEND_ID) => {
                (self.default_local_root.clone(), None, Value::Null)
            }
            (session_protocol::WorkspaceSpec::LocalPath { path }, LOCAL_BACKEND_ID) => {
                (path.clone(), None, Value::Null)
            }
            (session_protocol::WorkspaceSpec::DaemonDefault, E2B_BACKEND_ID) => {
                (E2B_WORKSPACE_ROOT.to_string(), None, Value::Null)
            }
            (
                session_protocol::WorkspaceSpec::Git {
                    url,
                    reference,
                    subdirectory,
                },
                E2B_BACKEND_ID,
            ) => {
                let metadata = serde_json::to_value(GitWorkspaceMetadata {
                    url: url.clone(),
                    reference: reference.clone(),
                    subdirectory: subdirectory.clone(),
                })
                .expect("GitWorkspaceMetadata serialization is infallible");
                (E2B_WORKSPACE_ROOT.to_string(), reference.clone(), metadata)
            }
            (workspace, backend_id) => {
                return Err(SessionDomainError::InvalidRequest {
                    message: format!(
                    "workspace kind {workspace:?} is not supported by PI backend_id '{backend_id}'"
                ),
                })
            }
        };

        let (boundary, network, capabilities) = match ext.backend_id.as_str() {
            LOCAL_BACKEND_ID => (
                IsolationBoundary::Host,
                NetworkIsolation::None,
                [
                    SandboxCapability::Exec,
                    SandboxCapability::FileRead,
                    SandboxCapability::FileWrite,
                    SandboxCapability::Network,
                ]
                .into_iter()
                .collect(),
            ),
            E2B_BACKEND_ID => (
                IsolationBoundary::Remote,
                // E2B is remote, but this integration does not disable the
                // sandbox's internet access; do not overstate network isolation.
                NetworkIsolation::None,
                [
                    SandboxCapability::Exec,
                    SandboxCapability::FileRead,
                    SandboxCapability::FileWrite,
                    SandboxCapability::Snapshot,
                    SandboxCapability::Network,
                ]
                .into_iter()
                .collect(),
            ),
            _ => unreachable!("backend profile checked above"),
        };

        let llm = resolve_llm(request.llm.as_ref())?.map(|config| config.descriptor());

        Ok(NormalizedSessionEnvironment {
            workspace: WorkspaceFacts {
                workspace_id: request.conversation_id.clone(),
                root,
                access: WorkspaceAccess::ReadWrite,
                revision,
                metadata,
            },
            isolation: IsolationFacts {
                boundary,
                workspace_access: WorkspaceAccess::ReadWrite,
                network,
                metadata: json!({
                    "runtime": "pi",
                    "tool_backend_id": ext.backend_id,
                    "controller_boundary": "host"
                }),
            },
            sandbox_capabilities: capabilities,
            llm,
            lease: None,
        })
    }
}

async fn clone_git_workspace(
    backend: &dyn OperationBackend,
    git: &GitWorkspaceMetadata,
    workspace_root: &str,
) -> Result<(), SessionDomainError> {
    let mut args = vec!["clone".to_string()];
    if let Some(reference) = &git.reference {
        args.push("--branch".to_string());
        args.push(reference.clone());
    }
    args.push(git.url.clone());
    args.push(workspace_root.to_string());

    let result = backend
        .exec()
        .exec(ExecRequest {
            command: "git".to_string(),
            args,
            shell: None,
            cwd: None,
            timeout_ms: Some(120_000),
            env: None,
            extra: None,
        })
        .await
        .map_err(|error| SessionDomainError::Internal {
            message: format!("git clone exec failed: {error}"),
            source: None,
        })?;

    if result.exit_code != Some(0) {
        let stderr = String::from_utf8_lossy(&result.stderr).into_owned();
        return Err(SessionDomainError::Internal {
            message: format!("git clone failed (exit {:?}): {stderr}", result.exit_code),
            source: None,
        });
    }
    Ok(())
}

/// Supervisor-side metadata for the single turn currently owned by a worker.
/// Pi-native state and event sequencing live exclusively in `pi-worker`.
struct CurrentTurn {
    turn_id: String,
    pending_interactions: HashMap<String, String>,
}

/// One running `pi-worker` subprocess. The worker owns the nested
/// `pi --mode rpc` process and translates its native protocol.
struct PiInstance {
    stdin: Mutex<ChildStdin>,
    child: Mutex<Child>,
    stdout: Mutex<BufReader<ChildStdout>>,
    current_turn: Mutex<Option<CurrentTurn>>,
    /// Bearer token this instance's backend is registered under on the
    /// shared [`Bridge`]. `stop()` unregisters it so a dangling token can't
    /// keep proxying to a backend whose sandbox is about to be torn down.
    bridge_token: String,
    /// Everything needed to reconstruct this instance's `start()` call after
    /// a restart — see [`PiPersistedState`]. Immutable snapshot taken at
    /// `start()` time; `export_state()` just clones and re-wraps it.
    persisted_state: PiPersistedState,
}

/// `AgentRuntime` backed by one `pi-worker` subprocess per `runtime_id`.
/// Each worker owns one nested `pi --mode rpc` process. A shared [`Bridge`]
/// proxies Pi's tool calls to the operation backend injected by the host.
pub struct PiRuntime {
    bridge: Arc<Bridge>,
    instances: RwLock<HashMap<String, Arc<PiInstance>>>,
    pi_session_root: PathBuf,
    worker_executable: PathBuf,
}

impl PiRuntime {
    /// `pi_session_root` is where each started session's `--session-dir`
    /// subdirectory (named after its `runtime_id`) is created — see the
    /// field doc on [`PiRuntime::pi_session_root`].
    ///
    /// Binds the bridge's HTTP listener synchronously as part of
    /// construction (`Bridge::spawn`), hence the `std::io::Result` return —
    /// this is the one fallible step in bringing up a `PiRuntime`.
    pub fn new(pi_session_root: PathBuf) -> std::io::Result<Self> {
        let worker_executable = std::env::var_os(PI_WORKER_ENV)
            .map(PathBuf::from)
            .or_else(|| std::env::current_exe().ok())
            .unwrap_or_else(|| PathBuf::from("pi-worker"));
        Self::new_with_worker(pi_session_root, worker_executable)
    }

    /// Constructs a supervisor with an explicit worker binary. Integration
    /// tests use this to launch Cargo's standalone `pi-worker` artifact.
    pub fn new_with_worker(
        pi_session_root: PathBuf,
        worker_executable: PathBuf,
    ) -> std::io::Result<Self> {
        let bridge = Bridge::spawn()?;
        Ok(Self {
            bridge,
            instances: RwLock::new(HashMap::new()),
            pi_session_root,
            worker_executable,
        })
    }

    async fn instance_for(&self, runtime_id: &str) -> Result<Arc<PiInstance>, SessionDomainError> {
        self.instances
            .read()
            .await
            .get(runtime_id)
            .cloned()
            .ok_or_else(|| SessionDomainError::NotFound {
                runtime_id: runtime_id.to_string(),
            })
    }

    /// Cold-start branch of `start()` (`request.state == None`): provisions a
    /// brand-new sandbox via `InstanceManager::start_instance`, optionally
    /// git-clones a workspace into it, and creates a fresh per-`runtime_id`
    /// session directory. Any failure inside here rolls back the sandbox it
    /// just created — nothing outside this call has taken ownership of it
    /// yet.
    async fn prepare_cold_start(
        &self,
        request: &RuntimeStartRequest,
        backend: Arc<dyn OperationBackend>,
    ) -> Result<PreparedStart, SessionDomainError> {
        let ext = read_pi_runtime_ext(&request.ext)?;
        let executable = ext
            .executable
            .clone()
            .or_else(|| std::env::var(PI_EXECUTABLE_ENV).ok())
            .unwrap_or_else(|| DEFAULT_PI_EXECUTABLE.to_string());
        let extension_dir = resolve_extension_dir(&ext);

        // Stashed verbatim into `PiPersistedState.workspace_metadata` (§1.1)
        // for a possible future explicit-rebuild path; Phase 1/2 only store
        // it, nothing reads it back yet.
        let workspace_metadata_snapshot = request.workspace.metadata.clone();

        // A Git workspace normalized by `PiSessionEnvironment` carries
        // `GitWorkspaceMetadata` (mirroring `apps/runtime-mock`'s
        // `clone_git_workspace`); materialize the clone inside the sandbox
        // before attaching the bridge, and roll the sandbox back if it fails.
        if request.workspace.metadata != Value::Null {
            let git: GitWorkspaceMetadata =
                serde_json::from_value(request.workspace.metadata.clone()).map_err(|error| {
                    SessionDomainError::InvalidRequest {
                        message: format!("invalid workspace_metadata for git clone: {error}"),
                    }
                })?;
            let clone_target = backend.paths().workspace_root().clone();
            if let Err(error) = clone_git_workspace(backend.as_ref(), &git, &clone_target.0).await {
                return Err(error);
            }
        }

        // Per-`runtime_id` `--session-dir` (decision 1,
        // `docs/pi_session_restore_plan.md` §1.1) — created up front so a
        // restart can find and re-attach to whatever `pi` writes here via
        // `PiPersistedState.pi_session_dir`, without cwd-based bucketing
        // (F10/F13: an explicit `--session-dir` lays files flat, unlike the
        // default no-flag behavior).
        let session_dir = self.pi_session_root.join(&request.runtime_id);
        if let Err(error) = tokio::fs::create_dir_all(&session_dir).await {
            return Err(SessionDomainError::Unavailable {
                message: format!(
                    "failed to create pi session directory {}: {error}",
                    session_dir.display()
                ),
            });
        }

        write_role(&session_dir, &ext.role).await?;
        let persisted_state = PiPersistedState {
            backend_id: ext.backend_id.clone(),
            executable: ext.executable.clone(),
            extension_dir: ext.extension_dir.clone(),
            pi_session_dir: session_dir.to_string_lossy().into_owned(),
            runtime_id: Some(request.runtime_id.clone()),
            workspace_metadata: workspace_metadata_snapshot,
            checkpoint: None,
        };

        Ok(PreparedStart {
            backend,
            executable,
            extension_dir,
            session_dir,
            resume_session_file: None,
            persisted_state,
        })
    }

    /// Restoration branch of `start()` (`request.state == Some(..)`,
    /// `docs/pi_session_restore_plan.md` §1.2): reuses an existing sandbox —
    /// still tracked by its `InstanceManager`'s post-`reconcile()` registry —
    /// instead of provisioning a new one, and re-attaches `pi` to whatever
    /// session file it left behind rather than cloning a workspace again.
    ///
    /// Deliberately does **not** call `stop_instance` on failure: unlike
    /// `prepare_cold_start`, the sandbox here predates this call and is not
    /// this call's to destroy just because `pi` itself failed to come back
    /// up (see `PreparedStart::destroy_sandbox_on_spawn_failure`'s doc).
    async fn prepare_resume(
        &self,
        request: &RuntimeStartRequest,
        opaque_state: &OpaqueRuntimeState,
        backend: Arc<dyn OperationBackend>,
    ) -> Result<PreparedStart, SessionDomainError> {
        let mut state = PiPersistedState::from_opaque(opaque_state)?;

        let executable = state
            .executable
            .clone()
            .or_else(|| std::env::var(PI_EXECUTABLE_ENV).ok())
            .unwrap_or_else(|| DEFAULT_PI_EXECUTABLE.to_string());
        let extension_dir = state
            .extension_dir
            .clone()
            .unwrap_or_else(|| DEFAULT_EXTENSION_DIR.to_string());

        let (session_dir, resume_session_file) = if let Some(snapshot) = state.checkpoint.take() {
            // A unique directory for each load/fork: neither parent writes nor sibling
            // resumes can alter this branch's history, even after the source closes.
            let dir = self
                .pi_session_root
                .join(format!("branch-{}", Uuid::new_v4()));
            tokio::fs::create_dir_all(&dir)
                .await
                .map_err(|e| SessionDomainError::Unavailable {
                    message: e.to_string(),
                })?;
            let result = async {
                let history = if let Some(jsonl) = snapshot.session_jsonl {
                    let path = dir.join("session.jsonl");
                    tokio::fs::write(&path, jsonl).await.map_err(|e| {
                        SessionDomainError::Unavailable {
                            message: e.to_string(),
                        }
                    })?;
                    Some(path)
                } else {
                    None
                };
                if let Some(reference) = snapshot.llm_config_ref.as_ref() {
                    let path = self.checkpoint_credential_path(reference)?;
                    let bytes = tokio::fs::read(path).await.map_err(|e| {
                        SessionDomainError::Unavailable {
                            message: format!("checkpoint credential unavailable: {e}"),
                        }
                    })?;
                    let config: PiLlmConfig = serde_json::from_slice(&bytes).map_err(|e| {
                        SessionDomainError::InvalidRequest {
                            message: e.to_string(),
                        }
                    })?;
                    persist_llm_config(&dir, &config).await?;
                }
                if let Some(mut config) = snapshot.llm_config {
                    if let Some(name) = config.credential_source.strip_prefix("env:") {
                        config.api_key = Some(std::env::var(name).map_err(|_| {
                            SessionDomainError::InvalidRequest {
                                message: format!(
                                    "checkpoint requires environment credential {name}"
                                ),
                            }
                        })?);
                    }
                    persist_llm_config(&dir, &config).await?;
                }
                write_role(&dir, &snapshot.role).await?;
                Ok::<_, SessionDomainError>(history)
            }
            .await;
            match result {
                Ok(history) => (dir, history),
                Err(error) => {
                    let _ = tokio::fs::remove_dir_all(dir).await;
                    return Err(error);
                }
            }
        } else {
            let dir = PathBuf::from(&state.pi_session_dir);
            let owner = state
                .runtime_id
                .as_deref()
                .or_else(|| dir.file_name().and_then(|v| v.to_str()));
            if owner != Some(request.runtime_id.as_str()) {
                return Err(SessionDomainError::InvalidRequest {message:"legacy Pi state is a live session reference, not an immutable branch checkpoint; create a new checkpoint first".into()});
            }
            let history = match session_file::latest_complete_turn_file(&dir) {
                Ok(path) => Some(path),
                Err(session_file::SessionFileError::NoSessionFiles(_)) => None,
                Err(error) => {
                    return Err(SessionDomainError::Unavailable {
                        message: format!("pi_session_state_lost: {error}"),
                    })
                }
            };
            (dir, history)
        };
        state.pi_session_dir = session_dir.to_string_lossy().into_owned();
        state.runtime_id = Some(request.runtime_id.clone());

        Ok(PreparedStart {
            backend,
            executable,
            extension_dir,
            session_dir,
            resume_session_file,
            persisted_state: state,
        })
    }
}

/// Everything `start()` needs to spawn `pi` and register the resulting
/// `PiInstance`, produced by either `prepare_cold_start` or `prepare_resume`
/// so the rest of `start()` — bridge registration, `Command` construction,
/// bookkeeping — is shared between the two branches instead of duplicated.
struct PreparedStart {
    backend: Arc<dyn OperationBackend>,
    executable: String,
    extension_dir: String,
    session_dir: PathBuf,
    /// `Some(path)` on a restoration start: passed to `pi` as `--session
    /// <path>` in addition to `--session-dir`. `None` on a cold start.
    resume_session_file: Option<PathBuf>,
    persisted_state: PiPersistedState,
}

async fn send_worker_request(
    instance: &PiInstance,
    request: WorkerRequest,
) -> Result<(), SessionDomainError> {
    let line = encode_worker_request(&request).map_err(|error| SessionDomainError::Internal {
        message: format!("failed to encode Pi worker request: {error}"),
        source: None,
    })?;
    let mut stdin = instance.stdin.lock().await;
    stdin
        .write_all(line.as_bytes())
        .await
        .map_err(|error| SessionDomainError::Unavailable {
            message: format!("Pi worker write failed: {error}"),
        })?;
    stdin
        .flush()
        .await
        .map_err(|error| SessionDomainError::Unavailable {
            message: format!("Pi worker flush failed: {error}"),
        })
}

fn map_answer_to_pi_value(
    method: &str,
    answer: &SessionInteractionAnswer,
) -> Result<Value, SessionDomainError> {
    use SessionInteractionAnswer as Answer;
    match (method, answer) {
        // A dismissed/cancelled dialog maps to `null` regardless of method —
        // there is no per-method "cancelled" shape documented in Pi's RPC
        // protocol, and `null` is the conventional "no value" sentinel.
        (_, Answer::Cancelled) => Ok(Value::Null),
        ("confirm", Answer::Confirm(value)) => Ok(Value::Bool(*value)),
        ("select", Answer::Selection(values)) => Ok(match values.as_slice() {
            [single] => Value::String(single.clone()),
            _ => Value::Array(values.iter().cloned().map(Value::String).collect()),
        }),
        ("input", Answer::Text(text)) | ("editor", Answer::Text(text)) => {
            let value = text
                .value
                .clone()
                .ok_or_else(|| SessionDomainError::InvalidRequest {
                    message: format!(
                        "text answer for pi dialog method '{method}' must carry a value"
                    ),
                })?;
            Ok(Value::String(value))
        }
        (other_method, other_answer) => Err(SessionDomainError::InvalidRequest {
            message: format!(
                "answer kind does not match pi dialog method '{other_method}': {other_answer:?}"
            ),
        }),
    }
}

fn extract_usage(message: &Value) -> SessionUsage {
    let usage = message.get("usage");
    SessionUsage {
        input_tokens: usage
            .and_then(|u| u.get("inputTokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: usage
            .and_then(|u| u.get("outputTokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        total_tokens: usage
            .and_then(|u| u.get("totalTokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
    }
}

async fn read_worker_turn(
    instance: Arc<PiInstance>,
    turn_id: String,
    output: mpsc::Sender<RuntimeEvent>,
) {
    let mut terminal_sent = false;
    while !terminal_sent {
        let mut line = String::new();
        let read = instance.stdout.lock().await.read_line(&mut line).await;
        let stream_closed = !matches!(read, Ok(count) if count > 0);
        let response = match read {
            Ok(0) => Err(RuntimeError::WorkerUnavailable {
                message: "Pi worker event stream closed".into(),
                retryable: true,
            }),
            Ok(_) => {
                decode_worker_response(&line).map_err(|error| RuntimeError::WorkerUnavailable {
                    message: format!("invalid Pi worker response: {error}"),
                    retryable: true,
                })
            }
            Err(error) => Err(RuntimeError::WorkerUnavailable {
                message: format!("failed to read Pi worker response: {error}"),
                retryable: true,
            }),
        };

        let event = match response {
            Ok(WorkerResponse::Event { event }) => Some(event),
            Ok(WorkerResponse::Error { error }) => Some(worker_error_event(error)),
            Ok(WorkerResponse::Ready | WorkerResponse::State { .. } | WorkerResponse::Unknown) => {
                None
            }
            Err(error) => Some(worker_error_event(error)),
        };

        if let Some(event) = event {
            if let RuntimeEvent::InteractionRequested {
                interaction_id,
                interaction_kind,
                ..
            } = &event
            {
                let mut current = instance.current_turn.lock().await;
                if let Some(turn) = current
                    .as_mut()
                    .filter(|current| current.turn_id == turn_id)
                {
                    turn.pending_interactions
                        .insert(interaction_id.clone(), interaction_kind.clone());
                }
            }
            terminal_sent = event.is_terminal();
            if terminal_sent {
                let mut current = instance.current_turn.lock().await;
                if current
                    .as_ref()
                    .is_some_and(|value| value.turn_id == turn_id)
                {
                    current.take();
                }
            }
            let _ = output.send(event).await;
        }

        if stream_closed {
            break;
        }
    }

    let mut current = instance.current_turn.lock().await;
    if current
        .as_ref()
        .is_some_and(|current| current.turn_id == turn_id)
    {
        current.take();
    }
}

#[async_trait]
impl AgentRuntime for PiRuntime {
    fn runtime_kind(&self) -> &str {
        "pi"
    }

    fn capabilities(&self) -> BTreeSet<RuntimeCapability> {
        BTreeSet::from([
            RuntimeCapability::Interaction,
            RuntimeCapability::ModelOverride,
        ])
    }

    fn capabilities_for_context(
        &self,
        _context: &RuntimeCapabilityContext,
    ) -> BTreeSet<RuntimeCapability> {
        self.capabilities()
    }

    async fn start(
        &self,
        request: RuntimeStartRequest,
        context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        self.start_inner(request, context)
            .await
            .map_err(to_runtime_error)
    }

    async fn probe_llm(&self, request: &RuntimeStartRequest) -> Result<(), RuntimeError> {
        let Some(override_req) = request.llm.as_ref() else {
            return Ok(());
        };
        let config = PiLlmConfig::resolve(override_req).map_err(to_runtime_error)?;
        let resolved = llm_client::resolve_config(ResolveInput {
            provider: Some(config.provider.clone()),
            api_key: config.api_key.clone(),
            base_url: config.api_base.clone(),
            ..Default::default()
        })
        .map_err(|e| RuntimeError::InvalidRequest {
            code: "llm_config".into(),
            message: e.to_string(),
        })?;
        probe_llm_endpoint(
            &resolved.base_url,
            resolved.api_key.as_deref(),
            &config.model,
        )
        .await
        .map_err(to_runtime_error)
    }

    async fn stop(&self, runtime_id: &str) -> Result<(), RuntimeError> {
        self.stop_inner(runtime_id).await.map_err(to_runtime_error)
    }

    async fn attach(
        &self,
        runtime_id: &str,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        self.instance_for(runtime_id)
            .await
            .map(|_| ())
            .map_err(to_runtime_error)
    }

    async fn check_alive(&self, runtime_id: &str) -> Result<bool, RuntimeError> {
        let instance = self
            .instance_for(runtime_id)
            .await
            .map_err(to_runtime_error)?;
        let alive = instance
            .child
            .lock()
            .await
            .try_wait()
            .map_err(|error| RuntimeError::WorkerUnavailable {
                message: error.to_string(),
                retryable: true,
            })?
            .is_none();
        Ok(alive)
    }

    async fn submit_turn(
        &self,
        input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, RuntimeError> {
        self.submit_turn_inner(input)
            .await
            .map_err(to_runtime_error)
    }

    async fn answer_interaction(&self, input: RuntimeInteractionInput) -> Result<(), RuntimeError> {
        self.answer_interaction_inner(input)
            .await
            .map_err(to_runtime_error)
    }

    async fn cancel(&self, request: RuntimeCancelRequest) -> Result<(), RuntimeError> {
        self.cancel_inner(&request.runtime_id, request.turn_id.as_deref())
            .await
            .map_err(to_runtime_error)
    }

    async fn export_state(&self, runtime_id: &str) -> Result<OpaqueRuntimeState, RuntimeError> {
        let instance = self
            .instance_for(runtime_id)
            .await
            .map_err(to_runtime_error)?;
        Ok(instance.persisted_state.clone().into_opaque())
    }

    async fn export_checkpoint_state(
        &self,
        runtime_id: &str,
    ) -> Result<OpaqueRuntimeState, RuntimeError> {
        let instance = self
            .instance_for(runtime_id)
            .await
            .map_err(to_runtime_error)?;
        if instance.current_turn.lock().await.is_some() {
            return Err(RuntimeError::Conflict {
                code: "active_turn".into(),
                message: "cannot checkpoint an active Pi turn".into(),
            });
        }
        let mut state = instance.persisted_state.clone();
        let dir = PathBuf::from(&state.pi_session_dir);
        let session_jsonl =
            match session_file::latest_complete_turn_file(&dir) {
                Ok(path) => Some(tokio::fs::read_to_string(path).await.map_err(|e| {
                    RuntimeError::Internal {
                        message: e.to_string(),
                    }
                })?),
                Err(session_file::SessionFileError::NoSessionFiles(_)) => None,
                Err(error) => {
                    return Err(RuntimeError::StateCorrupt {
                        message: format!("cannot checkpoint Pi history: {error}"),
                    })
                }
            };
        let role = read_role(&dir).await.map_err(to_runtime_error)?;
        let mut llm_config = load_llm_config(&dir).await.map_err(to_runtime_error)?;
        if let Some(config) = llm_config.as_mut() {
            if config.credential_source.starts_with("env:") {
                config.api_key = None;
            }
        }
        let llm_config_ref = if let Some(config) = llm_config
            .as_ref()
            .filter(|config| config.api_key.is_some())
        {
            // Keep credentials out of opaque state/SQLite. The immutable private
            // sidecar survives source-session cleanup and is deleted with the checkpoint.
            let credential_dir = self.pi_session_root.join(".checkpoint-credentials");
            std::fs::create_dir_all(&credential_dir).map_err(|e| RuntimeError::Internal {
                message: e.to_string(),
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&credential_dir, std::fs::Permissions::from_mode(0o700))
                    .map_err(|e| RuntimeError::Internal {
                    message: e.to_string(),
                })?;
            }
            let id = format!("{}.json", Uuid::new_v4());
            let path = credential_dir.join(&id);
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(path).map_err(|e| RuntimeError::Internal {
                message: e.to_string(),
            })?;
            use std::io::Write;
            file.write_all(&serde_json::to_vec(&config).expect("LLM config serializes"))
                .map_err(|e| RuntimeError::Internal {
                    message: e.to_string(),
                })?;
            Some(id)
        } else {
            None
        };
        if llm_config_ref.is_some() {
            llm_config = None;
        }
        state.checkpoint = Some(PiCheckpoint {
            session_jsonl,
            llm_config_ref,
            llm_config,
            role,
        });
        Ok(state.into_opaque())
    }

    async fn delete_checkpoint_state(
        &self,
        opaque: &OpaqueRuntimeState,
    ) -> Result<(), RuntimeError> {
        let state = PiPersistedState::from_opaque(opaque).map_err(to_runtime_error)?;
        if let Some(reference) = state
            .checkpoint
            .and_then(|snapshot| snapshot.llm_config_ref)
        {
            let path = self
                .checkpoint_credential_path(&reference)
                .map_err(to_runtime_error)?;
            match tokio::fs::remove_file(path).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(RuntimeError::Internal {
                        message: e.to_string(),
                    })
                }
            }
        }
        Ok(())
    }
}

impl PiRuntime {
    fn checkpoint_credential_path(&self, reference: &str) -> Result<PathBuf, SessionDomainError> {
        if reference.contains('/') || reference.contains('\\') || !reference.ends_with(".json") {
            return Err(SessionDomainError::InvalidRequest {
                message: "invalid checkpoint credential reference".into(),
            });
        }
        Ok(self
            .pi_session_root
            .join(".checkpoint-credentials")
            .join(reference))
    }

    async fn start_inner(
        &self,
        request: RuntimeStartRequest,
        context: RuntimeExecutionContext,
    ) -> Result<(), SessionDomainError> {
        // Registry conflict check up front, shared by both branches — this
        // is what naturally prevents concurrent double-restoration (plan
        // §2 Phase 2 item 2: "先到者 insert，后到者 attach 命中").
        {
            let registry = self.instances.read().await;
            if registry.contains_key(&request.runtime_id) {
                return Err(SessionDomainError::Conflict {
                    message: format!("runtime '{}' is already started", request.runtime_id),
                });
            }
        }

        // Validate request-supplied model configuration before provisioning a
        // sandbox. A malformed llm block must have no provider-side effects.
        let requested_llm = resolve_llm(request.llm.as_ref())?;
        let created_branch = request
            .state
            .as_ref()
            .is_some_and(|state| state.state.get("checkpoint").is_some_and(|v| !v.is_null()));
        let plan = match &request.state {
            None => {
                self.prepare_cold_start(&request, context.operation_backend)
                    .await?
            }
            Some(state) => {
                self.prepare_resume(&request, state, context.operation_backend)
                    .await?
            }
        };
        let PreparedStart {
            backend,
            executable,
            extension_dir,
            session_dir,
            resume_session_file,
            persisted_state,
        } = plan;
        let llm = match requested_llm {
            Some(config) => Some(config),
            None => load_llm_config(&session_dir).await?,
        };

        let workspace_root = backend.paths().workspace_root().clone();
        let bridge_token = Uuid::new_v4().to_string();
        let activity = Arc::new(tokio::sync::RwLock::new(()));
        self.bridge.register(
            bridge_token.clone(),
            Arc::clone(&backend),
            workspace_root.clone(),
            Arc::clone(&activity),
        );

        let worker_config = worker::PiWorkerConfig {
            runtime_id: request.runtime_id.clone(),
            executable,
            extension_dir,
            session_dir: session_dir.clone(),
            resume_session_file,
            bridge_url: self.bridge.base_url(),
            bridge_token: bridge_token.clone(),
            workspace_root: workspace_root.0.clone(),
            use_workspace_cwd: persisted_state.backend_id == LOCAL_BACKEND_ID,
            llm,
        };
        let (child, stdin, stdout) =
            match worker::spawn_worker_process(&self.worker_executable, &worker_config).await {
                Ok(parts) => parts,
                Err(error) => {
                    self.bridge.unregister(&bridge_token);
                    if created_branch {
                        let _ = tokio::fs::remove_dir_all(&session_dir).await;
                    }
                    return Err(error);
                }
            };

        let instance = Arc::new(PiInstance {
            stdin: Mutex::new(stdin),
            child: Mutex::new(child),
            stdout: Mutex::new(stdout),
            current_turn: Mutex::new(None),
            bridge_token,
            persisted_state,
        });

        self.instances
            .write()
            .await
            .insert(request.runtime_id, instance);
        Ok(())
    }

    async fn stop_inner(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
        let instance = {
            let mut registry = self.instances.write().await;
            registry.remove(runtime_id)
        };
        let Some(instance) = instance else {
            return Err(SessionDomainError::NotFound {
                runtime_id: runtime_id.to_string(),
            });
        };
        let _ = send_worker_request(&instance, WorkerRequest::Shutdown).await;
        {
            let mut child = instance.child.lock().await;
            if let Err(error) = child.start_kill() {
                // Already exited is fine; anything else is worth logging but
                // not worth failing `stop` over — the registry entry is
                // already gone.
                tracing::debug!(runtime_id = %runtime_id, %error, "start_kill on Pi worker failed");
            }
            let _ = child.wait().await;
        }
        self.bridge.unregister(&instance.bridge_token);
        Ok(())
    }

    async fn submit_turn_inner(
        &self,
        input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, SessionDomainError> {
        // Keep malformed model overrides as synchronous API errors. The
        // worker performs the actual Pi model reconfiguration.
        resolve_llm(input.llm.as_ref())?;
        if let Some(value) = input.ext.get(EXT_NAMESPACE) {
            let role: PiRoleConfig = serde_json::from_value(value.clone()).map_err(|e| {
                SessionDomainError::InvalidRequest {
                    message: e.to_string(),
                }
            })?;
            role.validate()?;
        }
        let instance = self.instance_for(&input.runtime_id).await?;
        let (tx, rx) = mpsc::channel(32);

        {
            let mut guard = instance.current_turn.lock().await;
            if let Some(existing) = guard.as_ref() {
                return Err(SessionDomainError::Conflict {
                    message: format!(
                        "runtime '{}' already has an active turn '{}'",
                        input.runtime_id, existing.turn_id
                    ),
                });
            }
            *guard = Some(CurrentTurn {
                turn_id: input.turn_id.clone(),
                pending_interactions: HashMap::new(),
            });
        }

        if let Err(error) =
            send_worker_request(&instance, WorkerRequest::SubmitTurn(input.clone())).await
        {
            instance.current_turn.lock().await.take();
            return Err(error);
        }
        tokio::spawn(read_worker_turn(Arc::clone(&instance), input.turn_id, tx));

        Ok(rx)
    }

    async fn answer_interaction_inner(
        &self,
        input: RuntimeInteractionInput,
    ) -> Result<(), SessionDomainError> {
        let instance = self.instance_for(&input.runtime_id).await?;
        let method = {
            let mut current = instance.current_turn.lock().await;
            let turn = current
                .as_mut()
                .ok_or_else(|| SessionDomainError::Conflict {
                    message: format!("runtime '{}' has no active turn", input.runtime_id),
                })?;
            if turn.turn_id != input.turn_id {
                return Err(SessionDomainError::Conflict {
                    message: format!(
                        "interaction '{}' belongs to active turn '{}', not '{}'",
                        input.interaction_id, turn.turn_id, input.turn_id
                    ),
                });
            }
            turn.pending_interactions
                .remove(&input.interaction_id)
                .ok_or_else(|| SessionDomainError::Conflict {
                    message: format!("interaction '{}' is not pending", input.interaction_id),
                })?
        };
        map_answer_to_pi_value(&method, &input.answer)?;
        send_worker_request(&instance, WorkerRequest::AnswerInteraction(input)).await
    }

    async fn cancel_inner(
        &self,
        runtime_id: &str,
        turn_id: Option<&str>,
    ) -> Result<(), SessionDomainError> {
        let instance = self.instance_for(runtime_id).await?;
        let active = instance.current_turn.lock().await;
        let should_cancel = active
            .as_ref()
            .is_some_and(|active| turn_id.is_none() || turn_id == Some(active.turn_id.as_str()));
        drop(active);
        if !should_cancel {
            return Ok(());
        }
        send_worker_request(
            &instance,
            WorkerRequest::Cancel(RuntimeCancelRequest {
                runtime_id: runtime_id.into(),
                turn_id: turn_id.map(str::to_owned),
            }),
        )
        .await
    }
}

// Contract tests live in `tests/contract.rs`, not here: they need
// `env!("CARGO_BIN_EXE_fake_pi")` to locate the sibling `fake_pi` test binary
// (`src/bin/fake_pi.rs`), and Cargo only defines that variable while
// building integration tests/benchmarks — it is not available to unit tests
// compiled into the library target itself.

#[cfg(test)]
mod tests {
    use super::*;
    use session_protocol::SessionOpenRequest;

    fn open_request(workspace: session_protocol::WorkspaceSpec) -> SessionOpenRequest {
        SessionOpenRequest {
            runtime_id: None,
            runtime_kind: None,
            conversation_id: "conversation-1".into(),
            sender_id: "sender-1".into(),
            workspace,
            deployment: Default::default(),
            requested_capabilities: Default::default(),
            llm: None,
            ext: [(EXT_NAMESPACE.to_string(), json!({ "backend_id": "e2b" }))]
                .into_iter()
                .collect(),
            lease: Default::default(),
        }
    }

    fn local_open_request(workspace: session_protocol::WorkspaceSpec) -> SessionOpenRequest {
        let mut request = open_request(workspace);
        request.ext = [(EXT_NAMESPACE.to_string(), json!({ "backend_id": "local" }))]
            .into_iter()
            .collect();
        request
    }

    fn pi_normalizer() -> PiSessionEnvironment {
        PiSessionEnvironment::new(
            "/tmp/xgovernor-default",
            ["local".to_string(), "e2b".to_string()],
        )
    }

    #[tokio::test]
    async fn tenant_git_e2b_is_admitted_as_remote() {
        let normalized = pi_normalizer()
            .normalize(
                &SecurityContext::tenant("tenant-1", "principal-1"),
                &open_request(session_protocol::WorkspaceSpec::Git {
                    url: "https://example.com/org/repo.git".into(),
                    reference: None,
                    subdirectory: None,
                }),
            )
            .await
            .expect("tenant + git + e2b must be admitted");
        assert_eq!(normalized.isolation.boundary, IsolationBoundary::Remote);
        assert_eq!(normalized.workspace.root, E2B_WORKSPACE_ROOT);
        let metadata = normalized
            .workspace
            .metadata
            .as_object()
            .expect("git metadata");
        assert_eq!(metadata["url"], "https://example.com/org/repo.git");
    }

    #[tokio::test]
    async fn tenant_git_local_is_rejected() {
        let result = pi_normalizer()
            .normalize(
                &SecurityContext::tenant("tenant-1", "principal-1"),
                &local_open_request(session_protocol::WorkspaceSpec::Git {
                    url: "https://example.com/org/repo.git".into(),
                    reference: None,
                    subdirectory: None,
                }),
            )
            .await;
        assert!(
            matches!(result, Err(SessionDomainError::InvalidRequest { .. })),
            "tenant + git + host-boundary local backend must be rejected"
        );
    }

    #[tokio::test]
    async fn tenant_daemon_default_e2b_is_rejected() {
        let result = pi_normalizer()
            .normalize(
                &SecurityContext::tenant("tenant-1", "principal-1"),
                &open_request(session_protocol::WorkspaceSpec::DaemonDefault),
            )
            .await;
        assert!(
            matches!(result, Err(SessionDomainError::InvalidRequest { .. })),
            "tenant + non-git workspace must be rejected even with a sandboxed backend"
        );
    }

    #[tokio::test]
    async fn admin_daemon_default_e2b_reports_remote() {
        let normalized = pi_normalizer()
            .normalize(
                &SecurityContext::admin("admin-1"),
                &open_request(session_protocol::WorkspaceSpec::DaemonDefault),
            )
            .await
            .expect("admin + e2b must be admitted");
        assert_eq!(normalized.isolation.boundary, IsolationBoundary::Remote);
        assert_eq!(normalized.workspace.root, E2B_WORKSPACE_ROOT);
        assert_eq!(
            normalized.isolation.metadata["tool_backend_id"], "e2b",
            "isolation metadata must name the selected tool backend"
        );
    }

    #[tokio::test]
    async fn admin_local_daemon_default_reports_host() {
        let normalized = pi_normalizer()
            .normalize(
                &SecurityContext::admin("admin-1"),
                &local_open_request(session_protocol::WorkspaceSpec::DaemonDefault),
            )
            .await
            .expect("admin + local must be admitted");
        assert_eq!(normalized.isolation.boundary, IsolationBoundary::Host);
        assert_eq!(
            normalized.workspace.root, "/tmp/xgovernor-default",
            "local daemon_default keeps the daemon's default workspace root"
        );
    }

    #[tokio::test]
    async fn unconfigured_backend_is_rejected_at_open() {
        let normalizer = PiSessionEnvironment::new("/tmp/xgovernor-default", ["local".to_string()]);
        let mut request = open_request(session_protocol::WorkspaceSpec::DaemonDefault);
        request.ext = [(EXT_NAMESPACE.to_string(), json!({ "backend_id": "e2b" }))]
            .into_iter()
            .collect();
        let result = normalizer
            .normalize(&SecurityContext::admin("admin-1"), &request)
            .await;
        assert!(
            matches!(result, Err(SessionDomainError::InvalidRequest { .. })),
            "unconfigured backend must fail closed at open"
        );
    }

    #[tokio::test]
    async fn missing_ext_namespace_is_rejected() {
        let mut request = open_request(session_protocol::WorkspaceSpec::DaemonDefault);
        request.ext = Default::default();
        let result = pi_normalizer()
            .normalize(&SecurityContext::admin("admin-1"), &request)
            .await;
        assert!(
            matches!(result, Err(SessionDomainError::InvalidRequest { .. })),
            "missing runtime_pi ext must be rejected"
        );
    }

    fn sample_persisted_state() -> PiPersistedState {
        PiPersistedState {
            backend_id: "local".to_string(),
            executable: Some("/opt/homebrew/bin/pi".to_string()),
            extension_dir: None,
            pi_session_dir: "/tmp/xgovernor-test/pi-sessions/runtime-1".to_string(),
            runtime_id: Some("runtime-1".into()),
            workspace_metadata: Value::Null,
            checkpoint: None,
        }
    }

    /// `PiPersistedState::into_opaque`/`from_opaque` (`docs/pi_session_restore_plan.md`
    /// §1.1) must round-trip byte-for-byte through the `OpaqueRuntimeState`
    /// wrapper `export_state`/`SessionRecord.runtime` actually carry.
    #[test]
    fn persisted_state_round_trips_through_opaque_runtime_state() {
        let original = sample_persisted_state();
        let opaque = original.clone().into_opaque();
        assert_eq!(opaque.runtime_kind, "pi");
        assert_eq!(opaque.schema_version, PI_PERSISTED_STATE_SCHEMA_VERSION);
        let decoded = PiPersistedState::from_opaque(&opaque).expect("must decode what we encoded");
        assert_eq!(decoded, original);
    }

    /// Fail-closed per `docs/pi_session_restore_plan.md` §1.1: a
    /// `schema_version` this build doesn't recognize must be rejected, not
    /// guessed at.
    #[test]
    fn persisted_state_rejects_unrecognized_schema_version() {
        let mut opaque = sample_persisted_state().into_opaque();
        opaque.schema_version = PI_PERSISTED_STATE_SCHEMA_VERSION + 1;
        let result = PiPersistedState::from_opaque(&opaque);
        assert!(
            matches!(result, Err(SessionDomainError::Internal { .. })),
            "unrecognized schema_version must fail closed, not silently coerce"
        );
    }

    /// Belt-and-suspenders: state tagged for a different `runtime_kind` must
    /// also be rejected rather than interpreted as pi's own shape.
    #[test]
    fn persisted_state_rejects_foreign_runtime_kind() {
        let mut opaque = sample_persisted_state().into_opaque();
        opaque.runtime_kind = "xiaoo".to_string();
        let result = PiPersistedState::from_opaque(&opaque);
        assert!(
            matches!(result, Err(SessionDomainError::Internal { .. })),
            "state tagged for a different runtime_kind must not be interpreted as pi's"
        );
    }
}
