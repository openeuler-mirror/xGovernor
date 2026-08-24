pub mod bridge;
mod session_file;

use async_trait::async_trait;
use bridge::Bridge;
use operation_protocol::capability::exec::ExecRequest;
use operation_protocol::OperationBackend;
use provider_protocol::{BackendId, ProviderControlError};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use session_protocol::{
    LlmOverrideRequest, SessionExtensions, SessionInteractionAnswer, SessionInteractionOption,
    SessionRuntimeCapability, SessionToolActivityPhase, SessionToolActivityStatus,
    SessionTurnOutcome, SessionUsage,
};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::time::{timeout, Duration};
use uuid::Uuid;
use xgovernor_core::{
    enforce_workspace_axiom, CheckpointPayload, IsolationBoundary, IsolationFacts,
    NetworkIsolation, NormalizedSessionEnvironment, OpaqueRuntimeState, ResolvedLlm,
    RuntimeAdapter, RuntimeEvent, RuntimeEventReceiver, RuntimeFailure, RuntimeInteractionInput,
    RuntimeLoadRequest, RuntimeStartRequest, RuntimeTurnInput, SandboxCapability, SecurityContext,
    SessionDomainError, SessionEnvironmentNormalizer, WorkspaceAccess, WorkspaceFacts,
};
use xgovernor_manager::InstanceManager;

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
const PI_RPC_RESPONSE_TIMEOUT: Duration = Duration::from_secs(15);

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
    /// The `WorkspaceFacts.metadata` blob this session opened with (e.g. git
    /// clone parameters), stashed verbatim for a future explicit-rebuild path
    /// (`docs/pi_session_restore_plan.md` §4.3 / risk #3). Phase 1 only
    /// stores this; nothing reads it back yet.
    #[serde(default)]
    workspace_metadata: Value,
}

/// Bump whenever [`PiPersistedState`]'s shape changes. `from_opaque` fails
/// closed on any version it doesn't recognize (`docs/pi_session_restore_plan.md`
/// §1.1) rather than guessing at a shape it was never told about.
const PI_PERSISTED_STATE_SCHEMA_VERSION: u32 = 1;

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
        if opaque.schema_version != PI_PERSISTED_STATE_SCHEMA_VERSION {
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
    if parsed.backend_id.trim().is_empty() {
        return Err(SessionDomainError::InvalidRequest {
            message: format!("'{EXT_NAMESPACE}.backend_id' must not be empty"),
        });
    }
    Ok(parsed)
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

/// Maps a `ProviderControlError` (surfaced by the composed
/// `InstanceManager`s) to `SessionDomainError`. Identical in shape to
/// `runtime-local`/`runtime-e2b`'s `map_provider_error` — kept as a
/// per-crate copy rather than a shared helper for the same reason those two
/// don't share one: it is a three-line mapping, not enough surface to
/// justify a cross-crate dependency.
fn map_provider_error(error: ProviderControlError) -> SessionDomainError {
    match error {
        ProviderControlError::NotFound { resource_ref } => SessionDomainError::NotFound {
            runtime_id: resource_ref,
        },
        ProviderControlError::InvalidRequest { message } => {
            SessionDomainError::InvalidRequest { message }
        }
        ProviderControlError::ResourceLimitExceeded { .. } => SessionDomainError::Unavailable {
            message: error.to_string(),
        },
        other => SessionDomainError::Internal {
            message: other.to_string(),
            source: None,
        },
    }
}

/// The turn `submit_turn` most recently accepted for a given [`PiInstance`],
/// still tracked here until an `agent_settled` (or process exit) resolves it.
/// A `pi` process only ever drives one turn at a time in this adapter's usage
/// (`submit_turn` rejects a second concurrent submission — see its impl), so
/// a single `Option` slot, not a map keyed by `turn_id`, is enough.
struct CurrentTurn {
    turn_id: String,
    tx: mpsc::Sender<RuntimeEvent>,
    /// Set by `cancel()` right before it sends `abort` to the process. Read
    /// by the `agent_settled` handler to decide whether the terminal event
    /// should report `Cancelled` or `Complete` — using our own request
    /// rather than trying to parse Pi's `stopReason` back out, since we
    /// already know unambiguously whether we asked for an abort.
    aborted: bool,
    /// `interaction_id` -> Pi dialog `method`, so `answer_interaction` knows
    /// how to shape the `extension_ui_response` value it sends back.
    pending_interactions: HashMap<String, String>,
    /// Monotonic counter backing `OutputDelta.sequence`. Deliberately not
    /// derived from Pi's own `contentIndex` (which resets per content
    /// block/message) since `RuntimeEvent::OutputDelta.sequence` is meant to
    /// be strictly increasing across the whole turn.
    output_sequence: u64,
}

impl CurrentTurn {
    fn next_output_sequence(&mut self) -> u64 {
        let sequence = self.output_sequence;
        self.output_sequence += 1;
        sequence
    }
}

/// One running `pi --mode rpc` subprocess and the plumbing needed to write
/// commands to its stdin and correlate its stdout events back to whichever
/// turn is currently in flight.
struct PiInstance {
    stdin: Mutex<ChildStdin>,
    child: Mutex<Child>,
    current_turn: Mutex<Option<CurrentTurn>>,
    /// Correlates the small number of RPC commands for which the adapter must
    /// observe acceptance before continuing (currently per-turn model
    /// selection). Normal prompts keep their existing asynchronous contract.
    pending_responses: Mutex<HashMap<String, oneshot::Sender<Value>>>,
    /// Bearer token this instance's backend is registered under on the
    /// shared [`Bridge`]. `stop()` unregisters it so a dangling token can't
    /// keep proxying to a backend whose sandbox is about to be torn down.
    bridge_token: String,
    /// The `InstanceManager` (looked up by `ext.backend_id` in `start()`)
    /// that provisioned this instance's sandbox, kept so `stop()` can call
    /// `stop_instance` on the *same* manager without re-deriving it from
    /// `ext` (the ext payload isn't available in `stop()`, which only
    /// receives a `runtime_id`).
    manager: Arc<InstanceManager>,
    /// Everything needed to reconstruct this instance's `start()` call after
    /// a restart — see [`PiPersistedState`]. Immutable snapshot taken at
    /// `start()` time; `export_state()` just clones and re-wraps it.
    persisted_state: PiPersistedState,
    session_dir: PathBuf,
    activity: Arc<tokio::sync::RwLock<()>>,
}

/// `RuntimeAdapter` backed by real `pi --mode rpc` subprocesses, one per
/// started `runtime_id`. Composes one `InstanceManager` per provider
/// `backend_id` this runtime is willing to provision sandboxes against
/// (`managers`), plus a single shared [`Bridge`] HTTP server (bound once at
/// construction, alive for this `PiRuntime`'s entire lifetime) that proxies
/// Pi's tool calls to whichever backend `start()` attached for that session.
/// See the module doc for why the `pi` process's own RPC control channel is
/// still `tokio::process`, not `operation-protocol`.
pub struct PiRuntime {
    managers: HashMap<String, Arc<InstanceManager>>,
    bridge: Arc<Bridge>,
    instances: RwLock<HashMap<String, Arc<PiInstance>>>,
    pi_session_root: PathBuf,
}

impl PiRuntime {
    /// `managers` maps `backend_id` (the same string `ext.runtime_pi.backend_id`
    /// on a start request must name) to the `InstanceManager` that should
    /// provision sandboxes for it — e.g. `{"local": <InstanceManager over
    /// LocalProvider>, "e2b": <InstanceManager over E2bProvider>}`. A
    /// `backend_id` naming a manager not present here is rejected by
    /// `start()` as `InvalidRequest` (see that method): there is no sandbox
    /// implementation to route to, so silently falling back to some default
    /// would hide a caller/config bug rather than surface it.
    ///
    /// `pi_session_root` is where each started session's `--session-dir`
    /// subdirectory (named after its `runtime_id`) is created — see the
    /// field doc on [`PiRuntime::pi_session_root`].
    ///
    /// Binds the bridge's HTTP listener synchronously as part of
    /// construction (`Bridge::spawn`), hence the `std::io::Result` return —
    /// this is the one fallible step in bringing up a `PiRuntime`.
    pub fn new(
        managers: HashMap<String, Arc<InstanceManager>>,
        pi_session_root: PathBuf,
    ) -> std::io::Result<Self> {
        let bridge = Bridge::spawn()?;
        Ok(Self {
            managers,
            bridge,
            instances: RwLock::new(HashMap::new()),
            pi_session_root,
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
    ) -> Result<PreparedStart, SessionDomainError> {
        let ext = read_pi_runtime_ext(&request.ext)?;
        let executable = ext
            .executable
            .clone()
            .or_else(|| std::env::var(PI_EXECUTABLE_ENV).ok())
            .unwrap_or_else(|| DEFAULT_PI_EXECUTABLE.to_string());
        let extension_dir = resolve_extension_dir(&ext);

        let manager = self.managers.get(&ext.backend_id).cloned().ok_or_else(|| {
            SessionDomainError::InvalidRequest {
                message: format!(
                    "no InstanceManager configured for backend_id '{}'; this PiRuntime only \
                     knows about: {:?}",
                    ext.backend_id,
                    self.managers.keys().collect::<Vec<_>>()
                ),
            }
        })?;

        // E2B sandboxes created by PI sessions keep internet access, exactly
        // like `apps/runtime-mock`'s `MockRuntime::start` does
        // (create-time-only knob); `PiSessionEnvironment` therefore reports
        // `NetworkIsolation::None`, not a stronger claim. `LocalProvider` has
        // no such option — local backends are host processes — so it is only
        // sent for the e2b backend.
        let mut provider_options = json!({ "workspace_root": request.workspace.root });
        if ext.backend_id == E2B_BACKEND_ID {
            provider_options["allow_internet_access"] = json!(true);
        }
        let backend: Arc<dyn OperationBackend> = manager
            .start_instance(
                request.runtime_id.clone(),
                BackendId(ext.backend_id.clone()),
                request.owner_ref.clone(),
                provider_options,
            )
            .await
            .map_err(map_provider_error)?;

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
                let _ = manager.stop_instance(&request.runtime_id).await;
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
            let _ = manager.stop_instance(&request.runtime_id).await;
            return Err(SessionDomainError::Unavailable {
                message: format!(
                    "failed to create pi session directory {}: {error}",
                    session_dir.display()
                ),
            });
        }

        let persisted_state = PiPersistedState {
            backend_id: ext.backend_id.clone(),
            executable: ext.executable.clone(),
            extension_dir: ext.extension_dir.clone(),
            pi_session_dir: session_dir.to_string_lossy().into_owned(),
            workspace_metadata: workspace_metadata_snapshot,
        };

        Ok(PreparedStart {
            manager,
            backend,
            executable,
            extension_dir,
            session_dir,
            resume_session_file: None,
            persisted_state,
            destroy_sandbox_on_spawn_failure: true,
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
    ) -> Result<PreparedStart, SessionDomainError> {
        let state = PiPersistedState::from_opaque(opaque_state)?;

        let manager = self
            .managers
            .get(&state.backend_id)
            .cloned()
            .ok_or_else(|| SessionDomainError::InvalidRequest {
                message: format!(
                    "no InstanceManager configured for backend_id '{}' from persisted state; \
                     this PiRuntime only knows about: {:?}",
                    state.backend_id,
                    self.managers.keys().collect::<Vec<_>>()
                ),
            })?;

        // Read-only lookup into the registry `InstanceManager::reconcile()`
        // repopulated at startup (F7) — never provisions. `NotFound` here is
        // exactly §1.4's "sandbox already dead" signal (e2b lease expired,
        // or `reconcile()` itself decided the ledger row was orphaned).
        let backend = manager
            .backend_for(&request.runtime_id)
            .map_err(|error| match error {
                ProviderControlError::NotFound { .. } => SessionDomainError::Unavailable {
                    message: format!(
                        "pi_sandbox_gone: sandbox for runtime '{}' is no longer tracked by its \
                         InstanceManager (likely reclaimed or expired since the last restart); \
                         cannot resume — a fresh explicit open is required instead",
                        request.runtime_id
                    ),
                },
                other => map_provider_error(other),
            })?;

        let executable = state
            .executable
            .clone()
            .or_else(|| std::env::var(PI_EXECUTABLE_ENV).ok())
            .unwrap_or_else(|| DEFAULT_PI_EXECUTABLE.to_string());
        let extension_dir = state
            .extension_dir
            .clone()
            .unwrap_or_else(|| DEFAULT_EXTENSION_DIR.to_string());

        let session_dir = PathBuf::from(&state.pi_session_dir);
        let resume_session_file =
            session_file::latest_complete_turn_file(&session_dir).map_err(|error| {
                SessionDomainError::Unavailable {
                    message: format!("pi_session_state_lost: {error}"),
                }
            })?;

        Ok(PreparedStart {
            manager,
            backend,
            executable,
            extension_dir,
            session_dir,
            resume_session_file: Some(resume_session_file),
            persisted_state: state,
            destroy_sandbox_on_spawn_failure: false,
        })
    }
}

fn copy_dir<'a>(
    src: &'a PathBuf,
    dst: &'a PathBuf,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SessionDomainError>> + Send + 'a>>
{
    Box::pin(async move {
        let mut entries =
            tokio::fs::read_dir(src)
                .await
                .map_err(|e| SessionDomainError::Unavailable {
                    message: format!("failed to read session directory: {e}"),
                })?;
        while let Some(entry) =
            entries
                .next_entry()
                .await
                .map_err(|e| SessionDomainError::Unavailable {
                    message: e.to_string(),
                })?
        {
            let target = dst.join(entry.file_name());
            let ty = entry
                .file_type()
                .await
                .map_err(|e| SessionDomainError::Unavailable {
                    message: e.to_string(),
                })?;
            if ty.is_dir() {
                tokio::fs::create_dir_all(&target).await.map_err(|e| {
                    SessionDomainError::Unavailable {
                        message: e.to_string(),
                    }
                })?;
                copy_dir(&entry.path(), &target).await?;
            } else {
                tokio::fs::copy(entry.path(), target).await.map_err(|e| {
                    SessionDomainError::Unavailable {
                        message: e.to_string(),
                    }
                })?;
            }
        }
        Ok(())
    })
}

/// Everything `start()` needs to spawn `pi` and register the resulting
/// `PiInstance`, produced by either `prepare_cold_start` or `prepare_resume`
/// so the rest of `start()` — bridge registration, `Command` construction,
/// bookkeeping — is shared between the two branches instead of duplicated.
struct PreparedStart {
    manager: Arc<InstanceManager>,
    backend: Arc<dyn OperationBackend>,
    executable: String,
    extension_dir: String,
    session_dir: PathBuf,
    /// `Some(path)` on a restoration start: passed to `pi` as `--session
    /// <path>` in addition to `--session-dir`. `None` on a cold start.
    resume_session_file: Option<PathBuf>,
    persisted_state: PiPersistedState,
    /// Whether a spawn failure should tear down `backend`'s sandbox. `true`
    /// for a cold start (this call is the sandbox's sole owner so far);
    /// `false` for a resume (the sandbox predates this call and outlives a
    /// `pi`-spawn failure — see `prepare_resume`'s doc).
    destroy_sandbox_on_spawn_failure: bool,
}

async fn write_command(instance: &PiInstance, command: &Value) -> std::io::Result<()> {
    let mut line = serde_json::to_string(command).expect("command must serialize to JSON");
    line.push('\n');
    let mut stdin = instance.stdin.lock().await;
    stdin.write_all(line.as_bytes()).await?;
    stdin.flush().await
}

async fn write_command_for_response(
    instance: &PiInstance,
    command: Value,
) -> Result<Value, SessionDomainError> {
    let id = command
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| SessionDomainError::Internal {
            message: "correlated pi RPC command is missing an id".to_string(),
            source: None,
        })?
        .to_string();
    let (tx, rx) = oneshot::channel();
    instance
        .pending_responses
        .lock()
        .await
        .insert(id.clone(), tx);
    if let Err(error) = write_command(instance, &command).await {
        instance.pending_responses.lock().await.remove(&id);
        return Err(SessionDomainError::Unavailable {
            message: format!("failed to write correlated pi RPC command: {error}"),
        });
    }
    let response = timeout(PI_RPC_RESPONSE_TIMEOUT, rx)
        .await
        .map_err(|_| SessionDomainError::Timeout {
            operation: "pi_rpc_response".to_string(),
            timeout_ms: PI_RPC_RESPONSE_TIMEOUT.as_millis() as u64,
        })?
        .map_err(|_| SessionDomainError::Unavailable {
            message: "pi process exited before replying to RPC command".to_string(),
        })?;
    if !response
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(SessionDomainError::InvalidRequest {
            message: response
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("pi rejected model configuration")
                .to_string(),
        });
    }
    Ok(response)
}

async fn configure_instance_llm(
    instance: &PiInstance,
    config: &PiLlmConfig,
) -> Result<(), SessionDomainError> {
    use base64::Engine as _;

    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(config).expect("PiLlmConfig always serializes"));
    let id = format!("xgovernor-model-{}", Uuid::new_v4());
    write_command_for_response(
        instance,
        json!({
            "id": id,
            "type": "prompt",
            "message": format!("/{PI_MODEL_COMMAND} {encoded}"),
        }),
    )
    .await?;
    write_command_for_response(
        instance,
        json!({
            "id": format!("xgovernor-set-model-{}", Uuid::new_v4()),
            "type": "set_model",
            "provider": config.provider,
            "modelId": config.model,
        }),
    )
    .await?;
    persist_llm_config(&instance.session_dir, config).await
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

async fn handle_response(instance: &PiInstance, message: &Value) {
    if let Some(id) = message.get("id").and_then(Value::as_str) {
        if let Some(waiter) = instance.pending_responses.lock().await.remove(id) {
            let _ = waiter.send(message.clone());
            return;
        }
    }
    let command = message.get("command").and_then(Value::as_str).unwrap_or("");
    let success = message
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    // Only the `prompt` command's rejection matters here: it means the turn
    // `submit_turn` thought it had accepted never actually started, so the
    // caller needs a terminal `Failed` event rather than silence. `abort`'s
    // response is not surfaced — a failed abort is not actionable beyond
    // what already happens (the turn simply keeps running to its own
    // terminal state).
    if command != "prompt" || success {
        return;
    }
    let error_text = message
        .get("error")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| "pi rejected the prompt command".to_string());

    let mut guard = instance.current_turn.lock().await;
    let Some(turn) = guard.take() else { return };
    drop(guard);
    let _ = turn
        .tx
        .send(RuntimeEvent::Failed {
            error: RuntimeFailure {
                code: "pi_prompt_rejected".to_string(),
                message: error_text,
                retryable: false,
                details: message.clone(),
            },
            usage: SessionUsage::default(),
        })
        .await;
}

async fn handle_message_update(instance: &PiInstance, message: &Value) {
    // Real pi RPC shape (verified against pi 0.84.2 on the wire): text
    // streamed during a turn arrives as `message_update` with the delta
    // under `assistantMessageEvent: {"type": "text_delta"|"thinking_delta",
    // "delta": "..."}` — there is no top-level `delta` field. `text_start`/
    // `text_end`/`thinking_start`/`thinking_end` carry no per-chunk text
    // (the `_end` variants repeat the full content, which the deltas already
    // covered), so only the two `*_delta` types are mapped.
    let Some(assistant_event) = message.get("assistantMessageEvent") else {
        return;
    };
    let delta_type = assistant_event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("");
    let text = match delta_type {
        "text_delta" | "thinking_delta" => assistant_event.get("delta").and_then(Value::as_str),
        // `toolcall_delta` (incremental tool-call argument streaming) has no
        // `OutputDelta` equivalent in the normalized vocabulary; the
        // completed call is instead surfaced via `tool_execution_start`/
        // `tool_execution_end` below as a `ToolActivity` pair.
        _ => None,
    };
    let Some(text) = text else { return };
    let stream_id = if delta_type == "thinking_delta" {
        "thinking"
    } else {
        "assistant"
    };

    let mut guard = instance.current_turn.lock().await;
    let Some(turn) = guard.as_mut() else { return };
    let sequence = turn.next_output_sequence();
    let _ = turn
        .tx
        .send(RuntimeEvent::OutputDelta {
            stream_id: stream_id.to_string(),
            sequence,
            delta: text.to_string(),
        })
        .await;
}

async fn handle_tool_execution_start(instance: &PiInstance, message: &Value) {
    let Some(activity_id) = message.get("toolCallId").and_then(Value::as_str) else {
        return;
    };
    let name = message
        .get("toolName")
        .and_then(Value::as_str)
        .unwrap_or("tool")
        .to_string();
    let summary = message.get("input").map(|value| value.to_string());

    let mut guard = instance.current_turn.lock().await;
    let Some(turn) = guard.as_mut() else { return };
    let _ = turn
        .tx
        .send(RuntimeEvent::ToolActivity {
            activity_id: activity_id.to_string(),
            phase: SessionToolActivityPhase::Begin,
            name,
            status: SessionToolActivityStatus::Running,
            summary,
            ext: Default::default(),
        })
        .await;
}

async fn handle_tool_execution_end(instance: &PiInstance, message: &Value) {
    let Some(activity_id) = message.get("toolCallId").and_then(Value::as_str) else {
        return;
    };
    let name = message
        .get("toolName")
        .and_then(Value::as_str)
        .unwrap_or("tool")
        .to_string();
    let is_error = message
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let status = if is_error {
        SessionToolActivityStatus::Failed
    } else {
        SessionToolActivityStatus::Succeeded
    };
    let summary = message.get("result").map(|value| value.to_string());

    let mut guard = instance.current_turn.lock().await;
    let Some(turn) = guard.as_mut() else { return };
    let _ = turn
        .tx
        .send(RuntimeEvent::ToolActivity {
            activity_id: activity_id.to_string(),
            phase: SessionToolActivityPhase::End,
            name,
            status,
            summary,
            ext: Default::default(),
        })
        .await;
}

async fn handle_extension_ui_request(instance: &PiInstance, message: &Value) {
    let Some(id) = message.get("id").and_then(Value::as_str) else {
        return;
    };
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return;
    };
    let params = message.get("params").cloned().unwrap_or(Value::Null);

    if !DIALOG_METHODS.contains(&method) {
        // Fire-and-forget UI directive: no `extension_ui_response` is
        // expected, so this cannot be modeled as `InteractionRequested`
        // (which implies the runtime is blocked awaiting
        // `answer_interaction`). Surface it through the `Extension` escape
        // hatch instead of silently dropping it.
        let mut guard = instance.current_turn.lock().await;
        if let Some(turn) = guard.as_mut() {
            let _ = turn
                .tx
                .send(RuntimeEvent::Extension {
                    namespace: "pi.ui".to_string(),
                    payload: json!({ "method": method, "params": params }),
                })
                .await;
        }
        return;
    }

    let prompt = params
        .get("message")
        .or_else(|| params.get("prompt"))
        .and_then(Value::as_str)
        .unwrap_or(method)
        .to_string();

    let options: Vec<SessionInteractionOption> = params
        .get("options")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .enumerate()
                .filter_map(|(index, option)| match option {
                    Value::String(text) => Some(SessionInteractionOption {
                        id: text.clone(),
                        label: text.clone(),
                        description: None,
                        value: option.clone(),
                    }),
                    Value::Object(_) => {
                        let id = option
                            .get("id")
                            .or_else(|| option.get("value"))
                            .and_then(Value::as_str)
                            .map(str::to_string)
                            .unwrap_or_else(|| index.to_string());
                        let label = option
                            .get("label")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        Some(SessionInteractionOption {
                            id,
                            label,
                            description: None,
                            value: option.clone(),
                        })
                    }
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();

    let mut guard = instance.current_turn.lock().await;
    let Some(turn) = guard.as_mut() else {
        // No active turn to attach this interaction to — should not happen
        // in practice, since Pi only emits UI requests while driving a turn
        // this adapter submitted. Nothing to do but drop it.
        return;
    };
    turn.pending_interactions
        .insert(id.to_string(), method.to_string());
    let _ = turn
        .tx
        .send(RuntimeEvent::InteractionRequested {
            interaction_id: id.to_string(),
            interaction_kind: method.to_string(),
            prompt,
            options,
            ext: Default::default(),
        })
        .await;
}

async fn handle_agent_settled(instance: &PiInstance, message: &Value) {
    let mut guard = instance.current_turn.lock().await;
    let Some(turn) = guard.take() else { return };
    drop(guard);

    let usage = extract_usage(message);

    if let Some(error) = message.get("error").filter(|value| !value.is_null()) {
        let _ = turn
            .tx
            .send(RuntimeEvent::Failed {
                error: RuntimeFailure {
                    code: "pi_agent_error".to_string(),
                    message: error
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| error.to_string()),
                    retryable: false,
                    details: error.clone(),
                },
                usage,
            })
            .await;
        return;
    }

    let outcome = if turn.aborted {
        SessionTurnOutcome::Cancelled
    } else {
        SessionTurnOutcome::Complete
    };
    let _ = turn
        .tx
        .send(RuntimeEvent::Completed { outcome, usage })
        .await;
}

async fn handle_pi_message(instance: &PiInstance, message: Value) {
    let Some(kind) = message.get("type").and_then(Value::as_str) else {
        tracing::warn!(?message, "pi rpc event missing 'type'");
        return;
    };

    match kind {
        "response" => handle_response(instance, &message).await,
        "message_update" => handle_message_update(instance, &message).await,
        "tool_execution_start" => handle_tool_execution_start(instance, &message).await,
        "tool_execution_end" => handle_tool_execution_end(instance, &message).await,
        "extension_ui_request" => handle_extension_ui_request(instance, &message).await,
        "agent_settled" => handle_agent_settled(instance, &message).await,
        // `agent_start`/`message_start`/`message_end`/`turn_start`/
        // `turn_end`/`tool_execution_update`/`agent_end`: no direct
        // `RuntimeEvent` mapping in this minimal-closed-loop scope.
        // `agent_end` may carry `willRetry: true`, meaning `agent_settled`
        // (the true terminal point, per Pi's own docs) has not fired yet —
        // deliberately not treated as terminal here.
        _ => {}
    }
}

async fn read_events(instance: Arc<PiInstance>, stdout: ChildStdout) {
    let mut lines = BufReader::new(stdout).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<Value>(&line) {
                    Ok(parsed) => handle_pi_message(&instance, parsed).await,
                    Err(error) => {
                        tracing::warn!(%error, %line, "failed to parse pi rpc event line");
                    }
                }
            }
            Ok(None) => break,
            Err(error) => {
                tracing::warn!(%error, "error reading pi rpc stdout");
                break;
            }
        }
    }

    let pending = std::mem::take(&mut *instance.pending_responses.lock().await);
    drop(pending);

    // The process ended (or its stdout pipe broke) while a turn was still in
    // flight: surface a terminal `Failed` event so the caller does not hang
    // forever waiting for one that will never come.
    if let Some(turn) = instance.current_turn.lock().await.take() {
        let _ = turn
            .tx
            .send(RuntimeEvent::Failed {
                error: RuntimeFailure {
                    code: "pi_process_exited".to_string(),
                    message: "pi rpc process exited before the turn reached a terminal state"
                        .to_string(),
                    retryable: false,
                    details: Value::Null,
                },
                usage: SessionUsage::default(),
            })
            .await;
    }
}

async fn log_stderr(runtime_id: String, stderr: ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::debug!(runtime_id = %runtime_id, "pi stderr: {line}");
    }
}

#[async_trait]
impl RuntimeAdapter for PiRuntime {
    fn kind(&self) -> &str {
        "pi"
    }

    fn capabilities(&self) -> BTreeSet<SessionRuntimeCapability> {
        let mut capabilities = BTreeSet::from([SessionRuntimeCapability::Interaction]);
        capabilities.insert(SessionRuntimeCapability::ModelOverride);
        if self.managers.contains_key(E2B_BACKEND_ID) {
            capabilities.insert(SessionRuntimeCapability::Checkpoint);
        }
        capabilities
    }

    fn capabilities_for_request(
        &self,
        request: &session_protocol::SessionOpenRequest,
    ) -> BTreeSet<SessionRuntimeCapability> {
        let mut capabilities = self.capabilities();
        let backend_id = request
            .ext
            .get(EXT_NAMESPACE)
            .and_then(|value| value.get("backend_id"))
            .and_then(Value::as_str);
        if backend_id != Some(E2B_BACKEND_ID) {
            capabilities.remove(&SessionRuntimeCapability::Checkpoint);
        }
        capabilities
    }

    async fn start(&self, request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
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
        let plan = match &request.state {
            None => self.prepare_cold_start(&request).await?,
            Some(state) => self.prepare_resume(&request, state).await?,
        };
        let PreparedStart {
            manager,
            backend,
            executable,
            extension_dir,
            session_dir,
            resume_session_file,
            persisted_state,
            destroy_sandbox_on_spawn_failure,
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

        let mut command = Command::new(&executable);
        command
            .arg("--mode")
            .arg("rpc")
            .arg("-e")
            .arg(&extension_dir)
            .arg("--session-dir")
            .arg(&session_dir)
            .env("XGOVERNOR_BRIDGE_URL", self.bridge.base_url())
            .env("XGOVERNOR_BRIDGE_TOKEN", &bridge_token)
            .env("XGOVERNOR_WORKSPACE_ROOT", &workspace_root.0)
            // Deliberately no `.current_dir(...)`: Pi's own built-in tools no
            // longer touch the daemon host's filesystem at all (that is the
            // whole point of the bridge), so there is no host-side directory
            // for this process to be rooted in.
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // Restoration only (`prepare_resume` set this): point `pi` at the
        // exact session file it should reload, on top of `--session-dir`
        // (`docs/pi_session_restore_plan.md` §1.2).
        if let Some(session_file) = &resume_session_file {
            command.arg("--session").arg(session_file);
        }
        if let Err(error) = configure_pi_launch(&mut command, &session_dir, llm.as_ref()).await {
            self.bridge.unregister(&bridge_token);
            if destroy_sandbox_on_spawn_failure {
                let _ = manager.stop_instance(&request.runtime_id).await;
            }
            return Err(error);
        }

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                // Compensating cleanup: the bridge registration must not
                // outlive the process that was supposed to use it. The
                // sandbox itself is only ours to tear down on a cold start —
                // see `PreparedStart::destroy_sandbox_on_spawn_failure`'s doc.
                self.bridge.unregister(&bridge_token);
                if destroy_sandbox_on_spawn_failure {
                    let _ = manager.stop_instance(&request.runtime_id).await;
                }
                return Err(SessionDomainError::Unavailable {
                    message: format!("failed to spawn pi executable '{executable}': {error}"),
                });
            }
        };

        let stdin = child.stdin.take().expect("stdin was piped");
        let stdout = child.stdout.take().expect("stdout was piped");
        let stderr = child.stderr.take();

        let instance = Arc::new(PiInstance {
            stdin: Mutex::new(stdin),
            child: Mutex::new(child),
            current_turn: Mutex::new(None),
            pending_responses: Mutex::new(HashMap::new()),
            bridge_token,
            manager,
            persisted_state,
            session_dir,
            activity,
        });

        tokio::spawn(read_events(Arc::clone(&instance), stdout));
        if let Some(stderr) = stderr {
            tokio::spawn(log_stderr(request.runtime_id.clone(), stderr));
        }

        self.instances
            .write()
            .await
            .insert(request.runtime_id, instance);
        Ok(())
    }

    async fn stop(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
        let instance = {
            let mut registry = self.instances.write().await;
            registry.remove(runtime_id)
        };
        let Some(instance) = instance else {
            return Err(SessionDomainError::NotFound {
                runtime_id: runtime_id.to_string(),
            });
        };
        {
            let mut child = instance.child.lock().await;
            if let Err(error) = child.start_kill() {
                // Already exited is fine; anything else is worth logging but
                // not worth failing `stop` over — the registry entry is
                // already gone.
                tracing::debug!(runtime_id = %runtime_id, %error, "start_kill on pi process failed");
            }
            let _ = child.wait().await;
        }
        self.bridge.unregister(&instance.bridge_token);
        instance
            .manager
            .stop_instance(runtime_id)
            .await
            .map_err(map_provider_error)
    }

    async fn checkpoint(&self, runtime_id: &str) -> Result<CheckpointPayload, SessionDomainError> {
        let instance = self.instance_for(runtime_id).await?;
        let _freeze = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            instance.activity.write(),
        )
        .await
        .map_err(|_| SessionDomainError::Unavailable {
            message: "checkpoint freeze timed out".into(),
        })?;
        let snapshot = instance
            .manager
            .checkpoint_instance(runtime_id)
            .await
            .map_err(map_provider_error)?;
        let checkpoint_id = format!("checkpoint-{}", Uuid::new_v4());
        let archive_root = self
            .pi_session_root
            .parent()
            .unwrap_or(&self.pi_session_root)
            .join("pi-checkpoints");
        let archive = archive_root.join(&checkpoint_id);
        tokio::fs::create_dir_all(&archive)
            .await
            .map_err(|e| SessionDomainError::Unavailable {
                message: format!("failed to create checkpoint archive: {e}"),
            })?;
        copy_dir(
            &PathBuf::from(&instance.persisted_state.pi_session_dir),
            &archive,
        )
        .await?;
        let mut state = instance.persisted_state.clone();
        state.pi_session_dir = archive.to_string_lossy().into_owned();
        Ok(CheckpointPayload {
            checkpoint_id,
            runtime_state: state.into_opaque(),
            provider_snapshot_id: snapshot.snapshot_id.0,
        })
    }

    async fn load_from_checkpoint(
        &self,
        request: RuntimeLoadRequest,
    ) -> Result<(), SessionDomainError> {
        let state = PiPersistedState::from_opaque(&request.runtime_state)?;
        let requested_llm = resolve_llm(request.llm.as_ref())?;
        let manager = self
            .managers
            .get(&state.backend_id)
            .cloned()
            .ok_or_else(|| SessionDomainError::InvalidRequest {
                message: format!("unknown backend_id '{}'", state.backend_id),
            })?;
        let provider_options = if state.backend_id == E2B_BACKEND_ID {
            json!({"workspace_root": E2B_WORKSPACE_ROOT, "allow_internet_access": true})
        } else {
            json!({"workspace_root": E2B_WORKSPACE_ROOT})
        };
        let backend = manager
            .load_instance_from_snapshot(
                request.new_runtime_id.clone(),
                BackendId(state.backend_id.clone()),
                request.owner_ref,
                provider_protocol::ProviderSnapshotId(request.provider_snapshot_id),
                provider_options,
            )
            .await
            .map_err(map_provider_error)?;
        let session_dir = self.pi_session_root.join(&request.new_runtime_id);
        tokio::fs::create_dir_all(&session_dir).await.map_err(|e| {
            SessionDomainError::Unavailable {
                message: e.to_string(),
            }
        })?;
        copy_dir(&PathBuf::from(&state.pi_session_dir), &session_dir).await?;
        let llm = match requested_llm {
            Some(config) => Some(config),
            None => load_llm_config(&session_dir).await?,
        };
        let executable = state
            .executable
            .clone()
            .or_else(|| std::env::var(PI_EXECUTABLE_ENV).ok())
            .unwrap_or_else(|| DEFAULT_PI_EXECUTABLE.to_string());
        let extension_dir = state
            .extension_dir
            .clone()
            .unwrap_or_else(|| DEFAULT_EXTENSION_DIR.to_string());
        let token = Uuid::new_v4().to_string();
        let activity = Arc::new(tokio::sync::RwLock::new(()));
        let workspace_root = backend.paths().workspace_root().clone();
        self.bridge.register(
            token.clone(),
            Arc::clone(&backend),
            workspace_root.clone(),
            Arc::clone(&activity),
        );
        let mut command = Command::new(executable);
        command
            .arg("--mode")
            .arg("rpc")
            .arg("-e")
            .arg(extension_dir)
            .arg("--session-dir")
            .arg(&session_dir)
            .env("XGOVERNOR_BRIDGE_URL", self.bridge.base_url())
            .env("XGOVERNOR_BRIDGE_TOKEN", &token)
            .env("XGOVERNOR_WORKSPACE_ROOT", &workspace_root.0)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Ok(file) = session_file::latest_complete_turn_file(&session_dir) {
            command.arg("--session").arg(file);
        }
        if let Err(error) = configure_pi_launch(&mut command, &session_dir, llm.as_ref()).await {
            self.bridge.unregister(&token);
            let _ = manager.stop_instance(&request.new_runtime_id).await;
            return Err(error);
        }
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                self.bridge.unregister(&token);
                let _ = manager.stop_instance(&request.new_runtime_id).await;
                return Err(SessionDomainError::Unavailable {
                    message: format!("failed to spawn pi from checkpoint: {error}"),
                });
            }
        };
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take();
        let instance = Arc::new(PiInstance {
            stdin: Mutex::new(stdin),
            child: Mutex::new(child),
            current_turn: Mutex::new(None),
            pending_responses: Mutex::new(HashMap::new()),
            bridge_token: token,
            manager,
            persisted_state: PiPersistedState {
                pi_session_dir: session_dir.to_string_lossy().into_owned(),
                ..state
            },
            session_dir,
            activity,
        });
        tokio::spawn(read_events(Arc::clone(&instance), stdout));
        if let Some(stderr) = stderr {
            tokio::spawn(log_stderr(request.new_runtime_id.clone(), stderr));
        }
        self.instances
            .write()
            .await
            .insert(request.new_runtime_id, instance);
        Ok(())
    }

    async fn delete_checkpoint(
        &self,
        runtime_state: OpaqueRuntimeState,
        provider_snapshot_id: String,
    ) -> Result<(), SessionDomainError> {
        let state = PiPersistedState::from_opaque(&runtime_state)?;
        let manager = self.managers.get(&state.backend_id).ok_or_else(|| {
            SessionDomainError::InvalidRequest {
                message: format!("unknown backend_id '{}'", state.backend_id),
            }
        })?;
        manager
            .delete_snapshot(
                BackendId(state.backend_id),
                provider_protocol::ProviderSnapshotId(provider_snapshot_id),
            )
            .await
            .map_err(map_provider_error)?;
        match tokio::fs::remove_dir_all(&state.pi_session_dir).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(SessionDomainError::Unavailable {
                message: format!(
                    "provider snapshot was deleted but checkpoint archive '{}' could not be removed: {error}",
                    state.pi_session_dir
                ),
            }),
        }
    }

    async fn attach(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
        self.instance_for(runtime_id).await.map(|_| ())
    }

    async fn check_alive(&self, runtime_id: &str) -> Result<bool, SessionDomainError> {
        let instance = self.instance_for(runtime_id).await?;
        match instance.manager.inspect_instance(runtime_id).await {
            Ok(_) => Ok(true),
            // The registry still knows about this instance but the platform
            // no longer does — confirmed reclaim, not a lookup miss.
            Err(ProviderControlError::NotFound { .. }) => Ok(false),
            Err(error) => Err(map_provider_error(error)),
        }
    }

    async fn submit_turn(
        &self,
        input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, SessionDomainError> {
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
                tx,
                aborted: false,
                pending_interactions: HashMap::new(),
                output_sequence: 0,
            });
        }

        if let Some(config) = resolve_llm(input.llm.as_ref())? {
            if let Err(error) = configure_instance_llm(&instance, &config).await {
                instance.current_turn.lock().await.take();
                return Err(error);
            }
        }

        let command = json!({
            "type": "prompt",
            "id": input.turn_id,
            "message": input.text,
        });

        // Contract (`RuntimeAdapter::submit_turn`'s doc comment): this write
        // is the only synchronous prep work done before returning `rx` — it
        // hands the prompt to the already-running `pi` process and returns
        // immediately, without waiting for the turn to actually run. All
        // subsequent progress arrives asynchronously through the reader task
        // `start()` spawned, which is already draining this process's
        // stdout.
        if let Err(error) = write_command(&instance, &command).await {
            instance.current_turn.lock().await.take();
            return Err(SessionDomainError::Unavailable {
                message: format!("failed to send prompt to pi process: {error}"),
            });
        }

        Ok(rx)
    }

    async fn answer_interaction(
        &self,
        input: RuntimeInteractionInput,
    ) -> Result<(), SessionDomainError> {
        let instance = self.instance_for(&input.runtime_id).await?;

        let method = {
            let mut guard = instance.current_turn.lock().await;
            let turn = guard
                .as_mut()
                .ok_or_else(|| SessionDomainError::InvalidRequest {
                    message: format!("no active turn for runtime '{}'", input.runtime_id),
                })?;
            if turn.turn_id != input.turn_id {
                return Err(SessionDomainError::InvalidRequest {
                    message: format!(
                        "turn '{}' is not the active turn for runtime '{}'",
                        input.turn_id, input.runtime_id
                    ),
                });
            }
            turn.pending_interactions
                .remove(&input.interaction_id)
                .ok_or_else(|| SessionDomainError::InvalidRequest {
                    message: format!(
                        "no pending interaction '{}' for runtime '{}'",
                        input.interaction_id, input.runtime_id
                    ),
                })?
        };

        let value = map_answer_to_pi_value(&method, &input.answer)?;
        let command = json!({
            "type": "extension_ui_response",
            "id": input.interaction_id,
            "value": value,
        });

        write_command(&instance, &command)
            .await
            .map_err(|error| SessionDomainError::Unavailable {
                message: format!("failed to send extension_ui_response to pi process: {error}"),
            })
    }

    async fn cancel(
        &self,
        runtime_id: &str,
        turn_id: Option<&str>,
    ) -> Result<(), SessionDomainError> {
        let instance = self.instance_for(runtime_id).await?;

        let should_send = {
            let mut guard = instance.current_turn.lock().await;
            let Some(turn) = guard.as_mut() else {
                // Nothing active — matches the existing "cancel whatever's
                // active, if anything" contract used elsewhere in this
                // workspace (e.g. `TurnCancellationRegistry::fire`'s doc).
                return Ok(());
            };
            if let Some(expected) = turn_id {
                if turn.turn_id != expected {
                    // Stale/mismatched turn_id: silent no-op, same rationale.
                    return Ok(());
                }
            }
            turn.aborted = true;
            true
        };

        if should_send {
            let command = json!({ "type": "abort" });
            write_command(&instance, &command).await.map_err(|error| {
                SessionDomainError::Unavailable {
                    message: format!("failed to send abort to pi process: {error}"),
                }
            })?;
        }
        Ok(())
    }

    /// Returns the `PiPersistedState` snapshot taken when this `runtime_id`
    /// was started (`docs/pi_session_restore_plan.md` §1.1), wrapped as an
    /// opaque blob. This overrides the trait's fail-closed default
    /// (`UnsupportedCapability`) — see [`PiPersistedState`]'s doc for why
    /// this is a governor-internal reuse of the state-quarantine slot, not a
    /// declaration of the (unrelated) checkpoint `StateExport` capability;
    /// `capabilities()` above is deliberately unchanged.
    async fn export_state(
        &self,
        runtime_id: &str,
    ) -> Result<OpaqueRuntimeState, SessionDomainError> {
        let instance = self.instance_for(runtime_id).await?;
        Ok(instance.persisted_state.clone().into_opaque())
    }

    async fn cleanup_from_state(
        &self,
        runtime_id: &str,
        state: &OpaqueRuntimeState,
    ) -> Result<(), SessionDomainError> {
        let persisted = PiPersistedState::from_opaque(state)?;
        if let Some(manager) = self.managers.get(&persisted.backend_id) {
            if let Err(error) = manager.destroy_by_runtime_id(runtime_id).await {
                tracing::warn!(
                    runtime_id,
                    backend_id = %persisted.backend_id,
                    %error,
                    "cleanup_from_state: failed to destroy the sandbox (registry- and ledger-driven \
                     paths both failed); it may be leaked and require manual cleanup"
                );
            }
        }
        let _ = tokio::fs::remove_dir_all(&persisted.pi_session_dir).await;
        Ok(())
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
            workspace_metadata: Value::Null,
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
