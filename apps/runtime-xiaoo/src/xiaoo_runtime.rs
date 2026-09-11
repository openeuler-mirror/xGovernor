use agent_runtime_protocol::{
    decode_worker_response, encode_worker_request, worker_error_event, AgentRuntime,
    RuntimeCancelRequest, RuntimeCapability, RuntimeCapabilityContext, RuntimeError, RuntimeEvent,
    RuntimeEventReceiver, RuntimeExecutionContext,
    RuntimeInteractionRequest as RuntimeInteractionInput, RuntimeStartRequest,
    RuntimeStateSnapshot, RuntimeTurnRequest as RuntimeTurnInput, WorkerRequest, WorkerResponse,
};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::{mpsc, Mutex, RwLock};
use uuid::Uuid;
use xgovernor_core::{CapabilityFamily, OpaqueRuntimeState, SessionDomainError};
use xgovernor_runtime_pi::bridge::Bridge;
use xiaoo_api::llm::{resolve_config, ResolveInput};
use xiaoo_api::runtime::RuntimeState;

use crate::xiaoo_backend::{spawn_worker_process, PersistedLlm, WorkerConfig};
use crate::{
    clone_git_workspace, read_ext, state_from_opaque, state_to_opaque, validate_persisted_llm,
    XiaooPersistedState, E2B_BACKEND_ID, STATE_SCHEMA_VERSION,
};

struct XiaooWorkerInstance {
    bridge: Arc<Bridge>,
    bridge_token: String,
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    stdout: Mutex<BufReader<ChildStdout>>,
    active_turn: Mutex<Option<String>>,
    pending_interactions: Arc<Mutex<HashMap<String, String>>>,
    persisted: Mutex<XiaooPersistedState>,
}

fn map_runtime_error(error: RuntimeError) -> SessionDomainError {
    match error {
        RuntimeError::InvalidRequest { message, .. } | RuntimeError::StateCorrupt { message } => {
            SessionDomainError::InvalidRequest { message }
        }
        RuntimeError::NotFound { runtime_id } => SessionDomainError::NotFound { runtime_id },
        RuntimeError::Conflict { message, .. } => SessionDomainError::Conflict { message },
        RuntimeError::UnsupportedCapability { capability } => {
            SessionDomainError::UnsupportedCapability {
                family: CapabilityFamily::Runtime,
                capability,
            }
        }
        RuntimeError::WorkerUnavailable { message, .. } => {
            SessionDomainError::Unavailable { message }
        }
        RuntimeError::Internal { message } => SessionDomainError::Internal {
            message,
            source: None,
        },
    }
}

pub struct XiaooRuntime {
    bridge: Arc<Bridge>,
    instances: RwLock<HashMap<String, Arc<XiaooWorkerInstance>>>,
    worker_executable: PathBuf,
}

impl XiaooRuntime {
    pub fn new() -> Self {
        let bridge = Bridge::spawn().expect("xiaoO operation bridge must bind");
        let worker_executable = std::env::var_os("XGOVERNOR_XIAOO_WORKER")
            .map(PathBuf::from)
            .or_else(|| std::env::current_exe().ok())
            .unwrap_or_else(|| PathBuf::from("xiaoo-worker"));
        Self {
            bridge,
            instances: RwLock::new(HashMap::new()),
            worker_executable,
        }
    }

    async fn instance_for(
        &self,
        runtime_id: &str,
    ) -> Result<Arc<XiaooWorkerInstance>, SessionDomainError> {
        self.instances
            .read()
            .await
            .get(runtime_id)
            .cloned()
            .ok_or_else(|| SessionDomainError::NotFound {
                runtime_id: runtime_id.into(),
            })
    }

    async fn send_worker(
        &self,
        instance: &XiaooWorkerInstance,
        request: WorkerRequest,
    ) -> Result<(), SessionDomainError> {
        let line = encode_worker_request(&request).map_err(|e| SessionDomainError::Internal {
            message: e.to_string(),
            source: None,
        })?;
        let mut stdin = instance.stdin.lock().await;
        stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| SessionDomainError::Unavailable {
                message: format!("xiaoO worker write failed: {e}"),
            })?;
        stdin
            .flush()
            .await
            .map_err(|e| SessionDomainError::Unavailable {
                message: format!("xiaoO worker flush failed: {e}"),
            })
    }
}

#[async_trait]
impl AgentRuntime for XiaooRuntime {
    fn runtime_kind(&self) -> &str {
        "xiaoo"
    }

    fn capabilities(&self) -> BTreeSet<RuntimeCapability> {
        BTreeSet::from([
            RuntimeCapability::Interaction,
            RuntimeCapability::StateExport,
            RuntimeCapability::ModelOverride,
            RuntimeCapability::ReasoningControl,
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
        let ext = read_ext(&request.ext).map_err(to_runtime_error)?;
        let resolved = resolve_config(ResolveInput {
            provider: Some(ext.provider.clone()),
            api_key_env: Some(ext.api_key_env.clone()),
            base_url: ext.api_base.clone(),
            ..Default::default()
        })
        .map_err(|e| RuntimeError::InvalidRequest {
            code: "llm_config".into(),
            message: e.to_string(),
        })?;
        let api_key = std::env::var(&ext.api_key_env)
            .ok()
            .filter(|v| !v.trim().is_empty());
        probe_llm_endpoint(&resolved.base_url, api_key.as_deref(), &ext.model)
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
        self.export_state_inner(runtime_id)
            .await
            .map_err(to_runtime_error)
    }

    async fn load_state(
        &self,
        runtime_id: &str,
        state: OpaqueRuntimeState,
    ) -> Result<(), RuntimeError> {
        self.load_state_inner(runtime_id, state)
            .await
            .map_err(to_runtime_error)
    }
}

impl XiaooRuntime {
    async fn start_inner(
        &self,
        request: RuntimeStartRequest,
        context: RuntimeExecutionContext,
    ) -> Result<(), SessionDomainError> {
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
        let backend = context.operation_backend;
        let persisted = if let Some(state) = request.state.as_ref() {
            let persisted = state_from_opaque(state)?;
            validate_persisted_llm(&persisted.llm)?;
            persisted
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
            let role_settings = ext.role_settings()?;
            // `allow_internet_access` is an e2b-only provider option; the
            // local provider rejects unknown fields, so only include it
            // for e2b (mirrors apps/runtime-pi/src/lib.rs::prepare_cold_start).
            let mut provider_options = json!({
                "workspace_root": request.workspace.root,
            });
            if ext.backend_id == E2B_BACKEND_ID {
                provider_options["allow_internet_access"] = json!(true);
            }
            if request.workspace.metadata != Value::Null {
                if let Err(error) = clone_git_workspace(
                    backend.as_ref(),
                    &request.workspace.metadata,
                    &request.workspace.root,
                )
                .await
                {
                    return Err(error);
                }
            }
            let persisted = XiaooPersistedState {
                backend_id: ext.backend_id,
                owner_ref: String::new(),
                workspace_root: request.workspace.root,
                provider_options,
                llm: PersistedLlm {
                    provider: ext.provider,
                    model: ext.model,
                    api_key_env: ext.api_key_env,
                    api_base: ext.api_base,
                },
                loop_state: RuntimeState::new(request.conversation_id).to_snapshot(),
                role_settings,
            };
            persisted
        };
        let bridge_token = Uuid::new_v4().to_string();
        let activity = Arc::new(tokio::sync::RwLock::new(()));
        self.bridge.register(
            bridge_token.clone(),
            Arc::clone(&backend),
            backend.paths().workspace_root().clone(),
            activity,
        );
        let config = WorkerConfig {
            llm: persisted.llm.clone(),
            loop_state: persisted.loop_state.clone(),
            role_settings: persisted.role_settings.clone(),
            bridge_url: self.bridge.base_url(),
            bridge_token: bridge_token.clone(),
            backend_id: backend.backend_id().to_string(),
            workspace_root: backend.paths().workspace_root().0.clone(),
            home_dir: backend.paths().home_dir().map(|p| p.0.clone()),
            supports_atomic_write: backend.capabilities().supports_atomic_write,
            supports_grep: backend.capabilities().supports_grep,
        };
        let spawned = spawn_worker_process(&self.worker_executable, &config).await;
        let (child, stdin, stdout) = match spawned {
            Ok(parts) => parts,
            Err(error) => {
                self.bridge.unregister(&bridge_token);
                return Err(error);
            }
        };
        self.instances.write().await.insert(
            request.runtime_id.clone(),
            Arc::new(XiaooWorkerInstance {
                bridge: Arc::clone(&self.bridge),
                bridge_token,
                child: Mutex::new(child),
                stdin: Mutex::new(stdin),
                stdout: Mutex::new(stdout),
                active_turn: Mutex::new(None),
                pending_interactions: Arc::new(Mutex::new(HashMap::new())),
                persisted: Mutex::new(persisted),
            }),
        );
        Ok(())
    }

    async fn stop_inner(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
        let instance = self
            .instances
            .write()
            .await
            .remove(runtime_id)
            .ok_or_else(|| SessionDomainError::NotFound {
                runtime_id: runtime_id.into(),
            })?;
        let _ = self.send_worker(&instance, WorkerRequest::Shutdown).await;
        {
            let mut child = instance.child.lock().await;
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        instance.bridge.unregister(&instance.bridge_token);
        Ok(())
    }

    async fn submit_turn_inner(
        &self,
        mut input: RuntimeTurnInput,
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
        let instance = self.instance_for(&input.runtime_id).await?;
        let role_settings;
        {
            let mut active = instance.active_turn.lock().await;
            if active.is_some() {
                return Err(SessionDomainError::Conflict {
                    message: "xiaoO worker already has an active turn".into(),
                });
            }
            role_settings = instance
                .persisted
                .lock()
                .await
                .role_settings
                .for_turn(&input.ext)?;
            input.ext.insert(
                crate::EXT_NAMESPACE.into(),
                serde_json::to_value(&role_settings).map_err(|error| {
                    SessionDomainError::Internal {
                        message: error.to_string(),
                        source: None,
                    }
                })?,
            );
            *active = Some(input.turn_id.clone());
        }
        if let Err(error) = self
            .send_worker(&instance, WorkerRequest::SubmitTurn(input.clone()))
            .await
        {
            instance.active_turn.lock().await.take();
            return Err(error);
        }
        instance.persisted.lock().await.role_settings = role_settings;
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let mut line = String::new();
                let read = instance.stdout.lock().await.read_line(&mut line).await;
                if !matches!(read, Ok(n) if n > 0) {
                    let _ = tx
                        .send(worker_error_event(RuntimeError::WorkerUnavailable {
                            message: "xiaoO worker event stream closed".into(),
                            retryable: true,
                        }))
                        .await;
                    break;
                }
                match decode_worker_response(&line) {
                    Ok(WorkerResponse::Event { event }) => {
                        let terminal = matches!(
                            event,
                            RuntimeEvent::Completed { .. } | RuntimeEvent::Failed { .. }
                        );
                        if let RuntimeEvent::InteractionRequested { interaction_id, .. } = &event {
                            instance
                                .pending_interactions
                                .lock()
                                .await
                                .insert(interaction_id.clone(), input.turn_id.clone());
                        }
                        if terminal {
                            // Export immediately after a terminal SSE event must see an idle
                            // worker and the preceding persisted State response.
                            instance.active_turn.lock().await.take();
                        }
                        if tx.send(event).await.is_err() || terminal {
                            break;
                        }
                    }
                    Ok(WorkerResponse::State { state }) => {
                        match state.decode("xiaoo", STATE_SCHEMA_VERSION) {
                            Ok(loop_state) => {
                                instance.persisted.lock().await.loop_state = loop_state
                            }
                            Err(error) => {
                                let _ = tx.send(worker_error_event(error)).await;
                                break;
                            }
                        }
                    }
                    Ok(WorkerResponse::Error { error }) => {
                        let _ = tx.send(worker_error_event(error)).await;
                        break;
                    }
                    Ok(WorkerResponse::Ready | WorkerResponse::Unknown) => {}
                    Err(error) => {
                        let _ = tx
                            .send(worker_error_event(RuntimeError::WorkerUnavailable {
                                message: format!("invalid xiaoO worker response: {error}"),
                                retryable: true,
                            }))
                            .await;
                        break;
                    }
                }
            }
            let mut active = instance.active_turn.lock().await;
            if active.as_deref() == Some(input.turn_id.as_str()) {
                active.take();
            }
        });
        Ok(rx)
    }

    async fn answer_interaction_inner(
        &self,
        input: RuntimeInteractionInput,
    ) -> Result<(), SessionDomainError> {
        let instance = self.instance_for(&input.runtime_id).await?;
        let mut pending = instance.pending_interactions.lock().await;
        let turn = pending.get(&input.interaction_id).cloned().ok_or_else(|| {
            SessionDomainError::Conflict {
                message: format!("interaction '{}' is not pending", input.interaction_id),
            }
        })?;
        if turn != input.turn_id {
            return Err(SessionDomainError::Conflict {
                message: format!(
                    "interaction '{}' belongs to turn '{}'",
                    input.interaction_id, turn
                ),
            });
        }
        pending.remove(&input.interaction_id);
        drop(pending);
        self.send_worker(&instance, WorkerRequest::AnswerInteraction(input))
            .await
    }

    async fn cancel_inner(
        &self,
        runtime_id: &str,
        turn_id: Option<&str>,
    ) -> Result<(), SessionDomainError> {
        let instance = self.instance_for(runtime_id).await?;
        let active = instance.active_turn.lock().await.clone();
        if active
            .as_deref()
            .is_some_and(|active| turn_id.is_none() || turn_id == Some(active))
        {
            self.send_worker(
                &instance,
                WorkerRequest::Cancel(RuntimeCancelRequest {
                    runtime_id: runtime_id.into(),
                    turn_id: turn_id.map(str::to_owned),
                }),
            )
            .await?;
        }
        Ok(())
    }

    async fn export_state_inner(
        &self,
        runtime_id: &str,
    ) -> Result<OpaqueRuntimeState, SessionDomainError> {
        let instance = self.instance_for(runtime_id).await?;
        if instance.active_turn.lock().await.is_some() {
            return Err(SessionDomainError::Conflict {
                message: "cannot export xiaoO state during an active turn".into(),
            });
        }
        let persisted = instance.persisted.lock().await.clone();
        state_to_opaque(&persisted)
    }

    async fn load_state_inner(
        &self,
        runtime_id: &str,
        state: OpaqueRuntimeState,
    ) -> Result<(), SessionDomainError> {
        let instance = self.instance_for(runtime_id).await?;
        let persisted = state_from_opaque(&state)?;
        validate_persisted_llm(&persisted.llm)?;
        self.send_worker(
            &instance,
            WorkerRequest::LoadState(
                RuntimeStateSnapshot::try_new("xiaoo", STATE_SCHEMA_VERSION, &persisted.loop_state)
                    .map_err(map_runtime_error)?,
            ),
        )
        .await?;
        *instance.persisted.lock().await = persisted;
        Ok(())
    }
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
    let mut req = client.post(&url).json(&serde_json::json!({
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
