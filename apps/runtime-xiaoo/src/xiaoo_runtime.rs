use async_trait::async_trait;
use provider_protocol::{BackendId, ProviderControlError};
use serde_json::{json, Value};
use session_protocol::{SessionRuntimeCapability, SessionUsage};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::{mpsc, Mutex, RwLock};
use uuid::Uuid;
use xgovernor_core::{
    CapabilityFamily, CheckpointPayload, OpaqueRuntimeState, RuntimeAdapter, RuntimeEvent,
    RuntimeEventReceiver, RuntimeFailure, RuntimeInteractionInput, RuntimeLoadRequest,
    RuntimeStartRequest, RuntimeTurnInput, SessionDomainError,
};
use xgovernor_manager::InstanceManager;
use xgovernor_runtime_pi::bridge::Bridge;
use xiaoo_api::runtime::RuntimeState;

use crate::map_provider_error;
use crate::xiaoo_backend::{spawn_worker_process, PersistedLlm, WorkerConfig, WorkerResponse};
use crate::{
    clone_git_workspace, read_ext, state_from_opaque, state_to_opaque, validate_persisted_llm,
    WorkerRequest, XiaooPersistedState, E2B_BACKEND_ID, EXT_NAMESPACE,
};

struct XiaooWorkerInstance {
    manager: Arc<InstanceManager>,
    bridge: Arc<Bridge>,
    bridge_token: String,
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    stdout: Mutex<BufReader<ChildStdout>>,
    active_turn: Mutex<Option<String>>,
    pending_interactions: Arc<Mutex<HashMap<String, String>>>,
    persisted: Mutex<XiaooPersistedState>,
}

pub struct XiaooRuntime {
    managers: HashMap<String, Arc<InstanceManager>>,
    bridge: Arc<Bridge>,
    instances: RwLock<HashMap<String, Arc<XiaooWorkerInstance>>>,
    worker_executable: PathBuf,
}

impl XiaooRuntime {
    pub fn new(managers: HashMap<String, Arc<InstanceManager>>) -> Self {
        let bridge = Bridge::spawn().expect("xiaoO operation bridge must bind");
        let worker_executable = std::env::var_os("XGOVERNOR_XIAOO_WORKER")
            .map(PathBuf::from)
            .or_else(|| std::env::current_exe().ok())
            .unwrap_or_else(|| PathBuf::from("xiaoo-worker"));
        Self {
            managers,
            bridge,
            instances: RwLock::new(HashMap::new()),
            worker_executable,
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
        let line = serde_json::to_string(&request).map_err(|e| SessionDomainError::Internal {
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
            .write_all(b"\n")
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
        if request
            .ext
            .get(EXT_NAMESPACE)
            .and_then(|v| v.get("backend_id"))
            .and_then(Value::as_str)
            != Some(E2B_BACKEND_ID)
        {
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
            // `allow_internet_access` is an e2b-only provider option; the
            // local provider rejects unknown fields, so only include it
            // for e2b (mirrors apps/runtime-pi/src/lib.rs::prepare_cold_start).
            let mut provider_options = json!({
                "workspace_root": request.workspace.root,
            });
            if ext.backend_id == E2B_BACKEND_ID {
                provider_options["allow_internet_access"] = json!(true);
            }
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
            let persisted = XiaooPersistedState {
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
                loop_state: RuntimeState::new(request.conversation_id).to_snapshot(),
            };
            (persisted, manager, backend)
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
                let _ = manager.stop_instance(&request.runtime_id).await;
                return Err(error);
            }
        };
        self.instances.write().await.insert(
            request.runtime_id.clone(),
            Arc::new(XiaooWorkerInstance {
                manager,
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

    async fn stop(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
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
        instance
            .manager
            .stop_instance(runtime_id)
            .await
            .map_err(map_provider_error)
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
        {
            let mut active = instance.active_turn.lock().await;
            if active.is_some() {
                return Err(SessionDomainError::Conflict {
                    message: "xiaoO worker already has an active turn".into(),
                });
            }
            *active = Some(input.turn_id.clone());
        }
        if let Err(error) = self
            .send_worker(
                &instance,
                WorkerRequest::Run {
                    turn_id: input.turn_id.clone(),
                    text: input.text,
                    model: input.llm.as_ref().and_then(|llm| llm.model.clone()),
                    reasoning_effort: input.reasoning_effort,
                },
            )
            .await
        {
            instance.active_turn.lock().await.take();
            return Err(error);
        }
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let mut line = String::new();
                let read = instance.stdout.lock().await.read_line(&mut line).await;
                if !matches!(read, Ok(n) if n > 0) {
                    let _ = tx
                        .send(RuntimeEvent::Failed {
                            error: RuntimeFailure {
                                code: "xiaoo_worker_exited".into(),
                                message: "xiaoO worker event stream closed".into(),
                                retryable: true,
                                details: Value::Null,
                            },
                            usage: SessionUsage::default(),
                        })
                        .await;
                    break;
                }
                match serde_json::from_str::<WorkerResponse>(line.trim()) {
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
                        if tx.send(event).await.is_err() || terminal {
                            break;
                        }
                    }
                    Ok(WorkerResponse::State { loop_state }) => {
                        instance.persisted.lock().await.loop_state = loop_state
                    }
                    Ok(WorkerResponse::Error { message }) => {
                        let _ = tx
                            .send(RuntimeEvent::Failed {
                                error: RuntimeFailure {
                                    code: "xiaoo_worker_error".into(),
                                    message,
                                    retryable: false,
                                    details: Value::Null,
                                },
                                usage: SessionUsage::default(),
                            })
                            .await;
                        break;
                    }
                    _ => {}
                }
            }
            instance.active_turn.lock().await.take();
        });
        Ok(rx)
    }

    async fn answer_interaction(
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
        self.send_worker(
            &instance,
            WorkerRequest::Answer {
                interaction_id: input.interaction_id,
                answer: input.answer,
            },
        )
        .await
    }

    async fn cancel(
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
            self.send_worker(&instance, WorkerRequest::Cancel).await?;
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
        let persisted = instance.persisted.lock().await.clone();
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
        self.send_worker(
            &instance,
            WorkerRequest::LoadState {
                loop_state: persisted.loop_state.clone(),
            },
        )
        .await?;
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
            checkpoint_id: format!("checkpoint-{}", Uuid::new_v4()),
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
        let backend = manager
            .load_instance_from_snapshot(
                request.new_runtime_id.clone(),
                BackendId(persisted.backend_id.clone()),
                request.owner_ref,
                provider_protocol::ProviderSnapshotId(request.provider_snapshot_id),
                persisted.provider_options.clone(),
            )
            .await
            .map_err(map_provider_error)?;
        let bridge_token = Uuid::new_v4().to_string();
        self.bridge.register(
            bridge_token.clone(),
            Arc::clone(&backend),
            backend.paths().workspace_root().clone(),
            Arc::new(tokio::sync::RwLock::new(())),
        );
        let config = WorkerConfig {
            llm: persisted.llm.clone(),
            loop_state: persisted.loop_state.clone(),
            bridge_url: self.bridge.base_url(),
            bridge_token: bridge_token.clone(),
            backend_id: backend.backend_id().to_string(),
            workspace_root: backend.paths().workspace_root().0.clone(),
            home_dir: backend.paths().home_dir().map(|path| path.0.clone()),
            supports_atomic_write: backend.capabilities().supports_atomic_write,
            supports_grep: backend.capabilities().supports_grep,
        };
        let (child, stdin, stdout) =
            match spawn_worker_process(&self.worker_executable, &config).await {
                Ok(parts) => parts,
                Err(error) => {
                    self.bridge.unregister(&bridge_token);
                    let _ = manager.stop_instance(&request.new_runtime_id).await;
                    return Err(error);
                }
            };
        self.instances.write().await.insert(
            request.new_runtime_id,
            Arc::new(XiaooWorkerInstance {
                manager,
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
