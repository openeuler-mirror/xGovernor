use agent_runtime_protocol::{
    decode_worker_response, encode_worker_request, worker_error_event, AgentRuntime,
    RuntimeCancelRequest, RuntimeCapability, RuntimeCapabilityContext, RuntimeError, RuntimeEvent,
    RuntimeEventReceiver, RuntimeExecutionContext,
    RuntimeInteractionRequest as RuntimeInteractionInput, RuntimeStartRequest,
    RuntimeStateSnapshot, RuntimeTurnRequest as RuntimeTurnInput, WorkerRequest, WorkerResponse,
};
use async_trait::async_trait;
use backend::execution::OperationExecutionExt;
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
use xiaoo_api::runtime::RuntimeState;

use crate::xiaoo_backend::{spawn_worker_process, WorkerConfig};
use crate::{
    clone_git_workspace, read_ext, state_from_opaque, state_to_opaque, validate_persisted_llm,
    XiaooPersistedState, E2B_BACKEND_ID, STATE_SCHEMA_VERSION,
};

struct XiaooWorkerInstance {
    backend: Arc<dyn operation_protocol::OperationBackend>,
    completion: Mutex<()>,
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
        self.validate_llm_config(request.llm.as_ref())?;
        self.start_inner(request, context)
            .await
            .map_err(to_runtime_error)
    }

    async fn probe_llm(&self, request: &RuntimeStartRequest) -> Result<(), RuntimeError> {
        self.validate_llm_config(request.llm.as_ref())?;
        let llm = if let Some(state) = &request.state {
            state_from_opaque(state)
                .map_err(to_runtime_error)?
                .llm
                .apply(request.llm.as_ref())
                .map_err(to_runtime_error)?
        } else {
            let ext = read_ext(&request.ext).map_err(to_runtime_error)?;
            crate::model::initial_llm(&ext, request.llm.as_ref()).map_err(to_runtime_error)?
        };
        crate::model::probe(&llm).await.map_err(to_runtime_error)
    }

    fn validate_llm_config(
        &self,
        llm: Option<&session_protocol::LlmOverrideRequest>,
    ) -> Result<(), RuntimeError> {
        crate::model::validate_override(llm).map_err(to_runtime_error)
    }

    async fn describe_llm(
        &self,
        runtime_id: &str,
    ) -> Result<Option<session_protocol::ResolvedLlmDescriptor>, RuntimeError> {
        let instance = self
            .instance_for(runtime_id)
            .await
            .map_err(to_runtime_error)?;
        let descriptor = instance.persisted.lock().await.llm.descriptor();
        Ok(Some(descriptor))
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
        self.validate_llm_config(input.llm.as_ref())?;
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
            let mut persisted = state_from_opaque(state)?;
            persisted.llm = persisted.llm.apply(request.llm.as_ref())?;
            persisted.role_settings.tool_policy.validate()?;
            persisted.role_settings.discovery.validate()?;
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
            let llm = crate::model::initial_llm(&ext, request.llm.as_ref())?;
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
                llm,
                responses_state: Default::default(),
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
            responses_state: persisted.responses_state.clone(),
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
                backend,
                completion: Mutex::new(()),
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
            let persisted = instance.persisted.lock().await;
            persisted.role_settings.for_turn(&input.ext)?;
            let next = persisted.llm.apply(input.llm.as_ref())?;
            let effort: agent_types::ReasoningEffort = input
                .reasoning_effort
                .as_deref()
                .unwrap_or("off")
                .parse()
                .map_err(|_| crate::policy::invalid("invalid reasoning effort"))?;
            if next.effective_capabilities().reasoning == Some(false)
                && effort != agent_types::ReasoningEffort::Off
            {
                return Err(crate::policy::invalid("model does not support reasoning"));
            }
            drop(persisted);
            instance
                .backend
                .begin_turn(&input.turn_id)
                .await
                .map_err(operation_error)?;
            *active = Some(input.turn_id.clone());
        }
        if let Err(error) = self
            .send_worker(&instance, WorkerRequest::SubmitTurn(input.clone()))
            .await
        {
            let _ = instance.backend.finish_turn(&input.turn_id).await;
            instance.active_turn.lock().await.take();
            return Err(error);
        }
        let (tx, mut internal_rx) = mpsc::channel(64);
        let (output, rx) = mpsc::channel(64);
        let terminal_instance = Arc::clone(&instance);
        let terminal_turn = input.turn_id.clone();
        tokio::spawn(async move {
            while let Some(event) = internal_rx.recv().await {
                if matches!(
                    event,
                    RuntimeEvent::Completed { .. } | RuntimeEvent::Failed { .. }
                ) {
                    let _completion = terminal_instance.completion.lock().await;
                    while terminal_instance
                        .backend
                        .finish_turn(&terminal_turn)
                        .await
                        .is_err()
                    {
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                    let mut active = terminal_instance.active_turn.lock().await;
                    if active.as_deref() == Some(terminal_turn.as_str()) {
                        active.take();
                    }
                    drop(active);
                    let _ = output.send(event).await;
                    break;
                }
                // Continue draining even if the SSE consumer disconnects.
                let _ = output.send(event).await;
            }
        });
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
                        if tx.send(event).await.is_err() || terminal {
                            break;
                        }
                    }
                    Ok(WorkerResponse::State { state }) => {
                        match state
                            .decode::<crate::worker::WorkerTurnState>("xiaoo", STATE_SCHEMA_VERSION)
                        {
                            Ok(state) => {
                                let mut persisted = instance.persisted.lock().await;
                                persisted.loop_state = state.loop_state;
                                persisted.llm = state.llm;
                                persisted.role_settings = state.role_settings;
                                persisted.responses_state = state.responses_state;
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
        let _completion = instance.completion.lock().await;
        let active = instance.active_turn.lock().await.clone();
        if let Some(selected) =
            active.filter(|active| turn_id.is_none() || turn_id == Some(active.as_str()))
        {
            instance
                .backend
                .block_turn(&selected)
                .map_err(operation_error)?;
            let abort = self
                .send_worker(
                    &instance,
                    WorkerRequest::Cancel(RuntimeCancelRequest {
                        runtime_id: runtime_id.into(),
                        turn_id: Some(selected.clone()),
                    }),
                )
                .await;
            instance
                .backend
                .cancel_turn(&selected)
                .await
                .map_err(operation_error)?;
            abort?;
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
        persisted.role_settings.tool_policy.validate()?;
        persisted.role_settings.discovery.validate()?;
        let active = instance.active_turn.lock().await;
        if active.is_some() {
            return Err(SessionDomainError::Conflict {
                message: "cannot load state during an active turn".into(),
            });
        }
        self.send_worker(
            &instance,
            WorkerRequest::LoadState(
                RuntimeStateSnapshot::try_new(
                    "xiaoo",
                    STATE_SCHEMA_VERSION,
                    &crate::worker::WorkerTurnState::from(&persisted),
                )
                .map_err(map_runtime_error)?,
            ),
        )
        .await?;
        *instance.persisted.lock().await = persisted;
        Ok(())
    }
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

fn operation_error(error: operation_protocol::OperationError) -> SessionDomainError {
    SessionDomainError::Unavailable {
        message: error.to_string(),
    }
}

#[cfg(all(test, unix))]
mod lifecycle_tests {
    use super::*;
    use operation_protocol::capability::*;
    use operation_protocol::{OperationBackend, OperationBackendCapabilities, OperationError};
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Semaphore;

    struct CleanupBackend {
        inner: Arc<dyn OperationBackend>,
        calls: std::sync::Mutex<Vec<String>>,
        finishing: Semaphore,
        release: Semaphore,
        hold: AtomicBool,
    }
    #[async_trait]
    impl OperationBackend for CleanupBackend {
        fn backend_id(&self) -> &str {
            "local"
        }
        fn capabilities(&self) -> OperationBackendCapabilities {
            self.inner.capabilities()
        }
        fn paths(&self) -> &dyn OperationPathResolver {
            self.inner.paths()
        }
        fn files(&self) -> &dyn OperationFileSystem {
            self.inner.files()
        }
        fn search(&self) -> &dyn OperationSearch {
            self.inner.search()
        }
        fn exec(&self) -> &dyn OperationExec {
            self.inner.exec()
        }
        fn export(&self) -> &dyn OperationExport {
            self.inner.export()
        }
        fn execution_control(&self) -> Option<&dyn operation_protocol::OperationExecutionControl> {
            Some(self)
        }
        async fn shutdown(&self) -> Result<(), OperationError> {
            Ok(())
        }
    }
    #[async_trait]
    impl operation_protocol::OperationExecutionControl for CleanupBackend {
        async fn begin_turn(&self, id: &str) -> Result<(), OperationError> {
            self.calls.lock().unwrap().push(format!("begin:{id}"));
            Ok(())
        }
        fn block_turn(&self, id: &str) -> Result<(), OperationError> {
            self.calls.lock().unwrap().push(format!("block:{id}"));
            Ok(())
        }
        async fn cancel_turn(&self, id: &str) -> Result<(), OperationError> {
            self.calls.lock().unwrap().push(format!("cancel:{id}"));
            Ok(())
        }
        async fn finish_turn(&self, id: &str) -> Result<(), OperationError> {
            self.calls.lock().unwrap().push(format!("finish:{id}"));
            if self.hold.swap(false, Ordering::SeqCst) {
                self.finishing.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            Ok(())
        }
    }
    fn turn(id: &str) -> RuntimeTurnInput {
        serde_json::from_value(json!({"runtime_id":"test","turn_id":id,"text":"synthetic"}))
            .unwrap()
    }
    #[tokio::test]
    async fn cancellation_waits_for_cleanup_and_old_cancel_cannot_touch_new_turn() {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let dir=tempfile::tempdir().unwrap();
            let script=dir.path().join("worker.py");
            std::fs::write(&script,r#"#!/usr/bin/python3
import json,sys
json.loads(sys.stdin.readline())
print('{"kind":"ready"}',flush=True)
for line in sys.stdin:
    req=json.loads(line)
    if req['kind']=='cancel':
        print('{"kind":"event","event":{"kind":"completed","outcome":"cancelled","usage":{}}}',flush=True)
    elif req['kind']=='shutdown': break
"#).unwrap();
            std::fs::set_permissions(&script,std::fs::Permissions::from_mode(0o700)).unwrap();
            let backend=Arc::new(CleanupBackend {
                inner:backend::local::local_backend(dir.path().into(),None,None,None).unwrap(),
                calls:std::sync::Mutex::new(Vec::new()),finishing:Semaphore::new(0),release:Semaphore::new(0),hold:AtomicBool::new(true),
            });
            let runtime=XiaooRuntime { bridge:Bridge::spawn().unwrap(),instances:RwLock::new(HashMap::new()),worker_executable:script };
            let start:RuntimeStartRequest=serde_json::from_value(json!({
                "runtime_id":"test","conversation_id":"test","sender_id":"test",
                "workspace":{"workspace_id":"test","root":dir.path(),"access":"read_write"},
                "ext":{"xiaoo":{"backend_id":"local","provider":"openai","model":"test","api_key_env":"PATH"}}
            })).unwrap();
            runtime.start_inner(start,RuntimeExecutionContext {operation_backend:backend.clone()}).await.unwrap();
            let mut events=runtime.submit_turn_inner(turn("one")).await.unwrap();
            runtime.cancel_inner("test",Some("old")).await.unwrap();
            assert_eq!(*backend.calls.lock().unwrap(),["begin:one"]);
            runtime.cancel_inner("test",Some("one")).await.unwrap();
            backend.finishing.acquire().await.unwrap().forget();
            assert!(events.try_recv().is_err(),"terminal must wait for cleanup");
            assert!(runtime.submit_turn_inner(turn("two")).await.is_err(),"new turn must wait for cleanup");
            backend.release.add_permits(1);
            assert!(matches!(events.recv().await,Some(RuntimeEvent::Completed { .. })));
            let mut next=runtime.submit_turn_inner(turn("two")).await.unwrap();
            runtime.cancel_inner("test",Some("one")).await.unwrap();
            assert_eq!(*backend.calls.lock().unwrap(),["begin:one","block:one","cancel:one","finish:one","begin:two"]);
            runtime.cancel_inner("test",Some("two")).await.unwrap();
            assert!(matches!(next.recv().await,Some(RuntimeEvent::Completed { .. })));
            runtime.stop_inner("test").await.unwrap();
        }).await.unwrap();
    }
}

#[cfg(test)]
mod model_config_tests {
    use super::*;
    #[tokio::test]
    async fn supported_model_features_are_accepted() {
        let runtime = XiaooRuntime::new();
        for value in [
            serde_json::json!({"api":"openai-responses"}),
            serde_json::json!({"capabilities":{"reasoning":true}}),
        ] {
            let config = serde_json::from_value(value).unwrap();
            assert!(runtime.validate_llm_config(Some(&config)).is_ok());
        }
        assert!(runtime.validate_llm_config(None).is_ok());
    }
}
