use super::{
    configure_pi_launch, extract_usage, map_answer_to_pi_value, resolve_llm, PiLlmConfig,
    DIALOG_METHODS,
};
use agent_runtime_protocol::{
    decode_worker_request, encode_worker_response, RuntimeError, RuntimeEvent, RuntimeFailure,
    WorkerRequest, WorkerResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use session_protocol::{
    SessionInteractionOption, SessionToolActivityPhase, SessionToolActivityStatus,
    SessionTurnOutcome, SessionUsage,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{timeout, Duration};
use uuid::Uuid;

const CONFIG_ENV: &str = "XGOVERNOR_PI_WORKER_CONFIG";
const RPC_RESPONSE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PiWorkerConfig {
    pub runtime_id: String,
    pub executable: String,
    pub extension_dir: String,
    pub session_dir: PathBuf,
    pub resume_session_file: Option<PathBuf>,
    pub bridge_url: String,
    pub bridge_token: String,
    pub workspace_root: String,
    pub use_workspace_cwd: bool,
    pub llm: Option<PiLlmConfig>,
}

struct ActiveTurn {
    turn_id: String,
    aborted: bool,
    pending_interactions: HashMap<String, String>,
    output_sequence: u64,
}

impl ActiveTurn {
    fn next_sequence(&mut self) -> u64 {
        let value = self.output_sequence;
        self.output_sequence += 1;
        value
    }
}

struct NativePi {
    runtime_id: String,
    stdin: Mutex<ChildStdin>,
    child: Mutex<Child>,
    active_turn: Mutex<Option<ActiveTurn>>,
    pending_responses: Mutex<HashMap<String, oneshot::Sender<Value>>>,
    responses: mpsc::UnboundedSender<WorkerResponse>,
    session_dir: PathBuf,
}

pub(crate) async fn spawn_worker_process(
    worker_executable: &PathBuf,
    config: &PiWorkerConfig,
) -> Result<(Child, ChildStdin, BufReader<ChildStdout>), xgovernor_core::SessionDomainError> {
    let config_json = serde_json::to_string(config).map_err(|error| {
        xgovernor_core::SessionDomainError::Internal {
            message: format!("failed to serialize Pi worker config: {error}"),
            source: None,
        }
    })?;
    let mut command = Command::new(worker_executable);
    command
        .arg("--pi-worker")
        .env(CONFIG_ENV, config_json)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    let mut child =
        command
            .spawn()
            .map_err(|error| xgovernor_core::SessionDomainError::Unavailable {
                message: format!(
                    "failed to spawn Pi worker '{}': {error}",
                    worker_executable.display()
                ),
            })?;
    let stdin = child.stdin.take().expect("Pi worker stdin was piped");
    let stdout = child.stdout.take().expect("Pi worker stdout was piped");
    let mut stdout = BufReader::new(stdout);
    let mut ready = String::new();
    stdout.read_line(&mut ready).await.map_err(|error| {
        xgovernor_core::SessionDomainError::Unavailable {
            message: format!("failed to read Pi worker readiness: {error}"),
        }
    })?;
    match agent_runtime_protocol::decode_worker_response(&ready) {
        Ok(WorkerResponse::Ready) => Ok((child, stdin, stdout)),
        Ok(WorkerResponse::Error { error }) => {
            Err(xgovernor_core::SessionDomainError::Unavailable {
                message: format!("Pi worker startup failed: {error:?}"),
            })
        }
        Ok(other) => Err(xgovernor_core::SessionDomainError::Unavailable {
            message: format!("Pi worker sent unexpected readiness response: {other:?}"),
        }),
        Err(error) => Err(xgovernor_core::SessionDomainError::Unavailable {
            message: format!("invalid Pi worker readiness response: {error}"),
        }),
    }
}

pub async fn run_worker_from_env() -> Result<(), String> {
    let raw = std::env::var(CONFIG_ENV).map_err(|error| error.to_string())?;
    let config: PiWorkerConfig = serde_json::from_str(&raw).map_err(|error| error.to_string())?;
    let (response_tx, response_rx) = mpsc::unbounded_channel();
    let writer = tokio::spawn(write_responses(response_rx));
    let native = match spawn_native_pi(&config, response_tx.clone()).await {
        Ok(native) => native,
        Err(message) => {
            let _ = response_tx.send(WorkerResponse::Error {
                error: RuntimeError::WorkerUnavailable {
                    message,
                    retryable: true,
                },
            });
            drop(response_tx);
            return writer.await.map_err(|error| error.to_string())?;
        }
    };
    let stdout = native
        .child
        .lock()
        .await
        .stdout
        .take()
        .ok_or_else(|| "Pi stdout was not piped".to_string())?;
    let reader_native = Arc::clone(&native);
    let reader = tokio::spawn(async move { read_native_events(reader_native, stdout).await });
    let _ = response_tx.send(WorkerResponse::Ready);

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let request = match decode_worker_request(&line) {
            Ok(request) => request,
            Err(error) => {
                let _ = response_tx.send(WorkerResponse::Error {
                    error: RuntimeError::InvalidRequest {
                        code: "invalid_worker_request".into(),
                        message: error.to_string(),
                    },
                });
                continue;
            }
        };
        let shutdown = matches!(request, WorkerRequest::Shutdown);
        if let Err(error) = handle_worker_request(&native, request).await {
            emit_error(&native, error);
        }
        if shutdown {
            break;
        }
    }

    native.active_turn.lock().await.take();
    {
        let mut child = native.child.lock().await;
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
    let _ = reader.await;
    drop(native);
    drop(response_tx);
    writer.await.map_err(|error| error.to_string())?
}

async fn write_responses(
    mut responses: mpsc::UnboundedReceiver<WorkerResponse>,
) -> Result<(), String> {
    let mut stdout = tokio::io::stdout();
    while let Some(response) = responses.recv().await {
        let line = encode_worker_response(&response).map_err(|error| error.to_string())?;
        stdout
            .write_all(line.as_bytes())
            .await
            .map_err(|error| error.to_string())?;
        stdout.flush().await.map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn spawn_native_pi(
    config: &PiWorkerConfig,
    responses: mpsc::UnboundedSender<WorkerResponse>,
) -> Result<Arc<NativePi>, String> {
    let mut command = Command::new(&config.executable);
    command
        .arg("--mode")
        .arg("rpc")
        .arg("-e")
        .arg(&config.extension_dir)
        .arg("--session-dir")
        .arg(&config.session_dir)
        .env("XGOVERNOR_BRIDGE_URL", &config.bridge_url)
        .env("XGOVERNOR_BRIDGE_TOKEN", &config.bridge_token)
        .env("XGOVERNOR_WORKSPACE_ROOT", &config.workspace_root);
    if config.use_workspace_cwd {
        command.current_dir(&config.workspace_root);
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    if let Some(session_file) = &config.resume_session_file {
        command.arg("--session").arg(session_file);
    }
    configure_pi_launch(&mut command, &config.session_dir, config.llm.as_ref())
        .await
        .map_err(|error| error.to_string())?;
    let mut child = command.spawn().map_err(|error| {
        format!(
            "failed to spawn Pi executable '{}': {error}",
            config.executable
        )
    })?;
    let stdin = child.stdin.take().expect("Pi stdin was piped");
    Ok(Arc::new(NativePi {
        runtime_id: config.runtime_id.clone(),
        stdin: Mutex::new(stdin),
        child: Mutex::new(child),
        active_turn: Mutex::new(None),
        pending_responses: Mutex::new(HashMap::new()),
        responses,
        session_dir: config.session_dir.clone(),
    }))
}

async fn handle_worker_request(
    native: &Arc<NativePi>,
    request: WorkerRequest,
) -> Result<(), RuntimeError> {
    let request_runtime_id = match &request {
        WorkerRequest::SubmitTurn(request) => Some(request.runtime_id.as_str()),
        WorkerRequest::AnswerInteraction(request) => Some(request.runtime_id.as_str()),
        WorkerRequest::Cancel(request) => Some(request.runtime_id.as_str()),
        WorkerRequest::LoadState(_) | WorkerRequest::Shutdown => None,
    };
    if request_runtime_id.is_some_and(|runtime_id| runtime_id != native.runtime_id) {
        return Err(RuntimeError::InvalidRequest {
            code: "runtime_mismatch".into(),
            message: format!(
                "request runtime '{}' does not match Pi worker runtime '{}'",
                request_runtime_id.unwrap_or_default(),
                native.runtime_id
            ),
        });
    }

    match request {
        WorkerRequest::SubmitTurn(request) => {
            if let Some(llm) = request.llm.as_ref() {
                if let Some(config) =
                    resolve_llm(Some(llm)).map_err(|error| RuntimeError::InvalidRequest {
                        code: "pi_model_configuration".into(),
                        message: error.to_string(),
                    })?
                {
                    configure_instance_llm(native, &config)
                        .await
                        .map_err(|message| RuntimeError::InvalidRequest {
                            code: "pi_model_configuration".into(),
                            message,
                        })?;
                }
            }
            let mut active = native.active_turn.lock().await;
            if let Some(existing) = active.as_ref() {
                Err(RuntimeError::Conflict {
                    code: "active_turn".into(),
                    message: format!("Pi worker already has active turn '{}'", existing.turn_id),
                })
            } else {
                *active = Some(ActiveTurn {
                    turn_id: request.turn_id.clone(),
                    aborted: false,
                    pending_interactions: HashMap::new(),
                    output_sequence: 0,
                });
                drop(active);
                write_native(
                    native,
                    &json!({"type": "prompt", "id": request.turn_id, "message": request.text}),
                )
                .await
                .map_err(worker_unavailable)
            }
        }
        WorkerRequest::AnswerInteraction(request) => {
            let method = {
                let mut active = native.active_turn.lock().await;
                let turn = active.as_mut().ok_or_else(|| RuntimeError::Conflict {
                    code: "no_active_turn".into(),
                    message: "Pi worker has no active turn".into(),
                });
                match turn {
                    Ok(turn) if turn.turn_id == request.turn_id => turn
                        .pending_interactions
                        .remove(&request.interaction_id)
                        .ok_or_else(|| RuntimeError::Conflict {
                            code: "interaction_not_pending".into(),
                            message: format!(
                                "interaction '{}' is not pending",
                                request.interaction_id
                            ),
                        }),
                    Ok(_) => Err(RuntimeError::Conflict {
                        code: "turn_mismatch".into(),
                        message: format!("turn '{}' is not active", request.turn_id),
                    }),
                    Err(error) => Err(error),
                }
            }?;
            let value = map_answer_to_pi_value(&method, &request.answer).map_err(|error| {
                RuntimeError::InvalidRequest {
                    code: "invalid_interaction_answer".into(),
                    message: error.to_string(),
                }
            })?;
            write_native(
                native,
                &json!({"type": "extension_ui_response", "id": request.interaction_id, "value": value}),
            )
            .await
            .map_err(worker_unavailable)
        }
        WorkerRequest::Cancel(request) => {
            let should_abort = {
                let mut active = native.active_turn.lock().await;
                match active.as_mut() {
                    Some(turn)
                        if request
                            .turn_id
                            .as_deref()
                            .is_none_or(|turn_id| turn_id == turn.turn_id) =>
                    {
                        turn.aborted = true;
                        true
                    }
                    _ => false,
                }
            };
            if should_abort {
                write_native(native, &json!({"type": "abort"}))
                    .await
                    .map_err(worker_unavailable)
            } else {
                Ok(())
            }
        }
        WorkerRequest::LoadState(_) => Err(RuntimeError::UnsupportedCapability {
            capability: "state_export".into(),
        }),
        WorkerRequest::Shutdown => Ok(()),
    }
}

fn worker_unavailable(error: std::io::Error) -> RuntimeError {
    RuntimeError::WorkerUnavailable {
        message: format!("Pi native RPC write failed: {error}"),
        retryable: true,
    }
}

fn emit_error(native: &NativePi, error: RuntimeError) {
    let _ = native.responses.send(WorkerResponse::Error { error });
}

async fn write_native(native: &NativePi, command: &Value) -> std::io::Result<()> {
    let mut line = serde_json::to_string(command).expect("Pi command must serialize");
    line.push('\n');
    let mut stdin = native.stdin.lock().await;
    stdin.write_all(line.as_bytes()).await?;
    stdin.flush().await
}

async fn write_native_for_response(native: &NativePi, command: Value) -> Result<Value, String> {
    let id = command
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "correlated Pi command is missing an id".to_string())?
        .to_string();
    let (tx, rx) = oneshot::channel();
    native.pending_responses.lock().await.insert(id.clone(), tx);
    if let Err(error) = write_native(native, &command).await {
        native.pending_responses.lock().await.remove(&id);
        return Err(format!(
            "failed to write correlated Pi RPC command: {error}"
        ));
    }
    let response = timeout(RPC_RESPONSE_TIMEOUT, rx)
        .await
        .map_err(|_| "timed out waiting for Pi RPC response".to_string())?
        .map_err(|_| "Pi exited before replying to RPC command".to_string())?;
    if !response
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(response
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("Pi rejected model configuration")
            .to_string());
    }
    Ok(response)
}

async fn configure_instance_llm(native: &NativePi, config: &PiLlmConfig) -> Result<(), String> {
    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(config).expect("PiLlmConfig always serializes"));
    write_native_for_response(
        native,
        json!({
            "id": format!("xgovernor-model-{}", Uuid::new_v4()),
            "type": "prompt",
            "message": format!("/{} {encoded}", super::PI_MODEL_COMMAND),
        }),
    )
    .await?;
    write_native_for_response(
        native,
        json!({
            "id": format!("xgovernor-set-model-{}", Uuid::new_v4()),
            "type": "set_model",
            "provider": config.provider,
            "modelId": config.model,
        }),
    )
    .await?;
    super::persist_llm_config(&native.session_dir, config)
        .await
        .map_err(|error| error.to_string())
}

async fn read_native_events(native: Arc<NativePi>, stdout: ChildStdout) {
    let mut lines = BufReader::new(stdout).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(message) = serde_json::from_str::<Value>(&line) {
            handle_native_message(&native, message).await;
        }
    }
    native.pending_responses.lock().await.clear();
    if native.active_turn.lock().await.take().is_some() {
        emit_error(
            &native,
            RuntimeError::WorkerUnavailable {
                message: "Pi process exited before the turn reached a terminal state".into(),
                retryable: true,
            },
        );
    }
}

async fn handle_native_message(native: &NativePi, message: Value) {
    match message.get("type").and_then(Value::as_str).unwrap_or("") {
        "response" => handle_response(native, &message).await,
        "message_update" => handle_message_update(native, &message).await,
        "tool_execution_start" => handle_tool_start(native, &message).await,
        "tool_execution_end" => handle_tool_end(native, &message).await,
        "extension_ui_request" => handle_interaction(native, &message).await,
        "agent_settled" => handle_settled(native, &message).await,
        _ => {}
    }
}

fn emit_event(native: &NativePi, event: RuntimeEvent) {
    let _ = native.responses.send(WorkerResponse::Event { event });
}

async fn handle_response(native: &NativePi, message: &Value) {
    if let Some(id) = message.get("id").and_then(Value::as_str) {
        if let Some(waiter) = native.pending_responses.lock().await.remove(id) {
            let _ = waiter.send(message.clone());
            return;
        }
    }
    if message.get("command").and_then(Value::as_str) == Some("prompt")
        && !message
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(true)
        && native.active_turn.lock().await.take().is_some()
    {
        emit_event(
            native,
            RuntimeEvent::Failed {
                error: RuntimeFailure {
                    code: "pi_prompt_rejected".into(),
                    message: message
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("Pi rejected the prompt command")
                        .into(),
                    retryable: false,
                    details: message.clone(),
                },
                usage: SessionUsage::default(),
            },
        );
    }
}

async fn handle_message_update(native: &NativePi, message: &Value) {
    let Some(event) = message.get("assistantMessageEvent") else {
        return;
    };
    let kind = event.get("type").and_then(Value::as_str).unwrap_or("");
    let Some(delta) = event.get("delta").and_then(Value::as_str) else {
        return;
    };
    if !matches!(kind, "text_delta" | "thinking_delta") {
        return;
    }
    let mut active = native.active_turn.lock().await;
    let Some(turn) = active.as_mut() else { return };
    let sequence = turn.next_sequence();
    emit_event(
        native,
        RuntimeEvent::OutputDelta {
            stream_id: if kind == "thinking_delta" {
                "thinking"
            } else {
                "assistant"
            }
            .into(),
            sequence,
            delta: delta.into(),
        },
    );
}

async fn handle_tool_start(native: &NativePi, message: &Value) {
    let Some(activity_id) = message.get("toolCallId").and_then(Value::as_str) else {
        return;
    };
    if native.active_turn.lock().await.is_none() {
        return;
    }
    emit_event(
        native,
        RuntimeEvent::ToolActivity {
            activity_id: activity_id.into(),
            phase: SessionToolActivityPhase::Begin,
            name: message
                .get("toolName")
                .and_then(Value::as_str)
                .unwrap_or("tool")
                .into(),
            status: SessionToolActivityStatus::Running,
            summary: message.get("input").map(Value::to_string),
            ext: Default::default(),
        },
    );
}

async fn handle_tool_end(native: &NativePi, message: &Value) {
    let Some(activity_id) = message.get("toolCallId").and_then(Value::as_str) else {
        return;
    };
    if native.active_turn.lock().await.is_none() {
        return;
    }
    emit_event(
        native,
        RuntimeEvent::ToolActivity {
            activity_id: activity_id.into(),
            phase: SessionToolActivityPhase::End,
            name: message
                .get("toolName")
                .and_then(Value::as_str)
                .unwrap_or("tool")
                .into(),
            status: if message
                .get("isError")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                SessionToolActivityStatus::Failed
            } else {
                SessionToolActivityStatus::Succeeded
            },
            summary: message.get("result").map(Value::to_string),
            ext: Default::default(),
        },
    );
}

async fn handle_interaction(native: &NativePi, message: &Value) {
    let Some(id) = message.get("id").and_then(Value::as_str) else {
        return;
    };
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return;
    };
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    if !DIALOG_METHODS.contains(&method) {
        emit_event(
            native,
            RuntimeEvent::Extension {
                namespace: "pi.ui".into(),
                payload: json!({"method": method, "params": params}),
            },
        );
        return;
    }
    let mut active = native.active_turn.lock().await;
    let Some(turn) = active.as_mut() else { return };
    turn.pending_interactions
        .insert(id.to_string(), method.to_string());
    let options = params
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
                    Value::Object(_) => Some(SessionInteractionOption {
                        id: option
                            .get("id")
                            .or_else(|| option.get("value"))
                            .and_then(Value::as_str)
                            .map(str::to_string)
                            .unwrap_or_else(|| index.to_string()),
                        label: option
                            .get("label")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .into(),
                        description: None,
                        value: option.clone(),
                    }),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    emit_event(
        native,
        RuntimeEvent::InteractionRequested {
            interaction_id: id.into(),
            interaction_kind: method.into(),
            prompt: params
                .get("message")
                .or_else(|| params.get("prompt"))
                .and_then(Value::as_str)
                .unwrap_or(method)
                .into(),
            options,
            ext: Default::default(),
        },
    );
}

async fn handle_settled(native: &NativePi, message: &Value) {
    let Some(turn) = native.active_turn.lock().await.take() else {
        return;
    };
    let usage = extract_usage(message);
    if let Some(error) = message.get("error").filter(|value| !value.is_null()) {
        emit_event(
            native,
            RuntimeEvent::Failed {
                error: RuntimeFailure {
                    code: "pi_agent_error".into(),
                    message: error
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| error.to_string()),
                    retryable: false,
                    details: error.clone(),
                },
                usage,
            },
        );
    } else {
        emit_event(
            native,
            RuntimeEvent::Completed {
                outcome: if turn.aborted {
                    SessionTurnOutcome::Cancelled
                } else {
                    SessionTurnOutcome::Complete
                },
                usage,
            },
        );
    }
}
