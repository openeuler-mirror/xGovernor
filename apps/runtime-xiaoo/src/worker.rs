use crate::xiaoo_backend::{HttpOperationBackend, WorkerConfig};
use crate::{build_runtime, STATE_SCHEMA_VERSION};
use agent_contracts::interaction::InteractionHandle;
use agent_runtime_protocol::RuntimeFailure;
use agent_runtime_protocol::{
    decode_worker_request, encode_worker_response, RuntimeError, RuntimeEvent,
    RuntimeStateSnapshot, WorkerRequest, WorkerResponse,
};
use agent_types::interaction::{InteractionRequest, InteractionResponse};
use agent_types::outcome::AgentOutcome;
use agent_types::AgentId;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use xiaoo_api::runtime::{RuntimeInput, RuntimeOutput, RuntimeState};

pub async fn run_worker_from_env() -> Result<(), String> {
    let stdin = tokio::io::stdin();
    let mut input = BufReader::new(stdin);
    let raw = if std::env::var_os("XGOVERNOR_XIAOO_WORKER_CONFIG_STDIN").is_some() {
        let mut raw = String::new();
        if input.read_line(&mut raw).await.map_err(|e| e.to_string())? == 0 {
            return Err("missing xiaoO worker bootstrap config".into());
        }
        raw
    } else {
        std::env::var("XGOVERNOR_XIAOO_WORKER_CONFIG").map_err(|e| e.to_string())?
    };
    let config: WorkerConfig = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
    let backend = Arc::new(HttpOperationBackend::new(&config));
    let mut role_settings = config.role_settings.clone();
    let mut llm = config.llm.clone();
    let responses_state = Arc::new(std::sync::Mutex::new(config.responses_state.clone()));
    let mut state = RuntimeState::from_snapshot(config.loop_state, CancellationToken::new());
    emit(&WorkerResponse::Ready)?;

    let mut lines = input.lines();
    let (request_tx, mut request_rx) = mpsc::unbounded_channel::<Result<WorkerRequest, String>>();
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            let request = decode_worker_request(&line).map_err(|error| error.to_string());
            if request_tx.send(request).is_err() {
                break;
            }
        }
        let _ = request_tx.send(Ok(WorkerRequest::Shutdown));
    });

    while let Some(request) = request_rx.recv().await {
        let request = match request {
            Ok(request) => request,
            Err(message) => {
                emit(&WorkerResponse::Error {
                    error: RuntimeError::InvalidRequest {
                        code: "invalid_worker_request".into(),
                        message,
                    },
                })?;
                continue;
            }
        };
        match request {
            WorkerRequest::SubmitTurn(request) => {
                // Construct the proposed configuration before accepting it. Failed
                // switches leave history and the last accepted model untouched.
                let proposed = async {
                    let roles = role_settings.for_turn(&request.ext)?;
                    let next_llm = llm.apply(request.llm.as_ref())?;
                    let effort = request
                        .reasoning_effort
                        .as_deref()
                        .unwrap_or("off")
                        .parse()
                        .map_err(|_| crate::policy::invalid("invalid reasoning effort"))?;
                    if next_llm.effective_capabilities().reasoning == Some(false)
                        && effort != agent_types::ReasoningEffort::Off
                    {
                        return Err(crate::policy::invalid("model does not support reasoning"));
                    }
                    let (runtime, meter) = build_runtime(
                        &next_llm,
                        Arc::clone(&responses_state),
                        Arc::new(backend.for_turn(request.turn_id.clone())),
                        &roles,
                    )
                    .await?;
                    Ok::<_, xgovernor_core::SessionDomainError>((
                        roles, next_llm, effort, runtime, meter,
                    ))
                }
                .await;
                let (roles, next_llm, effort, runtime, usage_meter) = match proposed {
                    Ok(value) => value,
                    Err(error) => {
                        emit(&WorkerResponse::Error {
                            error: RuntimeError::InvalidRequest {
                                code: "invalid_turn_configuration".into(),
                                message: error.to_string(),
                            },
                        })?;
                        continue;
                    }
                };
                role_settings = roles;
                llm = next_llm;
                state.cancel = CancellationToken::new();
                let (event_tx, mut event_rx) = mpsc::unbounded_channel();
                let pending = Arc::new(Mutex::new(HashMap::new()));
                let interaction = Arc::new(WorkerInteractionHandle {
                    events: event_tx.clone(),
                    pending,
                });
                let sink = Arc::new(crate::GovernorEventSink::new(event_tx));
                let cancel = state.cancel.clone();
                let mut run = Box::pin(
                    runtime.run(
                        &mut state,
                        RuntimeInput::new(request.text)
                            .with_visible_tools(runtime.visible_tools())
                            .with_agent_id(AgentId("xiaoo".to_string()))
                            .with_event_sink(sink)
                            .with_interaction(interaction.clone())
                            .with_reasoning_effort(effort),
                    ),
                );
                let mut terminal = loop {
                    tokio::select! {
                        result = &mut run => break match result {
                            Ok(RuntimeOutput::Complete(outcome)) => RuntimeEvent::Completed {
                                outcome: match outcome { AgentOutcome::Complete { .. } => session_protocol::SessionTurnOutcome::Complete, AgentOutcome::MaxTurnsReached { .. } => session_protocol::SessionTurnOutcome::MaxTurns, AgentOutcome::BudgetExhausted { .. } => session_protocol::SessionTurnOutcome::BudgetExhausted, AgentOutcome::Cancelled { .. } => session_protocol::SessionTurnOutcome::Cancelled },
                                usage: Default::default(),
                            },
                            Ok(RuntimeOutput::Suspended(_)) => failed("xiaoo_suspended", "xiaoO suspended without a pending interaction"),
                            Err(error) => failed("xiaoo_runtime_error", &error.to_string()),
                        },
                        Some(Ok(request)) = request_rx.recv() => match request {
                            WorkerRequest::AnswerInteraction(request) => {
                                if let Some(waiter) = interaction_pending(&interaction, &request.interaction_id).await { let _ = waiter.send(interaction_answer_to_agent(request.answer)); }
                            }
                            WorkerRequest::Cancel(_) | WorkerRequest::Shutdown => cancel.cancel(),
                            _ => {}
                        },
                        Some(event) = event_rx.recv() => { emit(&WorkerResponse::Event { event })?; }
                    }
                };
                drop(run);
                let usage = usage_meter.read();
                match &mut terminal {
                    RuntimeEvent::Completed {
                        usage: reported, ..
                    }
                    | RuntimeEvent::Failed {
                        usage: reported, ..
                    } => *reported = usage,
                    _ => unreachable!("run always produces a terminal event"),
                }
                while let Ok(event) = event_rx.try_recv() {
                    emit(&WorkerResponse::Event { event })?;
                }
                let snapshot = state.to_snapshot();
                responses_state
                    .lock()
                    .expect("Responses state lock")
                    .retain_history(&snapshot.messages);
                emit(&WorkerResponse::State {
                    state: RuntimeStateSnapshot::try_new(
                        "xiaoo",
                        STATE_SCHEMA_VERSION,
                        &WorkerTurnState {
                            loop_state: snapshot,
                            llm: llm.clone(),
                            role_settings: role_settings.clone(),
                            responses_state: responses_state
                                .lock()
                                .expect("Responses state lock")
                                .clone(),
                        },
                    )
                    .map_err(|error| format!("failed to encode xiaoO state: {error:?}"))?,
                })?;
                emit(&WorkerResponse::Event { event: terminal })?;
            }
            WorkerRequest::LoadState(snapshot) => {
                match snapshot.decode::<WorkerTurnState>("xiaoo", STATE_SCHEMA_VERSION) {
                    Ok(loaded) => {
                        state = RuntimeState::from_snapshot(
                            loaded.loop_state,
                            CancellationToken::new(),
                        );
                        llm = loaded.llm;
                        role_settings = loaded.role_settings;
                        *responses_state.lock().expect("Responses state lock") =
                            loaded.responses_state;
                    }
                    Err(error) => emit(&WorkerResponse::Error { error })?,
                }
            }
            WorkerRequest::Shutdown => break,
            WorkerRequest::AnswerInteraction(_) | WorkerRequest::Cancel(_) => {}
        }
    }
    Ok(())
}

fn emit(response: &WorkerResponse) -> Result<(), String> {
    print!(
        "{}",
        encode_worker_response(response).map_err(|e| e.to_string())?
    );
    std::io::stdout().flush().map_err(|e| e.to_string())
}

fn failed(code: &str, message: &str) -> RuntimeEvent {
    RuntimeEvent::Failed {
        error: RuntimeFailure {
            code: code.into(),
            message: message.into(),
            retryable: false,
            details: Value::Null,
        },
        usage: Default::default(),
    }
}

struct WorkerInteractionHandle {
    events: mpsc::UnboundedSender<RuntimeEvent>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<InteractionResponse>>>>,
}

async fn interaction_pending(
    handle: &WorkerInteractionHandle,
    id: &str,
) -> Option<oneshot::Sender<InteractionResponse>> {
    handle.pending.lock().await.remove(id)
}

#[async_trait]
impl InteractionHandle for WorkerInteractionHandle {
    async fn ask(&self, request: &InteractionRequest) -> InteractionResponse {
        let id = format!("interaction-{}", Uuid::new_v4());
        let (prompt, kind, options) = match request {
            InteractionRequest::Confirm { prompt, .. } => {
                (prompt.clone(), "confirm".into(), vec![])
            }
            InteractionRequest::TextInput { prompt, .. } => {
                (prompt.clone(), "text_input".into(), vec![])
            }
            InteractionRequest::Choice {
                prompt, options, ..
            } => (
                prompt.clone(),
                "choice".into(),
                options
                    .iter()
                    .map(|v| session_protocol::SessionInteractionOption {
                        id: v.clone(),
                        label: v.clone(),
                        description: None,
                        value: Value::String(v.clone()),
                    })
                    .collect(),
            ),
        };
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id.clone(), tx);
        if self
            .events
            .send(RuntimeEvent::InteractionRequested {
                interaction_id: id,
                interaction_kind: kind,
                prompt,
                options,
                ext: Default::default(),
            })
            .is_err()
        {
            return crate::cancelled_interaction_response(request);
        }
        rx.await
            .unwrap_or_else(|_| crate::cancelled_interaction_response(request))
    }
}

fn interaction_answer_to_agent(
    answer: session_protocol::SessionInteractionAnswer,
) -> InteractionResponse {
    match answer {
        session_protocol::SessionInteractionAnswer::Confirm(allowed) => {
            InteractionResponse::Confirmed { allowed }
        }
        session_protocol::SessionInteractionAnswer::Text(answer) => InteractionResponse::Text {
            value: answer.value,
            display_value: answer.display_value,
        },
        session_protocol::SessionInteractionAnswer::Selection(values) => {
            InteractionResponse::Choice {
                value: values.into_iter().next(),
            }
        }
        session_protocol::SessionInteractionAnswer::Cancelled
        | session_protocol::SessionInteractionAnswer::Data(_) => {
            InteractionResponse::Choice { value: None }
        }
    }
}

/// Internal worker snapshot atomically acknowledges the configuration used for
/// a turn along with its history; the public persisted schema remains v1.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct WorkerTurnState {
    pub loop_state: xiaoo_core::LoopStateSnapshot,
    pub llm: crate::xiaoo_backend::PersistedLlm,
    pub role_settings: crate::RoleSettings,
    pub responses_state: crate::responses::ResponsesState,
}
impl From<&crate::XiaooPersistedState> for WorkerTurnState {
    fn from(state: &crate::XiaooPersistedState) -> Self {
        Self {
            loop_state: state.loop_state.clone(),
            llm: state.llm.clone(),
            role_settings: state.role_settings.clone(),
            responses_state: state.responses_state.clone(),
        }
    }
}
