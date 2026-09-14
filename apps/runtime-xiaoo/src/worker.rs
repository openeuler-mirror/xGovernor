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
    let raw = std::env::var("XGOVERNOR_XIAOO_WORKER_CONFIG").map_err(|e| e.to_string())?;
    let config: WorkerConfig = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
    let backend = Arc::new(HttpOperationBackend::new(&config));
    let mut role_settings = config.role_settings.clone();
    let mut state = RuntimeState::from_snapshot(config.loop_state, CancellationToken::new());
    emit(&WorkerResponse::Ready)?;

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
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
                role_settings = match role_settings.for_turn(&request.ext) {
                    Ok(settings) => settings,
                    Err(error) => {
                        emit(&WorkerResponse::Error {
                            error: RuntimeError::InvalidRequest {
                                code: "invalid_role_policy".into(),
                                message: error.to_string(),
                            },
                        })?;
                        continue;
                    }
                };
                let effort = request
                    .reasoning_effort
                    .as_deref()
                    .unwrap_or("off")
                    .parse()
                    .map_err(|e| format!("invalid reasoning effort: {e}"))?;
                let (runtime, usage_meter) = build_runtime(
                    &config.llm,
                    request.llm.as_ref().and_then(|llm| llm.model.as_deref()),
                    backend.clone(),
                    &role_settings,
                )
                .await
                .map_err(|e| format!("failed to build xiaoO runtime: {e:?}"))?;
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
                emit(&WorkerResponse::State {
                    state: RuntimeStateSnapshot::try_new(
                        "xiaoo",
                        STATE_SCHEMA_VERSION,
                        &state.to_snapshot(),
                    )
                    .map_err(|error| format!("failed to encode xiaoO state: {error:?}"))?,
                })?;
                emit(&WorkerResponse::Event { event: terminal })?;
            }
            WorkerRequest::LoadState(snapshot) => {
                match snapshot.decode("xiaoo", STATE_SCHEMA_VERSION) {
                    Ok(loop_state) => {
                        state = RuntimeState::from_snapshot(loop_state, CancellationToken::new())
                    }
                    Err(error) => emit(&WorkerResponse::Error { error })?,
                }
            }
            WorkerRequest::Shutdown
            | WorkerRequest::AnswerInteraction(_)
            | WorkerRequest::Cancel(_) => {}
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
