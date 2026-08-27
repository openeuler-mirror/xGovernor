use crate::{
    RuntimeCancelRequest, RuntimeError, RuntimeEvent, RuntimeFailure, RuntimeInteractionRequest,
    RuntimeStateSnapshot, RuntimeTurnRequest, WorkerRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use session_protocol::SessionUsage;
use std::collections::BTreeSet;

/// Internal worker response envelope shared by runtime worker supervisors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkerResponse {
    Ready,
    Event {
        event: RuntimeEvent,
    },
    State {
        state: RuntimeStateSnapshot,
    },
    Error {
        error: RuntimeError,
    },
    #[serde(other)]
    Unknown,
}

pub fn encode_worker_request(request: &WorkerRequest) -> Result<String, serde_json::Error> {
    encode_ndjson(request)
}

pub fn decode_worker_request(line: &str) -> Result<WorkerRequest, serde_json::Error> {
    decode_ndjson(line)
}

pub fn encode_worker_response(response: &WorkerResponse) -> Result<String, serde_json::Error> {
    encode_ndjson(response)
}

pub fn decode_worker_response(line: &str) -> Result<WorkerResponse, serde_json::Error> {
    decode_ndjson(line)
}

/// Projects worker control-plane failures into the normalized terminal event
/// vocabulary used after a turn has already been accepted.
pub fn worker_error_event(error: RuntimeError) -> RuntimeEvent {
    let (code, message, retryable) = match error {
        RuntimeError::WorkerUnavailable { message, retryable } => {
            ("worker_unavailable", message, retryable)
        }
        other => (
            "worker_protocol_error",
            format!("worker protocol error: {other:?}"),
            false,
        ),
    };
    RuntimeEvent::Failed {
        error: RuntimeFailure {
            code: code.into(),
            message,
            retryable,
            details: Value::Null,
        },
        usage: SessionUsage::default(),
    }
}

fn encode_ndjson<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let mut line = serde_json::to_string(value)?;
    line.push('\n');
    Ok(line)
}

fn decode_ndjson<T: for<'de> Deserialize<'de>>(line: &str) -> Result<T, serde_json::Error> {
    serde_json::from_str(line.trim_end_matches(['\r', '\n']))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerEventDisposition {
    Continue,
    Terminal,
}

#[derive(Debug, Clone)]
struct ActiveTurn {
    runtime_id: String,
    turn_id: String,
    terminal_seen: bool,
    cancel_requested: bool,
    pending_interactions: BTreeSet<String>,
}

/// Runtime-neutral lifecycle checks shared by worker supervisors and their
/// contract tests. Process and provider ownership remain host concerns.
#[derive(Debug, Clone, Default)]
pub struct WorkerTurnTracker {
    active: Option<ActiveTurn>,
}

impl WorkerTurnTracker {
    pub fn accept_turn(&mut self, request: &RuntimeTurnRequest) -> Result<(), RuntimeError> {
        if self.active.as_ref().is_some_and(|turn| !turn.terminal_seen) {
            return Err(RuntimeError::Conflict {
                code: "active_turn".into(),
                message: "worker already has an active turn".into(),
            });
        }
        self.active = Some(ActiveTurn {
            runtime_id: request.runtime_id.clone(),
            turn_id: request.turn_id.clone(),
            terminal_seen: false,
            cancel_requested: false,
            pending_interactions: BTreeSet::new(),
        });
        Ok(())
    }

    pub fn observe_event(
        &mut self,
        event: &RuntimeEvent,
    ) -> Result<WorkerEventDisposition, RuntimeError> {
        let turn = self.active.as_mut().ok_or_else(|| RuntimeError::Conflict {
            code: "no_active_turn".into(),
            message: "worker emitted an event without an active turn".into(),
        })?;
        if turn.terminal_seen {
            return Err(RuntimeError::Conflict {
                code: "turn_already_terminal".into(),
                message: format!("turn '{}' already emitted a terminal event", turn.turn_id),
            });
        }
        if let RuntimeEvent::InteractionRequested { interaction_id, .. } = event {
            if !turn.pending_interactions.insert(interaction_id.clone()) {
                return Err(RuntimeError::Conflict {
                    code: "duplicate_interaction".into(),
                    message: format!("interaction '{interaction_id}' is already pending"),
                });
            }
        }
        if turn.cancel_requested
            && matches!(
                event,
                RuntimeEvent::Completed {
                    outcome,
                    ..
                } if *outcome != session_protocol::SessionTurnOutcome::Cancelled
            )
        {
            return Err(RuntimeError::Conflict {
                code: "cancel_outcome_mismatch".into(),
                message: format!(
                    "cancelled turn '{}' must complete with the cancelled outcome",
                    turn.turn_id
                ),
            });
        }
        if event.is_terminal() {
            turn.terminal_seen = true;
            turn.pending_interactions.clear();
            Ok(WorkerEventDisposition::Terminal)
        } else {
            Ok(WorkerEventDisposition::Continue)
        }
    }

    pub fn answer_interaction(
        &mut self,
        request: &RuntimeInteractionRequest,
    ) -> Result<(), RuntimeError> {
        let turn = self.matching_active_turn(&request.runtime_id, &request.turn_id)?;
        if !turn.pending_interactions.remove(&request.interaction_id) {
            return Err(RuntimeError::Conflict {
                code: "interaction_not_pending".into(),
                message: format!("interaction '{}' is not pending", request.interaction_id),
            });
        }
        Ok(())
    }

    /// No active turn and stale turn ids are intentional no-ops.
    pub fn cancel(&mut self, request: &RuntimeCancelRequest) -> bool {
        let Some(turn) = self.active.as_mut().filter(|turn| !turn.terminal_seen) else {
            return false;
        };
        if turn.runtime_id != request.runtime_id
            || request
                .turn_id
                .as_deref()
                .is_some_and(|turn_id| turn_id != turn.turn_id)
        {
            return false;
        }
        turn.cancel_requested = true;
        true
    }

    pub fn cancel_requested(&self) -> bool {
        self.active
            .as_ref()
            .is_some_and(|turn| turn.cancel_requested)
    }

    /// Converts an unexpected worker exit into the shared transport error and
    /// marks the accepted turn terminal so it cannot fail twice.
    pub fn worker_unavailable(&mut self, message: impl Into<String>) -> Option<WorkerResponse> {
        let turn = self.active.as_mut()?;
        if turn.terminal_seen {
            return None;
        }
        turn.terminal_seen = true;
        turn.pending_interactions.clear();
        Some(WorkerResponse::Error {
            error: RuntimeError::WorkerUnavailable {
                message: message.into(),
                retryable: true,
            },
        })
    }

    fn matching_active_turn(
        &mut self,
        runtime_id: &str,
        turn_id: &str,
    ) -> Result<&mut ActiveTurn, RuntimeError> {
        let turn = self.active.as_mut().ok_or_else(|| RuntimeError::Conflict {
            code: "no_active_turn".into(),
            message: "worker has no active turn".into(),
        })?;
        if turn.terminal_seen || turn.runtime_id != runtime_id || turn.turn_id != turn_id {
            return Err(RuntimeError::Conflict {
                code: "turn_mismatch".into(),
                message: format!("turn '{turn_id}' is not active for runtime '{runtime_id}'"),
            });
        }
        Ok(turn)
    }
}
