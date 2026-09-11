use super::response::session_error;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use serde::Deserialize;
use session_protocol::{
    SessionCancelRequest, SessionCheckpointDeleteRequest, SessionCheckpointRequest,
    SessionCloseRequest, SessionDetachRequest, SessionEvent, SessionExecRequest,
    SessionFileReadRequest, SessionFileWriteRequest, SessionForkRequest, SessionHeartbeatRequest,
    SessionInteractionRequest, SessionLoadRequest, SessionOpenRequest, SessionTurnRequest,
    SessionWireError,
};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use xgovernor_core::{project_session_error, SecurityContext, SessionApplication};

const STREAM_ENTRY_TTL: Duration = Duration::from_secs(30);
const STREAM_SWEEP_INTERVAL: Duration = Duration::from_secs(10);
const MAX_PENDING_STREAMS: usize = 1000;
/// Cap on `GET /api/v1/sessions` — v1 has no real pagination
/// (`docs/tenancy_design.md` §4 addendum): callers get the most-recent N
/// active sessions, full stop.
const DEFAULT_SESSION_LIST_LIMIT: usize = 100;
const DEFAULT_CHECKPOINT_LIST_LIMIT: usize = 100;
const MAX_CHECKPOINT_LIST_LIMIT: usize = 200;

#[derive(Debug, Deserialize)]
struct CheckpointListQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

/// One pending stream: the receiving half of a turn's forwarded eventx w
/// channel, plus when it was registered (for TTL expiry).
struct StreamEntry {
    receiver: mpsc::Receiver<SessionEvent>,
    registered_at: Instant,
}

#[derive(Clone)]
pub struct SessionHttpState {
    application: SessionApplication,
    streams: Arc<Mutex<HashMap<String, HashMap<String, StreamEntry>>>>,
}

impl SessionHttpState {
    pub fn new(application: SessionApplication) -> Self {
        Self {
            application,
            streams: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn spawn_stream_sweeper(&self) -> tokio::task::JoinHandle<()> {
        let streams = Arc::clone(&self.streams);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(STREAM_SWEEP_INTERVAL);
            loop {
                ticker.tick().await;
                sweep_expired_streams(&streams).await;
            }
        })
    }

    #[cfg(test)]
    async fn pending_stream_count(&self) -> usize {
        self.streams
            .lock()
            .await
            .values()
            .map(|turns| turns.len())
            .sum()
    }
}

async fn register_stream(
    streams: &Mutex<HashMap<String, HashMap<String, StreamEntry>>>,
    runtime_id: &str,
    turn_id: &str,
    events: mpsc::Receiver<SessionEvent>,
) -> bool {
    let mut streams = streams.lock().await;
    let total: usize = streams.values().map(|turns| turns.len()).sum();
    if total >= MAX_PENDING_STREAMS {
        tracing::warn!(
            runtime_id,
            turn_id,
            limit = MAX_PENDING_STREAMS,
            "pending stream table at capacity; this turn's stream will not be \
             attachable (the turn itself keeps running)"
        );
        return false;
    }
    streams.entry(runtime_id.to_string()).or_default().insert(
        turn_id.to_string(),
        StreamEntry {
            receiver: events,
            registered_at: Instant::now(),
        },
    );
    true
}

/// One sweep pass: drops every stream entry whose SSE consumer has not
/// attached within [`STREAM_ENTRY_TTL`] of `submit_turn` registering it.
/// Split out from [`SessionHttpState::spawn_stream_sweeper`] so a test can
/// drive exactly one pass synchronously (mirrors
/// `xgovernor_core::orphan_reaper::sweep_once`).
async fn sweep_expired_streams(streams: &Mutex<HashMap<String, HashMap<String, StreamEntry>>>) {
    let mut streams = streams.lock().await;
    let mut expired = 0usize;
    streams.retain(|_runtime_id, turns| {
        let before = turns.len();
        turns.retain(|_turn_id, entry| entry.registered_at.elapsed() < STREAM_ENTRY_TTL);
        expired += before - turns.len();
        !turns.is_empty()
    });
    if expired > 0 {
        tracing::debug!(
            expired,
            ttl_secs = STREAM_ENTRY_TTL.as_secs(),
            "stream sweeper expired unclaimed turn event streams"
        );
    }
}

pub fn session_router(state: Arc<SessionHttpState>) -> Router {
    Router::new()
        .route("/api/v1/sessions/open", post(open_session))
        .route("/api/v1/sessions/exec", post(exec_session))
        .route("/api/v1/sessions/files/read", post(read_session_file))
        .route("/api/v1/sessions/files/write", post(write_session_file))
        .route("/api/v1/sessions/turns", post(submit_turn))
        .route("/api/v1/sessions/interactions", post(answer_interaction))
        .route("/api/v1/sessions/close", post(close_session))
        .route("/api/v1/sessions/detach", post(detach_session))
        .route("/api/v1/sessions/heartbeat", post(heartbeat_session))
        .route("/api/v1/sessions/cancel", post(cancel_turn))
        .route("/api/v1/sessions/fork", post(fork_session))
        .route("/api/v1/sessions/checkpoint", post(checkpoint_session))
        .route(
            "/api/v1/sessions/checkpoint/delete",
            post(delete_checkpoint),
        )
        .route("/api/v1/sessions/load", post(load_checkpoint))
        .route(
            "/api/v1/sessions/:runtime_id/turns/:turn_id/events",
            get(stream_turn_events),
        )
        .route("/api/v1/sessions", get(list_sessions))
        .route("/api/v1/checkpoints", get(list_checkpoints))
        .with_state(state)
}

async fn exec_session(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(request): Json<SessionExecRequest>,
) -> Response {
    match state.application.exec(&ctx, request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn read_session_file(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(request): Json<SessionFileReadRequest>,
) -> Response {
    match state.application.read_file(&ctx, request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn write_session_file(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(request): Json<SessionFileWriteRequest>,
) -> Response {
    match state.application.write_file(&ctx, request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn open_session(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(request): Json<SessionOpenRequest>,
) -> Response {
    match state.application.open(&ctx, request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn submit_turn(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(request): Json<SessionTurnRequest>,
) -> Response {
    match state.application.submit_turn(&ctx, request).await {
        Ok(submission) => {
            // `events` is `None` when the receipt was replayed for a
            // duplicate `client_request_id`: no new stream exists, and the
            // original turn's stream (if still unclaimed) must not be
            // clobbered.
            if let Some(events) = submission.events {
                register_stream(
                    &state.streams,
                    &submission.receipt.runtime_id,
                    &submission.receipt.turn_id,
                    events,
                )
                .await;
            }
            (StatusCode::ACCEPTED, Json(submission.receipt)).into_response()
        }
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn answer_interaction(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(request): Json<SessionInteractionRequest>,
) -> Response {
    match state.application.answer_interaction(&ctx, request).await {
        Ok(receipt) => (StatusCode::ACCEPTED, Json(receipt)).into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn close_session(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(request): Json<SessionCloseRequest>,
) -> Response {
    let result = state
        .application
        .close(&ctx, &request.runtime_id, request.lease)
        .await;
    // Any turn stream still sitting unclaimed for this runtime_id will never
    // be attached to now that the session itself is closing — sweep it
    // immediately rather than waiting out STREAM_ENTRY_TTL. Done regardless
    // of whether close succeeded: a failure (e.g. NotFound because it was
    // already closed through another path) still means nobody is coming
    // back for this runtime_id's streams either.
    state.streams.lock().await.remove(&request.runtime_id);
    match result {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn detach_session(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(request): Json<SessionDetachRequest>,
) -> Response {
    match state
        .application
        .detach(&ctx, &request.runtime_id, request.lease)
        .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn heartbeat_session(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(request): Json<SessionHeartbeatRequest>,
) -> Response {
    match state
        .application
        .heartbeat(&ctx, &request.runtime_id, request.lease)
        .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

/// `SessionCancelRequest.lease` is accepted on the wire for symmetry with the
/// other control requests but is not forwarded: `SessionApplication::cancel`
/// deliberately performs no lease check (interrupting your own in-flight turn
/// does not require holding the single-writer lease). There is also no
/// dedicated wire response type for cancel in `session-protocol` yet (unlike
/// close/detach/heartbeat) since cancel does not return a session-lifecycle
/// fact worth projecting; a bare 202 marks the request as accepted.
async fn cancel_turn(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(request): Json<SessionCancelRequest>,
) -> Response {
    match state
        .application
        .cancel(&ctx, &request.runtime_id, request.turn_id.as_deref())
        .await
    {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn fork_session(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(request): Json<SessionForkRequest>,
) -> Response {
    match state.application.fork(&ctx, request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn checkpoint_session(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(request): Json<SessionCheckpointRequest>,
) -> Response {
    match state.application.checkpoint(&ctx, request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn load_checkpoint(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(request): Json<SessionLoadRequest>,
) -> Response {
    match state.application.load_checkpoint(&ctx, request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn delete_checkpoint(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Json(request): Json<SessionCheckpointDeleteRequest>,
) -> Response {
    match state.application.delete_checkpoint(&ctx, request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn list_sessions(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
) -> Response {
    match state
        .application
        .list_sessions(&ctx, DEFAULT_SESSION_LIST_LIMIT)
        .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn list_checkpoints(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Query(query): Query<CheckpointListQuery>,
) -> Response {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_CHECKPOINT_LIST_LIMIT)
        .clamp(1, MAX_CHECKPOINT_LIST_LIMIT);
    let offset = query.offset.unwrap_or(0);
    match state
        .application
        .list_checkpoints(&ctx, limit, offset)
        .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(project_session_error(error)),
    }
}

async fn stream_turn_events(
    State(state): State<Arc<SessionHttpState>>,
    Extension(ctx): Extension<SecurityContext>,
    Path((runtime_id, turn_id)): Path<(String, String)>,
) -> Response {
    // Ownership must be checked before the stream table is touched at all:
    // otherwise a cross-tenant caller could learn whether a `runtime_id`
    // exists (and had a live stream) purely from a 404-vs-nothing-consumed
    // timing/side-channel, even though the eventual response is the same
    // `NotFound`. `check_visible` reuses `require_session`'s 404-not-403
    // semantics (`docs/tenancy_design.md` §3).
    if let Err(error) = state.application.check_visible(&ctx, &runtime_id).await {
        return session_error(project_session_error(error));
    }
    let mut streams = state.streams.lock().await;
    let Some(entry) = streams
        .get_mut(&runtime_id)
        .and_then(|turns| turns.remove(&turn_id))
    else {
        return session_error(SessionWireError::NotFound {
            runtime_id: format!("{runtime_id}/turns/{turn_id}"),
        });
    };
    // Prune the now-possibly-empty inner map. Safe to re-borrow `streams`
    // here: the `get_mut` borrow above (and the `turns` reference it
    // produced) has already gone out of scope.
    if streams
        .get(&runtime_id)
        .is_some_and(|turns| turns.is_empty())
    {
        streams.remove(&runtime_id);
    }
    drop(streams);
    let stream = ReceiverStream::new(entry.receiver).map(move |event| {
        let event_name = session_event_name(&event);
        let data = serde_json::to_string(&event).unwrap_or_else(|error| {
            serde_json::json!({
                "kind": "turn_failed",
                "runtime_id": runtime_id,
                "turn_id": turn_id,
                "error": {
                    "code": "serialization_failed",
                    "message": error.to_string(),
                    "retryable": false,
                    "details": null
                },
                "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
            })
            .to_string()
        });
        Ok::<_, Infallible>(Event::default().event(event_name).data(data))
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

fn session_event_name(event: &SessionEvent) -> &'static str {
    match event {
        SessionEvent::OutputDelta { .. } => "output_delta",
        SessionEvent::ToolActivity { .. } => "tool_activity",
        SessionEvent::InteractionRequested { .. } => "interaction_requested",
        SessionEvent::TurnCompleted { .. } => "turn_completed",
        SessionEvent::TurnFailed { .. } => "turn_failed",
        SessionEvent::Extension { .. } => "extension",
    }
}
