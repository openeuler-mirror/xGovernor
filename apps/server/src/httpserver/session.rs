use super::response::session_error;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use session_protocol::{
    SessionCancelRequest, SessionCheckpointDeleteRequest, SessionCheckpointRequest,
    SessionCloseRequest, SessionDetachRequest, SessionEvent, SessionForkRequest,
    SessionHeartbeatRequest, SessionInteractionRequest, SessionLoadRequest, SessionOpenRequest,
    SessionTurnRequest, SessionWireError,
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
        .with_state(state)
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use session_protocol::SessionSubmitReceipt;
    use std::collections::BTreeSet;
    use tower::ServiceExt;
    use xgovernor_core::application::{SessionRepository, TurnIdGenerator};
    use xgovernor_core::{
        Clock, NormalizedSessionEnvironment, RuntimeAdapter, RuntimeEvent, RuntimeEventReceiver,
        RuntimeIdGenerator, RuntimeInteractionInput, RuntimeStartRequest, RuntimeTurnInput,
        SessionDomainError, SessionEnvironmentNormalizer, SessionListPage, SessionRecord,
    };

    struct EmptyRepository;

    #[async_trait]
    impl SessionRepository for EmptyRepository {
        async fn get(
            &self,
            _runtime_id: &str,
        ) -> Result<Option<SessionRecord>, SessionDomainError> {
            Ok(Some(test_record()))
        }

        async fn save(&self, _record: SessionRecord) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn list_active(
            &self,
            _tenant_id: Option<&str>,
            _limit: usize,
        ) -> Result<SessionListPage, SessionDomainError> {
            Ok(SessionListPage {
                sessions: Vec::new(),
                total_active: 0,
            })
        }
    }

    fn test_record() -> SessionRecord {
        SessionRecord {
            runtime_id: "runtime-1".into(),
            conversation_id: "conversation-1".into(),
            sender_id: "sender-1".into(),
            status: xgovernor_core::SessionStatus::Idle,
            created_at_ms: 1,
            updated_at_ms: 1,
            workspace: xgovernor_core::WorkspaceFacts {
                workspace_id: "workspace-1".into(),
                root: ".".into(),
                access: xgovernor_core::WorkspaceAccess::ReadWrite,
                revision: None,
                metadata: serde_json::Value::Null,
            },
            isolation: xgovernor_core::IsolationFacts {
                boundary: xgovernor_core::IsolationBoundary::Host,
                workspace_access: xgovernor_core::WorkspaceAccess::ReadWrite,
                network: xgovernor_core::NetworkIsolation::None,
                metadata: serde_json::Value::Null,
            },
            capabilities: xgovernor_core::EffectiveCapabilities {
                sandbox: Default::default(),
                runtime: [
                    xgovernor_core::RuntimeCapability::ModelOverride,
                    xgovernor_core::RuntimeCapability::ReasoningControl,
                    xgovernor_core::RuntimeCapability::Interaction,
                ]
                .into_iter()
                .collect(),
            },
            runtime: xgovernor_core::OpaqueRuntimeState {
                runtime_kind: "test".into(),
                schema_version: 1,
                state: serde_json::Value::Null,
            },
            llm: None,
            lease: None,
            lineage: None,
            last_error: None,
            tenant_id: None,
            created_by: "test".to_string(),
        }
    }

    /// A repository whose single record is owned by `tenant_id: "tenant-a"`,
    /// for exercising the cross-tenant 404 path over real HTTP requests
    /// (`security_layer` → `Extension<SecurityContext>` →
    /// `SessionApplication::require_session`'s `ctx.owns(...)` check).
    struct TenantARepository;

    #[async_trait]
    impl SessionRepository for TenantARepository {
        async fn get(
            &self,
            _runtime_id: &str,
        ) -> Result<Option<SessionRecord>, SessionDomainError> {
            let mut record = test_record();
            record.tenant_id = Some("tenant-a".to_string());
            Ok(Some(record))
        }

        async fn save(&self, _record: SessionRecord) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn list_active(
            &self,
            _tenant_id: Option<&str>,
            _limit: usize,
        ) -> Result<SessionListPage, SessionDomainError> {
            Ok(SessionListPage {
                sessions: Vec::new(),
                total_active: 0,
            })
        }
    }

    struct FixedTurnId;

    impl TurnIdGenerator for FixedTurnId {
        fn next_turn_id(&self) -> String {
            "turn-http".into()
        }
    }

    impl RuntimeIdGenerator for FixedTurnId {
        fn next_runtime_id(&self) -> String {
            "runtime-http".into()
        }
    }

    impl Clock for FixedTurnId {
        fn now_ms(&self) -> u64 {
            42
        }
    }

    struct UnusedEnvironment;

    #[async_trait]
    impl SessionEnvironmentNormalizer for UnusedEnvironment {
        async fn normalize(
            &self,
            _ctx: &SecurityContext,
            _request: &SessionOpenRequest,
        ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
            unreachable!("HTTP turn test does not open a session")
        }
    }

    struct CompletingRuntime;

    #[async_trait]
    impl RuntimeAdapter for CompletingRuntime {
        fn kind(&self) -> &str {
            "test"
        }

        fn capabilities(&self) -> BTreeSet<session_protocol::SessionRuntimeCapability> {
            BTreeSet::new()
        }

        async fn start(&self, _request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn stop(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn attach(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            Ok(())
        }
        async fn check_alive(&self, _runtime_id: &str) -> Result<bool, SessionDomainError>{
            Ok(true)
        }

        async fn submit_turn(
            &self,
            _input: RuntimeTurnInput,
        ) -> Result<RuntimeEventReceiver, SessionDomainError> {
            let (tx, rx) = mpsc::channel(1);
            tx.send(RuntimeEvent::Completed {
                outcome: session_protocol::SessionTurnOutcome::Complete,
                usage: session_protocol::SessionUsage::default(),
            })
            .await
            .unwrap();
            Ok(rx)
        }

        async fn answer_interaction(
            &self,
            _input: RuntimeInteractionInput,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn cancel(
            &self,
            _runtime_id: &str,
            _turn_id: Option<&str>,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }
    }

    fn test_router() -> Router {
        let router = session_router(Arc::new(SessionHttpState::new(SessionApplication::new(
            Arc::new(CompletingRuntime),
            Arc::new(EmptyRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        ))));
        // No token table: `security_layer` still injects an implicit admin
        // `SecurityContext` (see `auth.rs`), which every handler below now
        // requires via `Extension<SecurityContext>`.
        super::super::auth::security_layer(router, None)
    }

    /// A router backed by [`TenantARepository`] (single record owned by
    /// `tenant-a`), guarded by a real [`TokenTable`] mapping `t-a`/`t-b` to
    /// distinct tenant `SecurityContext`s and `t-admin` to admin — used to
    /// exercise the actual cross-tenant 404 behavior over HTTP.
    fn test_router_with_tenant_a_record() -> Router {
        use super::super::auth::TokenTable;
        use std::collections::HashMap as StdHashMap;
        use xgovernor_core::SecurityContext;

        let router = session_router(Arc::new(SessionHttpState::new(SessionApplication::new(
            Arc::new(CompletingRuntime),
            Arc::new(TenantARepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        ))));
        let mut entries = StdHashMap::new();
        entries.insert(
            "t-a".to_string(),
            SecurityContext::tenant("tenant-a", "alice"),
        );
        entries.insert(
            "t-b".to_string(),
            SecurityContext::tenant("tenant-b", "bob"),
        );
        entries.insert("t-admin".to_string(), SecurityContext::admin("root"));
        super::super::auth::security_layer(router, Some(TokenTable::new(entries)))
    }

    #[test]
    fn every_terminal_event_has_an_explicit_sse_name() {
        let failed = SessionEvent::TurnFailed {
            runtime_id: "runtime-1".into(),
            turn_id: "turn-1".into(),
            error: session_protocol::SessionTurnFailure {
                code: "failed".into(),
                message: "failed".into(),
                retryable: false,
                details: serde_json::Value::Null,
            },
            usage: session_protocol::SessionUsage::default(),
        };
        assert_eq!(session_event_name(&failed), "turn_failed");
    }

    #[test]
    fn transport_error_uses_protocol_status_and_body() {
        let response = session_error(SessionWireError::NotFound {
            runtime_id: "missing".into(),
        });
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn receipt_type_is_the_protocol_contract() {
        let receipt = SessionSubmitReceipt {
            runtime_id: "runtime-1".into(),
            turn_id: "turn-1".into(),
            accepted_kind: session_protocol::SessionAcceptedInputKind::Turn,
        };
        assert_eq!(receipt.turn_id, "turn-1");
    }

    #[tokio::test]
    async fn http_receipt_links_to_sse_terminal_event() {
        let app = test_router();
        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v1/sessions/turns")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1","text":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let receipt: SessionSubmitReceipt = serde_json::from_slice(&body).unwrap();
        assert_eq!(receipt.turn_id, "turn-http");

        let response = app
            .oneshot(
                Request::get(format!(
                    "/api/v1/sessions/{}/turns/{}/events",
                    receipt.runtime_id, receipt.turn_id
                ))
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("event: turn_completed"));
        assert!(body.contains(r#""runtime_id":"runtime-1""#));
        assert!(body.contains(r#""turn_id":"turn-http""#));
    }

    #[tokio::test]
    async fn close_detach_and_heartbeat_are_reachable_over_http() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v1/sessions/heartbeat")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let heartbeat: session_protocol::SessionHeartbeatResponse =
            serde_json::from_slice(&body).unwrap();
        assert!(
            heartbeat.accepted,
            "no lease table wired: heartbeat is a no-op accept"
        );

        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v1/sessions/detach")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/close")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let control: session_protocol::SessionControlResponse =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(
            control.status,
            session_protocol::SessionLifecycleStatus::Closed
        );
    }

    #[tokio::test]
    async fn cancel_accepts_without_requiring_a_lease() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/cancel")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn fork_surfaces_unsupported_capability_when_the_adapter_cannot_export_state() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/fork")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"parent_runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        // CompletingRuntime does not override `export_state`, so the trait's
        // default `UnsupportedCapability` should surface as a 422 over HTTP.
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn no_bearer_token_is_rejected_when_a_token_table_is_configured() {
        let app = test_router_with_tenant_a_record();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/heartbeat")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_foreign_tenant_gets_not_found_not_forbidden_on_someone_elses_session() {
        // `docs/tenancy_design.md` §3: cross-tenant probing must not get a
        // distinguishing echo. tenant-b has a valid token but does not own
        // the tenant-a record `TenantARepository` always returns, so
        // `require_session`'s `ctx.owns(...)` check must fail closed as a
        // plain 404 — never a 403 that would confirm the record exists.
        let app = test_router_with_tenant_a_record();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/heartbeat")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer t-b")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn the_owning_tenant_can_reach_its_own_session() {
        let app = test_router_with_tenant_a_record();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/heartbeat")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer t-a")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_bypasses_tenant_ownership() {
        let app = test_router_with_tenant_a_record();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/heartbeat")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer t-admin")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// `get` always misses — models a fresh deployment with no prior
    /// sessions, so `open()` always takes the create path (not the
    /// re-attach-to-an-existing-record path).
    struct NoRecordRepository;

    #[async_trait]
    impl SessionRepository for NoRecordRepository {
        async fn get(
            &self,
            _runtime_id: &str,
        ) -> Result<Option<SessionRecord>, SessionDomainError> {
            Ok(None)
        }

        async fn save(&self, _record: SessionRecord) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn list_active(
            &self,
            _tenant_id: Option<&str>,
            _limit: usize,
        ) -> Result<SessionListPage, SessionDomainError> {
            Ok(SessionListPage {
                sessions: Vec::new(),
                total_active: 0,
            })
        }
    }

    struct HostOnlyEnvironment;

    #[async_trait]
    impl SessionEnvironmentNormalizer for HostOnlyEnvironment {
        async fn normalize(
            &self,
            ctx: &SecurityContext,
            request: &SessionOpenRequest,
        ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
            xgovernor_core::enforce_workspace_axiom(ctx, &request.workspace, false)?;
            Ok(NormalizedSessionEnvironment {
                workspace: xgovernor_core::WorkspaceFacts {
                    workspace_id: request.conversation_id.clone(),
                    root: "/tmp".into(),
                    access: xgovernor_core::WorkspaceAccess::ReadWrite,
                    revision: None,
                    metadata: serde_json::Value::Null,
                },
                isolation: xgovernor_core::IsolationFacts {
                    boundary: xgovernor_core::IsolationBoundary::Host,
                    workspace_access: xgovernor_core::WorkspaceAccess::ReadWrite,
                    network: xgovernor_core::NetworkIsolation::None,
                    metadata: serde_json::Value::Null,
                },
                sandbox_capabilities: Default::default(),
                llm: None,
                lease: None,
            })
        }
    }

    /// A router over [`HostOnlyEnvironment`] + [`NoRecordRepository`], guarded
    /// by the same three-token `TokenTable` shape as
    /// [`test_router_with_tenant_a_record`], for exercising `/sessions/open`
    /// admission (§5.4/§0) over real HTTP requests rather than calling
    /// `SessionApplication::open` directly.
    fn test_router_for_open() -> Router {
        use super::super::auth::TokenTable;
        use std::collections::HashMap as StdHashMap;
        use xgovernor_core::SecurityContext;

        let router = session_router(Arc::new(SessionHttpState::new(SessionApplication::new(
            Arc::new(CompletingRuntime),
            Arc::new(NoRecordRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(HostOnlyEnvironment),
            Arc::new(FixedTurnId),
        ))));
        let mut entries = StdHashMap::new();
        entries.insert(
            "t-a".to_string(),
            SecurityContext::tenant("tenant-a", "alice"),
        );
        entries.insert("t-admin".to_string(), SecurityContext::admin("root"));
        super::super::auth::security_layer(router, Some(TokenTable::new(entries)))
    }

    #[tokio::test]
    async fn tenant_open_with_a_local_path_workspace_is_rejected_over_http() {
        // §0/§5.4 over the real HTTP path, against a host-only provider (the
        // only one this deployment has today): a tenant token requesting a
        // local_path workspace must be rejected — not silently downgraded,
        // not allowed through.
        let app = test_router_for_open();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/open")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer t-a")
                    .body(Body::from(
                        r#"{"conversation_id":"c1","sender_id":"s1","workspace":{"kind":"local_path","path":"/etc"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn admin_open_with_a_local_path_workspace_still_succeeds_over_http() {
        let app = test_router_for_open();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/open")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer t-admin")
                    .body(Body::from(
                        r#"{"conversation_id":"c1","sender_id":"s1","workspace":{"kind":"local_path","path":"/etc"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn stream_turn_events_is_ownership_checked_before_touching_the_stream_table() {
        // No stream was ever registered for this runtime/turn pair, so
        // without the ownership guard this would already 404 via the "no
        // receiver in the table" branch — that would make the guard
        // untested. Assert on tenant-a (the owner) instead: a 404 here means
        // the guard let the owner through to the real "not found" branch,
        // not that it wrongly rejected them.
        let app = test_router_with_tenant_a_record();
        let response = app
            .oneshot(
                Request::get("/api/v1/sessions/runtime-1/turns/turn-missing/events")
                    .header("authorization", "Bearer t-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // A foreign tenant must be rejected by the ownership guard itself
        // (same 404, but for a different reason — verified indirectly here
        // since the wire response is deliberately indistinguishable; the
        // `require_session`-level distinction is covered by
        // `a_foreign_tenant_gets_not_found_not_forbidden_on_someone_elses_session`).
        let app = test_router_with_tenant_a_record();
        let response = app
            .oneshot(
                Request::get("/api/v1/sessions/runtime-1/turns/turn-missing/events")
                    .header("authorization", "Bearer t-b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// Like [`test_router`], but also returns the [`Arc<SessionHttpState>`]
    /// so a test can inspect `streams` directly (e.g.
    /// [`SessionHttpState::pending_stream_count`]) instead of only observing
    /// it indirectly through HTTP responses.
    fn test_router_with_state() -> (Router, Arc<SessionHttpState>) {
        let state = Arc::new(SessionHttpState::new(SessionApplication::new(
            Arc::new(CompletingRuntime),
            Arc::new(EmptyRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        )));
        let router = super::super::auth::security_layer(session_router(state.clone()), None);
        (router, state)
    }

    #[tokio::test]
    async fn close_session_sweeps_any_unclaimed_stream_for_that_runtime_id() {
        let (app, state) = test_router_with_state();

        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v1/sessions/turns")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1","text":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            state.pending_stream_count().await,
            1,
            "submit_turn must register a stream that nobody has attached to yet"
        );

        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/close")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            state.pending_stream_count().await,
            0,
            "close_session must sweep unclaimed streams for the closed runtime_id, \
             not wait out STREAM_ENTRY_TTL"
        );
    }

    #[tokio::test]
    async fn stream_registration_is_rejected_once_the_global_cap_is_reached() {
        let streams: Mutex<HashMap<String, HashMap<String, StreamEntry>>> =
            Mutex::new(HashMap::new());
        for i in 0..MAX_PENDING_STREAMS {
            let (_tx, rx) = mpsc::channel::<SessionEvent>(1);
            assert!(
                register_stream(&streams, "runtime-x", &format!("turn-{i}"), rx).await,
                "registration {i} must succeed while under the cap"
            );
        }

        let (_tx, rx) = mpsc::channel::<SessionEvent>(1);
        assert!(
            !register_stream(&streams, "runtime-x", "turn-overflow", rx).await,
            "the cap must reject registration once MAX_PENDING_STREAMS is reached"
        );
        assert_eq!(
            streams
                .lock()
                .await
                .values()
                .map(|t| t.len())
                .sum::<usize>(),
            MAX_PENDING_STREAMS,
            "the rejected registration must not have been inserted"
        );
    }

    #[tokio::test]
    async fn sweep_expired_streams_removes_entries_past_the_ttl_and_keeps_fresh_ones() {
        let streams: Mutex<HashMap<String, HashMap<String, StreamEntry>>> =
            Mutex::new(HashMap::new());
        {
            let mut guard = streams.lock().await;
            let (_tx, rx_old) = mpsc::channel::<SessionEvent>(1);
            guard.entry("runtime-1".to_string()).or_default().insert(
                "turn-old".to_string(),
                StreamEntry {
                    receiver: rx_old,
                    // Comfortably past STREAM_ENTRY_TTL. Subtracting from
                    // `Instant::now()` is safe here: on every platform this
                    // test runs on, the monotonic clock's origin is long
                    // before "now minus a few seconds" (e.g. process/boot
                    // time), so this cannot underflow.
                    registered_at: Instant::now() - STREAM_ENTRY_TTL - Duration::from_secs(1),
                },
            );
            let (_tx, rx_fresh) = mpsc::channel::<SessionEvent>(1);
            guard.entry("runtime-1".to_string()).or_default().insert(
                "turn-fresh".to_string(),
                StreamEntry {
                    receiver: rx_fresh,
                    registered_at: Instant::now(),
                },
            );
        }

        sweep_expired_streams(&streams).await;

        let guard = streams.lock().await;
        let remaining: Vec<&str> = guard
            .get("runtime-1")
            .map(|turns| turns.keys().map(String::as_str).collect())
            .unwrap_or_default();
        assert_eq!(
            remaining,
            vec!["turn-fresh"],
            "only the TTL-expired entry should have been swept"
        );
    }

    #[tokio::test]
    async fn sweep_expired_streams_prunes_the_runtime_id_entirely_once_it_has_no_turns_left() {
        let streams: Mutex<HashMap<String, HashMap<String, StreamEntry>>> =
            Mutex::new(HashMap::new());
        {
            let mut guard = streams.lock().await;
            let (_tx, rx) = mpsc::channel::<SessionEvent>(1);
            guard.entry("runtime-1".to_string()).or_default().insert(
                "turn-old".to_string(),
                StreamEntry {
                    receiver: rx,
                    registered_at: Instant::now() - STREAM_ENTRY_TTL - Duration::from_secs(1),
                },
            );
        }

        sweep_expired_streams(&streams).await;

        assert!(
            streams.lock().await.is_empty(),
            "a runtime_id with no remaining turns must not linger as an empty inner map"
        );
    }
}
