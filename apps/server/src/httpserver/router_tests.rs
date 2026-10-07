use super::*;
use crate::httpserver::session::SessionHttpState;
use agent_runtime_protocol::{
    AgentRuntime, RuntimeCancelRequest, RuntimeCapability, RuntimeError, RuntimeEventReceiver,
    RuntimeExecutionContext, RuntimeInteractionRequest as RuntimeInteractionInput,
    RuntimeStartRequest, RuntimeTurnRequest as RuntimeTurnInput,
};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::collections::BTreeSet;
use std::sync::Arc;
use tower::ServiceExt;
use xgovernor_core::application::{SessionRepository, TurnIdGenerator};
use xgovernor_core::{
    Clock, NormalizedSessionEnvironment, RuntimeIdGenerator, SecurityContext, SessionApplication,
    SessionDomainError, SessionEnvironmentNormalizer, SessionListPage, SessionRecord,
};

#[tokio::test(start_paused = true)]
async fn long_operations_outlive_control_timeout() {
    async fn long_work() -> &'static str {
        tokio::time::sleep(Duration::from_secs(31)).await;
        "done"
    }
    let router = apply_transport_defenses(
        Router::new()
            .route("/api/v1/sessions/exec", axum::routing::post(long_work))
            .route(
                "/api/v1/sessions/checkpoint",
                axum::routing::post(long_work),
            )
            .route("/api/v1/sessions/heartbeat", axum::routing::post(long_work)),
    );
    for (path, body, status) in [
        ("exec", r#"{"timeout_ms":900000}"#, StatusCode::OK),
        ("checkpoint", "{}", StatusCode::OK),
        ("heartbeat", "{}", StatusCode::REQUEST_TIMEOUT),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::post(format!("/api/v1/sessions/{path}"))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status, "{path}");
    }
}

struct EmptyRepository;

#[async_trait]
impl SessionRepository for EmptyRepository {
    async fn get(&self, _runtime_id: &str) -> Result<Option<SessionRecord>, SessionDomainError> {
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

struct UnusedRuntime;

#[async_trait]
impl AgentRuntime for UnusedRuntime {
    fn runtime_kind(&self) -> &str {
        "unused"
    }

    fn capabilities(&self) -> BTreeSet<RuntimeCapability> {
        BTreeSet::new()
    }

    async fn start(
        &self,
        _request: RuntimeStartRequest,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        unreachable!("router tests never drive a real turn")
    }

    async fn stop(&self, _runtime_id: &str) -> Result<(), RuntimeError> {
        unreachable!("router tests never drive a real turn")
    }

    async fn attach(
        &self,
        _runtime_id: &str,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        unreachable!("router tests never drive a real turn")
    }
    async fn check_alive(&self, _runtime_id: &str) -> Result<bool, RuntimeError> {
        Ok(true)
    }

    async fn submit_turn(
        &self,
        _input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, RuntimeError> {
        unreachable!("router tests never drive a real turn")
    }

    async fn answer_interaction(
        &self,
        _input: RuntimeInteractionInput,
    ) -> Result<(), RuntimeError> {
        unreachable!("router tests never drive a real turn")
    }

    async fn cancel(&self, _request: RuntimeCancelRequest) -> Result<(), RuntimeError> {
        unreachable!("router tests never drive a real turn")
    }
}

struct UnusedIds;

impl TurnIdGenerator for UnusedIds {
    fn next_turn_id(&self) -> String {
        unreachable!("router tests never drive a real turn")
    }
}

impl RuntimeIdGenerator for UnusedIds {
    fn next_runtime_id(&self) -> String {
        unreachable!("router tests never drive a real turn")
    }
}

impl Clock for UnusedIds {
    fn now_ms(&self) -> u64 {
        0
    }
}

struct UnusedEnvironment;

#[async_trait]
impl SessionEnvironmentNormalizer for UnusedEnvironment {
    async fn normalize(
        &self,
        _ctx: &SecurityContext,
        _request: &session_protocol::SessionOpenRequest,
    ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
        unreachable!("router tests never drive a real turn")
    }
}

fn test_application() -> SessionApplication {
    SessionApplication::new(
        Arc::new(UnusedRuntime),
        std::collections::HashMap::new(),
        Arc::new(EmptyRepository),
        Arc::new(UnusedIds),
        Arc::new(UnusedIds),
        Arc::new(UnusedEnvironment),
        Arc::new(UnusedIds),
    )
}

fn test_state() -> SessionHttpState {
    SessionHttpState::new(test_application())
}

#[tokio::test]
async fn health_is_reachable_without_a_token_table() {
    let router = create_router(test_state(), None, None, None);
    let response = router
        .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn a_token_table_gates_every_route_including_health() {
    // Auth is applied to the whole composed router, not just the session
    // routes — a configured token table must also cover /health, so a
    // caller without credentials can't even probe liveness.
    use super::super::auth::TokenTable;
    use std::collections::HashMap;

    let mut entries = HashMap::new();
    entries.insert("root-token".to_string(), SecurityContext::admin("root"));
    let router = create_router(test_state(), Some(TokenTable::new(entries)), None, None);

    let response = router
        .clone()
        .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = router
        .oneshot(
            Request::get("/api/v1/health")
                .header("authorization", "Bearer root-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn role_gate_none_preserves_the_legacy_no_restriction_behavior() {
    // A tenant token must still reach every route when role_gate is None
    // — this is the single-listener backward-compat path
    // (XGOVERNOR_TENANT_BIND_ADDR unset), unchanged by adding the
    // parameter.
    use super::super::auth::TokenTable;
    use std::collections::HashMap;

    let mut entries = HashMap::new();
    entries.insert(
        "tenant-token".to_string(),
        SecurityContext::tenant("tenant-a", "alice"),
    );
    let router = create_router(test_state(), Some(TokenTable::new(entries)), None, None);

    let response = router
        .oneshot(
            Request::get("/api/v1/health")
                .header("authorization", "Bearer tenant-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn role_gate_admin_rejects_a_tenant_token_with_403() {
    use super::super::auth::TokenTable;
    use std::collections::HashMap;
    use xgovernor_core::Role;

    let mut entries = HashMap::new();
    entries.insert(
        "tenant-token".to_string(),
        SecurityContext::tenant("tenant-a", "alice"),
    );
    entries.insert("admin-token".to_string(), SecurityContext::admin("root"));
    let router = create_router(
        test_state(),
        Some(TokenTable::new(entries)),
        Some(Role::Admin),
        None,
    );

    let response = router
        .clone()
        .oneshot(
            Request::get("/api/v1/health")
                .header("authorization", "Bearer tenant-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = router
        .oneshot(
            Request::get("/api/v1/health")
                .header("authorization", "Bearer admin-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn role_gate_tenant_rejects_the_implicit_admin_fallback_with_403() {
    // The most important case: with no token table at all, every request
    // resolves to implicit admin (today's dev-mode behavior). A tenant
    // listener must still reject it — "no auth configured" must never
    // silently become "admin reachable on the public listener".
    use xgovernor_core::Role;

    let router = create_router(test_state(), None, Some(Role::Tenant), None);
    let response = router
        .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn oversized_request_bodies_are_rejected_before_reaching_the_handler() {
    // A body over MAX_REQUEST_BODY_BYTES must be rejected by the
    // RequestBodyLimitLayer (413) before session::open_session's Json
    // extractor -- and therefore before SessionApplication::open -- ever
    // sees it. Deliberately targets a real session route (not a
    // throwaway one) so this exercises the actual production
    // composition in create_router, not just the guard_with helper.
    let big_body = "x".repeat(MAX_REQUEST_BODY_BYTES + 1);
    let router = create_router(test_state(), None, None, None);
    let response = router
        .oneshot(
            Request::post("/api/v1/sessions/open")
                .header("content-type", "application/json")
                .body(Body::from(big_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test(start_paused = true)]
async fn slow_requests_are_cut_off_by_the_timeout_layer() {
    // tokio's paused clock lets this assert the real REQUEST_TIMEOUT
    // constant (30s) without the test actually taking 30s of wall-clock
    // time: with nothing else runnable, tokio auto-advances virtual time
    // to the next pending timer, so both the handler's sleep and the
    // TimeoutLayer's internal timer resolve near-instantly here.
    async fn slow_handler() {
        tokio::time::sleep(REQUEST_TIMEOUT + Duration::from_secs(1)).await;
    }

    let router = apply_transport_defenses(Router::new().route("/slow", get(slow_handler)));
    let response = router
        .oneshot(Request::get("/slow").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
}

#[tokio::test]
async fn sse_route_is_still_reachable_through_create_router_after_the_split() {
    // Structural regression test for the non_sse_session_router /
    // sse_session_router split: the SSE route must still be wired up
    // (and still fed by the *same* Arc<SessionHttpState>, hence still
    // ownership/lookup-checked) when reached through create_router, not
    // just through session_router directly. No stream was ever
    // registered, so this hits the ordinary "session not found" branch
    // -- proving the route exists and reaches the real handler, not that
    // it 404s for some routing/composition mistake.
    let router = create_router(test_state(), None, None, None);
    let response = router
        .oneshot(
            Request::get("/api/v1/sessions/runtime-x/turns/turn-y/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
