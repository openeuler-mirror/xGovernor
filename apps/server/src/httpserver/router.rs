use super::admin_tenants::{admin_tenants_router, TenantAdminState};
use super::auth::{require_role, security_layer, TokenTable};
use super::session::{session_router, SessionHttpState};
use axum::{
    error_handling::HandleErrorLayer, http::StatusCode, routing::get, BoxError, Json, Router,
};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tower::{limit::ConcurrencyLimit, timeout::TimeoutLayer, Layer, ServiceBuilder};
use tower_http::limit::RequestBodyLimitLayer;
use xgovernor_core::Role;

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

/// Request body size ceiling for every non-SSE route (item 3 of the
/// transport-layer defenses, alongside the streams-table TTL/cap and
/// per-tenant rate limit). `/health` and every `session_router` route today
/// only ever carries small JSON payloads (session open specs, turn text) —
/// 1 MiB is deliberately generous headroom against an oversized/malicious
/// body, not a tight fit against real traffic.
const MAX_REQUEST_BODY_BYTES: usize = 1024 * 1024;

/// Per-request timeout for every non-SSE route. The SSE event-stream route
/// is composed *outside* this layer entirely — see
/// [`session::sse_session_router`]'s doc comment for why.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// In-flight request ceiling for every non-SSE route — bounds concurrent
/// open/submit_turn/close/etc. calls, not concurrent SSE connections (which
/// never pass through this layer).
const MAX_CONCURRENT_REQUESTS: usize = 512;

/// Converts whatever [`TimeoutLayer`] (the only layer in
/// [`apply_transport_defenses`]'s stack that can actually fail) produces into
/// an HTTP response, satisfying `Router::layer`'s requirement that the
/// composed layer's error type be `Into<Infallible>`. Must be the outermost
/// layer in the [`ServiceBuilder`] stack (added first) so it sees errors
/// bubbling up from every layer beneath it — this is the same ordering axum
/// itself documents for pairing `TimeoutLayer` with `HandleErrorLayer`.
async fn handle_transport_layer_error(error: BoxError) -> (StatusCode, String) {
    if error.is::<tower::timeout::error::Elapsed>() {
        (
            StatusCode::REQUEST_TIMEOUT,
            format!(
                "request did not complete within {}s",
                REQUEST_TIMEOUT.as_secs()
            ),
        )
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unhandled transport-layer error: {error}"),
        )
    }
}

/// [`tower::limit::ConcurrencyLimitLayer`] constructs a brand-new
/// `Arc<Semaphore>` *every time* its `Layer::layer` method runs — and axum's
/// `Router::layer` invokes that method far more than once per router (each
/// clone triggers additional invocations; confirmed empirically: 10 calls
/// for a single route across 6 concurrent clones in a standalone repro).
/// Using `ConcurrencyLimitLayer` directly therefore silently hands out an
/// independent semaphore per invocation, so the ceiling is never actually
/// shared and never actually enforced. This wrapper builds the `Semaphore`
/// exactly once (in [`SharedConcurrencyLimitLayer::new`]) and hands every
/// subsequent `.layer()` invocation a clone of the same `Arc`, via
/// `ConcurrencyLimit::with_semaphore`, so the ceiling is genuinely global
/// across every `Router` clone.
#[derive(Clone)]
struct SharedConcurrencyLimitLayer {
    semaphore: Arc<Semaphore>,
}

impl SharedConcurrencyLimitLayer {
    fn new(max: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max)),
        }
    }
}

impl<S> Layer<S> for SharedConcurrencyLimitLayer {
    type Service = ConcurrencyLimit<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConcurrencyLimit::with_semaphore(inner, self.semaphore.clone())
    }
}

/// Wrap `router` in the three request-level transport defenses, with the
/// concrete ceiling for each passed in explicitly so tests can exercise the
/// composition with small, fast values instead of the real
/// [`MAX_REQUEST_BODY_BYTES`]/[`REQUEST_TIMEOUT`]/[`MAX_CONCURRENT_REQUESTS`]
/// constants. [`apply_transport_defenses`] is the production entry point.
fn guard_with(router: Router, body_limit: usize, timeout: Duration, concurrency: usize) -> Router {
    // SharedConcurrencyLimitLayer and RequestBodyLimitLayer are applied as
    // their own separate `.layer()` calls (rather than folded into the
    // ServiceBuilder stack below) because neither ever produces an error —
    // each individually satisfies `Router::layer`'s `Error: Into<Infallible>`
    // bound on its own. Only `TimeoutLayer` can fail, so it's the only layer
    // paired with `HandleErrorLayer` (which must wrap it directly for the
    // combined pair to itself satisfy that same bound).
    router
        .layer(SharedConcurrencyLimitLayer::new(concurrency))
        .layer(RequestBodyLimitLayer::new(body_limit))
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_transport_layer_error))
                .layer(TimeoutLayer::new(timeout)),
        )
}

fn apply_transport_defenses(router: Router) -> Router {
    guard_with(
        router,
        MAX_REQUEST_BODY_BYTES,
        REQUEST_TIMEOUT,
        MAX_CONCURRENT_REQUESTS,
    )
}

/// Compose the daemon HTTP surface. Feature routers are merged here; handlers
/// do not know about one another or about authentication middleware.
///
/// `role_gate`: `None` preserves the historical single-listener behavior —
/// no role restriction, any resolved `SecurityContext` (admin or tenant) may
/// reach every route, exactly as before this parameter existed. `Some(role)`
/// restricts the *entire* composed router (including `/health`, same
/// rationale as gating it by `security_layer` at all — see
/// `a_token_table_gates_every_route_including_health` below) to that role
/// only; `apps/server/src/main.rs`'s dual-listener setup
/// (`docs/tenancy_design.md` §3.1) passes `Some(Role::Admin)` for the
/// loopback-only admin listener and `Some(Role::Tenant)` for the public
/// tenant listener, so a valid credential of the wrong role gets 403 even if
/// it reaches the wrong port. `require_role` must be composed *inside*
/// `security_layer` (applied to `router` before `security_layer` wraps it) —
/// see that function's doc comment for why the order matters.
pub fn create_router(
    session_state: SessionHttpState,
    token_table: Option<TokenTable>,
    role_gate: Option<Role>,
    tenant_admin: Option<TenantAdminState>,
) -> Router {
    let state = Arc::new(session_state);
    let mut router = Router::new()
        .route("/api/v1/health", get(health))
        .merge(session_router(state.clone()));
    if let Some(tenant_admin_state) = tenant_admin {
        router = router.merge(admin_tenants_router(tenant_admin_state));
    }
    let router = apply_transport_defenses(router);
    let router = match role_gate {
        Some(role) => require_role(router, role),
        None => router,
    };
    security_layer(router, token_table)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::httpserver::session::SessionHttpState;
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use tower::ServiceExt;
    use xgovernor_core::application::{SessionRepository, TurnIdGenerator};
    use xgovernor_core::{
        Clock, NormalizedSessionEnvironment, RuntimeAdapter, RuntimeEventReceiver,
        RuntimeIdGenerator, RuntimeInteractionInput, RuntimeStartRequest, RuntimeTurnInput,
        SecurityContext, SessionApplication, SessionDomainError, SessionEnvironmentNormalizer,
        SessionListPage, SessionRecord,
    };

    struct EmptyRepository;

    #[async_trait]
    impl SessionRepository for EmptyRepository {
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

    struct UnusedRuntime;

    #[async_trait]
    impl RuntimeAdapter for UnusedRuntime {
        fn kind(&self) -> &str {
            "unused"
        }

        fn capabilities(&self) -> BTreeSet<session_protocol::SessionRuntimeCapability> {
            BTreeSet::new()
        }

        async fn start(&self, _request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
            unreachable!("router tests never drive a real turn")
        }

        async fn stop(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            unreachable!("router tests never drive a real turn")
        }

        async fn attach(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            unreachable!("router tests never drive a real turn")
        }
        async fn check_alive(&self, _runtime_id: &str) -> Result<bool, SessionDomainError> {
            Ok(true)
        }

        async fn submit_turn(
            &self,
            _input: RuntimeTurnInput,
        ) -> Result<RuntimeEventReceiver, SessionDomainError> {
            unreachable!("router tests never drive a real turn")
        }

        async fn answer_interaction(
            &self,
            _input: RuntimeInteractionInput,
        ) -> Result<(), SessionDomainError> {
            unreachable!("router tests never drive a real turn")
        }

        async fn cancel(
            &self,
            _runtime_id: &str,
            _turn_id: Option<&str>,
        ) -> Result<(), SessionDomainError> {
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
    async fn concurrency_limit_serializes_requests_beyond_the_ceiling() {
        // guard_with (not apply_transport_defenses) so this can use a small,
        // fast-to-exhaust ceiling instead of the real 512 -- spinning up 512+
        // concurrent tasks just to prove the same mechanism would be slow and
        // add nothing a smaller ceiling doesn't already demonstrate.
        use std::sync::atomic::{AtomicUsize, Ordering};

        static ENTERED: AtomicUsize = AtomicUsize::new(0);
        static MAX_CONCURRENT_SEEN: AtomicUsize = AtomicUsize::new(0);

        async fn handler() -> &'static str {
            let now = ENTERED.fetch_add(1, Ordering::SeqCst) + 1;
            MAX_CONCURRENT_SEEN.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            ENTERED.fetch_sub(1, Ordering::SeqCst);
            "ok"
        }

        let router = guard_with(
            Router::new().route("/probe", get(handler)),
            MAX_REQUEST_BODY_BYTES,
            Duration::from_secs(5),
            2,
        );

        let mut handles = Vec::new();
        for _ in 0..6 {
            let router = router.clone();
            handles.push(tokio::spawn(async move {
                router
                    .oneshot(Request::get("/probe").body(Body::empty()).unwrap())
                    .await
                    .unwrap()
                    .status()
            }));
        }
        for handle in handles {
            assert_eq!(handle.await.unwrap(), StatusCode::OK);
        }
        assert!(
            MAX_CONCURRENT_SEEN.load(Ordering::SeqCst) <= 2,
            "concurrency ceiling of 2 must never be exceeded, saw {}",
            MAX_CONCURRENT_SEEN.load(Ordering::SeqCst)
        );
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
}
