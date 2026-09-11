use super::admin_tenants::{admin_tenants_router, TenantAdminState};
use super::auth::{require_role, security_layer, TokenTable};
use super::session::{session_router, SessionHttpState};
use axum::{http::StatusCode, routing::get, Json, Router};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tower::{limit::ConcurrencyLimit, Layer};
use tower_http::limit::RequestBodyLimitLayer;
use xgovernor_core::Role;

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

/// Request body size ceiling for every non-SSE route (item 3 of the
/// transport-layer defenses, alongside the streams-table TTL/cap and
/// per-tenant rate limit). `/health` and every `session_router` route today
/// includes base64 file transfers: 2 MiB accommodates a 1 MiB binary chunk.
const MAX_REQUEST_BODY_BYTES: usize = 2 * 1024 * 1024;

/// Per-request timeout for every non-SSE route. The SSE event-stream route
/// is composed *outside* this layer entirely — see
/// [`session::sse_session_router`]'s doc comment for why.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// In-flight request ceiling for every non-SSE route — bounds concurrent
/// open/submit_turn/close/etc. calls, not concurrent SSE connections (which
/// never pass through this layer).
const MAX_CONCURRENT_REQUESTS: usize = 512;

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

async fn operation_timeout(
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let budget = match request.uri().path() {
        "/api/v1/sessions/exec" => {
            let (parts, body) = request.into_parts();
            let bytes = match axum::body::to_bytes(body, MAX_REQUEST_BODY_BYTES).await {
                Ok(bytes) => bytes,
                Err(_) => {
                    return (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "exec request body exceeds 2 MiB",
                    )
                        .into_response()
                }
            };
            let requested = serde_json::from_slice::<serde_json::Value>(&bytes)
                .ok()
                .and_then(|v| v.get("timeout_ms").and_then(serde_json::Value::as_u64))
                .unwrap_or(30_000);
            request = axum::http::Request::from_parts(parts, axum::body::Body::from(bytes));
            Duration::from_millis(requested.min(3_600_000).saturating_add(30_000))
        }
        "/api/v1/sessions/open"
        | "/api/v1/sessions/load"
        | "/api/v1/sessions/checkpoint"
        | "/api/v1/sessions/fork" => Duration::from_secs(900),
        _ => REQUEST_TIMEOUT,
    };
    match tokio::time::timeout(budget, next.run(request)).await {
        Ok(response) => response,
        Err(_) => (
            StatusCode::REQUEST_TIMEOUT,
            format!("request did not complete within {}s", budget.as_secs()),
        )
            .into_response(),
    }
}

fn apply_transport_defenses(router: Router) -> Router {
    router
        .layer(SharedConcurrencyLimitLayer::new(MAX_CONCURRENT_REQUESTS))
        .layer(RequestBodyLimitLayer::new(MAX_REQUEST_BODY_BYTES))
        .layer(axum::middleware::from_fn(operation_timeout))
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
