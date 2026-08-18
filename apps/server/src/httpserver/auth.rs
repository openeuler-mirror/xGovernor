use axum::{
    extract::State,
    http::{header::AUTHORIZATION, HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use xgovernor_core::{Role, SecurityContext, TenantQuota};

/// Fixed-window width for the per-tenant request-rate defense
/// (`TokenTable::check_rate_limit`) — a plain 60s wall-clock window, not a
/// sliding window or token bucket, since this only needs to bound worst-case
/// burst rate roughly (`max_requests_per_minute` is deliberately named per
/// *minute*), not smooth it precisely.
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);

/// One tenant's current fixed-window request count
/// (`TokenTable::check_rate_limit`).
struct RateWindow {
    window_start: Instant,
    count: u32,
}

/// Bearer token → [`SecurityContext`] map. This is the "token 表" of
/// `docs/tenancy_design.md` §7 step 1 — deliberately just a credential →
/// identity lookup, not the full per-tenant policy file (`tenants.toml`,
/// quotas/workspace-kind ceilings/etc., §4) which is a later, separate
/// landing-order step.
///
/// Also carries the per-tenant request-rate counters
/// (`XGovernor 传输层四道防线` item 2 — the lightweight companion to the
/// streams-table TTL/cap defenses in `httpserver::session`): a plain
/// `Mutex<HashMap<tenant_id, RateWindow>>`, never held across an `.await`,
/// rather than an external rate-limiting crate or a separate `tower::Layer`
/// — reusing this already-request-scoped, already-`Clone`+`Arc`'d state
/// instead of standing up a parallel subsystem.
#[derive(Clone)]
pub struct TokenTable {
    entries: Arc<HashMap<String, SecurityContext>>,
    rate_limiter: Arc<Mutex<HashMap<String, RateWindow>>>,
}

#[derive(Deserialize)]
struct TenantTokenEntry {
    token: String,
    tenant_id: String,
    #[serde(default = "default_principal")]
    principal: String,
    #[serde(default)]
    max_sessions: Option<u32>,
    #[serde(default)]
    max_requests_per_minute: Option<u32>,
}

fn default_principal() -> String {
    "tenant".to_string()
}

impl TokenTable {
    pub fn new(entries: HashMap<String, SecurityContext>) -> Self {
        Self {
            entries: Arc::new(entries),
            rate_limiter: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn resolve(&self, token: &str) -> Option<SecurityContext> {
        self.entries.get(token).cloned()
    }

    /// Fixed-window check for `tenant_id`: returns `true` (and records the
    /// call) if it has made fewer than `max_per_minute` requests within the
    /// current 60s window, `false` otherwise. Never called for admin
    /// contexts — `resolve_security_context` skips this entirely when
    /// `ctx.is_admin()`, mirroring every other "admin is 百无禁忌" exemption
    /// in this codebase (`SecurityContext::owns`, `ADMIN_OWNER_REF`).
    fn check_rate_limit(&self, tenant_id: &str, max_per_minute: u32) -> bool {
        let mut windows = self
            .rate_limiter
            .lock()
            .expect("rate limiter mutex poisoned");
        let now = Instant::now();
        let window = windows.entry(tenant_id.to_string()).or_insert(RateWindow {
            window_start: now,
            count: 0,
        });
        if now.duration_since(window.window_start) >= RATE_LIMIT_WINDOW {
            window.window_start = now;
            window.count = 0;
        }
        if window.count >= max_per_minute {
            false
        } else {
            window.count += 1;
            true
        }
    }

    /// Build a table from environment variables:
    ///
    /// - `XGOVERNOR_BEARER_TOKEN` — legacy single admin token (today's only
    ///   credential mechanism), mapped to `SecurityContext::admin(..)`.
    ///   Preserved unchanged so existing single-operator deployments keep
    ///   working with no config migration.
    /// - `XGOVERNOR_TENANT_TOKENS_JSON` — a JSON array of
    ///   `{"token": "...", "tenant_id": "...", "principal": "...",
    ///   "max_sessions": ..., "max_requests_per_minute": ...}` objects
    ///   (`principal`/`max_sessions`/`max_requests_per_minute` all optional —
    ///   an omitted `max_sessions`/`max_requests_per_minute` means
    ///   unlimited), each mapped to
    ///   `SecurityContext::tenant(..).with_quota(..)`. JSON via
    ///   `serde_json` rather than a `tenants.toml` file: this step is scoped
    ///   to auth (+ the session-count and request-rate quota tiers, §7 step
    ///   4) only, not the full policy file of `docs/tenancy_design.md` §4.
    ///
    /// Returns `None` when neither variable is set (or both are empty) —
    /// callers should then leave auth off entirely via [`security_layer`],
    /// matching today's "wide open" dev-mode behavior.
    pub fn from_env() -> Option<Self> {
        let mut entries = HashMap::new();

        if let Ok(admin_token) = std::env::var("XGOVERNOR_BEARER_TOKEN") {
            if !admin_token.is_empty() {
                entries.insert(
                    admin_token,
                    SecurityContext::admin("env:XGOVERNOR_BEARER_TOKEN"),
                );
            }
        }

        if let Ok(tenant_json) = std::env::var("XGOVERNOR_TENANT_TOKENS_JSON") {
            if !tenant_json.trim().is_empty() {
                match serde_json::from_str::<Vec<TenantTokenEntry>>(&tenant_json) {
                    Ok(parsed) => {
                        for entry in parsed {
                            let quota = TenantQuota {
                                max_sessions: entry.max_sessions,
                                max_requests_per_minute: entry.max_requests_per_minute,
                            };
                            entries.insert(
                                entry.token,
                                SecurityContext::tenant(entry.tenant_id, entry.principal)
                                    .with_quota(quota),
                            );
                        }
                    }
                    Err(error) => {
                        eprintln!(
                            "warning: XGOVERNOR_TENANT_TOKENS_JSON failed to parse ({error}); \
                             tenant tokens from it were not loaded"
                        );
                    }
                }
            }
        }

        if entries.is_empty() {
            None
        } else {
            Some(Self::new(entries))
        }
    }
}

/// Install the auth middleware. With `Some(table)`, every request must carry
/// a bearer token present in the table (401 otherwise); the resolved
/// `SecurityContext` is injected as a request extension. With `None` — no
/// token table configured — every request is treated as an implicit
/// `SecurityContext::admin(..)` instead of being auth-free in a way handlers
/// have to special-case: this keeps exactly one code path
/// (`Extension<SecurityContext>`) in every handler regardless of whether
/// real auth is configured, while preserving today's "everything reachable"
/// dev-mode behavior byte-for-byte.
pub fn security_layer<S>(router: axum::Router<S>, table: Option<TokenTable>) -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router.route_layer(middleware::from_fn_with_state(
        table,
        resolve_security_context,
    ))
}

/// Reject any request whose resolved [`SecurityContext`] role does not equal
/// `required`, with 403 (not 401 — the caller *did* authenticate, they're
/// just not welcome on this particular listener). `docs/tenancy_design.md`
/// §3.1: the dual-listener split (admin-only loopback surface, tenant-only
/// public surface, `apps/server/src/main.rs`) enforces "admin credentials
/// must never be reachable off loopback" at the router level too, not only
/// the bind-address level — a tenant token must never open the admin
/// surface, and an admin token (or the implicit-admin no-token-table
/// fallback) must never be usable on the public tenant surface, even if some
/// future change wires the two listeners to the wrong router by mistake.
///
/// MUST be composed *inside* [`security_layer`] — i.e. call this on the bare
/// router first, then wrap the result with `security_layer` — since this
/// middleware reads the `Extension<SecurityContext>` that
/// `resolve_security_context` inserts and has nothing to check if it runs
/// first. (axum layers run outside-in on the request path; the layer applied
/// *last* is outermost and runs first — see the two callers of this function
/// in `router.rs` and the ordering test in this module.)
pub fn require_role<S>(router: axum::Router<S>, required: Role) -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router.route_layer(middleware::from_fn(
        move |request: Request<axum::body::Body>, next: Next| async move {
            match request.extensions().get::<SecurityContext>() {
                Some(ctx) if ctx.role == required => next.run(request).await,
                _ => (
                    StatusCode::FORBIDDEN,
                    "this listener does not accept this credential's role",
                )
                    .into_response(),
            }
        },
    ))
}

async fn resolve_security_context(
    State(table): State<Option<TokenTable>>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let ctx = match &table {
        None => SecurityContext::admin("unauthenticated-dev"),
        Some(table) => {
            let Some(token) = parse_bearer_token(request.headers()) else {
                return (StatusCode::UNAUTHORIZED, "missing or invalid bearer token")
                    .into_response();
            };
            let Some(ctx) = table.resolve(token) else {
                return (StatusCode::UNAUTHORIZED, "invalid bearer token").into_response();
            };
            ctx
        }
    };

    // Per-tenant request-rate defense (transport-layer item 2, alongside the
    // streams-table TTL/cap in `httpserver::session`). Admin is always
    // exempt — `is_admin()` short-circuits before even looking at the quota
    // or touching the rate limiter, same exemption shape as
    // `SecurityContext::owns`/`ADMIN_OWNER_REF` elsewhere in this codebase.
    if !ctx.is_admin() {
        if let (Some(max_per_minute), Some(tenant_id), Some(table)) =
            (ctx.quota.max_requests_per_minute, ctx.tenant_id(), &table)
        {
            if !table.check_rate_limit(tenant_id, max_per_minute) {
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    "tenant request rate limit exceeded",
                )
                    .into_response();
            }
        }
    }

    request.extensions_mut().insert(ctx);
    next.run(request).await
}

fn parse_bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let mut parts = value.split_whitespace();
    (parts.next()?.eq_ignore_ascii_case("bearer")
        && parts.next().is_some()
        && parts.next().is_none())
    .then(|| value.split_whitespace().nth(1).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use xgovernor_core::Role;

    #[test]
    fn bearer_parser_rejects_extra_parts() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, "Bearer token extra".parse().unwrap());
        assert!(parse_bearer_token(&headers).is_none());
    }

    #[test]
    fn from_env_is_none_when_nothing_is_set() {
        // SAFETY (test-only, single-threaded env mutation): clearing these
        // two vars is scoped to this test's assertion window.
        std::env::remove_var("XGOVERNOR_BEARER_TOKEN");
        std::env::remove_var("XGOVERNOR_TENANT_TOKENS_JSON");
        assert!(TokenTable::from_env().is_none());
    }

    #[test]
    fn legacy_bearer_token_resolves_to_admin() {
        let mut entries = HashMap::new();
        entries.insert("root-token".to_string(), SecurityContext::admin("root"));
        let table = TokenTable::new(entries);
        let ctx = table.resolve("root-token").expect("token must resolve");
        assert_eq!(ctx.role, Role::Admin);
        assert!(table.resolve("wrong-token").is_none());
    }

    /// Locks in the composition order documented on [`require_role`]: it must
    /// be applied *inside* `security_layer` for the extension it reads to
    /// exist. If a future edit swaps the order, this test fails loudly
    /// (every request would 403, since `require_role` would find no
    /// `SecurityContext` extension yet) instead of the far worse silent
    /// failure mode of the check being skipped entirely.
    fn admin_only_router() -> axum::Router {
        let mut entries = HashMap::new();
        entries.insert("admin-token".to_string(), SecurityContext::admin("root"));
        entries.insert(
            "tenant-token".to_string(),
            SecurityContext::tenant("tenant-a", "alice"),
        );
        let base = axum::Router::new().route("/probe", axum::routing::get(|| async { "ok" }));
        let gated = require_role(base, Role::Admin);
        security_layer(gated, Some(TokenTable::new(entries)))
    }

    #[tokio::test]
    async fn require_role_lets_the_matching_role_through() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let response = admin_only_router()
            .oneshot(
                Request::get("/probe")
                    .header("authorization", "Bearer admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn require_role_rejects_a_mismatched_role_with_403() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let response = admin_only_router()
            .oneshot(
                Request::get("/probe")
                    .header("authorization", "Bearer tenant-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn tenant_token_json_parses_into_tenant_contexts() {
        let json = r#"[
            {"token": "t-a", "tenant_id": "tenant-a", "principal": "alice"},
            {"token": "t-b", "tenant_id": "tenant-b"}
        ]"#;
        let parsed: Vec<TenantTokenEntry> = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].principal, "alice");
        assert_eq!(
            parsed[1].principal, "tenant",
            "principal defaults when omitted"
        );
    }

    #[test]
    fn tenant_token_json_quota_field_defaults_to_unlimited_and_can_be_set() {
        let json = r#"[
            {"token": "t-a", "tenant_id": "tenant-a", "max_sessions": 5},
            {"token": "t-b", "tenant_id": "tenant-b"}
        ]"#;
        let parsed: Vec<TenantTokenEntry> = serde_json::from_str(json).unwrap();
        assert_eq!(parsed[0].max_sessions, Some(5));
        assert_eq!(
            parsed[1].max_sessions, None,
            "omitted quota field means unlimited"
        );
    }

    #[test]
    fn tenant_token_json_rate_field_defaults_to_unlimited_and_can_be_set() {
        let json = r#"[
            {"token": "t-a", "tenant_id": "tenant-a", "max_requests_per_minute": 30},
            {"token": "t-b", "tenant_id": "tenant-b"}
        ]"#;
        let parsed: Vec<TenantTokenEntry> = serde_json::from_str(json).unwrap();
        assert_eq!(parsed[0].max_requests_per_minute, Some(30));
        assert_eq!(
            parsed[1].max_requests_per_minute, None,
            "omitted rate field means unlimited"
        );
    }

    fn rate_limited_router() -> axum::Router {
        let mut entries = HashMap::new();
        entries.insert("admin-token".to_string(), SecurityContext::admin("root"));
        entries.insert(
            "limited-tenant-token".to_string(),
            SecurityContext::tenant("tenant-a", "alice").with_quota(TenantQuota {
                max_sessions: None,
                max_requests_per_minute: Some(2),
            }),
        );
        entries.insert(
            "unlimited-tenant-token".to_string(),
            SecurityContext::tenant("tenant-b", "bob"),
        );
        let base = axum::Router::new().route("/probe", axum::routing::get(|| async { "ok" }));
        security_layer(base, Some(TokenTable::new(entries)))
    }

    async fn probe_with(router: &axum::Router, token: &str) -> StatusCode {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        router
            .clone()
            .oneshot(
                Request::get("/probe")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn rate_limited_tenant_is_rejected_after_exceeding_its_per_minute_ceiling() {
        let router = rate_limited_router();
        assert_eq!(
            probe_with(&router, "limited-tenant-token").await,
            StatusCode::OK
        );
        assert_eq!(
            probe_with(&router, "limited-tenant-token").await,
            StatusCode::OK
        );
        assert_eq!(
            probe_with(&router, "limited-tenant-token").await,
            StatusCode::TOO_MANY_REQUESTS,
            "third request within the same 60s window must be rejected \
             (max_requests_per_minute: 2)"
        );
    }

    #[tokio::test]
    async fn rate_limit_is_scoped_per_tenant_not_global() {
        let router = rate_limited_router();
        assert_eq!(
            probe_with(&router, "limited-tenant-token").await,
            StatusCode::OK
        );
        assert_eq!(
            probe_with(&router, "limited-tenant-token").await,
            StatusCode::OK
        );
        // tenant-a's ceiling is now exhausted, but tenant-b (a different
        // tenant_id) must be entirely unaffected -- the limiter is keyed by
        // tenant_id, not shared across the whole table.
        assert_eq!(
            probe_with(&router, "unlimited-tenant-token").await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn admin_is_exempt_from_rate_limiting_even_past_a_tenants_ceiling() {
        let router = rate_limited_router();
        for _ in 0..5 {
            assert_eq!(probe_with(&router, "admin-token").await, StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn tenant_without_a_configured_rate_limit_is_unlimited() {
        let router = rate_limited_router();
        for _ in 0..5 {
            assert_eq!(
                probe_with(&router, "unlimited-tenant-token").await,
                StatusCode::OK
            );
        }
    }
}
