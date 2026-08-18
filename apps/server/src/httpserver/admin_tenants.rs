//! Admin-only tenant-management HTTP surface (`docs/tenancy_design.md` §4's
//! closing line — "...需要自助开通时再考虑数据库与管理 API": a real user
//! now needs it, so here it is). Create/patch/delete a `[[tenant]]` block in
//! `tenants.toml`, complementing the read-only self-service query
//! `session::list_sessions` already covers.
//!
//! These routes only exist at all when the server started with a real
//! `tenants.toml` already loaded into a [`TokenTable`]
//! (`main.rs`'s `token_table.is_some()`) — see [`TenantAdminState`]'s doc
//! comment for why a dev-mode server (no tenants.toml, implicit-admin
//! fallback) never gets [`admin_tenants_router`] merged in at all, rather
//! than every handler branching on "is auth even configured?" at request
//! time.

use super::auth::TokenTable;
use super::response::session_error;
use super::tenant_config::{
    into_entries, read_tenants_file_raw, write_tenants_file_atomically, TenantConfigError,
    TenantSection,
};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{patch, post};
use axum::{Json, Router};
use rand::Rng;
use session_protocol::{
    SessionWireError, TenantCreateRequest, TenantCreateResponse, TenantDeleteResponse,
    TenantPatchRequest, TenantPatchResponse,
};
use std::path::PathBuf;
use std::sync::Arc;
use xgovernor_core::{project_session_error, SessionApplication};

/// Length of the random alphanumeric body appended after the `xgt_` prefix
/// (`xgt` for "xGovernor tenant", so these are recognizable at a glance next
/// to any other token shape). 40 alphanumeric characters is
/// `log2(62) * 40 ≈ 238` bits of entropy — far beyond anything a
/// rate-limited HTTP endpoint needs to worry about brute-forcing; there is
/// no case for a configurable length here.
const GENERATED_TOKEN_LENGTH: usize = 40;

fn generate_token() -> String {
    let body: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(GENERATED_TOKEN_LENGTH)
        .map(char::from)
        .collect();
    format!("xgt_{body}")
}

/// Every `TenantConfigError` reaching these handlers (the startup load
/// already succeeded, or this state wouldn't exist — see
/// [`TenantAdminState`]) represents an unexpected server-side condition:
/// disk I/O failing, the file having been hand-edited into something that
/// no longer parses, or a post-edit re-validation catching a bug in this
/// module's own logic. None of that is the caller's fault, so it all maps
/// to one wire shape.
fn config_error_to_wire(error: TenantConfigError) -> SessionWireError {
    SessionWireError::Internal {
        message: error.to_string(),
        details: serde_json::Value::Null,
    }
}

/// Everything the admin tenant-management handlers need beyond the ordinary
/// [`super::session::SessionHttpState`]: where `tenants.toml` lives, the
/// live [`TokenTable`] to reload after every successful write, the
/// [`SessionApplication`] to check for active sessions before a delete, and
/// a write-serializing lock so two concurrent admin requests can't
/// read-modify-write the file into a lost update.
///
/// Deliberately holds a bare [`TokenTable`], not `Option<TokenTable>`: this
/// state is only ever constructed by `main.rs` when a real `tenants.toml`
/// was already loaded at startup. When it wasn't (dev mode), `main.rs`
/// never constructs this struct and never merges [`admin_tenants_router`]
/// into the composed router at all — these routes 404 rather than any
/// handler having to special-case "auth isn't configured, so a file write
/// here would have zero live effect until restart".
#[derive(Clone)]
pub struct TenantAdminState {
    token_table: TokenTable,
    tenants_config_path: PathBuf,
    application: SessionApplication,
    write_lock: Arc<tokio::sync::Mutex<()>>,
}

impl TenantAdminState {
    pub fn new(
        token_table: TokenTable,
        tenants_config_path: PathBuf,
        application: SessionApplication,
    ) -> Self {
        Self {
            token_table,
            tenants_config_path,
            application,
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

pub fn admin_tenants_router(state: TenantAdminState) -> Router {
    Router::new()
        .route("/api/v1/admin/tenants", post(create_tenant))
        .route(
            "/api/v1/admin/tenants/:tenant_id",
            patch(patch_tenant).delete(delete_tenant),
        )
        .with_state(state)
}

async fn create_tenant(
    State(state): State<TenantAdminState>,
    Json(request): Json<TenantCreateRequest>,
) -> Response {
    match create_tenant_impl(&state, request).await {
        Ok(response) => (StatusCode::CREATED, Json(response)).into_response(),
        Err(error) => session_error(error),
    }
}

async fn create_tenant_impl(
    state: &TenantAdminState,
    request: TenantCreateRequest,
) -> Result<TenantCreateResponse, SessionWireError> {
    let tenant_id = request.tenant_id.trim();
    if tenant_id.is_empty() {
        return Err(SessionWireError::InvalidRequest {
            message: "tenant_id must not be empty".to_string(),
        });
    }
    let principal = request
        .principal
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "tenant".to_string());

    let _guard = state.write_lock.lock().await;
    let mut file =
        read_tenants_file_raw(&state.tenants_config_path).map_err(config_error_to_wire)?;

    if file.tenant.iter().any(|t| t.tenant_id == tenant_id) {
        return Err(SessionWireError::Conflict {
            message: format!("tenant_id {tenant_id:?} already exists"),
        });
    }

    let token = generate_token();
    file.tenant.push(TenantSection {
        tenant_id: tenant_id.to_string(),
        tokens: vec![token.clone()],
        principal: principal.clone(),
        max_sessions: request.max_sessions,
        max_requests_per_minute: request.max_requests_per_minute,
    });

    let entries = into_entries(file.clone()).map_err(config_error_to_wire)?;
    write_tenants_file_atomically(&state.tenants_config_path, &file)
        .map_err(config_error_to_wire)?;
    state.token_table.reload(entries);

    Ok(TenantCreateResponse {
        tenant_id: tenant_id.to_string(),
        token,
        principal,
        max_sessions: request.max_sessions,
        max_requests_per_minute: request.max_requests_per_minute,
    })
}

async fn patch_tenant(
    State(state): State<TenantAdminState>,
    Path(tenant_id): Path<String>,
    Json(request): Json<TenantPatchRequest>,
) -> Response {
    match patch_tenant_impl(&state, &tenant_id, request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(error),
    }
}

async fn patch_tenant_impl(
    state: &TenantAdminState,
    tenant_id: &str,
    request: TenantPatchRequest,
) -> Result<TenantPatchResponse, SessionWireError> {
    let _guard = state.write_lock.lock().await;
    let mut file =
        read_tenants_file_raw(&state.tenants_config_path).map_err(config_error_to_wire)?;

    let section = file
        .tenant
        .iter_mut()
        .find(|t| t.tenant_id == tenant_id)
        .ok_or_else(|| SessionWireError::TenantNotFound {
            tenant_id: tenant_id.to_string(),
        })?;

    // Tri-state: `None` (field absent from the request body) leaves the
    // current value untouched; `Some(inner)` (field present, whether as
    // `null` or a number) overwrites it with `inner` — see
    // `session_protocol::TenantPatchRequest`'s doc comment.
    if let Some(max_sessions) = request.max_sessions {
        section.max_sessions = max_sessions;
    }
    if let Some(max_requests_per_minute) = request.max_requests_per_minute {
        section.max_requests_per_minute = max_requests_per_minute;
    }

    let principal = section.principal.clone();
    let max_sessions = section.max_sessions;
    let max_requests_per_minute = section.max_requests_per_minute;

    let entries = into_entries(file.clone()).map_err(config_error_to_wire)?;
    write_tenants_file_atomically(&state.tenants_config_path, &file)
        .map_err(config_error_to_wire)?;
    state.token_table.reload(entries);

    Ok(TenantPatchResponse {
        tenant_id: tenant_id.to_string(),
        principal,
        max_sessions,
        max_requests_per_minute,
    })
}

async fn delete_tenant(
    State(state): State<TenantAdminState>,
    Path(tenant_id): Path<String>,
) -> Response {
    match delete_tenant_impl(&state, &tenant_id).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => session_error(error),
    }
}

async fn delete_tenant_impl(
    state: &TenantAdminState,
    tenant_id: &str,
) -> Result<TenantDeleteResponse, SessionWireError> {
    // Checked before the write lock is even acquired: this is a DB query,
    // not a file edit, and there's nothing to serialize it against. Small
    // TOCTOU window against a session opening for this tenant between this
    // check and the removal below is accepted — this is an operator-driven
    // admin action, not a safety-critical path, and holding the write lock
    // across an awaited DB call would serialize every other admin request
    // behind it for no real benefit.
    if state
        .application
        .tenant_has_active_sessions(tenant_id)
        .await
        .map_err(project_session_error)?
    {
        return Err(SessionWireError::Conflict {
            message: format!(
                "tenant {tenant_id:?} has active sessions; close them before deleting the tenant"
            ),
        });
    }

    let _guard = state.write_lock.lock().await;
    let mut file =
        read_tenants_file_raw(&state.tenants_config_path).map_err(config_error_to_wire)?;

    let before = file.tenant.len();
    file.tenant.retain(|t| t.tenant_id != tenant_id);
    if file.tenant.len() == before {
        return Err(SessionWireError::TenantNotFound {
            tenant_id: tenant_id.to_string(),
        });
    }

    // Same invariant the SIGHUP reload path enforces reactively
    // (`main.rs::spawn_tenants_reload_task`) — this checks it proactively,
    // before the file is ever written, so a delete that would zero out
    // every credential is refused outright rather than applied and then
    // separately guarded against at reload time.
    let entries = into_entries(file.clone()).map_err(config_error_to_wire)?;
    if entries.is_empty() {
        return Err(SessionWireError::Conflict {
            message: "deleting this tenant would leave zero admin/tenant credentials \
                      configured in tenants.toml; add an admin token first"
                .to_string(),
        });
    }

    write_tenants_file_atomically(&state.tenants_config_path, &file)
        .map_err(config_error_to_wire)?;
    state.token_table.reload(entries);

    Ok(TenantDeleteResponse {
        tenant_id: tenant_id.to_string(),
    })
}

