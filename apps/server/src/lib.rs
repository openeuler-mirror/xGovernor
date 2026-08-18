//! HTTP/SSE transport adapter for xGovernor session wire contracts.

pub mod httpserver;

pub use httpserver::{
    admin_tenants_router, create_router, load_tenants_file, session_router, SessionHttpState,
    TenantAdminState, TenantConfigError, TokenTable,
};
