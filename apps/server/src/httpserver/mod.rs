//! HTTP transport assembly.
//!
//! The former monolithic runtime router is no longer part of the active
//! workspace: its xiaoO-specific dependencies were removed with the old app
//! surface. New protocol-native session traffic is assembled through
//! [`router::create_router`], and the actual session handlers live in
//! [`session`]. This keeps route composition separate from wire handlers.

pub mod auth;
pub mod response;
pub mod router;
pub mod session;
pub mod tenant_config;

pub use auth::TokenTable;
pub use router::create_router;
pub use session::{session_router, SessionHttpState};
pub use tenant_config::{load_tenants_file, TenantConfigError};
