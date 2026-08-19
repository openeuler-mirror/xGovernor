//! HTTP/SSE wire contract shared by governor daemons and session clients.
//!
//! Core types are runtime-neutral. Runtime-specific input and events belong in
//! the namespaced `ext` bags and remain opaque to this crate.

mod control;
mod environment;
mod error;
mod event;
mod interaction;
mod operation;
mod tenant_admin;

pub use control::*;
pub use environment::*;
pub use error::*;
pub use event::*;
pub use interaction::*;
pub use operation::*;
pub use tenant_admin::*;

use serde_json::Value;
use std::collections::BTreeMap;

pub type SessionExtensions = BTreeMap<String, Value>;
