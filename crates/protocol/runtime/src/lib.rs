//! Runtime-neutral contracts shared by xGovernor runtime adapters.
//!
//! This crate contains data only. It deliberately has no dependency on
//! xGovernor core, provider implementations, operation backends, or a
//! concrete agent runtime such as Pi or xiaoO.

mod capability;
mod error;
mod event;
mod request;
mod state;
mod worker;

pub use capability::*;
pub use error::*;
pub use event::*;
pub use request::*;
pub use state::*;
pub use worker::*;
