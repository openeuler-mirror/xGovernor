//! Control-plane contract between the governor and infrastructure providers.
//!
//! Business owners and provider-specific settings cross the boundary as
//! opaque values; this crate does not interpret either.

mod error;
mod lifecycle;
mod provider;
mod types;

pub use error::ProviderControlError;
pub use lifecycle::{
    ProviderLifecycleEvent, ProviderLifecycleOperation, ProviderLifecycleState,
    ProviderLifecycleStateMachine,
};
pub use provider::{Provider, ProviderLifecycle};
pub use types::*;
