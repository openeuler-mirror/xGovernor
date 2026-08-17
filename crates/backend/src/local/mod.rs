//! Local (in-process, on-host) provider and operation backend.

mod backend;
mod error;
mod exec;
mod export;
mod factory;
mod filesystem;
mod path;
mod policy;
mod provider;
mod search;

pub use error::LocalBuildError;
pub use factory::{local_backend, local_backend_with_isolation};
pub use provider::{local_provider, LocalProvider};
