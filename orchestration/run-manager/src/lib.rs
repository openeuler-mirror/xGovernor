//! Durable run admission and state. Execution is owned by a separately attached
//! worker; constructing this client does not start or simulate an executor.
pub mod config;
pub mod service;
mod store;
pub mod worker;

pub use store::{RunManager, Usage};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
