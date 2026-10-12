//! E2B cloud sandbox provider and operation backend.

mod backend;
mod bootstrap;
mod control;
mod error;
mod exec;
mod filesystem;
mod path;
mod provider;
mod search;

pub use provider::{e2b_provider, E2bProvider};

#[cfg(all(test, target_os = "linux"))]
mod process_guard_tests;

#[cfg(all(test, target_os = "linux"))]
mod control_transport_tests;
