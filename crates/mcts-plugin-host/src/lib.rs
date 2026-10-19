//! Host-side process management for the MCTS stdio plugin.
//!
//! This crate deliberately does not implement xg HTTP routes or LLM policy. It
//! only owns the plugin process boundary: manifest validation, spawning,
//! JSON-RPC framing, lifecycle handshakes and process cleanup.

pub mod error;
pub mod lifecycle;
pub mod manifest;
pub mod process;
pub mod registry;
pub mod stdio;

pub use error::PluginHostError;
pub use lifecycle::PluginSession;
pub use manifest::{ModeManifest, PluginManifest, ProfileManifest, PLUGIN_ID};
pub use process::PluginProcess;
pub use registry::{PluginManager, PluginRegistry, PluginStatus};
