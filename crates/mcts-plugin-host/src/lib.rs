//! Host-side process management for the MCTS stdio plugin.
//!
//! This crate deliberately does not implement xg HTTP routes or LLM policy. It
//! only owns the plugin process boundary: manifest validation, spawning,
//! JSON-RPC framing, lifecycle handshakes and process cleanup.

pub mod error;
pub mod apply_plan;
pub mod capability_map;
pub mod host_cap_service;
pub mod job_notifier;
pub mod lease_keeper;
pub mod lifecycle;
pub mod llm_policy;
pub mod manifest;
pub mod mcts_run;
pub mod process;
pub mod profile_registry;
pub mod registry;
pub mod run_map;
pub mod stdio;

pub use apply_plan::ApplyPlanValidator;
pub use capability_map::{HostCapability, ensure_capability_available, ensure_method_available, map_granted_capabilities};
pub use job_notifier::JobNotifier;
pub use lease_keeper::{spawn_lease_keeper, LeaseKeeperGuard, DEFAULT_HEARTBEAT_INTERVAL};
pub use error::PluginHostError;
pub use host_cap_service::{FakeApplication, HostApplication, HostCapService, HostJobOutcome, HostSnapshot};
pub use lifecycle::PluginSession;
pub use llm_policy::LlmPolicy;
pub use manifest::{ModeManifest, PluginManifest, ProfileManifest, PLUGIN_ID};
pub use mcts_run::{
    build_launch_command, LlmConfig, MctsRun, MctsRunOutcome, MctsRunRequest, MctsRunStatus,
};
pub use mcts_protocol::messages::Failure;
pub use process::PluginProcess;
pub use profile_registry::ProfileRegistry;
pub use registry::{PluginManager, PluginRegistry, PluginStatus};
pub use run_map::{JobStatus, RunMap};
