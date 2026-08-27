//! Application/domain layer between wire contracts and concrete runtimes.
//!
//! Nothing in this crate is serialized by the HTTP transport directly.

pub mod application;
pub mod domain;
pub mod orphan_reaper;
pub mod projection;
pub mod prompt_utils;
pub mod reclaim_sweeper;
pub mod security;
pub mod session_lease;
pub mod sqlite_repository;
pub mod turn_cancellation;

pub use agent_runtime_protocol as runtime_protocol;

pub use application::{
    enforce_workspace_axiom, CheckpointListEntry, CheckpointListPage, Clock,
    NormalizedSessionEnvironment, RuntimeIdGenerator, RuntimeRegistration, SessionApplication,
    SessionEnvironmentNormalizer, SessionListPage, SessionRepository, SessionSubmission,
    TurnIdGenerator,
};
pub use domain::*;
pub use orphan_reaper::{spawn_orphan_reaper, spawn_orphan_reaper_with_config, OrphanReaperConfig};
pub use projection::{project_session, project_session_error, project_session_summary};
pub use prompt_utils::{
    compose_subagent_delegation_rules, generate_skills_dirs_table, SubagentRoleRecord,
};
pub use reclaim_sweeper::{
    spawn_reclaim_sweeper, spawn_reclaim_sweeper_with_config, ReclaimSweeperConfig,
};
pub use security::{Role, SecurityContext, TenantQuota, ADMIN_OWNER_REF};
// NOTE: `session_lease::SessionLease` (the operational, heartbeat-bearing
// lease-table entry: client_id/pid/hostname + last_heartbeat_ms + is_stale())
// is a different type from `domain::SessionLease` (the lightweight snapshot
// fact embedded in `SessionRecord.lease`: client_id/pid/hostname +
// expires_at_ms). They serve different layers — the table tracks live
// heartbeats, the domain record publishes a point-in-time fact — so it is
// deliberately NOT re-exported at the crate root to avoid colliding with
// `domain::SessionLease` via the `pub use domain::*` above. Reach it as
// `xgovernor_core::session_lease::SessionLease`.
pub use session_lease::{
    daemon_channel_principal, daemon_cron_principal, daemon_hook_principal, is_daemon_principal,
    ClockSkew, LeaseAcquireOutcome, LeaseCheckFailure, SessionLeaseTable, DAEMON_PRINCIPAL_PREFIX,
    ORPHAN_SESSION_THRESHOLD_MS, REAPER_INTERVAL, STALE_LEASE_THRESHOLD_MS,
};
pub use sqlite_repository::SqliteSessionRepository;
pub use turn_cancellation::{CancelSignal, TurnCancellationRegistry};
