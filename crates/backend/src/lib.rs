//! Concrete provider implementations for xGovernor.
//!
//! Each submodule implements both [`provider_protocol::Provider`] /
//! [`provider_protocol::ProviderLifecycle`] (control-plane) and bridges to an
//! [`operation_protocol::OperationBackend`] (operation-plane) via
//! [`OperationAttach`], which is defined here. `provider-protocol`
//! deliberately excludes the attach step from its control-plane contract
//! (see the `ProviderOperationCapabilities` doc comment in that crate: "the
//! adapter itself is intentionally outside this control-plane protocol") —
//! so the bridge from a `ProviderInstance` to a live `OperationBackend` is
//! owned by each concrete provider crate/module instead.
//!
//! Scope of this phase: `local` and `e2b` providers are ported. Other
//! providers (conch) and the multi-session `BackendManager`/gateway
//! orchestration layer (leasing, fork/checkpoint lineage, sandbox pooling)
//! are deferred to a future phase, to be picked up once a `AgentRuntime`
//! implementation actually needs multi-session sandbox sharing.

pub mod e2b;
pub mod ledger;
pub mod local;
pub mod process_group;
pub mod sqlite_ledger;

pub use ledger::{ActiveLedgerEntry, ProviderInstanceLedger};
pub use sqlite_ledger::SqliteProviderInstanceLedger;

use async_trait::async_trait;
use operation_protocol::OperationBackend;
use provider_protocol::{ProviderControlError, ProviderInstance};
use std::sync::Arc;

/// Bridge from a control-plane [`ProviderInstance`] to its operation-plane
/// [`OperationBackend`].
///
/// Implemented by each concrete provider (alongside `Provider` /
/// `ProviderLifecycle`) once its instance has reached a state where the
/// operation plane is usable (typically `Active`).
#[async_trait]
pub trait OperationAttach: Send + Sync {
    async fn attach(
        &self,
        instance: &ProviderInstance,
    ) -> Result<Arc<dyn OperationBackend>, ProviderControlError>;
}
