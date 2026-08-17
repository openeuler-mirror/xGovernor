use async_trait::async_trait;
use provider_protocol::{ProviderControlError, ProviderInstance, ProviderKind};

/// One row from [`ProviderInstanceLedger::list_active`]: everything a
/// reconciliation pass needs to compare a ledger row against provider
/// reality and, if it survives, rehydrate quota tracking for it — without a
/// second round-trip back to the ledger.
#[derive(Debug, Clone, PartialEq)]
pub struct ActiveLedgerEntry {
    pub runtime_id: String,
    pub owner_ref: String,
    pub instance: ProviderInstance,
}

/// Durable ledger of provider instances, keyed by the control-plane
/// `runtime_id` that owns each one (not the provider's own `instance_id`,
/// which is opaque to everything outside the provider).
#[async_trait]
pub trait ProviderInstanceLedger: Send + Sync {
    async fn record_created(
        &self,
        runtime_id: &str,
        owner_ref: &str,
        instance: &ProviderInstance,
    ) -> Result<(), ProviderControlError>;

    async fn record_deleted(&self, runtime_id: &str) -> Result<(), ProviderControlError>;

    async fn list_active(
        &self,
        provider: &ProviderKind,
    ) -> Result<Vec<ActiveLedgerEntry>, ProviderControlError>;
}
