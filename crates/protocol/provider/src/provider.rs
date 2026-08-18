use crate::{
    ProviderControlError, ProviderCreateRequest, ProviderDeleteOutcome, ProviderDeleteRequest,
    ProviderInspectRequest, ProviderInstance, ProviderInstanceStatus, ProviderKind,
    ProviderLoadRequest, ProviderPauseRequest, ProviderSnapshot,
};
use async_trait::async_trait;

/// Metadata and the single lifecycle entry point implemented by every provider.
pub trait Provider: Send + Sync {
    fn kind(&self) -> &ProviderKind;
    fn lifecycle(&self) -> &dyn ProviderLifecycle;
}

#[async_trait]
pub trait ProviderLifecycle: Send + Sync {
    async fn create(
        &self,
        request: ProviderCreateRequest,
    ) -> Result<ProviderInstance, ProviderControlError>;

    async fn load(
        &self,
        request: ProviderLoadRequest,
    ) -> Result<ProviderInstance, ProviderControlError>;

    async fn pause(
        &self,
        request: ProviderPauseRequest,
    ) -> Result<ProviderSnapshot, ProviderControlError>;

    async fn delete(
        &self,
        request: ProviderDeleteRequest,
    ) -> Result<ProviderDeleteOutcome, ProviderControlError>;

    async fn inspect(
        &self,
        request: ProviderInspectRequest,
    ) -> Result<ProviderInstanceStatus, ProviderControlError>;

    async fn list_instances(&self) -> Result<Vec<ProviderInstance>, ProviderControlError>;
}
