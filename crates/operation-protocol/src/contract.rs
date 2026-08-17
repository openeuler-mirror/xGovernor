use crate::capability::{
    OperationExec, OperationExport, OperationFileSystem, OperationPathResolver, OperationSearch,
};
use crate::{OperationError, OperationPermissionControl};
use async_trait::async_trait;

/// Capabilities advertised by an operation backend implementation.
#[derive(Debug, Clone, Copy)]
pub struct OperationBackendCapabilities {
    pub supports_atomic_write: bool,
    pub supports_grep: bool,
    pub supports_export_file: bool,
    pub supports_lsp: bool,
}

/// Aggregate contract implemented by a concrete execution backend.
///
/// This is attached to (produced from) a `provider_protocol::ProviderInstance`
/// once its provider has finished create/load. Attaching itself is
/// intentionally not part of `provider-protocol` (that crate governs
/// lifecycle only); each concrete provider crate owns the bridge from its
/// `ProviderInstance` to an `Arc<dyn OperationBackend>`.
#[async_trait]
pub trait OperationBackend: Send + Sync {
    /// Stable identifier for logging and diagnostics.
    fn backend_id(&self) -> &str;

    /// Advertised capability metadata for gating and fail-fast decisions.
    fn capabilities(&self) -> OperationBackendCapabilities;

    fn paths(&self) -> &dyn OperationPathResolver;
    fn files(&self) -> &dyn OperationFileSystem;
    fn search(&self) -> &dyn OperationSearch;
    fn exec(&self) -> &dyn OperationExec;
    fn export(&self) -> &dyn OperationExport;
    fn permission_control(&self) -> Option<&dyn OperationPermissionControl> {
        None
    }
    async fn shutdown(&self) -> Result<(), OperationError>;
}
