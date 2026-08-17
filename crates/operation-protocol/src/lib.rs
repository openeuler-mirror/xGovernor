//! Operation-plane contract between a runtime adapter and a concrete backend
//! implementation (local process, remote sandbox, container, ...).
//!
//! This is the counterpart to `provider-protocol`: that crate governs the
//! *lifecycle* of a provider instance (create/load/pause/delete/inspect) and
//! deliberately treats the operation surface as an opaque capability flag
//! (`ProviderOperationCapabilities`). This crate defines what that operation
//! surface actually looks like in-process, once a provider instance has been
//! attached to.

pub mod capability;
pub mod diff;

mod contract;
mod error;
mod permission;
mod types;

pub use contract::{OperationBackend, OperationBackendCapabilities};
pub use diff::{line_change_counts, FileChangeDelta};
pub use error::{ExecutionState, OperationError};
pub use permission::{
    OperationPermissionControl, SandboxPermissionCapability, SandboxPermissionGrantId,
    SandboxPermissionGrantRequest, SandboxPermissionScope, SandboxPolicyDenial,
};
pub use types::{
    BackendPath, ExportedFileHandle, ExportedFileMeta, ExportedFileReader, PathKind, PathStat,
    SharedExportedFileHandle,
};
