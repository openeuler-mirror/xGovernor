use std::fmt;

/// Error building a local operation backend before it is ready to serve
/// requests (invalid config, unsupported isolation on this OS, missing
/// directories, etc).
///
/// This is a narrower, local-only replacement for xiaoO's
/// `agent_contracts::backend::OperationBackendBuildError`. That type
/// belonged to the generic multi-kind builder/config dispatch
/// (`OperationBackendBuilder` / `OperationBackendConfig`), which was
/// deliberately not ported into `operation-protocol` (see the crate-level
/// notes in `crates/operation-protocol` — this migration phase only carries
/// over the capability contracts, not the generic config-driven factory).
/// Each concrete backend crate now owns its own build-error type and maps it
/// into `provider_protocol::ProviderControlError` itself (see
/// `crate::local::provider`).
#[derive(Debug, Clone)]
pub enum LocalBuildError {
    InvalidConfig { message: String },
    Unsupported { message: String },
}

impl fmt::Display for LocalBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { message } => {
                write!(f, "invalid local backend config: {message}")
            }
            Self::Unsupported { message } => {
                write!(f, "unsupported local backend config: {message}")
            }
        }
    }
}

impl std::error::Error for LocalBuildError {}
