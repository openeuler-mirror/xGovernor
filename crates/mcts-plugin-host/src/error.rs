use mcts_protocol::{ProtocolError, RpcErrorCode};

#[derive(Debug, thiserror::Error)]
pub enum PluginHostError {
    #[error("plugin I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("plugin JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("plugin protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("plugin manifest error: {0}")]
    Manifest(String),

    #[error("plugin process ended before returning a message")]
    Eof,

    #[error("plugin process exited unexpectedly: {0}")]
    Exited(std::process::ExitStatus),

    #[error("plugin process is not running")]
    NotRunning,

    #[error("response id did not match the request")]
    ResponseIdMismatch,

    #[error("invalid JSON-RPC response: {0}")]
    InvalidResponse(String),

    #[error("run not found: {0}")]
    RunNotFound(String),

    #[error("branch not found: {0}")]
    BranchNotFound(String),

    #[error("capability denied: {0}")]
    CapabilityDenied(String),

    #[error("request deadline exceeded")]
    DeadlineExceeded,

    #[error("idempotency conflict: {0}")]
    IdempotencyConflict(String),

    #[error("run cancelled: {0}")]
    Cancelled(String),

    #[error("branch busy: {0}")]
    BranchBusy(String),
}

impl PluginHostError {
    pub fn rpc_code(&self) -> i64 {
        match self {
            Self::RunNotFound(_) => RpcErrorCode::RunNotFound as i64,
            Self::BranchNotFound(_) => RpcErrorCode::BranchNotFound as i64,
            Self::DeadlineExceeded => RpcErrorCode::DeadlineExceeded as i64,
            Self::Cancelled(_) => RpcErrorCode::Cancelled as i64,
            Self::IdempotencyConflict(_) => RpcErrorCode::IdempotencyConflict as i64,
            Self::BranchBusy(_) => RpcErrorCode::BranchBusy as i64,
            Self::CapabilityDenied(_) => RpcErrorCode::CapabilityDenied as i64,
            _ => RpcErrorCode::InternalError as i64,
        }
    }
}
