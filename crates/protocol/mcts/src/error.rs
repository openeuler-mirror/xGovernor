use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RpcErrorCode {
    ParseError = -32700,
    InvalidRequest = -32600,
    MethodNotFound = -32601,
    InvalidParams = -32602,
    InternalError = -32603,
    CapabilityDenied = -32001,
    RunNotFound = -32002,
    BranchNotFound = -32003,
    DeadlineExceeded = -32004,
    Cancelled = -32005,
    StateExpired = -32006,
    IdempotencyConflict = -32007,
    IncompatibleProtocol = -32008,
    BranchBusy = -32009,
    BudgetExhausted = -32010,
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("invalid JSON-RPC message: {0}")]
    InvalidMessage(String),

    #[error("unknown method: {0}")]
    UnknownMethod(String),

    #[error("unknown notification: {0}")]
    UnknownNotification(String),
}

impl From<serde_json::Error> for ProtocolError {
    fn from(value: serde_json::Error) -> Self {
        ProtocolError::InvalidMessage(value.to_string())
    }
}
