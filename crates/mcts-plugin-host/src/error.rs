use mcts_protocol::ProtocolError;

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
}
