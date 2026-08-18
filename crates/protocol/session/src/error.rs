use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionCapabilityFamily {
    Sandbox,
    Runtime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Error)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum SessionWireError {
    #[error("invalid request: {message}")]
    InvalidRequest { message: String },
    #[error("session not found: {runtime_id}")]
    NotFound { runtime_id: String },
    #[error("session conflict: {message}")]
    Conflict { message: String },
    #[error("session lease is required: {runtime_id}")]
    LeaseRequired { runtime_id: String },
    #[error("session lease is held by another client: {runtime_id}")]
    LeaseConflict {
        runtime_id: String,
        #[serde(default)]
        holder_client_id: Option<String>,
        #[serde(default)]
        holder_pid: Option<u32>,
        #[serde(default)]
        holder_hostname: Option<String>,
    },
    #[error("unsupported {family:?} capability: {capability}")]
    UnsupportedCapability {
        family: SessionCapabilityFamily,
        /// Kept as a string so clients can decode errors for capabilities
        /// introduced after the client was built.
        capability: String,
    },
    #[error("session operation timed out: {operation}")]
    Timeout { operation: String, timeout_ms: u64 },
    #[error("session is unavailable: {message}")]
    Unavailable { message: String },
    /// `docs/tenancy_design.md` §3.4/§6: distinct from `Unavailable` on
    /// purpose — `Unavailable` means "server-side problem, retry later";
    /// `QuotaExceeded` means "your tenant's own ceiling, retrying won't
    /// help". Conflating the two would teach clients the wrong retry
    /// behavior.
    #[error("tenant quota exceeded: {scope} (limit {limit})")]
    QuotaExceeded { scope: String, limit: u32 },
    #[error("internal session error")]
    Internal {
        message: String,
        #[serde(default)]
        details: Value,
    },
}

impl SessionWireError {
    /// HTTP status selected by the sole transport mapping.
    pub fn http_status(&self) -> u16 {
        match self {
            Self::InvalidRequest { .. } => 400,
            Self::NotFound { .. } => 404,
            Self::Conflict { .. } | Self::LeaseConflict { .. } => 409,
            Self::LeaseRequired { .. } => 401,
            Self::UnsupportedCapability { .. } => 422,
            Self::Timeout { .. } => 504,
            Self::Unavailable { .. } => 503,
            Self::QuotaExceeded { .. } => 429,
            Self::Internal { .. } => 500,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn wire_error_has_one_tagged_shape_and_status_mapping() {
        let error = SessionWireError::UnsupportedCapability {
            family: SessionCapabilityFamily::Runtime,
            capability: "future_capability".into(),
        };
        assert_eq!(error.http_status(), 422);
        assert_eq!(
            serde_json::to_value(error).unwrap(),
            json!({
                "code": "unsupported_capability",
                "family": "runtime",
                "capability": "future_capability"
            })
        );
    }
}
