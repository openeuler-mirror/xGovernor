use crate::{ProviderKind, ProviderLifecycleOperation, ProviderLifecycleState};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Error)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum ProviderControlError {
    #[error("invalid provider request: {message}")]
    InvalidRequest { message: String },
    #[error("provider resource not found: {resource_ref}")]
    NotFound { resource_ref: String },
    #[error("provider resource conflict: {message}")]
    Conflict { message: String },
    #[error("provider {provider} does not support capability {capability}")]
    UnsupportedCapability {
        provider: ProviderKind,
        capability: String,
    },
    #[error("invalid lifecycle transition for {operation:?} from {current:?}: {message}")]
    InvalidState {
        current: Option<ProviderLifecycleState>,
        operation: ProviderLifecycleOperation,
        message: String,
    },
    #[error("provider operation {operation} timed out after {timeout_ms} ms")]
    Timeout { operation: String, timeout_ms: u64 },
    #[error("provider {provider} failed: {message}")]
    ProviderFailure {
        provider: ProviderKind,
        message: String,
        #[serde(default)]
        details: Value,
    },
    #[error("provider transport failed: {message}")]
    Transport { message: String },
    #[error(
        "provider {provider} resource limit exceeded for owner {owner_ref}: current={current}, max={max}"
    )]
    ResourceLimitExceeded {
        provider: ProviderKind,
        owner_ref: String,
        current: usize,
        max: usize,
    },
}
