use crate::ProviderLifecycleState;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fmt;

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

string_id!(ProviderKind);
string_id!(BackendId);
string_id!(ProviderInstanceId);
string_id!(ProviderSnapshotId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderLifecycleReason {
    Acquire,
    Restore,
    Reclaim,
    Release,
    Shutdown,
    Reconcile,
    ErrorCleanup,
    UserRequested,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ProviderCapability {
    Pause,
    Snapshot,
    NetworkIsolation,
    ResourceLimits,
    SerializedHandle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProviderCapabilities {
    #[serde(default)]
    pub lifecycle: BTreeSet<ProviderCapability>,
    #[serde(default)]
    pub operation_plane: ProviderOperationCapabilities,
}

/// Capabilities of the instance's operation adapter. The adapter itself is
/// intentionally outside this control-plane protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProviderOperationCapabilities {
    #[serde(default)]
    pub exec: bool,
    #[serde(default)]
    pub file_read: bool,
    #[serde(default)]
    pub file_write: bool,
    #[serde(default)]
    pub search: bool,
    #[serde(default)]
    pub export_file: bool,
    #[serde(default)]
    pub lsp: bool,
    #[serde(default)]
    pub network: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProviderResourceLimits {
    #[serde(default)]
    pub vcpu_count: Option<u32>,
    #[serde(default)]
    pub memory_mb: Option<u64>,
    #[serde(default)]
    pub disk_mb: Option<u64>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProviderResourceAllocation {
    #[serde(default)]
    pub vcpu_count: Option<u32>,
    #[serde(default)]
    pub memory_mb: Option<u64>,
    #[serde(default)]
    pub disk_mb: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderEndpoint {
    Local,
    Tcp { host: String, port: u16 },
    Unix { path: String },
    Handle { value: Value },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderPauseMode {
    Suspend,
    Snapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ProviderLoadSource {
    Snapshot(ProviderSnapshotId),
    Instance(ProviderInstanceId),
    SerializedHandle(Value),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderCreateRequest {
    pub backend_id: BackendId,
    pub owner_ref: String,
    pub reason: ProviderLifecycleReason,
    #[serde(default)]
    pub resource_limits: ProviderResourceLimits,
    #[serde(default)]
    pub provider_options: Value,
    #[serde(default)]
    pub correlation: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderLoadRequest {
    pub backend_id: BackendId,
    pub owner_ref: String,
    pub source: ProviderLoadSource,
    pub reason: ProviderLifecycleReason,
    #[serde(default)]
    pub resource_limits: ProviderResourceLimits,
    #[serde(default)]
    pub provider_options: Value,
    #[serde(default)]
    pub correlation: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderPauseRequest {
    pub backend_id: BackendId,
    pub instance_id: ProviderInstanceId,
    pub mode: ProviderPauseMode,
    pub reason: ProviderLifecycleReason,
    #[serde(default)]
    pub correlation: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderDeleteRequest {
    pub backend_id: BackendId,
    #[serde(default)]
    pub instance_id: Option<ProviderInstanceId>,
    /// Delete a specific snapshot instead of (or in addition to) a live
    /// instance. Providers that have no snapshot concept independent of an
    /// instance should reject a request that carries only a `snapshot_id`
    /// with `ProviderControlError::UnsupportedCapability`.
    #[serde(default)]
    pub snapshot_id: Option<ProviderSnapshotId>,
    pub reason: ProviderLifecycleReason,
    #[serde(default)]
    pub correlation: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderInspectRequest {
    pub backend_id: BackendId,
    #[serde(default)]
    pub instance_id: Option<ProviderInstanceId>,
    pub reason: ProviderLifecycleReason,
    #[serde(default)]
    pub correlation: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderSnapshot {
    pub snapshot_id: ProviderSnapshotId,
    pub provider: ProviderKind,
    #[serde(default)]
    pub source_instance_id: Option<ProviderInstanceId>,
    #[serde(default)]
    pub serialized_handle: Option<Value>,
    #[serde(default)]
    pub metadata: Value,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderInstance {
    pub backend_id: BackendId,
    pub provider: ProviderKind,
    pub instance_id: ProviderInstanceId,
    pub state: ProviderLifecycleState,
    #[serde(default)]
    pub endpoint: Option<ProviderEndpoint>,
    #[serde(default)]
    pub snapshot: Option<ProviderSnapshot>,
    #[serde(default)]
    pub capabilities: ProviderCapabilities,
    #[serde(default)]
    pub resources: ProviderResourceAllocation,
    #[serde(default)]
    pub metadata: Value,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderInstanceStatus {
    pub backend_id: BackendId,
    pub provider: ProviderKind,
    #[serde(default)]
    pub instance_id: Option<ProviderInstanceId>,
    pub state: ProviderLifecycleState,
    #[serde(default)]
    pub endpoint: Option<ProviderEndpoint>,
    #[serde(default)]
    pub snapshot: Option<ProviderSnapshot>,
    #[serde(default)]
    pub capabilities: ProviderCapabilities,
    #[serde(default)]
    pub resources: ProviderResourceAllocation,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub metadata: Value,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderDeleteOutcome {
    pub backend_id: BackendId,
    pub provider: ProviderKind,
    #[serde(default)]
    pub instance_id: Option<ProviderInstanceId>,
    pub deleted: bool,
    #[serde(default)]
    pub retained_snapshots: Vec<ProviderSnapshotId>,
    /// Snapshots actually deleted by this call (distinct from
    /// `retained_snapshots`, which lists snapshots left behind).
    #[serde(default)]
    pub deleted_snapshots: Vec<ProviderSnapshotId>,
    #[serde(default)]
    pub correlation: Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_shape_uses_an_opaque_owner_reference() {
        let request = ProviderCreateRequest {
            backend_id: BackendId("backend-1".into()),
            owner_ref: "tenant/opaque-owner".into(),
            reason: ProviderLifecycleReason::Acquire,
            resource_limits: ProviderResourceLimits {
                vcpu_count: Some(2),
                ..Default::default()
            },
            provider_options: json!({"template": "base"}),
            correlation: json!({"trace_id": "trace-1"}),
        };
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({
                "backend_id": "backend-1",
                "owner_ref": "tenant/opaque-owner",
                "reason": "acquire",
                "resource_limits": {"vcpu_count": 2, "memory_mb": null, "disk_mb": null, "timeout_ms": null},
                "provider_options": {"template": "base"},
                "correlation": {"trace_id": "trace-1"}
            })
        );
    }
}
