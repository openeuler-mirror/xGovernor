use crate::{
    DeploymentProfile, IsolationState, LlmOverrideRequest, ResolvedLlmDescriptor,
    SessionCapabilities, SessionCapabilityRequest, SessionExtensions, WorkspaceSpec,
    WorkspaceState,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SessionLeaseClaim {
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub client_pid: Option<u32>,
    #[serde(default)]
    pub client_hostname: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionOpenRequest {
    #[serde(default)]
    pub runtime_id: Option<String>,
    #[serde(default)]
    pub runtime_kind: Option<String>,
    pub conversation_id: String,
    pub sender_id: String,
    #[serde(default)]
    pub workspace: WorkspaceSpec,
    #[serde(default)]
    pub deployment: DeploymentProfile,
    #[serde(default)]
    pub requested_capabilities: SessionCapabilityRequest,
    /// Transient, request-only model configuration consumed while the runtime
    /// is opened. Secrets must not be projected into responses or persisted.
    #[serde(default)]
    pub llm: Option<LlmOverrideRequest>,
    /// Runtime-specific bootstrap input, keyed by runtime namespace (for
    /// example `ext.xiaoo`). This remains opaque to the core protocol.
    #[serde(default)]
    pub ext: SessionExtensions,
    #[serde(default)]
    pub lease: SessionLeaseClaim,
}

macro_rules! session_control_request {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            pub runtime_id: String,
            #[serde(default)]
            pub lease: SessionLeaseClaim,
        }
    };
}

session_control_request!(SessionCloseRequest);
session_control_request!(SessionDetachRequest);
session_control_request!(SessionHeartbeatRequest);
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionLoadRequest {
    pub checkpoint_id: String,
    #[serde(default)]
    pub runtime_id: Option<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub sender_id: Option<String>,
    #[serde(default)]
    pub requested_capabilities: SessionCapabilityRequest,
    #[serde(default)]
    pub deployment: DeploymentProfile,
    #[serde(default)]
    pub lease: SessionLeaseClaim,
    #[serde(default)]
    pub llm: Option<LlmOverrideRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionCancelRequest {
    pub runtime_id: String,
    /// `Some` requests exact-turn cancellation; `None` requests cancellation
    /// of the runtime's currently active turn.
    #[serde(default)]
    pub turn_id: Option<String>,
    #[serde(default)]
    pub lease: SessionLeaseClaim,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionForkRequest {
    pub parent_runtime_id: String,
    #[serde(default)]
    pub runtime_id: Option<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub sender_id: Option<String>,
    #[serde(default)]
    pub workspace: Option<WorkspaceSpec>,
    #[serde(default)]
    pub deployment: Option<DeploymentProfile>,
    #[serde(default)]
    pub requested_capabilities: SessionCapabilityRequest,
    #[serde(default)]
    pub lease: SessionLeaseClaim,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionLifecycleStatus {
    Opening,
    Idle,
    Running,
    Paused,
    Failed,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionOpenResponse {
    pub runtime_id: String,
    pub conversation_id: String,
    pub sender_id: String,
    pub status: SessionLifecycleStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub runtime_kind: String,
    pub workspace: WorkspaceState,
    pub isolation: IsolationState,
    pub effective_capabilities: SessionCapabilities,
    #[serde(default)]
    pub llm: Option<ResolvedLlmDescriptor>,
}

/// Lightweight per-session projection for `GET /api/v1/sessions` — deliberately
/// narrower than [`SessionOpenResponse`] (no workspace/isolation/capabilities
/// detail). Callers who need the full projection for a specific session
/// already have `runtime_id` from here and can re-attach via `sessions/open`
/// (idempotent when `runtime_id` is set) to fetch it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSummary {
    pub runtime_id: String,
    pub conversation_id: String,
    pub sender_id: String,
    pub status: SessionLifecycleStatus,
    pub runtime_kind: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

/// Quota snapshot for the caller's own scope: an admin token sees the global
/// count (no `max_sessions` ceiling), a tenant token sees its own tenant's
/// count against its own ceiling (`docs/tenancy_design.md` §4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantQuotaSnapshot {
    #[serde(default)]
    pub max_sessions: Option<u32>,
    pub active_sessions: u32,
    #[serde(default)]
    pub max_requests_per_minute: Option<u32>,
}

/// Response for `GET /api/v1/sessions`. Deliberately not paginated (v1 scope,
/// `docs/tenancy_design.md` §4): `sessions` holds up to some server-chosen
/// cap of the most-recently-updated *active* sessions (never `failed`/
/// `closed`) visible to the caller — admin sees every tenant, a tenant sees
/// only its own. `has_more` is true when the caller's true active-session
/// count exceeds what's returned; `quota.active_sessions` always reflects the
/// true count, uncapped, so the quota snapshot stays accurate even when the
/// list itself is truncated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionListResponse {
    pub sessions: Vec<SessionSummary>,
    pub has_more: bool,
    pub quota: TenantQuotaSnapshot,
}

/// Response shared by close, cancel and detach control operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionControlResponse {
    pub runtime_id: String,
    pub status: SessionLifecycleStatus,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionHeartbeatResponse {
    pub runtime_id: String,
    pub accepted: bool,
    #[serde(default)]
    pub lease_expires_at_ms: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn open_accepts_transient_llm_and_namespaced_runtime_bootstrap() {
        let request: SessionOpenRequest = serde_json::from_value(json!({
            "conversation_id": "conversation-1",
            "sender_id": "sender-1",
            "llm": {
                "provider": "openai",
                "model": "gpt",
                "api_key": "secret"
            },
            "ext": {
                "xiaoo": {
                    "skills": ["/opt/company/skills"],
                    "runtime_profile_id": "plan",
                    "build_tags": ["remote"]
                }
            }
        }))
        .unwrap();

        assert_eq!(request.llm.unwrap().api_key.as_deref(), Some("secret"));
        assert_eq!(request.ext["xiaoo"]["runtime_profile_id"], "plan");
    }

    #[test]
    fn cancel_supports_exact_and_active_turn_semantics() {
        let active: SessionCancelRequest = serde_json::from_value(json!({
            "runtime_id": "runtime-1"
        }))
        .unwrap();
        assert_eq!(active.turn_id, None);

        let exact: SessionCancelRequest = serde_json::from_value(json!({
            "runtime_id": "runtime-1",
            "turn_id": "turn-1"
        }))
        .unwrap();
        assert_eq!(exact.turn_id.as_deref(), Some("turn-1"));
    }

    #[test]
    fn open_rejects_runtime_specific_root_fields() {
        assert!(serde_json::from_value::<SessionOpenRequest>(json!({
            "conversation_id": "conversation-1",
            "sender_id": "sender-1",
            "skills": ["/opt/company/skills"]
        }))
        .is_err());
    }
}
