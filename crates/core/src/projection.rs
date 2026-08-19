use crate::domain::*;
use session_protocol::{
    IsolationState, ResolvedLlmDescriptor, SessionCapabilities, SessionCapabilityFamily,
    SessionLifecycleStatus, SessionOpenResponse, SessionRuntimeCapability,
    SessionSandboxCapability, SessionSummary, SessionWireError, WorkspaceAccessMode,
    WorkspaceState,
};

pub fn project_session(record: &SessionRecord) -> SessionOpenResponse {
    SessionOpenResponse {
        runtime_id: record.runtime_id.clone(),
        conversation_id: record.conversation_id.clone(),
        sender_id: record.sender_id.clone(),
        status: project_status(record.status),
        created_at_ms: record.created_at_ms,
        updated_at_ms: record.updated_at_ms,
        runtime_kind: record.runtime.runtime_kind.clone(),
        workspace: WorkspaceState {
            workspace_id: record.workspace.workspace_id.clone(),
            root: record.workspace.root.clone(),
            access: project_workspace_access(record.workspace.access),
            revision: record.workspace.revision.clone(),
            metadata: record.workspace.metadata.clone(),
        },
        isolation: IsolationState {
            boundary: match record.isolation.boundary {
                IsolationBoundary::Host => session_protocol::IsolationBoundary::Host,
                IsolationBoundary::Process => session_protocol::IsolationBoundary::Process,
                IsolationBoundary::Container => session_protocol::IsolationBoundary::Container,
                IsolationBoundary::VirtualMachine => {
                    session_protocol::IsolationBoundary::VirtualMachine
                }
                IsolationBoundary::Remote => session_protocol::IsolationBoundary::Remote,
            },
            workspace_access: project_workspace_access(record.isolation.workspace_access),
            network: match record.isolation.network {
                NetworkIsolation::None => session_protocol::NetworkIsolation::None,
                NetworkIsolation::Restricted => session_protocol::NetworkIsolation::Restricted,
                NetworkIsolation::Isolated => session_protocol::NetworkIsolation::Isolated,
            },
            effective_capabilities: record
                .capabilities
                .sandbox
                .iter()
                .copied()
                .map(project_sandbox_capability)
                .collect(),
            metadata: record.isolation.metadata.clone(),
        },
        effective_capabilities: SessionCapabilities {
            sandbox: record
                .capabilities
                .sandbox
                .iter()
                .copied()
                .map(project_sandbox_capability)
                .collect(),
            runtime: record
                .capabilities
                .runtime
                .iter()
                .copied()
                .map(project_runtime_capability)
                .collect(),
        },
        llm: record.llm.as_ref().map(|llm| ResolvedLlmDescriptor {
            provider: llm.provider.clone(),
            model: llm.model.clone(),
            api_base: llm.api_base.clone(),
            credential_source: llm.credential_source.clone(),
        }),
    }
}

/// Narrow projection for `GET /api/v1/sessions` — deliberately skips
/// workspace/isolation/capabilities (see [`SessionSummary`]'s doc comment).
pub fn project_session_summary(record: &SessionRecord) -> SessionSummary {
    SessionSummary {
        runtime_id: record.runtime_id.clone(),
        conversation_id: record.conversation_id.clone(),
        sender_id: record.sender_id.clone(),
        status: project_status(record.status),
        runtime_kind: record.runtime.runtime_kind.clone(),
        created_at_ms: record.created_at_ms,
        updated_at_ms: record.updated_at_ms,
    }
}

pub fn project_session_error(error: SessionDomainError) -> SessionWireError {
    match error {
        SessionDomainError::InvalidRequest { message } => {
            SessionWireError::InvalidRequest { message }
        }
        SessionDomainError::NotFound { runtime_id } => SessionWireError::NotFound { runtime_id },
        SessionDomainError::Conflict { message } => SessionWireError::Conflict { message },
        SessionDomainError::LeaseRequired { runtime_id } => {
            SessionWireError::LeaseRequired { runtime_id }
        }
        SessionDomainError::LeaseConflict {
            runtime_id,
            holder_client_id,
            holder_pid,
            holder_hostname,
        } => SessionWireError::LeaseConflict {
            runtime_id,
            holder_client_id,
            holder_pid,
            holder_hostname,
        },
        SessionDomainError::UnsupportedCapability { family, capability } => {
            SessionWireError::UnsupportedCapability {
                family: match family {
                    CapabilityFamily::Sandbox => SessionCapabilityFamily::Sandbox,
                    CapabilityFamily::Runtime => SessionCapabilityFamily::Runtime,
                },
                capability,
            }
        }
        SessionDomainError::Timeout {
            operation,
            timeout_ms,
        } => SessionWireError::Timeout {
            operation,
            timeout_ms,
        },
        SessionDomainError::Unavailable { message } => SessionWireError::Unavailable { message },
        SessionDomainError::QuotaExceeded { scope, limit } => {
            SessionWireError::QuotaExceeded { scope, limit }
        }
        SessionDomainError::Internal { message, .. } => SessionWireError::Internal {
            message,
            details: serde_json::Value::Null,
        },
    }
}

/// `pub(crate)`: also used by `application.rs`'s `close`/`detach` control
/// responses, which need the same domain-status -> wire-status mapping
/// without duplicating the match arms.
pub(crate) fn project_status(status: SessionStatus) -> SessionLifecycleStatus {
    match status {
        SessionStatus::Opening => SessionLifecycleStatus::Opening,
        SessionStatus::Idle => SessionLifecycleStatus::Idle,
        SessionStatus::Running => SessionLifecycleStatus::Running,
        SessionStatus::Paused => SessionLifecycleStatus::Paused,
        SessionStatus::Failed => SessionLifecycleStatus::Failed,
        SessionStatus::Closed => SessionLifecycleStatus::Closed,
    }
}

fn project_workspace_access(access: WorkspaceAccess) -> WorkspaceAccessMode {
    match access {
        WorkspaceAccess::ReadOnly => WorkspaceAccessMode::ReadOnly,
        WorkspaceAccess::ReadWrite => WorkspaceAccessMode::ReadWrite,
    }
}

fn project_sandbox_capability(capability: SandboxCapability) -> SessionSandboxCapability {
    match capability {
        SandboxCapability::Exec => SessionSandboxCapability::Exec,
        SandboxCapability::FileRead => SessionSandboxCapability::FileRead,
        SandboxCapability::FileWrite => SessionSandboxCapability::FileWrite,
        SandboxCapability::Pause => SessionSandboxCapability::Pause,
        SandboxCapability::Snapshot => SessionSandboxCapability::Snapshot,
        SandboxCapability::Network => SessionSandboxCapability::Network,
    }
}

fn project_runtime_capability(capability: RuntimeCapability) -> SessionRuntimeCapability {
    match capability {
        RuntimeCapability::Interaction => SessionRuntimeCapability::Interaction,
        RuntimeCapability::Steering => SessionRuntimeCapability::Steering,
        RuntimeCapability::Fork => SessionRuntimeCapability::Fork,
        RuntimeCapability::Checkpoint => SessionRuntimeCapability::Checkpoint,
        RuntimeCapability::StateExport => SessionRuntimeCapability::StateExport,
        RuntimeCapability::ModelOverride => SessionRuntimeCapability::ModelOverride,
        RuntimeCapability::ReasoningControl => SessionRuntimeCapability::ReasoningControl,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn domain_projection_exposes_only_wire_facts() {
        let record = SessionRecord {
            runtime_id: "runtime-1".into(),
            conversation_id: "conversation-1".into(),
            sender_id: "sender-1".into(),
            status: SessionStatus::Idle,
            created_at_ms: 1,
            updated_at_ms: 2,
            workspace: WorkspaceFacts {
                workspace_id: "workspace-1".into(),
                root: "/workspace".into(),
                access: WorkspaceAccess::ReadWrite,
                revision: None,
                metadata: Value::Null,
            },
            isolation: IsolationFacts {
                boundary: IsolationBoundary::Container,
                workspace_access: WorkspaceAccess::ReadWrite,
                network: NetworkIsolation::Restricted,
                metadata: Value::Null,
            },
            capabilities: EffectiveCapabilities {
                sandbox: [SandboxCapability::Exec].into_iter().collect(),
                runtime: [RuntimeCapability::Interaction].into_iter().collect(),
            },
            runtime: OpaqueRuntimeState {
                runtime_kind: "xiaoo".into(),
                schema_version: 4,
                state: serde_json::json!({"private_runtime_state": true}),
            },
            llm: None,
            lease: None,
            lineage: None,
            last_error: None,
            tenant_id: None,
            created_by: "test".to_string(),
        };

        let wire = serde_json::to_value(project_session(&record)).unwrap();
        assert_eq!(wire["runtime_kind"], "xiaoo");
        assert!(wire.get("runtime").is_none());
        assert!(!wire.to_string().contains("private_runtime_state"));
    }
}
