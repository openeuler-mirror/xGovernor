//! Runtime registration and E2B workspace admission for orchestration hosts.
use std::{collections::HashMap, sync::Arc};
use xgovernor_core::*;
use xgovernor_manager::InstanceManager;
use xgovernor_runtime_opencode::OpenCodeRuntime;

/// The core coordinator supplies each runtime with the manager's per-instance
/// operation backend. OpenCode fences that binding by session and turn generation.
pub fn opencode_registration(
    executable: impl Into<std::path::PathBuf>,
    manager: Arc<InstanceManager>,
) -> RuntimeRegistration {
    RuntimeRegistration::new(
        Arc::new(OpenCodeRuntime::new(executable)),
        HashMap::from([("e2b".into(), manager)]),
        Arc::new(E2bEnvironment),
    )
}

/// E2B admission shared by host-selected Agent runtimes. Git materialization is
/// performed by prepare_root against the immutable template's workspace.
pub struct E2bEnvironment;
#[async_trait::async_trait]
impl SessionEnvironmentNormalizer for E2bEnvironment {
    async fn normalize(
        &self,
        ctx: &SecurityContext,
        request: &session_protocol::SessionOpenRequest,
    ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
        enforce_workspace_axiom(ctx, &request.workspace, true)?;
        let root = request.deployment.options["provider_options"]["workspace_root"]
            .as_str()
            .unwrap_or("/home/user/workspace");
        if !std::path::Path::new(root).is_absolute()
            || std::path::Path::new(root)
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(SessionDomainError::InvalidRequest {
                message: "absolute normalized workspace required".into(),
            });
        }
        let offline =
            request.deployment.options["provider_options"]["allow_internet_access"] == false;
        Ok(NormalizedSessionEnvironment {
            workspace: WorkspaceFacts {
                workspace_id: request.conversation_id.clone(),
                root: root.into(),
                access: WorkspaceAccess::ReadWrite,
                revision: None,
                metadata: serde_json::Value::Null,
            },
            isolation: IsolationFacts {
                boundary: IsolationBoundary::Remote,
                workspace_access: WorkspaceAccess::ReadWrite,
                network: if offline {
                    NetworkIsolation::Isolated
                } else {
                    NetworkIsolation::None
                },
                metadata: serde_json::json!({"backend_id":"e2b"}),
            },
            sandbox_capabilities: [
                SandboxCapability::Exec,
                SandboxCapability::FileRead,
                SandboxCapability::FileWrite,
                SandboxCapability::Snapshot,
            ]
            .into(),
            llm: None,
            lease: None,
        })
    }
}
