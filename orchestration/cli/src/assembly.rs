//! Native backend assembly. Admission and worker instances use backend declarations.
use std::{collections::HashMap, path::Path, sync::Arc};

use backend::{e2b::E2bProvider, SqliteProviderInstanceLedger};
use orchestration_backend_own::OwnBackend;
use orchestration_contract::{execution::*, run::BackendKind};
use orchestration_run_manager::{config::*, Result};
use serde_json::json;
use session_protocol::{LlmOverrideRequest, SessionOpenRequest};
use xgovernor_core::*;
use xgovernor_manager::{InstanceManager, InstanceManagerConfig};
use xgovernor_runtime_opencode::OpenCodeRuntime;

pub fn assemble(
    descriptor: &BackendDescriptor,
    config: &Configuration,
    directory: &Path,
    context: CallContext,
    policy: NetworkPolicy,
    resolve_files: bool,
) -> Result<Arc<dyn ExecutionBackend>> {
    if descriptor.driver != "own" || descriptor.kind != BackendKind::E2b {
        return Err("backend kind/driver is not supported by this distribution".into());
    }
    let e2b = descriptor
        .e2b
        .as_ref()
        .ok_or("own E2B connection configuration is required")?;
    let model = config
        .models
        .get(&e2b.model)
        .ok_or("configured E2B model is missing")?;
    if !e2b.opencode_executable.is_absolute() || e2b.timeout_secs == 0 {
        return Err("OpenCode executable must be absolute and E2B timeout must be positive".into());
    }
    // Environment references stay references; platform/Agent resolve them only
    // when opening an execution instance, never in the algorithm process.
    let llm = LlmOverrideRequest {
        provider: Some(model.provider.clone()),
        model: Some(model.model.clone()),
        api_base: Some(model.api_base.clone()),
        api_key_env: Some(credential_environment(
            &model.credential_ref,
            "MCTS_OWN_MODEL_KEY",
            resolve_files,
        )?),
        ..Default::default()
    };
    let environment = Arc::new(Environment {
        options: json!({"api_base":e2b.api_url,"domain":e2b.domain,"envd_scheme":e2b.envd_scheme,
            "api_key_env":credential_environment(&e2b.credential_ref, "MCTS_OWN_E2B_KEY", resolve_files)?,"timeout_secs":e2b.timeout_secs,"workspace_root":"/testbed"}),
        model: llm.clone(),
        policy,
    });
    let repository = Arc::new(SqliteSessionRepository::open(
        &directory.join("platform.sqlite"),
    )?);
    let provider = Arc::new(E2bProvider::new());
    let manager = Arc::new(InstanceManager::new(
        provider.clone(),
        provider,
        Arc::new(SqliteProviderInstanceLedger::open(
            &directory.join("platform.sqlite"),
        )?),
        provider_protocol::ProviderKind("e2b".into()),
        InstanceManagerConfig::new(6, 6),
    ));
    let leases = Arc::new(SessionLeaseTable::default());
    let app = Arc::new(
        SessionApplication::new(
            Arc::new(OpenCodeRuntime::new(&e2b.opencode_executable)),
            HashMap::from([("e2b".into(), manager.clone())]),
            repository.clone(),
            Arc::new(Ids),
            Arc::new(Ids),
            environment,
            Arc::new(Ids),
        )
        .with_lease_table(leases.clone()),
    );
    let lease = serde_json::from_value(json!({"client_id":context.run_id}))?;
    let own = OwnBackend::open(
        &directory.join("backend.sqlite"),
        app.clone(),
        manager.clone(),
        repository,
        SecurityContext::admin("mcts-local-user"),
        context,
        grants(),
        policy,
        Some(llm),
        Default::default(),
        lease,
    )?;
    for (image, template) in &e2b.image_templates {
        own.register_image_template(image, template)?;
    }
    if resolve_files {
        manager.spawn_retry_loop();
        spawn_orphan_reaper((*app).clone(), leases);
        spawn_reclaim_sweeper((*app).clone());
    }
    Ok(Arc::new(own))
}

// Grants cover the assembled backend's host and algorithm execution operations.
fn grants() -> std::collections::BTreeSet<Capability> {
    [
        Capability::PrepareRoot,
        Capability::RenewRunLease,
        Capability::ReclaimRun,
        Capability::ExportWorkspace,
        Capability::Snapshot,
        Capability::Fork,
        Capability::AgentTurn,
        Capability::CancelJob,
        Capability::Inspect,
        Capability::Exec,
        Capability::FileRead,
        Capability::FileWrite,
        Capability::Diff,
        Capability::Release,
    ]
    .into()
}

fn credential_environment(
    reference: &str,
    private_name: &str,
    resolve_files: bool,
) -> Result<String> {
    if let Some(file) = reference.strip_prefix("file:") {
        if !Path::new(file).is_absolute() {
            return Err("credential file path must be absolute".into());
        }
        if resolve_files {
            let value =
                std::fs::read_to_string(file).map_err(|_| "credential file is unavailable")?;
            let value = value.trim();
            if value.is_empty() {
                return Err("credential file is empty".into());
            }
            std::env::set_var(private_name, value);
        }
        return Ok(private_name.into());
    }
    let name = reference
        .strip_prefix("env:")
        .ok_or("credential source must be env:NAME or file:/absolute/path")?;
    if name.is_empty()
        || !name
            .bytes()
            .enumerate()
            .all(|(i, c)| c.is_ascii_alphabetic() || c == b'_' || (i > 0 && c.is_ascii_digit()))
    {
        return Err("invalid credential environment reference".into());
    }
    Ok(name.into())
}

struct Ids;
impl TurnIdGenerator for Ids {
    fn next_turn_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
}
impl RuntimeIdGenerator for Ids {
    fn next_runtime_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
}
impl Clock for Ids {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

struct Environment {
    options: serde_json::Value,
    model: LlmOverrideRequest,
    policy: NetworkPolicy,
}
#[async_trait::async_trait]
impl SessionEnvironmentNormalizer for Environment {
    async fn normalize(
        &self,
        ctx: &SecurityContext,
        request: &SessionOpenRequest,
    ) -> std::result::Result<NormalizedSessionEnvironment, SessionDomainError> {
        enforce_workspace_axiom(ctx, &request.workspace, true)?;
        let invalid = || SessionDomainError::InvalidRequest {
            message: "own root provisioning requires backend support".into(),
        };
        let root = match &request.workspace {
            session_protocol::WorkspaceSpec::DaemonDefault => "/testbed".to_string(),
            session_protocol::WorkspaceSpec::Git { .. } => request.deployment.options
                ["provider_options"]["workspace_root"]
                .as_str()
                .filter(|root| std::path::Path::new(root).is_absolute())
                .ok_or_else(invalid)?
                .to_owned(),
            _ => return Err(invalid()),
        };
        let mut options = self.options.clone();
        if let Some(supplied) = request.deployment.options.get("provider_options") {
            let supplied = supplied.as_object().ok_or_else(invalid)?;
            for (key, value) in supplied {
                match key.as_str() {
                    "workspace_root" | "template_id" => {
                        let text = value
                            .as_str()
                            .filter(|v| !v.is_empty())
                            .ok_or_else(invalid)?;
                        options[key] = json!(text);
                    }
                    // OwnBackend includes this assertion in its root request.
                    // It may confirm, but can never override, the recorded policy.
                    "allow_internet_access"
                        if value.as_bool() == Some(self.policy == NetworkPolicy::AllowInternet) => {
                    }
                    _ => {
                        return Err(SessionDomainError::InvalidRequest {
                            message: format!("provider option is not permitted: {key}"),
                        })
                    }
                }
            }
        }
        options["allow_internet_access"] = json!(self.policy == NetworkPolicy::AllowInternet);
        Ok(NormalizedSessionEnvironment {
            workspace: WorkspaceFacts {
                workspace_id: request.conversation_id.clone(),
                root,
                access: WorkspaceAccess::ReadWrite,
                revision: None,
                metadata: serde_json::Value::Null,
            },
            isolation: IsolationFacts {
                boundary: IsolationBoundary::Remote,
                workspace_access: WorkspaceAccess::ReadWrite,
                network: if self.policy == NetworkPolicy::Offline {
                    NetworkIsolation::Isolated
                } else {
                    NetworkIsolation::None
                },
                metadata: json!({"backend_id":"e2b","provider_options":options}),
            },
            sandbox_capabilities: [
                SandboxCapability::Exec,
                SandboxCapability::FileRead,
                SandboxCapability::FileWrite,
                SandboxCapability::Snapshot,
                SandboxCapability::Network,
            ]
            .into(),
            llm: Some(ResolvedLlm {
                provider: self.model.provider.clone().unwrap(),
                model: self.model.model.clone().unwrap(),
                api_base: self.model.api_base.clone(),
                api: None,
                capabilities: None,
                credential_source: format!("env:{}", self.model.api_key_env.as_ref().unwrap()),
            }),
            lease: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn root_options_cannot_override_host_connection_or_network_policy() {
        let environment = Environment {
            options: json!({"api_base":"https://trusted.invalid"}),
            model: LlmOverrideRequest {
                provider: Some("provider".into()),
                model: Some("model".into()),
                api_key_env: Some("KEY".into()),
                ..Default::default()
            },
            policy: NetworkPolicy::Offline,
        };
        let mut request: SessionOpenRequest = serde_json::from_value(json!({
            "conversation_id":"root", "sender_id":"host", "workspace":{"kind":"daemon_default"}
        }))
        .unwrap();
        request.deployment.options = json!({"provider_options":{"template_id":"template","workspace_root":"/testbed","allow_internet_access":false}});
        let ctx = SecurityContext::admin("host");
        let resolved = environment.normalize(&ctx, &request).await.unwrap();
        assert_eq!(
            resolved.isolation.metadata["provider_options"]["allow_internet_access"],
            false
        );
        request.deployment.options["provider_options"]["api_base"] =
            json!("https://untrusted.invalid");
        assert!(environment.normalize(&ctx, &request).await.is_err());
        request.deployment.options["provider_options"]
            .as_object_mut()
            .unwrap()
            .remove("api_base");
        request.deployment.options["provider_options"]["allow_internet_access"] = json!(true);
        assert!(environment.normalize(&ctx, &request).await.is_err());
    }
}
