use agent_contracts::tool::{DiscoveredTool, ToolRegistryBuilder, ToolSource};
use agent_types::common::ids::{AgentId, ToolName};
use agent_types::context::TokenBudgetConfig;
use agent_types::interaction::{InteractionRequest, InteractionResponse};
use agent_types::outcome::AgentOutcome;
use agent_types::tool::{ToolRegistryConfig, ToolVisibilityConfig};
use async_trait::async_trait;
use compact::{build_context_manager, CompactionPolicy};
use operation_protocol::capability::exec::ExecRequest;
use provider_protocol::ProviderControlError;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use session_protocol::{SessionToolActivityPhase, SessionToolActivityStatus, SessionUsage};
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use xgovernor_core::{
    enforce_workspace_axiom, IsolationBoundary, IsolationFacts, NetworkIsolation,
    NormalizedSessionEnvironment, OpaqueRuntimeState, ResolvedLlm, RuntimeEvent, SandboxCapability,
    SecurityContext, SessionDomainError, SessionEnvironmentNormalizer, WorkspaceAccess,
};
use xiaoo_api::events::{LoopEndSummary, LoopEventSink, ToolResultEvent};
use xiaoo_api::llm::{resolve_config, resolve_model_context_length, ResolveInput};
use xiaoo_api::runtime::Runtime;
use xiaoo_core::{EmptySkillRegistry, LoopStateSnapshot};

pub mod worker;
pub mod xiaoo_backend;
pub mod xiaoo_runtime;
pub use worker::run_worker_from_env;
use xiaoo_backend::PersistedLlm;
pub use xiaoo_runtime::XiaooRuntime;

pub const EXT_NAMESPACE: &str = "xiaoo";
pub const LOCAL_BACKEND_ID: &str = "local";
pub const E2B_BACKEND_ID: &str = "e2b";
pub const STATE_SCHEMA_VERSION: u32 = 1;
const E2B_WORKSPACE_ROOT: &str = "/home/user/workspace";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct XiaooRuntimeExt {
    backend_id: String,
    provider: String,
    model: String,
    api_key_env: String,
    #[serde(default)]
    api_base: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct XiaooPersistedState {
    backend_id: String,
    owner_ref: String,
    workspace_root: String,
    provider_options: Value,
    llm: PersistedLlm,
    loop_state: LoopStateSnapshot,
}

pub struct XiaooSessionEnvironment {
    default_local_root: String,
    configured_backends: BTreeSet<String>,
}

impl XiaooSessionEnvironment {
    pub fn new(
        default_local_root: impl Into<String>,
        configured_backends: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            default_local_root: default_local_root.into(),
            configured_backends: configured_backends.into_iter().collect(),
        }
    }
}

fn read_ext(
    ext: &session_protocol::SessionExtensions,
) -> Result<XiaooRuntimeExt, SessionDomainError> {
    let value = ext
        .get(EXT_NAMESPACE)
        .ok_or_else(|| SessionDomainError::InvalidRequest {
            message: "missing required 'ext.xiaoo' configuration".into(),
        })?;
    let parsed: XiaooRuntimeExt = serde_json::from_value(value.clone()).map_err(|error| {
        SessionDomainError::InvalidRequest {
            message: format!("invalid 'ext.xiaoo' configuration: {error}"),
        }
    })?;
    for (name, value) in [
        ("backend_id", parsed.backend_id.as_str()),
        ("provider", parsed.provider.as_str()),
        ("model", parsed.model.as_str()),
        ("api_key_env", parsed.api_key_env.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(SessionDomainError::InvalidRequest {
                message: format!("'ext.xiaoo.{name}' must not be empty"),
            });
        }
    }
    // Resolve now so open fails before a sandbox is provisioned. Only the
    // environment variable name is retained after this point.
    std::env::var(&parsed.api_key_env)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| SessionDomainError::InvalidRequest {
            message: format!(
                "environment variable '{}' required by xiaoO is missing or empty",
                parsed.api_key_env
            ),
        })?;
    resolve_config(ResolveInput {
        provider: Some(parsed.provider.clone()),
        api_key_env: Some(parsed.api_key_env.clone()),
        base_url: parsed.api_base.clone(),
        ..Default::default()
    })
    .map_err(|error| SessionDomainError::InvalidRequest {
        message: format!("invalid xiaoO LLM configuration: {error}"),
    })?;
    Ok(parsed)
}

#[async_trait]
impl SessionEnvironmentNormalizer for XiaooSessionEnvironment {
    async fn normalize(
        &self,
        ctx: &SecurityContext,
        request: &session_protocol::SessionOpenRequest,
    ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
        if request
            .llm
            .as_ref()
            .and_then(|llm| llm.api_key.as_ref())
            .is_some()
        {
            return Err(SessionDomainError::InvalidRequest {
                message: "xiaoO rejects inline api_key; use ext.xiaoo.api_key_env".into(),
            });
        }
        let ext = read_ext(&request.ext)?;
        if !self.configured_backends.contains(&ext.backend_id) {
            return Err(SessionDomainError::InvalidRequest {
                message: format!("xiaoo backend_id '{}' is not configured", ext.backend_id),
            });
        }
        let sandboxed = ext.backend_id == E2B_BACKEND_ID;
        enforce_workspace_axiom(ctx, &request.workspace, sandboxed)?;
        let (root, boundary, network, metadata) =
            match (&request.workspace, ext.backend_id.as_str()) {
                (session_protocol::WorkspaceSpec::DaemonDefault, LOCAL_BACKEND_ID) => (
                    self.default_local_root.clone(),
                    IsolationBoundary::Host,
                    NetworkIsolation::None,
                    Value::Null,
                ),
                (session_protocol::WorkspaceSpec::LocalPath { path }, LOCAL_BACKEND_ID) => (
                    path.clone(),
                    IsolationBoundary::Host,
                    NetworkIsolation::None,
                    Value::Null,
                ),
                (session_protocol::WorkspaceSpec::DaemonDefault, E2B_BACKEND_ID) => (
                    E2B_WORKSPACE_ROOT.into(),
                    IsolationBoundary::Remote,
                    NetworkIsolation::Restricted,
                    Value::Null,
                ),
                (
                    session_protocol::WorkspaceSpec::Git {
                        url,
                        reference,
                        subdirectory,
                    },
                    E2B_BACKEND_ID,
                ) => (
                    E2B_WORKSPACE_ROOT.into(),
                    IsolationBoundary::Remote,
                    NetworkIsolation::Restricted,
                    json!({"url": url, "reference": reference, "subdirectory": subdirectory}),
                ),
                (workspace, backend_id) => {
                    return Err(SessionDomainError::InvalidRequest {
                        message: format!(
                        "workspace {workspace:?} is not supported by xiaoO backend '{backend_id}'"
                    ),
                    })
                }
            };
        let mut sandbox = BTreeSet::from([
            SandboxCapability::Exec,
            SandboxCapability::FileRead,
            SandboxCapability::FileWrite,
        ]);
        if ext.backend_id == E2B_BACKEND_ID {
            sandbox.extend([SandboxCapability::Snapshot, SandboxCapability::Network]);
        }
        Ok(NormalizedSessionEnvironment {
            workspace: xgovernor_core::WorkspaceFacts {
                workspace_id: format!("xiaoo:{}", ext.backend_id),
                root,
                access: WorkspaceAccess::ReadWrite,
                revision: None,
                metadata,
            },
            isolation: IsolationFacts {
                boundary,
                workspace_access: WorkspaceAccess::ReadWrite,
                network,
                metadata: json!({"backend_id": ext.backend_id}),
            },
            sandbox_capabilities: sandbox,
            llm: Some(ResolvedLlm {
                provider: ext.provider,
                model: ext.model,
                api_base: ext.api_base,
                credential_source: format!("env:{}", ext.api_key_env),
            }),
            lease: None,
        })
    }
}

pub(crate) fn map_provider_error(error: ProviderControlError) -> SessionDomainError {
    match error {
        ProviderControlError::NotFound { resource_ref } => SessionDomainError::NotFound {
            runtime_id: resource_ref,
        },
        ProviderControlError::InvalidRequest { message } => {
            SessionDomainError::InvalidRequest { message }
        }
        other => SessionDomainError::Unavailable {
            message: other.to_string(),
        },
    }
}

struct CoreToolSource {
    discovered: Vec<DiscoveredTool>,
}

impl CoreToolSource {
    fn new() -> Self {
        const ALLOWED: &[&str] = &[
            "bash",
            "file_read",
            "file_write",
            "file_edit",
            "glob",
            "grep",
            "ask_user_question",
        ];
        let discovered = tool::load_tool_sources_with_services(tool::ToolRuntimeServices {
            disable_plugin_tools: true,
            ..Default::default()
        })
        .into_iter()
        .flat_map(|source| source.discover())
        .filter(|tool| ALLOWED.contains(&tool.spec.name().0.as_str()))
        .collect();
        Self { discovered }
    }
}

impl ToolSource for CoreToolSource {
    fn discover(&self) -> Vec<DiscoveredTool> {
        self.discovered
            .iter()
            .map(|tool| DiscoveredTool {
                spec: Arc::clone(&tool.spec),
                executor: Arc::clone(&tool.executor),
            })
            .collect()
    }
}

pub(crate) fn cancelled_interaction_response(request: &InteractionRequest) -> InteractionResponse {
    match request {
        InteractionRequest::Confirm { .. } => InteractionResponse::Confirmed { allowed: false },
        InteractionRequest::TextInput { .. } => InteractionResponse::Text {
            value: None,
            display_value: None,
        },
        InteractionRequest::Choice { .. } => InteractionResponse::Choice { value: None },
    }
}

async fn clone_git_workspace(
    backend: &dyn operation_protocol::OperationBackend,
    metadata: &Value,
    workspace_root: &str,
) -> Result<(), SessionDomainError> {
    let url = metadata.get("url").and_then(Value::as_str).ok_or_else(|| {
        SessionDomainError::InvalidRequest {
            message: "xiaoO git workspace metadata has no url".into(),
        }
    })?;
    let mut args = vec!["clone".to_string()];
    if let Some(reference) = metadata.get("reference").and_then(Value::as_str) {
        args.extend(["--branch".to_string(), reference.to_string()]);
    }
    args.extend([url.to_string(), workspace_root.to_string()]);
    let result = backend
        .exec()
        .exec(ExecRequest {
            command: "git".into(),
            args,
            timeout_ms: Some(120_000),
            ..Default::default()
        })
        .await
        .map_err(|error| SessionDomainError::Unavailable {
            message: format!("xiaoO git clone failed: {error}"),
        })?;
    if result.exit_code != Some(0) {
        return Err(SessionDomainError::Unavailable {
            message: format!(
                "xiaoO git clone exited {:?}: {}",
                result.exit_code,
                String::from_utf8_lossy(&result.stderr)
            ),
        });
    }
    Ok(())
}

fn state_to_opaque(state: &XiaooPersistedState) -> Result<OpaqueRuntimeState, SessionDomainError> {
    Ok(OpaqueRuntimeState {
        runtime_kind: "xiaoo".into(),
        schema_version: STATE_SCHEMA_VERSION,
        state: serde_json::to_value(state).map_err(|error| SessionDomainError::Internal {
            message: format!("failed to serialize xiaoO runtime state: {error}"),
            source: None,
        })?,
    })
}

fn state_from_opaque(
    state: &OpaqueRuntimeState,
) -> Result<XiaooPersistedState, SessionDomainError> {
    if state.runtime_kind != "xiaoo" || state.schema_version != STATE_SCHEMA_VERSION {
        return Err(SessionDomainError::InvalidRequest {
            message: format!(
                "unsupported xiaoO state kind/version: {}/{}",
                state.runtime_kind, state.schema_version
            ),
        });
    }
    serde_json::from_value(state.state.clone()).map_err(|error| {
        SessionDomainError::InvalidRequest {
            message: format!("invalid xiaoO runtime state: {error}"),
        }
    })
}

fn validate_persisted_llm(llm: &PersistedLlm) -> Result<(), SessionDomainError> {
    std::env::var(&llm.api_key_env)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| SessionDomainError::InvalidRequest {
            message: format!(
                "environment variable '{}' required by xiaoO is missing or empty",
                llm.api_key_env
            ),
        })?;
    resolve_config(ResolveInput {
        provider: Some(llm.provider.clone()),
        api_key_env: Some(llm.api_key_env.clone()),
        base_url: llm.api_base.clone(),
        ..Default::default()
    })
    .map(|_| ())
    .map_err(|error| SessionDomainError::InvalidRequest {
        message: format!("invalid persisted xiaoO LLM configuration: {error}"),
    })
}

pub(crate) async fn build_runtime(
    llm: &PersistedLlm,
    model_override: Option<&str>,
    backend: Arc<dyn xiaoo_api::backend::OperationBackend>,
) -> Result<Runtime, SessionDomainError> {
    let model = model_override.unwrap_or(&llm.model);
    let resolved = resolve_config(ResolveInput {
        provider: Some(llm.provider.clone()),
        api_key_env: Some(llm.api_key_env.clone()),
        base_url: llm.api_base.clone(),
        ..Default::default()
    })
    .map_err(|error| SessionDomainError::InvalidRequest {
        message: error.to_string(),
    })?;
    let context = resolve_model_context_length(&resolved, model)
        .await
        .map_err(|error| SessionDomainError::Unavailable {
            message: error.to_string(),
        })?
        .unwrap_or(128_000) as usize;
    let budget = TokenBudgetConfig {
        total_budget: context,
        reserved_for_output: context / 10,
        reserved_for_system: context / 20,
        hard_limit_ratio: 0.9,
    };
    let provider = Arc::new(
        llm_client::create_llm_provider_from_resolved(
            &resolved,
            model.to_string(),
            Some("xgovernor".into()),
            None,
        )
        .map_err(|error| SessionDomainError::InvalidRequest {
            message: error.to_string(),
        })?,
    );
    let compression = build_context_manager(None, Arc::clone(&provider)).map_err(|error| {
        SessionDomainError::Internal {
            message: error.to_string(),
            source: None,
        }
    })?;
    let anonymous = AgentId("anonymous".into());
    let allowed = [
        "bash",
        "file_read",
        "file_write",
        "file_edit",
        "glob",
        "grep",
        "ask_user_question",
    ]
    .into_iter()
    .map(|name| ToolName(name.into()))
    .collect();
    let registry = tool::ToolRegistryBuilderImpl::new()
        .with_sources(vec![Box::new(CoreToolSource::new())])
        .with_config(ToolRegistryConfig {
            visibility: ToolVisibilityConfig {
                per_agent_allowed_tools: [(anonymous, allowed)].into_iter().collect(),
            },
        })
        .build()
        .map_err(|error| SessionDomainError::Internal {
            message: format!("failed to build xiaoO core tool registry: {error}"),
            source: None,
        })?;
    Runtime::builder()
        .llm_provider(provider)
        .compression_pipeline(compression)
        .prompt_builder(Arc::new(prompt::PromptBuilderImpl::new()))
        .system_prompt("You are xiaoO, a coding assistant managed by xGovernor.")
        .tool_registry(Arc::from(registry))
        .skill_registry(Arc::new(EmptySkillRegistry::new()))
        .operation_backend(backend)
        .token_budget_config(budget.clone())
        .token_budget_policy(Arc::new(CompactionPolicy::from_budget(&budget)))
        .build()
        .map_err(|error| SessionDomainError::Internal {
            message: error.to_string(),
            source: None,
        })
}

struct GovernorEventSink {
    tx: mpsc::UnboundedSender<RuntimeEvent>,
    output: std::sync::Mutex<(String, u64)>,
    reasoning: std::sync::Mutex<String>,
}

impl GovernorEventSink {
    fn new(tx: mpsc::UnboundedSender<RuntimeEvent>) -> Self {
        Self {
            tx,
            output: std::sync::Mutex::new((String::new(), 0)),
            reasoning: std::sync::Mutex::new(String::new()),
        }
    }

    fn try_send(&self, event: RuntimeEvent) {
        let _ = self.tx.send(event);
    }
}

impl LoopEventSink for GovernorEventSink {
    fn on_turn_start(&self, _agent_id: &agent_types::common::ids::AgentId, _turn: u32) {}

    fn on_assistant_message(&self, _agent_id: &agent_types::common::ids::AgentId, text: &str) {
        let mut output = self.output.lock().expect("output snapshot lock poisoned");
        let delta = text.strip_prefix(&output.0).unwrap_or(text).to_string();
        output.0 = text.to_string();
        if !delta.is_empty() {
            output.1 += 1;
            self.try_send(RuntimeEvent::OutputDelta {
                stream_id: "assistant".into(),
                sequence: output.1,
                delta,
            });
        }
    }

    fn on_assistant_reasoning(&self, _agent_id: &agent_types::common::ids::AgentId, text: &str) {
        let mut previous = self
            .reasoning
            .lock()
            .expect("reasoning snapshot lock poisoned");
        let delta = text
            .strip_prefix(previous.as_str())
            .unwrap_or(text)
            .to_string();
        *previous = text.to_string();
        if !delta.is_empty() {
            self.try_send(RuntimeEvent::Extension {
                namespace: "xiaoo".into(),
                payload: json!({"kind": "reasoning_delta", "delta": delta}),
            });
        }
    }

    fn on_tool_result(
        &self,
        _agent_id: &agent_types::common::ids::AgentId,
        event: &ToolResultEvent,
    ) {
        self.try_send(RuntimeEvent::ToolActivity {
            activity_id: event.call_id.clone(),
            phase: SessionToolActivityPhase::End,
            name: event.tool_name.clone(),
            status: if event.is_error {
                SessionToolActivityStatus::Failed
            } else {
                SessionToolActivityStatus::Succeeded
            },
            summary: Some(event.output_preview.clone()),
            ext: [("xiaoo".into(), json!({"args_preview": event.args_preview}))]
                .into_iter()
                .collect(),
        });
    }

    fn on_loop_end(
        &self,
        _agent_id: &agent_types::common::ids::AgentId,
        _summary: &LoopEndSummary,
    ) {
    }
}

pub(crate) fn usage_from_outcome(outcome: &AgentOutcome) -> SessionUsage {
    let usage = match outcome {
        AgentOutcome::Complete { token_usage, .. }
        | AgentOutcome::MaxTurnsReached { token_usage, .. }
        | AgentOutcome::BudgetExhausted { token_usage, .. }
        | AgentOutcome::Cancelled { token_usage, .. } => token_usage,
    };
    SessionUsage {
        input_tokens: usage.prompt_tokens as u64,
        output_tokens: usage.completion_tokens as u64,
        total_tokens: usage.total_tokens as u64,
    }
}
