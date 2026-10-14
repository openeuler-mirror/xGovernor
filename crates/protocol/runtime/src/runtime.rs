use crate::{
    RuntimeCancelRequest, RuntimeCapability, RuntimeCapabilityContext, RuntimeError, RuntimeEvent,
    RuntimeInteractionRequest, RuntimeStartRequest, RuntimeStateSnapshot, RuntimeTurnRequest,
};
use async_trait::async_trait;
use operation_protocol::OperationBackend;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::mpsc;

pub type RuntimeEventReceiver = mpsc::Receiver<RuntimeEvent>;

#[derive(Clone)]
pub struct RuntimeExecutionContext {
    pub operation_backend: Arc<dyn OperationBackend>,
}

/// Runtime implementation seam independent of xGovernor's session/domain
/// crate. A host may adapt this trait to its own persistence and wire model.
#[async_trait]
pub trait AgentRuntime: Send + Sync {
    fn runtime_kind(&self) -> &str;
    fn capabilities(&self) -> BTreeSet<RuntimeCapability>;

    /// Validate optional model features before provider-side effects. Runtimes
    /// must opt in rather than silently ignore protocol/capability declarations.
    fn validate_llm_config(
        &self,
        config: Option<&session_protocol::LlmOverrideRequest>,
    ) -> Result<(), RuntimeError> {
        if config.is_some_and(|config| config.api.is_some() || config.capabilities.is_some()) {
            return Err(RuntimeError::InvalidRequest {
                code: "unsupported_llm_configuration".into(),
                message: "This runtime does not support model protocol or capability overrides"
                    .into(),
            });
        }
        Ok(())
    }

    fn capabilities_for_context(
        &self,
        _context: &RuntimeCapabilityContext,
    ) -> BTreeSet<RuntimeCapability> {
        self.capabilities()
    }

    async fn start(
        &self,
        request: RuntimeStartRequest,
        execution_context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError>;
    async fn attach(
        &self,
        runtime_id: &str,
        execution_context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError>;
    async fn stop(&self, runtime_id: &str) -> Result<(), RuntimeError>;
    async fn check_alive(&self, runtime_id: &str) -> Result<bool, RuntimeError>;
    async fn submit_turn(
        &self,
        request: RuntimeTurnRequest,
    ) -> Result<RuntimeEventReceiver, RuntimeError>;
    async fn answer_interaction(
        &self,
        request: RuntimeInteractionRequest,
    ) -> Result<(), RuntimeError>;
    async fn cancel(&self, request: RuntimeCancelRequest) -> Result<(), RuntimeError>;

    /// Open-time LLM connectivity probe. Called **before** the sandbox is
    /// provisioned, so a failure aborts the session open without any
    /// runtime-side side effects. The default is a no-op for runtimes that
    /// do not configure an LLM endpoint.
    async fn probe_llm(&self, _request: &RuntimeStartRequest) -> Result<(), RuntimeError> {
        Ok(())
    }

    /// Credential-free model configuration confirmed by the running adapter.
    /// None means this runtime does not supply model metadata.
    async fn describe_llm(
        &self,
        _runtime_id: &str,
    ) -> Result<Option<session_protocol::ResolvedLlmDescriptor>, RuntimeError> {
        Ok(None)
    }

    async fn export_state(&self, _runtime_id: &str) -> Result<RuntimeStateSnapshot, RuntimeError> {
        Err(RuntimeError::UnsupportedCapability {
            capability: "state_export".into(),
        })
    }

    /// Immutable export for branches; durable restart state may reference live files.
    async fn export_checkpoint_state(
        &self,
        runtime_id: &str,
    ) -> Result<RuntimeStateSnapshot, RuntimeError> {
        self.export_state(runtime_id).await
    }

    /// Release private resources owned by an immutable checkpoint export.
    async fn delete_checkpoint_state(
        &self,
        _state: &RuntimeStateSnapshot,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn load_state(
        &self,
        _runtime_id: &str,
        _state: RuntimeStateSnapshot,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::UnsupportedCapability {
            capability: "state_export".into(),
        })
    }
}
