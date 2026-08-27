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

    async fn export_state(&self, _runtime_id: &str) -> Result<RuntimeStateSnapshot, RuntimeError> {
        Err(RuntimeError::UnsupportedCapability {
            capability: "state_export".into(),
        })
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
