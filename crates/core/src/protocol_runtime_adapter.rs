//! Compatibility host for runtimes that implement `agent-runtime-protocol`.
//!
//! The session application still consumes the historical `RuntimeAdapter`
//! trait. This wrapper keeps the conversion in core, so a new runtime can
//! implement the protocol crate without depending on xGovernor domain types.

use crate::{
    CapabilityFamily, CheckpointPayload, OpaqueRuntimeState, RuntimeAdapter, RuntimeEventReceiver,
    RuntimeInteractionInput, RuntimeLoadRequest, RuntimeStartRequest, RuntimeTurnInput,
    SessionDomainError,
};
use agent_runtime_protocol::{
    AgentRuntime, RuntimeCancelRequest, RuntimeCapability, RuntimeCapabilityContext, RuntimeError,
    RuntimeStartRequest as ProtocolStartRequest,
};
use async_trait::async_trait;
use session_protocol::SessionRuntimeCapability;
use std::collections::BTreeSet;
use std::sync::Arc;

pub struct ProtocolRuntimeAdapter {
    inner: Arc<dyn AgentRuntime>,
}

impl ProtocolRuntimeAdapter {
    pub fn new(inner: Arc<dyn AgentRuntime>) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &Arc<dyn AgentRuntime> {
        &self.inner
    }
}

fn capability(capability: RuntimeCapability) -> Option<SessionRuntimeCapability> {
    Some(match capability {
        RuntimeCapability::Interaction => SessionRuntimeCapability::Interaction,
        RuntimeCapability::Steering => SessionRuntimeCapability::Steering,
        RuntimeCapability::StateExport => SessionRuntimeCapability::StateExport,
        RuntimeCapability::ModelOverride => SessionRuntimeCapability::ModelOverride,
        RuntimeCapability::ReasoningControl => SessionRuntimeCapability::ReasoningControl,
        RuntimeCapability::Unknown => return None,
    })
}

fn map_error(error: RuntimeError) -> SessionDomainError {
    match error {
        RuntimeError::InvalidRequest { message, .. } => {
            SessionDomainError::InvalidRequest { message }
        }
        RuntimeError::NotFound { runtime_id } => SessionDomainError::NotFound { runtime_id },
        RuntimeError::Conflict { message, .. } => SessionDomainError::Conflict { message },
        RuntimeError::UnsupportedCapability { capability } => {
            SessionDomainError::UnsupportedCapability {
                family: CapabilityFamily::Runtime,
                capability,
            }
        }
        RuntimeError::WorkerUnavailable { message, .. } => {
            SessionDomainError::Unavailable { message }
        }
        RuntimeError::StateCorrupt { message } => SessionDomainError::InvalidRequest { message },
        RuntimeError::Internal { message } => SessionDomainError::Internal {
            message,
            source: None,
        },
    }
}

#[async_trait]
impl RuntimeAdapter for ProtocolRuntimeAdapter {
    fn kind(&self) -> &str {
        self.inner.runtime_kind()
    }

    fn capabilities(&self) -> BTreeSet<SessionRuntimeCapability> {
        self.inner
            .capabilities()
            .into_iter()
            .filter_map(capability)
            .collect()
    }

    fn capabilities_for_request(
        &self,
        request: &session_protocol::SessionOpenRequest,
    ) -> BTreeSet<SessionRuntimeCapability> {
        self.inner
            .capabilities_for_context(&RuntimeCapabilityContext {
                ext: request.ext.clone(),
            })
            .into_iter()
            .filter_map(capability)
            .collect()
    }

    async fn start(&self, request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
        let RuntimeStartRequest {
            runtime_id,
            conversation_id,
            sender_id,
            workspace,
            state,
            llm,
            ext,
            ..
        } = request;
        self.inner
            .start(ProtocolStartRequest {
                runtime_id,
                conversation_id,
                sender_id,
                workspace,
                state,
                llm,
                ext,
            })
            .await
            .map_err(map_error)
    }

    async fn stop(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
        self.inner.stop(runtime_id).await.map_err(map_error)
    }

    async fn attach(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
        self.inner.attach(runtime_id).await.map_err(map_error)
    }

    async fn check_alive(&self, runtime_id: &str) -> Result<bool, SessionDomainError> {
        self.inner.check_alive(runtime_id).await.map_err(map_error)
    }

    async fn submit_turn(
        &self,
        input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, SessionDomainError> {
        self.inner.submit_turn(input).await.map_err(map_error)
    }

    async fn answer_interaction(
        &self,
        input: RuntimeInteractionInput,
    ) -> Result<(), SessionDomainError> {
        self.inner
            .answer_interaction(input)
            .await
            .map_err(map_error)
    }

    async fn cancel(
        &self,
        runtime_id: &str,
        turn_id: Option<&str>,
    ) -> Result<(), SessionDomainError> {
        self.inner
            .cancel(RuntimeCancelRequest {
                runtime_id: runtime_id.into(),
                turn_id: turn_id.map(str::to_owned),
            })
            .await
            .map_err(map_error)
    }

    async fn checkpoint(&self, runtime_id: &str) -> Result<CheckpointPayload, SessionDomainError> {
        let _ = runtime_id;
        Err(SessionDomainError::UnsupportedCapability {
            family: CapabilityFamily::Runtime,
            capability: "checkpoint".into(),
        })
    }

    async fn load_from_checkpoint(
        &self,
        request: RuntimeLoadRequest,
    ) -> Result<(), SessionDomainError> {
        let _ = request;
        Err(SessionDomainError::UnsupportedCapability {
            family: CapabilityFamily::Runtime,
            capability: "checkpoint".into(),
        })
    }

    async fn delete_checkpoint(
        &self,
        state: OpaqueRuntimeState,
        provider_snapshot_id: String,
    ) -> Result<(), SessionDomainError> {
        let _ = (state, provider_snapshot_id);
        Err(SessionDomainError::UnsupportedCapability {
            family: CapabilityFamily::Runtime,
            capability: "checkpoint_delete".into(),
        })
    }

    async fn export_state(
        &self,
        runtime_id: &str,
    ) -> Result<OpaqueRuntimeState, SessionDomainError> {
        self.inner.export_state(runtime_id).await.map_err(map_error)
    }

    async fn load_state(
        &self,
        runtime_id: &str,
        state: OpaqueRuntimeState,
    ) -> Result<(), SessionDomainError> {
        self.inner
            .load_state(runtime_id, state)
            .await
            .map_err(map_error)
    }

    async fn cleanup_from_state(
        &self,
        runtime_id: &str,
        state: &OpaqueRuntimeState,
    ) -> Result<(), SessionDomainError> {
        let _ = (runtime_id, state);
        Ok(())
    }
}
