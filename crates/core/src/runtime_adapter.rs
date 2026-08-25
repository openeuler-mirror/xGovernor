use crate::WorkspaceFacts;
use crate::{OpaqueRuntimeState, SessionDomainError};
pub use agent_runtime_protocol::{
    RuntimeEntryContext, RuntimeEvent, RuntimeFailure,
    RuntimeInteractionRequest as RuntimeInteractionInput, RuntimeTurnRequest as RuntimeTurnInput,
};
use async_trait::async_trait;
use session_protocol::{LlmOverrideRequest, SessionExtensions, SessionRuntimeCapability};
use std::collections::BTreeSet;
use tokio::sync::mpsc;

pub type RuntimeEventReceiver = mpsc::Receiver<RuntimeEvent>;

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeStartRequest {
    pub runtime_id: String,
    pub conversation_id: String,
    pub sender_id: String,
    pub workspace: WorkspaceFacts,
    pub state: Option<OpaqueRuntimeState>,
    pub llm: Option<LlmOverrideRequest>,
    pub owner_ref: String,
    pub ext: SessionExtensions,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeLoadRequest {
    pub new_runtime_id: String,
    pub owner_ref: String,
    pub provider_snapshot_id: String,
    pub runtime_state: OpaqueRuntimeState,
    pub llm: Option<LlmOverrideRequest>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CheckpointPayload {
    pub checkpoint_id: String,
    pub runtime_state: OpaqueRuntimeState,
    pub provider_snapshot_id: String,
}

/// The single internal seam for every agent runtime.
#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    fn kind(&self) -> &str;
    fn capabilities(&self) -> BTreeSet<SessionRuntimeCapability>;

    /// Capabilities for a particular open request. Runtime implementations
    /// may narrow the process-wide set based on provider/backend selection;
    /// the default preserves the historical process-wide behavior.
    fn capabilities_for_request(
        &self,
        _request: &session_protocol::SessionOpenRequest,
    ) -> BTreeSet<SessionRuntimeCapability> {
        self.capabilities()
    }

    async fn start(&self, request: RuntimeStartRequest) -> Result<(), SessionDomainError>;
    async fn stop(&self, runtime_id: &str) -> Result<(), SessionDomainError>;
    async fn attach(&self, runtime_id: &str) -> Result<(), SessionDomainError>;
    async fn check_alive(&self, _runtime_id: &str) -> Result<bool, SessionDomainError>;
    async fn submit_turn(
        &self,
        input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, SessionDomainError>;
    async fn answer_interaction(
        &self,
        input: RuntimeInteractionInput,
    ) -> Result<(), SessionDomainError>;
    async fn cancel(
        &self,
        runtime_id: &str,
        turn_id: Option<&str>,
    ) -> Result<(), SessionDomainError>;

    async fn checkpoint(&self, _runtime_id: &str) -> Result<CheckpointPayload, SessionDomainError> {
        Err(SessionDomainError::UnsupportedCapability {
            family: crate::CapabilityFamily::Runtime,
            capability: "checkpoint".into(),
        })
    }

    async fn load_from_checkpoint(
        &self,
        _request: RuntimeLoadRequest,
    ) -> Result<(), SessionDomainError> {
        Err(SessionDomainError::UnsupportedCapability {
            family: crate::CapabilityFamily::Runtime,
            capability: "checkpoint".into(),
        })
    }

    async fn delete_checkpoint(
        &self,
        _runtime_state: OpaqueRuntimeState,
        _provider_snapshot_id: String,
    ) -> Result<(), SessionDomainError> {
        Err(SessionDomainError::UnsupportedCapability {
            family: crate::CapabilityFamily::Runtime,
            capability: "checkpoint_delete".into(),
        })
    }

    async fn export_state(
        &self,
        _runtime_id: &str,
    ) -> Result<OpaqueRuntimeState, SessionDomainError> {
        Err(SessionDomainError::UnsupportedCapability {
            family: crate::CapabilityFamily::Runtime,
            capability: "state_export".into(),
        })
    }

    async fn load_state(
        &self,
        _runtime_id: &str,
        _state: OpaqueRuntimeState,
    ) -> Result<(), SessionDomainError> {
        Err(SessionDomainError::UnsupportedCapability {
            family: crate::CapabilityFamily::Runtime,
            capability: "state_export".into(),
        })
    }

    /// Best-effort destruction of whatever `state` still references,
    /// **without** spawning the runtime process itself. `close`'s special
    /// case (`docs/pi_session_restore_plan.md` §1.4) calls this instead of
    /// `stop()` when the adapter has no in-memory instance for `runtime_id`
    /// (a daemon restart happened since the session was last touched) but
    /// `SessionRecord.runtime.state` is non-`Null` — closing is a pure
    /// teardown, so it is not worth paying for a full
    /// spawn-pi-just-to-kill-it round trip through the restoration path.
    /// The default is a no-op success: every adapter that never writes
    /// non-`Null` state into a record (i.e. every adapter that has not
    /// overridden [`Self::export_state`]) is never actually called here, so
    /// there is nothing to override for it to be correct. An adapter that
    /// *does* export state must override this to match, or a restart-then-
    /// close sequence will silently leak whatever that state was pointing
    /// at.
    async fn cleanup_from_state(
        &self,
        _runtime_id: &str,
        _state: &OpaqueRuntimeState,
    ) -> Result<(), SessionDomainError> {
        Ok(())
    }
}

pub fn project_runtime_event(
    runtime_id: &str,
    turn_id: &str,
    event: RuntimeEvent,
) -> session_protocol::SessionEvent {
    use session_protocol::SessionEvent;

    match event {
        RuntimeEvent::OutputDelta {
            stream_id,
            sequence,
            delta,
        } => SessionEvent::OutputDelta {
            runtime_id: runtime_id.into(),
            turn_id: turn_id.into(),
            stream_id,
            sequence,
            delta,
        },
        RuntimeEvent::ToolActivity {
            activity_id,
            phase,
            name,
            status,
            summary,
            ext,
        } => SessionEvent::ToolActivity {
            runtime_id: runtime_id.into(),
            turn_id: turn_id.into(),
            activity_id,
            phase,
            name,
            status,
            summary,
            ext,
        },
        RuntimeEvent::InteractionRequested {
            interaction_id,
            interaction_kind,
            prompt,
            options,
            ext,
        } => SessionEvent::InteractionRequested {
            runtime_id: runtime_id.into(),
            turn_id: turn_id.into(),
            interaction_id,
            interaction_kind,
            prompt,
            // Kept as a temporary wire-compatibility field. Runtime adapters
            // do not model this unused concept and the protocol may remove it.
            sensitive: false,
            options,
            ext,
        },
        RuntimeEvent::Completed { outcome, usage } => SessionEvent::TurnCompleted {
            runtime_id: runtime_id.into(),
            turn_id: turn_id.into(),
            outcome,
            usage,
        },
        RuntimeEvent::Failed { error, usage } => SessionEvent::TurnFailed {
            runtime_id: runtime_id.into(),
            turn_id: turn_id.into(),
            error: session_protocol::SessionTurnFailure {
                code: error.code,
                message: error.message,
                retryable: error.retryable,
                details: error.details,
            },
            usage,
        },
        RuntimeEvent::Extension { namespace, payload } => SessionEvent::Extension {
            runtime_id: runtime_id.into(),
            turn_id: Some(turn_id.into()),
            namespace,
            payload,
        },
        RuntimeEvent::Unknown => SessionEvent::Extension {
            runtime_id: runtime_id.into(),
            turn_id: Some(turn_id.into()),
            namespace: "runtime.unknown".into(),
            payload: serde_json::Value::Null,
        },
    }
}
