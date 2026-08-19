use crate::{OpaqueRuntimeState, SessionDomainError, WorkspaceFacts};
use async_trait::async_trait;
use serde_json::Value;
use session_protocol::{
    LlmOverrideRequest, SessionExtensions, SessionInteractionAnswer, SessionRuntimeCapability,
};
use std::collections::BTreeSet;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeStartRequest {
    pub runtime_id: String,
    pub conversation_id: String,
    pub sender_id: String,
    pub workspace: WorkspaceFacts,
    pub state: Option<OpaqueRuntimeState>,
    /// Transient model override. In particular, credentials carried here must
    /// not be copied into domain records or exported runtime state.
    pub llm: Option<LlmOverrideRequest>,
    /// Provider-layer opaque owner bucket key for
    /// `xgovernor_manager::InstanceManager`'s (`crates/manager`) per-owner
    /// and global sandbox caps (`docs/tenancy_design.md` §7 step 4). Mechanically
    /// derived by the application layer from the authenticated
    /// `SecurityContext` via `SecurityContext::owner_ref()` — never read from
    /// `ext` or any other client-supplied input, since a client that could
    /// choose its own `owner_ref` could trivially dodge or pollute another
    /// tenant's quota. Deliberately a first-class field rather than living in
    /// `ext` like `backend_id`: unlike `backend_id` (a runtime-adapter
    /// placement decision), `owner_ref` is a cross-cutting identity fact the
    /// application layer itself must own and no adapter should be trusted to
    /// source independently.
    pub owner_ref: String,
    /// Runtime-specific bootstrap input kept behind a namespace such as
    /// `xiaoo`. The application layer passes it through without interpreting
    /// it. Unlike `owner_ref` above, everything still carried here is a
    /// genuine runtime-adapter placement/bootstrap detail (e.g. `backend_id`)
    /// that has no cross-cutting identity meaning outside that one adapter.
    pub ext: SessionExtensions,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CheckpointPayload {
    pub checkpoint_id: String,
    pub runtime_state: OpaqueRuntimeState,
    pub provider_snapshot_id: String,
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
pub struct RuntimeEntryContext {
    pub kind: Option<String>,
    pub instance_id: Option<String>,
    pub message_id: Option<String>,
    pub reply_to_message_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeTurnInput {
    pub runtime_id: String,
    pub turn_id: String,
    pub text: String,
    /// Runtime-neutral origin and message correlation for this turn. This is
    /// per-turn input and must not be used as the session identity.
    pub entry: RuntimeEntryContext,
    pub llm: Option<LlmOverrideRequest>,
    pub reasoning_effort: Option<String>,
    pub ext: SessionExtensions,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeInteractionInput {
    pub runtime_id: String,
    pub turn_id: String,
    pub interaction_id: String,
    pub answer: SessionInteractionAnswer,
    pub ext: SessionExtensions,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeFailure {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeEvent {
    OutputDelta {
        stream_id: String,
        sequence: u64,
        delta: String,
    },
    ToolActivity {
        activity_id: String,
        phase: session_protocol::SessionToolActivityPhase,
        name: String,
        status: session_protocol::SessionToolActivityStatus,
        summary: Option<String>,
        ext: SessionExtensions,
    },
    InteractionRequested {
        interaction_id: String,
        interaction_kind: String,
        prompt: String,
        options: Vec<session_protocol::SessionInteractionOption>,
        ext: SessionExtensions,
    },
    Completed {
        outcome: session_protocol::SessionTurnOutcome,
        usage: session_protocol::SessionUsage,
    },
    Failed {
        error: RuntimeFailure,
        usage: session_protocol::SessionUsage,
    },
    Extension {
        namespace: String,
        payload: Value,
    },
}

pub type RuntimeEventReceiver = mpsc::Receiver<RuntimeEvent>;

/// The single internal seam for every agent runtime.
#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    fn kind(&self) -> &str;
    fn capabilities(&self) -> BTreeSet<SessionRuntimeCapability>;

    async fn start(&self, request: RuntimeStartRequest) -> Result<(), SessionDomainError>;
    async fn stop(&self, runtime_id: &str) -> Result<(), SessionDomainError>;
    async fn attach(&self, runtime_id: &str) -> Result<(), SessionDomainError>;
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapter_event_projection_preserves_turn_correlation() {
        let event = project_runtime_event(
            "runtime-1",
            "turn-1",
            RuntimeEvent::Failed {
                error: RuntimeFailure {
                    code: "failed".into(),
                    message: "boom".into(),
                    retryable: false,
                    details: Value::Null,
                },
                usage: session_protocol::SessionUsage::default(),
            },
        );
        let wire = serde_json::to_value(event).unwrap();
        assert_eq!(wire["runtime_id"], "runtime-1");
        assert_eq!(wire["turn_id"], "turn-1");
        assert_eq!(wire["kind"], "turn_failed");
    }

    #[test]
    fn interaction_projection_emits_compatibility_sensitive_false() {
        let event = project_runtime_event(
            "runtime-1",
            "turn-1",
            RuntimeEvent::InteractionRequested {
                interaction_id: "interaction-1".into(),
                interaction_kind: "text_input".into(),
                prompt: "input".into(),
                options: Vec::new(),
                ext: Default::default(),
            },
        );
        let wire = serde_json::to_value(event).unwrap();
        assert_eq!(wire["sensitive"], false);
    }
}
