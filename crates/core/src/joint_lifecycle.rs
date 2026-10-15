//! Internal lifecycle boundary for resources that must be committed together.
use crate::{
    EffectiveCapabilities, IsolationFacts, NormalizedSessionEnvironment, SecurityContext,
    SessionDomainError, SessionRecord, SessionRepository, WorkspaceFacts,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointBinding {
    pub owner_kind: String,
    pub binding_id: String,
    pub configuration_revision: String,
}

impl JointBinding {
    pub fn validate(&self) -> Result<(), SessionDomainError> {
        if [
            &self.owner_kind,
            &self.binding_id,
            &self.configuration_revision,
        ]
        .iter()
        .any(|value| value.trim().is_empty())
        {
            return Err(SessionDomainError::Unavailable {
                message: "incomplete joint execution binding".into(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionExecutionBacking {
    #[default]
    SplitV1,
    JointV1 {
        binding: JointBinding,
    },
}

impl SessionExecutionBacking {
    pub fn joint(&self) -> Option<&JointBinding> {
        match self {
            Self::SplitV1 => None,
            Self::JointV1 { binding } => Some(binding),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CheckpointBacking {
    SplitV1 {
        provider_snapshot_id: String,
        runtime_state: crate::OpaqueRuntimeState,
    },
    JointV1 {
        binding: JointBinding,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointCheckpointRecord {
    pub checkpoint_id: String,
    pub source_runtime_id: String,
    pub runtime_kind: String,
    pub binding: JointBinding,
    pub workspace: WorkspaceFacts,
    pub isolation: IsolationFacts,
    pub capabilities: EffectiveCapabilities,
    pub tenant_id: Option<String>,
    pub owner_ref: String,
    pub created_by: Option<String>,
    pub created_at_ms: u64,
}

/// Created by the application after authorization. It contains no credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JointOperationContext {
    pub intent_id: String,
    pub operation: String,
    pub tenant_id: Option<String>,
    pub owner_ref: String,
    pub principal: String,
    pub actor_role: crate::Role,
    pub quota: crate::TenantQuota,
    pub runtime_kind: String,
    pub configuration_revision: String,
    pub source_runtime_id: Option<String>,
    pub target_runtime_id: Option<String>,
    pub checkpoint_id: Option<String>,
}

pub struct JointOpenInput {
    pub request: session_protocol::SessionOpenRequest,
    pub normalized: NormalizedSessionEnvironment,
    pub capabilities: EffectiveCapabilities,
}

pub fn unsupported_joint(operation: &str) -> SessionDomainError {
    SessionDomainError::UnsupportedCapability {
        family: crate::CapabilityFamily::Runtime,
        capability: format!("joint_{operation}"),
    }
}

pub type JointTurnFuture = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<crate::JointTurnTerminal, SessionDomainError>>
            + Send,
    >,
>;
pub struct JointTurnSubmission {
    pub turn_id: String,
    /// None replays an existing durable receipt without dispatching another Message.
    pub terminal: Option<JointTurnFuture>,
}

/// Implementations must publish through the supplied durable repository.
/// Returned records are already committed; the application must not save them again.
#[async_trait]
pub trait JointSessionLifecycle: Send + Sync {
    fn owner_kind(&self) -> &str;
    fn configuration_revision(&self) -> &str;

    /// Reconcile the original durable operation. Unknown mutations are queried, never recreated.
    async fn reconcile(
        &self,
        _pending: crate::JointPendingOperation,
        _repository: Arc<dyn SessionRepository>,
    ) -> Result<(), SessionDomainError> {
        Err(unsupported_joint("reconcile"))
    }
    async fn open(
        &self,
        _context: JointOperationContext,
        _input: JointOpenInput,
        _repository: Arc<dyn SessionRepository>,
    ) -> Result<SessionRecord, SessionDomainError> {
        Err(unsupported_joint("open"))
    }
    async fn submit_turn(
        &self,
        _context: JointOperationContext,
        _session: SessionRecord,
        _request: session_protocol::SessionTurnRequest,
        _turn_id: String,
        _repository: Arc<dyn SessionRepository>,
    ) -> Result<JointTurnSubmission, SessionDomainError> {
        Err(unsupported_joint("submit_turn"))
    }
    async fn checkpoint(
        &self,
        _context: JointOperationContext,
        _session: SessionRecord,
        _repository: Arc<dyn SessionRepository>,
    ) -> Result<JointCheckpointRecord, SessionDomainError> {
        Err(unsupported_joint("checkpoint"))
    }
    async fn load(
        &self,
        _context: JointOperationContext,
        _checkpoint: JointCheckpointRecord,
        _request: session_protocol::SessionLoadRequest,
        _repository: Arc<dyn SessionRepository>,
    ) -> Result<SessionRecord, SessionDomainError> {
        Err(unsupported_joint("load"))
    }
    async fn close(
        &self,
        _context: JointOperationContext,
        _session: SessionRecord,
        _note: Option<String>,
        _repository: Arc<dyn SessionRepository>,
    ) -> Result<SessionRecord, SessionDomainError> {
        Err(unsupported_joint("close"))
    }
    async fn ensure_attached(
        &self,
        _session: &SessionRecord,
        _repository: Arc<dyn SessionRepository>,
    ) -> Result<(), SessionDomainError> {
        Err(unsupported_joint("attach"))
    }
    async fn check_alive(
        &self,
        _session: &SessionRecord,
        _repository: Arc<dyn SessionRepository>,
    ) -> Result<bool, SessionDomainError> {
        Err(unsupported_joint("check_alive"))
    }
    async fn delete_checkpoint(
        &self,
        _context: JointOperationContext,
        _checkpoint: JointCheckpointRecord,
        _repository: Arc<dyn SessionRepository>,
    ) -> Result<bool, SessionDomainError> {
        Err(unsupported_joint("delete_checkpoint"))
    }
}

pub(crate) fn validate_committed_session(
    record: &SessionRecord,
    context: &JointOperationContext,
    owner_kind: &str,
) -> Result<(), SessionDomainError> {
    let binding = record
        .execution_backing
        .joint()
        .ok_or_else(|| unsupported_joint("publication"))?;
    binding.validate()?;
    if binding.owner_kind != owner_kind
        || record.runtime.runtime_kind != context.runtime_kind
        || record.runtime_id != context.target_runtime_id.as_deref().unwrap_or("")
        || record.tenant_id != context.tenant_id
        || binding.configuration_revision != context.configuration_revision
    {
        return Err(SessionDomainError::Unavailable {
            message: "joint publication identity mismatch".into(),
        });
    }
    Ok(())
}

pub(crate) fn context(
    ctx: &SecurityContext,
    intent_id: String,
    operation: &str,
    runtime_kind: &str,
    revision: &str,
    source: Option<&str>,
    target: Option<&str>,
    checkpoint: Option<&str>,
) -> JointOperationContext {
    JointOperationContext {
        intent_id,
        operation: operation.into(),
        tenant_id: ctx.tenant_id().map(str::to_owned),
        owner_ref: ctx.owner_ref(),
        principal: ctx.principal.clone(),
        actor_role: ctx.role,
        quota: ctx.quota,
        runtime_kind: runtime_kind.into(),
        configuration_revision: revision.into(),
        source_runtime_id: source.map(str::to_owned),
        target_runtime_id: target.map(str::to_owned),
        checkpoint_id: checkpoint.map(str::to_owned),
    }
}
