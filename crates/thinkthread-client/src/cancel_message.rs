//! Ordinary Message cancellation is a durable control on an existing turn.
use crate::{root_owner::*, sdk};
use std::sync::Arc;
use xgovernor_core::*;

fn unresolved() -> SessionDomainError {
    SessionDomainError::Unavailable {
        message: "Message cancellation unresolved; original identity and protection retained"
            .into(),
    }
}
fn identity_mismatch() -> SessionDomainError {
    SessionDomainError::Conflict {
        message: "Message control observation identity mismatch".into(),
    }
}
impl RootSessionOwner {
    pub(crate) async fn request_cancel(
        &self,
        context: JointOperationContext,
        session: SessionRecord,
        turn_id: Option<String>,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<(), SessionDomainError> {
        if context.actor_role != Role::Admin
            || context.tenant_id.is_some()
            || context.owner_ref != ADMIN_OWNER_REF
            || context.configuration_revision != self.revision
            || context.runtime_kind != ROOT_RUNTIME_KIND
        {
            return Err(unsupported_joint("cancel_profile"));
        }
        let Some(turn) = repository
            .prepare_joint_cancel(context, session, turn_id)
            .await?
        else {
            return Ok(());
        };
        if turn.terminal.is_some() {
            return Ok(());
        }
        let pending = repository
            .pending_joint_operations()
            .await?
            .into_iter()
            .find(|p| p.admission == turn.admission);
        let Some(pending) = pending else {
            return repository
                .joint_turn_result(turn.admission)
                .await?
                .map(|_| ())
                .ok_or_else(unresolved);
        };
        // A request before send admission is already durable. Its original observer
        // resolves the Message ID and dispatches this control once admission is known.
        let message = pending.native[0]
            .1
            .as_ref()
            .and_then(|o| o.resources.iter().find(|o| o.kind == "message"));
        let Some(message) = message else {
            return Ok(());
        };
        let client = self.connect().await?;
        let result = self
            .resume_cancel(&client, &pending, &message.id, repository.clone())
            .await;
        let _ = client.close().await;
        if result.is_err()
            && repository
                .joint_turn_result(turn.admission)
                .await?
                .is_some()
        {
            return Ok(());
        }
        result
    }

    pub(crate) async fn resume_cancel(
        &self,
        client: &sdk::ThinkThreadClient,
        pending: &JointPendingOperation,
        message_id: &str,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<(), SessionDomainError> {
        let Some(mut control) = repository
            .joint_cancel_control(pending.admission.clone())
            .await?
        else {
            return Ok(());
        };
        let params = serde_json::to_value(sdk::MessageTargetRequest {
            recipient_thinkthread_id: pending.request.native[0].params["recipientThinkthreadId"]
                .as_str()
                .ok_or_else(unresolved)?
                .into(),
            message_id: message_id.into(),
        })
        .map_err(|_| unresolved())?;
        let digest = sdk::operation_digest(client.context(), "message.cancel", &params)
            .map_err(|_| unresolved())?;
        if control.native.is_none() {
            control = repository
                .prepare_joint_cancel_native(
                    pending.admission.clone(),
                    JointNativeIntent {
                        step: "cancel".into(),
                        operation_id: uuid::Uuid::new_v4().to_string(),
                        method: "message.cancel".into(),
                        digest: digest.clone(),
                        params: params.clone(),
                        effect: NativeEffect::Mutate,
                        compensation: false,
                    },
                )
                .await?;
        }
        let native = control.native.ok_or_else(unresolved)?;
        if native.params != params || native.digest != digest || native.method != "message.cancel" {
            return Err(identity_mismatch());
        }
        if control.outcome.as_ref().is_some_and(|o| {
            matches!(
                o.state,
                NativeOperationState::Completed
                    | NativeOperationState::Failed
                    | NativeOperationState::NotSent
            )
        }) {
            return Ok(());
        }
        if repository
            .joint_turn_result(pending.admission.clone())
            .await?
            .is_some()
        {
            return Ok(());
        }
        let (handle, method, state, result) = if repository
            .claim_joint_cancel_send(pending.admission.clone())
            .await?
        {
            match client
                .messages()
                .cancel_with_operation(
                    native.operation_id.clone(),
                    serde_json::from_value(native.params.clone()).map_err(|_| unresolved())?,
                )
                .await
            {
                Ok(reply) => (
                    reply.operation,
                    "message.cancel".to_owned(),
                    NativeOperationState::Completed,
                    serde_json::json!({"message":reply.message}),
                ),
                Err(error) => {
                    if error.delivery() == Some(sdk::Delivery::NotSent) {
                        repository
                            .record_joint_cancel_result(
                                pending.admission.clone(),
                                native,
                                JointNativeOutcome {
                                    state: NativeOperationState::NotSent,
                                    resources: vec![],
                                    payload: serde_json::json!({"delivery":"not_sent"}),
                                },
                            )
                            .await?;
                    }
                    return Err(identity_mismatch());
                }
            }
        } else {
            let view = client
                .get_operation(sdk::OperationGetRequest {
                    scope: sdk::OperationScope::ThinkThread,
                    operation_id: native.operation_id.clone(),
                })
                .await
                .map_err(|error| match error {
                    sdk::ManagementError::Protocol(_) => identity_mismatch(),
                    _ => unresolved(),
                })?;
            let state = match view.state {
                sdk::OperationState::Succeeded => NativeOperationState::Completed,
                sdk::OperationState::Failed => NativeOperationState::Failed,
                sdk::OperationState::NeedsRecovery => NativeOperationState::NeedsRecovery,
                _ => NativeOperationState::Accepted,
            };
            (
                view.operation,
                view.method,
                state,
                view.result.unwrap_or(serde_json::Value::Null),
            )
        };
        if handle.scope != sdk::OperationScope::ThinkThread
            || handle.operation_id != native.operation_id
            || handle.operation_digest != native.digest
            || method != "message.cancel"
        {
            return Err(identity_mismatch());
        }
        if let Some(raw) = result.get("message") {
            let view: sdk::MessageView =
                serde_json::from_value(raw.clone()).map_err(|_| identity_mismatch())?;
            if view.message_id != message_id
                || view.recipient_thinkthread_id != params["recipientThinkthreadId"]
                || view.text != pending.request.native[0].params["text"]
                || view.truncated
            {
                return Err(identity_mismatch());
            }
        } else if state == NativeOperationState::Completed {
            return Err(identity_mismatch());
        }
        repository
            .record_joint_cancel_result(
                pending.admission.clone(),
                native,
                JointNativeOutcome {
                    state,
                    resources: vec![],
                    payload: result,
                },
            )
            .await
    }
}

impl RootSessionOwner {
    /// Pins an observer before the caller issues its own SDK checkpoint request.
    /// It neither sends that mutation nor exposes a public save/load capability.
    pub async fn prepare_checkpoint_boundary_observation(
        &self,
        session: &SessionRecord,
        request: sdk::CheckpointCreateRequest,
        operation_id: String,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<JointCheckpointBoundaryObservation, SessionDomainError> {
        let root = self.bound_root(session, repository.clone()).await?;
        if request.runtime_thinkthread_id != root || request.thinkthread_id != root {
            return Err(unresolved());
        }
        let client = self.connect().await?;
        let params = serde_json::to_value(request).map_err(|_| unresolved())?;
        let digest = sdk::operation_digest(client.context(), "checkpoint.create", &params)
            .map_err(|_| unresolved())?;
        client.close().await.map_err(|_| unresolved())?;
        repository
            .prepare_joint_checkpoint_boundary(
                session
                    .execution_backing
                    .joint()
                    .ok_or_else(unresolved)?
                    .clone(),
                JointNativeIntent {
                    step: "checkpoint_boundary".into(),
                    operation_id,
                    digest,
                    method: "checkpoint.create".into(),
                    params,
                    effect: NativeEffect::Mutate,
                    compensation: false,
                },
            )
            .await
    }

    /// Consumes only the original operation and typed retained checkpoint evidence.
    /// A rejected or unresolved boundary is returned once, without mutation or retry.
    pub async fn observe_checkpoint_boundary(
        &self,
        session: &SessionRecord,
        ticket: JointCheckpointBoundaryObservation,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<bool, SessionDomainError> {
        if session.execution_backing.joint() != Some(&ticket.boundary.binding) {
            return Err(unresolved());
        }
        let root = self.bound_root(session, repository.clone()).await?;
        let client = self.connect().await?;
        let digest =
            sdk::operation_digest(client.context(), "checkpoint.create", &ticket.native.params)
                .map_err(|_| unresolved())?;
        if ticket.native.digest != digest
            || ticket.native.method != "checkpoint.create"
            || ticket.native.params["runtimeThinkthreadId"] != root
            || ticket.native.params["thinkthreadId"] != root
        {
            return Err(unresolved());
        }
        let operation = client
            .get_operation(sdk::OperationGetRequest {
                scope: sdk::OperationScope::ThinkThread,
                operation_id: ticket.native.operation_id.clone(),
            })
            .await
            .map_err(|_| unresolved())?;
        if operation.operation.scope != sdk::OperationScope::ThinkThread
            || operation.operation.operation_id != ticket.native.operation_id
            || operation.operation.operation_digest != digest
            || operation.method != "checkpoint.create"
        {
            return Err(unresolved());
        }
        if operation
            .error
            .as_ref()
            .is_some_and(|e| e.code == "NotAtCheckpointBoundary")
        {
            return Err(SessionDomainError::Unavailable {
                message: "NotAtCheckpointBoundary: workspace is temporarily unavailable".into(),
            });
        }
        if operation.state != sdk::OperationState::Succeeded {
            return Err(unresolved());
        }
        let result = operation.result.ok_or_else(unresolved)?;
        let mut request: sdk::CheckpointRequestView =
            serde_json::from_value(result.get("request").cloned().unwrap_or(result))
                .map_err(|_| unresolved())?;
        if matches!(
            request.state,
            sdk::CheckpointRequestState::Accepted | sdk::CheckpointRequestState::Running
        ) {
            let status = client
                .checkpoints()
                .get_request(sdk::CheckpointRequestGetRequest {
                    runtime_thinkthread_id: root.clone(),
                    request_id: request.request_id.clone(),
                })
                .await
                .map_err(|_| unresolved())?;
            if status.request.request_id != request.request_id {
                return Err(unresolved());
            }
            request = status.request;
        }
        if request
            .error
            .as_ref()
            .is_some_and(|e| e.code == "NotAtCheckpointBoundary")
        {
            return Err(SessionDomainError::Unavailable {
                message: "NotAtCheckpointBoundary: workspace is temporarily unavailable".into(),
            });
        }
        if request.kind != sdk::CheckpointRequestKind::Create
            || request.state != sdk::CheckpointRequestState::Succeeded
        {
            return Err(unresolved());
        }
        let Some(sdk::CheckpointMainResult::Created { checkpoint_id }) = request.result else {
            return Err(unresolved());
        };
        let checkpoint = client
            .checkpoints()
            .get(sdk::CheckpointTargetRequest {
                runtime_thinkthread_id: root.clone(),
                checkpoint_id: checkpoint_id.clone(),
            })
            .await
            .map_err(|_| unresolved())?;
        if checkpoint.id != checkpoint_id
            || checkpoint.source_thinkthread_id != root
            || checkpoint.state != sdk::CheckpointState::Available
            || !checkpoint.source_present
            || checkpoint.created_by_request_id != request.request_id
        {
            return Err(unresolved());
        }
        client.close().await.map_err(|_| unresolved())?;
        repository
            .confirm_joint_checkpoint_boundary(ticket, checkpoint_id)
            .await
    }
}
