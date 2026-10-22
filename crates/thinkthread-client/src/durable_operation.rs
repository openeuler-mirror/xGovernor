//! Durable sending and exact-identity recovery for a bounded Management operation set.
use crate::sdk;
use serde_json::{json, Value};
use std::sync::Arc;
use xgovernor_core::{
    JointAdmission, JointNativeIntent, JointNativeObject, JointNativeOutcome,
    JointPendingOperation, NativeEffect, NativeOperationState, SessionDomainError,
    SessionRepository,
};

#[derive(Debug, thiserror::Error)]
pub enum DurableOperationError {
    #[error(transparent)]
    Repository(#[from] SessionDomainError),
    #[error(transparent)]
    Sdk(#[from] sdk::ManagementError),
    #[error("durable Management operation rejected: {0}")]
    Identity(&'static str),
}

/// Uses only persisted intent inputs. This bridge neither publishes Sessions nor repairs Roots.
pub struct DurableManagementOperation<'a> {
    client: &'a sdk::ThinkThreadClient,
    repository: Arc<dyn SessionRepository>,
}
impl<'a> DurableManagementOperation<'a> {
    pub fn new(client: &'a sdk::ThinkThreadClient, repository: Arc<dyn SessionRepository>) -> Self {
        Self { client, repository }
    }

    async fn original(
        &self,
        admission: &JointAdmission,
        step: &str,
    ) -> Result<
        (
            JointPendingOperation,
            JointNativeIntent,
            Option<JointNativeOutcome>,
        ),
        DurableOperationError,
    > {
        let pending = self
            .repository
            .pending_joint_operations()
            .await?
            .into_iter()
            .find(|p| p.admission == *admission)
            .ok_or(DurableOperationError::Identity(
                "pending admission not found",
            ))?;
        let (native, outcome) = pending
            .native
            .iter()
            .find(|(n, _)| n.step == step)
            .cloned()
            .ok_or(DurableOperationError::Identity("native step not found"))?;
        if ![
            "thinkthread.create",
            "thinkthread.start",
            "thinkthread.stop",
            "thinkthread.remove",
            "message.send",
        ]
        .contains(&native.method.as_str())
        {
            return Err(DurableOperationError::Identity(
                "method is not supported by durable dispatch",
            ));
        }
        let digest = sdk::operation_digest(self.client.context(), &native.method, &native.params)?;
        if digest != native.digest {
            return Err(DurableOperationError::Identity(
                "pinned context or parameters changed",
            ));
        }
        match native.method.as_str() {
            "thinkthread.create" => {
                if native.effect != NativeEffect::Create
                    || pending.request.context.operation != "open"
                {
                    return Err(DurableOperationError::Identity(
                        "create is not owned by an opening binding",
                    ));
                }
                let req: sdk::ThinkThreadCreateRequest = decode(native.params.clone())?;
                if serde_json::to_value(&req)
                    .map_err(|_| DurableOperationError::Identity("create encoding"))?
                    != native.params
                {
                    return Err(DurableOperationError::Identity(
                        "noncanonical create parameters",
                    ));
                }
            }
            "thinkthread.start" => {
                let req: sdk::ThinkThreadTargetRequest = decode(native.params.clone())?;
                if serde_json::to_value(&req)
                    .map_err(|_| DurableOperationError::Identity("start encoding"))?
                    != native.params
                {
                    return Err(DurableOperationError::Identity(
                        "noncanonical start parameters",
                    ));
                }
                if native.effect != NativeEffect::Mutate
                    || !pending.native.iter().any(|(n, o)| {
                        n.effect == NativeEffect::Create
                            && o.as_ref().is_some_and(|v| {
                                v.state == NativeOperationState::Completed
                                    && v.resources
                                        .iter()
                                        .any(|r| r.kind == "root" && r.id == req.thinkthread_id)
                            })
                    })
                {
                    return Err(DurableOperationError::Identity(
                        "start target is not owned by this binding",
                    ));
                }
            }
            "message.send" => {
                let req: sdk::MessageSendRequest = decode(native.params.clone())?;
                let binding = self
                    .repository
                    .get_joint_binding(admission.binding.clone())
                    .await?
                    .ok_or(DurableOperationError::Identity("Message binding missing"))?;
                if pending.request.context.operation != "turn"
                    || native.effect != NativeEffect::Mutate
                    || native.compensation
                    || binding.stage != "active"
                    || serde_json::to_value(&req)
                        .map_err(|_| DurableOperationError::Identity("Message encoding"))?
                        != native.params
                    || !binding
                        .objects
                        .iter()
                        .any(|o| o.kind == "root" && o.id == req.recipient_thinkthread_id)
                {
                    return Err(DurableOperationError::Identity(
                        "Message target is not owned",
                    ));
                }
            }
            "thinkthread.stop" | "thinkthread.remove" => {
                if pending.request.context.operation != "open"
                    || pending.request.publication_input.is_none()
                {
                    return Err(DurableOperationError::Identity(
                        "only unpublished creation compensation is supported",
                    ));
                }
                let (params, targets, force) = if native.method == "thinkthread.stop" {
                    let req: sdk::ThinkThreadStopRequest = decode(native.params.clone())?;
                    (
                        serde_json::to_value(&req)
                            .map_err(|_| DurableOperationError::Identity("stop encoding"))?,
                        req.thinkthread_ids,
                        req.force,
                    )
                } else {
                    let req: sdk::ThinkThreadRemoveRequest = decode(native.params.clone())?;
                    (
                        serde_json::to_value(&req)
                            .map_err(|_| DurableOperationError::Identity("remove encoding"))?,
                        req.thinkthread_ids,
                        req.force,
                    )
                };
                let expected = if native.method == "thinkthread.remove" {
                    NativeEffect::Remove
                } else {
                    NativeEffect::Mutate
                };
                let binding = self
                    .repository
                    .get_joint_binding(admission.binding.clone())
                    .await?
                    .ok_or(DurableOperationError::Identity(
                        "compensation binding not found",
                    ))?;
                if native.params != params
                    || force
                    || native.effect != expected
                    || targets.len() != 1
                    || binding.stage != "compensating"
                    || !binding
                        .objects
                        .iter()
                        .any(|o| o.kind == "root" && o.id == targets[0])
                {
                    return Err(DurableOperationError::Identity(
                        "compensation target is not owned or requires force",
                    ));
                }
            }
            _ => unreachable!(),
        }
        Ok((pending, native, outcome))
    }

    /// At most one bridge caller receives initial send admission. A retained result is never resent.
    pub async fn execute(
        &self,
        admission: JointAdmission,
        step: &str,
    ) -> Result<JointNativeOutcome, DurableOperationError> {
        let (_, native, outcome) = self.original(&admission, step).await?;
        if let Some(outcome) = outcome {
            return match outcome.state {
                NativeOperationState::Completed
                | NativeOperationState::Failed
                | NativeOperationState::NotSent => Ok(outcome),
                _ => self.query(admission, step).await,
            };
        }
        let claimed = self
            .repository
            .claim_joint_native_send(admission.clone(), step.into())
            .await?;
        if claimed.as_ref() != Some(&native) {
            return self.query(admission, step).await;
        }
        let reply = match native.method.as_str() {
            "thinkthread.create" => self
                .client
                .thinkthreads()
                .create_with_operation(native.operation_id.clone(), decode(native.params.clone())?)
                .await
                .map(|r| {
                    (
                        r.operation,
                        json!({"thinkthreadId":r.thinkthread_id,"changed":r.changed}),
                    )
                }),
            "thinkthread.start" => self
                .client
                .thinkthreads()
                .start_with_operation(native.operation_id.clone(), decode(native.params.clone())?)
                .await
                .map(|r| {
                    (
                        r.operation,
                        json!({"thinkthreadId":r.thinkthread_id,"changed":r.changed}),
                    )
                }),
            "thinkthread.stop" => self
                .client
                .thinkthreads()
                .stop_with_operation(native.operation_id.clone(), decode(native.params.clone())?)
                .await
                .map(|r| (r.operation, json!({"targets":r.targets}))),
            "message.send" => self
                .client
                .messages()
                .send_with_operation(native.operation_id.clone(), decode(native.params.clone())?)
                .await
                .map(|r| (r.operation, json!({"message":r.message}))),
            "thinkthread.remove" => self
                .client
                .thinkthreads()
                .remove_with_operation(native.operation_id.clone(), decode(native.params.clone())?)
                .await
                .map(|r| (r.operation, json!({"targets":r.targets}))),
            _ => unreachable!(),
        };
        match reply {
            Ok((handle, result)) => {
                validate_handle(&native, &handle)?;
                self.save(
                    admission,
                    &native,
                    NativeOperationState::Completed,
                    Some(result),
                )
                .await
            }
            Err(error) => {
                let state = if error.delivery() == Some(sdk::Delivery::NotSent) {
                    NativeOperationState::NotSent
                } else {
                    NativeOperationState::Unknown
                };
                self.save(admission, &native, state, None).await?;
                Err(error.into())
            }
        }
    }

    /// Recovery only observes the original SDK UUID. NotFound and observation errors keep the journal pending.
    pub async fn query(
        &self,
        admission: JointAdmission,
        step: &str,
    ) -> Result<JointNativeOutcome, DurableOperationError> {
        let (_, native, outcome) = self.original(&admission, step).await?;
        if let Some(outcome) = outcome {
            if matches!(
                outcome.state,
                NativeOperationState::Completed
                    | NativeOperationState::Failed
                    | NativeOperationState::NotSent
            ) {
                return Ok(outcome);
            }
        }
        let view = self
            .client
            .get_operation(sdk::OperationGetRequest {
                scope: sdk::OperationScope::ThinkThread,
                operation_id: native.operation_id.clone(),
            })
            .await?;
        validate_handle(&native, &view.operation)?;
        if view.method != native.method {
            return Err(DurableOperationError::Identity("operation method changed"));
        }
        let state = match view.state {
            sdk::OperationState::Accepted | sdk::OperationState::Running => {
                NativeOperationState::Accepted
            }
            sdk::OperationState::NeedsRecovery => NativeOperationState::NeedsRecovery,
            sdk::OperationState::Succeeded => NativeOperationState::Completed,
            sdk::OperationState::Failed => NativeOperationState::Failed,
        };
        self.save(admission, &native, state, view.result).await
    }

    async fn save(
        &self,
        admission: JointAdmission,
        native: &JointNativeIntent,
        state: NativeOperationState,
        result: Option<Value>,
    ) -> Result<JointNativeOutcome, DurableOperationError> {
        let mut state = state;
        if ["thinkthread.stop", "thinkthread.remove"].contains(&native.method.as_str()) {
            if let Some(result) = &result {
                let targets: Vec<sdk::ThinkThreadTargetStatus> = decode(
                    result
                        .get("targets")
                        .cloned()
                        .ok_or(DurableOperationError::Identity("missing batch targets"))?,
                )?;
                let expected = native.params["thinkthreadIds"]
                    .as_array()
                    .ok_or(DurableOperationError::Identity("missing owned target"))?;
                if targets.len() != 1
                    || expected.len() != 1
                    || expected[0] != targets[0].thinkthread_id
                {
                    return Err(DurableOperationError::Identity(
                        "batch result target mismatch",
                    ));
                }
                let target = &targets[0];
                let success = if native.method == "thinkthread.stop" {
                    "stopped"
                } else {
                    "removed"
                };
                let noop = if native.method == "thinkthread.stop" {
                    "already_stopped"
                } else {
                    "already_removed"
                };
                if state == NativeOperationState::Completed
                    && !((target.accepted && target.status == success)
                        || (!target.accepted && !target.changed && target.status == noop))
                {
                    state = NativeOperationState::Failed;
                }
            } else if state == NativeOperationState::Completed {
                return Err(DurableOperationError::Identity(
                    "completed batch has no result",
                ));
            }
        }
        if native.method == "message.send" {
            let mut resources = vec![];
            if let Some(raw) = result.as_ref().and_then(|r| r.get("message")) {
                let message: sdk::MessageView = decode(raw.clone())?;
                if message.message_id.trim().is_empty()
                    || native.params["recipientThinkthreadId"] != message.recipient_thinkthread_id
                    || native.params["text"] != message.text
                    || message.truncated
                {
                    return Err(DurableOperationError::Identity("Message admission changed"));
                }
                resources.push(JointNativeObject {
                    kind: "message".into(),
                    id: message.message_id,
                });
            } else if state == NativeOperationState::Completed {
                return Err(DurableOperationError::Identity(
                    "Message result lacks admission",
                ));
            }
            let outcome = JointNativeOutcome {
                state,
                resources,
                payload: json!({"result":result}),
            };
            self.repository
                .record_joint_native_result(
                    admission,
                    native.step.clone(),
                    native.operation_id.clone(),
                    native.digest.clone(),
                    outcome.clone(),
                )
                .await?;
            return Ok(outcome);
        }
        let id = result
            .as_ref()
            .and_then(|r| r.get("thinkthreadId"))
            .and_then(Value::as_str)
            .filter(|v| !v.trim().is_empty());
        if state == NativeOperationState::Completed
            && !["thinkthread.stop", "thinkthread.remove"].contains(&native.method.as_str())
            && id.is_none()
        {
            return Err(DurableOperationError::Identity(
                "completed result lacks target identity",
            ));
        }
        if native.method == "thinkthread.start"
            && id.is_some_and(|id| native.params["thinkthreadId"] != id)
        {
            return Err(DurableOperationError::Identity(
                "start result target changed",
            ));
        }
        let resources = if native.effect == NativeEffect::Create {
            id.map(|id| {
                vec![JointNativeObject {
                    kind: "root".into(),
                    id: id.into(),
                }]
            })
            .unwrap_or_default()
        } else {
            vec![]
        };
        let outcome = JointNativeOutcome {
            state,
            resources,
            payload: json!({"result":result}),
        };
        self.repository
            .record_joint_native_result(
                admission,
                native.step.clone(),
                native.operation_id.clone(),
                native.digest.clone(),
                outcome.clone(),
            )
            .await?;
        Ok(outcome)
    }
}
fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, DurableOperationError> {
    serde_json::from_value(value)
        .map_err(|_| DurableOperationError::Identity("persisted request encoding"))
}
fn validate_handle(
    native: &JointNativeIntent,
    handle: &sdk::OperationHandle,
) -> Result<(), DurableOperationError> {
    if handle.scope != sdk::OperationScope::ThinkThread
        || handle.operation_id != native.operation_id
        || handle.operation_digest != native.digest
    {
        return Err(DurableOperationError::Identity(
            "SDK operation handle mismatch",
        ));
    }
    Ok(())
}
