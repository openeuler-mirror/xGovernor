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
        if !["thinkthread.create", "thinkthread.start"].contains(&native.method.as_str()) {
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
                .map(|r| (r.operation, r.thinkthread_id, r.changed)),
            "thinkthread.start" => self
                .client
                .thinkthreads()
                .start_with_operation(native.operation_id.clone(), decode(native.params.clone())?)
                .await
                .map(|r| (r.operation, r.thinkthread_id, r.changed)),
            _ => unreachable!(),
        };
        match reply {
            Ok((handle, id, changed)) => {
                validate_handle(&native, &handle)?;
                self.save(
                    admission,
                    &native,
                    NativeOperationState::Completed,
                    Some(json!({"thinkthreadId":id,"changed":changed})),
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
        let id = result
            .as_ref()
            .and_then(|r| r.get("thinkthreadId"))
            .and_then(Value::as_str)
            .filter(|v| !v.trim().is_empty());
        if state == NativeOperationState::Completed && id.is_none() {
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
