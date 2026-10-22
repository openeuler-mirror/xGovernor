//! Durable, terminal-only Message adaptation for explicitly assembled Root owners.
use crate::{durable_operation::DurableManagementOperation, root_owner::*, sdk};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use xgovernor_core::*;

fn unavailable(message: &str) -> SessionDomainError {
    SessionDomainError::Unavailable {
        message: message.into(),
    }
}
impl RootSessionOwner {
    /// Only an empty-policy, unmetered administrator profile is currently expressible.
    pub async fn admit_message(
        &self,
        context: JointOperationContext,
        session: SessionRecord,
        request: session_protocol::SessionTurnRequest,
        turn_id: String,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<JointTurnRecord, SessionDomainError> {
        if context.operation != "turn"
            || context.actor_role != Role::Admin
            || context.tenant_id.is_some()
            || context.owner_ref != ADMIN_OWNER_REF
            || context.runtime_kind != ROOT_RUNTIME_KIND
            || context.configuration_revision != self.revision
            || context.source_runtime_id.as_deref() != Some(session.runtime_id.as_str())
            || request.runtime_id != session.runtime_id
            || request.llm.is_some()
            || request.reasoning_effort.is_some()
            || !request.ext.is_empty()
            || request.entry != Default::default()
            || !session.capabilities.runtime.is_empty()
            || !session.capabilities.sandbox.is_empty()
        {
            return Err(unsupported_joint("message_profile"));
        }
        if session.last_error.as_deref() == Some("message_execution_invalidated") {
            return Err(unavailable("Message execution requires recovery"));
        }
        let root = self.bound_root(&session, repository.clone()).await?;
        let client = self.connect().await?;
        let params = serde_json::to_value(sdk::MessageSendRequest {
            recipient_thinkthread_id: root,
            text: request.text,
        })
        .map_err(|_| unavailable("Message encoding failed"))?;
        let digest = sdk::operation_digest(client.context(), "message.send", &params)
            .map_err(|_| unavailable("Message digest failed"))?;
        if let Some(key) = request.client_request_id.as_ref() {
            if let Some(old) = repository
                .get_joint_turn(session.runtime_id.clone(), key.clone())
                .await?
            {
                client
                    .close()
                    .await
                    .map_err(|_| unavailable("Management close failed"))?;
                if old.identity.input_digest != digest {
                    return Err(SessionDomainError::Conflict {
                        message: "idempotency key has different Message input".into(),
                    });
                }
                return Ok(old);
            }
        }
        let binding = session
            .execution_backing
            .joint()
            .cloned()
            .ok_or_else(|| unavailable("joint binding missing"))?;
        let state = repository
            .get_joint_binding(binding.clone())
            .await?
            .ok_or_else(|| unavailable("owner missing"))?;
        let identity = JointTurnIdentity {
            turn_id,
            client_request_id: request.client_request_id,
            input_digest: digest.clone(),
        };
        // Closing this observation-only Host cannot leave a half-admitted Message.
        client
            .close()
            .await
            .map_err(|_| unavailable("Management close failed"))?;
        let admission = repository
            .prepare_joint_operation(JointPrepareRequest {
                context,
                binding,
                root_id: state.creation.root_id,
                source_id: session.runtime_id.clone(),
                exclusive_root: false,
                parent_binding: None,
                reservations: vec![],
                publication_input: None,
                turn: Some(identity.clone()),
                native: vec![JointNativeIntent {
                    step: "message".into(),
                    operation_id: uuid::Uuid::new_v4().to_string(),
                    digest,
                    method: "message.send".into(),
                    params,
                    effect: NativeEffect::Mutate,
                    compensation: false,
                }],
            })
            .await?;
        Ok(JointTurnRecord {
            runtime_id: session.runtime_id,
            identity,
            admission,
            terminal: None,
        })
    }

    /// Bounded observation; expiry or disconnect retains the original UUID and source fence.
    pub async fn resume_message(
        &self,
        pending: JointPendingOperation,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<JointTurnTerminal, SessionDomainError> {
        if let Some(terminal) = repository
            .joint_turn_result(pending.admission.clone())
            .await?
        {
            return Ok(terminal);
        }
        let c = &pending.request.context;
        if c.operation != "turn"
            || c.actor_role != Role::Admin
            || c.tenant_id.is_some()
            || c.owner_ref != ADMIN_OWNER_REF
            || c.configuration_revision != self.revision
            || c.runtime_kind != ROOT_RUNTIME_KIND
            || pending.request.turn.is_none()
            || pending.native.len() != 1
        {
            return Err(unsupported_joint("message_reconcile"));
        }
        let record = repository
            .get(&pending.request.source_id)
            .await?
            .ok_or_else(|| unavailable("Message Session missing"))?;
        let root = match self.bound_root(&record, repository.clone()).await {
            Ok(root) => root,
            Err(error) => {
                if let Some(terminal) = repository
                    .joint_turn_result(pending.admission.clone())
                    .await?
                {
                    return Ok(terminal);
                }
                return Err(error);
            }
        };
        let (native, _) = &pending.native[0];
        let request: sdk::MessageSendRequest = serde_json::from_value(native.params.clone())
            .map_err(|_| unavailable("persisted Message invalid"))?;
        if request.recipient_thinkthread_id != root
            || pending.request.binding != pending.admission.binding
            || record.execution_backing.joint() != Some(&pending.admission.binding)
        {
            return Err(unavailable("Message ownership mismatch"));
        }
        let observed = tokio::time::timeout(Duration::from_millis(self.timeout_ms), async {
            let client = self.connect().await?;
            // Only a never-dispatched Message needs a Ready check before its first send.
            if pending.native[0].1.is_none() {
                let view = client
                    .thinkthreads()
                    .get(sdk::ThinkThreadTargetRequest {
                        thinkthread_id: root.clone(),
                    })
                    .await
                    .map_err(|_| unavailable("Message readiness observation failed"))?;
                if view.thinkthread_id != root
                    || view.root_id != root
                    || view.parent_id.is_some()
                    || view.fs != self.config.fs_root
                    || view.fs_branch_id.is_some()
                {
                    return Err(unavailable("Message Root observation identity changed"));
                }
                if view.thinkthread_id == root
                    && view.root_id == root
                    && (view.state.eq_ignore_ascii_case("stopped") || view.needs_recovery)
                {
                    repository
                        .invalidate_joint_turn(pending.admission.clone())
                        .await?;
                }
                self.validate_view(&root, &view, true)?;
            }
            let bridge = DurableManagementOperation::new(&client, repository.clone());
            let admitted = loop {
                let result = bridge
                    .execute(pending.admission.clone(), &native.step)
                    .await
                    .map_err(|_| {
                        unavailable("Message admission unresolved; original UUID retained")
                    })?;
                match result.state {
                    NativeOperationState::Completed => break result,
                    NativeOperationState::Failed if !result.resources.is_empty() => break result,
                    NativeOperationState::Failed => {
                        return Err(unavailable(
                            "failed admission has no proof of Message absence",
                        ))
                    }
                    NativeOperationState::NotSent => {
                        let view = client
                            .thinkthreads()
                            .get(sdk::ThinkThreadTargetRequest {
                                thinkthread_id: root.clone(),
                            })
                            .await
                            .map_err(|_| unavailable("failed admission readiness unavailable"))?;
                        self.validate_view(&root, &view, true)?;
                        client
                            .close()
                            .await
                            .map_err(|_| unavailable("Management close failed"))?;
                        return Ok(JointTurnTerminal {
                            cancelled: false,
                            execution_invalidated: false,
                            message_id: None,
                            reply: None,
                            failure: Some("message_admission_failed".into()),
                            completed_at_ms: now_ms(),
                        });
                    }
                    _ => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            };
            let id = admitted
                .resources
                .iter()
                .find(|o| o.kind == "message")
                .map(|o| o.id.clone())
                .ok_or_else(|| unavailable("admitted Message identity missing"))?;
            loop {
                if let Some(terminal) = repository
                    .joint_turn_result(pending.admission.clone())
                    .await?
                {
                    return Ok(terminal);
                }
                if let Err(error) = self
                    .resume_cancel(&client, &pending, &id, repository.clone())
                    .await
                {
                    if !matches!(error, SessionDomainError::Unavailable { .. }) {
                        return Err(error);
                    }
                    // An unknown cancel operation does not determine the turn.
                    // Continue observing only the original Message's authoritative state.
                }
                let message = client
                    .messages()
                    .wait(sdk::MessageWaitRequest {
                        recipient_thinkthread_id: root.clone(),
                        message_id: id.clone(),
                        timeout_ms: 250u64.into(),
                    })
                    .await
                    .map_err(|_| unavailable("Message terminal observation unavailable"))?;
                if message.message_id != id
                    || message.recipient_thinkthread_id != root
                    || message.text != request.text
                    || message.truncated
                {
                    return Err(unavailable("Message terminal identity changed"));
                }
                let view = client
                    .thinkthreads()
                    .get(sdk::ThinkThreadTargetRequest {
                        thinkthread_id: root.clone(),
                    })
                    .await
                    .map_err(|_| unavailable("terminal Session readiness unavailable"))?;
                if view.thinkthread_id != root
                    || view.root_id != root
                    || view.parent_id.is_some()
                    || view.fs != self.config.fs_root
                    || view.fs_branch_id.is_some()
                {
                    return Err(unavailable("Message Root observation identity changed"));
                }
                if self
                    .validate_view(&root, &view, message.state == sdk::MessageState::Completed)
                    .is_err()
                    && view.state.eq_ignore_ascii_case("running")
                    && !view.needs_recovery
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                match message.state {
                    sdk::MessageState::Queued
                    | sdk::MessageState::Delivering
                    | sdk::MessageState::AwaitingReply => {
                        if !view.state.eq_ignore_ascii_case("running") || view.needs_recovery {
                            repository
                                .invalidate_joint_turn(pending.admission.clone())
                                .await?;
                            return Err(unavailable(
                                "Message execution invalidated; protection retained",
                            ));
                        }
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        continue;
                    }
                    sdk::MessageState::Completed
                    | sdk::MessageState::Failed
                    | sdk::MessageState::Cancelled => {}
                }
                let failure = if message.state == sdk::MessageState::Cancelled {
                    None
                } else if !view.state.eq_ignore_ascii_case("running") || view.needs_recovery {
                    Some("message_execution_invalidated")
                } else if admitted.state == NativeOperationState::Failed {
                    Some("message_admission_failed")
                } else if message.reply_truncated {
                    Some("message_reply_truncated")
                } else if message.state == sdk::MessageState::Failed {
                    Some("message_failed")
                } else if message.state == sdk::MessageState::Completed
                    && message.reply_text.is_none()
                {
                    Some("message_reply_missing")
                } else {
                    None
                };
                let has_cancelled = repository
                    .joint_workspace_boundary(pending.admission.binding.clone())
                    .await?
                    .cancel_epoch
                    > 0;
                let terminal = JointTurnTerminal {
                    cancelled: message.state == sdk::MessageState::Cancelled,
                    execution_invalidated: !view.state.eq_ignore_ascii_case("running")
                        || view.needs_recovery
                        || (message.state == sdk::MessageState::Failed && has_cancelled),
                    message_id: Some(id),
                    reply: if failure.is_none() && message.state == sdk::MessageState::Completed {
                        message.reply_text
                    } else {
                        None
                    },
                    failure: failure.map(str::to_string),
                    completed_at_ms: now_ms(),
                };
                client.close().await.map_err(|_| {
                    unavailable("Management close failed before terminal persistence")
                })?;
                return Ok(terminal);
            }
        })
        .await
        .map_err(|_| unavailable("Message observation deadline; durable fence retained"))
        .and_then(|result| result);
        if let Some(terminal) = repository
            .joint_turn_result(pending.admission.clone())
            .await?
        {
            return Ok(terminal);
        }
        let observed = observed?;
        if let Err(error) = repository
            .finish_joint_turn(pending.admission.clone(), observed)
            .await
        {
            if let Some(terminal) = repository
                .joint_turn_result(pending.admission.clone())
                .await?
            {
                return Ok(terminal);
            }
            return Err(error);
        }
        repository
            .joint_turn_result(pending.admission)
            .await?
            .ok_or_else(|| unavailable("settled Message terminal missing"))
    }
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
