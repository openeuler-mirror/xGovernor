//! Explicit administrator-only Root lifecycle owner. No server registration is installed.
use crate::{
    durable_operation::{DurableManagementOperation, DurableOperationError},
    sdk, ManagementConfig,
};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use xgovernor_core::*;

pub const ROOT_OWNER_KIND: &str = "thinkthread_root_v1";
pub const ROOT_RUNTIME_KIND: &str = "thinkthread-root";

#[derive(Clone)]
pub struct RootSessionOwner {
    pub(crate) config: ManagementConfig,
    create: sdk::ThinkThreadCreateRequest,
    pub(crate) revision: String,
    pub(crate) timeout_ms: u64,
}
impl RootSessionOwner {
    /// The legacy limit arguments are ignored; repository assembly owns resource budgets.
    pub fn new(
        config: ManagementConfig,
        create: sdk::ThinkThreadCreateRequest,
        revision: String,
        _session_limit: u32,
        _root_limit: u32,
        timeout_ms: u64,
    ) -> Result<Self, SessionDomainError> {
        config
            .validate()
            .map_err(|_| unavailable("invalid explicit Management configuration"))?;
        if create.fs.as_ref() != Some(&config.fs_root)
            || create.allow_shared_fs
            || revision.trim().is_empty()
            || !(1..=120_000).contains(&timeout_ms)
        {
            return Err(unavailable("invalid Root lifecycle deployment"));
        }
        Ok(Self {
            config,
            create,
            revision,
            timeout_ms,
        })
    }

    fn validate_context(&self, c: &JointOperationContext) -> Result<(), SessionDomainError> {
        if c.actor_role != Role::Admin
            || c.tenant_id.is_some()
            || c.owner_ref != ADMIN_OWNER_REF
            || c.runtime_kind != ROOT_RUNTIME_KIND
            || c.configuration_revision != self.revision
            || c.operation != "open"
        {
            return Err(unsupported_joint("root_admission"));
        }
        Ok(())
    }
    fn validate_pending(
        &self,
        p: &JointPendingOperation,
    ) -> Result<SessionRecord, SessionDomainError> {
        self.validate_context(&p.request.context)?;
        if p.admission.binding.owner_kind != ROOT_OWNER_KIND
            || p.admission.binding != p.request.binding
        {
            return Err(unavailable("persisted owner binding mismatch"));
        }
        let record = p
            .request
            .publication_input
            .clone()
            .ok_or_else(|| unavailable("missing durable publication input"))?;
        if record.runtime.runtime_kind != ROOT_RUNTIME_KIND
            || record.workspace.root != self.config.fs_root.to_string_lossy()
            || record.execution_backing.joint() != Some(&p.admission.binding)
            || record.tenant_id.is_some()
            || !record.capabilities.runtime.is_empty()
            || !record.capabilities.sandbox.is_empty()
            || record.isolation.boundary != IsolationBoundary::Host
        {
            return Err(unavailable("persisted Root publication input mismatch"));
        }
        let expected = serde_json::to_value(&self.create)
            .map_err(|_| unavailable("create encoding failed"))?;
        if !p.native.iter().any(|(n, _)| {
            n.step == "create" && n.method == "thinkthread.create" && n.params == expected
        }) {
            return Err(unavailable("pinned create template changed"));
        }
        Ok(record)
    }
    pub(crate) async fn connect(&self) -> Result<sdk::ThinkThreadClient, SessionDomainError> {
        self.config
            .connect()
            .await
            .map_err(|_| unavailable("explicit Management connection failed"))
    }
    fn native(
        &self,
        client: &sdk::ThinkThreadClient,
        step: &str,
        method: &str,
        params: Value,
        effect: NativeEffect,
    ) -> Result<JointNativeIntent, SessionDomainError> {
        let digest = sdk::operation_digest(client.context(), method, &params)
            .map_err(|_| unavailable("native intent encoding failed"))?;
        Ok(JointNativeIntent {
            step: step.into(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            digest,
            method: method.into(),
            params,
            effect,
            compensation: step.starts_with("compensate_"),
        })
    }
    async fn step(
        &self,
        client: &sdk::ThinkThreadClient,
        repository: &Arc<dyn SessionRepository>,
        a: &JointAdmission,
        step: &str,
        method: &str,
        params: Value,
        effect: NativeEffect,
    ) -> Result<(), SessionDomainError> {
        let pending = repository
            .pending_joint_operations()
            .await?
            .into_iter()
            .find(|p| p.admission == *a)
            .ok_or_else(|| unavailable("pending owner operation disappeared"))?;
        let digest = sdk::operation_digest(client.context(), method, &params)
            .map_err(|_| unavailable("native intent encoding failed"))?;
        if let Some((old, _)) = pending.native.iter().find(|(n, _)| n.step == step) {
            if old.method != method
                || old.params != params
                || old.effect != effect
                || old.digest != digest
            {
                return Err(unavailable("persisted native step changed"));
            }
            return Ok(());
        }
        let native = self.native(client, step, method, params, effect)?;
        match repository
            .append_joint_native_step(a.clone(), native.clone())
            .await
        {
            Ok(()) => Ok(()),
            Err(error @ SessionDomainError::Conflict { .. }) => {
                let current = repository
                    .pending_joint_operations()
                    .await?
                    .into_iter()
                    .find(|p| p.admission == *a);
                if let Some((old, _)) = current
                    .as_ref()
                    .and_then(|p| p.native.iter().find(|(n, _)| n.step == step))
                {
                    let mut replay = native;
                    replay.operation_id = old.operation_id.clone();
                    if replay == *old {
                        return Ok(());
                    }
                }
                Err(error)
            }
            Err(error) => Err(error),
        }
    }
    pub(crate) fn validate_view(
        &self,
        id: &str,
        v: &sdk::ThinkThreadView,
        ready: bool,
    ) -> Result<(), SessionDomainError> {
        if v.thinkthread_id != id
            || v.root_id != id
            || v.parent_id.is_some()
            || v.fs != self.config.fs_root
            || v.fs_branch_id.is_some()
            || v.needs_recovery
            || !v.state.eq_ignore_ascii_case("running")
            || (ready
                && !v
                    .session_state
                    .as_deref()
                    .is_some_and(|s| s.eq_ignore_ascii_case("ready")))
        {
            return Err(unavailable("Root has no verified running binding"));
        }
        Ok(())
    }
    async fn ready(
        &self,
        client: &sdk::ThinkThreadClient,
        id: &str,
    ) -> Result<(), SessionDomainError> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(self.timeout_ms);
        loop {
            let v = client
                .thinkthreads()
                .get(sdk::ThinkThreadTargetRequest {
                    thinkthread_id: id.into(),
                })
                .await
                .map_err(|_| unavailable("Root projection is unavailable"))?;
            if self.validate_view(id, &v, true).is_ok() {
                return Ok(());
            }
            if v.needs_recovery
                || v.thinkthread_id != id
                || v.root_id != id
                || v.fs != self.config.fs_root
                || tokio::time::Instant::now() >= deadline
            {
                return Err(unavailable(
                    "Root did not establish a committed Ready binding",
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    async fn resume(
        &self,
        pending: JointPendingOperation,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<SessionRecord, SessionDomainError> {
        let record = self.validate_pending(&pending)?;
        let native_work = async {
            let client = self.connect().await?;
            let bridge = DurableManagementOperation::new(&client, repository.clone());
            let created = bridge
                .execute(pending.admission.clone(), "create")
                .await
                .map_err(map_bridge)?;
            if created.state != NativeOperationState::Completed {
                return Err(unavailable(
                    "Root creation is not completed; original intent retained",
                ));
            }
            let root = created
                .resources
                .iter()
                .find(|r| r.kind == "root")
                .ok_or_else(|| unavailable("Root creation lacks owned identity"))?;
            self.step(
                &client,
                &repository,
                &pending.admission,
                "start",
                "thinkthread.start",
                json!({"thinkthreadId":root.id}),
                NativeEffect::Mutate,
            )
            .await?;
            let started = bridge
                .execute(pending.admission.clone(), "start")
                .await
                .map_err(map_bridge)?;
            if started.state != NativeOperationState::Completed {
                return Err(unavailable(
                    "Root start is not completed; original intent retained",
                ));
            }
            self.ready(&client, &root.id).await?;
            client.close().await.map_err(|_| {
                unavailable("Management connection close failed before publication")
            })?;
            Ok::<(), SessionDomainError>(())
        };
        tokio::time::timeout(Duration::from_millis(self.timeout_ms), native_work)
            .await
            .map_err(|_| timeout("root_open", self.timeout_ms))??;
        repository
            .publish_joint_session(pending.admission, record)
            .await
    }

    /// Compensates only an unpublished creation with all original mutations already settled.
    pub async fn compensate_unpublished(
        &self,
        pending: JointPendingOperation,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<(), SessionDomainError> {
        tokio::time::timeout(
            Duration::from_millis(self.timeout_ms),
            self.compensate(pending, repository),
        )
        .await
        .map_err(|_| timeout("root_compensation", self.timeout_ms))?
    }
    async fn compensate(
        &self,
        pending: JointPendingOperation,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<(), SessionDomainError> {
        self.validate_pending(&pending)?;
        let client = self.connect().await?;
        // Recompute the original create digest before observing or deleting any target.
        let create = pending
            .native
            .iter()
            .find(|(n, _)| n.step == "create")
            .ok_or_else(|| unavailable("missing original create"))?;
        if sdk::operation_digest(client.context(), &create.0.method, &create.0.params)
            .map_err(|_| unavailable("native digest failed"))?
            != create.0.digest
        {
            return Err(unavailable("original Management context changed"));
        }
        if pending
            .native
            .iter()
            .filter(|(n, _)| n.step == "create" || n.step == "start")
            .any(|(_, o)| {
                !o.as_ref().is_some_and(|o| {
                    matches!(
                        o.state,
                        NativeOperationState::Completed
                            | NativeOperationState::Failed
                            | NativeOperationState::NotSent
                    )
                })
            })
        {
            return Err(unavailable(
                "unsettled original mutation cannot be compensated",
            ));
        }
        if create
            .1
            .as_ref()
            .is_some_and(|o| o.state == NativeOperationState::Failed && o.resources.is_empty())
        {
            return Err(unavailable(
                "failed create has no trustworthy residual identity; ownership retained",
            ));
        }
        repository
            .begin_joint_compensation(pending.admission.clone())
            .await?;
        let binding = repository
            .get_joint_binding(pending.admission.binding.clone())
            .await?
            .ok_or_else(|| unavailable("missing durable binding"))?;
        if binding.stage != "compensating"
            || binding.objects.iter().any(|o| o.kind != "root")
            || binding.objects.len() > 1
        {
            return Err(unavailable(
                "compensation does not own an unpublished single Root",
            ));
        }
        if let Some(root) = binding.objects.first() {
            let bridge = DurableManagementOperation::new(&client, repository.clone());
            self.step(
                &client,
                &repository,
                &pending.admission,
                "compensate_stop",
                "thinkthread.stop",
                json!({"thinkthreadIds":[root.id],"force":false}),
                NativeEffect::Mutate,
            )
            .await?;
            let stopped = bridge
                .execute(pending.admission.clone(), "compensate_stop")
                .await
                .map_err(map_bridge)?;
            if stopped.state != NativeOperationState::Completed {
                return Err(unavailable("compensation stop is not confirmed"));
            }
            let current = repository
                .pending_joint_operations()
                .await?
                .into_iter()
                .find(|p| p.admission == pending.admission)
                .ok_or_else(|| unavailable("compensation intent disappeared"))?;
            let removal_observed = current
                .native
                .iter()
                .any(|(n, o)| n.step == "compensate_remove" && o.is_some());
            if !removal_observed {
                let view = client
                    .thinkthreads()
                    .get(sdk::ThinkThreadTargetRequest {
                        thinkthread_id: root.id.clone(),
                    })
                    .await
                    .map_err(|_| unavailable("stopped Root projection is unavailable"))?;
                if view.thinkthread_id != root.id
                    || view.root_id != root.id
                    || view.fs != self.config.fs_root
                    || view.needs_recovery
                    || !(view.state.eq_ignore_ascii_case("stopped")
                        || view.state.eq_ignore_ascii_case("ready"))
                {
                    return Err(unavailable("compensation Root is not exactly stopped"));
                }
            }
            self.step(
                &client,
                &repository,
                &pending.admission,
                "compensate_remove",
                "thinkthread.remove",
                json!({"thinkthreadIds":[root.id],"force":false}),
                NativeEffect::Remove,
            )
            .await?;
            let removed = bridge
                .execute(pending.admission.clone(), "compensate_remove")
                .await
                .map_err(map_bridge)?;
            if removed.state != NativeOperationState::Completed {
                return Err(unavailable("compensation remove is not confirmed"));
            }
            self.step(
                &client,
                &repository,
                &pending.admission,
                "compensate_absence",
                "thinkthread.get",
                json!({"thinkthreadId":root.id}),
                NativeEffect::ObserveAbsence,
            )
            .await?;
            match client
                .thinkthreads()
                .get(sdk::ThinkThreadTargetRequest {
                    thinkthread_id: root.id.clone(),
                })
                .await
            {
                Err(sdk::ManagementError::Rejected(body)) if body.code == "ThinkThreadNotFound" => {
                }
                _ => return Err(unavailable("Root absence is not confirmed")),
            }
            let current = repository
                .pending_joint_operations()
                .await?
                .into_iter()
                .find(|p| p.admission == pending.admission)
                .ok_or_else(|| unavailable("compensation intent disappeared"))?;
            let (native, _) = current
                .native
                .iter()
                .find(|(n, _)| n.step == "compensate_absence")
                .ok_or_else(|| unavailable("absence intent disappeared"))?;
            repository
                .record_joint_native_result(
                    pending.admission.clone(),
                    native.step.clone(),
                    native.operation_id.clone(),
                    native.digest.clone(),
                    JointNativeOutcome {
                        state: NativeOperationState::Completed,
                        resources: vec![root.clone()],
                        payload: json!({"absent":true}),
                    },
                )
                .await?;
        }
        client
            .close()
            .await
            .map_err(|_| unavailable("Management close failed before compensation finish"))?;
        repository
            .finish_joint_compensation(pending.admission)
            .await?;
        Ok(())
    }

    pub(crate) async fn bound_root(
        &self,
        record: &SessionRecord,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<String, SessionDomainError> {
        let binding = record
            .execution_backing
            .joint()
            .ok_or_else(|| unavailable("not a joint record"))?;
        let state = repository
            .get_joint_binding(binding.clone())
            .await?
            .ok_or_else(|| unavailable("joint binding missing"))?;
        self.validate_context(&state.creation.context)?;
        if binding.owner_kind != ROOT_OWNER_KIND
            || state.stage != "active"
            || state
                .creation
                .publication_input
                .as_ref()
                .is_none_or(|original| {
                    let mut expected = original.clone();
                    expected.status = record.status;
                    expected.updated_at_ms = record.updated_at_ms;
                    expected.last_error = record.last_error.clone();
                    expected != *record
                })
            || repository.get(&record.runtime_id).await?.as_ref() != Some(record)
            || state.objects.len() != 1
            || state.objects[0].kind != "root"
        {
            return Err(unavailable("active Root ownership mismatch"));
        }
        Ok(state.objects[0].id.clone())
    }
}

#[async_trait]
impl JointSessionLifecycle for RootSessionOwner {
    fn owner_kind(&self) -> &str {
        ROOT_OWNER_KIND
    }
    fn configuration_revision(&self) -> &str {
        &self.revision
    }
    async fn open(
        &self,
        context: JointOperationContext,
        input: JointOpenInput,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<SessionRecord, SessionDomainError> {
        self.validate_context(&context)?;
        if !input.capabilities.runtime.is_empty()
            || !input.capabilities.sandbox.is_empty()
            || input.normalized.isolation.boundary != IsolationBoundary::Host
            || input.normalized.workspace.root != self.config.fs_root.to_string_lossy()
            || !input.request.ext.is_empty()
            || !empty(&input.normalized.workspace.metadata)
            || !empty(&input.normalized.isolation.metadata)
            || input.request.llm.is_some()
            || input.normalized.llm.is_some()
        {
            return Err(unsupported_joint("root_profile"));
        }
        let runtime_id = context
            .target_runtime_id
            .clone()
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| unavailable("missing public target"))?;
        let binding = JointBinding {
            owner_kind: ROOT_OWNER_KIND.into(),
            binding_id: format!("root-binding-{runtime_id}"),
            configuration_revision: self.revision.clone(),
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| unavailable("clock before epoch"))?
            .as_millis() as u64;
        let mut record = SessionRecord {
            execution_backing: SessionExecutionBacking::JointV1 {
                binding: binding.clone(),
            },
            runtime_id: runtime_id.clone(),
            conversation_id: input.request.conversation_id,
            sender_id: input.request.sender_id,
            status: SessionStatus::Idle,
            created_at_ms: now,
            updated_at_ms: now,
            workspace: input.normalized.workspace,
            isolation: input.normalized.isolation,
            capabilities: input.capabilities,
            runtime: OpaqueRuntimeState {
                runtime_kind: ROOT_RUNTIME_KIND.into(),
                schema_version: 1,
                state: json!({"binding_id":binding.binding_id}),
            },
            llm: None,
            lease: input.normalized.lease,
            lineage: None,
            last_error: None,
            tenant_id: None,
            created_by: context.principal.clone(),
        };
        if let Some(old) = repository.get(&runtime_id).await? {
            record.created_at_ms = old.created_at_ms;
            record.updated_at_ms = old.updated_at_ms;
            let state = repository
                .get_joint_binding(binding.clone())
                .await?
                .ok_or_else(|| unavailable("public target belongs to another owner"))?;
            if old != record
                || serde_json::to_value(&state.creation.context).ok()
                    != serde_json::to_value(&context).ok()
            {
                return Err(unavailable("open replay changed identity or input"));
            }
            return Ok(old);
        }
        let pending = repository
            .pending_joint_operations()
            .await?
            .into_iter()
            .find(|p| p.request.context.intent_id == context.intent_id);
        let pending = if let Some(pending) = pending {
            let old = self.validate_pending(&pending)?;
            record.created_at_ms = old.created_at_ms;
            record.updated_at_ms = old.updated_at_ms;
            if old != record
                || serde_json::to_value(&pending.request.context).ok()
                    != serde_json::to_value(&context).ok()
            {
                return Err(unavailable("open replay changed input"));
            }
            pending
        } else {
            let client = self.connect().await?;
            let create = self.native(
                &client,
                "create",
                "thinkthread.create",
                serde_json::to_value(&self.create)
                    .map_err(|_| unavailable("create encoding failed"))?,
                NativeEffect::Create,
            )?;
            client
                .close()
                .await
                .map_err(|_| unavailable("Management preflight close failed"))?;
            let mut request = JointPrepareRequest {
                turn: None,
                context,
                binding: binding.clone(),
                root_id: format!("root-owner-{}", binding.binding_id),
                source_id: runtime_id,
                exclusive_root: false,
                parent_binding: None,
                reservations: vec![
                    JointResourceReservation {
                        kind: "session".into(),
                        units: 1,
                        limit: 0,
                    },
                    JointResourceReservation {
                        kind: "root".into(),
                        units: 1,
                        limit: 0,
                    },
                ],
                native: vec![create.clone()],
                publication_input: Some(record),
            };
            let admission = match repository.prepare_joint_operation(request.clone()).await {
                Ok(admission) => admission,
                Err(error @ SessionDomainError::Conflict { .. }) => {
                    let Some(state) = repository.get_joint_binding(binding.clone()).await? else {
                        return Err(error);
                    };
                    let original = state.creation;
                    // Only locally generated timestamps and UUIDs may differ on a racing replay.
                    if let (Some(candidate), Some(stored)) =
                        (&mut request.publication_input, &original.publication_input)
                    {
                        candidate.created_at_ms = stored.created_at_ms;
                        candidate.updated_at_ms = stored.updated_at_ms;
                    }
                    for (candidate, stored) in request.native.iter_mut().zip(&original.native) {
                        candidate.operation_id = stored.operation_id.clone();
                    }
                    if serde_json::to_value(&request).ok() != serde_json::to_value(&original).ok() {
                        return Err(error);
                    }
                    request = original;
                    repository.prepare_joint_operation(request.clone()).await?
                }
                Err(error) => return Err(error),
            };
            if let Some(pending) = repository
                .pending_joint_operations()
                .await?
                .into_iter()
                .find(|p| p.admission == admission)
            {
                pending
            } else {
                let committed = repository
                    .get(request.context.target_runtime_id.as_deref().unwrap())
                    .await?
                    .ok_or_else(|| unavailable("open replay has no pending or committed result"))?;
                if request.publication_input.as_ref() != Some(&committed) {
                    return Err(unavailable("open replay committed input changed"));
                }
                return Ok(committed);
            }
        };
        self.resume(pending, repository).await
    }

    async fn submit_turn(
        &self,
        context: JointOperationContext,
        session: SessionRecord,
        request: session_protocol::SessionTurnRequest,
        turn_id: String,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<JointTurnSubmission, SessionDomainError> {
        let admitted = self
            .admit_message(
                context,
                session,
                request,
                turn_id.clone(),
                repository.clone(),
            )
            .await?;
        if admitted.identity.turn_id != turn_id {
            return Ok(JointTurnSubmission {
                turn_id: admitted.identity.turn_id,
                terminal: None,
            });
        }
        let owner = self.clone();
        Ok(JointTurnSubmission {
            turn_id,
            terminal: Some(Box::pin(async move {
                let pending = repository
                    .pending_joint_operations()
                    .await?
                    .into_iter()
                    .find(|p| p.admission == admitted.admission);
                match pending {
                    Some(pending) => owner.resume_message(pending, repository).await,
                    None => repository
                        .joint_turn_result(admitted.admission)
                        .await?
                        .ok_or_else(|| unavailable("Message intent missing")),
                }
            })),
        })
    }
    async fn reconcile(
        &self,
        pending: JointPendingOperation,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<(), SessionDomainError> {
        if pending.request.context.operation == "turn" {
            self.resume_message(pending, repository).await?;
            return Ok(());
        }
        if pending.stage == "compensating"
            || pending.native.iter().any(|(n, o)| {
                n.step.starts_with("compensate_")
                    || ((n.step == "create" || n.step == "start")
                        && o.as_ref().is_some_and(|o| {
                            matches!(
                                o.state,
                                NativeOperationState::Failed | NativeOperationState::NotSent
                            )
                        }))
            })
        {
            return self.compensate_unpublished(pending, repository).await;
        }
        self.resume(pending, repository).await?;
        Ok(())
    }
    async fn check_alive(
        &self,
        record: &SessionRecord,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<bool, SessionDomainError> {
        let id = self.bound_root(record, repository).await?;
        let client = self.connect().await?;
        let result = client
            .thinkthreads()
            .get(sdk::ThinkThreadTargetRequest {
                thinkthread_id: id.clone(),
            })
            .await;
        let alive = match result {
            Ok(view) => self.validate_view(&id, &view, false).is_ok(),
            Err(sdk::ManagementError::Rejected(body)) if body.code == "ThinkThreadNotFound" => {
                false
            }
            Err(_) => return Err(unavailable("Root liveness observation failed")),
        };
        client
            .close()
            .await
            .map_err(|_| unavailable("Management close failed"))?;
        Ok(alive)
    }
    async fn ensure_attached(
        &self,
        record: &SessionRecord,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<(), SessionDomainError> {
        let id = self.bound_root(record, repository).await?;
        let client = self.connect().await?;
        let view = client
            .thinkthreads()
            .get(sdk::ThinkThreadTargetRequest {
                thinkthread_id: id.clone(),
            })
            .await
            .map_err(|_| unavailable("Root is unavailable"))?;
        self.validate_view(&id, &view, false)?;
        client
            .close()
            .await
            .map_err(|_| unavailable("Management close failed"))?;
        Ok(())
    }
}
fn unavailable(message: &str) -> SessionDomainError {
    SessionDomainError::Unavailable {
        message: message.into(),
    }
}
fn timeout(operation: &str, timeout_ms: u64) -> SessionDomainError {
    SessionDomainError::Timeout {
        operation: operation.into(),
        timeout_ms,
    }
}
fn empty(value: &Value) -> bool {
    value.is_null() || value.as_object().is_some_and(|v| v.is_empty())
}
fn map_bridge(error: DurableOperationError) -> SessionDomainError {
    match error {
        DurableOperationError::Repository(error) => error,
        _ => unavailable("durable Management operation is not settled"),
    }
}
