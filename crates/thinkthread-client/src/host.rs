//! Thin, request-bound current-view Host adapter. No server assembly is installed.
use crate::{sdk, ManagementConfig};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use operation_protocol::{
    capability::{exec::*, export::*, filesystem::*, path::*, search::*},
    *,
};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc, time::Duration};
use xgovernor_core::*;

pub fn management_config_digest(c: &ManagementConfig) -> String {
    host_digest(
        &json!({"executable":c.host_executable,"state":c.state_root,"runtime":c.runtime_root,"config":c.config_root,"cache":c.cache_root,"working":c.working_directory,"fs":c.fs_root,"home":c.home_directory,"path":c.executable_search_path,"environment":c.base_environment}),
    )
}
fn store(r: &Arc<dyn SessionRepository>) -> Result<&dyn HostRepository, SessionDomainError> {
    r.host_repository()
        .ok_or_else(|| unsupported_joint("host_repository"))
}
fn op_error(error: SessionDomainError) -> OperationError {
    match error {
        SessionDomainError::UnsupportedCapability { .. } => OperationError::Unsupported {
            message: error.to_string(),
        },
        SessionDomainError::InvalidRequest { message } => OperationError::InvalidPath { message },
        _ => OperationError::Transport {
            message: error.to_string(),
        },
    }
}
pub struct CandidateHostOwner {
    config: ManagementConfig,
    owner_kind: String,
    revision: String,
    observation_timeout: Duration,
}
impl CandidateHostOwner {
    pub fn new(
        config: ManagementConfig,
        owner_kind: String,
        revision: String,
        observation_timeout: Duration,
    ) -> Result<Self, SessionDomainError> {
        config
            .validate()
            .map_err(|_| host_invalid("invalid fixed Management configuration"))?;
        if owner_kind.is_empty() || revision.is_empty() || observation_timeout.is_zero() {
            return Err(host_invalid("invalid Host owner configuration"));
        }
        Ok(Self {
            config,
            owner_kind,
            revision,
            observation_timeout,
        })
    }
    fn validate_policy(&self, p: &HostCapabilityPolicyV1) -> Result<(), SessionDomainError> {
        p.validate()?;
        if p.configuration_revision != self.revision
            || p.sdk_fingerprint != sdk::MANAGEMENT_CONTRACT_FINGERPRINT
            || p.frozen_host_config_digest != management_config_digest(&self.config)
        {
            return Err(host_unavailable("frozen Host configuration or SDK changed"));
        }
        Ok(())
    }
    pub async fn recover_host(
        &self,
        id: String,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<Value, SessionDomainError> {
        let h = store(&repository)?.host_intent(id).await?;
        self.validate_policy(&h.prepared.policy)?;
        if h.prepared.target.live_binding.owner_kind != self.owner_kind {
            return Err(host_unavailable("persisted Host owner changed"));
        }
        if let Some(result) = h.result {
            return result_value(result);
        }
        CandidateHostBackend::new(
            self.config.clone(),
            repository,
            h.access,
            h.prepared,
            self.observation_timeout,
        )
        .run(None)
        .await
    }
}
#[async_trait]
impl JointSessionLifecycle for CandidateHostOwner {
    fn owner_kind(&self) -> &str {
        &self.owner_kind
    }
    fn configuration_revision(&self) -> &str {
        &self.revision
    }
    async fn prepare_host_operation(
        &self,
        mut context: JointOperationContext,
        session: SessionRecord,
        request: JointHostRequest,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<JointHostAccess, SessionDomainError> {
        let (target, policy) = store(&repository)?
            .host_target(session.runtime_id.clone())
            .await?;
        self.validate_policy(&policy)?;
        if session.execution_backing.joint() != Some(&target.live_binding)
            || target.live_binding.owner_kind != self.owner_kind
            || context.owner_ref != target.owner_ref
            || context.source_runtime_id.as_deref() != Some(&session.runtime_id)
            || context.runtime_kind != session.runtime.runtime_kind
            || context.tenant_id != session.tenant_id
        {
            return Err(host_unavailable(
                "Host authenticated Session target mismatch",
            ));
        }
        let canonical = policy.preflight(&request)?;
        context.operation = canonical.operation().into();
        let client = self
            .config
            .connect()
            .await
            .map_err(|_| host_unavailable("Host connection failed"))?;
        let mut native = Vec::new();
        for (method, params) in sdk_params(&target, &canonical) {
            let digest = sdk::operation_digest(client.context(), &method, &params)
                .map_err(|_| host_invalid("Host SDK digest failed"))?;
            native.push(JointNativeIntent {
                step: format!("host-{}", native.len()),
                operation_id: uuid::Uuid::new_v4().to_string(),
                digest,
                method,
                params,
                effect: NativeEffect::Mutate,
                compensation: false,
            });
        }
        client
            .close()
            .await
            .map_err(|_| host_unavailable("Host preflight connection failed"))?;
        let prepared = HostPreparedV1 {
            context,
            target: target.clone(),
            policy,
            request: canonical,
            native,
        };
        let access = store(&repository)?.prepare_host(prepared.clone()).await?;
        let backend = Arc::new(CandidateHostBackend::new(
            self.config.clone(),
            repository,
            access.clone(),
            prepared,
            self.observation_timeout,
        ));
        Ok(JointHostAccess {
            access,
            target,
            backend,
        })
    }
}
fn sdk_params(t: &HostTargetV1, r: &HostCanonicalRequestV1) -> Vec<(String, Value)> {
    match r {
        HostCanonicalRequestV1::Read { .. } => vec![],
        HostCanonicalRequestV1::Exec {
            argv,
            cwd,
            timeout_ms,
            max_output_bytes,
        } => vec![(
            "thinkthread.exec".into(),
            json!({"thinkthreadId":t.native_child_id,"argv":argv,"cwd":cwd,"timeoutMs":timeout_ms.to_string(),"maxOutputBytes":max_output_bytes}),
        )],
        HostCanonicalRequestV1::Write {
            path,
            size,
            sha256,
            max_bytes,
            mkdir,
        } => {
            let mut v = Vec::new();
            if let Some(m) = mkdir {
                v.push(("thinkthread.exec".into(),json!({"thinkthreadId":t.native_child_id,"argv":m.argv,"cwd":m.cwd,"timeoutMs":m.timeout_ms.to_string(),"maxOutputBytes":m.max_output_bytes})));
            }
            v.push(("thinkthread.write".into(),json!({"thinkthreadId":t.native_child_id,"path":path,"maxBytes":max_bytes,"byteLength":size,"sha256":sha256})));
            v
        }
    }
}
pub struct CandidateHostBackend {
    config: ManagementConfig,
    repository: Arc<dyn SessionRepository>,
    access: HostAccessTokenV1,
    prepared: HostPreparedV1,
    root: BackendPath,
    home: Option<BackendPath>,
    observation_timeout: Duration,
}
impl CandidateHostBackend {
    fn new(
        config: ManagementConfig,
        repository: Arc<dyn SessionRepository>,
        access: HostAccessTokenV1,
        prepared: HostPreparedV1,
        observation_timeout: Duration,
    ) -> Self {
        let root = BackendPath(prepared.policy.paths.workspace_backend_root.clone());
        let home = prepared.policy.paths.home_relative.as_ref().map(|path| {
            let workspace = prepared
                .policy
                .paths
                .fs_relative_workspace
                .trim_end_matches('/');
            let relative = if workspace.is_empty() || workspace == "." {
                path.as_str()
            } else {
                path.strip_prefix(workspace)
                    .unwrap_or(path)
                    .trim_start_matches('/')
            };
            BackendPath(if relative.is_empty() || relative == "." {
                root.0.clone()
            } else {
                format!("{}/{}", root.0.trim_end_matches('/'), relative)
            })
        });
        Self {
            config,
            repository,
            access,
            prepared,
            root,
            home,
            observation_timeout,
        }
    }
    fn verify_view(&self, v: &sdk::ThinkThreadView) -> Result<(), SessionDomainError> {
        let t = &self.prepared.target;
        if v.thinkthread_id != t.native_child_id
            || v.root_id != t.root_id
            || v.parent_id.as_deref() != Some(&t.root_id)
            || v.fs_branch_id.as_deref() != Some(&t.branch_id)
            || v.fs != self.config.fs_root
            || v.needs_recovery
            || !v.state.eq_ignore_ascii_case("running")
            || !v
                .session_state
                .as_deref()
                .is_some_and(|s| s.eq_ignore_ascii_case("ready"))
        {
            return Err(host_unavailable(
                "exact private Child is not verified Ready",
            ));
        }
        Ok(())
    }
    fn matches(&self, r: &JointHostRequest) -> Result<(), SessionDomainError> {
        if self.prepared.policy.preflight(r)? != self.prepared.request {
            return Err(host_invalid(
                "Host access is bound to another complete request",
            ));
        }
        Ok(())
    }
    fn transfer(&self, state: &str, bytes: Option<&[u8]>) -> Option<HostFileTransferV1> {
        let (path, size, sha) = match &self.prepared.request {
            HostCanonicalRequestV1::Read { path, .. } => (path, None, None),
            HostCanonicalRequestV1::Write {
                path, size, sha256, ..
            } => (path, Some(*size), Some(sha256.clone())),
            _ => return None,
        };
        Some(HostFileTransferV1 {
            relative_path: path.clone(),
            expected_size: size,
            expected_sha256: sha,
            observed_size: bytes.map(|b| b.len() as u64),
            observed_sha256: bytes.map(host_bytes_digest),
            state: state.into(),
        })
    }
    async fn run(&self, content: Option<&[u8]>) -> Result<Value, SessionDomainError> {
        let repository = store(&self.repository)?;
        let h = repository
            .host_intent(self.access.admission.intent_id.clone())
            .await?;
        if h.access != self.access {
            return Err(host_unavailable("Host access epoch changed"));
        }
        if let Some(result) = h.result {
            return result_value(result);
        }
        if content.is_none()
            && matches!(self.prepared.request, HostCanonicalRequestV1::Write { .. })
            && h.observations.iter().all(Option::is_none)
        {
            return self
                .fail_before_claim("unclaimed write has no retained payload; no send attempted")
                .await;
        }
        let client = match self.config.connect().await {
            Ok(client) => client,
            Err(_) => return self.fail_before_claim("Host connection failed").await,
        };
        let result =
            tokio::time::timeout(self.observation_timeout, self.observe(&client, content)).await;
        let _ = client.close().await;
        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                let current = store(&self.repository)?
                    .host_intent(self.access.admission.intent_id.clone())
                    .await?;
                if current.observations.iter().all(Option::is_none) {
                    return self.fail_before_claim(&error.to_string()).await;
                }
                if current.state != "unknown"
                    && current.observations.iter().flatten().any(|o| !o.settled)
                {
                    let _ = self.unknown(&error.to_string()).await;
                }
                Err(error)
            }
            Err(_) => {
                let current = repository
                    .host_intent(self.access.admission.intent_id.clone())
                    .await?;
                if current.observations.iter().all(Option::is_none) {
                    self.fail_before_claim("Host target observation timed out before send")
                        .await
                } else {
                    self.unknown("Host observation timed out; original intent retained")
                        .await
                }
            }
        }
    }
    async fn fail_before_claim(&self, message: &str) -> Result<Value, SessionDomainError> {
        let repository = store(&self.repository)?;
        let current = repository
            .host_intent(self.access.admission.intent_id.clone())
            .await?;
        if current.observations.iter().all(Option::is_none) {
            repository
                .finish_host(self.access.clone(), json!({"error":message}))
                .await?;
        }
        Err(host_unavailable(message))
    }
    async fn observe(
        &self,
        client: &sdk::ThinkThreadClient,
        content: Option<&[u8]>,
    ) -> Result<Value, SessionDomainError> {
        let repository = store(&self.repository)?;
        let view = client
            .thinkthreads()
            .get(sdk::ThinkThreadTargetRequest {
                thinkthread_id: self.prepared.target.native_child_id.clone(),
            })
            .await
            .map_err(|_| host_unavailable("Host target observation failed"))?;
        self.verify_view(&view)?;
        for n in &self.prepared.native {
            if sdk::operation_digest(client.context(), &n.method, &n.params)
                .map_err(|_| host_unavailable("Host canonical digest unavailable"))?
                != n.digest
            {
                return Err(host_unavailable(
                    "Host connection context changed before send",
                ));
            }
        }
        if let HostCanonicalRequestV1::Read { path, max_bytes } = &self.prepared.request {
            if !repository.claim_host_step(self.access.clone(), 0).await? {
                let h = repository
                    .host_intent(self.access.admission.intent_id.clone())
                    .await?;
                if let Some(o) = h.observations[0]
                    .as_ref()
                    .filter(|o| o.settled && o.error.is_some())
                {
                    return self.finish(o.clone()).await;
                }
                return Err(host_unavailable(
                    "read observation was interrupted; stable evidence unavailable",
                ));
            }
            let result = match client
                .thinkthreads()
                .read(sdk::ThinkThreadReadRequest {
                    thinkthread_id: self.prepared.target.native_child_id.clone(),
                    path: PathBuf::from(path),
                    max_bytes: *max_bytes,
                })
                .await
            {
                Ok(raw) => {
                    if raw.thinkthread_id != self.prepared.target.native_child_id {
                        return self.unknown("read result target mismatch").await;
                    }
                    let bytes = STANDARD
                        .decode(&raw.data_base64)
                        .map_err(|_| host_unavailable("invalid SDK read encoding"))?;
                    if bytes.len() > *max_bytes as usize {
                        return self.unknown("read result exceeds admitted size").await;
                    }
                    HostObservationV1 {
                        state: NativeOperationState::Completed,
                        settled: true,
                        result: json!({"data_base64":raw.data_base64}),
                        transfer: self.transfer("completed", Some(&bytes)),
                        exec: None,
                        error: None,
                        native_outcome: None,
                    }
                }
                Err(e) => self.error_observation(e, true),
            };
            if result.error.is_some() {
                repository
                    .record_host_step(self.access.clone(), 0, result.clone())
                    .await?;
                return self.finish(result).await;
            }
            let after = client
                .thinkthreads()
                .get(sdk::ThinkThreadTargetRequest {
                    thinkthread_id: self.prepared.target.native_child_id.clone(),
                })
                .await;
            let stable = match &after {
                Ok(after) => {
                    self.verify_view(after).is_ok()
                        && view.content_revision == after.content_revision
                }
                Err(_) => false,
            };
            let local = repository
                .host_target(self.prepared.target.public_runtime_id.clone())
                .await;
            if !matches!(&local, Ok((target, policy)) if target == &self.prepared.target && policy == &self.prepared.policy)
            {
                return result_value(repository.fail_stale_host_read(self.access.clone()).await?);
            }
            if !stable {
                let mut failed = result;
                failed.state = NativeOperationState::Failed;
                failed.error = Some("artifact target or revision changed during read".into());
                failed.result = Value::Null;
                repository
                    .record_host_step(self.access.clone(), 0, failed.clone())
                    .await?;
                return self.finish(failed).await;
            }
            if repository
                .record_host_step(self.access.clone(), 0, result.clone())
                .await
                .is_err()
            {
                return result_value(repository.fail_stale_host_read(self.access.clone()).await?);
            }
            return result_value(
                repository
                    .finish_host_read(
                        self.access.clone(),
                        result.result,
                        serde_json::to_value(view.content_revision)
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .to_owned(),
                        serde_json::to_value(after.unwrap().content_revision)
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .to_owned(),
                    )
                    .await?,
            );
        }
        for step in 0..self.prepared.native.len() {
            let n = &self.prepared.native[step];
            let first = repository
                .claim_host_step(self.access.clone(), step)
                .await?;
            let observation = if !first {
                let h = repository
                    .host_intent(self.access.admission.intent_id.clone())
                    .await?;
                if let Some(o) = h.observations[step].as_ref().filter(|o| o.settled) {
                    o.clone()
                } else {
                    self.query_step(client, step).await?
                }
            } else if n.method == "thinkthread.write" {
                let Some(bytes) = content else {
                    let not_sent = HostObservationV1 {
                        state: NativeOperationState::NotSent,
                        settled: true,
                        result: Value::Null,
                        transfer: self.transfer("failed", None),
                        exec: None,
                        error: Some("unclaimed write has no retained payload; not sent".into()),
                        native_outcome: None,
                    };
                    repository
                        .record_host_step(self.access.clone(), step, not_sent.clone())
                        .await?;
                    return self.finish(not_sent).await;
                };
                let HostCanonicalRequestV1::Write {
                    path,
                    size,
                    sha256,
                    max_bytes,
                    ..
                } = &self.prepared.request
                else {
                    unreachable!()
                };
                if bytes.len() as u64 != *size || host_bytes_digest(bytes) != *sha256 {
                    return self.unknown("write payload mismatch").await;
                }
                match client
                    .thinkthreads()
                    .write_with_operation(
                        n.operation_id.clone(),
                        sdk::ThinkThreadWriteRequest {
                            thinkthread_id: self.prepared.target.native_child_id.clone(),
                            path: PathBuf::from(path),
                            max_bytes: *max_bytes,
                            data_base64: STANDARD.encode(bytes),
                        },
                    )
                    .await
                {
                    Ok(raw) => self.write_observation(raw, step)?,
                    Err(e) => self.mutation_error(client, step, e).await?,
                }
            } else {
                let req: sdk::ThinkThreadExecRequest = serde_json::from_value(n.params.clone())
                    .map_err(|_| host_invalid("Host exec encoding"))?;
                match client
                    .thinkthreads()
                    .exec_with_operation(n.operation_id.clone(), req)
                    .await
                {
                    Ok(raw) => self.exec_observation(raw, step)?,
                    Err(e) => self.mutation_error(client, step, e).await?,
                }
            };
            repository
                .record_host_step(self.access.clone(), step, observation.clone())
                .await?;
            if observation.error.is_some()
                || !observation.settled
                || step + 1 == self.prepared.native.len()
            {
                return self.finish(observation).await;
            }
        }
        Err(host_unavailable("Host operation has no executable step"))
    }
    async fn unknown(&self, message: &str) -> Result<Value, SessionDomainError> {
        let repository = store(&self.repository)?;
        let h = repository
            .host_intent(self.access.admission.intent_id.clone())
            .await?;
        if let Some(step) = h
            .observations
            .iter()
            .position(|o| o.as_ref().is_some_and(|o| !o.settled))
        {
            repository
                .record_host_step(
                    self.access.clone(),
                    step,
                    HostObservationV1 {
                        state: NativeOperationState::Unknown,
                        settled: false,
                        result: Value::Null,
                        transfer: self.transfer("unknown", None),
                        exec: None,
                        error: Some(message.into()),
                        native_outcome: None,
                    },
                )
                .await?;
        }
        Err(host_unavailable(message))
    }
    async fn mutation_error(
        &self,
        client: &sdk::ThinkThreadClient,
        step: usize,
        error: sdk::ManagementError,
    ) -> Result<HostObservationV1, SessionDomainError> {
        let query = matches!(&error, sdk::ManagementError::Rejected(body) if body.delivery==sdk::Delivery::Accepted && body.code!="NeedsRecovery" && body.code!="OperationInProgress");
        let observation = self.error_observation(error, false);
        if !query {
            return Ok(observation);
        }
        store(&self.repository)?.record_host_recovery(self.access.clone(),json!({"kind":"accepted_rejection","step":step,"original_uuid":self.prepared.native[step].operation_id,"response":observation.native_outcome})).await?;
        store(&self.repository)?
            .record_host_step(self.access.clone(), step, observation)
            .await?;
        self.query_step(client, step).await
    }

    async fn failed_postcondition(
        &self,
        client: &sdk::ThinkThreadClient,
        step: usize,
    ) -> Result<bool, SessionDomainError> {
        let n = &self.prepared.native[step];
        let (confirmed, observation) = if let HostCanonicalRequestV1::Write {
            path,
            size,
            sha256,
            max_bytes,
            ..
        } = &self.prepared.request
        {
            if n.method != "thinkthread.write" {
                (
                    false,
                    json!({"reason":"exec business postcondition and settlement evidence are not declared"}),
                )
            } else {
                match client
                    .thinkthreads()
                    .read(sdk::ThinkThreadReadRequest {
                        thinkthread_id: self.prepared.target.native_child_id.clone(),
                        path: PathBuf::from(path),
                        max_bytes: *max_bytes,
                    })
                    .await
                {
                    Ok(raw) => {
                        let bytes = STANDARD
                            .decode(&raw.data_base64)
                            .map_err(|_| host_unavailable("postcondition read encoding invalid"))?;
                        let confirmed = raw.thinkthread_id == self.prepared.target.native_child_id
                            && bytes.len() <= *max_bytes as usize
                            && (bytes.len() as u64 != *size
                                || host_bytes_digest(&bytes) != *sha256);
                        (
                            confirmed,
                            json!({"thinkthread_id":raw.thinkthread_id,"length":bytes.len(),"sha256":host_bytes_digest(&bytes)}),
                        )
                    }
                    Err(error) => {
                        let confirmed = matches!(&error,sdk::ManagementError::Rejected(body) if body.code=="FileOperationFailed" && (body.message=="filesystem branch: invalid execution policy: target is not a regular file" || body.message=="filesystem branch: invalid execution policy: only regular file reads are supported" || (body.message.starts_with("filesystem branch: ") && body.message.ends_with("No such file or directory (os error 2)"))));
                        let raw = match &error {
                            sdk::ManagementError::Rejected(body) => json!({"error":body}),
                            _ => json!({"error":error.to_string()}),
                        };
                        (confirmed, raw)
                    }
                }
            }
        } else {
            (
                false,
                json!({"reason":"exec business postcondition and settlement evidence are not declared"}),
            )
        };
        store(&self.repository)?.record_host_recovery(self.access.clone(),json!({"kind":"failed_write_postcondition","step":step,"original_uuid":n.operation_id,"target_digest":self.access.target_digest,"confirmed":confirmed,"observation":observation})).await?;
        Ok(confirmed)
    }
    fn error_observation(&self, e: sdk::ManagementError, read: bool) -> HostObservationV1 {
        let recovery_required = matches!(&e, sdk::ManagementError::Rejected(body)
            if body.code == "NeedsRecovery" || body.code == "OperationInProgress");
        let settled = read || (!recovery_required && e.delivery() == Some(sdk::Delivery::NotSent));
        let native_outcome = match &e {
            sdk::ManagementError::Rejected(body) => Some(json!({"error":body})),
            _ => None,
        };
        HostObservationV1 {
            state: if settled {
                NativeOperationState::Failed
            } else {
                NativeOperationState::Unknown
            },
            settled,
            result: json!({"error":e.to_string()}),
            transfer: self.transfer(if settled { "failed" } else { "unknown" }, None),
            exec: None,
            error: Some(e.to_string()),
            native_outcome,
        }
    }
    fn validate_handle(
        &self,
        handle: &sdk::OperationHandle,
        step: usize,
    ) -> Result<(), SessionDomainError> {
        let n = &self.prepared.native[step];
        if handle.scope != sdk::OperationScope::ThinkThread
            || handle.operation_id != n.operation_id
            || handle.operation_digest != n.digest
        {
            return Err(host_unavailable("Host original operation handle mismatch"));
        }
        Ok(())
    }
    fn write_observation(
        &self,
        raw: sdk::ThinkThreadWriteResult,
        step: usize,
    ) -> Result<HostObservationV1, SessionDomainError> {
        self.validate_handle(&raw.operation, step)?;
        let native_outcome = Some(
            serde_json::to_value(&raw).map_err(|_| host_unavailable("write outcome encoding"))?,
        );
        let HostCanonicalRequestV1::Write { size, .. } = &self.prepared.request else {
            return Err(host_invalid("not a write"));
        };
        if raw.thinkthread_id != self.prepared.target.native_child_id
            || u64::from(raw.bytes_written) != *size
        {
            return Err(host_unavailable(
                "Host write result target or length mismatch",
            ));
        }
        let mut transfer = self.transfer("completed", None).unwrap();
        transfer.observed_size = Some(*size);
        Ok(HostObservationV1 {
            state: NativeOperationState::Completed,
            settled: true,
            result: json!({"bytes_written":size}),
            transfer: Some(transfer),
            exec: None,
            error: None,
            native_outcome,
        })
    }
    fn exec_observation(
        &self,
        raw: sdk::ThinkThreadExecResult,
        step: usize,
    ) -> Result<HostObservationV1, SessionDomainError> {
        self.validate_handle(&raw.operation, step)?;
        let native_outcome = Some(
            serde_json::to_value(&raw).map_err(|_| host_unavailable("exec outcome encoding"))?,
        );
        if raw.thinkthread_id != self.prepared.target.native_child_id {
            return Err(host_unavailable("Host exec result target mismatch"));
        }
        let stdout = STANDARD
            .decode(&raw.stdout_base64)
            .map_err(|_| host_unavailable("Host stdout encoding"))?;
        let stderr = STANDARD
            .decode(&raw.stderr_base64)
            .map_err(|_| host_unavailable("Host stderr encoding"))?;
        let reason = serde_json::to_value(raw.reason)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();
        let complete = stdout.len() + stderr.len()
            <= self.prepared.policy.max_output_bytes as usize
            && reason == "exited"
            && raw.code.is_some();
        let settled = (complete && self.prepared.policy.exited_settlement_declared)
            || (reason == "timeout" && self.prepared.policy.timeout_settlement_declared);
        let parent_failed = self.prepared.native.len() > 1 && step == 0 && raw.code != Some(0);
        let error = if parent_failed {
            Some("Host parent creation exited unsuccessfully".into())
        } else if complete {
            None
        } else {
            Some(format!(
                "Host exec ended with {reason}; output is not a complete successful result"
            ))
        };
        Ok(HostObservationV1 {
            state: NativeOperationState::Completed,
            settled,
            result: json!({"stdout_base64":raw.stdout_base64,"stderr_base64":raw.stderr_base64,"exit_code":if reason=="exited"{raw.code}else{None},"timed_out":reason=="timeout","reason":reason,"native_code":raw.code}),
            transfer: None,
            exec: Some(HostExecOutcomeV1 {
                reason,
                code: raw.code,
                stdout_len: stdout.len() as u64,
                stderr_len: stderr.len() as u64,
                stdout_digest: host_bytes_digest(&stdout),
                stderr_digest: host_bytes_digest(&stderr),
                timed_out: matches!(raw.reason, sdk::FileExecReason::Timeout),
                completeness: complete,
            }),
            error,
            native_outcome,
        })
    }
    async fn query_step(
        &self,
        client: &sdk::ThinkThreadClient,
        step: usize,
    ) -> Result<HostObservationV1, SessionDomainError> {
        let n = &self.prepared.native[step];
        if sdk::operation_digest(client.context(), &n.method, &n.params)
            .map_err(|_| host_unavailable("Host recovery digest"))?
            != n.digest
        {
            return Err(host_unavailable("Host recovery context changed"));
        }
        let view = match client
            .get_operation(sdk::OperationGetRequest {
                scope: sdk::OperationScope::ThinkThread,
                operation_id: n.operation_id.clone(),
            })
            .await
        {
            Ok(view) => view,
            Err(error) => {
                let raw = match &error {
                    sdk::ManagementError::Rejected(body) => json!({"error":body}),
                    _ => json!({"error":error.to_string()}),
                };
                store(&self.repository)?.record_host_recovery(self.access.clone(),json!({"kind":"operation_query_error","original_uuid":n.operation_id,"response":raw})).await?;
                return Err(host_unavailable(
                    "original Host operation unavailable; retained unknown",
                ));
            }
        };
        store(&self.repository)?
            .record_host_recovery(
                self.access.clone(),
                serde_json::to_value(&view)
                    .map_err(|_| host_unavailable("Host recovery observation encoding"))?,
            )
            .await?;
        self.validate_handle(&view.operation, step)?;
        if view.method != n.method {
            return Err(host_unavailable("original Host operation method mismatch"));
        }
        if n.params["thinkthreadId"] != self.prepared.target.native_child_id {
            return Err(host_unavailable("original Host target changed"));
        }
        if view.state == sdk::OperationState::Failed
            && view.completed_at.is_some()
            && view
                .error
                .as_ref()
                .is_some_and(|e| e.code != "NeedsRecovery" && e.code != "OperationInProgress")
        {
            if view
                .error
                .as_ref()
                .and_then(|e| e.details.as_ref())
                .and_then(|d| d.get("thinkthreadId"))
                .is_some_and(|id| id != &json!(self.prepared.target.native_child_id))
            {
                return Err(host_unavailable("failed Host result target mismatch"));
            }
            let settled = self.failed_postcondition(client, step).await?;
            return Ok(HostObservationV1 {
                state: NativeOperationState::Failed,
                settled,
                result: Value::Null,
                transfer: self.transfer(if settled { "failed" } else { "unknown" }, None),
                exec: None,
                error: Some("original Host operation failed; postcondition checked".into()),
                native_outcome: Some(serde_json::to_value(view).unwrap()),
            });
        }
        if view.state != sdk::OperationState::Succeeded {
            let state = match view.state {
                sdk::OperationState::NeedsRecovery => NativeOperationState::NeedsRecovery,
                sdk::OperationState::Failed => NativeOperationState::Failed,
                _ => NativeOperationState::Accepted,
            };
            let h = store(&self.repository)?
                .host_intent(self.access.admission.intent_id.clone())
                .await?;
            if !h.observations[step].as_ref().is_some_and(|o| {
                matches!(
                    o.state,
                    NativeOperationState::Completed | NativeOperationState::Failed
                )
            }) {
                store(&self.repository)?
                    .record_host_step(
                        self.access.clone(),
                        step,
                        HostObservationV1 {
                            state,
                            settled: false,
                            result: Value::Null,
                            transfer: self.transfer("unknown", None),
                            exec: None,
                            error: Some("original Host operation is not settled".into()),
                            native_outcome: Some(serde_json::to_value(view).unwrap()),
                        },
                    )
                    .await?;
            }
            return Err(host_unavailable(
                "original Host operation has no settled complete result",
            ));
        }
        let mut raw = view
            .result
            .ok_or_else(|| host_unavailable("original Host result is missing"))?;
        if raw.get("operation").is_none() {
            raw["operation"] = serde_json::to_value(view.operation).unwrap();
        }
        let observation = if n.method == "thinkthread.write" {
            self.write_observation(
                serde_json::from_value(raw)
                    .map_err(|_| host_unavailable("original write result invalid"))?,
                step,
            )?
        } else {
            self.exec_observation(
                serde_json::from_value(raw)
                    .map_err(|_| host_unavailable("original exec result invalid"))?,
                step,
            )?
        };
        store(&self.repository)?
            .record_host_step(self.access.clone(), step, observation.clone())
            .await?;
        Ok(observation)
    }
    async fn finish(&self, o: HostObservationV1) -> Result<Value, SessionDomainError> {
        if !o.settled {
            return Err(host_unavailable(
                "Host execution or effect is unknown; owner, allocation and fence retained",
            ));
        }
        let result = if let Some(error) = o.error {
            json!({"error":error})
        } else {
            o.result
        };
        let result = store(&self.repository)?
            .finish_host(self.access.clone(), result)
            .await?;
        result_value(result)
    }
}
fn result_value(result: Value) -> Result<Value, SessionDomainError> {
    if result.get("unsupported").and_then(Value::as_bool) == Some(true) {
        return Err(unsupported_joint("host_request"));
    }
    if let Some(e) = result.get("error").and_then(Value::as_str) {
        Err(host_unavailable(e))
    } else {
        Ok(result)
    }
}
#[async_trait]
impl JointHostBackend for CandidateHostBackend {
    async fn write_host(&self, request: JointHostRequest) -> Result<u64, SessionDomainError> {
        self.matches(&request)?;
        let JointHostRequest::Write { content, .. } = request else {
            return Err(host_invalid("not a public write"));
        };
        let r = self.run(Some(&content)).await?;
        r["bytes_written"]
            .as_u64()
            .ok_or_else(|| host_unavailable("write result missing size"))
    }
}
#[async_trait]
impl OperationBackend for CandidateHostBackend {
    fn backend_id(&self) -> &str {
        "joint-host"
    }
    fn capabilities(&self) -> OperationBackendCapabilities {
        OperationBackendCapabilities {
            supports_atomic_write: false,
            supports_grep: false,
            supports_export_file: false,
            supports_lsp: false,
        }
    }
    fn paths(&self) -> &dyn OperationPathResolver {
        self
    }
    fn files(&self) -> &dyn OperationFileSystem {
        self
    }
    fn search(&self) -> &dyn OperationSearch {
        self
    }
    fn exec(&self) -> &dyn OperationExec {
        self
    }
    fn export(&self) -> &dyn OperationExport {
        self
    }
    async fn shutdown(&self) -> Result<(), OperationError> {
        Ok(())
    }
}
#[async_trait]
impl OperationPathResolver for CandidateHostBackend {
    fn workspace_root(&self) -> &BackendPath {
        &self.root
    }
    fn home_dir(&self) -> Option<&BackendPath> {
        self.home.as_ref()
    }
    async fn resolve_path(&self, r: ResolvePathRequest) -> Result<BackendPath, OperationError> {
        let raw_path = match r.base {
            ResolveBase::WorkspaceRoot => r.raw_path,
            ResolveBase::HomeDir if self.home.is_some() => {
                if r.raw_path.starts_with('/') || r.raw_path.starts_with('~') {
                    r.raw_path
                } else {
                    format!("~/{}", r.raw_path)
                }
            }
            _ => {
                return Err(OperationError::Unsupported {
                    message: "Host path base".into(),
                })
            }
        };
        let mapped = self
            .prepared
            .policy
            .paths
            .resolve(&raw_path)
            .map_err(op_error)?;
        let base = &self.prepared.policy.paths.fs_relative_workspace;
        let relative = if base == "." || base.is_empty() {
            mapped.as_str()
        } else {
            mapped
                .strip_prefix(base)
                .unwrap_or(&mapped)
                .trim_start_matches('/')
        };
        Ok(BackendPath(if relative == "." {
            self.root.0.clone()
        } else {
            format!("{}/{}", self.root.0.trim_end_matches('/'), relative)
        }))
    }
}
#[async_trait]
impl OperationExec for CandidateHostBackend {
    async fn exec(&self, r: ExecRequest) -> Result<ExecResult, OperationError> {
        self.matches(&JointHostRequest::Exec {
            request: r,
            required_output_bytes: None,
        })
        .map_err(op_error)?;
        let v = self.run(None).await.map_err(op_error)?;
        Ok(ExecResult {
            stdout: STANDARD
                .decode(v["stdout_base64"].as_str().unwrap_or(""))
                .map_err(|_| OperationError::Transport {
                    message: "stdout decoding".into(),
                })?,
            stderr: STANDARD
                .decode(v["stderr_base64"].as_str().unwrap_or(""))
                .map_err(|_| OperationError::Transport {
                    message: "stderr decoding".into(),
                })?,
            exit_code: v["exit_code"].as_i64().map(|v| v as i32),
            timed_out: v["timed_out"].as_bool().unwrap_or(false),
        })
    }
}
fn unsupported() -> OperationError {
    OperationError::Unsupported {
        message: "Host method is not declared".into(),
    }
}
#[async_trait]
impl OperationFileSystem for CandidateHostBackend {
    async fn stat(&self, _: &BackendPath) -> Result<PathStat, OperationError> {
        Err(unsupported())
    }
    async fn read_bytes(&self, r: ReadBytesRequest) -> Result<Vec<u8>, OperationError> {
        self.matches(&JointHostRequest::Read { path: r.path.0 })
            .map_err(op_error)?;
        let v = self.run(None).await.map_err(op_error)?;
        STANDARD
            .decode(v["data_base64"].as_str().unwrap_or(""))
            .map_err(|_| OperationError::Transport {
                message: "file decoding".into(),
            })
    }
    async fn write_bytes(&self, _: WriteBytesRequest) -> Result<WriteBytesOutcome, OperationError> {
        Err(unsupported())
    }
    async fn create_dir_all(&self, _: &BackendPath) -> Result<(), OperationError> {
        Err(unsupported())
    }
    async fn temp_path(&self, _: TempPathRequest) -> Result<BackendPath, OperationError> {
        Err(unsupported())
    }
}
#[async_trait]
impl OperationSearch for CandidateHostBackend {
    async fn glob(&self, _: GlobRequest) -> Result<Vec<BackendPath>, OperationError> {
        Err(unsupported())
    }
    async fn grep(&self, _: GrepRequest) -> Result<GrepResult, OperationError> {
        Err(unsupported())
    }
}
#[async_trait]
impl OperationExport for CandidateHostBackend {
    async fn export_file(
        &self,
        _: ExportFileRequest,
    ) -> Result<SharedExportedFileHandle, OperationError> {
        Err(unsupported())
    }
}
