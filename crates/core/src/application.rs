use crate::{
    project_session, project_session_summary, CheckpointLineage, EffectiveCapabilities,
    IsolationBoundary, IsolationFacts, LeaseCheckFailure, OpaqueRuntimeState, RuntimeCapability,
    SecurityContext, SessionDomainError, SessionLease, SessionLeaseTable, SessionRecord,
    SessionStatus, WorkspaceFacts,
};
use agent_runtime_protocol::{
    AgentRuntime, RuntimeCancelRequest, RuntimeEntryContext, RuntimeError, RuntimeEvent,
    RuntimeExecutionContext, RuntimeFailure, RuntimeInteractionRequest as RuntimeInteractionInput,
    RuntimeStartRequest, RuntimeTurnRequest as RuntimeTurnInput,
};
use session_protocol::{
    SessionCheckpointDeleteRequest, SessionCheckpointDeleteResult, SessionCheckpointListResponse,
    SessionCheckpointRequest, SessionCheckpointResult, SessionCheckpointSummary,
    SessionControlResponse, SessionEvent, SessionForkRequest, SessionHeartbeatResponse,
    SessionInteractionRequest, SessionLeaseClaim, SessionLifecycleStatus, SessionListResponse,
    SessionLoadRequest, SessionOpenRequest, SessionOpenResponse, SessionSubmitReceipt,
    SessionTurnRequest, TenantQuotaSnapshot,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use xgovernor_manager::InstanceManager;

fn map_provider_error(error: provider_protocol::ProviderControlError) -> SessionDomainError {
    match error {
        provider_protocol::ProviderControlError::NotFound { resource_ref } => {
            SessionDomainError::NotFound {
                runtime_id: resource_ref,
            }
        }
        provider_protocol::ProviderControlError::InvalidRequest { message } => {
            SessionDomainError::InvalidRequest { message }
        }
        provider_protocol::ProviderControlError::UnsupportedCapability { capability, .. } => {
            SessionDomainError::UnsupportedCapability {
                family: crate::CapabilityFamily::Sandbox,
                capability,
            }
        }
        error => SessionDomainError::Unavailable {
            message: error.to_string(),
        },
    }
}

fn map_runtime_error(error: RuntimeError) -> SessionDomainError {
    match error {
        RuntimeError::InvalidRequest { message, .. } => {
            SessionDomainError::InvalidRequest { message }
        }
        RuntimeError::NotFound { runtime_id } => SessionDomainError::NotFound { runtime_id },
        RuntimeError::Conflict { message, .. } => SessionDomainError::Conflict { message },
        RuntimeError::UnsupportedCapability { capability } => {
            SessionDomainError::UnsupportedCapability {
                family: crate::CapabilityFamily::Runtime,
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

fn runtime_capability(
    capability: agent_runtime_protocol::RuntimeCapability,
) -> Option<session_protocol::SessionRuntimeCapability> {
    Some(match capability {
        agent_runtime_protocol::RuntimeCapability::Interaction => {
            session_protocol::SessionRuntimeCapability::Interaction
        }
        agent_runtime_protocol::RuntimeCapability::Steering => {
            session_protocol::SessionRuntimeCapability::Steering
        }
        agent_runtime_protocol::RuntimeCapability::StateExport => {
            session_protocol::SessionRuntimeCapability::StateExport
        }
        agent_runtime_protocol::RuntimeCapability::ModelOverride => {
            session_protocol::SessionRuntimeCapability::ModelOverride
        }
        agent_runtime_protocol::RuntimeCapability::ReasoningControl => {
            session_protocol::SessionRuntimeCapability::ReasoningControl
        }
        agent_runtime_protocol::RuntimeCapability::Unknown => return None,
    })
}

fn project_runtime_event(runtime_id: &str, turn_id: &str, event: RuntimeEvent) -> SessionEvent {
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

fn provider_id_from_metadata(metadata: &serde_json::Value) -> Option<&str> {
    metadata
        .get("backend_id")
        .or_else(|| metadata.get("tool_backend_id"))
        .and_then(serde_json::Value::as_str)
}

fn provider_for_request(
    registration: &RuntimeRegistration,
    request: &SessionOpenRequest,
) -> Result<(String, Arc<InstanceManager>), SessionDomainError> {
    let provider_id = request
        .ext
        .values()
        .find_map(provider_id_from_metadata)
        .map(str::to_owned)
        .or_else(|| {
            (registration.providers.len() == 1)
                .then(|| registration.providers.keys().next().unwrap().clone())
        })
        .ok_or_else(|| SessionDomainError::InvalidRequest {
            message: "runtime extension must specify backend_id".into(),
        })?;
    let manager = registration
        .providers
        .get(&provider_id)
        .cloned()
        .ok_or_else(|| SessionDomainError::InvalidRequest {
            message: format!("provider backend_id '{provider_id}' is not registered"),
        })?;
    Ok((provider_id, manager))
}

fn provider_for_isolation(
    registration: &RuntimeRegistration,
    isolation: &IsolationFacts,
) -> Result<(String, Arc<InstanceManager>), SessionDomainError> {
    let provider_id = provider_id_from_metadata(&isolation.metadata)
        .map(str::to_owned)
        .or_else(|| {
            (registration.providers.len() == 1)
                .then(|| registration.providers.keys().next().unwrap().clone())
        })
        .ok_or_else(|| SessionDomainError::Internal {
            message: "persisted session is missing provider backend_id".into(),
            source: None,
        })?;
    let manager = registration
        .providers
        .get(&provider_id)
        .cloned()
        .ok_or_else(|| SessionDomainError::InvalidRequest {
            message: format!("provider backend_id '{provider_id}' is not registered"),
        })?;
    Ok((provider_id, manager))
}

fn provider_options(
    workspace: &WorkspaceFacts,
    _isolation: &IsolationFacts,
    provider_id: &str,
) -> serde_json::Value {
    let mut options = serde_json::json!({"workspace_root": workspace.root});
    if provider_id == "e2b" {
        options["allow_internet_access"] = serde_json::json!(true);
    }
    options
}

pub struct SessionSubmission {
    pub receipt: SessionSubmitReceipt,
    /// `Some` for a newly started turn. `None` when the receipt was replayed
    /// for a duplicate `client_request_id` — the original turn's event stream
    /// (if still unclaimed) is the one to attach to; no new stream exists.
    pub events: Option<mpsc::Receiver<SessionEvent>>,
}

/// One runtime implementation and the environment normalizer that owns its
/// provider/workspace admission rules. Keeping the pair together prevents a
/// session from being normalized with (for example) Pi rules and then
/// started by xiaoO.
#[derive(Clone)]
pub struct RuntimeRegistration {
    pub runtime: Arc<dyn AgentRuntime>,
    pub providers: Arc<HashMap<String, Arc<InstanceManager>>>,
    pub environment: Arc<dyn SessionEnvironmentNormalizer>,
}

impl RuntimeRegistration {
    pub fn new(
        runtime: Arc<dyn AgentRuntime>,
        providers: HashMap<String, Arc<InstanceManager>>,
        environment: Arc<dyn SessionEnvironmentNormalizer>,
    ) -> Self {
        Self {
            runtime,
            providers: Arc::new(providers),
            environment,
        }
    }

    pub fn with_providers(
        runtime: Arc<dyn AgentRuntime>,
        providers: HashMap<String, Arc<InstanceManager>>,
        environment: Arc<dyn SessionEnvironmentNormalizer>,
    ) -> Self {
        Self::new(runtime, providers, environment)
    }
}

/// How long the event-forwarding task waits for the subscriber to drain one
/// event before treating the stream as abandoned (client crashed without ever
/// attaching, or attached and stalled indefinitely).
const FORWARD_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);

/// Per-session retention cap for `client_request_id` → `turn_id` receipt
/// memory. Bounds memory per session; a client running more than this many
/// concurrent-ish retryable submissions against one session is outside the
/// idempotency window's design envelope.
const RECEIPT_WINDOW_CAP: usize = 64;

/// In-memory turn gate: single active turn per session + bounded
/// idempotency-receipt memory. Deliberately process-local (same posture as
/// the in-memory `SessionRepository` and `SessionLeaseTable`): a daemon
/// restart clears it, which is safe — the runtimes' in-flight turns are
/// orphaned by the restart anyway, and replaying a `client_request_id` after
/// a restart starts a fresh turn rather than corrupting anything.
#[derive(Default)]
struct TurnGate {
    /// runtime_id → the currently active turn_id.
    active: Mutex<HashMap<String, String>>,
    /// runtime_id → bounded FIFO of remembered (client_request_id, turn_id).
    receipts: Mutex<HashMap<String, ReceiptWindow>>,
}

#[derive(Default)]
struct ReceiptWindow {
    by_request: HashMap<String, String>,
    order: VecDeque<String>,
}

impl TurnGate {
    /// Claim the session's single-active-turn slot for `turn_id`. Fails with
    /// the currently active turn's id when occupied.
    fn claim(&self, runtime_id: &str, turn_id: &str) -> Result<(), String> {
        let mut active = self.active.lock().expect("turn gate lock poisoned");
        if let Some(existing) = active.get(runtime_id) {
            return Err(existing.clone());
        }
        active.insert(runtime_id.to_string(), turn_id.to_string());
        Ok(())
    }

    /// Release the slot iff it is still held by `turn_id` (guards against a
    /// stale forwarding task releasing a successor turn's claim).
    fn release(&self, runtime_id: &str, turn_id: &str) {
        let mut active = self.active.lock().expect("turn gate lock poisoned");
        if active.get(runtime_id).map(String::as_str) == Some(turn_id) {
            active.remove(runtime_id);
        }
    }

    fn replayed_receipt(&self, runtime_id: &str, client_request_id: &str) -> Option<String> {
        let receipts = self.receipts.lock().expect("turn gate lock poisoned");
        receipts
            .get(runtime_id)
            .and_then(|window| window.by_request.get(client_request_id).cloned())
    }

    fn remember_receipt(&self, runtime_id: &str, client_request_id: &str, turn_id: &str) {
        let mut receipts = self.receipts.lock().expect("turn gate lock poisoned");
        let window = receipts.entry(runtime_id.to_string()).or_default();
        if window.by_request.len() >= RECEIPT_WINDOW_CAP {
            if let Some(evicted) = window.order.pop_front() {
                window.by_request.remove(&evicted);
            }
        }
        window
            .by_request
            .insert(client_request_id.to_string(), turn_id.to_string());
        window.order.push_back(client_request_id.to_string());
    }

    /// Forget everything about a closed session.
    fn forget_session(&self, runtime_id: &str) {
        self.active
            .lock()
            .expect("turn gate lock poisoned")
            .remove(runtime_id);
        self.receipts
            .lock()
            .expect("turn gate lock poisoned")
            .remove(runtime_id);
    }
}

#[derive(Clone)]
pub struct SessionApplication {
    runtimes: Arc<BTreeMap<String, RuntimeRegistration>>,
    default_runtime_kind: Arc<str>,
    records: Arc<dyn SessionRepository>,
    turn_ids: Arc<dyn TurnIdGenerator>,
    runtime_ids: Arc<dyn RuntimeIdGenerator>,
    clock: Arc<dyn Clock>,
    /// `None` (the `new()` default) leaves lease enforcement off, matching
    /// today's behavior exactly. `Some` opts every lease-gated method
    /// (`close`/`heartbeat`/`submit_turn`) into single-writer enforcement via
    /// `SessionLeaseTable`; see `docs/session_orchestration_skeleton.md`
    /// (Component B / Open decision 3) for why this is a process-wide
    /// construction-time switch rather than a per-request flag.
    lease_table: Option<Arc<SessionLeaseTable>>,
    /// Single-active-turn enforcement + `client_request_id` idempotency.
    turn_gate: Arc<TurnGate>,
    /// tenant_id → count of sessions currently open for that tenant.
    tenant_sessions: Arc<Mutex<HashMap<String, usize>>>,
}

impl SessionApplication {
    pub fn new(
        runtime: Arc<dyn AgentRuntime>,
        providers: HashMap<String, Arc<InstanceManager>>,
        records: Arc<dyn SessionRepository>,
        turn_ids: Arc<dyn TurnIdGenerator>,
        runtime_ids: Arc<dyn RuntimeIdGenerator>,
        environment: Arc<dyn SessionEnvironmentNormalizer>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let default_runtime_kind = runtime.runtime_kind().to_string();
        Self::with_runtime_registry(
            default_runtime_kind,
            [RuntimeRegistration::new(runtime, providers, environment)],
            records,
            turn_ids,
            runtime_ids,
            clock,
        )
        .expect("single runtime registration is valid")
    }

    /// Construct an application that can host multiple runtime kinds in one
    /// process. `default_runtime_kind` is used only for new open requests
    /// that omit `runtime_kind`; persisted sessions are always routed by
    /// `OpaqueRuntimeState.runtime_kind`.
    pub fn with_runtime_registry(
        default_runtime_kind: impl Into<String>,
        registrations: impl IntoIterator<Item = RuntimeRegistration>,
        records: Arc<dyn SessionRepository>,
        turn_ids: Arc<dyn TurnIdGenerator>,
        runtime_ids: Arc<dyn RuntimeIdGenerator>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, SessionDomainError> {
        let default_runtime_kind = default_runtime_kind.into();
        let mut runtimes = BTreeMap::new();
        for registration in registrations {
            let kind = registration.runtime.runtime_kind().trim().to_string();
            if kind.is_empty() {
                return Err(SessionDomainError::InvalidRequest {
                    message: "runtime kind must not be empty".into(),
                });
            }
            if runtimes.insert(kind.clone(), registration).is_some() {
                return Err(SessionDomainError::InvalidRequest {
                    message: format!("duplicate runtime kind: {kind}"),
                });
            }
        }
        if !runtimes.contains_key(&default_runtime_kind) {
            return Err(SessionDomainError::InvalidRequest {
                message: format!("default runtime kind '{default_runtime_kind}' is not registered"),
            });
        }
        Ok(Self {
            runtimes: Arc::new(runtimes),
            default_runtime_kind: Arc::from(default_runtime_kind),
            records,
            turn_ids,
            runtime_ids,
            clock,
            lease_table: None,
            turn_gate: Arc::new(TurnGate::default()),
            tenant_sessions: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn registration(&self, kind: &str) -> Result<&RuntimeRegistration, SessionDomainError> {
        self.runtimes
            .get(kind)
            .ok_or_else(|| SessionDomainError::InvalidRequest {
                message: format!("unknown runtime_kind '{kind}'"),
            })
    }

    fn registration_for_record(
        &self,
        record: &SessionRecord,
    ) -> Result<&RuntimeRegistration, SessionDomainError> {
        self.registration(&record.runtime.runtime_kind)
    }

    /// Opt in to `SessionLeaseTable`-backed single-writer enforcement
    /// (Phase 3 of `docs/session_orchestration_skeleton.md`). Left unset by
    /// `new()` so existing callers are unaffected.
    pub fn with_lease_table(mut self, lease_table: Arc<SessionLeaseTable>) -> Self {
        self.lease_table = Some(lease_table);
        self
    }

    /// Rebuild the in-memory admission counter from durable active session
    /// rows. Assembly must call this before serving requests and fail closed
    /// if it fails; otherwise a daemon restart would temporarily reset every
    /// tenant's `max_sessions` usage to zero.
    pub async fn restore_tenant_session_counts(&self) -> Result<(), SessionDomainError> {
        let counts = self.records.active_session_counts_by_tenant().await?;
        let mut current = self
            .tenant_sessions
            .lock()
            .expect("tenant session counter lock poisoned");
        *current = counts;
        Ok(())
    }

    /// Track one more active session for `tenant_id`, rejecting it when an
    /// optional limit is already reached. Counts are maintained even when
    /// `limit` is `None`: tenant configuration can be hot-reloaded, and a
    /// later close must never decrement some other restored session merely
    /// because this session was opened while quota was unlimited.
    fn reserve_tenant_session(&self, tenant_id: &str, limit: Option<u32>) -> Result<(), u32> {
        let mut counts = self
            .tenant_sessions
            .lock()
            .expect("tenant session counter lock poisoned");
        let current = counts.get(tenant_id).copied().unwrap_or(0);
        if let Some(limit) = limit {
            if current >= limit as usize {
                return Err(limit);
            }
        }
        *counts.entry(tenant_id.to_string()).or_insert(0) += 1;
        Ok(())
    }

    /// Release one tracked active-session slot. A missing or already-zero
    /// entry is a no-op, which keeps rollback and legacy-record close paths
    /// safe.
    fn release_tenant_session(&self, tenant_id: &str) {
        let mut counts = self
            .tenant_sessions
            .lock()
            .expect("tenant session counter lock poisoned");
        if let Some(count) = counts.get_mut(tenant_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(tenant_id);
            }
        }
    }

    /// Thin audit-logging wrapper (`docs/tenancy_design.md` §6) around
    /// [`Self::open_impl`] — see [`audit_log`].
    pub async fn open(
        &self,
        ctx: &SecurityContext,
        request: SessionOpenRequest,
    ) -> Result<SessionOpenResponse, SessionDomainError> {
        let requested_runtime_id = request.runtime_id.clone();
        let result = self.open_impl(ctx, request).await;
        let runtime_id_for_log = result
            .as_ref()
            .ok()
            .map(|response| response.runtime_id.as_str())
            .or(requested_runtime_id.as_deref());
        audit_log(ctx, "open", runtime_id_for_log, &result);
        result
    }

    async fn open_impl(
        &self,
        ctx: &SecurityContext,
        request: SessionOpenRequest,
    ) -> Result<SessionOpenResponse, SessionDomainError> {
        let runtime_id = request
            .runtime_id
            .clone()
            .unwrap_or_else(|| self.runtime_ids.next_runtime_id());
        if let Some(mut record) = self.records.get(&runtime_id).await? {
            // Re-attach branch: an existing `runtime_id` belonging to another
            // tenant must 404, not silently attach the caller to it
            // (`docs/tenancy_design.md` §3.2 — same 404-not-403 rule as
            // `require_session`, since this is the same ownership check on a
            // different code path).
            if !ctx.owns(record.tenant_id.as_deref()) {
                return Err(SessionDomainError::NotFound { runtime_id });
            }
            if let Some(requested_kind) = request.runtime_kind.as_deref() {
                if requested_kind != record.runtime.runtime_kind {
                    return Err(SessionDomainError::Conflict {
                        message: format!(
                            "session runtime_kind is '{}', not requested '{}'",
                            record.runtime.runtime_kind, requested_kind
                        ),
                    });
                }
            }
            self.ensure_runtime_attached(ctx, &record).await?;
            // `ensure_runtime_attached` already persisted a `running` ->
            // `idle` restoration fallback (§1.3); mirror it onto this local
            // copy so the response the caller sees matches what was saved
            // rather than the pre-restoration snapshot fetched above.
            if record.status == SessionStatus::Running {
                record.status = SessionStatus::Idle;
            }
            return Ok(project_session(&record));
        }

        let runtime_kind = request
            .runtime_kind
            .as_deref()
            .unwrap_or(&self.default_runtime_kind);
        let registration = self.registration(runtime_kind)?;
        let runtime = Arc::clone(&registration.runtime);
        let normalized = registration.environment.normalize(ctx, &request).await?;

        // Fail-closed second gate (`docs/tenancy_design.md` §0/§7 step 2):
        // deliberately independent of whatever `self.environment.normalize`
        // just did. Even if some future normalizer forgets — or gets wrong —
        // the `(role, workspace, provider)` admission check
        // (`enforce_workspace_axiom`), a non-admin session that normalized to
        // a host boundary or a local-path workspace is refused here, before
        // `runtime.start` below ever spins up a real process.
        if !ctx.is_admin() {
            if normalized.isolation.boundary == IsolationBoundary::Host {
                return Err(SessionDomainError::InvalidRequest {
                    message: "fail-closed: non-admin session normalized to a host isolation \
                              boundary — refusing to start (docs/tenancy_design.md §0)"
                        .to_string(),
                });
            }
            if matches!(
                request.workspace,
                session_protocol::WorkspaceSpec::LocalPath { .. }
            ) {
                return Err(SessionDomainError::InvalidRequest {
                    message: "fail-closed: non-admin session requested a local-path workspace \
                              — refusing to start (docs/tenancy_design.md §0)"
                        .to_string(),
                });
            }
        }

        let mut runtime_wire_capabilities: std::collections::BTreeSet<_> = runtime
            .capabilities_for_context(&agent_runtime_protocol::RuntimeCapabilityContext {
                ext: request.ext.clone(),
            })
            .into_iter()
            .filter_map(runtime_capability)
            .collect();
        if normalized
            .sandbox_capabilities
            .contains(&crate::SandboxCapability::Snapshot)
        {
            runtime_wire_capabilities
                .insert(session_protocol::SessionRuntimeCapability::Checkpoint);
        }
        for requested in &request.requested_capabilities.sandbox {
            if !normalized
                .sandbox_capabilities
                .contains(&domain_sandbox_capability(requested))
            {
                return Err(SessionDomainError::UnsupportedCapability {
                    family: crate::CapabilityFamily::Sandbox,
                    capability: capability_name(requested),
                });
            }
        }
        for requested in &request.requested_capabilities.runtime {
            if !runtime_wire_capabilities.contains(requested) {
                return Err(SessionDomainError::UnsupportedCapability {
                    family: crate::CapabilityFamily::Runtime,
                    capability: capability_name(requested),
                });
            }
        }

        let tenant_id_for_quota = ctx.tenant_id().map(str::to_string);
        if let Some(tenant_id) = &tenant_id_for_quota {
            self.reserve_tenant_session(tenant_id, ctx.quota.max_sessions)
                .map_err(|limit| SessionDomainError::QuotaExceeded {
                    scope: "sessions".to_string(),
                    limit,
                })?;
        }

        // LLM connectivity probe: abort the session open *before* the sandbox
        // is provisioned so a misconfigured endpoint (wrong URL, bad key,
        // unreachable host, unknown model) surfaces as an immediate error
        // rather than a silent failure mid-turn.
        let start_request = RuntimeStartRequest {
            runtime_id: runtime_id.clone(),
            conversation_id: request.conversation_id.clone(),
            sender_id: request.sender_id.clone(),
            workspace: normalized.workspace.clone(),
            state: None,
            llm: request.llm.clone(),
            ext: request.ext.clone(),
        };
        if let Err(error) = runtime.probe_llm(&start_request).await.map_err(map_runtime_error) {
            if let Some(tenant_id) = &tenant_id_for_quota {
                self.release_tenant_session(tenant_id);
            }
            return Err(error);
        }

        let now = self.clock.now_ms();
        let (provider_id, manager) = provider_for_request(registration, &request)?;
        let backend = match manager
            .start_instance(
                runtime_id.clone(),
                provider_protocol::BackendId(provider_id.clone()),
                ctx.owner_ref(),
                provider_options(&normalized.workspace, &normalized.isolation, &provider_id),
            )
            .await
            .map_err(map_provider_error)
        {
            Ok(backend) => backend,
            Err(error) => {
                if let Some(tenant_id) = &tenant_id_for_quota {
                    self.release_tenant_session(tenant_id);
                }
                return Err(error);
            }
        };
        if let Err(error) = runtime
            .start(
                start_request,
                RuntimeExecutionContext {
                    operation_backend: backend,
                },
            )
            .await
            .map_err(map_runtime_error)
        {
            let _ = manager.stop_instance(&runtime_id).await;
            if let Some(tenant_id) = &tenant_id_for_quota {
                self.release_tenant_session(tenant_id);
            }
            return Err(error);
        }

        // Reuse the pre-existing runtime-state quarantine slot
        let runtime_state = match runtime
            .export_state(&runtime_id)
            .await
            .map_err(map_runtime_error)
        {
            Ok(state) => state,
            Err(SessionDomainError::UnsupportedCapability { .. }) => OpaqueRuntimeState {
                runtime_kind: runtime.runtime_kind().into(),
                schema_version: 1,
                state: serde_json::Value::Null,
            },
            Err(error) => {
                let _ = runtime.stop(&runtime_id).await;
                let _ = manager.stop_instance(&runtime_id).await;
                if let Some(tenant_id) = &tenant_id_for_quota {
                    self.release_tenant_session(tenant_id);
                }
                return Err(error);
            }
        };

        let record = SessionRecord {
            runtime_id,
            conversation_id: request.conversation_id,
            sender_id: request.sender_id,
            status: SessionStatus::Idle,
            created_at_ms: now,
            updated_at_ms: now,
            workspace: normalized.workspace,
            isolation: normalized.isolation,
            capabilities: EffectiveCapabilities {
                sandbox: normalized.sandbox_capabilities,
                runtime: runtime_wire_capabilities
                    .into_iter()
                    .filter_map(domain_runtime_capability)
                    .collect(),
            },
            runtime: runtime_state,
            llm: normalized.llm,
            lease: normalized.lease,
            lineage: None,
            last_error: None,
            tenant_id: ctx.tenant_id().map(str::to_string),
            created_by: ctx.principal.clone(),
        };
        if let Err(error) = self.records.save(record.clone()).await {
            let _ = runtime.stop(&record.runtime_id).await;
            let _ = manager.stop_instance(&record.runtime_id).await;
            if let Some(tenant_id) = &tenant_id_for_quota {
                self.release_tenant_session(tenant_id);
            }
            return Err(error);
        }
        Ok(project_session(&record))
    }

    /// Thin audit-logging wrapper (`docs/tenancy_design.md` §6) around
    /// [`Self::submit_turn_impl`] — see [`audit_log`].
    pub async fn submit_turn(
        &self,
        ctx: &SecurityContext,
        request: SessionTurnRequest,
    ) -> Result<SessionSubmission, SessionDomainError> {
        let runtime_id = request.runtime_id.clone();
        let result = self.submit_turn_impl(ctx, request).await;
        audit_log(ctx, "submit_turn", Some(&runtime_id), &result);
        result
    }

    async fn submit_turn_impl(
        &self,
        ctx: &SecurityContext,
        request: SessionTurnRequest,
    ) -> Result<SessionSubmission, SessionDomainError> {
        let record = self.require_session(ctx, &request.runtime_id).await?;
        self.ensure_runtime_attached(ctx, &record).await?;
        let registration = self.registration_for_record(&record)?;
        let runtime = Arc::clone(&registration.runtime);
        self.check_lease_holder(&request.runtime_id, &request.lease)
            .await?;
        if request.llm.is_some()
            && !record
                .capabilities
                .runtime
                .contains(&RuntimeCapability::ModelOverride)
        {
            return Err(unsupported_runtime_capability("model_override"));
        }
        if request.reasoning_effort.is_some()
            && !record
                .capabilities
                .runtime
                .contains(&RuntimeCapability::ReasoningControl)
        {
            return Err(unsupported_runtime_capability("reasoning_control"));
        }
        let runtime_id = request.runtime_id.clone();

        // Idempotent replay: a retried submission (same `client_request_id`)
        // returns the original receipt instead of starting a second turn.
        // Checked before the active-turn claim so a retry of the *currently
        // running* turn replays its receipt rather than bouncing off its own
        // claim with `Conflict`.
        if let Some(request_id) = request.client_request_id.as_deref() {
            if let Some(turn_id) = self.turn_gate.replayed_receipt(&runtime_id, request_id) {
                return Ok(SessionSubmission {
                    receipt: SessionSubmitReceipt {
                        runtime_id,
                        turn_id,
                        accepted_kind: session_protocol::SessionAcceptedInputKind::Turn,
                    },
                    events: None,
                });
            }
        }

        let turn_id = self.turn_ids.next_turn_id();

        // Single active turn per session: claim before touching the runtime
        // so two concurrent submissions can never both reach the adapter.
        if let Err(active_turn_id) = self.turn_gate.claim(&runtime_id, &turn_id) {
            return Err(SessionDomainError::Conflict {
                message: format!("another turn is active on this session: {active_turn_id}"),
            });
        }

        let submitted = runtime
            .submit_turn(RuntimeTurnInput {
                runtime_id: runtime_id.clone(),
                turn_id: turn_id.clone(),
                text: request.text,
                entry: RuntimeEntryContext {
                    kind: request.entry.entry_kind,
                    instance_id: request.entry.instance_id,
                    message_id: request.entry.message_id,
                    reply_to_message_id: request.entry.reply_to_message_id,
                },
                llm: request.llm,
                reasoning_effort: request.reasoning_effort,
                ext: request.ext,
            })
            .await
            .map_err(map_runtime_error);
        let mut runtime_events = match submitted {
            Ok(events) => events,
            Err(error) => {
                self.turn_gate.release(&runtime_id, &turn_id);
                return Err(error);
            }
        };

        // Only a submission the runtime actually accepted is remembered for
        // replay — a rejected one should be retried for real.
        if let Some(request_id) = request.client_request_id.as_deref() {
            self.turn_gate
                .remember_receipt(&runtime_id, request_id, &turn_id);
        }

        let (tx, rx) = mpsc::channel(64);
        let event_runtime_id = runtime_id.clone();
        let event_turn_id = turn_id.clone();
        let gate = Arc::clone(&self.turn_gate);
        let cancel_runtime = Arc::clone(&runtime);
        let state_runtime = Arc::clone(&runtime);
        let records = Arc::clone(&self.records);
        let clock = Arc::clone(&self.clock);
        tokio::spawn(async move {
            let mut terminal_seen = false;
            let mut subscriber_abandoned = false;
            while let Some(mut event) = runtime_events.recv().await {
                let terminal = matches!(
                    event,
                    RuntimeEvent::Completed { .. } | RuntimeEvent::Failed { .. }
                );
                // A terminal event is the durability boundary: persist the
                // runtime's latest opaque state before allowing a client to
                // observe completion. This makes a successful terminal event
                // a reliable restart-recovery point for every adapter that
                // implements state export.
                if terminal {
                    let persisted = async {
                        let state = match state_runtime
                            .export_state(&event_runtime_id)
                            .await
                            .map_err(map_runtime_error)
                        {
                            Ok(state) => Some(state),
                            Err(SessionDomainError::UnsupportedCapability { .. }) => None,
                            Err(error) => return Err(error),
                        };
                        let mut record =
                            records.get(&event_runtime_id).await?.ok_or_else(|| {
                                SessionDomainError::NotFound {
                                    runtime_id: event_runtime_id.clone(),
                                }
                            })?;
                        if let Some(state) = state {
                            record.runtime = state;
                        }
                        record.status = match &event {
                            RuntimeEvent::Completed { .. } => SessionStatus::Idle,
                            RuntimeEvent::Failed { .. } => SessionStatus::Failed,
                            _ => unreachable!("guarded by terminal match"),
                        };
                        record.updated_at_ms = clock.now_ms();
                        records.save(record).await
                    }
                    .await;
                    if let Err(error) = persisted {
                        event = RuntimeEvent::Failed {
                            error: RuntimeFailure {
                                code: "runtime_state_persist_failed".into(),
                                message: error.to_string(),
                                retryable: true,
                                details: serde_json::Value::Null,
                            },
                            usage: session_protocol::SessionUsage::default(),
                        };
                    }
                }
                let sent = tokio::time::timeout(
                    FORWARD_SEND_TIMEOUT,
                    tx.send(project_runtime_event(
                        &event_runtime_id,
                        &event_turn_id,
                        event,
                    )),
                )
                .await;
                let delivered = matches!(sent, Ok(Ok(())));
                if !delivered {
                    subscriber_abandoned = true;
                }
                if !delivered || terminal {
                    terminal_seen = terminal;
                    break;
                }
            }
            if subscriber_abandoned && !terminal_seen {
                // The subscriber is gone but the runtime is still thinking:
                // stop the orphaned work best-effort so it doesn't run (and
                // bill) to completion for nobody.
                let _ = cancel_runtime
                    .cancel(RuntimeCancelRequest {
                        runtime_id: event_runtime_id.clone(),
                        turn_id: Some(event_turn_id.clone()),
                    })
                    .await;
                // Drain the runtime's stream to its end so the adapter is
                // never blocked on a channel nobody reads, then fall through
                // to release the gate.
                while runtime_events.recv().await.is_some() {}
            } else if !terminal_seen {
                let _ = tokio::time::timeout(
                    FORWARD_SEND_TIMEOUT,
                    tx.send(SessionEvent::TurnFailed {
                        runtime_id: event_runtime_id.clone(),
                        turn_id: event_turn_id.clone(),
                        error: session_protocol::SessionTurnFailure {
                            code: "event_stream_closed".into(),
                            message: "runtime event stream closed before a terminal event".into(),
                            retryable: true,
                            details: serde_json::Value::Null,
                        },
                        usage: session_protocol::SessionUsage::default(),
                    }),
                )
                .await;
            }
            // Every exit path funnels here: the session accepts its next turn
            // only once this turn's forwarding is truly finished.
            gate.release(&event_runtime_id, &event_turn_id);
        });
        Ok(SessionSubmission {
            receipt: SessionSubmitReceipt {
                runtime_id,
                turn_id,
                accepted_kind: session_protocol::SessionAcceptedInputKind::Turn,
            },
            events: Some(rx),
        })
    }

    /// Thin audit-logging wrapper (`docs/tenancy_design.md` §6) around
    /// [`Self::answer_interaction_impl`] — see [`audit_log`].
    pub async fn answer_interaction(
        &self,
        ctx: &SecurityContext,
        request: SessionInteractionRequest,
    ) -> Result<SessionSubmitReceipt, SessionDomainError> {
        let runtime_id = request.runtime_id.clone();
        let result = self.answer_interaction_impl(ctx, request).await;
        audit_log(ctx, "answer_interaction", Some(&runtime_id), &result);
        result
    }

    async fn answer_interaction_impl(
        &self,
        ctx: &SecurityContext,
        request: SessionInteractionRequest,
    ) -> Result<SessionSubmitReceipt, SessionDomainError> {
        let record = self.require_session(ctx, &request.runtime_id).await?;
        self.ensure_runtime_attached(ctx, &record).await?;
        if !record
            .capabilities
            .runtime
            .contains(&RuntimeCapability::Interaction)
        {
            return Err(unsupported_runtime_capability("interaction"));
        }
        let registration = self.registration_for_record(&record)?;
        let runtime = Arc::clone(&registration.runtime);
        runtime
            .answer_interaction(RuntimeInteractionInput {
                runtime_id: request.runtime_id.clone(),
                turn_id: request.turn_id.clone(),
                interaction_id: request.interaction_id,
                answer: request.answer,
                ext: request.ext,
            })
            .await
            .map_err(map_runtime_error)?;
        Ok(SessionSubmitReceipt {
            runtime_id: request.runtime_id,
            turn_id: request.turn_id,
            accepted_kind: session_protocol::SessionAcceptedInputKind::Interaction,
        })
    }

    /// Stop the runtime, tombstone the record (`status: Closed`, kept in the
    /// repository rather than deleted — see
    /// `docs/session_orchestration_skeleton.md`, Open decision 2), and drop
    /// any lease. Lease-gated the same way `submit_turn`/`heartbeat` are:
    /// a no-op when `lease_table` is `None`. Thin audit-logging wrapper
    /// (`docs/tenancy_design.md` §6) around [`Self::close_impl`] — see
    /// [`audit_log`].
    pub async fn close(
        &self,
        ctx: &SecurityContext,
        runtime_id: &str,
        lease: SessionLeaseClaim,
    ) -> Result<SessionControlResponse, SessionDomainError> {
        let result = self.close_impl(ctx, runtime_id, lease).await;
        audit_log(ctx, "close", Some(runtime_id), &result);
        result
    }

    async fn close_impl(
        &self,
        ctx: &SecurityContext,
        runtime_id: &str,
        lease: SessionLeaseClaim,
    ) -> Result<SessionControlResponse, SessionDomainError> {
        let record = self.require_session(ctx, runtime_id).await?;
        self.check_lease_holder(runtime_id, &lease).await?;
        self.finalize_closed_session(record, runtime_id, None).await
    }

    /// System-forced counterpart to `close`: called only by the reclaim
    /// sweep (`reclaim_sweeper.rs`) once `AgentRuntime::check_alive` has
    /// already confirmed the provider-side sandbox is gone. Deliberately
    /// skips `check_lease_holder` — unlike `close`, there is no live runtime
    /// left for a lease to protect, so a fresh/active lease must not block
    /// this from recording what the provider already did. Thin
    /// audit-logging wrapper (`docs/tenancy_design.md` §6) around
    /// [`Self::reclaim_impl`] — see [`audit_log`].
    pub async fn reclaim(
        &self,
        ctx: &SecurityContext,
        runtime_id: &str,
    ) -> Result<SessionControlResponse, SessionDomainError> {
        let result = self.reclaim_impl(ctx, runtime_id).await;
        audit_log(ctx, "reclaim", Some(runtime_id), &result);
        result
    }

    async fn reclaim_impl(
        &self,
        ctx: &SecurityContext,
        runtime_id: &str,
    ) -> Result<SessionControlResponse, SessionDomainError> {
        let record = self.require_session(ctx, runtime_id).await?;
        // TOCTOU guard: the reclaim sweep's candidate list is a snapshot, and
        // this record may have already reached a terminal status by the time
        // `check_alive` and this call run (e.g. the owning client noticed the
        // same dead sandbox and called `close` first). Must not overwrite a
        // legitimate `Closed`/`Failed` with a second close — treat "already
        // terminal" as a no-op success, since the desired end state (not left
        // dangling as active) already holds.
        if matches!(record.status, SessionStatus::Closed | SessionStatus::Failed) {
            return Ok(SessionControlResponse {
                runtime_id: runtime_id.to_string(),
                status: crate::projection::project_status(record.status),
                updated_at_ms: record.updated_at_ms,
            });
        }
        self.finalize_closed_session(
            record,
            runtime_id,
            Some("provider sandbox no longer exists (idle-timeout expiry)".to_string()),
        )
        .await
    }

    /// Shared tail for `close`/`reclaim`: stop the runtime (tolerating the
    /// no-in-memory-instance case the same way for both — see the §1.4
    /// comment below), tombstone the record as `Closed`, and drop
    /// lease/turn-gate/tenant-slot bookkeeping. `note`, when present,
    /// overwrites `last_error` — used by `reclaim` to record why the session
    /// was force-closed; `close` passes `None` and leaves any prior error
    /// untouched.
    async fn finalize_closed_session(
        &self,
        mut record: SessionRecord,
        runtime_id: &str,
        note: Option<String>,
    ) -> Result<SessionControlResponse, SessionDomainError> {
        let registration = self.registration_for_record(&record)?;
        let runtime = Arc::clone(&registration.runtime);
        match runtime.stop(runtime_id).await.map_err(map_runtime_error) {
            Ok(()) | Err(SessionDomainError::NotFound { .. }) => {}
            Err(error) => return Err(error),
        }
        if let Ok((_, manager)) = provider_for_isolation(registration, &record.isolation) {
            manager
                .destroy_by_runtime_id(runtime_id)
                .await
                .map_err(map_provider_error)?;
        }

        let now = self.clock.now_ms();
        record.status = SessionStatus::Closed;
        record.updated_at_ms = now;
        if note.is_some() {
            record.last_error = note;
        }
        let tenant_id = record.tenant_id.clone();
        self.records.save(record).await?;

        if let Some(lease_table) = &self.lease_table {
            lease_table.remove(runtime_id).await;
        }
        // Drop the closed session's active-turn slot and idempotency window.
        self.turn_gate.forget_session(runtime_id);
        // `docs/tenancy_design.md` §7 step 4: release this tenant's tracked
        // active-session slot. Tracking is unconditional for tenant
        // sessions, even when the admission limit itself is unset.
        if let Some(tenant_id) = &tenant_id {
            self.release_tenant_session(tenant_id);
        }

        Ok(SessionControlResponse {
            runtime_id: runtime_id.to_string(),
            status: SessionLifecycleStatus::Closed,
            updated_at_ms: now,
        })
    }

    /// Best-effort platform-liveness probe for the reclaim sweep
    /// (`reclaim_sweeper.rs`) — resolves `runtime_id`'s current record and
    /// runtime, then delegates to `AgentRuntime::check_alive`. Not
    /// audit-logged (unlike `close`/`reclaim`): this is a read-only probe, not
    /// a state-changing control operation.
    pub async fn check_alive(
        &self,
        ctx: &SecurityContext,
        runtime_id: &str,
    ) -> Result<bool, SessionDomainError> {
        let record = self.require_session(ctx, runtime_id).await?;
        let registration = self.registration_for_record(&record)?;
        let runtime = Arc::clone(&registration.runtime);
        let runtime_alive = runtime
            .check_alive(runtime_id)
            .await
            .map_err(map_runtime_error)?;
        let (_, manager) = provider_for_isolation(registration, &record.isolation)?;
        let provider_alive = match manager.inspect_instance(runtime_id).await {
            Ok(_) => true,
            Err(provider_protocol::ProviderControlError::NotFound { .. }) => false,
            Err(error) => return Err(map_provider_error(error)),
        };
        Ok(runtime_alive && provider_alive)
    }

    /// Release `lease.client_id`'s write lease without stopping the runtime,
    /// so it stays warm for the next `open`. A no-op (beyond the existence
    /// check) when lease enforcement is off or the claim carries no
    /// `client_id` — there is nothing in `SessionLeaseTable` to release.
    /// Thin audit-logging wrapper (`docs/tenancy_design.md` §6) around
    /// [`Self::detach_impl`] — see [`audit_log`].
    pub async fn detach(
        &self,
        ctx: &SecurityContext,
        runtime_id: &str,
        lease: SessionLeaseClaim,
    ) -> Result<SessionControlResponse, SessionDomainError> {
        let result = self.detach_impl(ctx, runtime_id, lease).await;
        audit_log(ctx, "detach", Some(runtime_id), &result);
        result
    }

    async fn detach_impl(
        &self,
        ctx: &SecurityContext,
        runtime_id: &str,
        lease: SessionLeaseClaim,
    ) -> Result<SessionControlResponse, SessionDomainError> {
        let record = self.require_session(ctx, runtime_id).await?;
        if let Some(lease_table) = &self.lease_table {
            if let Some(client_id) = lease.client_id.as_deref() {
                lease_table.detach(runtime_id, client_id).await;
            }
        }
        Ok(SessionControlResponse {
            runtime_id: runtime_id.to_string(),
            status: crate::projection::project_status(record.status),
            updated_at_ms: record.updated_at_ms,
        })
    }

    /// Refresh `lease.client_id`'s write lease. When lease enforcement is
    /// off (`lease_table: None`) this is a trivially-accepted no-op, matching
    /// `close`/`submit_turn`'s off-by-default behavior. When enforcement is
    /// on, a claim with no `client_id` is rejected with `LeaseRequired`
    /// (there is no holder identity to record a heartbeat for). Thin
    /// audit-logging wrapper (`docs/tenancy_design.md` §6) around
    /// [`Self::heartbeat_impl`] — see [`audit_log`].
    pub async fn heartbeat(
        &self,
        ctx: &SecurityContext,
        runtime_id: &str,
        lease: SessionLeaseClaim,
    ) -> Result<SessionHeartbeatResponse, SessionDomainError> {
        let result = self.heartbeat_impl(ctx, runtime_id, lease).await;
        audit_log(ctx, "heartbeat", Some(runtime_id), &result);
        result
    }

    async fn heartbeat_impl(
        &self,
        ctx: &SecurityContext,
        runtime_id: &str,
        lease: SessionLeaseClaim,
    ) -> Result<SessionHeartbeatResponse, SessionDomainError> {
        self.require_session(ctx, runtime_id).await?;

        let Some(lease_table) = &self.lease_table else {
            return Ok(SessionHeartbeatResponse {
                runtime_id: runtime_id.to_string(),
                accepted: true,
                lease_expires_at_ms: None,
            });
        };

        let client_id =
            lease
                .client_id
                .clone()
                .ok_or_else(|| SessionDomainError::LeaseRequired {
                    runtime_id: runtime_id.to_string(),
                })?;
        lease_table
            .heartbeat(
                runtime_id,
                &client_id,
                lease.client_pid,
                lease.client_hostname,
            )
            .await
            .map_err(|failure| lease_check_failure_to_domain_error(runtime_id, failure))?;

        Ok(SessionHeartbeatResponse {
            runtime_id: runtime_id.to_string(),
            accepted: true,
            lease_expires_at_ms: Some(self.clock.now_ms() + lease_table.stale_threshold_ms()),
        })
    }

    /// Thin delegation to `runtime.cancel()`. Deliberately not lease-gated:
    /// cancelling your own in-flight turn should not require holding the
    /// write lease (see `docs/session_orchestration_skeleton.md`, Component
    /// B). Thin audit-logging wrapper (`docs/tenancy_design.md` §6) around
    /// [`Self::cancel_impl`] — see [`audit_log`].
    pub async fn cancel(
        &self,
        ctx: &SecurityContext,
        runtime_id: &str,
        turn_id: Option<&str>,
    ) -> Result<(), SessionDomainError> {
        let result = self.cancel_impl(ctx, runtime_id, turn_id).await;
        audit_log(ctx, "cancel", Some(runtime_id), &result);
        result
    }

    async fn cancel_impl(
        &self,
        ctx: &SecurityContext,
        runtime_id: &str,
        turn_id: Option<&str>,
    ) -> Result<(), SessionDomainError> {
        let record = self.require_session(ctx, runtime_id).await?;
        self.ensure_runtime_attached(ctx, &record).await?;
        let registration = self.registration_for_record(&record)?;
        let runtime = Arc::clone(&registration.runtime);
        runtime
            .cancel(RuntimeCancelRequest {
                runtime_id: runtime_id.to_string(),
                turn_id: turn_id.map(str::to_string),
            })
            .await
            .map_err(map_runtime_error)
    }

    /// Export `request.parent_runtime_id`'s state and start a new runtime
    /// from it. Not lease-gated (forking off a parent does not mutate it).
    /// `runtime.export_state` defaults to `UnsupportedCapability` on every
    /// `AgentRuntime` that hasn't opted in —
    /// so this correctly fails closed rather than faking a feature no
    /// adapter implements yet. Thin audit-logging wrapper
    /// (`docs/tenancy_design.md` §6) around [`Self::fork_impl`] — see
    /// [`audit_log`].
    pub async fn fork(
        &self,
        ctx: &SecurityContext,
        request: SessionForkRequest,
    ) -> Result<SessionOpenResponse, SessionDomainError> {
        let parent_runtime_id = request.parent_runtime_id.clone();
        let requested_runtime_id = request.runtime_id.clone();
        let result = self.fork_impl(ctx, request).await;
        let runtime_id_for_log = result
            .as_ref()
            .ok()
            .map(|response| response.runtime_id.as_str())
            .or(requested_runtime_id.as_deref())
            .or(Some(parent_runtime_id.as_str()));
        audit_log(ctx, "fork", runtime_id_for_log, &result);
        result
    }

    pub async fn checkpoint(
        &self,
        ctx: &SecurityContext,
        request: SessionCheckpointRequest,
    ) -> Result<SessionCheckpointResult, SessionDomainError> {
        let record = self.require_session(ctx, &request.runtime_id).await?;
        let registration = self.registration_for_record(&record)?;
        let runtime = Arc::clone(&registration.runtime);
        if !record
            .capabilities
            .runtime
            .contains(&RuntimeCapability::Checkpoint)
            || !record
                .capabilities
                .sandbox
                .contains(&crate::SandboxCapability::Snapshot)
        {
            return Err(SessionDomainError::UnsupportedCapability {
                family: crate::CapabilityFamily::Runtime,
                capability: "checkpoint".into(),
            });
        }
        let runtime_state = runtime
            .export_state(&request.runtime_id)
            .await
            .map_err(map_runtime_error)?;
        let (provider_id, manager) = provider_for_isolation(registration, &record.isolation)?;
        let provider_snapshot = manager
            .checkpoint_instance(&request.runtime_id)
            .await
            .map_err(map_provider_error)?;
        let checkpoint_id = format!("checkpoint-{}", self.runtime_ids.next_runtime_id());
        let created_at_ms = self.clock.now_ms();
        if let Err(error) = self
            .records
            .save_checkpoint(crate::CheckpointRecord {
                checkpoint_id: checkpoint_id.clone(),
                source_runtime_id: request.runtime_id.clone(),
                provider_snapshot_id: provider_snapshot.snapshot_id.0.clone(),
                runtime_state,
                workspace: record.workspace,
                isolation: record.isolation,
                capabilities: record.capabilities,
                owner_ref: ctx.owner_ref(),
                tenant_id: record.tenant_id,
                created_by: Some(ctx.principal.clone()),
                created_at_ms,
            })
            .await
        {
            let _ = manager
                .delete_snapshot(
                    provider_protocol::BackendId(provider_id),
                    provider_snapshot.snapshot_id,
                )
                .await;
            return Err(error);
        }
        Ok(SessionCheckpointResult {
            checkpoint_id,
            runtime_id: request.runtime_id,
            checkpoint_scope: session_protocol::SessionCheckpointScope::Full,
            created_at_ms,
        })
    }

    pub async fn load_checkpoint(
        &self,
        ctx: &SecurityContext,
        request: SessionLoadRequest,
    ) -> Result<SessionOpenResponse, SessionDomainError> {
        let resolved_llm = request.llm.as_ref().and_then(|llm| {
            let provider = llm.provider.as_deref()?.trim();
            let model = llm.model.as_deref()?.trim();
            if provider.is_empty() || model.is_empty() {
                return None;
            }
            Some(crate::ResolvedLlm {
                provider: provider.to_string(),
                model: model.to_string(),
                api_base: llm.api_base.clone(),
                credential_source: llm
                    .api_key
                    .as_deref()
                    .map(str::trim)
                    .filter(|key| !key.is_empty())
                    .map(|_| "request".to_string())
                    .or_else(|| {
                        llm.api_key_env
                            .as_deref()
                            .map(str::trim)
                            .filter(|name| !name.is_empty())
                            .map(|name| format!("env:{name}"))
                    })
                    .unwrap_or_else(|| "runtime_default".to_string()),
            })
        });
        let runtime_id = request
            .runtime_id
            .unwrap_or_else(|| self.runtime_ids.next_runtime_id());
        let _deployment = request.deployment.clone();
        let _requested_capabilities = request.requested_capabilities.clone();
        let checkpoint = self
            .records
            .get_checkpoint(&request.checkpoint_id)
            .await?
            .filter(|record| ctx.owns(record.tenant_id.as_deref()))
            .ok_or_else(|| SessionDomainError::NotFound {
                runtime_id: request.checkpoint_id.clone(),
            })?;
        let registration = self.registration(&checkpoint.runtime_state.runtime_kind)?;
        let runtime = Arc::clone(&registration.runtime);
        let tenant_id_for_quota = ctx.tenant_id().map(str::to_string);
        if let Some(tenant_id) = &tenant_id_for_quota {
            self.reserve_tenant_session(tenant_id, ctx.quota.max_sessions)
                .map_err(|limit| SessionDomainError::QuotaExceeded {
                    scope: "sessions".into(),
                    limit,
                })?;
        }
        let (provider_id, manager) = provider_for_isolation(registration, &checkpoint.isolation)?;
        let backend = match manager
            .load_instance_from_snapshot(
                runtime_id.clone(),
                provider_protocol::BackendId(provider_id.clone()),
                ctx.owner_ref(),
                provider_protocol::ProviderSnapshotId(checkpoint.provider_snapshot_id.clone()),
                provider_options(&checkpoint.workspace, &checkpoint.isolation, &provider_id),
            )
            .await
            .map_err(map_provider_error)
        {
            Ok(backend) => backend,
            Err(error) => {
                if let Some(id) = &tenant_id_for_quota {
                    self.release_tenant_session(id);
                }
                return Err(error);
            }
        };
        if let Err(error) = runtime
            .start(
                RuntimeStartRequest {
                    runtime_id: runtime_id.clone(),
                    conversation_id: request
                        .conversation_id
                        .clone()
                        .unwrap_or_else(|| checkpoint.source_runtime_id.clone()),
                    sender_id: request
                        .sender_id
                        .clone()
                        .unwrap_or_else(|| ctx.principal.clone()),
                    workspace: checkpoint.workspace.clone(),
                    state: Some(checkpoint.runtime_state.clone()),
                    llm: request.llm.clone(),
                    ext: Default::default(),
                },
                RuntimeExecutionContext {
                    operation_backend: backend,
                },
            )
            .await
            .map_err(map_runtime_error)
        {
            let _ = manager.stop_instance(&runtime_id).await;
            if let Some(id) = &tenant_id_for_quota {
                self.release_tenant_session(id);
            }
            return Err(error);
        }
        let now = self.clock.now_ms();
        let record = SessionRecord {
            runtime_id: runtime_id.clone(),
            conversation_id: request
                .conversation_id
                .unwrap_or_else(|| checkpoint.source_runtime_id.clone()),
            sender_id: request.sender_id.unwrap_or_else(|| ctx.principal.clone()),
            status: SessionStatus::Idle,
            created_at_ms: now,
            updated_at_ms: now,
            workspace: checkpoint.workspace,
            isolation: checkpoint.isolation,
            capabilities: checkpoint.capabilities,
            runtime: checkpoint.runtime_state,
            llm: resolved_llm,
            lease: None,
            lineage: Some(CheckpointLineage {
                parent_runtime_id: None,
                source_checkpoint_id: Some(request.checkpoint_id),
            }),
            last_error: None,
            tenant_id: ctx.tenant_id().map(str::to_string),
            created_by: ctx.principal.clone(),
        };
        if let Err(error) = self.records.save(record.clone()).await {
            let _ = runtime.stop(&runtime_id).await;
            let _ = manager.stop_instance(&runtime_id).await;
            if let Some(id) = &tenant_id_for_quota {
                self.release_tenant_session(id);
            }
            return Err(error);
        }
        Ok(project_session(&record))
    }

    pub async fn delete_checkpoint(
        &self,
        ctx: &SecurityContext,
        request: SessionCheckpointDeleteRequest,
    ) -> Result<SessionCheckpointDeleteResult, SessionDomainError> {
        let checkpoint_id = request.checkpoint_id.clone();
        let result = self.delete_checkpoint_impl(ctx, request).await;
        audit_log(ctx, "checkpoint_delete", Some(&checkpoint_id), &result);
        result
    }

    /// Lists checkpoints visible to the caller. Tenant callers are restricted
    /// to their own tenant; admin callers see all checkpoints.
    pub async fn list_checkpoints(
        &self,
        ctx: &SecurityContext,
        limit: usize,
        offset: usize,
    ) -> Result<SessionCheckpointListResponse, SessionDomainError> {
        let result = self.list_checkpoints_impl(ctx, limit, offset).await;
        audit_log(ctx, "list_checkpoints", None, &result);
        result
    }

    async fn list_checkpoints_impl(
        &self,
        ctx: &SecurityContext,
        limit: usize,
        offset: usize,
    ) -> Result<SessionCheckpointListResponse, SessionDomainError> {
        let page = self
            .records
            .list_checkpoints(ctx.tenant_id(), limit, offset)
            .await?;
        let returned = page.checkpoints.len();
        let has_more = (offset as u64).saturating_add(returned as u64) < page.total;
        Ok(SessionCheckpointListResponse {
            checkpoints: page
                .checkpoints
                .into_iter()
                .map(|entry| SessionCheckpointSummary {
                    checkpoint_id: entry.checkpoint_id,
                    source_runtime_id: entry.source_runtime_id,
                    tenant_id: entry.tenant_id,
                    created_by: entry.created_by,
                    created_at_ms: entry.created_at_ms,
                })
                .collect(),
            total: page.total,
            has_more,
            next_offset: has_more.then_some(offset.saturating_add(returned)),
        })
    }

    async fn delete_checkpoint_impl(
        &self,
        ctx: &SecurityContext,
        request: SessionCheckpointDeleteRequest,
    ) -> Result<SessionCheckpointDeleteResult, SessionDomainError> {
        let checkpoint = self
            .records
            .get_checkpoint(&request.checkpoint_id)
            .await?
            .filter(|record| ctx.owns(record.tenant_id.as_deref()))
            .ok_or_else(|| SessionDomainError::NotFound {
                runtime_id: request.checkpoint_id.clone(),
            })?;
        // External resources first, metadata last. If provider/archive
        // cleanup fails, keeping the SQLite row makes the operation
        // discoverable and retryable rather than silently orphaning it.
        let registration = self.registration(&checkpoint.runtime_state.runtime_kind)?;
        let (provider_id, manager) = provider_for_isolation(registration, &checkpoint.isolation)?;
        manager
            .delete_snapshot(
                provider_protocol::BackendId(provider_id),
                provider_protocol::ProviderSnapshotId(checkpoint.provider_snapshot_id.clone()),
            )
            .await
            .map_err(map_provider_error)?;
        self.records
            .delete_checkpoint(&request.checkpoint_id)
            .await?;
        Ok(SessionCheckpointDeleteResult {
            checkpoint_id: request.checkpoint_id,
            deleted: true,
        })
    }

    async fn fork_impl(
        &self,
        ctx: &SecurityContext,
        request: SessionForkRequest,
    ) -> Result<SessionOpenResponse, SessionDomainError> {
        let parent = self
            .require_session(ctx, &request.parent_runtime_id)
            .await?;
        let registration = self.registration_for_record(&parent)?;
        let runtime = Arc::clone(&registration.runtime);
        let exported = runtime
            .export_state(&request.parent_runtime_id)
            .await
            .map_err(map_runtime_error)?;

        let runtime_id = request
            .runtime_id
            .clone()
            .unwrap_or_else(|| self.runtime_ids.next_runtime_id());
        let conversation_id = request
            .conversation_id
            .clone()
            .unwrap_or_else(|| parent.conversation_id.clone());
        let sender_id = request
            .sender_id
            .clone()
            .unwrap_or_else(|| parent.sender_id.clone());
        let now = self.clock.now_ms();

        // `docs/tenancy_design.md` §7 step 4: a fork opens a new session
        // owned by `ctx`'s tenant just like `open` does, so it counts against
        // the same `max_sessions` ceiling.
        let tenant_id_for_quota = ctx.tenant_id().map(str::to_string);
        if let Some(tenant_id) = &tenant_id_for_quota {
            self.reserve_tenant_session(tenant_id, ctx.quota.max_sessions)
                .map_err(|limit| SessionDomainError::QuotaExceeded {
                    scope: "sessions".to_string(),
                    limit,
                })?;
        }

        let (provider_id, manager) = provider_for_isolation(registration, &parent.isolation)?;
        let snapshot = manager
            .checkpoint_instance(&request.parent_runtime_id)
            .await
            .map_err(map_provider_error)?;
        let backend = manager
            .load_instance_from_snapshot(
                runtime_id.clone(),
                provider_protocol::BackendId(provider_id.clone()),
                ctx.owner_ref(),
                snapshot.snapshot_id.clone(),
                provider_options(&parent.workspace, &parent.isolation, &provider_id),
            )
            .await
            .map_err(map_provider_error)?;
        let _ = manager
            .delete_snapshot(
                provider_protocol::BackendId(provider_id),
                snapshot.snapshot_id,
            )
            .await;
        if let Err(error) = runtime
            .start(
                RuntimeStartRequest {
                    runtime_id: runtime_id.clone(),
                    conversation_id: conversation_id.clone(),
                    sender_id: sender_id.clone(),
                    workspace: parent.workspace.clone(),
                    state: Some(exported.clone()),
                    llm: None,
                    ext: Default::default(),
                },
                RuntimeExecutionContext {
                    operation_backend: backend,
                },
            )
            .await
            .map_err(map_runtime_error)
        {
            let _ = manager.stop_instance(&runtime_id).await;
            if let Some(tenant_id) = &tenant_id_for_quota {
                self.release_tenant_session(tenant_id);
            }
            return Err(error);
        }

        let record = SessionRecord {
            runtime_id,
            conversation_id,
            sender_id,
            status: SessionStatus::Idle,
            created_at_ms: now,
            updated_at_ms: now,
            workspace: parent.workspace,
            isolation: parent.isolation,
            capabilities: parent.capabilities,
            runtime: exported,
            llm: parent.llm,
            lease: None,
            lineage: Some(CheckpointLineage {
                parent_runtime_id: Some(request.parent_runtime_id),
                source_checkpoint_id: None,
            }),
            last_error: None,
            tenant_id: ctx.tenant_id().map(str::to_string),
            created_by: ctx.principal.clone(),
        };
        if let Err(error) = self.records.save(record.clone()).await {
            let _ = runtime.stop(&record.runtime_id).await;
            let _ = manager.stop_instance(&record.runtime_id).await;
            if let Some(tenant_id) = &tenant_id_for_quota {
                self.release_tenant_session(tenant_id);
            }
            return Err(error);
        }
        Ok(project_session(&record))
    }

    /// Shared `lease_table.is_some()`-gated holder check used by
    /// `submit_turn`/`close`. `Ok(())` unconditionally when lease enforcement
    /// is off. `SessionLeaseTable::check_holder` itself treats a claim with
    /// no `client_id` as an anonymous caller (passes only if the recorded
    /// lease, if any, is absent or stale) rather than rejecting outright —
    /// unlike `heartbeat`, which needs an identity to record against and so
    /// rejects a missing `client_id` explicitly.
    async fn check_lease_holder(
        &self,
        runtime_id: &str,
        lease: &SessionLeaseClaim,
    ) -> Result<(), SessionDomainError> {
        let Some(lease_table) = &self.lease_table else {
            return Ok(());
        };
        lease_table
            .check_holder(runtime_id, lease.client_id.as_deref())
            .await
            .map_err(|failure| lease_check_failure_to_domain_error(runtime_id, failure))
    }

    /// Look up `runtime_id` and enforce that `ctx` owns it. A session that
    /// does not exist and a session that exists but belongs to a different
    /// tenant are indistinguishable to the caller — both return `NotFound`
    /// (never `Forbidden`), so cross-tenant probing gets no distinguishing
    /// echo (`docs/tenancy_design.md` §3.2). This is the single ownership
    /// check point every session-lookup path in this crate funnels through.
    async fn require_session(
        &self,
        ctx: &SecurityContext,
        runtime_id: &str,
    ) -> Result<SessionRecord, SessionDomainError> {
        let record =
            self.records
                .get(runtime_id)
                .await?
                .ok_or_else(|| SessionDomainError::NotFound {
                    runtime_id: runtime_id.into(),
                })?;
        if !ctx.owns(record.tenant_id.as_deref()) {
            return Err(SessionDomainError::NotFound {
                runtime_id: runtime_id.into(),
            });
        }
        Ok(record)
    }

    /// Bridges the "SQLite row exists, adapter has no in-memory instance" gap
    /// a daemon restart leaves behind (`docs/pi_session_restore_plan.md`
    /// §1.2). Every session-lookup path that goes on to touch the runtime
    /// calls this right after `require_session`: a plain `attach` hit is the
    /// normal warm path (zero overhead, the common case); a `NotFound` with
    /// persisted runtime state (`record.runtime.state != Null`) replays
    /// `runtime.start` to lazily rebuild the adapter's in-memory instance,
    /// then retries `attach`. A `NotFound` with no persisted state (the
    /// runtime never captured any — e.g. `export_state` is unsupported, or
    /// this predates Phase 1) is genuinely unrecoverable and propagates
    /// unchanged. A restoration failure is recorded onto the row
    /// (`last_error` + `status: failed`, §1.4) before propagating, so the row
    /// itself explains why the next attempt hits the same wall until an
    /// explicit new `open`. On a successful restoration, a row left at
    /// `running` by the restart (the in-flight turn was orphaned, per §1.3)
    /// falls back to `idle`.
    async fn ensure_runtime_attached(
        &self,
        _ctx: &SecurityContext,
        record: &SessionRecord,
    ) -> Result<(), SessionDomainError> {
        let registration = self.registration_for_record(record)?;
        let runtime = Arc::clone(&registration.runtime);
        match runtime
            .check_alive(&record.runtime_id)
            .await
            .map_err(map_runtime_error)
        {
            Ok(true) => return Ok(()),
            Ok(false) | Err(SessionDomainError::NotFound { .. }) => {}
            Err(error) => return Err(error),
        }
        let (_, manager) = provider_for_isolation(registration, &record.isolation)?;
        let operation_backend = match manager
            .backend_for(&record.runtime_id)
            .map_err(map_provider_error)
        {
            Ok(backend) => backend,
            Err(error) => {
                self.mark_restoration_failed(record, &error).await;
                return Err(error);
            }
        };
        let context = RuntimeExecutionContext { operation_backend };
        match runtime
            .attach(&record.runtime_id, context.clone())
            .await
            .map_err(map_runtime_error)
        {
            Ok(()) => return Ok(()),
            Err(SessionDomainError::NotFound { .. }) => {}
            Err(error) => return Err(error),
        }

        if record.runtime.state == serde_json::Value::Null {
            return Err(SessionDomainError::NotFound {
                runtime_id: record.runtime_id.clone(),
            });
        }

        if let Err(error) = runtime
            .start(
                RuntimeStartRequest {
                    runtime_id: record.runtime_id.clone(),
                    conversation_id: record.conversation_id.clone(),
                    sender_id: record.sender_id.clone(),
                    workspace: record.workspace.clone(),
                    state: Some(record.runtime.clone()),
                    // `RuntimeStartRequest.llm` is a caller-supplied *override*
                    // hint (`LlmOverrideRequest`), not the already-resolved
                    // `record.llm: Option<ResolvedLlm>` — a replay has no fresh
                    // override to offer, same as `fork_impl`'s `llm: None`.
                    llm: None,
                    ext: Default::default(),
                },
                context.clone(),
            )
            .await
            .map_err(map_runtime_error)
        {
            self.mark_restoration_failed(record, &error).await;
            return Err(error);
        }

        if let Err(error) = runtime
            .attach(&record.runtime_id, context)
            .await
            .map_err(map_runtime_error)
        {
            self.mark_restoration_failed(record, &error).await;
            return Err(error);
        }

        if record.status == SessionStatus::Running {
            let mut updated = record.clone();
            updated.status = SessionStatus::Idle;
            updated.updated_at_ms = self.clock.now_ms();
            self.records.save(updated).await?;
        }
        Ok(())
    }

    /// Persists a failed restoration attempt onto the row (§1.4): `last_error`
    /// gets the adapter's fail-closed message (already carrying the
    /// `pi_sandbox_gone`/`pi_session_state_lost` prefix where applicable) and
    /// `status` flips to `failed`. Best-effort — a `SessionRepository` save
    /// failure here is swallowed so the caller still sees the original
    /// restoration error rather than having it masked by a save error.
    async fn mark_restoration_failed(&self, record: &SessionRecord, error: &SessionDomainError) {
        let mut updated = record.clone();
        updated.status = SessionStatus::Failed;
        updated.last_error = Some(error.to_string());
        updated.updated_at_ms = self.clock.now_ms();
        let _ = self.records.save(updated).await;
    }

    /// Ownership-only check for transport-layer paths that don't otherwise
    /// go through `SessionApplication` (e.g. `apps/server`'s SSE stream
    /// handler, which reads its own in-process stream table keyed only by
    /// `runtime_id`/`turn_id`). Same 404 semantics as `require_session`.
    pub async fn check_visible(
        &self,
        ctx: &SecurityContext,
        runtime_id: &str,
    ) -> Result<(), SessionDomainError> {
        self.require_session(ctx, runtime_id).await.map(|_| ())
    }

    /// Thin audit-logging wrapper around [`Self::list_sessions_impl`] — see
    /// [`audit_log`]. Not tied to one `runtime_id`, so the audit line carries
    /// `runtime_id = "-"`.
    pub async fn list_sessions(
        &self,
        ctx: &SecurityContext,
        limit: usize,
    ) -> Result<SessionListResponse, SessionDomainError> {
        let result = self.list_sessions_impl(ctx, limit).await;
        audit_log(ctx, "list_sessions", None, &result);
        result
    }

    /// v1 self-service query (`docs/tenancy_design.md` §4 closing line):
    /// most-recent-N active sessions visible to `ctx` plus a quota snapshot.
    /// Ownership scoping is the same `ctx.tenant_id()` passthrough used
    /// elsewhere (`None` = admin's global view, `Some(id)` = that tenant's
    /// own sessions only) — no separate admin-only branch.
    async fn list_sessions_impl(
        &self,
        ctx: &SecurityContext,
        limit: usize,
    ) -> Result<SessionListResponse, SessionDomainError> {
        let page = self.records.list_active(ctx.tenant_id(), limit).await?;
        let has_more = page.total_active as usize > page.sessions.len();
        Ok(SessionListResponse {
            sessions: page.sessions.iter().map(project_session_summary).collect(),
            has_more,
            quota: TenantQuotaSnapshot {
                max_sessions: ctx.quota.max_sessions,
                active_sessions: page.total_active,
                max_requests_per_minute: ctx.quota.max_requests_per_minute,
            },
        })
    }

    pub async fn tenant_has_active_sessions(
        &self,
        tenant_id: &str,
    ) -> Result<bool, SessionDomainError> {
        let page = self.records.list_active(Some(tenant_id), 1).await?;
        Ok(page.total_active > 0)
    }

    /// All non-terminal sessions across every tenant, most-recently-updated
    /// first, capped at `limit` — candidate source for the reclaim sweep
    /// (`reclaim_sweeper.rs`). Deliberately tenant-unscoped (`None`): unlike
    /// `list_sessions` (a self-service query gated by the caller's own
    /// `ctx.tenant_id()`), the sweep is server-internal trusted code that
    /// must see every tenant's sessions to do its job.
    pub async fn list_active_sessions_for_reclaim_sweep(
        &self,
        limit: usize,
    ) -> Result<SessionListPage, SessionDomainError> {
        self.records.list_active(None, limit).await
    }
}

/// Result of [`SessionRepository::list_active`]: `sessions` is capped at the
/// caller-supplied `limit` (most-recently-updated first), `total_active` is
/// the true count matching the filter, uncapped — callers derive `has_more`
/// from the two (`total_active > sessions.len()`) and can report an accurate
/// quota-usage number even when the list itself is truncated.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionListPage {
    pub sessions: Vec<SessionRecord>,
    pub total_active: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CheckpointListPage {
    pub checkpoints: Vec<CheckpointListEntry>,
    pub total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointListEntry {
    pub checkpoint_id: String,
    pub source_runtime_id: String,
    pub tenant_id: Option<String>,
    pub created_by: Option<String>,
    pub created_at_ms: u64,
}

#[async_trait::async_trait]
pub trait SessionRepository: Send + Sync {
    async fn get(&self, runtime_id: &str) -> Result<Option<SessionRecord>, SessionDomainError>;
    async fn save(&self, record: SessionRecord) -> Result<(), SessionDomainError>;
    async fn list_active(
        &self,
        tenant_id: Option<&str>,
        limit: usize,
    ) -> Result<SessionListPage, SessionDomainError>;
    async fn save_checkpoint(
        &self,
        _record: crate::CheckpointRecord,
    ) -> Result<(), SessionDomainError> {
        Err(SessionDomainError::Internal {
            message: "checkpoint repository is not configured".into(),
            source: None,
        })
    }
    async fn get_checkpoint(
        &self,
        _checkpoint_id: &str,
    ) -> Result<Option<crate::CheckpointRecord>, SessionDomainError> {
        Err(SessionDomainError::Internal {
            message: "checkpoint repository is not configured".into(),
            source: None,
        })
    }
    async fn delete_checkpoint(&self, _checkpoint_id: &str) -> Result<bool, SessionDomainError> {
        Err(SessionDomainError::Internal {
            message: "checkpoint repository is not configured".into(),
            source: None,
        })
    }
    async fn list_checkpoints(
        &self,
        _tenant_id: Option<&str>,
        _limit: usize,
        _offset: usize,
    ) -> Result<CheckpointListPage, SessionDomainError> {
        Err(SessionDomainError::Internal {
            message: "checkpoint repository is not configured".into(),
            source: None,
        })
    }
    async fn active_session_counts_by_tenant(
        &self,
    ) -> Result<HashMap<String, usize>, SessionDomainError> {
        Ok(HashMap::new())
    }
}

pub trait TurnIdGenerator: Send + Sync {
    fn next_turn_id(&self) -> String;
}

pub trait RuntimeIdGenerator: Send + Sync {
    fn next_runtime_id(&self) -> String;
}

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

pub struct NormalizedSessionEnvironment {
    pub workspace: WorkspaceFacts,
    pub isolation: IsolationFacts,
    pub sandbox_capabilities: std::collections::BTreeSet<crate::SandboxCapability>,
    pub llm: Option<crate::ResolvedLlm>,
    pub lease: Option<SessionLease>,
}

#[async_trait::async_trait]
pub trait SessionEnvironmentNormalizer: Send + Sync {
    async fn normalize(
        &self,
        ctx: &SecurityContext,
        request: &SessionOpenRequest,
    ) -> Result<NormalizedSessionEnvironment, SessionDomainError>;
}

/// `docs/tenancy_design.md` §5.4's `(role, workspace_spec, provider)` triple,
/// collapsed into one reusable call so every `SessionEnvironmentNormalizer`
/// impl enforces the same rule instead of hand-rolling its own version:
///
/// ```text
/// (Admin,  _,          _      ) => allow
/// (Tenant, Git(url),   sandbox) => allow iff the url passes hygiene checks
/// (Tenant, _,          _      ) => reject
/// ```
///
/// `provider_is_sandbox` names whether the caller's runtime/normalizer
/// pair actually provides an isolation boundary (container/VM/remote) rather
/// than running on the host directly. A normalizer backing a host-only
///
/// This function only performs the *admission* check — it does not clone or
/// touch a git url in any way; actual sandboxed clone/bootstrap is `docs/
/// tenancy_design.md` §7 step 3's scope.
pub fn enforce_workspace_axiom(
    ctx: &SecurityContext,
    workspace: &session_protocol::WorkspaceSpec,
    provider_is_sandbox: bool,
) -> Result<(), SessionDomainError> {
    if ctx.is_admin() {
        return Ok(());
    }
    match (workspace, provider_is_sandbox) {
        (session_protocol::WorkspaceSpec::Git { url, .. }, true) => validate_git_url_hygiene(url),
        _ => Err(SessionDomainError::InvalidRequest {
            message:
                "tenant sessions require a sandboxed provider and a git workspace source (see \
                 docs/tenancy_design.md §0/§5.4)"
                    .to_string(),
        }),
    }
}

/// Minimal URL hygiene for the `(Tenant, Git(url), sandbox)` admission arm:
/// only `https://` is accepted (no `file://`/`ssh://`/`git://` — those either
/// reach the host filesystem directly or carry key-based auth this control
/// plane doesn't manage), and the URL must not carry an embedded
/// `user:password@host` credential (those leak into logs/records verbatim
/// since `WorkspaceSpec` round-trips through `SessionRecord`).
///
/// This is deliberately narrow: it is an admission gate, not a full URL
/// validator. Widen it if a real sandboxed clone step (§7 step 3) turns up a
/// concrete bypass this doesn't catch.
fn validate_git_url_hygiene(url: &str) -> Result<(), SessionDomainError> {
    let reject = |reason: &str| {
        Err(SessionDomainError::InvalidRequest {
            message: format!("git workspace url rejected: {reason}"),
        })
    };
    let Some(rest) = url.strip_prefix("https://") else {
        return reject("only https:// git urls are allowed for tenant sessions");
    };
    let authority = rest.split('/').next().unwrap_or("");
    if authority.contains('@') {
        return reject("git urls must not embed credentials (user:password@host)");
    }
    Ok(())
}

fn domain_runtime_capability(
    capability: session_protocol::SessionRuntimeCapability,
) -> Option<RuntimeCapability> {
    Some(match capability {
        session_protocol::SessionRuntimeCapability::Interaction => RuntimeCapability::Interaction,
        session_protocol::SessionRuntimeCapability::Steering => RuntimeCapability::Steering,
        session_protocol::SessionRuntimeCapability::Fork => RuntimeCapability::Fork,
        session_protocol::SessionRuntimeCapability::Checkpoint => RuntimeCapability::Checkpoint,
        session_protocol::SessionRuntimeCapability::StateExport => RuntimeCapability::StateExport,
        session_protocol::SessionRuntimeCapability::ModelOverride => {
            RuntimeCapability::ModelOverride
        }
        session_protocol::SessionRuntimeCapability::ReasoningControl => {
            RuntimeCapability::ReasoningControl
        }
        _ => return None,
    })
}

fn domain_sandbox_capability(
    capability: &session_protocol::SessionSandboxCapability,
) -> crate::SandboxCapability {
    match capability {
        session_protocol::SessionSandboxCapability::Exec => crate::SandboxCapability::Exec,
        session_protocol::SessionSandboxCapability::FileRead => crate::SandboxCapability::FileRead,
        session_protocol::SessionSandboxCapability::FileWrite => {
            crate::SandboxCapability::FileWrite
        }
        session_protocol::SessionSandboxCapability::Pause => crate::SandboxCapability::Pause,
        session_protocol::SessionSandboxCapability::Snapshot => crate::SandboxCapability::Snapshot,
        session_protocol::SessionSandboxCapability::Network => crate::SandboxCapability::Network,
        _ => unreachable!("unknown wire capabilities are discarded during deserialization"),
    }
}

fn capability_name<T: serde::Serialize>(capability: &T) -> String {
    serde_json::to_value(capability)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".into())
}

fn unsupported_runtime_capability(capability: &str) -> SessionDomainError {
    SessionDomainError::UnsupportedCapability {
        family: crate::CapabilityFamily::Runtime,
        capability: capability.into(),
    }
}

/// Maps a lease-table rejection onto the wire-facing domain error. `Busy`
/// carries the holder identity straight through — `SessionDomainError::
/// LeaseConflict` has exactly these fields already (see
/// `docs/session_orchestration_skeleton.md`, Component B). `ClockSkew` is
/// surfaced as `Unavailable` rather than a lease-specific variant since it is
/// a daemon-wide fault, not a per-session conflict.
fn lease_check_failure_to_domain_error(
    runtime_id: &str,
    failure: LeaseCheckFailure,
) -> SessionDomainError {
    match failure {
        LeaseCheckFailure::Busy {
            holder_client_id,
            holder_hostname,
            holder_pid,
            ..
        } => SessionDomainError::LeaseConflict {
            runtime_id: runtime_id.to_string(),
            holder_client_id: Some(holder_client_id),
            holder_pid,
            holder_hostname,
        },
        LeaseCheckFailure::ClockSkew => SessionDomainError::Unavailable {
            message: "daemon wall clock is before UNIX_EPOCH; lease enforcement is fail-closed"
                .to_string(),
        },
    }
}

/// `docs/tenancy_design.md` §6: one structured event per control-plane
/// operation, carrying `principal`/`tenant`/`operation`/`runtime_id`/
/// `result`. Emitted as a `tracing::info!` under the `"audit"` target rather
/// than a bespoke persistence subsystem — this crate already depends on
/// `tracing`, and §6 only asks that these facts be recorded somewhere
/// queryable, not that this crate own storage/rotation/querying for them.
/// Whoever operates the daemon wires a `tracing` subscriber (file, syslog,
/// OTel, ...) onto the `"audit"` target the same way they would for any
/// other `tracing` output.
///
/// Every `SessionApplication` public method that changes or reveals
/// control-plane state funnels through this — see each method's thin
/// `pub async fn` wrapper around its `*_impl`. `check_visible` is the one
/// exception: it is a read-only ownership probe used internally by
/// transport-layer code, not a control-plane operation in its own right.
fn audit_log<T>(
    ctx: &SecurityContext,
    operation: &str,
    runtime_id: Option<&str>,
    result: &Result<T, SessionDomainError>,
) {
    match result {
        Ok(_) => {
            tracing::info!(
                target: "audit",
                principal = %ctx.principal,
                tenant = ctx.tenant_id().unwrap_or("-"),
                operation,
                runtime_id = runtime_id.unwrap_or("-"),
                result = "ok",
            );
        }
        Err(error) => {
            tracing::info!(
                target: "audit",
                principal = %ctx.principal,
                tenant = ctx.tenant_id().unwrap_or("-"),
                operation,
                runtime_id = runtime_id.unwrap_or("-"),
                result = "error",
                error = %error,
            );
        }
    }
}
