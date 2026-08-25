use crate::{
    project_runtime_event, project_session, project_session_summary, CheckpointLineage,
    EffectiveCapabilities, IsolationBoundary, IsolationFacts, LeaseCheckFailure,
    OpaqueRuntimeState, RuntimeAdapter, RuntimeCapability, RuntimeEntryContext,
    RuntimeInteractionInput, RuntimeLoadRequest, RuntimeStartRequest, RuntimeTurnInput,
    SecurityContext, SessionDomainError, SessionLease, SessionLeaseTable, SessionRecord,
    SessionStatus, WorkspaceFacts,
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
    pub adapter: Arc<dyn RuntimeAdapter>,
    pub environment: Arc<dyn SessionEnvironmentNormalizer>,
}

impl RuntimeRegistration {
    pub fn new(
        adapter: Arc<dyn RuntimeAdapter>,
        environment: Arc<dyn SessionEnvironmentNormalizer>,
    ) -> Self {
        Self {
            adapter,
            environment,
        }
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
        runtime: Arc<dyn RuntimeAdapter>,
        records: Arc<dyn SessionRepository>,
        turn_ids: Arc<dyn TurnIdGenerator>,
        runtime_ids: Arc<dyn RuntimeIdGenerator>,
        environment: Arc<dyn SessionEnvironmentNormalizer>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let default_runtime_kind = runtime.kind().to_string();
        Self::with_runtime_registry(
            default_runtime_kind,
            [RuntimeRegistration::new(runtime, environment)],
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
            let kind = registration.adapter.kind().trim().to_string();
            if kind.is_empty() {
                return Err(SessionDomainError::InvalidRequest {
                    message: "runtime adapter kind must not be empty".into(),
                });
            }
            if runtimes.insert(kind.clone(), registration).is_some() {
                return Err(SessionDomainError::InvalidRequest {
                    message: format!("duplicate runtime adapter kind: {kind}"),
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
        let runtime = Arc::clone(&registration.adapter);
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

        let runtime_wire_capabilities = runtime.capabilities_for_request(&request);
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

        let now = self.clock.now_ms();
        if let Err(error) = runtime
            .start(RuntimeStartRequest {
                runtime_id: runtime_id.clone(),
                conversation_id: request.conversation_id.clone(),
                sender_id: request.sender_id.clone(),
                workspace: normalized.workspace.clone(),
                state: None,
                llm: request.llm.clone(),
                owner_ref: ctx.owner_ref(),
                ext: request.ext.clone(),
            })
            .await
        {
            if let Some(tenant_id) = &tenant_id_for_quota {
                self.release_tenant_session(tenant_id);
            }
            return Err(error);
        }

        // Reuse the pre-existing runtime-state quarantine slot
        let runtime_state = match runtime.export_state(&runtime_id).await {
            Ok(state) => state,
            Err(SessionDomainError::UnsupportedCapability { .. }) => OpaqueRuntimeState {
                runtime_kind: runtime.kind().into(),
                schema_version: 1,
                state: serde_json::Value::Null,
            },
            Err(error) => {
                let _ = runtime.stop(&runtime_id).await;
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
        let runtime = Arc::clone(&self.registration_for_record(&record)?.adapter);
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
            .await;
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
                    crate::RuntimeEvent::Completed { .. } | crate::RuntimeEvent::Failed { .. }
                );
                // A terminal event is the durability boundary: persist the
                // runtime's latest opaque state before allowing a client to
                // observe completion. This makes a successful terminal event
                // a reliable restart-recovery point for every adapter that
                // implements state export.
                if terminal {
                    let persisted = async {
                        let state = match state_runtime.export_state(&event_runtime_id).await {
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
                            crate::RuntimeEvent::Completed { .. } => SessionStatus::Idle,
                            crate::RuntimeEvent::Failed { .. } => SessionStatus::Failed,
                            _ => unreachable!("guarded by terminal match"),
                        };
                        record.updated_at_ms = clock.now_ms();
                        records.save(record).await
                    }
                    .await;
                    if let Err(error) = persisted {
                        event = crate::RuntimeEvent::Failed {
                            error: crate::RuntimeFailure {
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
                    .cancel(&event_runtime_id, Some(&event_turn_id))
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
        let runtime = Arc::clone(&self.registration_for_record(&record)?.adapter);
        runtime
            .answer_interaction(RuntimeInteractionInput {
                runtime_id: request.runtime_id.clone(),
                turn_id: request.turn_id.clone(),
                interaction_id: request.interaction_id,
                answer: request.answer,
                ext: request.ext,
            })
            .await?;
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
    /// sweep (`reclaim_sweeper.rs`) once `RuntimeAdapter::check_alive` has
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
        let runtime = Arc::clone(&self.registration_for_record(&record)?.adapter);
        match runtime.stop(runtime_id).await {
            Ok(()) => {}
            Err(SessionDomainError::NotFound { .. })
                if record.runtime.state != serde_json::Value::Null =>
            {
                // §1.4 close special case: no in-memory adapter instance to
                // stop (a daemon restart happened since this session was
                // last touched) but persisted runtime state is available —
                // destroy whatever it references directly instead of paying
                // for a full restore-then-kill round trip through
                // `ensure_runtime_attached` just to close it right back down.
                runtime
                    .cleanup_from_state(runtime_id, &record.runtime)
                    .await?;
            }
            Err(error) => return Err(error),
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
    /// runtime adapter, then delegates to `RuntimeAdapter::check_alive`. Not
    /// audit-logged (unlike `close`/`reclaim`): this is a read-only probe, not
    /// a state-changing control operation.
    pub async fn check_alive(
        &self,
        ctx: &SecurityContext,
        runtime_id: &str,
    ) -> Result<bool, SessionDomainError> {
        let record = self.require_session(ctx, runtime_id).await?;
        let runtime = Arc::clone(&self.registration_for_record(&record)?.adapter);
        runtime.check_alive(runtime_id).await
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
        let runtime = Arc::clone(&self.registration_for_record(&record)?.adapter);
        runtime.cancel(runtime_id, turn_id).await
    }

    /// Export `request.parent_runtime_id`'s state and start a new runtime
    /// from it. Not lease-gated (forking off a parent does not mutate it).
    /// `runtime.export_state` defaults to `UnsupportedCapability` on every
    /// `RuntimeAdapter` that hasn't opted in — today that is every adapter —
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
        let runtime = Arc::clone(&self.registration_for_record(&record)?.adapter);
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
        let payload = runtime.checkpoint(&request.runtime_id).await?;
        let checkpoint_id = payload.checkpoint_id.clone();
        let created_at_ms = self.clock.now_ms();
        self.records
            .save_checkpoint(crate::CheckpointRecord {
                checkpoint_id: checkpoint_id.clone(),
                source_runtime_id: request.runtime_id.clone(),
                provider_snapshot_id: payload.provider_snapshot_id,
                runtime_state: payload.runtime_state,
                workspace: record.workspace,
                isolation: record.isolation,
                capabilities: record.capabilities,
                owner_ref: ctx.owner_ref(),
                tenant_id: record.tenant_id,
                created_by: Some(ctx.principal.clone()),
                created_at_ms,
            })
            .await?;
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
        let runtime = Arc::clone(
            &self
                .registration(&checkpoint.runtime_state.runtime_kind)?
                .adapter,
        );
        let tenant_id_for_quota = ctx.tenant_id().map(str::to_string);
        if let Some(tenant_id) = &tenant_id_for_quota {
            self.reserve_tenant_session(tenant_id, ctx.quota.max_sessions)
                .map_err(|limit| SessionDomainError::QuotaExceeded {
                    scope: "sessions".into(),
                    limit,
                })?;
        }
        if let Err(error) = runtime
            .load_from_checkpoint(RuntimeLoadRequest {
                new_runtime_id: runtime_id.clone(),
                owner_ref: ctx.owner_ref(),
                provider_snapshot_id: checkpoint.provider_snapshot_id.clone(),
                runtime_state: checkpoint.runtime_state.clone(),
                llm: request.llm,
            })
            .await
        {
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
        let runtime = Arc::clone(
            &self
                .registration(&checkpoint.runtime_state.runtime_kind)?
                .adapter,
        );
        runtime
            .delete_checkpoint(
                checkpoint.runtime_state.clone(),
                checkpoint.provider_snapshot_id.clone(),
            )
            .await?;
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
        let runtime = Arc::clone(&self.registration_for_record(&parent)?.adapter);
        let exported = runtime.export_state(&request.parent_runtime_id).await?;

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

        if let Err(error) = runtime
            .start(RuntimeStartRequest {
                runtime_id: runtime_id.clone(),
                conversation_id: conversation_id.clone(),
                sender_id: sender_id.clone(),
                workspace: parent.workspace.clone(),
                state: Some(exported.clone()),
                llm: None,
                owner_ref: ctx.owner_ref(),
                ext: Default::default(),
            })
            .await
        {
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
        ctx: &SecurityContext,
        record: &SessionRecord,
    ) -> Result<(), SessionDomainError> {
        let runtime = Arc::clone(&self.registration_for_record(record)?.adapter);
        match runtime.attach(&record.runtime_id).await {
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
            .start(RuntimeStartRequest {
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
                owner_ref: ctx.owner_ref(),
                ext: Default::default(),
            })
            .await
        {
            self.mark_restoration_failed(record, &error).await;
            return Err(error);
        }

        if let Err(error) = runtime.attach(&record.runtime_id).await {
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
/// `provider_is_sandbox` names whether the caller's `RuntimeAdapter`/normalizer
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RuntimeEvent, RuntimeStartRequest, TenantQuota, STALE_LEASE_THRESHOLD_MS};
    use async_trait::async_trait;
    use session_protocol::{SessionAcceptedInputKind, SessionLifecycleStatus, SessionUsage};
    use std::collections::BTreeSet;
    use tokio::sync::Mutex;

    fn admin_ctx() -> SecurityContext {
        SecurityContext::admin("test")
    }

    fn tenant_ctx() -> SecurityContext {
        SecurityContext::tenant("tenant-a", "test")
    }

    /// Shared `list_active` logic for the in-memory test doubles below
    /// ([`MemoryRepository`], [`MapRepository`]) so each one only has to
    /// supply its records; the filter/sort/cap semantics stay in one place
    /// rather than being duplicated per double.
    fn select_active(
        records: impl Iterator<Item = SessionRecord>,
        tenant_id: Option<&str>,
        limit: usize,
    ) -> SessionListPage {
        let mut matching: Vec<SessionRecord> = records
            .filter(|record| {
                matches!(
                    record.status,
                    SessionStatus::Opening
                        | SessionStatus::Idle
                        | SessionStatus::Running
                        | SessionStatus::Paused
                )
            })
            .filter(|record| match tenant_id {
                None => true,
                Some(id) => record.tenant_id.as_deref() == Some(id),
            })
            .collect();
        matching.sort_by(|a, b| b.updated_at_ms.cmp(&a.updated_at_ms));
        let total_active = matching.len() as u32;
        matching.truncate(limit);
        SessionListPage {
            sessions: matching,
            total_active,
        }
    }

    struct EmptyRepository;

    #[async_trait]
    impl SessionRepository for EmptyRepository {
        async fn get(
            &self,
            _runtime_id: &str,
        ) -> Result<Option<SessionRecord>, SessionDomainError> {
            Ok(Some(test_record()))
        }

        async fn save(&self, _record: SessionRecord) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn list_active(
            &self,
            _tenant_id: Option<&str>,
            _limit: usize,
        ) -> Result<SessionListPage, SessionDomainError> {
            Ok(SessionListPage {
                sessions: Vec::new(),
                total_active: 0,
            })
        }
    }

    fn test_record() -> SessionRecord {
        SessionRecord {
            runtime_id: "runtime-1".into(),
            conversation_id: "conversation-1".into(),
            sender_id: "sender-1".into(),
            status: crate::SessionStatus::Idle,
            created_at_ms: 1,
            updated_at_ms: 1,
            workspace: crate::WorkspaceFacts {
                workspace_id: "workspace-1".into(),
                root: ".".into(),
                access: crate::WorkspaceAccess::ReadWrite,
                revision: None,
                metadata: serde_json::Value::Null,
            },
            isolation: crate::IsolationFacts {
                boundary: crate::IsolationBoundary::Host,
                workspace_access: crate::WorkspaceAccess::ReadWrite,
                network: crate::NetworkIsolation::None,
                metadata: serde_json::Value::Null,
            },
            capabilities: crate::EffectiveCapabilities {
                sandbox: Default::default(),
                runtime: [
                    crate::RuntimeCapability::ModelOverride,
                    crate::RuntimeCapability::ReasoningControl,
                    crate::RuntimeCapability::Interaction,
                ]
                .into_iter()
                .collect(),
            },
            runtime: crate::OpaqueRuntimeState {
                runtime_kind: "test".into(),
                schema_version: 1,
                state: serde_json::Value::Null,
            },
            llm: None,
            lease: None,
            lineage: None,
            last_error: None,
            tenant_id: None,
            created_by: "test".to_string(),
        }
    }

    struct FixedTurnId;

    impl TurnIdGenerator for FixedTurnId {
        fn next_turn_id(&self) -> String {
            "turn-fixed".into()
        }
    }

    impl RuntimeIdGenerator for FixedTurnId {
        fn next_runtime_id(&self) -> String {
            "runtime-fixed".into()
        }
    }

    impl Clock for FixedTurnId {
        fn now_ms(&self) -> u64 {
            42
        }
    }

    struct UnusedEnvironment;

    #[async_trait]
    impl SessionEnvironmentNormalizer for UnusedEnvironment {
        async fn normalize(
            &self,
            _ctx: &SecurityContext,
            _request: &SessionOpenRequest,
        ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
            unreachable!("turn submission does not normalize an open request")
        }
    }

    struct TestEnvironment;

    #[async_trait]
    impl SessionEnvironmentNormalizer for TestEnvironment {
        async fn normalize(
            &self,
            _ctx: &SecurityContext,
            _request: &SessionOpenRequest,
        ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
            Ok(NormalizedSessionEnvironment {
                workspace: crate::WorkspaceFacts {
                    workspace_id: "workspace-normalized".into(),
                    root: "/normalized".into(),
                    access: crate::WorkspaceAccess::ReadWrite,
                    revision: None,
                    metadata: serde_json::Value::Null,
                },
                isolation: crate::IsolationFacts {
                    boundary: crate::IsolationBoundary::Container,
                    workspace_access: crate::WorkspaceAccess::ReadWrite,
                    network: crate::NetworkIsolation::Restricted,
                    metadata: serde_json::Value::Null,
                },
                sandbox_capabilities: [crate::SandboxCapability::Exec].into_iter().collect(),
                llm: None,
                lease: None,
            })
        }
    }

    /// Simulates a buggy normalizer that ignores `ctx` entirely and always
    /// grants a host isolation boundary — i.e. it does *not* call
    /// `enforce_workspace_axiom` itself. Used to prove the fail-closed second
    /// gate in `SessionApplication::open` (§0/§7 step 2) rejects a tenant
    /// session on its own, without depending on the normalizer having gotten
    /// the `(role, workspace, provider)` check right.
    struct BuggyHostBoundaryEnvironment;

    #[async_trait]
    impl SessionEnvironmentNormalizer for BuggyHostBoundaryEnvironment {
        async fn normalize(
            &self,
            _ctx: &SecurityContext,
            _request: &SessionOpenRequest,
        ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
            Ok(NormalizedSessionEnvironment {
                workspace: crate::WorkspaceFacts {
                    workspace_id: "workspace-buggy".into(),
                    root: "/buggy".into(),
                    access: crate::WorkspaceAccess::ReadWrite,
                    revision: None,
                    metadata: serde_json::Value::Null,
                },
                isolation: crate::IsolationFacts {
                    boundary: crate::IsolationBoundary::Host,
                    workspace_access: crate::WorkspaceAccess::ReadWrite,
                    network: crate::NetworkIsolation::None,
                    metadata: serde_json::Value::Null,
                },
                sandbox_capabilities: [crate::SandboxCapability::Exec].into_iter().collect(),
                llm: None,
                lease: None,
            })
        }
    }

    #[derive(Default)]
    struct MemoryRepository(Mutex<Option<SessionRecord>>);

    #[async_trait]
    impl SessionRepository for MemoryRepository {
        async fn get(&self, runtime_id: &str) -> Result<Option<SessionRecord>, SessionDomainError> {
            Ok(self
                .0
                .lock()
                .await
                .clone()
                .filter(|record| record.runtime_id == runtime_id))
        }

        async fn save(&self, record: SessionRecord) -> Result<(), SessionDomainError> {
            *self.0.lock().await = Some(record);
            Ok(())
        }

        async fn list_active(
            &self,
            tenant_id: Option<&str>,
            limit: usize,
        ) -> Result<SessionListPage, SessionDomainError> {
            Ok(select_active(
                self.0.lock().await.clone().into_iter(),
                tenant_id,
                limit,
            ))
        }
    }

    #[derive(Default)]
    struct CompletingRuntime {
        started: Mutex<Option<RuntimeStartRequest>>,
        submitted: Mutex<Option<RuntimeTurnInput>>,
        stopped: Mutex<bool>,
        checkpoint_delete_fails: bool,
    }

    #[async_trait]
    impl RuntimeAdapter for CompletingRuntime {
        fn kind(&self) -> &str {
            "test"
        }

        fn capabilities(&self) -> BTreeSet<session_protocol::SessionRuntimeCapability> {
            BTreeSet::new()
        }

        async fn start(&self, request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
            *self.started.lock().await = Some(request);
            Ok(())
        }

        async fn stop(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            *self.stopped.lock().await = true;
            Ok(())
        }

        async fn attach(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn check_alive(&self, _runtime_id: &str) -> Result<bool, SessionDomainError> {
            Ok(true)
        }

        async fn submit_turn(
            &self,
            input: RuntimeTurnInput,
        ) -> Result<crate::RuntimeEventReceiver, SessionDomainError> {
            *self.submitted.lock().await = Some(input);
            let (tx, rx) = mpsc::channel(2);
            tx.send(RuntimeEvent::Completed {
                outcome: session_protocol::SessionTurnOutcome::Complete,
                usage: SessionUsage::default(),
            })
            .await
            .unwrap();
            Ok(rx)
        }

        async fn answer_interaction(
            &self,
            _input: RuntimeInteractionInput,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn cancel(
            &self,
            _runtime_id: &str,
            _turn_id: Option<&str>,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn delete_checkpoint(
            &self,
            _runtime_state: OpaqueRuntimeState,
            _provider_snapshot_id: String,
        ) -> Result<(), SessionDomainError> {
            if self.checkpoint_delete_fails {
                Err(SessionDomainError::Unavailable {
                    message: "snapshot delete failed".into(),
                })
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn receipt_and_stream_share_the_server_assigned_turn_id() {
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            Arc::new(EmptyRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        );
        let submission = application
            .submit_turn(
                &admin_ctx(),
                SessionTurnRequest {
                    runtime_id: "runtime-1".into(),
                    text: "hello".into(),
                    entry: Default::default(),
                    llm: None,
                    reasoning_effort: None,
                    client_request_id: None,
                    ext: Default::default(),
                    lease: Default::default(),
                },
            )
            .await
            .unwrap();
        assert_eq!(submission.receipt.turn_id, "turn-fixed");
        assert_eq!(
            submission.receipt.accepted_kind,
            SessionAcceptedInputKind::Turn
        );
        let mut events = submission.events.expect("new turn carries an event stream");
        let event = events.recv().await.unwrap();
        let event = serde_json::to_value(event).unwrap();
        assert_eq!(event["runtime_id"], submission.receipt.runtime_id);
        assert_eq!(event["turn_id"], submission.receipt.turn_id);
        assert_eq!(event["kind"], "turn_completed");
    }

    #[tokio::test]
    async fn open_normalizes_intent_and_persists_opaque_runtime_state() {
        let repository = Arc::new(MemoryRepository::default());
        let runtime = Arc::new(CompletingRuntime::default());
        let application = SessionApplication::new(
            runtime.clone(),
            repository.clone(),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(TestEnvironment),
            Arc::new(FixedTurnId),
        );
        let response = application
            .open(
                &admin_ctx(),
                SessionOpenRequest {
                    runtime_id: None,
                    runtime_kind: None,
                    conversation_id: "conversation".into(),
                    sender_id: "sender".into(),
                    workspace: Default::default(),
                    deployment: Default::default(),
                    requested_capabilities: Default::default(),
                    llm: None,
                    ext: Default::default(),
                    lease: Default::default(),
                },
            )
            .await
            .unwrap();

        assert_eq!(response.runtime_id, "runtime-fixed");
        assert_eq!(response.runtime_kind, "test");
        assert_eq!(response.workspace.root, "/normalized");
        let stored = repository.0.lock().await.clone().unwrap();
        assert_eq!(stored.runtime.runtime_kind, "test");
        assert_eq!(stored.runtime.schema_version, 1);
        assert_eq!(stored.runtime.state, serde_json::Value::Null);
        let started = runtime.started.lock().await.clone().unwrap();
        assert_eq!(started.conversation_id, "conversation");
        assert_eq!(started.sender_id, "sender");
    }

    #[tokio::test]
    async fn runtime_registry_uses_default_and_explicit_kinds() {
        #[derive(Default)]
        struct XiaooTestRuntime(CompletingRuntime);

        #[async_trait]
        impl RuntimeAdapter for XiaooTestRuntime {
            fn kind(&self) -> &str {
                "xiaoo"
            }
            fn capabilities(&self) -> BTreeSet<session_protocol::SessionRuntimeCapability> {
                BTreeSet::new()
            }
            async fn start(&self, request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
                self.0.start(request).await
            }
            async fn stop(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
                self.0.stop(runtime_id).await
            }
            async fn attach(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
                self.0.attach(runtime_id).await
            }
            async fn check_alive(&self, _runtime_id: &str) -> Result<bool, SessionDomainError> {
                Ok(true)
            }
            async fn submit_turn(
                &self,
                input: RuntimeTurnInput,
            ) -> Result<crate::RuntimeEventReceiver, SessionDomainError> {
                self.0.submit_turn(input).await
            }
            async fn answer_interaction(
                &self,
                input: RuntimeInteractionInput,
            ) -> Result<(), SessionDomainError> {
                self.0.answer_interaction(input).await
            }
            async fn cancel(
                &self,
                runtime_id: &str,
                turn_id: Option<&str>,
            ) -> Result<(), SessionDomainError> {
                self.0.cancel(runtime_id, turn_id).await
            }
        }

        let pi = Arc::new(CompletingRuntime::default());
        let xiaoo = Arc::new(XiaooTestRuntime::default());
        let application = SessionApplication::with_runtime_registry(
            "test",
            [
                RuntimeRegistration::new(pi.clone(), Arc::new(TestEnvironment)),
                RuntimeRegistration::new(xiaoo.clone(), Arc::new(TestEnvironment)),
            ],
            Arc::new(MemoryRepository::default()),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
        )
        .unwrap();

        application
            .open(&admin_ctx(), open_request(Default::default()))
            .await
            .unwrap();
        assert!(pi.started.lock().await.is_some());

        let unknown = application
            .open(
                &admin_ctx(),
                SessionOpenRequest {
                    runtime_id: Some("other".into()),
                    runtime_kind: Some("unknown".into()),
                    ..open_request(Default::default())
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(unknown, SessionDomainError::InvalidRequest { .. }));

        // Use a separate repository so the fixed id from the default-open
        // branch cannot trigger re-attach instead of a fresh explicit open.
        let explicit = SessionApplication::with_runtime_registry(
            "test",
            [
                RuntimeRegistration::new(pi, Arc::new(TestEnvironment)),
                RuntimeRegistration::new(xiaoo.clone(), Arc::new(TestEnvironment)),
            ],
            Arc::new(MemoryRepository::default()),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
        )
        .unwrap();
        explicit
            .open(
                &admin_ctx(),
                SessionOpenRequest {
                    runtime_kind: Some("xiaoo".into()),
                    ..open_request(Default::default())
                },
            )
            .await
            .unwrap();
        assert!(xiaoo.0.started.lock().await.is_some());
    }

    fn open_request(workspace: session_protocol::WorkspaceSpec) -> SessionOpenRequest {
        SessionOpenRequest {
            runtime_id: None,
            runtime_kind: None,
            conversation_id: "conversation".into(),
            sender_id: "sender".into(),
            workspace,
            deployment: Default::default(),
            requested_capabilities: Default::default(),
            llm: None,
            ext: Default::default(),
            lease: Default::default(),
        }
    }

    /// §0/§7 step 2, enforcement point 2: the fail-closed assertion in
    /// `open()` must reject a non-admin session even when the normalizer
    /// itself never checks `ctx` — this is the whole point of it being a
    /// second, independent gate rather than trusting the normalizer alone.
    #[tokio::test]
    async fn open_fails_closed_when_a_buggy_normalizer_grants_host_boundary_to_a_tenant() {
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            Arc::new(MemoryRepository::default()),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(BuggyHostBoundaryEnvironment),
            Arc::new(FixedTurnId),
        );
        let error = application
            .open(&tenant_ctx(), open_request(Default::default()))
            .await
            .unwrap_err();
        assert!(matches!(error, SessionDomainError::InvalidRequest { .. }));
    }

    /// Same buggy normalizer, but an admin caller must still go through —
    /// the fail-closed gate only fires for non-admin `ctx`, admin keeps its
    /// "百无禁忌" bypass (`docs/tenancy_design.md` §0).
    #[tokio::test]
    async fn open_still_succeeds_for_admin_even_with_a_host_boundary_normalizer() {
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            Arc::new(MemoryRepository::default()),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(BuggyHostBoundaryEnvironment),
            Arc::new(FixedTurnId),
        );
        application
            .open(&admin_ctx(), open_request(Default::default()))
            .await
            .unwrap();
    }

    /// The second half of the §0 invariant — `workspace 来源 ≠ LocalPath` —
    /// fires independently of the isolation-boundary check above, even when
    /// the normalizer (here `TestEnvironment`, `Container` boundary) got the
    /// boundary right.
    #[tokio::test]
    async fn open_fails_closed_when_a_tenant_requests_a_local_path_workspace() {
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            Arc::new(MemoryRepository::default()),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(TestEnvironment),
            Arc::new(FixedTurnId),
        );
        let error = application
            .open(
                &tenant_ctx(),
                open_request(session_protocol::WorkspaceSpec::LocalPath {
                    path: "/etc".into(),
                }),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, SessionDomainError::InvalidRequest { .. }));
    }

    // --- enforce_workspace_axiom (§5.4 triple) -----------------------------

    #[test]
    fn axiom_admin_passes_regardless_of_workspace_or_provider() {
        let workspace = session_protocol::WorkspaceSpec::LocalPath {
            path: "/etc".into(),
        };
        assert!(enforce_workspace_axiom(&admin_ctx(), &workspace, false).is_ok());
        assert!(enforce_workspace_axiom(&admin_ctx(), &workspace, true).is_ok());
    }

    #[test]
    fn axiom_tenant_on_a_non_sandbox_provider_is_always_rejected() {
        let local = session_protocol::WorkspaceSpec::LocalPath {
            path: "/etc".into(),
        };
        let git = session_protocol::WorkspaceSpec::Git {
            url: "https://example.com/repo.git".into(),
            reference: None,
            subdirectory: None,
        };
        assert!(enforce_workspace_axiom(&tenant_ctx(), &local, false).is_err());
        assert!(enforce_workspace_axiom(&tenant_ctx(), &git, false).is_err());
        assert!(enforce_workspace_axiom(&tenant_ctx(), &Default::default(), false).is_err());
    }

    #[test]
    fn axiom_tenant_on_a_sandbox_provider_requires_git() {
        let local = session_protocol::WorkspaceSpec::LocalPath {
            path: "/etc".into(),
        };
        assert!(enforce_workspace_axiom(&tenant_ctx(), &local, true).is_err());
        assert!(enforce_workspace_axiom(&tenant_ctx(), &Default::default(), true).is_err());
    }

    #[test]
    fn axiom_tenant_git_over_https_with_no_embedded_credentials_is_allowed() {
        let git = session_protocol::WorkspaceSpec::Git {
            url: "https://example.com/org/repo.git".into(),
            reference: Some("main".into()),
            subdirectory: None,
        };
        assert!(enforce_workspace_axiom(&tenant_ctx(), &git, true).is_ok());
    }

    #[test]
    fn axiom_tenant_git_rejects_non_https_schemes() {
        for url in [
            "http://example.com/repo.git",
            "ssh://git@example.com/repo.git",
            "git://example.com/repo.git",
            "file:///etc/passwd",
        ] {
            let git = session_protocol::WorkspaceSpec::Git {
                url: url.into(),
                reference: None,
                subdirectory: None,
            };
            assert!(
                enforce_workspace_axiom(&tenant_ctx(), &git, true).is_err(),
                "expected {url} to be rejected"
            );
        }
    }

    #[test]
    fn axiom_tenant_git_rejects_embedded_credentials() {
        let git = session_protocol::WorkspaceSpec::Git {
            url: "https://user:secret@example.com/repo.git".into(),
            reference: None,
            subdirectory: None,
        };
        assert!(enforce_workspace_axiom(&tenant_ctx(), &git, true).is_err());
    }

    #[tokio::test]
    async fn capability_gate_rejects_before_runtime_submission() {
        let mut record = test_record();
        record.capabilities.runtime.clear();
        let repository = Arc::new(MemoryRepository(Mutex::new(Some(record))));
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            repository,
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        );
        let error = application
            .submit_turn(
                &admin_ctx(),
                SessionTurnRequest {
                    runtime_id: "runtime-1".into(),
                    text: "hello".into(),
                    entry: Default::default(),
                    llm: Some(Default::default()),
                    reasoning_effort: None,
                    client_request_id: None,
                    ext: Default::default(),
                    lease: Default::default(),
                },
            )
            .await
            .err()
            .expect("missing model override capability must fail");
        assert!(matches!(
            error,
            SessionDomainError::UnsupportedCapability { capability, .. }
                if capability == "model_override"
        ));
    }

    #[tokio::test]
    async fn turn_entry_is_projected_to_the_runtime_adapter() {
        let runtime = Arc::new(CompletingRuntime::default());
        let application = SessionApplication::new(
            runtime.clone(),
            Arc::new(EmptyRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        );

        application
            .submit_turn(
                &admin_ctx(),
                SessionTurnRequest {
                    runtime_id: "runtime-1".into(),
                    text: "hello".into(),
                    entry: session_protocol::SessionEntryContext {
                        entry_kind: Some("channel".into()),
                        instance_id: Some("feishu-tenant-1".into()),
                        message_id: Some("message-1".into()),
                        reply_to_message_id: Some("message-0".into()),
                    },
                    llm: None,
                    reasoning_effort: None,
                    client_request_id: None,
                    ext: Default::default(),
                    lease: Default::default(),
                },
            )
            .await
            .unwrap();

        let input = runtime.submitted.lock().await.clone().unwrap();
        assert_eq!(input.entry.kind.as_deref(), Some("channel"));
        assert_eq!(input.entry.instance_id.as_deref(), Some("feishu-tenant-1"));
        assert_eq!(input.entry.message_id.as_deref(), Some("message-1"));
        assert_eq!(
            input.entry.reply_to_message_id.as_deref(),
            Some("message-0")
        );
    }

    // -- Phase 2: lease-gated close/detach/heartbeat/cancel, and fork --------

    /// A `RuntimeAdapter` that overrides `export_state` (unlike
    /// `CompletingRuntime`), so `fork`'s happy path can be exercised without
    /// stubbing out the entire adapter surface.
    #[derive(Default)]
    struct ForkableRuntime {
        started: Mutex<Option<RuntimeStartRequest>>,
    }

    #[async_trait]
    impl RuntimeAdapter for ForkableRuntime {
        fn kind(&self) -> &str {
            "test"
        }

        fn capabilities(&self) -> BTreeSet<session_protocol::SessionRuntimeCapability> {
            BTreeSet::new()
        }

        async fn start(&self, request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
            *self.started.lock().await = Some(request);
            Ok(())
        }

        async fn stop(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn attach(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            Ok(())
        }
        async fn check_alive(&self, _runtime_id: &str) -> Result<bool, SessionDomainError> {
            Ok(true)
        }

        async fn submit_turn(
            &self,
            _input: RuntimeTurnInput,
        ) -> Result<crate::RuntimeEventReceiver, SessionDomainError> {
            unreachable!("the fork tests never submit a turn")
        }

        async fn answer_interaction(
            &self,
            _input: RuntimeInteractionInput,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn cancel(
            &self,
            _runtime_id: &str,
            _turn_id: Option<&str>,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn export_state(
            &self,
            _runtime_id: &str,
        ) -> Result<OpaqueRuntimeState, SessionDomainError> {
            Ok(OpaqueRuntimeState {
                runtime_kind: "forkable".into(),
                schema_version: 1,
                state: serde_json::json!({"turns": 3}),
            })
        }
    }

    #[tokio::test]
    async fn submit_turn_rejects_when_another_client_holds_the_lease() {
        let repository = Arc::new(MemoryRepository(Mutex::new(Some(test_record()))));
        let lease_table = Arc::new(SessionLeaseTable::new());
        lease_table
            .acquire("runtime-1", "client-a", None, None)
            .await;
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            repository,
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        )
        .with_lease_table(lease_table);

        let error = application
            .submit_turn(
                &admin_ctx(),
                SessionTurnRequest {
                    runtime_id: "runtime-1".into(),
                    text: "hello".into(),
                    entry: Default::default(),
                    llm: None,
                    reasoning_effort: None,
                    client_request_id: None,
                    ext: Default::default(),
                    lease: SessionLeaseClaim {
                        client_id: Some("client-b".into()),
                        ..Default::default()
                    },
                },
            )
            .await
            .err()
            .expect("a different client's turn submission must be rejected");
        assert!(matches!(
            error,
            SessionDomainError::LeaseConflict { holder_client_id, .. }
                if holder_client_id.as_deref() == Some("client-a")
        ));
    }

    #[tokio::test]
    async fn close_rejects_when_another_client_holds_the_lease() {
        let repository = Arc::new(MemoryRepository(Mutex::new(Some(test_record()))));
        let lease_table = Arc::new(SessionLeaseTable::new());
        lease_table
            .acquire("runtime-1", "client-a", None, None)
            .await;
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            repository,
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        )
        .with_lease_table(lease_table);

        let error = application
            .close(
                &admin_ctx(),
                "runtime-1",
                SessionLeaseClaim {
                    client_id: Some("client-b".into()),
                    ..Default::default()
                },
            )
            .await
            .err()
            .expect("a different client's close must be rejected");
        assert!(matches!(error, SessionDomainError::LeaseConflict { .. }));
    }

    #[tokio::test]
    async fn close_by_the_lease_holder_stops_the_runtime_and_tombstones_the_record() {
        let repository = Arc::new(MemoryRepository(Mutex::new(Some(test_record()))));
        let runtime = Arc::new(CompletingRuntime::default());
        let lease_table = Arc::new(SessionLeaseTable::new());
        lease_table
            .acquire("runtime-1", "client-a", None, None)
            .await;
        let application = SessionApplication::new(
            runtime.clone(),
            repository.clone(),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        )
        .with_lease_table(lease_table.clone());

        let response = application
            .close(
                &admin_ctx(),
                "runtime-1",
                SessionLeaseClaim {
                    client_id: Some("client-a".into()),
                    ..Default::default()
                },
            )
            .await
            .expect("the lease holder's close must succeed");
        assert_eq!(response.status, SessionLifecycleStatus::Closed);
        assert!(*runtime.stopped.lock().await, "close must stop the runtime");

        let stored = repository.0.lock().await.clone().unwrap();
        assert_eq!(
            stored.status,
            crate::SessionStatus::Closed,
            "close tombstones the record instead of deleting it"
        );
        assert!(
            lease_table.check_holder("runtime-1", None).await.is_ok(),
            "close must remove the lease so a later open starts clean"
        );
    }

    #[tokio::test]
    async fn close_by_a_new_client_succeeds_once_the_prior_holders_lease_goes_stale() {
        let repository = Arc::new(MemoryRepository(Mutex::new(Some(test_record()))));
        let lease_table = Arc::new(SessionLeaseTable::new());
        lease_table
            .acquire("runtime-1", "client-a", None, None)
            .await;
        lease_table
            .set_last_heartbeat_ms_for_test(
                "runtime-1",
                crate::session_lease::current_time_ms()
                    .expect("wall clock")
                    .saturating_sub(STALE_LEASE_THRESHOLD_MS + 1_000),
            )
            .await;
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            repository,
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        )
        .with_lease_table(lease_table);

        // `close` uses `check_holder` (read-only): a stale prior holder must
        // not block a new client, mirroring
        // `session_lease::check_holder_does_not_take_over_stale_lease`.
        let response = application
            .close(
                &admin_ctx(),
                "runtime-1",
                SessionLeaseClaim {
                    client_id: Some("client-b".into()),
                    ..Default::default()
                },
            )
            .await
            .expect("a stale prior holder must not block a new client's close");
        assert_eq!(response.status, SessionLifecycleStatus::Closed);
    }

    #[tokio::test]
    async fn detach_releases_only_the_callers_own_lease_and_leaves_the_runtime_running() {
        let repository = Arc::new(MemoryRepository(Mutex::new(Some(test_record()))));
        let runtime = Arc::new(CompletingRuntime::default());
        let lease_table = Arc::new(SessionLeaseTable::new());
        lease_table
            .acquire("runtime-1", "client-a", None, None)
            .await;
        let application = SessionApplication::new(
            runtime.clone(),
            repository,
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        )
        .with_lease_table(lease_table.clone());

        // A different client's detach is a no-op (matches
        // `SessionLeaseTable::detach`'s "only releases self lease" contract).
        application
            .detach(
                &admin_ctx(),
                "runtime-1",
                SessionLeaseClaim {
                    client_id: Some("client-b".into()),
                    ..Default::default()
                },
            )
            .await
            .expect("detach must succeed even when the caller does not hold the lease");
        assert!(
            lease_table
                .check_holder("runtime-1", Some("client-b"))
                .await
                .is_err(),
            "client-a must still hold the lease"
        );

        let response = application
            .detach(
                &admin_ctx(),
                "runtime-1",
                SessionLeaseClaim {
                    client_id: Some("client-a".into()),
                    ..Default::default()
                },
            )
            .await
            .expect("the holder's detach must succeed");
        assert_eq!(response.status, SessionLifecycleStatus::Idle);
        assert!(
            lease_table.check_holder("runtime-1", None).await.is_ok(),
            "lease must be released after detach"
        );
        assert!(
            !*runtime.stopped.lock().await,
            "detach must not stop the runtime"
        );
    }

    #[tokio::test]
    async fn heartbeat_without_lease_enforcement_is_a_no_op_accept() {
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            Arc::new(EmptyRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        );

        let response = application
            .heartbeat(&admin_ctx(), "runtime-1", SessionLeaseClaim::default())
            .await
            .expect("heartbeat must be accepted when lease enforcement is off");
        assert!(response.accepted);
        assert_eq!(response.lease_expires_at_ms, None);
    }

    #[tokio::test]
    async fn heartbeat_requires_a_client_id_when_lease_enforcement_is_on() {
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            Arc::new(EmptyRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        )
        .with_lease_table(Arc::new(SessionLeaseTable::new()));

        let error = application
            .heartbeat(&admin_ctx(), "runtime-1", SessionLeaseClaim::default())
            .await
            .err()
            .expect("an anonymous heartbeat must be rejected once lease enforcement is on");
        assert!(matches!(error, SessionDomainError::LeaseRequired { .. }));
    }

    #[tokio::test]
    async fn heartbeat_auto_reacquires_when_no_lease_is_currently_held() {
        // Mirrors `SessionLeaseTable::heartbeat`'s `None` arm: an empty table
        // (e.g. after a `detach`, or a daemon restart) lets the next
        // heartbeat re-acquire without a round trip through `open`.
        let lease_table = Arc::new(SessionLeaseTable::new());
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            Arc::new(EmptyRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        )
        .with_lease_table(lease_table.clone());

        let response = application
            .heartbeat(
                &admin_ctx(),
                "runtime-1",
                SessionLeaseClaim {
                    client_id: Some("client-a".into()),
                    ..Default::default()
                },
            )
            .await
            .expect("heartbeat on an unheld session must auto-acquire");
        assert!(response.accepted);
        assert!(response.lease_expires_at_ms.is_some());

        // client-a is now the recorded holder; a different client's
        // heartbeat must fail against it.
        let error = lease_table
            .heartbeat("runtime-1", "client-b", None, None)
            .await
            .err()
            .expect("a different client must not be able to heartbeat client-a's lease");
        assert!(matches!(
            error,
            LeaseCheckFailure::Busy { holder_client_id, .. } if holder_client_id == "client-a"
        ));
    }

    #[tokio::test]
    async fn cancel_delegates_to_the_runtime_without_a_lease_check() {
        let repository = Arc::new(MemoryRepository(Mutex::new(Some(test_record()))));
        let lease_table = Arc::new(SessionLeaseTable::new());
        lease_table
            .acquire("runtime-1", "client-a", None, None)
            .await;
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            repository,
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        )
        .with_lease_table(lease_table);

        // client-b does not hold the lease, but cancel is not lease-gated.
        application
            .cancel(&admin_ctx(), "runtime-1", Some("turn-1"))
            .await
            .expect("cancel must not require holding the write lease");
    }

    #[tokio::test]
    async fn fork_fails_with_unsupported_capability_when_the_adapter_has_not_implemented_export_state(
    ) {
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            Arc::new(EmptyRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        );

        let error = application
            .fork(
                &admin_ctx(),
                SessionForkRequest {
                    parent_runtime_id: "runtime-1".into(),
                    runtime_id: None,
                    conversation_id: None,
                    sender_id: None,
                    workspace: None,
                    deployment: None,
                    requested_capabilities: Default::default(),
                    lease: Default::default(),
                },
            )
            .await
            .err()
            .expect("fork must fail when the adapter has not implemented export_state");
        assert!(matches!(
            error,
            SessionDomainError::UnsupportedCapability { capability, .. }
                if capability == "state_export"
        ));
    }

    #[tokio::test]
    async fn fork_starts_a_new_runtime_from_the_parents_exported_state() {
        let repository = Arc::new(MemoryRepository(Mutex::new(Some(test_record()))));
        let runtime = Arc::new(ForkableRuntime::default());
        let application = SessionApplication::new(
            runtime.clone(),
            repository.clone(),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        );

        let response = application
            .fork(
                &admin_ctx(),
                SessionForkRequest {
                    parent_runtime_id: "runtime-1".into(),
                    runtime_id: None,
                    conversation_id: None,
                    sender_id: None,
                    workspace: None,
                    deployment: None,
                    requested_capabilities: Default::default(),
                    lease: Default::default(),
                },
            )
            .await
            .expect("fork must succeed against an adapter that implements export_state");

        assert_eq!(response.runtime_id, "runtime-fixed");
        assert_eq!(response.conversation_id, "conversation-1");

        let stored = repository.0.lock().await.clone().unwrap();
        assert_eq!(
            stored
                .lineage
                .as_ref()
                .and_then(|lineage| lineage.parent_runtime_id.as_deref()),
            Some("runtime-1"),
            "the forked record must carry lineage back to its parent"
        );
        assert_eq!(stored.runtime.state, serde_json::json!({"turns": 3}));

        let started = runtime.started.lock().await.clone().unwrap();
        assert_eq!(started.runtime_id, "runtime-fixed");
        assert_eq!(
            started.state.unwrap().state,
            serde_json::json!({"turns": 3})
        );
    }

    fn plain_turn_request(client_request_id: Option<&str>) -> SessionTurnRequest {
        SessionTurnRequest {
            runtime_id: "runtime-1".into(),
            text: "hello".into(),
            entry: Default::default(),
            llm: None,
            reasoning_effort: None,
            client_request_id: client_request_id.map(str::to_string),
            ext: Default::default(),
            lease: Default::default(),
        }
    }

    /// Retry a submission until the turn gate frees up (the forwarding task
    /// releases it asynchronously after the terminal event), failing the test
    /// if it stays occupied well past any plausible forwarding delay.
    async fn submit_when_gate_frees(
        application: &SessionApplication,
        request: SessionTurnRequest,
    ) -> SessionSubmission {
        for _ in 0..200 {
            match application.submit_turn(&admin_ctx(), request.clone()).await {
                Ok(submission) => return submission,
                Err(SessionDomainError::Conflict { .. }) => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(other) => panic!("unexpected submit error while waiting for gate: {other:?}"),
            }
        }
        panic!("turn gate was never released");
    }

    /// Runtime whose turns stay open until the test completes them through
    /// the stashed sender.
    #[derive(Default)]
    struct HoldingRuntime {
        turn_sender: Mutex<Option<mpsc::Sender<crate::RuntimeEvent>>>,
    }

    #[async_trait]
    impl RuntimeAdapter for HoldingRuntime {
        fn kind(&self) -> &str {
            "test"
        }

        fn capabilities(&self) -> BTreeSet<session_protocol::SessionRuntimeCapability> {
            BTreeSet::new()
        }

        async fn start(&self, _request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn stop(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn attach(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            Ok(())
        }
        async fn check_alive(&self, _runtime_id: &str) -> Result<bool, SessionDomainError> {
            Ok(true)
        }

        async fn submit_turn(
            &self,
            _input: RuntimeTurnInput,
        ) -> Result<crate::RuntimeEventReceiver, SessionDomainError> {
            let (tx, rx) = mpsc::channel(4);
            *self.turn_sender.lock().await = Some(tx);
            Ok(rx)
        }

        async fn answer_interaction(
            &self,
            _input: RuntimeInteractionInput,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn cancel(
            &self,
            _runtime_id: &str,
            _turn_id: Option<&str>,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn second_submit_while_a_turn_is_active_is_rejected_with_conflict() {
        let runtime = Arc::new(HoldingRuntime::default());
        let application = SessionApplication::new(
            runtime.clone(),
            Arc::new(EmptyRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        );

        let mut first = application
            .submit_turn(&admin_ctx(), plain_turn_request(None))
            .await
            .expect("first submission must be accepted");

        let error = application
            .submit_turn(&admin_ctx(), plain_turn_request(None))
            .await
            .err()
            .expect("second submission must be rejected while the first turn is active");
        assert!(
            matches!(error, SessionDomainError::Conflict { ref message }
                if message.contains(&first.receipt.turn_id)),
            "conflict must name the active turn: {error:?}"
        );

        // Complete the first turn and drain it to its terminal event.
        let sender = runtime.turn_sender.lock().await.take().unwrap();
        sender
            .send(crate::RuntimeEvent::Completed {
                outcome: session_protocol::SessionTurnOutcome::Complete,
                usage: SessionUsage::default(),
            })
            .await
            .unwrap();
        drop(sender);
        let mut events = first.events.take().expect("new turn has an event stream");
        while let Some(event) = events.recv().await {
            if matches!(event, session_protocol::SessionEvent::TurnCompleted { .. }) {
                break;
            }
        }

        // The gate frees once the forwarding task finishes.
        submit_when_gate_frees(&application, plain_turn_request(None)).await;
    }

    /// Immediately-completing runtime that counts how many turns the adapter
    /// actually received — the observable difference between a replayed
    /// receipt and an accidentally restarted turn.
    #[derive(Default)]
    struct CountingRuntime {
        submissions: std::sync::atomic::AtomicU32,
    }

    #[async_trait]
    impl RuntimeAdapter for CountingRuntime {
        fn kind(&self) -> &str {
            "test"
        }

        fn capabilities(&self) -> BTreeSet<session_protocol::SessionRuntimeCapability> {
            BTreeSet::new()
        }

        async fn start(&self, _request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn stop(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn attach(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            Ok(())
        }
        async fn check_alive(&self, _runtime_id: &str) -> Result<bool, SessionDomainError> {
            Ok(true)
        }

        async fn submit_turn(
            &self,
            _input: RuntimeTurnInput,
        ) -> Result<crate::RuntimeEventReceiver, SessionDomainError> {
            self.submissions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let (tx, rx) = mpsc::channel(2);
            tx.send(crate::RuntimeEvent::Completed {
                outcome: session_protocol::SessionTurnOutcome::Complete,
                usage: SessionUsage::default(),
            })
            .await
            .unwrap();
            Ok(rx)
        }

        async fn answer_interaction(
            &self,
            _input: RuntimeInteractionInput,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn cancel(
            &self,
            _runtime_id: &str,
            _turn_id: Option<&str>,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }
    }

    struct SequencedIds(std::sync::atomic::AtomicU32);

    impl TurnIdGenerator for SequencedIds {
        fn next_turn_id(&self) -> String {
            let n = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            format!("turn-{n}")
        }
    }

    #[tokio::test]
    async fn duplicate_client_request_id_replays_the_receipt_without_a_new_turn() {
        let runtime = Arc::new(CountingRuntime::default());
        let application = SessionApplication::new(
            runtime.clone(),
            Arc::new(EmptyRepository),
            Arc::new(SequencedIds(std::sync::atomic::AtomicU32::new(1))),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        );

        let mut first = application
            .submit_turn(&admin_ctx(), plain_turn_request(Some("req-1")))
            .await
            .expect("first submission must be accepted");
        assert_eq!(first.receipt.turn_id, "turn-1");
        let mut events = first.events.take().expect("new turn has an event stream");
        while events.recv().await.is_some() {}

        // Retry with the same key: same turn_id, no new stream, and — the
        // load-bearing assertion — the adapter never saw a second turn.
        let replay = submit_when_gate_frees(&application, plain_turn_request(Some("req-1"))).await;
        assert_eq!(replay.receipt.turn_id, "turn-1");
        assert!(
            replay.events.is_none(),
            "a replay must not open a new stream"
        );
        assert_eq!(
            runtime
                .submissions
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );

        // A different key is a genuinely new turn.
        let second = submit_when_gate_frees(&application, plain_turn_request(Some("req-2"))).await;
        assert_eq!(second.receipt.turn_id, "turn-2");
        assert!(second.events.is_some());
        assert_eq!(
            runtime
                .submissions
                .load(std::sync::atomic::Ordering::SeqCst),
            2
        );
    }

    /// Runtime whose event stream closes without ever producing a terminal
    /// event (a crashed runtime process, from the governor's point of view).
    struct VanishingRuntime;

    #[async_trait]
    impl RuntimeAdapter for VanishingRuntime {
        fn kind(&self) -> &str {
            "test"
        }

        fn capabilities(&self) -> BTreeSet<session_protocol::SessionRuntimeCapability> {
            BTreeSet::new()
        }

        async fn start(&self, _request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn stop(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn attach(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            Ok(())
        }
        async fn check_alive(&self, _runtime_id: &str) -> Result<bool, SessionDomainError> {
            Ok(true)
        }

        async fn submit_turn(
            &self,
            _input: RuntimeTurnInput,
        ) -> Result<crate::RuntimeEventReceiver, SessionDomainError> {
            let (tx, rx) = mpsc::channel(1);
            drop(tx);
            Ok(rx)
        }

        async fn answer_interaction(
            &self,
            _input: RuntimeInteractionInput,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn cancel(
            &self,
            _runtime_id: &str,
            _turn_id: Option<&str>,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn gate_is_released_after_a_stream_closes_without_a_terminal_event() {
        let application = SessionApplication::new(
            Arc::new(VanishingRuntime),
            Arc::new(EmptyRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        );

        let mut submission = application
            .submit_turn(&admin_ctx(), plain_turn_request(None))
            .await
            .expect("submission must be accepted");
        let mut events = submission
            .events
            .take()
            .expect("new turn has an event stream");
        let event = events.recv().await.expect("synthesized terminal event");
        let event = serde_json::to_value(event).unwrap();
        assert_eq!(event["kind"], "turn_failed");
        assert_eq!(event["error"]["code"], "event_stream_closed");

        // The synthesized failure must also release the single-turn slot.
        submit_when_gate_frees(&application, plain_turn_request(None)).await;
    }

    // -- Phase 3: quota admission (§7 step 4) --------------------------------

    fn quota_tenant_ctx(max_sessions: u32) -> SecurityContext {
        SecurityContext::tenant("tenant-a", "test").with_quota(TenantQuota {
            max_sessions: Some(max_sessions),
            ..Default::default()
        })
    }

    fn tenant_test_record() -> SessionRecord {
        SessionRecord {
            tenant_id: Some("tenant-a".to_string()),
            ..test_record()
        }
    }

    /// A `SessionRepository` keyed by `runtime_id`, unlike `MemoryRepository`
    /// (single-slot — overwritten on every `save`). Needed for the fork quota
    /// test, which must keep the parent record addressable while a distinct
    /// child record is saved alongside it.
    #[derive(Default)]
    struct MapRepository(Mutex<HashMap<String, SessionRecord>>);

    #[async_trait]
    impl SessionRepository for MapRepository {
        async fn get(&self, runtime_id: &str) -> Result<Option<SessionRecord>, SessionDomainError> {
            Ok(self.0.lock().await.get(runtime_id).cloned())
        }

        async fn save(&self, record: SessionRecord) -> Result<(), SessionDomainError> {
            self.0
                .lock()
                .await
                .insert(record.runtime_id.clone(), record);
            Ok(())
        }

        async fn list_active(
            &self,
            tenant_id: Option<&str>,
            limit: usize,
        ) -> Result<SessionListPage, SessionDomainError> {
            Ok(select_active(
                self.0.lock().await.values().cloned(),
                tenant_id,
                limit,
            ))
        }

        async fn active_session_counts_by_tenant(
            &self,
        ) -> Result<HashMap<String, usize>, SessionDomainError> {
            let records = self.0.lock().await;
            let mut counts = HashMap::new();
            for record in records.values().filter(|record| {
                record.tenant_id.is_some()
                    && matches!(
                        record.status,
                        SessionStatus::Opening
                            | SessionStatus::Idle
                            | SessionStatus::Running
                            | SessionStatus::Paused
                    )
            }) {
                *counts.entry(record.tenant_id.clone().unwrap()).or_insert(0) += 1;
            }
            Ok(counts)
        }
    }

    /// Fails `start` exactly once, then succeeds on every subsequent call —
    /// lets a test observe that a failed `open`'s quota reservation was
    /// rolled back, by immediately retrying and expecting success.
    #[derive(Default)]
    struct FirstStartFailsRuntime {
        already_failed: Mutex<bool>,
    }

    #[async_trait]
    impl RuntimeAdapter for FirstStartFailsRuntime {
        fn kind(&self) -> &str {
            "test"
        }

        fn capabilities(&self) -> BTreeSet<session_protocol::SessionRuntimeCapability> {
            BTreeSet::new()
        }

        async fn start(&self, _request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
            let mut already_failed = self.already_failed.lock().await;
            if !*already_failed {
                *already_failed = true;
                return Err(SessionDomainError::Internal {
                    message: "boom".into(),
                    source: None,
                });
            }
            Ok(())
        }

        async fn stop(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn attach(&self, _runtime_id: &str) -> Result<(), SessionDomainError> {
            Ok(())
        }
        async fn check_alive(&self, _runtime_id: &str) -> Result<bool, SessionDomainError> {
            Ok(true)
        }

        async fn submit_turn(
            &self,
            _input: RuntimeTurnInput,
        ) -> Result<crate::RuntimeEventReceiver, SessionDomainError> {
            unreachable!("this test never submits a turn")
        }

        async fn answer_interaction(
            &self,
            _input: RuntimeInteractionInput,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }

        async fn cancel(
            &self,
            _runtime_id: &str,
            _turn_id: Option<&str>,
        ) -> Result<(), SessionDomainError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn open_rejects_a_second_session_once_tenant_max_sessions_is_reached() {
        let repository = Arc::new(MemoryRepository::default());
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            repository,
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(TestEnvironment),
            Arc::new(FixedTurnId),
        );
        let ctx = quota_tenant_ctx(1);

        application
            .open(
                &ctx,
                SessionOpenRequest {
                    runtime_id: Some("r1".into()),
                    ..open_request(Default::default())
                },
            )
            .await
            .expect("first session is within quota");

        let error = application
            .open(
                &ctx,
                SessionOpenRequest {
                    runtime_id: Some("r2".into()),
                    ..open_request(Default::default())
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            SessionDomainError::QuotaExceeded { scope, limit }
                if scope == "sessions" && limit == 1
        ));
    }

    #[tokio::test]
    async fn restored_tenant_session_count_enforces_quota_after_restart() {
        let repository = Arc::new(MapRepository::default());
        repository
            .0
            .lock()
            .await
            .insert("existing".into(), tenant_test_record());
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            repository,
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(TestEnvironment),
            Arc::new(FixedTurnId),
        );
        application.restore_tenant_session_counts().await.unwrap();

        let error = application
            .open(
                &quota_tenant_ctx(1),
                SessionOpenRequest {
                    runtime_id: Some("new".into()),
                    ..open_request(Default::default())
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            SessionDomainError::QuotaExceeded { scope, limit }
                if scope == "sessions" && limit == 1
        ));
    }

    #[tokio::test]
    async fn unlimited_session_tracking_cannot_decrement_a_restored_sessions_slot() {
        let repository = Arc::new(MapRepository::default());
        repository
            .0
            .lock()
            .await
            .insert("runtime-1".into(), tenant_test_record());
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            repository,
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(TestEnvironment),
            Arc::new(FixedTurnId),
        );
        application.restore_tenant_session_counts().await.unwrap();
        application
            .open(
                &tenant_ctx(),
                SessionOpenRequest {
                    runtime_id: Some("unlimited".into()),
                    ..open_request(Default::default())
                },
            )
            .await
            .unwrap();
        application
            .close(&tenant_ctx(), "unlimited", Default::default())
            .await
            .unwrap();

        let error = application
            .open(
                &quota_tenant_ctx(1),
                SessionOpenRequest {
                    runtime_id: Some("limited".into()),
                    ..open_request(Default::default())
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            SessionDomainError::QuotaExceeded { limit: 1, .. }
        ));
    }

    fn test_checkpoint_record() -> crate::CheckpointRecord {
        let record = tenant_test_record();
        crate::CheckpointRecord {
            checkpoint_id: "checkpoint-1".into(),
            source_runtime_id: record.runtime_id,
            provider_snapshot_id: "snapshot-1".into(),
            runtime_state: record.runtime,
            workspace: record.workspace,
            isolation: record.isolation,
            capabilities: record.capabilities,
            owner_ref: "tenant/tenant-a".into(),
            tenant_id: Some("tenant-a".into()),
            created_by: Some("test".into()),
            created_at_ms: 1,
        }
    }

    #[tokio::test]
    async fn tenant_can_delete_own_checkpoint_and_metadata() {
        let repository = Arc::new(crate::SqliteSessionRepository::open_in_memory().unwrap());
        repository
            .save_checkpoint(test_checkpoint_record())
            .await
            .unwrap();
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            repository.clone(),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        );

        let result = application
            .delete_checkpoint(
                &tenant_ctx(),
                SessionCheckpointDeleteRequest {
                    checkpoint_id: "checkpoint-1".into(),
                    lease: Default::default(),
                },
            )
            .await
            .unwrap();
        assert!(result.deleted);
        assert_eq!(
            repository.get_checkpoint("checkpoint-1").await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn checkpoint_delete_failure_preserves_metadata_and_cross_tenant_is_hidden() {
        let repository = Arc::new(crate::SqliteSessionRepository::open_in_memory().unwrap());
        repository
            .save_checkpoint(test_checkpoint_record())
            .await
            .unwrap();
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime {
                checkpoint_delete_fails: true,
                ..Default::default()
            }),
            repository.clone(),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        );
        let request = || SessionCheckpointDeleteRequest {
            checkpoint_id: "checkpoint-1".into(),
            lease: Default::default(),
        };
        let other = SecurityContext::tenant("tenant-b", "other");
        assert!(matches!(
            application.delete_checkpoint(&other, request()).await,
            Err(SessionDomainError::NotFound { .. })
        ));
        assert!(matches!(
            application
                .delete_checkpoint(&tenant_ctx(), request())
                .await,
            Err(SessionDomainError::Unavailable { .. })
        ));
        assert!(repository
            .get_checkpoint("checkpoint-1")
            .await
            .unwrap()
            .is_some());
    }

    /// An admin `ctx` has no `tenant_id`, so `open` never even reads this
    /// quota — admin bypasses tenant-scoped admission entirely regardless of
    /// what happens to be attached (`docs/tenancy_design.md` §0: 百无禁忌).
    #[tokio::test]
    async fn open_admin_ignores_max_sessions_even_if_a_quota_is_attached() {
        let repository = Arc::new(MemoryRepository::default());
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            repository,
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(TestEnvironment),
            Arc::new(FixedTurnId),
        );
        let ctx = SecurityContext::admin("root").with_quota(TenantQuota {
            max_sessions: Some(0),
            ..Default::default()
        });

        application
            .open(&ctx, open_request(Default::default()))
            .await
            .expect("admin is never subject to a tenant session quota");
    }

    #[tokio::test]
    async fn close_releases_the_tenant_session_slot_so_a_new_open_can_reuse_it() {
        let repository = Arc::new(MemoryRepository::default());
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            repository,
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(TestEnvironment),
            Arc::new(FixedTurnId),
        );
        let ctx = quota_tenant_ctx(1);

        application
            .open(
                &ctx,
                SessionOpenRequest {
                    runtime_id: Some("r1".into()),
                    ..open_request(Default::default())
                },
            )
            .await
            .expect("first session is within quota");

        application
            .close(&ctx, "r1", SessionLeaseClaim::default())
            .await
            .expect("close must succeed");

        application
            .open(
                &ctx,
                SessionOpenRequest {
                    runtime_id: Some("r2".into()),
                    ..open_request(Default::default())
                },
            )
            .await
            .expect("the freed slot must admit a new session");
    }

    #[tokio::test]
    async fn open_rolls_back_the_reservation_when_runtime_start_fails() {
        let repository = Arc::new(MemoryRepository::default());
        let application = SessionApplication::new(
            Arc::new(FirstStartFailsRuntime::default()),
            repository,
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(TestEnvironment),
            Arc::new(FixedTurnId),
        );
        let ctx = quota_tenant_ctx(1);

        application
            .open(
                &ctx,
                SessionOpenRequest {
                    runtime_id: Some("r1".into()),
                    ..open_request(Default::default())
                },
            )
            .await
            .expect_err("runtime.start is rigged to fail on its first call");

        // If the failed attempt's reservation had leaked, this second,
        // independent open would incorrectly bounce off the quota even
        // though no session actually exists yet.
        application
            .open(
                &ctx,
                SessionOpenRequest {
                    runtime_id: Some("r2".into()),
                    ..open_request(Default::default())
                },
            )
            .await
            .expect("the rolled-back slot must be available to a fresh attempt");
    }

    #[tokio::test]
    async fn fork_counts_against_the_same_tenant_session_quota() {
        let repository = Arc::new(MapRepository::default());
        repository
            .0
            .lock()
            .await
            .insert("runtime-1".to_string(), tenant_test_record());
        let application = SessionApplication::new(
            Arc::new(ForkableRuntime::default()),
            repository,
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        );
        let ctx = quota_tenant_ctx(1);
        let fork_request = || SessionForkRequest {
            parent_runtime_id: "runtime-1".into(),
            runtime_id: None,
            conversation_id: None,
            sender_id: None,
            workspace: None,
            deployment: None,
            requested_capabilities: Default::default(),
            lease: Default::default(),
        };

        application
            .fork(&ctx, fork_request())
            .await
            .expect("first fork is within quota");

        let error = application.fork(&ctx, fork_request()).await.unwrap_err();
        assert!(matches!(
            error,
            SessionDomainError::QuotaExceeded { scope, limit }
                if scope == "sessions" && limit == 1
        ));
    }

    // ---- Phase 3b: audit logging (§6) -------------------------------
    //
    // No `tracing-subscriber` dependency exists in this workspace (only
    // `tracing` itself), so this hand-rolls a capture harness — but a naive
    // per-test `tracing::subscriber::set_default(...)` (thread-local guard)
    // is genuinely racy here and was tried first: `tracing-core`'s callsite
    // `Interest` cache and its companion global max-level hint are
    // process-wide state, recomputed by *whichever* thread happens to
    // construct a `Dispatch` at that moment (`Dispatch::new` registers +
    // triggers a rebuild internally). While only one dispatcher has *ever*
    // been registered process-wide, that rebuild resolves "the current
    // subscriber" via `dispatcher::get_default()` on the *registering*
    // thread — which, depending on scheduling, is not necessarily seeing
    // the dispatch being constructed yet. Two of these tests each doing
    // their own `set_default`/guard-drop dance were observed to
    // intermittently clobber the global max-level hint back down while
    // another, unrelated, concurrently-running test's events were in
    // flight — reproduced empirically (a loop of 5 runs of `cargo test -p
    // xgovernor-core application::` failed 3/5 times, a different test each
    // time, even after adding an explicit `rebuild_interest_cache()` call).
    //
    // The fix: install exactly one subscriber, exactly once, as the true
    // process-wide default (`set_global_default`, not the thread-local
    // `set_default`) behind a `OnceLock` — so there is only ever one
    // `Dispatch::new()` call in the whole test binary, and every thread's
    // `dispatcher::get_default()` resolves to it directly with no per-test
    // registration churn left to race on. Events are bucketed by the
    // capturing thread's `ThreadId` so concurrently-running tests (each on
    // its own OS thread — `cargo test` schedules one thread per test, and
    // `#[tokio::test]` defaults to a current-thread runtime that never
    // migrates a task mid-`.await`) don't see each other's events. Each
    // test *drains* (not just reads) its own bucket, so a setup call earlier
    // in the same test doesn't pollute the assertion.

    #[derive(Default)]
    struct FieldRecorder(HashMap<String, String>);

    impl tracing::field::Visit for FieldRecorder {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
    }

    #[derive(Clone, Default)]
    struct AuditCapture(Arc<std::sync::Mutex<HashMap<std::thread::ThreadId, Vec<String>>>>);

    impl AuditCapture {
        /// Remove and return every "audit"-target event captured so far on
        /// the calling thread, each rendered as a space-joined, key-sorted
        /// `"field=value"` line so assertions can use plain `contains(...)`
        /// checks instead of parsing. Draining (not cloning) means a call
        /// made earlier in the same test (e.g. a setup `open()` before the
        /// `close()` under test) doesn't leak into a later assertion.
        fn drain_events_for_current_thread(&self) -> Vec<String> {
            self.0
                .lock()
                .unwrap()
                .remove(&std::thread::current().id())
                .unwrap_or_default()
        }
    }

    impl tracing::Subscriber for AuditCapture {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target() == "audit"
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            if event.metadata().target() != "audit" {
                return;
            }
            let mut recorder = FieldRecorder::default();
            event.record(&mut recorder);
            let mut fields: Vec<_> = recorder.0.into_iter().collect();
            fields.sort();
            let line = fields
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(" ");
            self.0
                .lock()
                .unwrap()
                .entry(std::thread::current().id())
                .or_default()
                .push(line);
        }

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    /// The one-and-only `AuditCapture`, installed as the process's real
    /// global default tracing subscriber on first call. Safe to call from
    /// every test — `OnceLock` serializes the install to a single
    /// execution; later calls just hand back the same handle.
    fn audit_capture() -> AuditCapture {
        static CAPTURE: std::sync::OnceLock<AuditCapture> = std::sync::OnceLock::new();
        CAPTURE
            .get_or_init(|| {
                let capture = AuditCapture::default();
                // `set_global_default` succeeds at most once per process;
                // ignore `Err` from a second racing call (`OnceLock` already
                // guarantees only one of them constructs `capture`, but the
                // registration call itself races against no one once this
                // closure runs, so this is defensive, not load-bearing).
                let _ = tracing::subscriber::set_global_default(capture.clone());
                // Force one recompute against the dispatch we just made the
                // true global default, closing the narrow first-registration
                // race described above for this one-time install too.
                tracing::callsite::rebuild_interest_cache();
                capture
            })
            .clone()
    }

    #[tokio::test]
    async fn open_emits_an_audit_event_with_principal_tenant_and_runtime_id_on_success() {
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            Arc::new(MemoryRepository::default()),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(TestEnvironment),
            Arc::new(FixedTurnId),
        );
        let capture = audit_capture();
        capture.drain_events_for_current_thread();

        application
            .open(&tenant_ctx(), open_request(Default::default()))
            .await
            .expect("open must succeed");

        let events = capture.drain_events_for_current_thread();
        assert_eq!(
            events.len(),
            1,
            "exactly one audit event per call: {events:?}"
        );
        let event = &events[0];
        assert!(event.contains("operation=open"), "{event}");
        assert!(event.contains("principal=test"), "{event}");
        assert!(event.contains("tenant=tenant-a"), "{event}");
        assert!(event.contains("result=ok"), "{event}");
        assert!(
            event.contains("runtime_id=runtime-fixed"),
            "server-assigned runtime_id must be captured, not just the (absent) requested one: {event}"
        );
    }

    #[tokio::test]
    async fn open_emits_an_audit_event_with_the_error_on_a_rejected_request() {
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            Arc::new(MemoryRepository::default()),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(TestEnvironment),
            Arc::new(FixedTurnId),
        );
        let capture = audit_capture();
        capture.drain_events_for_current_thread();
        let ctx = quota_tenant_ctx(0);

        application
            .open(&ctx, open_request(Default::default()))
            .await
            .expect_err("zero quota must reject the open");

        let events = capture.drain_events_for_current_thread();
        assert_eq!(events.len(), 1, "{events:?}");
        let event = &events[0];
        assert!(event.contains("operation=open"), "{event}");
        assert!(event.contains("result=error"), "{event}");
        assert!(event.contains("error="), "{event}");
        assert!(
            event.contains("runtime_id=-"),
            "a rejected open never reaches the server-assigned id, and the \
             request itself specified none: {event}"
        );
    }

    #[tokio::test]
    async fn close_emits_an_audit_event_using_the_caller_supplied_runtime_id() {
        let application = SessionApplication::new(
            Arc::new(CompletingRuntime::default()),
            Arc::new(MemoryRepository::default()),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(TestEnvironment),
            Arc::new(FixedTurnId),
        );
        let capture = audit_capture();

        application
            .open(&admin_ctx(), open_request(Default::default()))
            .await
            .expect("open must succeed");
        // Discard the `open`'s own audit event — only `close`'s is under test.
        capture.drain_events_for_current_thread();

        application
            .close(&admin_ctx(), "runtime-fixed", SessionLeaseClaim::default())
            .await
            .expect("close must succeed");

        let events = capture.drain_events_for_current_thread();
        assert_eq!(events.len(), 1, "{events:?}");
        let event = &events[0];
        assert!(event.contains("operation=close"), "{event}");
        assert!(event.contains("runtime_id=runtime-fixed"), "{event}");
        assert!(event.contains("result=ok"), "{event}");
    }
}
