//! Assembly crate: wires a [`backend::local::LocalProvider`] (real
//! sandbox/operation plumbing) behind the `xgovernor_core::RuntimeAdapter`
//! seam.
//!
//! This is deliberately *not* an agent execution engine: xGovernor's product
//! scope is managing other agent runtimes via their own SDKs/APIs, not
//! reimplementing an LLM decision loop. What `submit_turn` decides to do is
//! therefore hardcoded/mocked here (a single fixed `exec` call), but how it
//! is carried out is fully real: a real `LocalProvider::create`, a real
//! `OperationAttach::attach`, and a real `OperationExec::exec` against the
//! attached backend. The point of this crate is to prove that plumbing, not
//! to stand in for a runtime's intelligence.
//!
//! `backend_id` is not modeled as a first-class field on
//! `RuntimeStartRequest` (that struct is runtime-neutral by design — see
//! `core/src/runtime_adapter.rs`). This adapter reads it out of the
//! namespaced `ext` bag instead, under the [`EXT_NAMESPACE`] key, matching
//! the "runtime-specific input belongs in namespaced ext bags" convention
//! documented on `session_protocol::SessionExtensions`. `owner_ref` used to
//! live in that same `ext` bag but has been promoted to a first-class
//! `RuntimeStartRequest` field, mechanically derived by the application layer
//! from the authenticated `SecurityContext` (`docs/tenancy_design.md` §7 step
//! 4) — this adapter now reads it from `request.owner_ref` directly and no
//! longer accepts it via `ext` at all (no dual-path/fallback).

use async_trait::async_trait;
use backend::local::LocalProvider;
use backend::{OperationAttach, ProviderInstanceLedger, SqliteProviderInstanceLedger};
use operation_protocol::capability::exec::ExecRequest;
use operation_protocol::OperationBackend;
use provider_protocol::{BackendId, ProviderControlError, ProviderKind, ProviderLifecycle};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use xgovernor_core::{
    CancelSignal, CapabilityFamily, RuntimeAdapter, RuntimeEvent, RuntimeEventReceiver,
    RuntimeInteractionInput, RuntimeStartRequest, RuntimeTurnInput, SessionDomainError,
    TurnCancellationRegistry,
};
use xgovernor_manager::{InstanceManager, InstanceManagerConfig};

/// Namespaced `ext` key this adapter reads `backend_id` from, since
/// `RuntimeStartRequest` intentionally has no dedicated field for it (a
/// runtime-adapter placement decision, not a cross-cutting session concept —
/// unlike `owner_ref`, which is a first-class `RuntimeStartRequest` field).
pub const EXT_NAMESPACE: &str = "runtime_local";

/// Per-owner concurrent sandbox cap enforced by the wrapped
/// [`InstanceManager`]. A minimal fixed default is enough for the first
/// mocked-runtime validation; making this configurable is future work once a
/// real caller needs it.
const DEFAULT_MAX_SANDBOXES_PER_OWNER: usize = 20;

/// Global (cross-owner) concurrent sandbox ceiling enforced by the wrapped
/// [`InstanceManager`] — see that type's module doc for why this exists
/// alongside the per-owner cap above. Sized loosely against
/// `docs/tenancy_design.md`'s stated scale assumption ("数十个租户，1000–2000
/// 并发 agent"); like `DEFAULT_MAX_SANDBOXES_PER_OWNER`, making this
/// per-deployment-configurable is future work.
const DEFAULT_MAX_SANDBOXES_GLOBAL: usize = 1024;

#[derive(Debug, Clone, Deserialize)]
struct LocalRuntimeExt {
    backend_id: String,
}

fn read_local_runtime_ext(
    ext: &session_protocol::SessionExtensions,
) -> Result<LocalRuntimeExt, SessionDomainError> {
    let value = ext
        .get(EXT_NAMESPACE)
        .ok_or_else(|| SessionDomainError::InvalidRequest {
            message: format!("missing required '{EXT_NAMESPACE}' ext payload (backend_id)"),
        })?;
    serde_json::from_value(value.clone()).map_err(|error| SessionDomainError::InvalidRequest {
        message: format!("invalid '{EXT_NAMESPACE}' ext payload: {error}"),
    })
}

fn map_provider_error(error: ProviderControlError) -> SessionDomainError {
    match error {
        ProviderControlError::NotFound { resource_ref } => SessionDomainError::NotFound {
            runtime_id: resource_ref,
        },
        ProviderControlError::InvalidRequest { message } => {
            SessionDomainError::InvalidRequest { message }
        }
        ProviderControlError::ResourceLimitExceeded { .. } => SessionDomainError::Unavailable {
            message: error.to_string(),
        },
        other => SessionDomainError::Internal {
            message: other.to_string(),
            source: None,
        },
    }
}

/// Mock `RuntimeAdapter` backed by a real [`LocalProvider`] wrapped in a
/// shared [`InstanceManager`] (`crates/manager`), instead of hand-rolling the
/// create/attach/registry/quota dance here. See that crate's module doc for
/// everything it owns on this adapter's behalf: the create/attach/registry
/// plumbing, per-owner + global sandbox quota, a per-`runtime_id` idempotency
/// lock, attach-failure compensating delete, and a pending-release retry
/// queue for delete failures. `LocalProvider`/`E2bProvider` both need the
/// same plumbing, so `apps/runtime-e2b`'s `E2bMockRuntime` composes the same
/// `InstanceManager` type rather than re-copying this file.
pub struct LocalMockRuntime {
    manager: Arc<InstanceManager>,
    /// Backs the `submit_turn`/`cancel` contract (see their doc comments on
    /// `RuntimeAdapter`): `submit_turn` spawns a background task to drive the
    /// turn and registers it here so a later `cancel` call can actually
    /// reach and interrupt it, instead of `cancel` being a no-op because
    /// `submit_turn` already ran the whole turn to completion synchronously
    /// before returning (the defect this field's introduction fixes).
    cancellation: Arc<TurnCancellationRegistry>,
}

impl LocalMockRuntime {
    /// In-memory provider-instance ledger (lost on restart). Fine for tests
    /// and quick manual runs; a real deployment should use
    /// [`Self::with_ledger_path`] instead — see that constructor's doc.
    pub fn new() -> Self {
        Self::with_max_sandboxes_per_owner(DEFAULT_MAX_SANDBOXES_PER_OWNER)
    }

    /// Same in-memory-ledger caveat as [`Self::new`], with a non-default
    /// per-owner sandbox cap.
    pub fn with_max_sandboxes_per_owner(max_per_owner: usize) -> Self {
        let ledger: Arc<dyn ProviderInstanceLedger> = Arc::new(
            SqliteProviderInstanceLedger::open_in_memory()
                .expect("open in-memory provider instance ledger"),
        );
        Self::with_ledger(max_per_owner, ledger)
    }

    /// Durable variant: provider-instance creation/deletion is recorded to a
    /// SQLite file at `ledger_path` (parent directories created if absent)
    /// via [`SqliteProviderInstanceLedger::open`], instead of the in-memory
    /// ledger `new()`/`with_max_sandboxes_per_owner()` use. This is what any
    /// deployment meant to survive a process restart should call — see
    /// `apps/server/src/main.rs`, which points this at a file under
    /// `~/.xgovernor` alongside the session repository's own SQLite file
    /// (both connections may safely target the same physical file in WAL
    /// mode; see `crates/backend/src/sqlite_ledger.rs`'s module doc).
    pub fn with_ledger_path(ledger_path: &Path) -> rusqlite::Result<Self> {
        Self::with_ledger_path_and_max_sandboxes_per_owner(
            ledger_path,
            DEFAULT_MAX_SANDBOXES_PER_OWNER,
        )
    }

    /// [`Self::with_ledger_path`] with a non-default per-owner sandbox cap.
    pub fn with_ledger_path_and_max_sandboxes_per_owner(
        ledger_path: &Path,
        max_per_owner: usize,
    ) -> rusqlite::Result<Self> {
        let ledger: Arc<dyn ProviderInstanceLedger> =
            Arc::new(SqliteProviderInstanceLedger::open(ledger_path)?);
        Ok(Self::with_ledger(max_per_owner, ledger))
    }

    fn with_ledger(max_per_owner: usize, ledger: Arc<dyn ProviderInstanceLedger>) -> Self {
        let local = Arc::new(LocalProvider::new());
        let lifecycle: Arc<dyn ProviderLifecycle> = local.clone();
        let attach: Arc<dyn OperationAttach> = local;
        let manager = Arc::new(InstanceManager::new(
            lifecycle,
            attach,
            ledger,
            ProviderKind("local".to_string()),
            InstanceManagerConfig::new(max_per_owner, DEFAULT_MAX_SANDBOXES_GLOBAL),
        ));
        Self {
            manager,
            cancellation: Arc::new(TurnCancellationRegistry::new()),
        }
    }

    /// Restore this runtime's operational state from durable storage before
    /// it starts serving real traffic: cross-check the ledger against what
    /// the provider itself currently reports (`InstanceManager::reconcile`,
    /// which repopulates the in-memory `runtime_id` registry *and* rehydrates
    /// owner/global quota tracking in one call). Without this, a restart
    /// silently reset quota counts to zero even though the ledger already
    /// knew better, allowing oversubscription past `max_per_owner` — see
    /// `docs/session_orchestration_skeleton.md` §6.
    ///
    /// Meant to be called once, right after construction. Safe to call on a
    /// brand-new ledger (nothing to reconcile, nothing to rehydrate).
    pub async fn reconcile_on_startup(
        &self,
    ) -> Result<xgovernor_manager::ReconcileOutcome, ProviderControlError> {
        self.manager.reconcile().await
    }

    /// Spawns the detached background task that automatically retries
    /// provider-instance deletes that failed on the direct `stop`/
    /// `stop_instance` path (see `xgovernor_manager::InstanceManager::
    /// spawn_retry_loop`'s doc for the exact retry/backoff behavior). Meant
    /// to be called once, alongside [`Self::reconcile_on_startup`] — see
    /// `apps/server/src/main.rs`. Dropping the returned `JoinHandle` does not
    /// stop the loop.
    pub fn spawn_pending_release_retry_loop(&self) -> tokio::task::JoinHandle<()> {
        Arc::clone(&self.manager).spawn_retry_loop()
    }
}

impl Default for LocalMockRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RuntimeAdapter for LocalMockRuntime {
    fn kind(&self) -> &str {
        "local-mock"
    }

    fn capabilities(&self) -> BTreeSet<session_protocol::SessionRuntimeCapability> {
        BTreeSet::new()
    }

    async fn start(&self, request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
        let ext = read_local_runtime_ext(&request.ext)?;

        self.manager
            .start_instance(
                request.runtime_id,
                BackendId(ext.backend_id),
                request.owner_ref,
                json!({ "workspace_root": request.workspace.root }),
            )
            .await
            .map_err(map_provider_error)?;
        Ok(())
    }

    async fn stop(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
        self.manager
            .stop_instance(runtime_id)
            .await
            .map_err(map_provider_error)
    }

    async fn attach(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
        self.manager
            .backend_for(runtime_id)
            .map(|_| ())
            .map_err(map_provider_error)
    }

    async fn submit_turn(
        &self,
        input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, SessionDomainError> {
        let backend = self
            .manager
            .backend_for(&input.runtime_id)
            .map_err(map_provider_error)?;
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        // Contract (`RuntimeAdapter::submit_turn`'s doc comment): register the
        // cancel signal and spawn the background task *before* returning —
        // this call must hand back `rx` without waiting for the turn to run,
        // so a `cancel` that arrives the instant this returns can never miss
        // the registration and the caller never blocks on the turn's actual
        // duration.
        let cancel_signal = self.cancellation.begin(&input.runtime_id, &input.turn_id);
        let cancellation = Arc::clone(&self.cancellation);
        tokio::spawn(run_mock_turn(
            backend,
            cancellation,
            input.runtime_id,
            input.turn_id,
            input.text,
            tx,
            cancel_signal,
        ));

        Ok(rx)
    }

    async fn answer_interaction(
        &self,
        _input: RuntimeInteractionInput,
    ) -> Result<(), SessionDomainError> {
        Err(SessionDomainError::UnsupportedCapability {
            family: CapabilityFamily::Runtime,
            capability: "interaction".to_string(),
        })
    }

    async fn cancel(
        &self,
        runtime_id: &str,
        turn_id: Option<&str>,
    ) -> Result<(), SessionDomainError> {
        // Fires the cancel signal `run_mock_turn`'s `tokio::select!` is
        // racing the exec future against. A stale/mismatched `turn_id` is a
        // silent no-op (see `TurnCancellationRegistry::fire`'s doc) rather
        // than an error — matches this method's existing "cancel whatever's
        // active, if anything" contract.
        self.cancellation.fire(runtime_id, turn_id);
        Ok(())
    }
}

/// The background task `submit_turn` spawns to actually drive a turn — this
/// is where the "mocked decision loop, real execution" split lives: what to
/// run is hardcoded (a single `echo`, no LLM decision loop; that is the
/// "mock" part), but how it runs is real (the attached backend's actual
/// `OperationExec`), and now it races that real execution against
/// cancellation instead of being the only thing this task can do.
async fn run_mock_turn(
    backend: Arc<dyn OperationBackend>,
    cancellation: Arc<TurnCancellationRegistry>,
    runtime_id: String,
    turn_id: String,
    text: String,
    tx: tokio::sync::mpsc::Sender<RuntimeEvent>,
    cancel_signal: CancelSignal,
) {
    let activity_id = format!("{turn_id}-exec");
    if tx
        .send(RuntimeEvent::ToolActivity {
            activity_id: activity_id.clone(),
            phase: session_protocol::SessionToolActivityPhase::Begin,
            name: "exec".to_string(),
            status: session_protocol::SessionToolActivityStatus::Running,
            summary: Some(text.clone()),
            ext: Default::default(),
        })
        .await
        .is_err()
    {
        // Nobody is listening (subscriber already gone) — nothing left to
        // drive; just release our slot so a same-runtime_id resubmission
        // isn't blocked by a stale entry.
        cancellation.end(&runtime_id, &turn_id);
        return;
    }

    let exec_future = backend.exec().exec(ExecRequest {
        command: "echo".to_string(),
        args: vec![text],
        shell: None,
        cwd: None,
        timeout_ms: Some(5_000),
        env: None,
    });
    tokio::pin!(exec_future);
    tokio::pin!(cancel_signal);

    enum Outcome {
        Exec(
            Result<
                operation_protocol::capability::exec::ExecResult,
                operation_protocol::OperationError,
            >,
        ),
        Cancelled,
    }
    let outcome = tokio::select! {
        result = &mut exec_future => Outcome::Exec(result),
        _ = &mut cancel_signal => Outcome::Cancelled,
    };

    match outcome {
        Outcome::Exec(Ok(result)) => {
            let stdout = String::from_utf8_lossy(&result.stdout).into_owned();
            tx.send(RuntimeEvent::ToolActivity {
                activity_id,
                phase: session_protocol::SessionToolActivityPhase::End,
                name: "exec".to_string(),
                status: session_protocol::SessionToolActivityStatus::Succeeded,
                summary: Some(stdout),
                ext: Default::default(),
            })
            .await
            .ok();
            tx.send(RuntimeEvent::Completed {
                outcome: session_protocol::SessionTurnOutcome::Complete,
                usage: session_protocol::SessionUsage::default(),
            })
            .await
            .ok();
        }
        Outcome::Exec(Err(error)) => {
            tx.send(RuntimeEvent::ToolActivity {
                activity_id,
                phase: session_protocol::SessionToolActivityPhase::End,
                name: "exec".to_string(),
                status: session_protocol::SessionToolActivityStatus::Failed,
                summary: Some(error.to_string()),
                ext: Default::default(),
            })
            .await
            .ok();
            tx.send(RuntimeEvent::Failed {
                error: xgovernor_core::RuntimeFailure {
                    code: "exec_failed".to_string(),
                    message: error.to_string(),
                    retryable: false,
                    details: Value::Null,
                },
                usage: session_protocol::SessionUsage::default(),
            })
            .await
            .ok();
        }
        Outcome::Cancelled => {
            // Best-effort interrupt: `exec_future` is simply dropped here (it
            // is never polled again once `select!` picks the other branch).
            // Whether the sandbox's underlying process actually dies depends
            // on the attached backend's `OperationExec` implementation
            // reacting to the in-flight request being dropped. A real
            // (non-mock) adapter driving a genuinely long-running agent loop
            // needs a stronger guarantee than "we stopped awaiting it" —
            // that is future work for whichever adapter first needs it, not
            // something this mock claims to solve.
            tx.send(RuntimeEvent::ToolActivity {
                activity_id,
                phase: session_protocol::SessionToolActivityPhase::End,
                name: "exec".to_string(),
                status: session_protocol::SessionToolActivityStatus::Cancelled,
                summary: Some("cancelled".to_string()),
                ext: Default::default(),
            })
            .await
            .ok();
            tx.send(RuntimeEvent::Completed {
                outcome: session_protocol::SessionTurnOutcome::Cancelled,
                usage: session_protocol::SessionUsage::default(),
            })
            .await
            .ok();
        }
    }

    cancellation.end(&runtime_id, &turn_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use session_protocol::SessionExtensions;
    use session_protocol::{SessionOpenRequest, SessionTurnRequest};
    use std::sync::Arc as StdArc;
    use tempfile::TempDir;
    use xgovernor_core::{
        Clock, IsolationBoundary, IsolationFacts, NetworkIsolation, NormalizedSessionEnvironment,
        RuntimeIdGenerator, SecurityContext, SessionApplication, SessionDomainError as DomainError,
        SessionEnvironmentNormalizer, SessionRecord, SessionRepository, TurnIdGenerator,
        WorkspaceAccess, WorkspaceFacts,
    };

    fn local_runtime_ext() -> SessionExtensions {
        [(EXT_NAMESPACE.to_string(), json!({ "backend_id": "local" }))]
            .into_iter()
            .collect()
    }

    /// Real in-memory persistence (unlike a no-op stub) so that
    /// `submit_turn`'s `require_session` lookup after `open()` actually
    /// finds the record `open()` saved — this validation is meant to
    /// exercise the full application flow, not just the adapter in
    /// isolation.
    #[derive(Default)]
    struct MemoryRepository(std::sync::Mutex<Option<SessionRecord>>);

    #[async_trait]
    impl SessionRepository for MemoryRepository {
        async fn get(&self, runtime_id: &str) -> Result<Option<SessionRecord>, DomainError> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .clone()
                .filter(|record| record.runtime_id == runtime_id))
        }
        async fn save(&self, record: SessionRecord) -> Result<(), DomainError> {
            *self.0.lock().unwrap() = Some(record);
            Ok(())
        }
    }

    struct FixedIds;
    impl TurnIdGenerator for FixedIds {
        fn next_turn_id(&self) -> String {
            "turn-1".into()
        }
    }
    impl RuntimeIdGenerator for FixedIds {
        fn next_runtime_id(&self) -> String {
            "runtime-1".into()
        }
    }
    impl Clock for FixedIds {
        fn now_ms(&self) -> u64 {
            1
        }
    }

    struct TempWorkspaceEnvironment {
        root: String,
    }

    #[async_trait]
    impl SessionEnvironmentNormalizer for TempWorkspaceEnvironment {
        async fn normalize(
            &self,
            _ctx: &SecurityContext,
            _request: &SessionOpenRequest,
        ) -> Result<NormalizedSessionEnvironment, DomainError> {
            Ok(NormalizedSessionEnvironment {
                workspace: WorkspaceFacts {
                    workspace_id: "workspace-1".into(),
                    root: self.root.clone(),
                    access: WorkspaceAccess::ReadWrite,
                    revision: None,
                    metadata: Value::Null,
                },
                isolation: IsolationFacts {
                    boundary: IsolationBoundary::Host,
                    workspace_access: WorkspaceAccess::ReadWrite,
                    network: NetworkIsolation::None,
                    metadata: Value::Null,
                },
                sandbox_capabilities: Default::default(),
                llm: None,
                lease: None,
            })
        }
    }

    #[tokio::test]
    async fn open_and_submit_turn_drive_a_real_local_sandbox_exec() {
        let workspace = TempDir::new().expect("tempdir");
        let root = workspace.path().to_str().unwrap().to_string();

        let application = SessionApplication::new(
            StdArc::new(LocalMockRuntime::new()),
            StdArc::new(MemoryRepository::default()),
            StdArc::new(FixedIds),
            StdArc::new(FixedIds),
            StdArc::new(TempWorkspaceEnvironment { root }),
            StdArc::new(FixedIds),
        );

        let ctx = SecurityContext::admin("test");

        let opened = application
            .open(
                &ctx,
                SessionOpenRequest {
                    runtime_id: None,
                    conversation_id: "conversation-1".into(),
                    sender_id: "sender-1".into(),
                    workspace: Default::default(),
                    deployment: Default::default(),
                    requested_capabilities: Default::default(),
                    llm: None,
                    ext: local_runtime_ext(),
                    lease: Default::default(),
                },
            )
            .await
            .expect("open must succeed against a real local sandbox");
        assert_eq!(opened.runtime_id, "runtime-1");

        let submission = application
            .submit_turn(
                &ctx,
                SessionTurnRequest {
                    runtime_id: "runtime-1".into(),
                    text: "hello-from-validation-test".into(),
                    entry: Default::default(),
                    llm: None,
                    reasoning_effort: None,
                    client_request_id: None,
                    ext: Default::default(),
                    lease: Default::default(),
                },
            )
            .await
            .expect("submit_turn must be accepted");

        let mut events = submission.events.expect("new turn carries an event stream");
        let mut saw_tool_activity_with_real_output = false;
        let mut saw_completed = false;
        while let Some(event) = events.recv().await {
            match event {
                session_protocol::SessionEvent::ToolActivity { summary, phase, .. } => {
                    if phase == session_protocol::SessionToolActivityPhase::End {
                        assert_eq!(summary.as_deref(), Some("hello-from-validation-test\n"));
                        saw_tool_activity_with_real_output = true;
                    }
                }
                session_protocol::SessionEvent::TurnCompleted { .. } => {
                    saw_completed = true;
                    break;
                }
                session_protocol::SessionEvent::TurnFailed { error, .. } => {
                    panic!("turn unexpectedly failed: {error:?}");
                }
                _ => {}
            }
        }
        assert!(
            saw_tool_activity_with_real_output,
            "expected a real exec output to flow through ToolActivity"
        );
        assert!(saw_completed, "expected a terminal turn_completed event");
    }

    /// Sets up a started runtime the direct-adapter contract tests can call
    /// `submit_turn`/`cancel` on without going through `SessionApplication`
    /// (unlike `open_and_submit_turn_drive_a_real_local_sandbox_exec`, these
    /// tests need to control the exact moment `submit_turn` returns relative
    /// to when its background task has had a chance to run, which requires
    /// calling the adapter directly).
    async fn started_runtime(workspace_root: &str) -> LocalMockRuntime {
        let runtime = LocalMockRuntime::new();
        runtime
            .start(RuntimeStartRequest {
                runtime_id: "runtime-1".into(),
                conversation_id: "conversation-1".into(),
                sender_id: "sender-1".into(),
                workspace: WorkspaceFacts {
                    workspace_id: "workspace-1".into(),
                    root: workspace_root.into(),
                    access: WorkspaceAccess::ReadWrite,
                    revision: None,
                    metadata: Value::Null,
                },
                state: None,
                llm: None,
                owner_ref: "admin".into(),
                ext: local_runtime_ext(),
            })
            .await
            .expect("start must succeed");
        runtime
    }

    /// Pins down the `RuntimeAdapter::submit_turn` contract documented on the
    /// trait itself: the receiver must come back *before* the turn has
    /// produced anything, because the turn runs in a background task the
    /// call spawns rather than inline. This is deterministic, not a timing
    /// race: `#[tokio::test]` defaults to a current-thread runtime, so the
    /// spawned task cannot run at all until this test hits a genuine
    /// yielding `.await` — which it never does between `submit_turn`
    /// returning and `try_recv()`. The old synchronous implementation would
    /// already have written events into the channel by this point and this
    /// assertion would fail.
    #[tokio::test]
    async fn submit_turn_returns_before_the_turn_has_produced_any_events() {
        let workspace = TempDir::new().expect("tempdir");
        let runtime = started_runtime(workspace.path().to_str().unwrap()).await;

        let mut events = runtime
            .submit_turn(RuntimeTurnInput {
                runtime_id: "runtime-1".into(),
                turn_id: "turn-1".into(),
                text: "hello".into(),
                entry: xgovernor_core::RuntimeEntryContext {
                    kind: None,
                    instance_id: None,
                    message_id: None,
                    reply_to_message_id: None,
                },
                llm: None,
                reasoning_effort: None,
                ext: Default::default(),
            })
            .await
            .expect("submit_turn must be accepted");

        assert!(
            matches!(
                events.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "submit_turn must return before its background task has run, \
             not after the turn already completed synchronously"
        );
    }

    /// Pins down `RuntimeAdapter::cancel`'s "real interrupt semantics"
    /// contract: calling `cancel` must make an in-flight turn reach
    /// `Completed { outcome: Cancelled }`, not let it run to its original
    /// completion unaffected. Also deterministic rather than a timing race:
    /// `cancel`'s body (a mutex lock + oneshot send) never yields, so by the
    /// time this test's first genuinely yielding `.await` (`events.recv()`)
    /// lets the background task run for the first time, the cancel signal
    /// has already fired — `tokio::select!` inside the task sees the cancel
    /// branch ready and the exec branch not yet ready (real `exec` requires
    /// actual I/O, never `Ready` on first poll), so it deterministically
    /// picks cancellation, not a 50/50 race.
    #[tokio::test]
    async fn cancel_called_immediately_after_submit_turn_reaches_a_cancelled_terminal_state() {
        let workspace = TempDir::new().expect("tempdir");
        let runtime = started_runtime(workspace.path().to_str().unwrap()).await;

        let mut events = runtime
            .submit_turn(RuntimeTurnInput {
                runtime_id: "runtime-1".into(),
                turn_id: "turn-1".into(),
                text: "hello".into(),
                entry: xgovernor_core::RuntimeEntryContext {
                    kind: None,
                    instance_id: None,
                    message_id: None,
                    reply_to_message_id: None,
                },
                llm: None,
                reasoning_effort: None,
                ext: Default::default(),
            })
            .await
            .expect("submit_turn must be accepted");

        runtime
            .cancel("runtime-1", Some("turn-1"))
            .await
            .expect("cancel must be accepted");

        let mut saw_cancelled_tool_activity = false;
        let mut saw_cancelled_completion = false;
        while let Some(event) = events.recv().await {
            match event {
                RuntimeEvent::ToolActivity { status, phase, .. } => {
                    if phase == session_protocol::SessionToolActivityPhase::End {
                        assert_eq!(
                            status,
                            session_protocol::SessionToolActivityStatus::Cancelled,
                            "a cancelled turn's tool activity must end as Cancelled, not \
                             Succeeded/Failed"
                        );
                        saw_cancelled_tool_activity = true;
                    }
                }
                RuntimeEvent::Completed { outcome, .. } => {
                    assert_eq!(
                        outcome,
                        session_protocol::SessionTurnOutcome::Cancelled,
                        "a cancelled turn must complete with outcome Cancelled"
                    );
                    saw_cancelled_completion = true;
                    break;
                }
                RuntimeEvent::Failed { error, .. } => {
                    panic!("a cancelled turn must not surface as Failed: {error:?}");
                }
                _ => {}
            }
        }
        assert!(saw_cancelled_tool_activity);
        assert!(saw_cancelled_completion);
    }

    #[tokio::test]
    async fn start_without_ext_namespace_is_rejected() {
        let runtime = LocalMockRuntime::new();
        let error = runtime
            .start(RuntimeStartRequest {
                runtime_id: "runtime-x".into(),
                conversation_id: "conversation-x".into(),
                sender_id: "sender-x".into(),
                workspace: WorkspaceFacts {
                    workspace_id: "workspace-x".into(),
                    root: "/tmp".into(),
                    access: WorkspaceAccess::ReadWrite,
                    revision: None,
                    metadata: Value::Null,
                },
                state: None,
                llm: None,
                owner_ref: "admin".into(),
                ext: Default::default(),
            })
            .await
            .expect_err("missing ext namespace must be rejected");
        assert!(matches!(error, SessionDomainError::InvalidRequest { .. }));
    }
}
