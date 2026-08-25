//! Assembly crate: wires real [`backend::local::LocalProvider`] /
//! [`backend::e2b::E2bProvider`] sandbox plumbing behind the
//! `xgovernor_core::RuntimeAdapter` seam, dispatching between them by
//! `ext.runtime_mock.backend_id` at `start()` time.
//!
//! This is deliberately *not* an agent execution engine: xGovernor's product
//! scope is managing other agent runtimes via their own SDKs/APIs, not
//! reimplementing an LLM decision loop. What `submit_turn` decides to do is
//! therefore hardcoded/mocked here (a single fixed `exec` call), but how it
//! is carried out is fully real: a real `ProviderLifecycle::create`, a real
//! `OperationAttach::attach`, and a real `OperationExec::exec` against the
//! attached backend. The point of this crate is to prove that plumbing (and
//! to give `docs/runtime_adapter_guide.md` a minimal reference adapter), not
//! to stand in for a runtime's intelligence.
//!
//! This used to be two separate crates (`apps/runtime-local`,
//! `apps/runtime-e2b`), each hardwired at construction time to exactly one
//! provider and each re-parsing an `ext.backend_id` field that, because of
//! that hardwiring, was validated but never actually used to *select*
//! anything. Neither crate was referenced by `apps/server` or any other
//! production wiring — both existed purely to exercise `crates/manager`'s
//! `InstanceManager` plumbing end-to-end without needing a real runtime.
//! `crates/manager` has since grown its own direct test suite covering the
//! quota/idempotency/compensating-delete/retry contracts those crates used
//! to pin down, so this merge additionally drops the tests that had become
//! pure duplicates of that suite.
//!
//! The merge folds both into one [`MockRuntime`] that composes an
//! `InstanceManager` *per* `backend_id` (`managers: HashMap<String,
//! Arc<InstanceManager>>`) and dispatches on `ext.runtime_mock.backend_id` at
//! `start()`, exactly mirroring how the real `apps/runtime-pi::PiRuntime`
//! selects between its own `"local"`/`"e2b"` managers. That makes
//! `backend_id` do what it always claimed to do, and gives this crate the
//! same shape a reader would see in the real adapter.

use async_trait::async_trait;
use backend::e2b::E2bProvider;
use backend::local::LocalProvider;
use backend::{OperationAttach, ProviderInstanceLedger, SqliteProviderInstanceLedger};
use operation_protocol::capability::exec::ExecRequest;
use operation_protocol::OperationBackend;
use provider_protocol::{BackendId, ProviderControlError, ProviderKind, ProviderLifecycle};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use xgovernor_core::{
    enforce_workspace_axiom, CancelSignal, CapabilityFamily, IsolationBoundary, IsolationFacts,
    NetworkIsolation, NormalizedSessionEnvironment, RuntimeAdapter, RuntimeEvent,
    RuntimeEventReceiver, RuntimeInteractionInput, RuntimeStartRequest, RuntimeTurnInput,
    SandboxCapability, SecurityContext, SessionDomainError, SessionEnvironmentNormalizer,
    TurnCancellationRegistry, WorkspaceAccess, WorkspaceFacts,
};
use xgovernor_manager::{InstanceManager, InstanceManagerConfig};

/// Namespaced `ext` key this adapter reads `backend_id` from, since
/// `RuntimeStartRequest` intentionally has no dedicated field for it (a
/// runtime-adapter placement decision, not a cross-cutting session concept —
/// unlike `owner_ref`, which is a first-class `RuntimeStartRequest` field,
/// mechanically derived by the application layer from the authenticated
/// `SecurityContext`, `docs/tenancy_design.md` §7 step 4).
pub const EXT_NAMESPACE: &str = "runtime_mock";

/// Backend id naming the [`LocalProvider`]-backed manager in
/// [`MockRuntime::local_only`] / [`MockRuntime::local_and_e2b`].
pub const LOCAL_BACKEND_ID: &str = "local";
/// Backend id naming the [`E2bProvider`]-backed manager in
/// [`MockRuntime::local_and_e2b`].
pub const E2B_BACKEND_ID: &str = "e2b";

/// Per-owner concurrent sandbox cap enforced by each wrapped
/// [`InstanceManager`]. A minimal fixed default is enough for a mock/demo
/// runtime; making this configurable is future work once a real caller needs
/// it (the real `apps/runtime-pi` adapter takes fully-constructed managers
/// instead, so a production deployment already configures this per-manager
/// there, not here).
const DEFAULT_MAX_SANDBOXES_PER_OWNER: usize = 20;

/// Global (cross-owner) concurrent sandbox ceiling enforced by each wrapped
/// [`InstanceManager`]. Sized loosely against `docs/tenancy_design.md`'s
/// stated scale assumption ("数十个租户，1000–2000 并发 agent").
const DEFAULT_MAX_SANDBOXES_GLOBAL: usize = 1024;

/// e2b's own workspace-root convention (`backend::e2b::backend::
/// DEFAULT_WORKSPACE_ROOT`, not exported outside that module). Duplicated
/// here as a literal rather than threading a new export through `crates/
/// backend`, since [`GitSandboxWorkspaceEnvironment`] is the only caller
/// outside that crate that needs it: it uses this as the `git clone` target
/// so the clone lands in the same directory e2b's own envd bootstrap already
/// created (empty, ready to clone into).
const E2B_WORKSPACE_ROOT: &str = "/home/user/workspace";

#[derive(Debug, Clone, Deserialize)]
struct MockRuntimeExt {
    backend_id: String,
}

fn read_backend_id_ext(
    ext: &session_protocol::SessionExtensions,
) -> Result<MockRuntimeExt, SessionDomainError> {
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

/// Wire shape carried through `RuntimeStartRequest.workspace.metadata`
/// (`docs/tenancy_design.md` §7 step 3) from
/// [`GitSandboxWorkspaceEnvironment::normalize`] to [`MockRuntime::start`].
/// Private to this crate: only these two types need to agree on the shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct GitWorkspaceMetadata {
    url: String,
    #[serde(default)]
    reference: Option<String>,
    /// Deferred to future work (subdirectory-scoped checkout, `docs/
    /// tenancy_design.md` §5.2). Parsed now so the metadata schema is
    /// already stable, but `clone_git_workspace` below does not act on it
    /// yet — the whole repo is always cloned to the sandbox workspace root.
    #[serde(default)]
    #[allow(dead_code)]
    subdirectory: Option<String>,
}

/// Runs `git clone` inside the just-created sandbox via its attached
/// `OperationExec`, targeting `workspace_root` directly (the sandbox's own
/// envd bootstrap already created that directory, empty — `git clone` can
/// target an existing empty directory without error). Backend-agnostic: it
/// runs whenever `start()` sees git workspace metadata, regardless of which
/// `backend_id` provisioned the sandbox — in practice only
/// [`GitSandboxWorkspaceEnvironment`] ever produces that metadata, and it is
/// e2b-only, but nothing here hardcodes that assumption.
async fn clone_git_workspace(
    backend: &dyn OperationBackend,
    git: &GitWorkspaceMetadata,
    workspace_root: &str,
) -> Result<(), SessionDomainError> {
    let mut args = vec!["clone".to_string()];
    if let Some(reference) = &git.reference {
        args.push("--branch".to_string());
        args.push(reference.clone());
    }
    args.push(git.url.clone());
    args.push(workspace_root.to_string());

    let result = backend
        .exec()
        .exec(ExecRequest {
            command: "git".to_string(),
            args,
            shell: None,
            cwd: None,
            timeout_ms: Some(120_000),
            env: None,
            extra: None,
        })
        .await
        .map_err(|error| SessionDomainError::Internal {
            message: format!("git clone exec failed: {error}"),
            source: None,
        })?;

    if result.exit_code != Some(0) {
        let stderr = String::from_utf8_lossy(&result.stderr).into_owned();
        return Err(SessionDomainError::Internal {
            message: format!("git clone failed (exit {:?}): {stderr}", result.exit_code),
            source: None,
        });
    }
    Ok(())
}

fn in_memory_local_manager(max_per_owner: usize) -> Arc<InstanceManager> {
    let ledger: Arc<dyn ProviderInstanceLedger> = Arc::new(
        SqliteProviderInstanceLedger::open_in_memory()
            .expect("open in-memory provider instance ledger"),
    );
    let local = Arc::new(LocalProvider::new());
    let lifecycle: Arc<dyn ProviderLifecycle> = local.clone();
    let attach: Arc<dyn OperationAttach> = local;
    Arc::new(InstanceManager::new(
        lifecycle,
        attach,
        ledger,
        ProviderKind(LOCAL_BACKEND_ID.to_string()),
        InstanceManagerConfig::new(max_per_owner, DEFAULT_MAX_SANDBOXES_GLOBAL),
    ))
}

fn in_memory_e2b_manager(max_per_owner: usize) -> Arc<InstanceManager> {
    let ledger: Arc<dyn ProviderInstanceLedger> = Arc::new(
        SqliteProviderInstanceLedger::open_in_memory()
            .expect("open in-memory provider instance ledger"),
    );
    let e2b = Arc::new(E2bProvider::new());
    let lifecycle: Arc<dyn ProviderLifecycle> = e2b.clone();
    let attach: Arc<dyn OperationAttach> = e2b;
    Arc::new(InstanceManager::new(
        lifecycle,
        attach,
        ledger,
        ProviderKind(E2B_BACKEND_ID.to_string()),
        InstanceManagerConfig::new(max_per_owner, DEFAULT_MAX_SANDBOXES_GLOBAL),
    ))
}

/// Mock `RuntimeAdapter` composing one [`InstanceManager`] per `backend_id`
/// it is willing to provision sandboxes against, selected at `start()` by
/// `ext.runtime_mock.backend_id` — the same shape the real
/// `apps/runtime-pi::PiRuntime` uses for its own `managers` field.
pub struct MockRuntime {
    managers: HashMap<String, Arc<InstanceManager>>,
    /// Which manager each started `runtime_id` was provisioned against —
    /// needed because `stop`/`attach`/`submit_turn` only receive a
    /// `runtime_id`, not the `backend_id` that was used at `start()` time.
    /// Mirrors `apps/runtime-pi`'s `PiInstance::manager` field, which solves
    /// the identical problem for the real adapter.
    instances: tokio::sync::RwLock<HashMap<String, Arc<InstanceManager>>>,
    /// Backs the `submit_turn`/`cancel` contract (see their doc comments on
    /// `RuntimeAdapter`): `submit_turn` spawns a background task to drive the
    /// turn and registers it here so a later `cancel` call can actually
    /// reach and interrupt it, instead of `cancel` being a no-op because
    /// `submit_turn` already ran the whole turn to completion synchronously
    /// before returning.
    cancellation: Arc<TurnCancellationRegistry>,
}

impl MockRuntime {
    /// Primary constructor: takes already-built managers, one per
    /// `backend_id` this runtime should accept in `ext.runtime_mock.
    /// backend_id`. Mirrors `apps/runtime-pi::PiRuntime::new`'s injection
    /// pattern — a real deployment builds each `InstanceManager` with a
    /// durable (`SqliteProviderInstanceLedger::open`) ledger and whatever
    /// per-manager caps it needs, the same way `apps/server/src/main.rs`
    /// would for `PiRuntime`.
    pub fn new(managers: HashMap<String, Arc<InstanceManager>>) -> Self {
        Self {
            managers,
            instances: tokio::sync::RwLock::new(HashMap::new()),
            cancellation: Arc::new(TurnCancellationRegistry::new()),
        }
    }

    /// Test/demo convenience: a single `"local"` backend over a real
    /// `LocalProvider`, in-memory ledger, default sandbox caps.
    pub fn local_only() -> Self {
        let mut managers = HashMap::new();
        managers.insert(
            LOCAL_BACKEND_ID.to_string(),
            in_memory_local_manager(DEFAULT_MAX_SANDBOXES_PER_OWNER),
        );
        Self::new(managers)
    }

    /// Test/demo convenience: both `"local"` and `"e2b"` backends
    /// registered, each with its own in-memory ledger and default sandbox
    /// caps.
    pub fn local_and_e2b() -> Self {
        let mut managers = HashMap::new();
        managers.insert(
            LOCAL_BACKEND_ID.to_string(),
            in_memory_local_manager(DEFAULT_MAX_SANDBOXES_PER_OWNER),
        );
        managers.insert(
            E2B_BACKEND_ID.to_string(),
            in_memory_e2b_manager(DEFAULT_MAX_SANDBOXES_PER_OWNER),
        );
        Self::new(managers)
    }

    fn manager_for(&self, backend_id: &str) -> Result<Arc<InstanceManager>, SessionDomainError> {
        self.managers
            .get(backend_id)
            .cloned()
            .ok_or_else(|| SessionDomainError::InvalidRequest {
                message: format!(
                    "no InstanceManager configured for backend_id '{backend_id}'; configured \
                     backends: {:?}",
                    self.managers.keys().collect::<Vec<_>>()
                ),
            })
    }

    async fn manager_for_runtime(
        &self,
        runtime_id: &str,
    ) -> Result<Arc<InstanceManager>, SessionDomainError> {
        self.instances
            .read()
            .await
            .get(runtime_id)
            .cloned()
            .ok_or_else(|| SessionDomainError::NotFound {
                runtime_id: runtime_id.to_string(),
            })
    }
}

#[async_trait]
impl RuntimeAdapter for MockRuntime {
    fn kind(&self) -> &str {
        "mock"
    }
    async fn check_alive(&self, _runtime_id: &str) -> Result<bool, SessionDomainError> {
        Ok(true)
    }

    fn capabilities(&self) -> BTreeSet<session_protocol::SessionRuntimeCapability> {
        BTreeSet::new()
    }

    async fn start(&self, request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
        let ext = read_backend_id_ext(&request.ext)?;
        let manager = self.manager_for(&ext.backend_id)?;

        // `allow_internet_access: true` is static/create-time-only, and only
        // meaningful to the e2b provider — the local provider ignores extra
        // `provider_options` keys it doesn't recognize, but there is no
        // reason to send it a key it has no use for.
        let mut provider_options = json!({ "workspace_root": request.workspace.root });
        if ext.backend_id == E2B_BACKEND_ID {
            provider_options["allow_internet_access"] = json!(true);
        }

        let backend = manager
            .start_instance(
                request.runtime_id.clone(),
                BackendId(ext.backend_id.clone()),
                request.owner_ref.clone(),
                provider_options,
            )
            .await
            .map_err(map_provider_error)?;

        if request.workspace.metadata != Value::Null {
            let git: GitWorkspaceMetadata =
                serde_json::from_value(request.workspace.metadata.clone()).map_err(|error| {
                    SessionDomainError::InvalidRequest {
                        message: format!("invalid workspace_metadata for git clone: {error}"),
                    }
                })?;

            if let Err(error) =
                clone_git_workspace(backend.as_ref(), &git, &request.workspace.root).await
            {
                // Roll back the sandbox we just created rather than leaving
                // an orphaned instance registered with no usable workspace.
                let _ = manager.stop_instance(&request.runtime_id).await;
                return Err(error);
            }
        }

        self.instances
            .write()
            .await
            .insert(request.runtime_id.clone(), manager);
        Ok(())
    }

    async fn stop(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
        let manager = self.manager_for_runtime(runtime_id).await?;
        manager
            .stop_instance(runtime_id)
            .await
            .map_err(map_provider_error)?;
        self.instances.write().await.remove(runtime_id);
        Ok(())
    }

    async fn attach(&self, runtime_id: &str) -> Result<(), SessionDomainError> {
        let manager = self.manager_for_runtime(runtime_id).await?;
        manager
            .backend_for(runtime_id)
            .map(|_| ())
            .map_err(map_provider_error)
    }

    async fn submit_turn(
        &self,
        input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, SessionDomainError> {
        let manager = self.manager_for_runtime(&input.runtime_id).await?;
        let backend = manager
            .backend_for(&input.runtime_id)
            .map_err(map_provider_error)?;
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        // Contract (`RuntimeAdapter::submit_turn`'s doc comment): register
        // the cancel signal and spawn the background task *before*
        // returning — this call must hand back `rx` without waiting for the
        // turn to run, so a `cancel` that arrives the instant this returns
        // can never miss the registration and the caller never blocks on
        // the turn's actual duration.
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
        // active, if anything" contract. No manager lookup needed here: the
        // cancellation registry is keyed by `runtime_id`/`turn_id`
        // directly, independent of which backend provisioned the sandbox.
        self.cancellation.fire(runtime_id, turn_id);
        Ok(())
    }
}

/// The background task `submit_turn` spawns to actually drive a turn — this
/// is where the "mocked decision loop, real execution" split lives: what to
/// run is hardcoded (a single `echo`, no LLM decision loop; that is the
/// "mock" part), but how it runs is real (the attached backend's actual
/// `OperationExec`), racing that real execution against cancellation.
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
        extra: None,
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

/// `SessionEnvironmentNormalizer` for the §7 step-3 narrow slice: sandboxed
/// git-workspace sessions materialized by [`MockRuntime`] against its
/// `"e2b"` backend. Calls [`enforce_workspace_axiom`] with
/// `provider_is_sandbox = true` (this normalizer only ever backs the e2b
/// sandbox path, never a host-only one), then only knows how to produce
/// facts for `WorkspaceSpec::Git` — any other spec (including from an admin,
/// who `enforce_workspace_axiom` would otherwise allow through
/// unconditionally) is rejected here, since this normalizer has no
/// host-path or shared-workspace handling to fall back to.
pub struct GitSandboxWorkspaceEnvironment;

#[async_trait]
impl SessionEnvironmentNormalizer for GitSandboxWorkspaceEnvironment {
    async fn normalize(
        &self,
        ctx: &SecurityContext,
        request: &session_protocol::SessionOpenRequest,
    ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
        enforce_workspace_axiom(ctx, &request.workspace, true)?;

        let (url, reference, subdirectory) = match &request.workspace {
            session_protocol::WorkspaceSpec::Git {
                url,
                reference,
                subdirectory,
            } => (url.clone(), reference.clone(), subdirectory.clone()),
            _ => {
                return Err(SessionDomainError::InvalidRequest {
                    message: "GitSandboxWorkspaceEnvironment only supports WorkspaceSpec::Git \
                              (docs/tenancy_design.md §7 step 3)"
                        .to_string(),
                });
            }
        };

        let metadata = serde_json::to_value(GitWorkspaceMetadata {
            url,
            reference: reference.clone(),
            subdirectory,
        })
        .expect("GitWorkspaceMetadata serialization is infallible");

        Ok(NormalizedSessionEnvironment {
            workspace: WorkspaceFacts {
                workspace_id: format!("git-sandbox-{}", request.conversation_id),
                root: E2B_WORKSPACE_ROOT.to_string(),
                access: WorkspaceAccess::ReadWrite,
                revision: reference,
                metadata,
            },
            isolation: IsolationFacts {
                boundary: IsolationBoundary::VirtualMachine,
                workspace_access: WorkspaceAccess::ReadWrite,
                // Honest, not aspirational: this slice never cuts network
                // mid-session (see module doc), so it must not claim
                // `Restricted`/`Isolated`.
                network: NetworkIsolation::None,
                metadata: Value::Null,
            },
            sandbox_capabilities: [
                SandboxCapability::Exec,
                SandboxCapability::FileRead,
                SandboxCapability::FileWrite,
                SandboxCapability::Network,
            ]
            .into_iter()
            .collect(),
            llm: None,
            lease: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use session_protocol::SessionExtensions;
    use session_protocol::{SessionOpenRequest, SessionTurnRequest, WorkspaceSpec};
    use std::sync::Arc as StdArc;
    use tempfile::TempDir;
    use xgovernor_core::{
        Clock, IsolationBoundary as CoreIsolationBoundary, IsolationFacts as CoreIsolationFacts,
        NetworkIsolation as CoreNetworkIsolation,
        NormalizedSessionEnvironment as CoreNormalizedSessionEnvironment, RuntimeIdGenerator,
        SecurityContext as CoreSecurityContext, SessionApplication,
        SessionDomainError as DomainError, SessionEnvironmentNormalizer as CoreEnvNormalizer,
        SessionListPage, SessionRecord, SessionRepository, TurnIdGenerator,
        WorkspaceAccess as CoreWorkspaceAccess, WorkspaceFacts as CoreWorkspaceFacts,
    };

    fn backend_ext(backend_id: &str) -> SessionExtensions {
        [(
            EXT_NAMESPACE.to_string(),
            json!({ "backend_id": backend_id }),
        )]
        .into_iter()
        .collect()
    }

    fn e2b_runtime_ext() -> SessionExtensions {
        backend_ext(E2B_BACKEND_ID)
    }

    fn open_request(workspace: WorkspaceSpec) -> SessionOpenRequest {
        SessionOpenRequest {
            runtime_id: None,
            runtime_kind: None,
            conversation_id: "conversation-1".into(),
            sender_id: "sender-1".into(),
            workspace,
            deployment: Default::default(),
            requested_capabilities: Default::default(),
            llm: None,
            ext: Default::default(),
            lease: Default::default(),
        }
    }

    // ---- GitSandboxWorkspaceEnvironment (formerly apps/runtime-e2b) ----

    #[tokio::test]
    async fn tenant_git_https_workspace_is_admitted_with_sandbox_facts() {
        let normalizer = GitSandboxWorkspaceEnvironment;
        let ctx = CoreSecurityContext::tenant("tenant-1", "tenant-1");
        let normalized = normalizer
            .normalize(
                &ctx,
                &open_request(WorkspaceSpec::Git {
                    url: "https://example.com/org/repo.git".into(),
                    reference: Some("main".into()),
                    subdirectory: None,
                }),
            )
            .await
            .expect("https git workspace must be admitted for a tenant");

        assert_eq!(
            normalized.isolation.boundary,
            CoreIsolationBoundary::VirtualMachine
        );
        assert_eq!(normalized.isolation.network, CoreNetworkIsolation::None);
        assert_eq!(normalized.workspace.root, E2B_WORKSPACE_ROOT);
        assert_eq!(normalized.workspace.revision.as_deref(), Some("main"));
        assert_eq!(
            normalized.workspace.metadata["url"],
            "https://example.com/org/repo.git"
        );
    }

    #[tokio::test]
    async fn tenant_non_git_workspace_is_rejected() {
        let normalizer = GitSandboxWorkspaceEnvironment;
        let ctx = CoreSecurityContext::tenant("tenant-1", "tenant-1");
        let error = match normalizer
            .normalize(&ctx, &open_request(WorkspaceSpec::DaemonDefault))
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("non-git workspace must be rejected for a tenant"),
        };
        assert!(matches!(error, SessionDomainError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn tenant_git_url_with_embedded_credentials_is_rejected() {
        let normalizer = GitSandboxWorkspaceEnvironment;
        let ctx = CoreSecurityContext::tenant("tenant-1", "tenant-1");
        let error = match normalizer
            .normalize(
                &ctx,
                &open_request(WorkspaceSpec::Git {
                    url: "https://user:pass@example.com/org/repo.git".into(),
                    reference: None,
                    subdirectory: None,
                }),
            )
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("credentials embedded in the git url must be rejected"),
        };
        assert!(matches!(error, SessionDomainError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn admin_non_git_workspace_is_still_rejected_by_this_normalizer() {
        let normalizer = GitSandboxWorkspaceEnvironment;
        let ctx = CoreSecurityContext::admin("admin-1");
        let error = match normalizer
            .normalize(&ctx, &open_request(WorkspaceSpec::DaemonDefault))
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("non-git workspace must be rejected even for an admin"),
        };
        assert!(matches!(error, SessionDomainError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn admin_git_workspace_is_admitted() {
        let normalizer = GitSandboxWorkspaceEnvironment;
        let ctx = CoreSecurityContext::admin("admin-1");
        normalizer
            .normalize(
                &ctx,
                &open_request(WorkspaceSpec::Git {
                    url: "https://example.com/org/repo.git".into(),
                    reference: None,
                    subdirectory: None,
                }),
            )
            .await
            .expect("admin git workspace must be admitted");
    }

    // ---- ext validation ----

    #[tokio::test]
    async fn start_without_ext_namespace_is_rejected() {
        let runtime = MockRuntime::local_only();
        let error = runtime
            .start(RuntimeStartRequest {
                runtime_id: "runtime-x".into(),
                conversation_id: "conversation-x".into(),
                sender_id: "sender-x".into(),
                workspace: CoreWorkspaceFacts {
                    workspace_id: "workspace-x".into(),
                    root: "/tmp".into(),
                    access: CoreWorkspaceAccess::ReadWrite,
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

    #[tokio::test]
    async fn start_with_unregistered_backend_id_is_rejected() {
        // `local_only()` never registers an `"e2b"` manager — dispatch must
        // reject it up front rather than falling through to some default
        // manager.
        let runtime = MockRuntime::local_only();
        let error = runtime
            .start(RuntimeStartRequest {
                runtime_id: "runtime-x".into(),
                conversation_id: "conversation-x".into(),
                sender_id: "sender-x".into(),
                workspace: CoreWorkspaceFacts {
                    workspace_id: "workspace-x".into(),
                    root: "/tmp".into(),
                    access: CoreWorkspaceAccess::ReadWrite,
                    revision: None,
                    metadata: Value::Null,
                },
                state: None,
                llm: None,
                owner_ref: "admin".into(),
                ext: e2b_runtime_ext(),
            })
            .await
            .expect_err("an unregistered backend_id must be rejected");
        assert!(matches!(error, SessionDomainError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn submit_turn_for_an_unknown_runtime_id_is_not_found() {
        let runtime = MockRuntime::local_only();
        let error = runtime
            .submit_turn(RuntimeTurnInput {
                runtime_id: "never-started".into(),
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
            .expect_err("submit_turn against a runtime_id that was never started must fail");
        assert!(matches!(error, SessionDomainError::NotFound { .. }));
    }

    // ---- full application-level flow against a real local sandbox ----

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
        async fn list_active(
            &self,
            _tenant_id: Option<&str>,
            _limit: usize,
        ) -> Result<SessionListPage, DomainError> {
            Ok(SessionListPage {
                sessions: Vec::new(),
                total_active: 0,
            })
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
    impl CoreEnvNormalizer for TempWorkspaceEnvironment {
        async fn normalize(
            &self,
            _ctx: &CoreSecurityContext,
            _request: &SessionOpenRequest,
        ) -> Result<CoreNormalizedSessionEnvironment, DomainError> {
            Ok(CoreNormalizedSessionEnvironment {
                workspace: CoreWorkspaceFacts {
                    workspace_id: "workspace-1".into(),
                    root: self.root.clone(),
                    access: CoreWorkspaceAccess::ReadWrite,
                    revision: None,
                    metadata: Value::Null,
                },
                isolation: CoreIsolationFacts {
                    boundary: CoreIsolationBoundary::Host,
                    workspace_access: CoreWorkspaceAccess::ReadWrite,
                    network: CoreNetworkIsolation::None,
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
            StdArc::new(MockRuntime::local_only()),
            StdArc::new(MemoryRepository::default()),
            StdArc::new(FixedIds),
            StdArc::new(FixedIds),
            StdArc::new(TempWorkspaceEnvironment { root }),
            StdArc::new(FixedIds),
        );

        let ctx = CoreSecurityContext::admin("test");

        let opened = application
            .open(
                &ctx,
                SessionOpenRequest {
                    runtime_id: None,
                    runtime_kind: None,
                    conversation_id: "conversation-1".into(),
                    sender_id: "sender-1".into(),
                    workspace: Default::default(),
                    deployment: Default::default(),
                    requested_capabilities: Default::default(),
                    llm: None,
                    ext: backend_ext(LOCAL_BACKEND_ID),
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
    /// calling the adapter directly). Uses the `"local"` backend so the
    /// contract can be pinned down without live e2b credentials — it is the
    /// same `submit_turn`/`cancel` code path regardless of which backend
    /// provisioned the sandbox.
    async fn started_runtime(workspace_root: &str) -> MockRuntime {
        let runtime = MockRuntime::local_only();
        runtime
            .start(RuntimeStartRequest {
                runtime_id: "runtime-1".into(),
                conversation_id: "conversation-1".into(),
                sender_id: "sender-1".into(),
                workspace: CoreWorkspaceFacts {
                    workspace_id: "workspace-1".into(),
                    root: workspace_root.into(),
                    access: CoreWorkspaceAccess::ReadWrite,
                    revision: None,
                    metadata: Value::Null,
                },
                state: None,
                llm: None,
                owner_ref: "admin".into(),
                ext: backend_ext(LOCAL_BACKEND_ID),
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
    /// returning and `try_recv()`.
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
    /// has already fired.
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
}
