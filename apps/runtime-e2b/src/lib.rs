use async_trait::async_trait;
use backend::e2b::E2bProvider;
use backend::{OperationAttach, ProviderInstanceLedger, SqliteProviderInstanceLedger};
use operation_protocol::capability::exec::ExecRequest;
use operation_protocol::OperationBackend;
use provider_protocol::{BackendId, ProviderControlError, ProviderKind, ProviderLifecycle};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use xgovernor_core::{
    enforce_workspace_axiom, CancelSignal, CapabilityFamily, IsolationBoundary, IsolationFacts,
    NetworkIsolation, NormalizedSessionEnvironment, RuntimeAdapter, RuntimeEvent,
    RuntimeEventReceiver, RuntimeInteractionInput, RuntimeStartRequest, RuntimeTurnInput,
    SandboxCapability, SecurityContext, SessionDomainError, SessionEnvironmentNormalizer,
    TurnCancellationRegistry, WorkspaceAccess, WorkspaceFacts,
};
use xgovernor_manager::{InstanceManager, InstanceManagerConfig};

/// Namespaced `ext` key this adapter reads `backend_id` from, mirroring
/// `apps/runtime-local`'s `EXT_NAMESPACE` convention — a runtime-adapter
/// placement decision, not a cross-cutting session concept, so
/// `RuntimeStartRequest` intentionally has no dedicated field for it. Unlike
/// `backend_id`, `owner_ref` is a first-class `RuntimeStartRequest` field
/// (mechanically derived by the application layer from `SecurityContext`,
/// `docs/tenancy_design.md` §7 step 4) and is no longer read from `ext` at
/// all — no dual-path/fallback.
pub const EXT_NAMESPACE: &str = "runtime_e2b";

/// Per-owner concurrent sandbox cap enforced by the wrapped
/// [`InstanceManager`]. Matches `apps/runtime-local`'s default; making this
/// configurable is future work once a real caller needs it.
const DEFAULT_MAX_SANDBOXES_PER_OWNER: usize = 20;

/// Global (cross-owner) concurrent sandbox ceiling enforced by the wrapped
/// [`InstanceManager`]. Matches `apps/runtime-local`'s default — see that
/// crate's copy of this constant for the rationale.
const DEFAULT_MAX_SANDBOXES_GLOBAL: usize = 1024;

/// e2b's own workspace-root convention (`backend::e2b::backend::
/// DEFAULT_WORKSPACE_ROOT`, not exported outside that module). Duplicated
/// here as a literal rather than threading a new export through `crates/
/// backend`, since this is the only caller outside that crate that needs it:
/// [`GitSandboxWorkspaceEnvironment`] uses it as the `git clone` target so
/// the clone lands in the same directory e2b's own envd bootstrap already
/// created (empty, ready to clone into).
const E2B_WORKSPACE_ROOT: &str = "/home/user/workspace";

/// Wire shape carried through `RuntimeStartRequest.workspace.metadata`
/// (`docs/tenancy_design.md` §7 step 3) from
/// [`GitSandboxWorkspaceEnvironment::normalize`] to [`E2bMockRuntime::start`].
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

#[derive(Debug, Clone, Deserialize)]
struct E2bRuntimeExt {
    backend_id: String,
}

fn read_e2b_runtime_ext(
    ext: &session_protocol::SessionExtensions,
) -> Result<E2bRuntimeExt, SessionDomainError> {
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

/// Runs `git clone` inside the just-created sandbox via its attached
/// `OperationExec`, targeting `workspace_root` directly (the sandbox's own
/// envd bootstrap already created that directory, empty — `git clone` can
/// target an existing empty directory without error).
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

/// `RuntimeAdapter` backed by a real [`E2bProvider`] via a shared
/// [`InstanceManager`] — the same composition `apps/runtime-local`'s
/// `LocalMockRuntime` uses, per the reuse note on `InstanceManager`'s own
/// module doc. `start()` additionally runs a real `git clone` inside the
/// sandbox when `RuntimeStartRequest.workspace.metadata` carries
/// [`GitWorkspaceMetadata`] (i.e. whenever the session was admitted through
/// [`GitSandboxWorkspaceEnvironment`]).
pub struct E2bMockRuntime {
    manager: Arc<InstanceManager>,
    /// Backs the `submit_turn`/`cancel` contract — see `apps/runtime-local`'s
    /// `LocalMockRuntime::cancellation` doc for the full rationale, which
    /// applies here unchanged.
    cancellation: Arc<TurnCancellationRegistry>,
}

impl E2bMockRuntime {
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

    /// Durable variant — see `apps/runtime-local`'s
    /// `LocalMockRuntime::with_ledger_path` doc for the full rationale; this
    /// mirrors it exactly for the e2b-backed adapter.
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
        let e2b = Arc::new(E2bProvider::new());
        let lifecycle: Arc<dyn ProviderLifecycle> = e2b.clone();
        let attach: Arc<dyn OperationAttach> = e2b;
        let manager = Arc::new(InstanceManager::new(
            lifecycle,
            attach,
            ledger,
            ProviderKind("e2b".to_string()),
            InstanceManagerConfig::new(max_per_owner, DEFAULT_MAX_SANDBOXES_GLOBAL),
        ));
        Self {
            manager,
            cancellation: Arc::new(TurnCancellationRegistry::new()),
        }
    }

    /// Restore this runtime's operational state from durable storage before
    /// it starts serving real traffic — see `apps/runtime-local`'s
    /// `LocalMockRuntime::reconcile_on_startup` doc for the full rationale;
    /// this mirrors it exactly for the e2b-backed adapter. Note
    /// `E2bProvider::list_instances` is currently a fresh-process in-memory
    /// snapshot (not a real E2B "list sandboxes" API call, see that method's
    /// doc comment in `crates/backend/src/e2b/provider.rs`), so today this
    /// will treat every e2b ledger row as orphaned on restart regardless of
    /// whether the sandbox is still actually running on the platform — a
    /// known, documented limitation, not a silent gap.
    pub async fn reconcile_on_startup(
        &self,
    ) -> Result<xgovernor_manager::ReconcileOutcome, ProviderControlError> {
        self.manager.reconcile().await
    }

    /// Spawns the background sweep that retries pending sandbox-delete
    /// releases that failed inline — see `apps/runtime-local`'s
    /// `LocalMockRuntime::spawn_pending_release_retry_loop` doc for the full
    /// rationale; this mirrors it exactly for the e2b-backed adapter. Call
    /// once alongside [`Self::reconcile_on_startup`] (see
    /// `apps/server/src/main.rs`).
    pub fn spawn_pending_release_retry_loop(&self) -> tokio::task::JoinHandle<()> {
        Arc::clone(&self.manager).spawn_retry_loop()
    }
}

impl Default for E2bMockRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RuntimeAdapter for E2bMockRuntime {
    fn kind(&self) -> &str {
        "e2b-git-sandbox"
    }

    fn capabilities(&self) -> BTreeSet<session_protocol::SessionRuntimeCapability> {
        BTreeSet::new()
    }

    async fn start(&self, request: RuntimeStartRequest) -> Result<(), SessionDomainError> {
        let ext = read_e2b_runtime_ext(&request.ext)?;

        // `allow_internet_access: true` is static/create-time-only (see the
        // module doc): this slice never toggles it after the sandbox exists.
        let backend = self
            .manager
            .start_instance(
                request.runtime_id.clone(),
                BackendId(ext.backend_id),
                request.owner_ref.clone(),
                json!({
                    "workspace_root": request.workspace.root,
                    "allow_internet_access": true,
                }),
            )
            .await
            .map_err(map_provider_error)?;

        if request.workspace.metadata != Value::Null {
            let git: GitWorkspaceMetadata = serde_json::from_value(request.workspace.metadata)
                .map_err(|error| SessionDomainError::InvalidRequest {
                    message: format!("invalid workspace_metadata for git clone: {error}"),
                })?;

            if let Err(error) =
                clone_git_workspace(backend.as_ref(), &git, &request.workspace.root).await
            {
                // Roll back the sandbox we just created rather than leaving
                // an orphaned instance registered with no usable workspace.
                let _ = self.manager.stop_instance(&request.runtime_id).await;
                return Err(error);
            }
        }

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

        // Same contract as `apps/runtime-local`'s `submit_turn`: register the
        // cancel signal and spawn the background task before returning, so
        // this call hands back `rx` immediately instead of blocking on the
        // turn's full duration — see `RuntimeAdapter::submit_turn`'s doc.
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
        // Same rationale as `apps/runtime-local`: fires the cancel signal
        // `run_mock_turn`'s `tokio::select!` is racing the exec future
        // against. A stale/mismatched `turn_id` is a silent no-op.
        self.cancellation.fire(runtime_id, turn_id);
        Ok(())
    }
}

/// The background task `submit_turn` spawns to drive a turn — mirrors
/// `apps/runtime-local::run_mock_turn` exactly (same "mocked decision loop,
/// real execution" split, same cancellation race), duplicated here rather
/// than shared because each adapter's mock turn-driving logic is its own
/// business logic (only the cancellation bookkeeping in
/// `TurnCancellationRegistry` is shared — see that type's module doc).
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
            // Best-effort interrupt — same caveat as `apps/runtime-local`:
            // `exec_future` is simply dropped, and whether the sandbox's
            // process actually dies depends on the attached backend's
            // `OperationExec` reacting to that.
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
/// git-workspace sessions materialized by [`E2bMockRuntime`]. Calls
/// [`enforce_workspace_axiom`] with `provider_is_sandbox = true` (this
/// normalizer only ever backs the e2b sandbox adapter, never a host-only
/// one), then only knows how to produce facts for `WorkspaceSpec::Git` — any
/// other spec (including from an admin, who `enforce_workspace_axiom` would
/// otherwise allow through unconditionally) is rejected here, since this
/// normalizer has no host-path or shared-workspace handling to fall back to.
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
    use session_protocol::{SessionExtensions, SessionOpenRequest, WorkspaceSpec};

    fn e2b_runtime_ext() -> SessionExtensions {
        [(EXT_NAMESPACE.to_string(), json!({ "backend_id": "e2b" }))]
            .into_iter()
            .collect()
    }

    fn open_request(workspace: WorkspaceSpec) -> SessionOpenRequest {
        SessionOpenRequest {
            runtime_id: None,
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

    #[tokio::test]
    async fn tenant_git_https_workspace_is_admitted_with_sandbox_facts() {
        let normalizer = GitSandboxWorkspaceEnvironment;
        let ctx = SecurityContext::tenant("tenant-1", "tenant-1");
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
            IsolationBoundary::VirtualMachine
        );
        assert_eq!(normalized.isolation.network, NetworkIsolation::None);
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
        let ctx = SecurityContext::tenant("tenant-1", "tenant-1");
        // `NormalizedSessionEnvironment` has no `Debug` impl (it can carry an
        // opaque `Value`/`ResolvedLlm`), so `expect_err` (which requires
        // `T: Debug` to format a panic message on the non-error branch)
        // can't be used here — match instead.
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
        let ctx = SecurityContext::tenant("tenant-1", "tenant-1");
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
        // `enforce_workspace_axiom` alone would allow an admin through with
        // any workspace spec, but this normalizer only knows how to produce
        // facts for `Git` — it must not silently mishandle anything else,
        // even for an admin.
        let normalizer = GitSandboxWorkspaceEnvironment;
        let ctx = SecurityContext::admin("admin-1");
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
        let ctx = SecurityContext::admin("admin-1");
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

    #[tokio::test]
    async fn start_without_ext_namespace_is_rejected() {
        let runtime = E2bMockRuntime::new();
        let error = runtime
            .start(RuntimeStartRequest {
                runtime_id: "runtime-x".into(),
                conversation_id: "conversation-x".into(),
                sender_id: "sender-x".into(),
                workspace: WorkspaceFacts {
                    workspace_id: "workspace-x".into(),
                    root: "/home/user/workspace".into(),
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

    /// Test double for `run_mock_turn`'s contract tests below. Exercising
    /// the contract through the full `RuntimeAdapter::submit_turn`/`cancel`
    /// would require `E2bMockRuntime::start` to succeed, which needs a real
    /// `E2B_API_KEY` and network access (`resolve_api_key` fails fast
    /// without one) — exactly why the existing live integration test at the
    /// bottom of this module is `#[ignore]`d. `run_mock_turn` is the actual
    /// piece of logic the fix changed (mirrors
    /// `apps/runtime-local::run_mock_turn` exactly); `submit_turn`/`cancel`
    /// are now just thin `TurnCancellationRegistry::begin`/`spawn`/`fire`
    /// wrappers around it (see their bodies above), so exercising it
    /// directly against a fake `OperationBackend` tests the same contract
    /// without needing live e2b infrastructure. Only `exec()` is ever
    /// called; every other `OperationBackend` method is unreachable here.
    struct NeverRespondingBackend;

    #[async_trait]
    impl operation_protocol::capability::OperationExec for NeverRespondingBackend {
        async fn exec(
            &self,
            _request: ExecRequest,
        ) -> Result<
            operation_protocol::capability::exec::ExecResult,
            operation_protocol::OperationError,
        > {
            // Never `Ready` on first poll, like a real exec against a real
            // sandbox — lets `tokio::select!` in `run_mock_turn` deterministically
            // pick an already-fired cancel signal instead of racing.
            std::future::pending::<()>().await;
            unreachable!("test backend's exec never resolves")
        }
    }

    #[async_trait]
    impl operation_protocol::OperationBackend for NeverRespondingBackend {
        fn backend_id(&self) -> &str {
            "never-responding-test-backend"
        }
        fn capabilities(&self) -> operation_protocol::OperationBackendCapabilities {
            unimplemented!("not exercised by run_mock_turn")
        }
        fn paths(&self) -> &dyn operation_protocol::capability::OperationPathResolver {
            unimplemented!("not exercised by run_mock_turn")
        }
        fn files(&self) -> &dyn operation_protocol::capability::OperationFileSystem {
            unimplemented!("not exercised by run_mock_turn")
        }
        fn search(&self) -> &dyn operation_protocol::capability::OperationSearch {
            unimplemented!("not exercised by run_mock_turn")
        }
        fn exec(&self) -> &dyn operation_protocol::capability::OperationExec {
            self
        }
        fn export(&self) -> &dyn operation_protocol::capability::OperationExport {
            unimplemented!("not exercised by run_mock_turn")
        }
        async fn shutdown(&self) -> Result<(), operation_protocol::OperationError> {
            unimplemented!("not exercised by run_mock_turn")
        }
    }

    /// Same contract pinned down as `apps/runtime-local`'s
    /// `submit_turn_returns_before_the_turn_has_produced_any_events` — see
    /// that test's doc for why this is deterministic, not a timing race.
    /// Exercises `run_mock_turn` directly (see `NeverRespondingBackend`'s
    /// doc for why).
    #[tokio::test]
    async fn submit_turn_returns_before_the_turn_has_produced_any_events() {
        let cancellation = Arc::new(TurnCancellationRegistry::new());
        let cancel_signal = cancellation.begin("runtime-1", "turn-1");
        let (tx, mut events) = tokio::sync::mpsc::channel(4);

        tokio::spawn(run_mock_turn(
            Arc::new(NeverRespondingBackend),
            cancellation,
            "runtime-1".into(),
            "turn-1".into(),
            "hello".into(),
            tx,
            cancel_signal,
        ));

        assert!(
            matches!(
                events.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "the spawned background task must not have run yet immediately after spawning, \
             on a current-thread test runtime, without an intervening yield"
        );
    }

    /// Same contract pinned down as `apps/runtime-local`'s
    /// `cancel_called_immediately_after_submit_turn_reaches_a_cancelled_terminal_state`
    /// — see that test's doc for why this is deterministic, not a timing
    /// race. Exercises `run_mock_turn` directly (see
    /// `NeverRespondingBackend`'s doc for why).
    #[tokio::test]
    async fn cancel_called_immediately_after_submit_turn_reaches_a_cancelled_terminal_state() {
        let cancellation = Arc::new(TurnCancellationRegistry::new());
        let cancel_signal = cancellation.begin("runtime-1", "turn-1");
        let (tx, mut events) = tokio::sync::mpsc::channel(4);

        tokio::spawn(run_mock_turn(
            Arc::new(NeverRespondingBackend),
            Arc::clone(&cancellation),
            "runtime-1".into(),
            "turn-1".into(),
            "hello".into(),
            tx,
            cancel_signal,
        ));

        // Synchronous (mutex lock + oneshot send, no internal `.await`), so
        // this resolves without yielding — the spawned task above still has
        // not run a single poll by the time this returns.
        cancellation.fire("runtime-1", Some("turn-1"));

        let mut saw_cancelled_tool_activity = false;
        let mut saw_cancelled_completion = false;
        while let Some(event) = events.recv().await {
            match event {
                RuntimeEvent::ToolActivity { status, phase, .. } => {
                    if phase == session_protocol::SessionToolActivityPhase::End {
                        assert_eq!(
                            status,
                            session_protocol::SessionToolActivityStatus::Cancelled,
                            "a cancelled turn's tool activity must end as Cancelled"
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

    /// Live integration test: creates a real e2b sandbox, clones a real
    /// public repo into it via `E2bMockRuntime::start`, verifies the clone
    /// landed by `exec`-ing `git rev-parse HEAD` inside the sandbox, then
    /// tears the sandbox down. Requires `E2B_API_KEY` in the environment and
    /// outbound network access, so this stays `#[ignore]`d like the existing
    /// live e2b test in `crates/backend/src/e2b/provider.rs`.
    #[tokio::test]
    #[ignore = "requires E2B_API_KEY and creates a real E2B sandbox with network access"]
    async fn start_clones_a_real_public_repo_into_a_real_e2b_sandbox() {
        let runtime = E2bMockRuntime::new();
        let normalizer = GitSandboxWorkspaceEnvironment;
        let ctx = SecurityContext::tenant("tenant-1", "tenant-1");

        let request = open_request(WorkspaceSpec::Git {
            url: "https://github.com/octocat/Hello-World.git".into(),
            reference: None,
            subdirectory: None,
        });
        let normalized = normalizer
            .normalize(&ctx, &request)
            .await
            .expect("normalize must succeed");

        let runtime_id = "runtime-live-e2b-clone-1".to_string();
        runtime
            .start(RuntimeStartRequest {
                runtime_id: runtime_id.clone(),
                conversation_id: request.conversation_id.clone(),
                sender_id: request.sender_id.clone(),
                workspace: normalized.workspace.clone(),
                state: None,
                llm: None,
                owner_ref: ctx.owner_ref(),
                ext: e2b_runtime_ext(),
            })
            .await
            .expect("start must clone the repo into the sandbox");

        let backend = runtime
            .manager
            .backend_for(&runtime_id)
            .expect("backend must be registered after start");
        let result = backend
            .exec()
            .exec(ExecRequest {
                command: "git".to_string(),
                args: vec![
                    "-C".to_string(),
                    normalized.workspace.root,
                    "rev-parse".to_string(),
                    "HEAD".to_string(),
                ],
                shell: None,
                cwd: None,
                timeout_ms: Some(10_000),
                env: None,
            })
            .await
            .expect("git rev-parse exec must succeed");
        assert_eq!(result.exit_code, Some(0));

        runtime
            .stop(&runtime_id)
            .await
            .expect("stop must tear down the sandbox");
    }
}
