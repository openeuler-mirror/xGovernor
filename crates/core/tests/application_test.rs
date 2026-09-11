use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use agent_runtime_protocol::{
    AgentRuntime, RuntimeCancelRequest, RuntimeError, RuntimeEvent, RuntimeEventReceiver,
    RuntimeExecutionContext, RuntimeInteractionRequest as RuntimeInteractionInput,
    RuntimeStartRequest, RuntimeTurnRequest as RuntimeTurnInput,
};
use async_trait::async_trait;
use session_protocol::*;
use tokio::sync::{mpsc, Mutex};
use xgovernor_core::*;
use xgovernor_manager::InstanceManager;

use backend;

struct SnapshotLifecycle {
    local: Arc<backend::local::LocalProvider>,
    fail_snapshot_delete: bool,
}

#[async_trait]
impl provider_protocol::ProviderLifecycle for SnapshotLifecycle {
    async fn create(
        &self,
        request: provider_protocol::ProviderCreateRequest,
    ) -> Result<provider_protocol::ProviderInstance, provider_protocol::ProviderControlError> {
        let mut instance = self.local.create(request).await?;
        instance
            .capabilities
            .lifecycle
            .insert(provider_protocol::ProviderCapability::Snapshot);
        Ok(instance)
    }

    async fn load(
        &self,
        request: provider_protocol::ProviderLoadRequest,
    ) -> Result<provider_protocol::ProviderInstance, provider_protocol::ProviderControlError> {
        self.create(provider_protocol::ProviderCreateRequest {
            backend_id: request.backend_id,
            owner_ref: request.owner_ref,
            reason: request.reason,
            resource_limits: request.resource_limits,
            provider_options: serde_json::json!({
                "workspace_root": std::env::current_dir().unwrap().to_string_lossy()
            }),
            correlation: request.correlation,
        })
        .await
    }

    async fn pause(
        &self,
        request: provider_protocol::ProviderPauseRequest,
    ) -> Result<provider_protocol::ProviderSnapshot, provider_protocol::ProviderControlError> {
        self.local.pause(request).await
    }

    async fn checkpoint(
        &self,
        request: provider_protocol::ProviderCheckpointRequest,
    ) -> Result<provider_protocol::ProviderSnapshot, provider_protocol::ProviderControlError> {
        Ok(provider_protocol::ProviderSnapshot {
            snapshot_id: provider_protocol::ProviderSnapshotId(format!(
                "{}-snapshot",
                request.instance_id
            )),
            provider: provider_protocol::ProviderKind("local".into()),
            source_instance_id: Some(request.instance_id),
            serialized_handle: None,
            metadata: serde_json::Value::Null,
            created_at_ms: 1,
        })
    }

    async fn delete(
        &self,
        request: provider_protocol::ProviderDeleteRequest,
    ) -> Result<provider_protocol::ProviderDeleteOutcome, provider_protocol::ProviderControlError>
    {
        if let Some(snapshot_id) = request.snapshot_id {
            if self.fail_snapshot_delete {
                return Err(provider_protocol::ProviderControlError::Transport {
                    message: "snapshot delete failed".into(),
                });
            }
            return Ok(provider_protocol::ProviderDeleteOutcome {
                backend_id: request.backend_id,
                provider: provider_protocol::ProviderKind("local".into()),
                instance_id: None,
                deleted: true,
                retained_snapshots: Vec::new(),
                deleted_snapshots: vec![snapshot_id],
                correlation: request.correlation,
            });
        }
        self.local.delete(request).await
    }

    async fn inspect(
        &self,
        request: provider_protocol::ProviderInspectRequest,
    ) -> Result<provider_protocol::ProviderInstanceStatus, provider_protocol::ProviderControlError>
    {
        self.local.inspect(request).await
    }

    async fn list_instances(
        &self,
    ) -> Result<Vec<provider_protocol::ProviderInstance>, provider_protocol::ProviderControlError>
    {
        self.local.list_instances().await
    }
}

fn snapshot_provider_managers(fail_snapshot_delete: bool) -> HashMap<String, Arc<InstanceManager>> {
    use backend::{OperationAttach, ProviderInstanceLedger, SqliteProviderInstanceLedger};
    use provider_protocol::{ProviderKind, ProviderLifecycle};
    use xgovernor_manager::InstanceManagerConfig;

    let local = Arc::new(backend::local::LocalProvider::new());
    let lifecycle: Arc<dyn ProviderLifecycle> = Arc::new(SnapshotLifecycle {
        local: Arc::clone(&local),
        fail_snapshot_delete,
    });
    let attach: Arc<dyn OperationAttach> = local;
    let ledger: Arc<dyn ProviderInstanceLedger> = Arc::new(
        SqliteProviderInstanceLedger::open_in_memory().expect("open test provider ledger"),
    );
    [(
        "local".to_string(),
        Arc::new(InstanceManager::new(
            lifecycle,
            attach,
            ledger,
            ProviderKind("local".into()),
            InstanceManagerConfig::new(100, 100),
        )),
    )]
    .into_iter()
    .collect()
}

async fn start_test_provider(providers: &HashMap<String, Arc<InstanceManager>>, runtime_id: &str) {
    providers["local"]
        .start_instance(
            runtime_id.into(),
            provider_protocol::BackendId("local".into()),
            "test".into(),
            serde_json::json!({
                "workspace_root": std::env::current_dir().unwrap().to_string_lossy()
            }),
        )
        .await
        .expect("start test provider instance");
}

fn test_provider_managers() -> HashMap<String, Arc<InstanceManager>> {
    use backend::local::LocalProvider;
    use backend::{OperationAttach, ProviderInstanceLedger, SqliteProviderInstanceLedger};
    use provider_protocol::{ProviderKind, ProviderLifecycle};
    use xgovernor_manager::InstanceManagerConfig;

    let provider = Arc::new(LocalProvider::new());
    let lifecycle: Arc<dyn ProviderLifecycle> = provider.clone();
    let attach: Arc<dyn OperationAttach> = provider;
    let ledger: Arc<dyn ProviderInstanceLedger> = Arc::new(
        SqliteProviderInstanceLedger::open_in_memory().expect("open test provider ledger"),
    );
    [(
        "local".to_string(),
        Arc::new(InstanceManager::new(
            lifecycle,
            attach,
            ledger,
            ProviderKind("local".into()),
            InstanceManagerConfig::new(100, 100),
        )),
    )]
    .into_iter()
    .collect()
}

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
    async fn get(&self, _runtime_id: &str) -> Result<Option<SessionRecord>, SessionDomainError> {
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
        status: xgovernor_core::SessionStatus::Idle,
        created_at_ms: 1,
        updated_at_ms: 1,
        workspace: xgovernor_core::WorkspaceFacts {
            workspace_id: "workspace-1".into(),
            root: ".".into(),
            access: xgovernor_core::WorkspaceAccess::ReadWrite,
            revision: None,
            metadata: serde_json::Value::Null,
        },
        isolation: xgovernor_core::IsolationFacts {
            boundary: xgovernor_core::IsolationBoundary::Host,
            workspace_access: xgovernor_core::WorkspaceAccess::ReadWrite,
            network: xgovernor_core::NetworkIsolation::None,
            metadata: serde_json::json!({"backend_id": "local"}),
        },
        capabilities: xgovernor_core::EffectiveCapabilities {
            sandbox: Default::default(),
            runtime: [
                xgovernor_core::RuntimeCapability::ModelOverride,
                xgovernor_core::RuntimeCapability::ReasoningControl,
                xgovernor_core::RuntimeCapability::Interaction,
            ]
            .into_iter()
            .collect(),
        },
        runtime: xgovernor_core::OpaqueRuntimeState {
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
            workspace: xgovernor_core::WorkspaceFacts {
                workspace_id: "workspace-normalized".into(),
                root: std::env::current_dir()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                access: xgovernor_core::WorkspaceAccess::ReadWrite,
                revision: None,
                metadata: serde_json::Value::Null,
            },
            isolation: xgovernor_core::IsolationFacts {
                boundary: xgovernor_core::IsolationBoundary::Container,
                workspace_access: xgovernor_core::WorkspaceAccess::ReadWrite,
                network: xgovernor_core::NetworkIsolation::Restricted,
                metadata: serde_json::Value::Null,
            },
            sandbox_capabilities: [xgovernor_core::SandboxCapability::Exec]
                .into_iter()
                .collect(),
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
            workspace: xgovernor_core::WorkspaceFacts {
                workspace_id: "workspace-buggy".into(),
                root: std::env::current_dir()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                access: xgovernor_core::WorkspaceAccess::ReadWrite,
                revision: None,
                metadata: serde_json::Value::Null,
            },
            isolation: xgovernor_core::IsolationFacts {
                boundary: xgovernor_core::IsolationBoundary::Host,
                workspace_access: xgovernor_core::WorkspaceAccess::ReadWrite,
                network: xgovernor_core::NetworkIsolation::None,
                metadata: serde_json::Value::Null,
            },
            sandbox_capabilities: [xgovernor_core::SandboxCapability::Exec]
                .into_iter()
                .collect(),
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
}

#[async_trait]
impl AgentRuntime for CompletingRuntime {
    fn runtime_kind(&self) -> &str {
        "test"
    }

    fn capabilities(&self) -> BTreeSet<agent_runtime_protocol::RuntimeCapability> {
        BTreeSet::new()
    }

    async fn start(
        &self,
        request: RuntimeStartRequest,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        *self.started.lock().await = Some(request);
        Ok(())
    }

    async fn stop(&self, _runtime_id: &str) -> Result<(), RuntimeError> {
        *self.stopped.lock().await = true;
        Ok(())
    }

    async fn attach(
        &self,
        _runtime_id: &str,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn check_alive(&self, _runtime_id: &str) -> Result<bool, RuntimeError> {
        Ok(true)
    }

    async fn submit_turn(
        &self,
        input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, RuntimeError> {
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
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn cancel(&self, _request: RuntimeCancelRequest) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[tokio::test]
async fn receipt_and_stream_share_the_server_assigned_turn_id() {
    let application = SessionApplication::new(
        Arc::new(CompletingRuntime::default()),
        test_provider_managers(),
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
        test_provider_managers(),
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
    assert_eq!(response.workspace.workspace_id, "workspace-normalized");
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
    impl AgentRuntime for XiaooTestRuntime {
        fn runtime_kind(&self) -> &str {
            "xiaoo"
        }
        fn capabilities(&self) -> BTreeSet<agent_runtime_protocol::RuntimeCapability> {
            BTreeSet::new()
        }
        async fn start(
            &self,
            request: RuntimeStartRequest,
            context: RuntimeExecutionContext,
        ) -> Result<(), RuntimeError> {
            self.0.start(request, context).await
        }
        async fn stop(&self, runtime_id: &str) -> Result<(), RuntimeError> {
            self.0.stop(runtime_id).await
        }
        async fn attach(
            &self,
            runtime_id: &str,
            context: RuntimeExecutionContext,
        ) -> Result<(), RuntimeError> {
            self.0.attach(runtime_id, context).await
        }
        async fn check_alive(&self, _runtime_id: &str) -> Result<bool, RuntimeError> {
            Ok(true)
        }
        async fn submit_turn(
            &self,
            input: RuntimeTurnInput,
        ) -> Result<RuntimeEventReceiver, RuntimeError> {
            self.0.submit_turn(input).await
        }
        async fn answer_interaction(
            &self,
            input: RuntimeInteractionInput,
        ) -> Result<(), RuntimeError> {
            self.0.answer_interaction(input).await
        }
        async fn cancel(&self, request: RuntimeCancelRequest) -> Result<(), RuntimeError> {
            self.0.cancel(request).await
        }
    }

    let pi = Arc::new(CompletingRuntime::default());
    let xiaoo = Arc::new(XiaooTestRuntime::default());
    let application = SessionApplication::with_runtime_registry(
        "test",
        [
            RuntimeRegistration::new(
                pi.clone(),
                test_provider_managers(),
                Arc::new(TestEnvironment),
            ),
            RuntimeRegistration::new(
                xiaoo.clone(),
                test_provider_managers(),
                Arc::new(TestEnvironment),
            ),
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
            RuntimeRegistration::new(pi, test_provider_managers(), Arc::new(TestEnvironment)),
            RuntimeRegistration::new(
                xiaoo.clone(),
                test_provider_managers(),
                Arc::new(TestEnvironment),
            ),
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
        ext: [(
            "test".to_string(),
            serde_json::json!({"backend_id": "local"}),
        )]
        .into_iter()
        .collect(),
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
        test_provider_managers(),
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
        test_provider_managers(),
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
        test_provider_managers(),
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
        test_provider_managers(),
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
        test_provider_managers(),
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

/// An `AgentRuntime` that overrides `export_state` (unlike
/// `CompletingRuntime`), so `fork`'s happy path can be exercised without
/// stubbing out the entire adapter surface.
#[derive(Default)]
struct ForkableRuntime {
    started: Mutex<Option<RuntimeStartRequest>>,
}

#[async_trait]
impl AgentRuntime for ForkableRuntime {
    fn runtime_kind(&self) -> &str {
        "test"
    }

    fn capabilities(&self) -> BTreeSet<agent_runtime_protocol::RuntimeCapability> {
        BTreeSet::new()
    }

    async fn start(
        &self,
        request: RuntimeStartRequest,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        *self.started.lock().await = Some(request);
        Ok(())
    }

    async fn stop(&self, _runtime_id: &str) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn attach(
        &self,
        _runtime_id: &str,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
    async fn check_alive(&self, _runtime_id: &str) -> Result<bool, RuntimeError> {
        Ok(true)
    }

    async fn submit_turn(
        &self,
        _input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, RuntimeError> {
        unreachable!("the fork tests never submit a turn")
    }

    async fn answer_interaction(
        &self,
        _input: RuntimeInteractionInput,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn cancel(&self, _request: RuntimeCancelRequest) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn export_state(&self, _runtime_id: &str) -> Result<OpaqueRuntimeState, RuntimeError> {
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
        test_provider_managers(),
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
        test_provider_managers(),
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
        test_provider_managers(),
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
        xgovernor_core::SessionStatus::Closed,
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
    let lease_table = Arc::new(SessionLeaseTable::with_stale_threshold_ms(0));
    lease_table
        .acquire("runtime-1", "client-a", None, None)
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    let application = SessionApplication::new(
        Arc::new(CompletingRuntime::default()),
        test_provider_managers(),
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
        test_provider_managers(),
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
        test_provider_managers(),
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
        test_provider_managers(),
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
        test_provider_managers(),
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
        test_provider_managers(),
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
async fn fork_fails_with_unsupported_capability_when_the_adapter_has_not_implemented_export_state()
{
    let application = SessionApplication::new(
        Arc::new(CompletingRuntime::default()),
        test_provider_managers(),
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
    let providers = snapshot_provider_managers(false);
    start_test_provider(&providers, "runtime-1").await;
    let application = SessionApplication::new(
        runtime.clone(),
        providers,
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
    turn_sender: Mutex<Option<mpsc::Sender<RuntimeEvent>>>,
}

#[async_trait]
impl AgentRuntime for HoldingRuntime {
    fn runtime_kind(&self) -> &str {
        "test"
    }

    fn capabilities(&self) -> BTreeSet<agent_runtime_protocol::RuntimeCapability> {
        BTreeSet::new()
    }

    async fn start(
        &self,
        _request: RuntimeStartRequest,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn stop(&self, _runtime_id: &str) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn attach(
        &self,
        _runtime_id: &str,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
    async fn check_alive(&self, _runtime_id: &str) -> Result<bool, RuntimeError> {
        Ok(true)
    }

    async fn submit_turn(
        &self,
        _input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, RuntimeError> {
        let (tx, rx) = mpsc::channel(4);
        *self.turn_sender.lock().await = Some(tx);
        Ok(rx)
    }

    async fn answer_interaction(
        &self,
        _input: RuntimeInteractionInput,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn cancel(&self, _request: RuntimeCancelRequest) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[tokio::test]
async fn second_submit_while_a_turn_is_active_is_rejected_with_conflict() {
    let runtime = Arc::new(HoldingRuntime::default());
    let application = SessionApplication::new(
        runtime.clone(),
        test_provider_managers(),
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
        .send(RuntimeEvent::Completed {
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
impl AgentRuntime for CountingRuntime {
    fn runtime_kind(&self) -> &str {
        "test"
    }

    fn capabilities(&self) -> BTreeSet<agent_runtime_protocol::RuntimeCapability> {
        BTreeSet::new()
    }

    async fn start(
        &self,
        _request: RuntimeStartRequest,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn stop(&self, _runtime_id: &str) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn attach(
        &self,
        _runtime_id: &str,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
    async fn check_alive(&self, _runtime_id: &str) -> Result<bool, RuntimeError> {
        Ok(true)
    }

    async fn submit_turn(
        &self,
        _input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, RuntimeError> {
        self.submissions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn cancel(&self, _request: RuntimeCancelRequest) -> Result<(), RuntimeError> {
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
        test_provider_managers(),
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
impl AgentRuntime for VanishingRuntime {
    fn runtime_kind(&self) -> &str {
        "test"
    }

    fn capabilities(&self) -> BTreeSet<agent_runtime_protocol::RuntimeCapability> {
        BTreeSet::new()
    }

    async fn start(
        &self,
        _request: RuntimeStartRequest,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn stop(&self, _runtime_id: &str) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn attach(
        &self,
        _runtime_id: &str,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
    async fn check_alive(&self, _runtime_id: &str) -> Result<bool, RuntimeError> {
        Ok(true)
    }

    async fn submit_turn(
        &self,
        _input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, RuntimeError> {
        let (tx, rx) = mpsc::channel(1);
        drop(tx);
        Ok(rx)
    }

    async fn answer_interaction(
        &self,
        _input: RuntimeInteractionInput,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn cancel(&self, _request: RuntimeCancelRequest) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[tokio::test]
async fn gate_is_released_after_a_stream_closes_without_a_terminal_event() {
    let application = SessionApplication::new(
        Arc::new(VanishingRuntime),
        test_provider_managers(),
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
impl AgentRuntime for FirstStartFailsRuntime {
    fn runtime_kind(&self) -> &str {
        "test"
    }

    fn capabilities(&self) -> BTreeSet<agent_runtime_protocol::RuntimeCapability> {
        BTreeSet::new()
    }

    async fn start(
        &self,
        _request: RuntimeStartRequest,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        let mut already_failed = self.already_failed.lock().await;
        if !*already_failed {
            *already_failed = true;
            return Err(RuntimeError::Internal {
                message: "boom".into(),
            });
        }
        Ok(())
    }

    async fn stop(&self, _runtime_id: &str) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn attach(
        &self,
        _runtime_id: &str,
        _context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
    async fn check_alive(&self, _runtime_id: &str) -> Result<bool, RuntimeError> {
        Ok(true)
    }

    async fn submit_turn(
        &self,
        _input: RuntimeTurnInput,
    ) -> Result<RuntimeEventReceiver, RuntimeError> {
        unreachable!("this test never submits a turn")
    }

    async fn answer_interaction(
        &self,
        _input: RuntimeInteractionInput,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn cancel(&self, _request: RuntimeCancelRequest) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[tokio::test]
async fn open_rejects_a_second_session_once_tenant_max_sessions_is_reached() {
    let repository = Arc::new(MemoryRepository::default());
    let application = SessionApplication::new(
        Arc::new(CompletingRuntime::default()),
        test_provider_managers(),
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
        test_provider_managers(),
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
        test_provider_managers(),
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

fn test_checkpoint_record() -> xgovernor_core::CheckpointRecord {
    let record = tenant_test_record();
    xgovernor_core::CheckpointRecord {
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

fn checkpoint_manager_record() -> xgovernor_core::CheckpointRecord {
    let mut record = test_checkpoint_record();
    record.isolation.metadata = serde_json::json!({"backend_id": "local"});
    record
}

#[tokio::test]
async fn tenant_can_delete_own_checkpoint_and_metadata() {
    let repository = Arc::new(xgovernor_core::SqliteSessionRepository::open_in_memory().unwrap());
    repository
        .save_checkpoint(checkpoint_manager_record())
        .await
        .unwrap();
    let application = SessionApplication::new(
        Arc::new(CompletingRuntime::default()),
        snapshot_provider_managers(false),
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
    let repository = Arc::new(xgovernor_core::SqliteSessionRepository::open_in_memory().unwrap());
    repository
        .save_checkpoint(checkpoint_manager_record())
        .await
        .unwrap();
    let application = SessionApplication::new(
        Arc::new(CompletingRuntime {
            ..Default::default()
        }),
        snapshot_provider_managers(true),
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
        test_provider_managers(),
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
        test_provider_managers(),
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
        test_provider_managers(),
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
    let providers = snapshot_provider_managers(false);
    start_test_provider(&providers, "runtime-1").await;
    let application = SessionApplication::new(
        Arc::new(ForkableRuntime::default()),
        providers,
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
        test_provider_managers(),
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
        test_provider_managers(),
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
        test_provider_managers(),
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

#[tokio::test]
async fn external_operations_roundtrip_bytes_and_enforce_identity_lease_and_capabilities() {
    use base64::Engine;
    let dir = tempfile::tempdir().unwrap();
    let providers = test_provider_managers();
    providers["local"]
        .start_instance(
            "runtime-1".into(),
            provider_protocol::BackendId("local".into()),
            "test".into(),
            serde_json::json!({"workspace_root":dir.path()}),
        )
        .await
        .unwrap();
    let mut record = test_record();
    record.tenant_id = Some("tenant-a".into());
    record.capabilities.sandbox = [
        SandboxCapability::Exec,
        SandboxCapability::FileRead,
        SandboxCapability::FileWrite,
    ]
    .into_iter()
    .collect();
    let repository = Arc::new(MemoryRepository(Mutex::new(Some(record))));
    let leases = Arc::new(SessionLeaseTable::new());
    leases.acquire("runtime-1", "owner", None, None).await;
    let app = SessionApplication::new(
        Arc::new(CompletingRuntime::default()),
        providers,
        repository.clone(),
        Arc::new(FixedTurnId),
        Arc::new(FixedTurnId),
        Arc::new(UnusedEnvironment),
        Arc::new(FixedTurnId),
    )
    .with_lease_table(leases);
    let lease = SessionLeaseClaim {
        client_id: Some("owner".into()),
        ..Default::default()
    };
    let write = SessionFileWriteRequest {
        runtime_id: "runtime-1".into(),
        path: "nested/data.bin".into(),
        content_base64: base64::engine::general_purpose::STANDARD.encode([0, 1, 255, 128]),
        create_parents: true,
        lease: lease.clone(),
    };
    assert!(matches!(
        app.write_file(&SecurityContext::tenant("tenant-b", "other"), write.clone())
            .await,
        Err(SessionDomainError::NotFound { .. })
    ));
    let mut wrong = write.clone();
    wrong.lease.client_id = Some("intruder".into());
    assert!(matches!(
        app.write_file(&tenant_ctx(), wrong).await,
        Err(SessionDomainError::LeaseConflict { .. })
    ));
    let result = app.write_file(&tenant_ctx(), write.clone()).await.unwrap();
    assert_eq!(result.bytes_written, 4);
    let read = app
        .read_file(
            &tenant_ctx(),
            SessionFileReadRequest {
                runtime_id: "runtime-1".into(),
                path: write.path.clone(),
                lease: lease.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(read.content_base64, write.content_base64);
    let output = app
        .exec(
            &tenant_ctx(),
            SessionExecRequest {
                runtime_id: "runtime-1".into(),
                command: vec![
                    "sh".into(),
                    "-c".into(),
                    "printf hello; printf error >&2; exit 7".into(),
                ],
                cwd: None,
                env: Default::default(),
                timeout_ms: Some(1000),
                lease: lease.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(output.stdout, "hello");
    assert_eq!(output.stderr, "error");
    assert_eq!(output.exit_code, Some(7));
    let mut invalid = write.clone();
    invalid.content_base64 = "!!!".into();
    assert!(matches!(
        app.write_file(&tenant_ctx(), invalid).await,
        Err(SessionDomainError::InvalidRequest { .. })
    ));
    app.write_file(&tenant_ctx(), write.clone()).await.unwrap(); // error released the operation claim
    repository
        .0
        .lock()
        .await
        .as_mut()
        .unwrap()
        .capabilities
        .sandbox
        .remove(&SandboxCapability::FileWrite);
    assert!(matches!(
        app.write_file(&tenant_ctx(), write).await,
        Err(SessionDomainError::UnsupportedCapability { .. })
    ));
}

#[tokio::test]
async fn active_turn_blocks_external_operations_and_checkpoint() {
    let mut record = test_record();
    record.capabilities.sandbox = [SandboxCapability::FileWrite, SandboxCapability::Snapshot]
        .into_iter()
        .collect();
    record
        .capabilities
        .runtime
        .insert(RuntimeCapability::Checkpoint);
    let runtime = Arc::new(HoldingRuntime {
        turn_sender: Mutex::new(None),
    });
    let app = SessionApplication::new(
        runtime,
        test_provider_managers(),
        Arc::new(MemoryRepository(Mutex::new(Some(record)))),
        Arc::new(FixedTurnId),
        Arc::new(FixedTurnId),
        Arc::new(UnusedEnvironment),
        Arc::new(FixedTurnId),
    );
    app.submit_turn(
        &admin_ctx(),
        SessionTurnRequest {
            runtime_id: "runtime-1".into(),
            text: "hold".into(),
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
    let write = SessionFileWriteRequest {
        runtime_id: "runtime-1".into(),
        path: "file".into(),
        content_base64: "".into(),
        create_parents: false,
        lease: Default::default(),
    };
    assert!(matches!(
        app.write_file(&admin_ctx(), write).await,
        Err(SessionDomainError::Conflict { .. })
    ));
    assert!(matches!(
        app.checkpoint(
            &admin_ctx(),
            SessionCheckpointRequest {
                runtime_id: "runtime-1".into(),
                name: None,
                requested_scope: None,
                lease: Default::default()
            }
        )
        .await,
        Err(SessionDomainError::Conflict { .. })
    ));
}
