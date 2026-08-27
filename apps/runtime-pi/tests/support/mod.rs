//! Shared fixtures for `xgovernor-runtime-pi`'s integration test binaries
//! (`tests/contract.rs`, `tests/restore.rs`). Lives under `tests/support/`
//! (not directly under `tests/`) so Cargo does not treat it as its own
//! integration test target — see
//! <https://doc.rust-lang.org/cargo/reference/cargo-targets.html#integration-tests>.
//! Both callers drive a deterministic fake `pi --mode rpc` process
//! (`src/bin/fake_pi.rs`, built as the sibling `fake_pi` binary target)
//! rather than the real Pi CLI; see `contract.rs`'s module doc for why.

use agent_runtime_protocol::{
    AgentRuntime, RuntimeEntryContext, RuntimeExecutionContext, RuntimeStartRequest,
};
use async_trait::async_trait;
use backend::local::LocalProvider;
use backend::{OperationAttach, ProviderInstanceLedger, SqliteProviderInstanceLedger};
use provider_protocol::{ProviderKind, ProviderLifecycle};
use serde_json::{json, Value};
use session_protocol::{SessionExtensions, SessionOpenRequest};
use std::collections::HashMap;
use std::sync::Arc as StdArc;
use tempfile::TempDir;
use xgovernor_core::{
    Clock, IsolationBoundary, IsolationFacts, NetworkIsolation, NormalizedSessionEnvironment,
    RuntimeIdGenerator, SecurityContext, SessionApplication, SessionDomainError,
    SessionEnvironmentNormalizer, SessionListPage, SessionRecord, SessionRepository,
    TurnIdGenerator, WorkspaceAccess, WorkspaceFacts,
};
use xgovernor_manager::{InstanceManager, InstanceManagerConfig};
use xgovernor_runtime_pi::{PiRuntime, EXT_NAMESPACE};

/// Path to the `fake_pi` binary this same package builds under
/// `src/bin/fake_pi.rs` — cargo sets this env var at compile time for every
/// integration test target in the package, pointing at the built artifact.
pub fn fake_pi_path() -> String {
    env!("CARGO_BIN_EXE_fake_pi").to_string()
}

pub fn pi_worker_path() -> std::path::PathBuf {
    env!("CARGO_BIN_EXE_pi-worker").into()
}

/// `backend_id` these tests register their sandbox under, matching
/// [`build_managers`]'s single `"local"` entry.
pub const LOCAL_BACKEND_ID: &str = "local";

pub fn pi_runtime_ext() -> SessionExtensions {
    [(
        EXT_NAMESPACE.to_string(),
        json!({ "executable": fake_pi_path(), "backend_id": LOCAL_BACKEND_ID }),
    )]
    .into_iter()
    .collect()
}

/// Builds the `backend_id -> InstanceManager` map every `PiRuntime::new` call
/// in these tests needs, composing a real in-memory-ledger `LocalProvider`
/// under the `"local"` key — mirrors `apps/runtime-mock`'s own test setup
/// (`in_memory_local_manager`), since `PiRuntime` now routes Pi's tool
/// execution through the exact same `InstanceManager`/`OperationBackend`
/// plumbing that adapter uses.
pub fn build_managers() -> HashMap<String, StdArc<InstanceManager>> {
    let local = StdArc::new(LocalProvider::new());
    let lifecycle: StdArc<dyn ProviderLifecycle> = local.clone();
    let attach: StdArc<dyn OperationAttach> = local;
    let ledger: StdArc<dyn ProviderInstanceLedger> = StdArc::new(
        SqliteProviderInstanceLedger::open_in_memory().expect("open in-memory provider ledger"),
    );
    let manager = StdArc::new(InstanceManager::new(
        lifecycle,
        attach,
        ledger,
        ProviderKind(LOCAL_BACKEND_ID.to_string()),
        InstanceManagerConfig::new(20, 1024),
    ));
    [(LOCAL_BACKEND_ID.to_string(), manager)]
        .into_iter()
        .collect()
}

pub fn new_pi_runtime() -> PiRuntime {
    let session_root = TempDir::new()
        .expect("tempdir for pi session-dir root")
        .keep();
    PiRuntime::new_with_worker(session_root, pi_worker_path())
        .expect("bridge http listener must bind")
}

pub fn workspace_facts(root: &str) -> WorkspaceFacts {
    WorkspaceFacts {
        workspace_id: "workspace-1".into(),
        root: root.into(),
        access: WorkspaceAccess::ReadWrite,
        revision: None,
        metadata: Value::Null,
    }
}

pub async fn runtime_context(root: &str) -> RuntimeExecutionContext {
    let root = std::fs::canonicalize(root)
        .expect("workspace root must exist")
        .to_string_lossy()
        .into_owned();
    let managers = build_managers();
    let runtime_id = format!("direct-test-{}", uuid::Uuid::new_v4());
    let backend = managers[LOCAL_BACKEND_ID]
        .start_instance(
            runtime_id,
            provider_protocol::BackendId(LOCAL_BACKEND_ID.into()),
            "admin".into(),
            json!({"workspace_root": root}),
        )
        .await
        .expect("provider start must succeed");
    RuntimeExecutionContext {
        operation_backend: backend,
    }
}

/// Real in-memory persistence (unlike a no-op stub) so that `submit_turn`'s
/// `require_session` lookup after `open()` actually finds the record
/// `open()` saved.
#[derive(Default)]
pub struct MemoryRepository(pub std::sync::Mutex<Option<SessionRecord>>);

#[async_trait]
impl SessionRepository for MemoryRepository {
    async fn get(&self, runtime_id: &str) -> Result<Option<SessionRecord>, SessionDomainError> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .clone()
            .filter(|record| record.runtime_id == runtime_id))
    }
    async fn save(&self, record: SessionRecord) -> Result<(), SessionDomainError> {
        *self.0.lock().unwrap() = Some(record);
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

pub struct FixedIds;
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

pub struct TempWorkspaceEnvironment {
    pub root: String,
}

#[async_trait]
impl SessionEnvironmentNormalizer for TempWorkspaceEnvironment {
    async fn normalize(
        &self,
        _ctx: &SecurityContext,
        _request: &SessionOpenRequest,
    ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
        Ok(NormalizedSessionEnvironment {
            workspace: workspace_facts(&self.root),
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

/// Returns the built `SessionApplication` alongside the `MemoryRepository`
/// backing it, so tests can inspect exactly what `open()` persisted into
/// `SessionRecord.runtime` (`docs/pi_session_restore_plan.md` §1) — the
/// application only takes `Arc<dyn SessionRepository>`, so this handle has to
/// be kept on the side rather than fetched back out of `SessionApplication`.
pub fn application(root: String) -> (SessionApplication, StdArc<MemoryRepository>) {
    let repository = StdArc::new(MemoryRepository::default());
    let app = SessionApplication::new(
        StdArc::new(new_pi_runtime()),
        build_managers(),
        repository.clone(),
        StdArc::new(FixedIds),
        StdArc::new(FixedIds),
        StdArc::new(TempWorkspaceEnvironment { root }),
        StdArc::new(FixedIds),
    );
    (app, repository)
}

/// Same as [`application`], but lets the caller supply an already-built
/// `PiRuntime` (and thus control its `managers`/session-root) instead of a
/// fresh throwaway one from [`new_pi_runtime`] — needed by the restart-
/// simulation tests in `tests/restore.rs`, which must share both the
/// `InstanceManager` map and the on-disk session root across two distinct
/// `PiRuntime` instances (simulating a daemon restart) while still going
/// through `SessionApplication` for the close-path test.
///
/// `#[allow(dead_code)]`: only `tests/restore.rs` calls this; `tests/support`
/// is compiled fresh into every integration-test binary that includes it
/// (`mod support;`), so `tests/contract.rs`'s own binary would otherwise warn
/// on it as unused.
#[allow(dead_code)]
pub fn application_with_runtime(
    runtime: PiRuntime,
    managers: HashMap<String, StdArc<InstanceManager>>,
    repository: StdArc<MemoryRepository>,
    root: String,
) -> SessionApplication {
    SessionApplication::new(
        StdArc::new(runtime),
        managers,
        repository,
        StdArc::new(FixedIds),
        StdArc::new(FixedIds),
        StdArc::new(TempWorkspaceEnvironment { root }),
        StdArc::new(FixedIds),
    )
}

pub fn no_entry() -> RuntimeEntryContext {
    RuntimeEntryContext {
        kind: None,
        instance_id: None,
        message_id: None,
        reply_to_message_id: None,
    }
}

pub async fn started_runtime(workspace_root: &str) -> PiRuntime {
    let runtime = new_pi_runtime();
    start_runtime(
        &runtime,
        RuntimeStartRequest {
            runtime_id: "runtime-1".into(),
            conversation_id: "conversation-1".into(),
            sender_id: "sender-1".into(),
            workspace: workspace_facts(workspace_root),
            state: None,
            llm: None,
            ext: pi_runtime_ext(),
        },
    )
    .await
    .expect("start must succeed against the fake pi process");
    runtime
}

pub async fn start_runtime(
    runtime: &PiRuntime,
    request: RuntimeStartRequest,
) -> Result<(), agent_runtime_protocol::RuntimeError> {
    let managers = build_managers();
    let workspace_root = request.workspace.root.clone();
    let runtime_id = request.runtime_id.clone();
    let backend = managers[LOCAL_BACKEND_ID]
        .start_instance(
            runtime_id,
            provider_protocol::BackendId(LOCAL_BACKEND_ID.into()),
            "admin".into(),
            json!({"workspace_root": workspace_root}),
        )
        .await
        .expect("provider start must succeed");
    runtime
        .start(
            request,
            RuntimeExecutionContext {
                operation_backend: backend,
            },
        )
        .await
}
