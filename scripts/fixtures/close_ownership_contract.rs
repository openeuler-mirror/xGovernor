//! Close ownership invariant: rejected close must preserve owner streams.
use super::*;
use agent_runtime_protocol::{
    AgentRuntime, RuntimeCancelRequest, RuntimeCapability, RuntimeError, RuntimeEvent,
    RuntimeEventReceiver, RuntimeExecutionContext,
    RuntimeInteractionRequest as RuntimeInteractionInput, RuntimeStartRequest,
    RuntimeTurnRequest as RuntimeTurnInput,
};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::Request;
use std::collections::BTreeSet;
use tower::ServiceExt;
use xgovernor_core::application::{SessionRepository, TurnIdGenerator};
use xgovernor_core::{
    Clock, NormalizedSessionEnvironment, RuntimeIdGenerator, SessionDomainError,
    SessionEnvironmentNormalizer, SessionListPage, SessionRecord,
};

async fn pending_stream_count(state: &SessionHttpState) -> usize {
    state
        .streams
        .lock()
        .await
        .values()
        .map(|turns| turns.len())
        .sum()
}

struct EmptyRepository;

#[async_trait]
impl SessionRepository for EmptyRepository {
    async fn get(&self, _runtime_id: &str) -> Result<Option<SessionRecord>, SessionDomainError> {
        Ok(Some({
            let mut record = test_record();
            record.tenant_id = Some("tenant-a".into());
            record
        }))
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
            metadata: serde_json::Value::Null,
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
        "turn-http".into()
    }
}

impl RuntimeIdGenerator for FixedTurnId {
    fn next_runtime_id(&self) -> String {
        "runtime-http".into()
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
        unreachable!("HTTP turn test does not open a session")
    }
}

#[derive(Default)]
struct CompletingRuntime {
    stops: std::sync::atomic::AtomicU32,
}

#[async_trait]
impl AgentRuntime for CompletingRuntime {
    fn runtime_kind(&self) -> &str {
        "test"
    }

    fn capabilities(&self) -> BTreeSet<RuntimeCapability> {
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
        self.stops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        tx.send(RuntimeEvent::Completed {
            outcome: session_protocol::SessionTurnOutcome::Complete,
            usage: session_protocol::SessionUsage::default(),
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

async fn rejected_close_preserves_stream(tenant: &str, client: Option<&str>, expected: StatusCode) {
    use super::super::auth::{security_layer, TokenTable};
    use xgovernor_core::session_lease::SessionLeaseTable;
    let runtime = Arc::new(CompletingRuntime::default());
    let leases = Arc::new(SessionLeaseTable::new());
    leases.acquire("runtime-1", "writer-a", None, None).await;
    let state = Arc::new(SessionHttpState::new(
        SessionApplication::new(
            runtime.clone(),
            HashMap::new(),
            Arc::new(EmptyRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        )
        .with_lease_table(leases),
    ));
    let tokens = TokenTable::new(
        [
            ("owner".into(), SecurityContext::tenant("tenant-a", "alice")),
            ("foreign".into(), SecurityContext::tenant("tenant-b", "bob")),
        ]
        .into_iter()
        .collect(),
    );
    let app = security_layer(session_router(state.clone()), Some(tokens));
    let (tx, rx) = mpsc::channel(1);
    assert!(register_stream(&state.streams, "runtime-1", "turn-owner", rx).await);
    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/sessions/close")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {tenant}"))
                .body(Body::from(
                    serde_json::json!({"runtime_id":"runtime-1", "lease":{"client_id":client}})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), expected);
    assert_eq!(
        runtime.stops.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "application rejects the close before touching the runtime"
    );
    assert_eq!(
        pending_stream_count(&state).await,
        1,
        "required ownership invariant: rejected close must preserve owner's unclaimed stream"
    );
    assert!(
        !tx.is_closed(),
        "owner's event channel must remain attached"
    );
    let response = app
        .oneshot(
            Request::get("/api/v1/sessions/runtime-1/turns/turn-owner/events")
                .header("authorization", "Bearer owner")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "owner must still claim its SSE stream"
    );
}

#[tokio::test]
async fn session_contract_foreign_tenant_close_preserves_owner_stream() {
    rejected_close_preserves_stream("foreign", None, StatusCode::NOT_FOUND).await;
}

#[tokio::test]
async fn session_contract_wrong_writer_close_preserves_owner_stream() {
    rejected_close_preserves_stream("owner", Some("writer-b"), StatusCode::CONFLICT).await;
}
