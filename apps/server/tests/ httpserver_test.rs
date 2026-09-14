mod tests {
    use super::*;
    use crate::httpserver::session::SessionHttpState;
    use agent_runtime_protocol::{
        AgentRuntime, RuntimeCancelRequest, RuntimeCapability, RuntimeError, RuntimeEventReceiver,
        RuntimeExecutionContext, RuntimeInteractionRequest as RuntimeInteractionInput,
        RuntimeStartRequest, RuntimeTurnRequest as RuntimeTurnInput,
    };
    use async_trait::async_trait;
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::body::Body;
    use axum::http::Request;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use session_protocol::SessionSubmitReceipt;
    use std::collections::BTreeSet;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use tower::ServiceExt;
    use tower::ServiceExt;
    use xgovernor_core::application::{SessionRepository, TurnIdGenerator};
    use xgovernor_core::application::{SessionRepository, TurnIdGenerator};
    use xgovernor_core::{
        Clock, NormalizedSessionEnvironment, RuntimeIdGenerator, SecurityContext,
        SessionApplication, SessionDomainError, SessionEnvironmentNormalizer, SessionListPage,
        SessionRecord,
    };
    use xgovernor_core::{
        Clock, NormalizedSessionEnvironment, RuntimeIdGenerator, SessionDomainError,
        SessionEnvironmentNormalizer, SessionListPage, SessionRecord,
    };

    struct EmptyRepository;

    #[tokio::test(start_paused = true)]
    async fn long_operations_outlive_control_timeout() {
        async fn long_work() -> &'static str {
            tokio::time::sleep(Duration::from_secs(31)).await;
            "done"
        }
        let router = apply_transport_defenses(
            Router::new()
                .route("/api/v1/sessions/exec", axum::routing::post(long_work))
                .route(
                    "/api/v1/sessions/checkpoint",
                    axum::routing::post(long_work),
                )
                .route("/api/v1/sessions/heartbeat", axum::routing::post(long_work)),
        );
        for (path, body, status) in [
            ("exec", r#"{"timeout_ms":900000}"#, StatusCode::OK),
            ("checkpoint", "{}", StatusCode::OK),
            ("heartbeat", "{}", StatusCode::REQUEST_TIMEOUT),
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::post(format!("/api/v1/sessions/{path}"))
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{path}");
        }
    }

    struct EmptyRepository;

    #[async_trait]
    impl SessionRepository for EmptyRepository {
        async fn get(
            &self,
            _runtime_id: &str,
        ) -> Result<Option<SessionRecord>, SessionDomainError> {
            Ok(None)
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

    struct UnusedRuntime;

    #[async_trait]
    impl AgentRuntime for UnusedRuntime {
        fn runtime_kind(&self) -> &str {
            "unused"
        }

        fn capabilities(&self) -> BTreeSet<RuntimeCapability> {
            BTreeSet::new()
        }

        async fn start(
            &self,
            _request: RuntimeStartRequest,
            _context: RuntimeExecutionContext,
        ) -> Result<(), RuntimeError> {
            unreachable!("router tests never drive a real turn")
        }

        async fn stop(&self, _runtime_id: &str) -> Result<(), RuntimeError> {
            unreachable!("router tests never drive a real turn")
        }

        async fn attach(
            &self,
            _runtime_id: &str,
            _context: RuntimeExecutionContext,
        ) -> Result<(), RuntimeError> {
            unreachable!("router tests never drive a real turn")
        }
        async fn check_alive(&self, _runtime_id: &str) -> Result<bool, RuntimeError> {
            Ok(true)
        }

        async fn submit_turn(
            &self,
            _input: RuntimeTurnInput,
        ) -> Result<RuntimeEventReceiver, RuntimeError> {
            unreachable!("router tests never drive a real turn")
        }

        async fn answer_interaction(
            &self,
            _input: RuntimeInteractionInput,
        ) -> Result<(), RuntimeError> {
            unreachable!("router tests never drive a real turn")
        }

        async fn cancel(&self, _request: RuntimeCancelRequest) -> Result<(), RuntimeError> {
            unreachable!("router tests never drive a real turn")
        }
    }

    struct UnusedIds;

    impl TurnIdGenerator for UnusedIds {
        fn next_turn_id(&self) -> String {
            unreachable!("router tests never drive a real turn")
        }
    }

    impl RuntimeIdGenerator for UnusedIds {
        fn next_runtime_id(&self) -> String {
            unreachable!("router tests never drive a real turn")
        }
    }

    impl Clock for UnusedIds {
        fn now_ms(&self) -> u64 {
            0
        }
    }

    struct UnusedEnvironment;

    #[async_trait]
    impl SessionEnvironmentNormalizer for UnusedEnvironment {
        async fn normalize(
            &self,
            _ctx: &SecurityContext,
            _request: &session_protocol::SessionOpenRequest,
        ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
            unreachable!("router tests never drive a real turn")
        }
    }

    fn test_application() -> SessionApplication {
        SessionApplication::new(
            Arc::new(UnusedRuntime),
            std::collections::HashMap::new(),
            Arc::new(EmptyRepository),
            Arc::new(UnusedIds),
            Arc::new(UnusedIds),
            Arc::new(UnusedEnvironment),
            Arc::new(UnusedIds),
        )
    }

    fn test_state() -> SessionHttpState {
        SessionHttpState::new(test_application())
    }

    #[tokio::test]
    async fn health_is_reachable_without_a_token_table() {
        let router = create_router(test_state(), None, None, None);
        let response = router
            .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn a_token_table_gates_every_route_including_health() {
        // Auth is applied to the whole composed router, not just the session
        // routes — a configured token table must also cover /health, so a
        // caller without credentials can't even probe liveness.
        use super::super::auth::TokenTable;
        use std::collections::HashMap;

        let mut entries = HashMap::new();
        entries.insert("root-token".to_string(), SecurityContext::admin("root"));
        let router = create_router(test_state(), Some(TokenTable::new(entries)), None, None);

        let response = router
            .clone()
            .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = router
            .oneshot(
                Request::get("/api/v1/health")
                    .header("authorization", "Bearer root-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn role_gate_none_preserves_the_legacy_no_restriction_behavior() {
        // A tenant token must still reach every route when role_gate is None
        // — this is the single-listener backward-compat path
        // (XGOVERNOR_TENANT_BIND_ADDR unset), unchanged by adding the
        // parameter.
        use super::super::auth::TokenTable;
        use std::collections::HashMap;

        let mut entries = HashMap::new();
        entries.insert(
            "tenant-token".to_string(),
            SecurityContext::tenant("tenant-a", "alice"),
        );
        let router = create_router(test_state(), Some(TokenTable::new(entries)), None, None);

        let response = router
            .oneshot(
                Request::get("/api/v1/health")
                    .header("authorization", "Bearer tenant-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn role_gate_admin_rejects_a_tenant_token_with_403() {
        use super::super::auth::TokenTable;
        use std::collections::HashMap;
        use xgovernor_core::Role;

        let mut entries = HashMap::new();
        entries.insert(
            "tenant-token".to_string(),
            SecurityContext::tenant("tenant-a", "alice"),
        );
        entries.insert("admin-token".to_string(), SecurityContext::admin("root"));
        let router = create_router(
            test_state(),
            Some(TokenTable::new(entries)),
            Some(Role::Admin),
            None,
        );

        let response = router
            .clone()
            .oneshot(
                Request::get("/api/v1/health")
                    .header("authorization", "Bearer tenant-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let response = router
            .oneshot(
                Request::get("/api/v1/health")
                    .header("authorization", "Bearer admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn role_gate_tenant_rejects_the_implicit_admin_fallback_with_403() {
        // The most important case: with no token table at all, every request
        // resolves to implicit admin (today's dev-mode behavior). A tenant
        // listener must still reject it — "no auth configured" must never
        // silently become "admin reachable on the public listener".
        use xgovernor_core::Role;

        let router = create_router(test_state(), None, Some(Role::Tenant), None);
        let response = router
            .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn oversized_request_bodies_are_rejected_before_reaching_the_handler() {
        // A body over MAX_REQUEST_BODY_BYTES must be rejected by the
        // RequestBodyLimitLayer (413) before session::open_session's Json
        // extractor -- and therefore before SessionApplication::open -- ever
        // sees it. Deliberately targets a real session route (not a
        // throwaway one) so this exercises the actual production
        // composition in create_router, not just the guard_with helper.
        let big_body = "x".repeat(MAX_REQUEST_BODY_BYTES + 1);
        let router = create_router(test_state(), None, None, None);
        let response = router
            .oneshot(
                Request::post("/api/v1/sessions/open")
                    .header("content-type", "application/json")
                    .body(Body::from(big_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_requests_are_cut_off_by_the_timeout_layer() {
        // tokio's paused clock lets this assert the real REQUEST_TIMEOUT
        // constant (30s) without the test actually taking 30s of wall-clock
        // time: with nothing else runnable, tokio auto-advances virtual time
        // to the next pending timer, so both the handler's sleep and the
        // TimeoutLayer's internal timer resolve near-instantly here.
        async fn slow_handler() {
            tokio::time::sleep(REQUEST_TIMEOUT + Duration::from_secs(1)).await;
        }

        let router = apply_transport_defenses(Router::new().route("/slow", get(slow_handler)));
        let response = router
            .oneshot(Request::get("/slow").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    }

    #[tokio::test]
    async fn sse_route_is_still_reachable_through_create_router_after_the_split() {
        // Structural regression test for the non_sse_session_router /
        // sse_session_router split: the SSE route must still be wired up
        // (and still fed by the *same* Arc<SessionHttpState>, hence still
        // ownership/lookup-checked) when reached through create_router, not
        // just through session_router directly. No stream was ever
        // registered, so this hits the ordinary "session not found" branch
        // -- proving the route exists and reaches the real handler, not that
        // it 404s for some routing/composition mistake.
        let router = create_router(test_state(), None, None, None);
        let response = router
            .oneshot(
                Request::get("/api/v1/sessions/runtime-x/turns/turn-y/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    use super::*;
    use agent_runtime_protocol::{
        AgentRuntime, RuntimeCancelRequest, RuntimeCapability, RuntimeError, RuntimeEvent,
        RuntimeEventReceiver, RuntimeExecutionContext,
        RuntimeInteractionRequest as RuntimeInteractionInput, RuntimeStartRequest,
        RuntimeTurnRequest as RuntimeTurnInput,
    };

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

    /// A repository whose single record is owned by `tenant_id: "tenant-a"`,
    /// for exercising the cross-tenant 404 path over real HTTP requests
    /// (`security_layer` → `Extension<SecurityContext>` →
    /// `SessionApplication::require_session`'s `ctx.owns(...)` check).
    struct TenantARepository;

    #[async_trait]
    impl SessionRepository for TenantARepository {
        async fn get(
            &self,
            _runtime_id: &str,
        ) -> Result<Option<SessionRecord>, SessionDomainError> {
            let mut record = test_record();
            record.tenant_id = Some("tenant-a".to_string());
            Ok(Some(record))
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

    struct CompletingRuntime;

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

    fn test_router() -> Router {
        let router = session_router(Arc::new(SessionHttpState::new(SessionApplication::new(
            Arc::new(CompletingRuntime),
            std::collections::HashMap::new(),
            Arc::new(EmptyRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        ))));
        // No token table: `security_layer` still injects an implicit admin
        // `SecurityContext` (see `auth.rs`), which every handler below now
        // requires via `Extension<SecurityContext>`.
        super::super::auth::security_layer(router, None)
    }

    /// A router backed by [`TenantARepository`] (single record owned by
    /// `tenant-a`), guarded by a real [`TokenTable`] mapping `t-a`/`t-b` to
    /// distinct tenant `SecurityContext`s and `t-admin` to admin — used to
    /// exercise the actual cross-tenant 404 behavior over HTTP.
    fn test_router_with_tenant_a_record() -> Router {
        use super::super::auth::TokenTable;
        use std::collections::HashMap as StdHashMap;
        use xgovernor_core::SecurityContext;

        let router = session_router(Arc::new(SessionHttpState::new(SessionApplication::new(
            Arc::new(CompletingRuntime),
            std::collections::HashMap::new(),
            Arc::new(TenantARepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        ))));
        let mut entries = StdHashMap::new();
        entries.insert(
            "t-a".to_string(),
            SecurityContext::tenant("tenant-a", "alice"),
        );
        entries.insert(
            "t-b".to_string(),
            SecurityContext::tenant("tenant-b", "bob"),
        );
        entries.insert("t-admin".to_string(), SecurityContext::admin("root"));
        super::super::auth::security_layer(router, Some(TokenTable::new(entries)))
    }

    #[test]
    fn every_terminal_event_has_an_explicit_sse_name() {
        let failed = SessionEvent::TurnFailed {
            runtime_id: "runtime-1".into(),
            turn_id: "turn-1".into(),
            error: session_protocol::SessionTurnFailure {
                code: "failed".into(),
                message: "failed".into(),
                retryable: false,
                details: serde_json::Value::Null,
            },
            usage: session_protocol::SessionUsage::default(),
        };
        assert_eq!(session_event_name(&failed), "turn_failed");
    }

    #[test]
    fn transport_error_uses_protocol_status_and_body() {
        let response = session_error(SessionWireError::NotFound {
            runtime_id: "missing".into(),
        });
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn receipt_type_is_the_protocol_contract() {
        let receipt = SessionSubmitReceipt {
            runtime_id: "runtime-1".into(),
            turn_id: "turn-1".into(),
            accepted_kind: session_protocol::SessionAcceptedInputKind::Turn,
        };
        assert_eq!(receipt.turn_id, "turn-1");
    }

    #[tokio::test]
    async fn http_receipt_links_to_sse_terminal_event() {
        let app = test_router();
        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v1/sessions/turns")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1","text":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let receipt: SessionSubmitReceipt = serde_json::from_slice(&body).unwrap();
        assert_eq!(receipt.turn_id, "turn-http");

        let response = app
            .oneshot(
                Request::get(format!(
                    "/api/v1/sessions/{}/turns/{}/events",
                    receipt.runtime_id, receipt.turn_id
                ))
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("event: turn_completed"));
        assert!(body.contains(r#""runtime_id":"runtime-1""#));
        assert!(body.contains(r#""turn_id":"turn-http""#));
    }

    #[tokio::test]
    async fn close_detach_and_heartbeat_are_reachable_over_http() {
        let app = test_router();

        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v1/sessions/heartbeat")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let heartbeat: session_protocol::SessionHeartbeatResponse =
            serde_json::from_slice(&body).unwrap();
        assert!(
            heartbeat.accepted,
            "no lease table wired: heartbeat is a no-op accept"
        );

        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v1/sessions/detach")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/close")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let control: session_protocol::SessionControlResponse =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(
            control.status,
            session_protocol::SessionLifecycleStatus::Closed
        );
    }

    #[tokio::test]
    async fn external_operation_routes_are_ownership_checked() {
        for (route, body) in [
            ("exec", r#"{"runtime_id":"runtime-1","command":["true"]}"#),
            ("files/read", r#"{"runtime_id":"runtime-1","path":"file"}"#),
            (
                "files/write",
                r#"{"runtime_id":"runtime-1","path":"file","content_base64":""}"#,
            ),
        ] {
            let response = test_router_with_tenant_a_record()
                .oneshot(
                    Request::post(format!("/api/v1/sessions/{route}"))
                        .header("content-type", "application/json")
                        .header("authorization", "Bearer t-b")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            let response = test_router_with_tenant_a_record()
                .oneshot(
                    Request::post(format!("/api/v1/sessions/{route}"))
                        .header("content-type", "application/json")
                        .header("authorization", "Bearer t-a")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        }
    }

    #[tokio::test]
    async fn cancel_accepts_without_requiring_a_lease() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/cancel")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn fork_surfaces_unsupported_capability_when_the_adapter_cannot_export_state() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/fork")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"parent_runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        // CompletingRuntime does not override `export_state`, so the trait's
        // default `UnsupportedCapability` should surface as a 422 over HTTP.
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn no_bearer_token_is_rejected_when_a_token_table_is_configured() {
        let app = test_router_with_tenant_a_record();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/heartbeat")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_foreign_tenant_gets_not_found_not_forbidden_on_someone_elses_session() {
        // `docs/tenancy_design.md` §3: cross-tenant probing must not get a
        // distinguishing echo. tenant-b has a valid token but does not own
        // the tenant-a record `TenantARepository` always returns, so
        // `require_session`'s `ctx.owns(...)` check must fail closed as a
        // plain 404 — never a 403 that would confirm the record exists.
        let app = test_router_with_tenant_a_record();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/heartbeat")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer t-b")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn the_owning_tenant_can_reach_its_own_session() {
        let app = test_router_with_tenant_a_record();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/heartbeat")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer t-a")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_bypasses_tenant_ownership() {
        let app = test_router_with_tenant_a_record();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/heartbeat")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer t-admin")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// `get` always misses — models a fresh deployment with no prior
    /// sessions, so `open()` always takes the create path (not the
    /// re-attach-to-an-existing-record path).
    struct NoRecordRepository;

    #[async_trait]
    impl SessionRepository for NoRecordRepository {
        async fn get(
            &self,
            _runtime_id: &str,
        ) -> Result<Option<SessionRecord>, SessionDomainError> {
            Ok(None)
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

    struct HostOnlyEnvironment;

    #[async_trait]
    impl SessionEnvironmentNormalizer for HostOnlyEnvironment {
        async fn normalize(
            &self,
            ctx: &SecurityContext,
            request: &SessionOpenRequest,
        ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
            xgovernor_core::enforce_workspace_axiom(ctx, &request.workspace, false)?;
            Ok(NormalizedSessionEnvironment {
                workspace: xgovernor_core::WorkspaceFacts {
                    workspace_id: request.conversation_id.clone(),
                    root: "/tmp".into(),
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
                sandbox_capabilities: Default::default(),
                llm: None,
                lease: None,
            })
        }
    }

    fn test_local_provider_managers(
    ) -> std::collections::HashMap<String, Arc<xgovernor_manager::InstanceManager>> {
        use backend::local::LocalProvider;
        use backend::{OperationAttach, ProviderInstanceLedger, SqliteProviderInstanceLedger};
        use provider_protocol::{ProviderKind, ProviderLifecycle};
        use xgovernor_manager::{InstanceManager, InstanceManagerConfig};

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
                InstanceManagerConfig::new(10, 10),
            )),
        )]
        .into_iter()
        .collect()
    }

    /// A router over [`HostOnlyEnvironment`] + [`NoRecordRepository`], guarded
    /// by the same three-token `TokenTable` shape as
    /// [`test_router_with_tenant_a_record`], for exercising `/sessions/open`
    /// admission (§5.4/§0) over real HTTP requests rather than calling
    /// `SessionApplication::open` directly.
    fn test_router_for_open() -> Router {
        use super::super::auth::TokenTable;
        use std::collections::HashMap as StdHashMap;
        use xgovernor_core::SecurityContext;

        let router = session_router(Arc::new(SessionHttpState::new(SessionApplication::new(
            Arc::new(CompletingRuntime),
            test_local_provider_managers(),
            Arc::new(NoRecordRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(HostOnlyEnvironment),
            Arc::new(FixedTurnId),
        ))));
        let mut entries = StdHashMap::new();
        entries.insert(
            "t-a".to_string(),
            SecurityContext::tenant("tenant-a", "alice"),
        );
        entries.insert("t-admin".to_string(), SecurityContext::admin("root"));
        super::super::auth::security_layer(router, Some(TokenTable::new(entries)))
    }

    #[tokio::test]
    async fn tenant_open_with_a_local_path_workspace_is_rejected_over_http() {
        // §0/§5.4 over the real HTTP path, against a host-only provider (the
        // only one this deployment has today): a tenant token requesting a
        // local_path workspace must be rejected — not silently downgraded,
        // not allowed through.
        let app = test_router_for_open();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/open")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer t-a")
                    .body(Body::from(
                        r#"{"conversation_id":"c1","sender_id":"s1","workspace":{"kind":"local_path","path":"/etc"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn admin_open_with_a_local_path_workspace_still_succeeds_over_http() {
        let app = test_router_for_open();
        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/open")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer t-admin")
                    .body(Body::from(
                        r#"{"conversation_id":"c1","sender_id":"s1","workspace":{"kind":"local_path","path":"/etc"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn stream_turn_events_is_ownership_checked_before_touching_the_stream_table() {
        // No stream was ever registered for this runtime/turn pair, so
        // without the ownership guard this would already 404 via the "no
        // receiver in the table" branch — that would make the guard
        // untested. Assert on tenant-a (the owner) instead: a 404 here means
        // the guard let the owner through to the real "not found" branch,
        // not that it wrongly rejected them.
        let app = test_router_with_tenant_a_record();
        let response = app
            .oneshot(
                Request::get("/api/v1/sessions/runtime-1/turns/turn-missing/events")
                    .header("authorization", "Bearer t-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // A foreign tenant must be rejected by the ownership guard itself
        // (same 404, but for a different reason — verified indirectly here
        // since the wire response is deliberately indistinguishable; the
        // `require_session`-level distinction is covered by
        // `a_foreign_tenant_gets_not_found_not_forbidden_on_someone_elses_session`).
        let app = test_router_with_tenant_a_record();
        let response = app
            .oneshot(
                Request::get("/api/v1/sessions/runtime-1/turns/turn-missing/events")
                    .header("authorization", "Bearer t-b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// Like [`test_router`], but also returns the [`Arc<SessionHttpState>`]
    /// so a test can inspect `streams` directly (e.g.
    /// [`SessionHttpState::pending_stream_count`]) instead of only observing
    /// it indirectly through HTTP responses.
    fn test_router_with_state() -> (Router, Arc<SessionHttpState>) {
        let state = Arc::new(SessionHttpState::new(SessionApplication::new(
            Arc::new(CompletingRuntime),
            std::collections::HashMap::new(),
            Arc::new(EmptyRepository),
            Arc::new(FixedTurnId),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        )));
        let router = super::super::auth::security_layer(session_router(state.clone()), None);
        (router, state)
    }

    #[tokio::test]
    async fn close_session_sweeps_any_unclaimed_stream_for_that_runtime_id() {
        let (app, state) = test_router_with_state();

        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v1/sessions/turns")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1","text":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            state.pending_stream_count().await,
            1,
            "submit_turn must register a stream that nobody has attached to yet"
        );

        let response = app
            .oneshot(
                Request::post("/api/v1/sessions/close")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"runtime_id":"runtime-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            state.pending_stream_count().await,
            0,
            "close_session must sweep unclaimed streams for the closed runtime_id, \
             not wait out STREAM_ENTRY_TTL"
        );
    }

    #[tokio::test]
    async fn stream_registration_is_rejected_once_the_global_cap_is_reached() {
        let streams: Mutex<HashMap<String, HashMap<String, StreamEntry>>> =
            Mutex::new(HashMap::new());
        for i in 0..MAX_PENDING_STREAMS {
            let (_tx, rx) = mpsc::channel::<SessionEvent>(1);
            assert!(
                register_stream(&streams, "runtime-x", &format!("turn-{i}"), rx).await,
                "registration {i} must succeed while under the cap"
            );
        }

        let (_tx, rx) = mpsc::channel::<SessionEvent>(1);
        assert!(
            !register_stream(&streams, "runtime-x", "turn-overflow", rx).await,
            "the cap must reject registration once MAX_PENDING_STREAMS is reached"
        );
        assert_eq!(
            streams
                .lock()
                .await
                .values()
                .map(|t| t.len())
                .sum::<usize>(),
            MAX_PENDING_STREAMS,
            "the rejected registration must not have been inserted"
        );
    }

    #[tokio::test]
    async fn sweep_expired_streams_removes_entries_past_the_ttl_and_keeps_fresh_ones() {
        let streams: Mutex<HashMap<String, HashMap<String, StreamEntry>>> =
            Mutex::new(HashMap::new());
        {
            let mut guard = streams.lock().await;
            let (_tx, rx_old) = mpsc::channel::<SessionEvent>(1);
            guard.entry("runtime-1".to_string()).or_default().insert(
                "turn-old".to_string(),
                StreamEntry {
                    receiver: rx_old,
                    // Comfortably past STREAM_ENTRY_TTL. Subtracting from
                    // `Instant::now()` is safe here: on every platform this
                    // test runs on, the monotonic clock's origin is long
                    // before "now minus a few seconds" (e.g. process/boot
                    // time), so this cannot underflow.
                    registered_at: Instant::now() - STREAM_ENTRY_TTL - Duration::from_secs(1),
                },
            );
            let (_tx, rx_fresh) = mpsc::channel::<SessionEvent>(1);
            guard.entry("runtime-1".to_string()).or_default().insert(
                "turn-fresh".to_string(),
                StreamEntry {
                    receiver: rx_fresh,
                    registered_at: Instant::now(),
                },
            );
        }

        sweep_expired_streams(&streams).await;

        let guard = streams.lock().await;
        let remaining: Vec<&str> = guard
            .get("runtime-1")
            .map(|turns| turns.keys().map(String::as_str).collect())
            .unwrap_or_default();
        assert_eq!(
            remaining,
            vec!["turn-fresh"],
            "only the TTL-expired entry should have been swept"
        );
    }

    #[tokio::test]
    async fn sweep_expired_streams_prunes_the_runtime_id_entirely_once_it_has_no_turns_left() {
        let streams: Mutex<HashMap<String, HashMap<String, StreamEntry>>> =
            Mutex::new(HashMap::new());
        {
            let mut guard = streams.lock().await;
            let (_tx, rx) = mpsc::channel::<SessionEvent>(1);
            guard.entry("runtime-1".to_string()).or_default().insert(
                "turn-old".to_string(),
                StreamEntry {
                    receiver: rx,
                    registered_at: Instant::now() - STREAM_ENTRY_TTL - Duration::from_secs(1),
                },
            );
        }

        sweep_expired_streams(&streams).await;

        assert!(
            streams.lock().await.is_empty(),
            "a runtime_id with no remaining turns must not linger as an empty inner map"
        );
    }
}
