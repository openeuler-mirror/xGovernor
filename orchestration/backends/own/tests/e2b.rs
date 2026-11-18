//! Requires a real E2B account, Pi worker and host-only model credentials.
use backend::{e2b::E2bProvider, SqliteProviderInstanceLedger};
use orchestration_backend_own::runtime::E2bEnvironment;
use orchestration_backend_own::OwnBackend;
use orchestration_compat as compat;
use orchestration_contract::execution::*;
use serde_json::json;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use xgovernor_core::*;
use xgovernor_manager::{InstanceManager, InstanceManagerConfig};
use xgovernor_runtime_pi::PiRuntime;
struct Ids;
impl TurnIdGenerator for Ids {
    fn next_turn_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
}
impl RuntimeIdGenerator for Ids {
    fn next_runtime_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
}
impl Clock for Ids {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires real E2B, Pi and model credentials"]
async fn execution_interface_on_e2b() {
    let root =
        PathBuf::from(std::env::var("OWN_E2B_STATE_DIR").expect("run-private state directory"));
    std::fs::create_dir_all(&root).unwrap();
    let db = root.join("sessions.db");
    let repository = Arc::new(SqliteSessionRepository::open(&db).unwrap());
    let provider = Arc::new(E2bProvider::new());
    let manager = Arc::new(InstanceManager::new(
        provider.clone(),
        provider,
        Arc::new(SqliteProviderInstanceLedger::open(&db).unwrap()),
        provider_protocol::ProviderKind("e2b".into()),
        InstanceManagerConfig::new(6, 6),
    ));
    let runtime = Arc::new(
        PiRuntime::new_with_worker(
            root.join("pi"),
            PathBuf::from(std::env::var("OWN_PI_WORKER").unwrap()),
        )
        .unwrap(),
    );
    let leases = Arc::new(SessionLeaseTable::default());
    let app = Arc::new(
        SessionApplication::with_runtime_registry(
            if std::env::var("OWN_AGENT").as_deref() == Ok("opencode") {
                "opencode"
            } else {
                "pi"
            },
            [
                RuntimeRegistration::new(
                    runtime,
                    HashMap::from([("e2b".into(), manager.clone())]),
                    Arc::new(E2bEnvironment),
                ),
                orchestration_backend_own::runtime::opencode_registration(
                    std::env::var("OPENCODE_EXECUTABLE")
                        .unwrap_or_else(|_| "/root/.opencode/bin/opencode".into()),
                    manager.clone(),
                ),
            ],
            repository.clone(),
            Arc::new(Ids),
            Arc::new(Ids),
            Arc::new(Ids),
        )
        .unwrap()
        .with_lease_table(leases),
    );
    let owner = SecurityContext::admin("own-compat");
    let llm:session_protocol::LlmOverrideRequest=serde_json::from_value(json!({"provider":"deepseek","model":std::env::var("DEEPSEEK_LLM_MODEL").unwrap(),"api_base":std::env::var("XGOVERNOR_LLM_API_BASE").unwrap(),"api":"openai-completions","api_key_env":"MODEL_API_KEY","capabilities":{"reasoning":false,"input":["text"],"context_window":32000,"max_tokens":256}})).unwrap();
    let bootstrap:session_protocol::SessionExtensions=serde_json::from_value(json!({"runtime_pi":{"backend_id":"e2b","executable":"/usr/local/bin/pi","tools_enabled":false,"discovery":{"extensions":false,"skills":false,"context_files":false}}})).unwrap();
    let lease: session_protocol::SessionLeaseClaim =
        serde_json::from_value(json!({"client_id":"own-compat"})).unwrap();
    for case in [
        "opencode_restore",
        "prepare_root",
        "renew_run_lease",
        "reclaim_run",
        "export_workspace",
        "snapshot",
        "fork",
        "agent_turn",
        "cancel_job",
        "inspect",
        "exec",
        "file_read",
        "file_write",
        "diff",
        "release",
        "outcome_unknown",
    ] {
        if std::env::var("OWN_COMPAT_CASES")
            .ok()
            .is_some_and(|selected| !selected.split(',').any(|c| c == case))
        {
            continue;
        }
        let opencode = std::env::var("OWN_AGENT").as_deref() == Ok("opencode");
        if case == "opencode_restore" && (!opencode || std::env::var("OWN_COMPAT_CASES").is_err()) {
            continue;
        }
        let mut selected_llm = llm.clone();
        if opencode {
            selected_llm.api = None;
            selected_llm.capabilities = None;
        }
        let source_llm = selected_llm.clone();
        if case == "opencode_restore" {
            selected_llm.api_key_env = Some("RESTORED_MODEL_API_KEY".into());
        }
        let case_llm = (case == "agent_turn" || opencode).then_some(selected_llm);
        let source = uuid::Uuid::new_v4().to_string();
        let context = CallContext {
            plugin_id: "compat".into(),
            run_id: case.into(),
            timeout_ms: 90_000,
        };
        let backend = OwnBackend::open(
            &root.join(format!("{case}.db")),
            app.clone(),
            manager.clone(),
            repository.clone(),
            owner.clone(),
            context.clone(),
            [
                Capability::PrepareRoot,
                Capability::RenewRunLease,
                Capability::ReclaimRun,
                Capability::ExportWorkspace,
                Capability::Snapshot,
                Capability::Fork,
                Capability::AgentTurn,
                Capability::CancelJob,
                Capability::Inspect,
                Capability::Exec,
                Capability::FileRead,
                Capability::FileWrite,
                Capability::Diff,
                Capability::Release,
            ]
            .into(),
            NetworkPolicy::Offline,
            case_llm.clone(),
            bootstrap.clone(),
            lease.clone(),
        )
        .unwrap();
        backend
            .renew_run_lease(RenewRunLeaseRequest {
                context: MutationContext {
                    call: context.clone(),
                    idempotency_key: "initial-lease".into(),
                },
                ttl_ms: 600_000,
            })
            .await
            .unwrap();
        if std::env::var("OWN_SWE_TEMPLATE").is_err() {
            app.open(&owner,serde_json::from_value(json!({"runtime_id":source,"conversation_id":source,"sender_id":"own-compat","llm":if case == "opencode_restore" { Some(source_llm) } else { case_llm },"ext":bootstrap,"lease":lease})).unwrap()).await.unwrap();
        }
        if std::env::var("OWN_SWE_TEMPLATE").is_ok() {
            let image = std::env::var("OWN_SWE_IMAGE").unwrap();
            let template = std::env::var("OWN_SWE_TEMPLATE").unwrap();
            backend.register_image_template(&image, &template).unwrap();
            let root = RootDescription {
                environment: EnvironmentRef::Image(image),
                workspace: WorkspaceSource::Git {
                    repository: "https://github.com/sympy/sympy.git".into(),
                    commit: "a36caf5c74fe654cedc488e8a8a05fad388f8406".into(),
                },
                working_directory: "/testbed".into(),
                network_policy: NetworkPolicy::Offline,
            };
            let prepared = backend
                .prepare_root(PrepareRootRequest {
                    context: MutationContext {
                        call: context.clone(),
                        idempotency_key: "swe-root".into(),
                    },
                    root,
                })
                .await
                .unwrap();
            let branch = backend
                .fork(ForkRequest {
                    context: MutationContext {
                        call: context.clone(),
                        idempotency_key: "swe-fork".into(),
                    },
                    parent_state_id: prepared.state_id,
                    inherit_history: false,
                    network_policy: NetworkPolicy::Offline,
                })
                .await
                .unwrap();
            let job=backend.exec(ExecRequest{context:MutationContext{call:context.clone(),idempotency_key:"swe-check".into()},branch_id:branch.branch_id,argv:vec!["sh".into(),"-c".into(),"test \"$(git rev-parse HEAD)\" = a36caf5c74fe654cedc488e8a8a05fad388f8406 && /opt/miniconda3/envs/testbed/bin/python -c 'import sympy; print(sympy.__version__)' && ! curl --connect-timeout 5 --max-time 8 -sSf https://example.com".into()],cwd:"/testbed".into()}).await.unwrap();
            assert!(matches!(
                backend
                    .wait_job(context.clone(), &job.job_id)
                    .await
                    .unwrap(),
                JobState::Completed(JobCompleted {
                    exit_code: Some(0),
                    ..
                })
            ));
            backend.cleanup().await.unwrap();
            println!("SWE-bench immutable offline root passed");
            return;
        }
        if case == "opencode_restore" {
            let mutation = |key: &str| MutationContext {
                call: context.clone(),
                idempotency_key: key.into(),
            };
            let seed = app.submit_turn(&owner, serde_json::from_value(json!({
                "runtime_id": source, "text": "Remember the exact marker restored-cedar-741. Reply OK.", "lease": lease
            })).unwrap()).await.unwrap();
            let mut events = seed.events.unwrap();
            loop {
                match events.recv().await.unwrap() {
                    session_protocol::SessionEvent::TurnCompleted { .. } => break,
                    session_protocol::SessionEvent::TurnFailed { error, .. } => panic!("{error:?}"),
                    _ => {}
                }
            }
            let state = backend.register_root(&source).await.unwrap();
            backend
                .release(ReleaseRequest {
                    context: mutation("source-release"),
                    branch_id: source,
                })
                .await
                .unwrap();
            // The restored runtime must resolve the current host credential reference.
            std::env::remove_var("MODEL_API_KEY");
            let fork = |key: &str| {
                backend.fork(ForkRequest {
                    context: mutation(key),
                    parent_state_id: state.state_id.clone(),
                    inherit_history: true,
                    network_policy: NetworkPolicy::Offline,
                })
            };
            let (first, second) = tokio::join!(fork("restore-first"), fork("restore-second"));
            let first = first.unwrap();
            let second = second.unwrap();
            assert_ne!(first.branch_id, second.branch_id);
            let job = backend.agent_turn(AgentTurnRequest { context: mutation("restored-turn"), branch_id: first.branch_id.clone(), input: "Use the write tool with path /home/user/workspace/remembered.txt and content equal to the exact marker from our earlier conversation. This is an absolute sandbox path. Do not use shell.".into(), profile: None }).await.unwrap();
            assert!(matches!(
                backend
                    .wait_job(context.clone(), &job.job_id)
                    .await
                    .unwrap(),
                JobState::Completed(_)
            ));
            use base64::Engine;
            let file = backend
                .file_read(FileReadRequest {
                    context: context.clone(),
                    branch_id: first.branch_id,
                    path: "remembered.txt".into(),
                })
                .await
                .unwrap();
            assert_eq!(
                String::from_utf8(
                    base64::engine::general_purpose::STANDARD
                        .decode(file.content_base64)
                        .unwrap()
                )
                .unwrap()
                .trim(),
                "restored-cedar-741"
            );
            assert!(backend
                .file_read(FileReadRequest {
                    context: context.clone(),
                    branch_id: second.branch_id,
                    path: "remembered.txt".into()
                })
                .await
                .is_err());
            backend.cleanup().await.unwrap();
            println!("OpenCode nonempty history, new host credential reference and sandbox tool restore passed");
            return;
        }
        let state = backend.register_root(&source).await.unwrap();
        let fixture = compat::Fixture {
            backend: &backend,
            context: context.clone(),
            root_state_id: state.state_id.clone(),
            network_policy: NetworkPolicy::Offline,
            test_path: "compat.txt".into(),
            exec_argv: vec![
                "sh".into(),
                "-c".into(),
                "printf compat-exec; exit 7".into(),
            ],
            cancellable_argv: vec!["sh".into(), "-c".into(), "sleep 120 & wait".into()],
            cwd: "/home/user/workspace".into(),
            turn_input: "Reply exactly: own-backend-ready".into(),
            lease_ttl_ms: 30_000,
            reclaim_wait_ms: 60_000,
        };
        match case {
            "prepare_root" => compat::prepare_root(&fixture).await,
            "renew_run_lease" => compat::renew_run_lease(&fixture).await,
            "reclaim_run" => compat::reclaim_run(&fixture).await,
            "export_workspace" => {
                let export = root.join("exports");
                std::fs::create_dir_all(&export).unwrap();
                compat::export_workspace(&fixture, export.to_str().unwrap()).await;
            }
            "snapshot" => compat::snapshot(&fixture).await,
            "fork" => compat::fork(&fixture).await,
            "agent_turn" => compat::agent_turn(&fixture).await,
            "cancel_job" => compat::cancel_job(&fixture).await,
            "inspect" => compat::inspect(&fixture).await,
            "exec" => compat::exec(&fixture).await,
            "file_read" => compat::file_read(&fixture).await,
            "file_write" => compat::file_write(&fixture).await,
            "diff" => compat::diff(&fixture).await,
            "release" => compat::release(&fixture).await,
            "outcome_unknown" => {
                let bounded = BoundedFork(&backend);
                let fixture = compat::Fixture {
                    backend: &bounded,
                    ..fixture
                };
                compat::outcome_unknown(&fixture).await;
            }
            _ => unreachable!(),
        }
        println!("compatibility case {case}: passed");
        if !matches!(case, "renew_run_lease" | "reclaim_run") {
            backend
                .release(ReleaseRequest {
                    context: MutationContext {
                        call: context.clone(),
                        idempotency_key: "source-release".into(),
                    },
                    branch_id: source.clone(),
                })
                .await
                .unwrap();
        }
        backend.cleanup().await.unwrap();
    }
}

struct BoundedFork<'a>(&'a OwnBackend);
#[async_trait::async_trait]
impl ExecutionBackend for BoundedFork<'_> {
    fn capabilities(&self) -> BackendCapabilities {
        self.0.capabilities()
    }
    async fn fork(&self, mut request: ForkRequest) -> BackendResult<Branch> {
        request.context.call.timeout_ms = 1;
        self.0.fork(request).await
    }
    async fn snapshot(&self, request: SnapshotRequest) -> BackendResult<Snapshot> {
        self.0.snapshot(request).await
    }
    async fn agent_turn(&self, request: AgentTurnRequest) -> BackendResult<JobReceipt> {
        self.0.agent_turn(request).await
    }
    async fn cancel_job(&self, request: CancelJobRequest) -> BackendResult<CancelJobResponse> {
        self.0.cancel_job(request).await
    }
    async fn inspect(&self, request: InspectRequest) -> BackendResult<Content> {
        self.0.inspect(request).await
    }
    async fn exec(&self, request: ExecRequest) -> BackendResult<JobReceipt> {
        self.0.exec(request).await
    }
    async fn file_read(&self, request: FileReadRequest) -> BackendResult<FileContent> {
        self.0.file_read(request).await
    }
    async fn file_write(&self, request: FileWriteRequest) -> BackendResult<FileWritten> {
        self.0.file_write(request).await
    }
    async fn diff(&self, request: DiffRequest) -> BackendResult<Content> {
        self.0.diff(request).await
    }
    async fn release(&self, request: ReleaseRequest) -> BackendResult<Released> {
        self.0.release(request).await
    }

    async fn wait_job(&self, c: CallContext, job: &str) -> BackendResult<JobState> {
        self.0.wait_job(c, job).await
    }
    async fn reconcile(
        &self,
        c: CallContext,
        op: OperationIdentity,
    ) -> BackendResult<OperationResolution> {
        self.0.reconcile(c, op).await
    }
}
