use super::*;
use backend::{e2b::E2bProvider, OperationAttach};
use futures_util::FutureExt;
use operation_protocol::capability::filesystem::ReadBytesRequest;
use provider_protocol::*;
use session_protocol::LlmOverrideRequest;

type TestResult = Result<(), Box<dyn std::error::Error>>;
fn checked<T>(r: Result<T, RuntimeError>) -> Result<T, Box<dyn std::error::Error>> {
    r.map_err(|e| format!("{e:?}").into())
}

fn request(id: &str, state: Option<RuntimeStateSnapshot>) -> RuntimeStartRequest {
    RuntimeStartRequest {
        runtime_id: id.into(),
        conversation_id: id.into(),
        sender_id: "operator".into(),
        workspace: RuntimeWorkspace {
            workspace_id: id.into(),
            root: "/home/user/workspace".into(),
            access: RuntimeWorkspaceAccess::ReadWrite,
            revision: None,
            metadata: Value::Null,
        },
        state,
        llm: Some(LlmOverrideRequest {
            provider: Some("openai-compatible".into()),
            model: Some(std::env::var("DEEPSEEK_LLM_MODEL").unwrap()),
            api_base: Some(std::env::var("XGOVERNOR_LLM_API_BASE").unwrap()),
            api_key_env: Some("MODEL_API_KEY".into()),
            ..Default::default()
        }),
        ext: Default::default(),
    }
}

#[tokio::test]
#[ignore = "requires OpenCode 2.3.0, E2B and the configured real model"]
async fn e2b_tools_credentials_and_parallel_restore() -> TestResult {
    let provider = E2bProvider::new();
    let owner = format!("opencode-test-{}", uuid::Uuid::new_v4());
    let source=provider.create(ProviderCreateRequest { backend_id:BackendId("e2b".into()), owner_ref:owner.clone(),reason:ProviderLifecycleReason::Acquire, resource_limits:Default::default(),provider_options:json!({"timeout_secs":600,"allow_internet_access":false,"metadata":{"probe_owner":owner}}),correlation:Value::Null }).await?;
    println!("created {}", source.instance_id.0);
    let runtime = OpenCodeRuntime::new(std::env::var("OPENCODE_EXECUTABLE").unwrap());
    let mut instances = vec![source.clone()];
    let mut checkpoint = None;
    let result=std::panic::AssertUnwindSafe(async {
        let backend=provider.attach(&source).await?;
        checked(runtime.start(request("source",None),RuntimeExecutionContext{operation_backend:backend.clone()}).await)?;
        println!("controlled runtime ready: {OPENCODE_VERSION}");
        let s=checked(runtime.session("source").await)?;
        // This marker exists only in the adapter's host directory.
        let marker=s.root.path().join("host-only.txt");
        tokio::fs::write(&marker,"host-private-marker").await?;
        let text=format!("Use each of these six tools exactly once in this order: write path probe.txt content before; read path probe.txt; edit path probe.txt oldString before newString after; glob pattern probe.txt; grep pattern after path .; shell command `printf sandbox-ok; test ! -e {}; test -z \"$MODEL_API_KEY$E2B_API_KEY$XG_OPERATION_TOKEN$OPENCODE_PASSWORD\"` . No other tools. Then reply routing-done.",marker.display());
        let mut rx=checked(runtime.submit_turn(RuntimeTurnRequest {runtime_id:"source".into(),turn_id:"source-turn".into(),text,entry:Default::default(),llm:None,reasoning_effort:None,ext:Default::default()}).await)?;
        let mut seen=BTreeSet::new();let mut log=String::new();let mut complete=false;
        while let Some(event)=rx.recv().await {
            log.push_str(&serde_json::to_string(&event)?);
            match event {
                RuntimeEvent::ToolActivity{name,status:session_protocol::SessionToolActivityStatus::Succeeded,..}=>{seen.insert(name);},
                RuntimeEvent::Completed{outcome:SessionTurnOutcome::Complete,usage}=>{assert!(usage.total_tokens>0);complete=true;},
                RuntimeEvent::Failed{error,..}=>{
                    return Err(format!("turn failed: {}",error.message).into());
                },
                _=>{}
            }
        }
        assert!(complete);
        assert_eq!(seen,TOOLS.iter().map(|s|s.to_string()).collect());
        assert_eq!(backend.files().read_bytes(ReadBytesRequest{path:operation_protocol::BackendPath("/home/user/workspace/probe.txt".into())}).await?,b"after");
        assert_eq!(tokio::fs::read_to_string(&marker).await?,"host-private-marker");
        let state=checked(runtime.export_state("source").await)?;
        let encoded=serde_json::to_string(&state)?;
        assert!(encoded.contains("sandbox-ok"));
        assert!(encoded.contains("exit=Some(0) timeout=false"));
        for secret in [std::env::var("MODEL_API_KEY")?,std::env::var("E2B_API_KEY")?,s.bridge.token.clone(),s.api.password.clone()] {
            assert!(!encoded.contains(&secret));assert!(!log.contains(&secret));
        }
        // Requests cannot borrow a current binding from another session or replay a retired turn/token.
        let (url,probe_task)=s.bridge.listen().await?;
        for (token,session,turn) in [(s.bridge.token.as_str(),s.bridge.session.read().await.clone(),"source-turn"),(s.bridge.token.as_str(),"other-branch".into(),"next-turn"),("retired-token",s.bridge.session.read().await.clone(),"next-turn")] {
            *s.bridge.turn.write().await=Some("next-turn".into());
            let response=reqwest::Client::new().post(format!("{url}/tool")).bearer_auth(token).json(&json!({"session":session,"turn":turn,"operation":"replay","tool":"shell","args":{"command":"touch forbidden"}})).send().await?;
            assert_eq!(response.status(),reqwest::StatusCode::UNAUTHORIZED);
        }
        *s.bridge.turn.write().await=None;probe_task.abort();
        println!("PASS six tools through E2B; credentials absent; retired and cross-branch calls rejected");
        let snap=provider.checkpoint(ProviderCheckpointRequest{backend_id:BackendId("e2b".into()),instance_id:source.instance_id.clone(),reason:ProviderLifecycleReason::UserRequested,correlation:Value::Null}).await?;
        checkpoint=Some(snap.clone());
        let load=|| provider.load(ProviderLoadRequest {backend_id:BackendId("e2b".into()),owner_ref:owner.clone(),source:ProviderLoadSource::Snapshot(snap.snapshot_id.clone()),reason:ProviderLifecycleReason::Restore,resource_limits:Default::default(),provider_options:json!({"timeout_secs":300,"allow_internet_access":false}),correlation:Value::Null});
        let (a,b)=tokio::join!(load(),load());
        for item in [a,b] {instances.push(item?);}
        let a=provider.attach(&instances[1]).await?;let b=provider.attach(&instances[2]).await?;
        let (ra,rb)=tokio::join!(runtime.start(request("a",Some(state.clone())),RuntimeExecutionContext{operation_backend:a}),runtime.start(request("b",Some(state.clone())),RuntimeExecutionContext{operation_backend:b}));checked(ra)?;checked(rb)?;
        let (a,b)=tokio::join!(runtime.export_state("a"),runtime.export_state("b"));let a=checked(a)?;let b=checked(b)?;
        assert_eq!(a.state["export"]["messages"],state.state["export"]["messages"]);
        assert_eq!(b.state["export"]["messages"],state.state["export"]["messages"]);
        let ca=checked(runtime.continuation("a").await)?;let cb=checked(runtime.continuation("b").await)?;
        assert_ne!(ca.session_id,cb.session_id);assert_ne!(ca.url,cb.url);assert_ne!(ca.password,cb.password);
        assert_ne!(instances[1].instance_id,instances[2].instance_id);
        println!("PASS same nonempty snapshot restored concurrently with independent server, session and sandbox identities");
        Ok::<(),Box<dyn std::error::Error>>(())
    }).catch_unwind().await;
    let result: TestResult =
        result.unwrap_or_else(|_| Err("end-to-end property assertion failed".into()));
    let mut clean = true;
    for id in ["source", "a", "b"] {
        if runtime.check_alive(id).await.unwrap_or(false) {
            if runtime.stop(id).await.is_err() {
                clean = false;
            }
        }
    }
    for instance in instances {
        if provider
            .reclaim_orphan_sandbox(Value::Null, &instance.instance_id.0)
            .await
            .is_err()
        {
            clean = false;
        } else {
            println!("deleted {}", instance.instance_id.0);
        }
    }
    if let Some(snap) = checkpoint {
        if provider
            .delete(ProviderDeleteRequest {
                backend_id: BackendId("e2b".into()),
                instance_id: None,
                snapshot_id: Some(snap.snapshot_id),
                reason: ProviderLifecycleReason::ErrorCleanup,
                correlation: Value::Null,
            })
            .await
            .is_err()
        {
            clean = false;
        }
    }
    assert!(clean, "cleanup incomplete");
    result
}

#[tokio::test]
async fn operation_binding_rejects_stale_and_cross_session_requests() {
    let dir = tempfile::tempdir().unwrap();
    let backend = backend::local::local_backend(dir.path().into(), None, None, None).unwrap();
    let bridge = Bridge::new(
        backend,
        "secret".into(),
        "http://localhost".into(),
        "control-password".into(),
    );
    *bridge.session.write().await = "branch-a".into();
    *bridge.turn.write().await = Some("current".into());
    let (url, task) = bridge.listen().await.unwrap();
    let client = reqwest::Client::new();
    for (token, session, turn) in [
        (bridge.token.as_str(), "branch-a", "retired"),
        (bridge.token.as_str(), "branch-b", "current"),
        ("old-token", "branch-a", "current"),
    ] {
        let response=client.post(format!("{url}/tool")).bearer_auth(token).json(&json!({"session":session,"turn":turn,"operation":"attempt","tool":"write","args":{"path":"forbidden","content":"changed"}})).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
    assert!(!dir.path().join("forbidden").exists());
    task.abort();
}

async fn controlled_session() -> TestResultSession {
    let dir = tempfile::tempdir()?;
    let backend = backend::local::local_backend(dir.path().into(), None, None, None)?;
    let runtime = OpenCodeRuntime::new(std::env::var("OPENCODE_EXECUTABLE")?);
    checked(
        runtime
            .start(
                request("boundary", None),
                RuntimeExecutionContext {
                    operation_backend: backend,
                },
            )
            .await,
    )?;
    let session = checked(runtime.session("boundary").await)?;
    Ok((runtime, session, dir))
}
type TestResultSession =
    Result<(OpenCodeRuntime, Arc<Session>, tempfile::TempDir), Box<dyn std::error::Error>>;

#[tokio::test]
#[ignore = "requires OpenCode 2.3.0 and the configured real model"]
async fn controlled_active_plugins_and_model_tools() -> TestResult {
    let (runtime, s, _dir) = controlled_session().await?;
    let inventory = checked(s.api.get("/api/plugin").await)?;
    checked(validate_plugins(&inventory))?;
    println!("active plugin inventory: {inventory}");
    let mut unexpected = inventory.clone();
    unexpected["data"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id":"opencode.browser","state":{"status":"active"}}));
    assert!(validate_plugins(&unexpected).is_err());
    let mut rx = checked(
        runtime
            .submit_turn(RuntimeTurnRequest {
                runtime_id: "boundary".into(),
                turn_id: "catalog".into(),
                text: "Reply boundary-ok without using tools.".into(),
                entry: Default::default(),
                llm: None,
                reasoning_effort: None,
                ext: Default::default(),
            })
            .await,
    )?;
    let mut complete = false;
    while let Some(event) = rx.recv().await {
        if let RuntimeEvent::Completed {
            outcome: SessionTurnOutcome::Complete,
            ..
        } = event
        {
            complete = true;
        }
        if let RuntimeEvent::Failed { error, .. } = event {
            return Err(error.message.into());
        }
    }
    assert!(complete);
    let names: BTreeSet<_> = s.bridge.model_tools.lock().await.iter().cloned().collect();
    assert_eq!(names, TOOLS.iter().map(|s| s.to_string()).collect());
    println!("actual model request tools: {names:?}; credential boundary passed");
    checked(runtime.stop("boundary").await)?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires OpenCode 2.3.0"]
async fn controlled_pty_is_unreachable_and_paths_are_private() -> TestResult {
    let (runtime, s, _dir) = controlled_session().await?;
    let info = checked(s.api.get("/api/info").await)?;
    checked(s.validate_info(&info).await)?;
    checked(validate_plugins(&checked(s.api.get("/api/plugin").await)?))?;
    assert_eq!(info["capabilities"]["persistentPty"], true);
    let response = s
        .api
        .client
        .get(format!("{}/api/info", s.api.url))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    checked(s.check_logs().await)?;
    println!("authenticated loopback info: {info}; unauthenticated=401; private data/tmp and startup logs checked");
    checked(runtime.stop("boundary").await)?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires OpenCode 2.3.0 and the configured real model"]
async fn turn_step_limit_completes_with_usage() -> TestResult {
    let dir = tempfile::tempdir()?;
    let backend = backend::local::local_backend(dir.path().into(), None, None, None)?;
    let runtime = OpenCodeRuntime::new(std::env::var("OPENCODE_EXECUTABLE")?);
    let mut start = request("bounded", None);
    start
        .ext
        .insert("opencode".into(), json!({"steps":2,"output_tokens":2048}));
    checked(
        runtime
            .start(
                start,
                RuntimeExecutionContext {
                    operation_backend: backend,
                },
            )
            .await,
    )?;
    let result = async {
        let session = checked(runtime.session("bounded").await)?;
        let mut rx = checked(runtime.submit_turn(RuntimeTurnRequest {
            runtime_id: "bounded".into(), turn_id: "bounded-turn".into(),
            text: "Use shell to run `printf step` exactly once per model step. Repeat 100 times, making a separate tool call each time and waiting for its result. Start with the first shell call now. Do not read files or inspect the environment.".into(),
            entry: Default::default(), llm: None, reasoning_effort: None, ext: Default::default(),
        }).await)?;
        let mut terminal = None;
        let mut tools = 0;
        while let Some(event) = rx.recv().await {
            match event {
                RuntimeEvent::Completed { outcome, usage } => terminal = Some((outcome, usage)),
                RuntimeEvent::Failed { error, .. } => return Err(error.message.into()),
                RuntimeEvent::ToolActivity { phase: session_protocol::SessionToolActivityPhase::Begin, .. } => tools += 1,
                _ => {}
            }
        }
        let (outcome, usage) = terminal.ok_or("missing terminal event")?;
        assert_eq!(outcome, SessionTurnOutcome::MaxTurns);
        assert_eq!(tools, 1, "only the first step may run tools");
        assert_eq!(session.bridge.model_requests.load(Ordering::Acquire), 2);
        assert!(usage.total_tokens > 0);
        let state = checked(runtime.export_state("bounded").await)?;
        let assistants: Vec<_> = state.state["export"]["messages"].as_array().unwrap().iter().filter(|m| m["type"] == "assistant").collect();
        assert_eq!(assistants.len(), 2);
        assert!(assistants.iter().all(|m| m["tokens"]["output"].as_u64().unwrap() + m["tokens"]["reasoning"].as_u64().unwrap() <= 2048));
        println!("PASS steps=2 output_tokens=2048 tools={tools} model_requests=2 outcome={outcome:?} usage={usage:?}");
        Ok(())
    }.await;
    checked(runtime.stop("bounded").await)?;
    result
}

#[tokio::test]
async fn disconnected_model_consumer_keeps_provider_usage() -> TestResult {
    use axum::{routing::post, Router};
    use std::future::IntoFuture;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(axum::serve(listener, Router::new().route("/chat/completions", post(|| async {
        let stream = futures_util::stream::unfold(0, |step| async move {
            match step {
                0 => Some((Ok::<_, std::io::Error>("data: {\"choices\":[]}\n\n"), 1)),
                1 => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Some((Ok("data: {\"usage\":{\"prompt_tokens\":71,\"completion_tokens\":9,\"total_tokens\":80}}\n\ndata: [DONE]\n\n"), 2))
                },
                _ => None,
            }
        });
        axum::body::Body::from_stream(stream)
    }))).into_future());
    let dir = tempfile::tempdir()?;
    let backend = backend::local::local_backend(dir.path().into(), None, None, None)?;
    let bridge = Bridge::new(backend, "secret".into(), base, "control".into());
    *bridge.turn.write().await = Some("turn".into());
    let (url, task) = bridge.listen().await?;
    let response = reqwest::Client::new()
        .post(format!("{url}/model/chat/completions"))
        .bearer_auth(&bridge.token)
        .json(&json!({"stream":true}))
        .send()
        .await?;
    drop(response);
    let _settled = bridge.model_activity.write().await;
    assert_eq!(bridge.usage.lock().await.total_tokens, 80);
    server.abort();
    task.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires OpenCode 2.3.0 and the configured real model; runs for three minutes"]
async fn turn_time_budget_keeps_candidate_state_and_usage() -> TestResult {
    let dir = tempfile::tempdir()?;
    let backend = backend::local::local_backend(dir.path().into(), None, None, None)?;
    let runtime = OpenCodeRuntime::new(std::env::var("OPENCODE_EXECUTABLE")?);
    let mut start = request("timed", None);
    start
        .ext
        .insert("opencode".into(), json!({"turn_timeout_s":180}));
    checked(
        runtime
            .start(
                start,
                RuntimeExecutionContext {
                    operation_backend: backend,
                },
            )
            .await,
    )?;
    let started = std::time::Instant::now();
    let result = async {
        let mut rx = checked(runtime.submit_turn(RuntimeTurnRequest {
            runtime_id: "timed".into(), turn_id: "timed-turn".into(),
            text: format!("Use shell with workdir {0} to execute `printf timeout-kept > {0}/candidate.txt; sleep 110` with timeout 120000. Repeat this five times, making separate tool calls and waiting for each result. Do not do any other work or stop early.", dir.path().display()),
            entry: Default::default(), llm: None, reasoning_effort: None, ext: Default::default(),
        }).await)?;
        let mut terminal = None;
        while let Some(event) = rx.recv().await {
            if event.is_terminal() { terminal = Some(event); }
        }
        let terminal = terminal.ok_or("missing terminal")?;
        let session = checked(runtime.session("timed").await)?;
        println!("elapsed={:?} model_requests={} candidate_exists={} terminal={terminal:?}", started.elapsed(), session.bridge.model_requests.load(Ordering::Acquire), dir.path().join("candidate.txt").exists());
        assert!(started.elapsed() >= Duration::from_secs(180));
        assert!(started.elapsed() < Duration::from_secs(310));
        match terminal {
            RuntimeEvent::Completed { outcome, usage } => {
                assert_eq!(outcome, SessionTurnOutcome::BudgetExhausted);
                assert!(usage.total_tokens > 0);
            },
            other => panic!("expected completed time budget, got {other:?}"),
        }
        assert!(checked(runtime.check_alive("timed").await)?);
        let state = checked(runtime.export_state("timed").await)?;
        println!("candidate history={}", state.state["export"]["messages"]);
        assert_eq!(tokio::fs::read_to_string(dir.path().join("candidate.txt")).await?, "timeout-kept");
        assert!(!state.state["export"]["messages"].as_array().unwrap().is_empty());
        assert!(!session.poisoned.load(Ordering::Acquire));
        println!("PASS Completed/BudgetExhausted; actual usage, candidate.txt and exported history retained; session usable");
        Ok(())
    }.await;
    checked(runtime.stop("timed").await)?;
    result
}

#[tokio::test]
#[ignore = "requires OpenCode 2.3.0 and the configured real model"]
async fn restored_session_waits_for_plugin_activation_before_turn() -> TestResult {
    let (runtime, source, _dir) = controlled_session().await?;
    let snapshot = checked(runtime.export_state("boundary").await)?;
    checked(runtime.stop("boundary").await)?;
    let dir = tempfile::tempdir()?;
    let backend = backend::local::local_backend(dir.path().into(), None, None, None)?;
    // Hold setup after its handshake to expose the inventory publication race deterministically.
    let delayed = PLUGIN.replace("await call(\"/ready\", { tools: (await ctx.tool.list()).map((tool) => tool.id) });", "await call(\"/ready\", { tools: (await ctx.tool.list()).map((tool) => tool.id) }); await new Promise(resolve => setTimeout(resolve, 500));");
    assert_ne!(delayed, PLUGIN);
    let restored = checked(
        runtime
            .launch(
                &request("restored", Some(snapshot)),
                RuntimeExecutionContext {
                    operation_backend: backend,
                },
                &delayed,
            )
            .await,
    )?;
    assert_ne!(source.api.url, restored.api.url);
    checked(validate_plugins(&checked(
        restored.api.get("/api/plugin").await,
    )?))?;
    runtime
        .sessions
        .write()
        .await
        .insert("restored".into(), restored.clone());
    let result = async {
        let mut events = checked(runtime.submit_turn(RuntimeTurnRequest {
            runtime_id: "restored".into(), turn_id: "restored-turn".into(),
            text: format!("Use shell with workdir {} to run `printf restored-ok`, then reply done. No other tools.",dir.path().display()),
            entry: Default::default(), llm: None, reasoning_effort: None, ext: Default::default(),
        }).await)?;
        let mut tool = false; let mut completed = false;
        while let Some(event) = events.recv().await {
            match event {
                RuntimeEvent::ToolActivity { status: session_protocol::SessionToolActivityStatus::Succeeded, .. } => tool = true,
                RuntimeEvent::Completed { outcome: SessionTurnOutcome::Complete, usage } => { assert!(usage.total_tokens > 0); println!("restored usage={usage:?}"); completed = true; },
                RuntimeEvent::Failed { error, .. } => return Err(error.message.into()),
                _ => {},
            }
        }
        assert!(tool && completed);
        let state = checked(runtime.export_state("restored").await)?;
        assert!(serde_json::to_string(&state)?.contains("restored-ok"));
        println!("PASS snapshot -> restored active plugins -> redirected shell -> completed turn -> export; setup handshake preceded inventory by 500 ms");
        Ok(())
    }.await;
    checked(runtime.stop("restored").await)?;
    result
}
