//! Contract tests for `xgovernor-runtime-pi`, driven against a deterministic
//! fake `pi --mode rpc` process (`src/bin/fake_pi.rs`, built as the sibling
//! `fake_pi` binary target) rather than the real Pi CLI, since the sandbox
//! this crate was developed in does not have Pi installed and the point of
//! these tests is to pin down this adapter's own contract (`submit_turn`
//! returns before the turn finishes, `cancel` has real interrupt semantics,
//! the event-mapping table, the interaction round trip), not to validate
//! Pi's actual RPC protocol implementation.
//!
//! Lives under `tests/` (an integration test, not a `#[cfg(test)]` module
//! inside `src/lib.rs`) specifically so `env!("CARGO_BIN_EXE_fake_pi")` is
//! available — Cargo only defines `CARGO_BIN_EXE_<name>` while building
//! integration tests/benchmarks, not while building a package's own library
//! unit tests.
//!
//! Shared fixtures (fake pi path, `InstanceManager` wiring, in-memory
//! repository, etc.) live in `tests/support/mod.rs` and are also used by
//! `tests/restore.rs` (Phase 2 lazy-restoration tests,
//! `docs/pi_session_restore_plan.md`).

mod support;

use agent_runtime_protocol::{
    AgentRuntime, RuntimeCancelRequest, RuntimeError, RuntimeEvent,
    RuntimeInteractionRequest as RuntimeInteractionInput, RuntimeStartRequest,
    RuntimeTurnRequest as RuntimeTurnInput,
};
use serde_json::{json, Value};
use session_protocol::{
    LlmOverrideRequest, SessionExtensions, SessionInteractionAnswer, SessionOpenRequest,
    SessionToolActivityPhase, SessionToolActivityStatus, SessionTurnOutcome, SessionTurnRequest,
};
use std::time::{Duration, Instant};
use support::{
    application, fake_pi_path, new_pi_runtime, no_entry, pi_runtime_ext, started_runtime,
    workspace_facts, LOCAL_BACKEND_ID,
};
use tempfile::TempDir;
use xgovernor_core::SecurityContext;
use xgovernor_runtime_pi::EXT_NAMESPACE;

fn llm(provider: &str, model: &str, api_key: &str) -> LlmOverrideRequest {
    LlmOverrideRequest {
        provider: Some(provider.into()),
        model: Some(model.into()),
        api_base: None,
        api_key_env: None,
        api_key: Some(api_key.into()),
    }
}

#[tokio::test]
async fn llm_selection_is_wired_into_pi_start_and_turn_requests() {
    let workspace = TempDir::new().expect("tempdir");
    let runtime = new_pi_runtime();
    assert!(runtime
        .capabilities()
        .contains(&agent_runtime_protocol::RuntimeCapability::ModelOverride));

    runtime
        .start(
            RuntimeStartRequest {
                runtime_id: "runtime-llm".into(),
                conversation_id: "conversation-llm".into(),
                sender_id: "sender-llm".into(),
                workspace: workspace_facts(workspace.path().to_str().unwrap()),
                state: None,
                llm: Some(llm("openai", "gpt-4.1-mini", "open-key")),
                ext: pi_runtime_ext(),
            },
            support::runtime_context(workspace.path().to_str().unwrap()).await,
        )
        .await
        .expect("start must accept a caller-selected provider/model");

    let mut events = runtime
        .submit_turn(RuntimeTurnInput {
            runtime_id: "runtime-llm".into(),
            turn_id: "turn-llm".into(),
            text: "hello-after-switch".into(),
            entry: no_entry(),
            llm: Some(llm("anthropic", "claude-test-model", "anthropic-key")),
            reasoning_effort: None,
            ext: Default::default(),
        })
        .await
        .expect("turn must switch to the caller-selected provider/model");
    while let Some(event) = events.recv().await {
        if matches!(
            event,
            RuntimeEvent::Completed { .. } | RuntimeEvent::Failed { .. }
        ) {
            break;
        }
    }

    let state = runtime
        .export_state("runtime-llm")
        .await
        .expect("runtime state must be exportable");
    let session_dir = std::path::PathBuf::from(
        state
            .state
            .get("pi_session_dir")
            .and_then(Value::as_str)
            .expect("runtime state must carry its pi session directory"),
    );
    let launch_args: Vec<String> = serde_json::from_slice(
        &std::fs::read(session_dir.join("fake_pi_launch_args.json")).expect("launch args"),
    )
    .expect("launch args JSON");
    let flag_value = |flag: &str| {
        launch_args
            .iter()
            .position(|arg| arg == flag)
            .and_then(|index| launch_args.get(index + 1))
            .map(String::as_str)
    };
    assert_eq!(flag_value("--provider"), Some("openai"));
    assert_eq!(flag_value("--model"), Some("gpt-4.1-mini"));
    assert_eq!(flag_value("--api-key"), Some("open-key"));

    let commands: Vec<Value> = std::fs::read_to_string(session_dir.join("fake_pi_commands.jsonl"))
        .expect("command log")
        .lines()
        .map(|line| serde_json::from_str(line).expect("command JSON"))
        .collect();
    assert!(commands[0]
        .get("message")
        .and_then(Value::as_str)
        .is_some_and(|message| message.starts_with("/xgovernor-model ")));
    assert_eq!(
        commands[1].get("type").and_then(Value::as_str),
        Some("set_model")
    );
    assert_eq!(
        commands[1].get("provider").and_then(Value::as_str),
        Some("anthropic")
    );
    assert_eq!(
        commands[1].get("modelId").and_then(Value::as_str),
        Some("claude-test-model")
    );
    assert_eq!(
        commands[2].get("message").and_then(Value::as_str),
        Some("hello-after-switch")
    );

    let persisted: Value = serde_json::from_slice(
        &std::fs::read(session_dir.join(".xgovernor-llm.json")).expect("persisted llm config"),
    )
    .expect("persisted llm JSON");
    assert_eq!(persisted["provider"], "anthropic");
    assert_eq!(persisted["model"], "claude-test-model");
    runtime.stop("runtime-llm").await.ok();
}

#[tokio::test]
async fn llm_override_requires_explicit_provider_and_model() {
    let workspace = TempDir::new().expect("tempdir");
    let runtime = new_pi_runtime();
    let error = runtime
        .start(
            RuntimeStartRequest {
                runtime_id: "runtime-invalid-llm".into(),
                conversation_id: "conversation".into(),
                sender_id: "sender".into(),
                workspace: workspace_facts(workspace.path().to_str().unwrap()),
                state: None,
                llm: Some(LlmOverrideRequest {
                    model: Some("some-model".into()),
                    ..Default::default()
                }),
                ext: pi_runtime_ext(),
            },
            support::runtime_context(workspace.path().to_str().unwrap()).await,
        )
        .await
        .expect_err("missing provider must fail closed");
    assert!(matches!(error, RuntimeError::InvalidRequest { .. }));
}

#[tokio::test]
async fn api_base_creates_a_session_isolated_openai_compatible_provider() {
    let workspace = TempDir::new().expect("tempdir");
    let runtime = new_pi_runtime();
    runtime
        .start(
            RuntimeStartRequest {
                runtime_id: "runtime-custom-base".into(),
                conversation_id: "conversation".into(),
                sender_id: "sender".into(),
                workspace: workspace_facts(workspace.path().to_str().unwrap()),
                state: None,
                llm: Some(LlmOverrideRequest {
                    provider: Some("my-gateway".into()),
                    model: Some("my-model".into()),
                    api_base: Some("https://llm.example.test/v1".into()),
                    api_key: Some("gateway-key".into()),
                    api_key_env: None,
                }),
                ext: pi_runtime_ext(),
            },
            support::runtime_context(workspace.path().to_str().unwrap()).await,
        )
        .await
        .expect("custom OpenAI-compatible endpoint must start");

    let state = runtime.export_state("runtime-custom-base").await.unwrap();
    let session_dir = std::path::Path::new(state.state["pi_session_dir"].as_str().unwrap());
    let models: Value = serde_json::from_slice(
        &std::fs::read(session_dir.join(".pi-agent/models.json")).expect("models.json"),
    )
    .expect("models JSON");
    assert_eq!(
        models["providers"]["my-gateway"]["baseUrl"],
        "https://llm.example.test/v1"
    );
    assert_eq!(
        models["providers"]["my-gateway"]["api"],
        "openai-completions"
    );
    assert_eq!(
        models["providers"]["my-gateway"]["models"][0]["id"],
        "my-model"
    );
    runtime.stop("runtime-custom-base").await.ok();
}

#[tokio::test]
async fn open_and_submit_turn_streams_output_from_a_real_pi_process_and_completes() {
    let workspace = TempDir::new().expect("tempdir");
    let (app, repository) = application(workspace.path().to_str().unwrap().to_string());
    let ctx = SecurityContext::admin("test");

    app.open(
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
            ext: pi_runtime_ext(),
            lease: Default::default(),
        },
    )
    .await
    .expect("open must succeed against a real pi subprocess");

    // Phase 1 (`docs/pi_session_restore_plan.md` §2): `open_impl` must have
    // called `PiRuntime::export_state` and persisted a non-null blob
    // carrying enough to rebuild `start()` after a restart.
    let record = repository
        .0
        .lock()
        .unwrap()
        .clone()
        .expect("open() must have saved a SessionRecord");
    assert_eq!(record.runtime.runtime_kind, "pi");
    assert_ne!(
        record.runtime.state,
        Value::Null,
        "PiRuntime::export_state must produce a non-null state blob"
    );
    assert_eq!(
        record
            .runtime
            .state
            .get("backend_id")
            .and_then(Value::as_str),
        Some(LOCAL_BACKEND_ID),
        "persisted state must carry the backend_id start() provisioned against"
    );
    let persisted_session_dir = record
        .runtime
        .state
        .get("pi_session_dir")
        .and_then(Value::as_str)
        .expect("persisted state must carry pi_session_dir")
        .to_string();
    assert!(
        std::path::Path::new(&persisted_session_dir).is_dir(),
        "start() must have created the per-runtime_id --session-dir directory at {persisted_session_dir}"
    );

    let submission = app
        .submit_turn(
            &ctx,
            SessionTurnRequest {
                runtime_id: "runtime-1".into(),
                text: "hello-from-contract-test".into(),
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
    let mut saw_output = false;
    let mut saw_tool_activity = false;
    let mut saw_completed = false;
    while let Some(event) = events.recv().await {
        match event {
            session_protocol::SessionEvent::OutputDelta { delta, .. } => {
                if delta.contains("hello-from-contract-test") {
                    saw_output = true;
                }
            }
            session_protocol::SessionEvent::ToolActivity { phase, status, .. } => {
                if phase == SessionToolActivityPhase::End {
                    assert_eq!(status, SessionToolActivityStatus::Succeeded);
                    saw_tool_activity = true;
                }
            }
            session_protocol::SessionEvent::TurnCompleted { outcome, .. } => {
                assert_eq!(outcome, SessionTurnOutcome::Complete);
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
        saw_output,
        "expected the echoed text to flow through OutputDelta"
    );
    assert!(
        saw_tool_activity,
        "expected a tool activity to flow through"
    );
    assert!(saw_completed, "expected a terminal turn_completed event");

    // Confirm the spawned `fake_pi` process actually received
    // `--session-dir <persisted_session_dir>` on its own argv (not just that
    // our bookkeeping believes it did) — see `fake_pi.rs`'s
    // `record_launch_args_if_session_dir_present`. Checked here, after the
    // turn has fully round-tripped through the real subprocess, rather than
    // immediately after `open()`: `start()` only guarantees the child was
    // spawned, not that it has already run past its own first line of code,
    // so reading the file any earlier races the child's own scheduling. By
    // this point the child has emitted a `prompt` response, tool-activity
    // events, and a completion — proof it is alive and has long since run
    // past `record_launch_args_if_session_dir_present`.
    let launch_args_path =
        std::path::Path::new(&persisted_session_dir).join("fake_pi_launch_args.json");
    let launch_args_json = std::fs::read_to_string(&launch_args_path).unwrap_or_else(|error| {
        panic!("expected fake_pi to record its launch args at {launch_args_path:?}: {error}")
    });
    let launch_args: Vec<String> =
        serde_json::from_str(&launch_args_json).expect("launch args must be a JSON string array");
    let session_dir_flag_index = launch_args
        .iter()
        .position(|arg| arg == "--session-dir")
        .expect("fake_pi's argv must include --session-dir");
    assert_eq!(
        launch_args.get(session_dir_flag_index + 1),
        Some(&persisted_session_dir),
        "the --session-dir value fake_pi received must match the persisted pi_session_dir"
    );
}

/// Pins down the `AgentRuntime::submit_turn` contract: it must return
/// without waiting for the turn to actually finish. Unlike `runtime-local`'s
/// equivalent test (which gets a hard zero-race guarantee from controlling
/// both sides of an in-process future), this adapter drives a real OS
/// subprocess, so "returns immediately" is instead pinned down by timing:
/// `submit_turn` must come back long before the fake pi process's own
/// built-in minimum turn duration (`CANCEL_WINDOW_MS` = 600ms in
/// `src/bin/fake_pi.rs`).
#[tokio::test]
async fn submit_turn_returns_without_waiting_for_the_turn_to_finish() {
    let workspace = TempDir::new().expect("tempdir");
    let runtime = started_runtime(workspace.path().to_str().unwrap()).await;

    let started = Instant::now();
    let mut events = runtime
        .submit_turn(RuntimeTurnInput {
            runtime_id: "runtime-1".into(),
            turn_id: "turn-1".into(),
            text: "hello".into(),
            entry: no_entry(),
            llm: None,
            reasoning_effort: None,
            ext: Default::default(),
        })
        .await
        .expect("submit_turn must be accepted");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(200),
        "submit_turn took {elapsed:?}, expected it to return long before the fake pi process's \
         own >=600ms natural-completion delay, proving it does not block on the turn actually \
         finishing"
    );

    while let Some(event) = events.recv().await {
        if matches!(
            event,
            RuntimeEvent::Completed { .. } | RuntimeEvent::Failed { .. }
        ) {
            break;
        }
    }
    runtime.stop("runtime-1").await.ok();
}

#[tokio::test]
async fn native_pi_crash_is_reported_as_worker_unavailable() {
    let workspace = TempDir::new().expect("tempdir");
    let runtime = started_runtime(workspace.path().to_str().unwrap()).await;

    let mut events = runtime
        .submit_turn(RuntimeTurnInput {
            runtime_id: "runtime-1".into(),
            turn_id: "turn-1".into(),
            text: "trigger-worker-crash".into(),
            entry: no_entry(),
            llm: None,
            reasoning_effort: None,
            ext: Default::default(),
        })
        .await
        .expect("submit_turn must be accepted before the nested Pi process exits");

    let terminal = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(event) = events.recv().await {
            if event.is_terminal() {
                return Some(event);
            }
        }
        None
    })
    .await
    .expect("worker failure must not leave the event stream hanging")
    .expect("worker failure must emit a terminal event");

    match terminal {
        RuntimeEvent::Failed { error, .. } => {
            assert_eq!(error.code, "worker_unavailable");
            assert!(error.retryable);
        }
        other => panic!("Pi crash must fail the turn, got {other:?}"),
    }
    runtime.stop("runtime-1").await.ok();
}

/// Pins down `AgentRuntime::cancel`'s "real interrupt semantics" contract
/// against a real subprocess: calling `cancel` must make the fake pi process
/// actually stop short, not just eventually report `Cancelled` after
/// quietly waiting out its own natural completion. The elapsed-time
/// assertion is what tells those two apart.
#[tokio::test]
async fn cancel_interrupts_a_real_pi_turn_before_its_natural_completion() {
    let workspace = TempDir::new().expect("tempdir");
    let runtime = started_runtime(workspace.path().to_str().unwrap()).await;

    let mut events = runtime
        .submit_turn(RuntimeTurnInput {
            runtime_id: "runtime-1".into(),
            turn_id: "turn-1".into(),
            text: "hello".into(),
            entry: no_entry(),
            llm: None,
            reasoning_effort: None,
            ext: Default::default(),
        })
        .await
        .expect("submit_turn must be accepted");

    runtime
        .cancel(RuntimeCancelRequest {
            runtime_id: "runtime-1".into(),
            turn_id: Some("turn-1".into()),
        })
        .await
        .expect("cancel must be accepted");

    let started = Instant::now();
    let mut saw_cancelled_completion = false;
    while let Some(event) = events.recv().await {
        match event {
            RuntimeEvent::Completed { outcome, .. } => {
                assert_eq!(
                    outcome,
                    SessionTurnOutcome::Cancelled,
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
    assert!(saw_cancelled_completion);
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "a real interrupt must resolve well before the fake pi process's own 600ms \
         natural-completion window — otherwise this test would also pass for an adapter that \
         just silently waits the turn out instead of actually cancelling it"
    );
    runtime.stop("runtime-1").await.ok();
}

/// Exercises the `InteractionRequested` -> `answer_interaction` round trip
/// end to end against a real pi process: the fake process's
/// `"trigger-interaction"` scenario emits a `confirm`-method
/// `extension_ui_request` and then echoes whatever value it receives back
/// through a `message_update`, so a successful round trip is observable
/// from this side as that echoed text arriving.
#[tokio::test]
async fn answer_interaction_round_trips_through_a_real_pi_confirm_dialog() {
    let workspace = TempDir::new().expect("tempdir");
    let runtime = started_runtime(workspace.path().to_str().unwrap()).await;

    let mut events = runtime
        .submit_turn(RuntimeTurnInput {
            runtime_id: "runtime-1".into(),
            turn_id: "turn-1".into(),
            text: "trigger-interaction".into(),
            entry: no_entry(),
            llm: None,
            reasoning_effort: None,
            ext: Default::default(),
        })
        .await
        .expect("submit_turn must be accepted");

    let mut interaction_id = None;
    while let Some(event) = events.recv().await {
        if let RuntimeEvent::InteractionRequested {
            interaction_id: id,
            interaction_kind,
            ..
        } = event
        {
            assert_eq!(interaction_kind, "confirm");
            interaction_id = Some(id);
            break;
        }
    }
    let interaction_id = interaction_id.expect("expected an InteractionRequested event");

    runtime
        .answer_interaction(RuntimeInteractionInput {
            runtime_id: "runtime-1".into(),
            turn_id: "turn-1".into(),
            interaction_id,
            answer: SessionInteractionAnswer::Confirm(true),
            ext: Default::default(),
        })
        .await
        .expect("answer_interaction must be accepted");

    let mut saw_confirmed_output = false;
    let mut saw_completed = false;
    while let Some(event) = events.recv().await {
        match event {
            RuntimeEvent::OutputDelta { delta, .. } => {
                if delta.contains("confirmed: true") {
                    saw_confirmed_output = true;
                }
            }
            RuntimeEvent::Completed { outcome, .. } => {
                assert_eq!(outcome, SessionTurnOutcome::Complete);
                saw_completed = true;
                break;
            }
            RuntimeEvent::Failed { error, .. } => panic!("unexpected failure: {error:?}"),
            _ => {}
        }
    }
    assert!(
        saw_confirmed_output,
        "expected the fake pi process to echo back our answer through OutputDelta"
    );
    assert!(saw_completed);
    runtime.stop("runtime-1").await.ok();
}

#[tokio::test]
async fn start_rejects_a_malformed_executable_ext_payload() {
    let workspace = TempDir::new().expect("tempdir");
    let runtime = new_pi_runtime();
    // `backend_id` is present and valid here so this failure is unambiguously
    // about the malformed `executable` field, not a side effect of the
    // (separately tested) missing-backend_id path below.
    let ext: SessionExtensions = [(
        EXT_NAMESPACE.to_string(),
        json!({ "executable": 123, "backend_id": LOCAL_BACKEND_ID }),
    )]
    .into_iter()
    .collect();

    let error = runtime
        .start(
            RuntimeStartRequest {
                runtime_id: "runtime-x".into(),
                conversation_id: "conversation-x".into(),
                sender_id: "sender-x".into(),
                workspace: workspace_facts(workspace.path().to_str().unwrap()),
                state: None,
                llm: None,
                ext,
            },
            support::runtime_context(workspace.path().to_str().unwrap()).await,
        )
        .await
        .expect_err("a non-string 'executable' field must be rejected");
    assert!(matches!(error, RuntimeError::InvalidRequest { .. }));
}

/// Mirrors `runtime-local`'s `start_without_ext_namespace_is_rejected`: Task
/// 1's mandatory-`backend_id` requirement means both a wholly missing
/// `runtime_pi` ext namespace and a namespace present but missing/empty
/// `backend_id` must fail closed as `InvalidRequest`.
#[tokio::test]
async fn start_rejects_a_missing_or_empty_backend_id() {
    let workspace = TempDir::new().expect("tempdir");

    let runtime = new_pi_runtime();
    let error = runtime
        .start(
            RuntimeStartRequest {
                runtime_id: "runtime-x".into(),
                conversation_id: "conversation-x".into(),
                sender_id: "sender-x".into(),
                workspace: workspace_facts(workspace.path().to_str().unwrap()),
                state: None,
                llm: None,
                ext: Default::default(),
            },
            support::runtime_context(workspace.path().to_str().unwrap()).await,
        )
        .await
        .expect_err("a wholly missing 'runtime_pi' ext namespace must be rejected");
    assert!(matches!(error, RuntimeError::InvalidRequest { .. }));

    let runtime = new_pi_runtime();
    let ext: SessionExtensions = [(
        EXT_NAMESPACE.to_string(),
        json!({ "executable": fake_pi_path(), "backend_id": "" }),
    )]
    .into_iter()
    .collect();
    let error = runtime
        .start(
            RuntimeStartRequest {
                runtime_id: "runtime-y".into(),
                conversation_id: "conversation-y".into(),
                sender_id: "sender-y".into(),
                workspace: workspace_facts(workspace.path().to_str().unwrap()),
                state: None,
                llm: None,
                ext,
            },
            support::runtime_context(workspace.path().to_str().unwrap()).await,
        )
        .await
        .expect_err("an empty 'backend_id' must be rejected");
    assert!(matches!(error, RuntimeError::InvalidRequest { .. }));
}

/// A `backend_id` naming no configured `InstanceManager` must fail closed
/// rather than silently falling back to some default manager (see
/// `PiRuntime::new`'s doc and `start()`'s comment on this lookup).
#[tokio::test]
async fn start_rejects_an_unconfigured_backend_id() {
    let workspace = TempDir::new().expect("tempdir");
    let runtime = new_pi_runtime();
    let ext: SessionExtensions = [(
        EXT_NAMESPACE.to_string(),
        json!({ "executable": fake_pi_path(), "backend_id": "does-not-exist" }),
    )]
    .into_iter()
    .collect();

    let error = runtime
        .start(
            RuntimeStartRequest {
                runtime_id: "runtime-z".into(),
                conversation_id: "conversation-z".into(),
                sender_id: "sender-z".into(),
                workspace: workspace_facts(workspace.path().to_str().unwrap()),
                state: None,
                llm: None,
                ext,
            },
            support::runtime_context(workspace.path().to_str().unwrap()).await,
        )
        .await
        .expect_err("an unrecognized backend_id must be rejected");
    assert!(matches!(error, RuntimeError::InvalidRequest { .. }));
}

#[tokio::test]
async fn cancel_without_an_active_turn_is_a_silent_no_op() {
    let workspace = TempDir::new().expect("tempdir");
    let runtime = started_runtime(workspace.path().to_str().unwrap()).await;

    runtime
        .cancel(RuntimeCancelRequest {
            runtime_id: "runtime-1".into(),
            turn_id: None,
        })
        .await
        .expect("cancel with nothing active must be a no-op, not an error");
    runtime.stop("runtime-1").await.ok();
}

#[tokio::test]
async fn operations_against_an_unknown_runtime_id_return_not_found() {
    let runtime = new_pi_runtime();
    let error = runtime
        .attach("does-not-exist", support::runtime_context(".").await)
        .await
        .expect_err("must be not found");
    assert!(matches!(error, RuntimeError::NotFound { .. }));
}
