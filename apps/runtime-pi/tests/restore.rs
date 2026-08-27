//! Phase 2 ("lazy restoration path") integration tests for
//! `xgovernor-runtime-pi`, `docs/pi_session_restore_plan.md` §2 step 5. Like
//! `tests/contract.rs`, everything here is driven against the deterministic
//! fake `pi --mode rpc` process (`src/bin/fake_pi.rs`) rather than the real
//! Pi CLI — shared fixtures live in `tests/support/mod.rs`.
//!
//! `fake_pi` does not itself write real `pi`-style session JSONL content (it
//! only records its own launch args for test introspection, via
//! `record_launch_args_if_session_dir_present`); every test below that needs
//! `PiRuntime::prepare_resume` to find a valid session file seeds one
//! directly, standing in for what a real `pi` process would have left behind
//! after a completed turn (see `session_file.rs`'s validation shape).
//!
//! "Restart" is simulated the same way throughout: build an `InstanceManager`
//! map and an on-disk session root once, share both `clone()`s across two
//! distinct `PiRuntime` instances ("daemon A" and "daemon B"), and let A go
//! out of scope between them. Dropping `PiRuntime` drops its `instances`
//! registry (and, with it, every `PiInstance`'s `Mutex<Child>`, which has
//! `kill_on_drop(true)`) — so the fake `pi` process actually dies, while the
//! sandbox/ledger state tracked by the shared `InstanceManager` survives,
//! exactly matching what a real daemon restart leaves behind.

mod support;

use serde_json::Value;
use session_protocol::{SessionEvent, SessionOpenRequest, SessionTurnOutcome, SessionTurnRequest};
use std::path::Path;
use std::sync::Arc as StdArc;
use tempfile::TempDir;
use xgovernor_core::{SecurityContext, SessionDomainError, SessionStatus};
use xgovernor_runtime_pi::PiRuntime;

/// Minimal valid `.jsonl` line pair matching `session_file.rs`'s validation
/// shape (a terminal, non-pending assistant message as the last line) — a
/// stand-in for what a real `pi` process would have written to its
/// `--session-dir` after completing the turn above.
fn seed_valid_session_file(session_dir: &str, file_name: &str) -> String {
    let path = Path::new(session_dir).join(file_name);
    std::fs::write(
        &path,
        "{\"type\":\"message\",\"message\":{\"role\":\"user\",\"content\":\"hi\"}}\n\
         {\"type\":\"message\",\"message\":{\"role\":\"assistant\",\"content\":\"ok\",\"stopReason\":\"end_turn\"}}\n",
    )
    .expect("seed a valid session jsonl file");
    path.to_string_lossy().into_owned()
}

fn open_request() -> SessionOpenRequest {
    SessionOpenRequest {
        runtime_id: None,
        runtime_kind: None,
        conversation_id: "conversation-1".into(),
        sender_id: "sender-1".into(),
        workspace: Default::default(),
        deployment: Default::default(),
        requested_capabilities: Default::default(),
        llm: None,
        ext: support::pi_runtime_ext(),
        lease: Default::default(),
    }
}

fn turn_request(text: &str) -> SessionTurnRequest {
    SessionTurnRequest {
        runtime_id: "runtime-1".into(),
        text: text.into(),
        entry: Default::default(),
        llm: None,
        reasoning_effort: None,
        client_request_id: None,
        ext: Default::default(),
        lease: Default::default(),
    }
}

/// The core Phase 2 happy path: a `submit_turn` against a session whose
/// adapter registry was wiped by a restart must transparently restore it
/// (`ensure_runtime_attached`, `application.rs`) — replaying `start()` with
/// the persisted state so the resumed `pi` process is spawned with an
/// explicit `--session <file>` on top of `--session-dir`, on the same
/// sandbox the original session used — rather than failing or silently
/// cold-starting a fresh one.
#[tokio::test]
async fn restart_then_submit_turn_triggers_lazy_restoration_and_resumes_the_persisted_session() {
    let workspace = TempDir::new().expect("tempdir");
    let workspace_root = workspace.path().to_str().unwrap().to_string();
    let session_root = TempDir::new()
        .expect("tempdir for pi session-dir root")
        .keep();
    let managers = support::build_managers();
    let repository = StdArc::new(support::MemoryRepository::default());
    let ctx = SecurityContext::admin("test");

    // "Daemon A": open the session (cold start) exactly like a pre-restart
    // daemon would have.
    {
        let runtime_a = PiRuntime::new_with_worker(session_root.clone(), support::pi_worker_path())
            .expect("bridge must bind");
        let app_a = support::application_with_runtime(
            runtime_a,
            managers.clone(),
            repository.clone(),
            workspace_root.clone(),
        );
        app_a
            .open(&ctx, open_request())
            .await
            .expect("open must succeed against a real pi subprocess");
    } // app_a (and runtime_a) dropped here: kill_on_drop kills the fake_pi
      // process, simulating a daemon restart. `managers` and `session_root`
      // outlive this scope, matching how a restart leaves the sandbox/ledger
      // intact but the adapter's own in-memory registry empty.

    let record = repository
        .0
        .lock()
        .unwrap()
        .clone()
        .expect("open() must have saved a SessionRecord");
    let session_dir = record
        .runtime
        .state
        .get("pi_session_dir")
        .and_then(Value::as_str)
        .expect("persisted state must carry pi_session_dir")
        .to_string();
    let seeded_file = seed_valid_session_file(&session_dir, "session.jsonl");

    // Simulate the row having been left `running` by whatever the daemon was
    // doing right before it restarted (`docs/pi_session_restore_plan.md`
    // §1.3: an orphaned in-flight turn is not resumed, just cleared).
    {
        let mut guard = repository.0.lock().unwrap();
        let mut running = guard.clone().unwrap();
        running.status = SessionStatus::Running;
        *guard = Some(running);
    }

    // "Daemon B": fresh `PiRuntime` sharing the same managers/session root,
    // with no in-memory instance for "runtime-1" at all.
    let runtime_b = PiRuntime::new_with_worker(session_root.clone(), support::pi_worker_path())
        .expect("bridge must bind");
    let app_b = support::application_with_runtime(
        runtime_b,
        managers.clone(),
        repository.clone(),
        workspace_root,
    );

    let submission = app_b
        .submit_turn(&ctx, turn_request("hello-after-restart"))
        .await
        .expect("submit_turn must transparently restore the session and succeed");

    let mut events = submission
        .events
        .expect("resumed turn carries an event stream");
    let mut saw_output = false;
    let mut saw_completed = false;
    while let Some(event) = events.recv().await {
        match event {
            SessionEvent::OutputDelta { delta, .. } => {
                if delta.contains("hello-after-restart") {
                    saw_output = true;
                }
            }
            SessionEvent::TurnCompleted { outcome, .. } => {
                assert_eq!(outcome, SessionTurnOutcome::Complete);
                saw_completed = true;
                break;
            }
            SessionEvent::TurnFailed { error, .. } => panic!("resumed turn failed: {error:?}"),
            _ => {}
        }
    }
    assert!(
        saw_output,
        "the resumed pi process must still process turns normally"
    );
    assert!(saw_completed);

    // The newly spawned (second) fake_pi process must have received an
    // explicit `--session <seeded file>` on top of `--session-dir` — proof
    // restoration went through `PiRuntime::prepare_resume`'s real code path
    // (`docs/pi_session_restore_plan.md` §1.2), not e.g. a silent cold start.
    let launch_args_path = Path::new(&session_dir).join("fake_pi_launch_args.json");
    let launch_args_json = std::fs::read_to_string(&launch_args_path)
        .expect("fake_pi must have recorded its (second, resumed) launch args");
    let launch_args: Vec<String> =
        serde_json::from_str(&launch_args_json).expect("launch args must be a JSON string array");
    let session_flag_index = launch_args
        .iter()
        .position(|arg| arg == "--session")
        .expect("the resumed pi process must receive an explicit --session flag");
    assert_eq!(
        launch_args.get(session_flag_index + 1),
        Some(&seeded_file),
        "the --session value must point at the exact session file this test seeded"
    );

    // And the row's status, left `running` by the simulated restart, must
    // have fallen back to `idle` (§1.3) rather than staying stuck.
    let after = repository.0.lock().unwrap().clone().unwrap();
    assert_eq!(
        after.status,
        SessionStatus::Idle,
        "a successful restoration must clear an orphaned `running` status back to `idle`"
    );
}

/// When the provider instance itself is gone, restoration fails closed and
/// the Application persists that failure on the session row.
#[tokio::test]
async fn resume_fails_closed_and_persists_failure_when_the_provider_instance_is_gone() {
    let workspace = TempDir::new().expect("tempdir");
    let workspace_root = workspace.path().to_str().unwrap().to_string();
    let session_root = TempDir::new()
        .expect("tempdir for pi session-dir root")
        .keep();
    let managers = support::build_managers();
    let repository = StdArc::new(support::MemoryRepository::default());
    let ctx = SecurityContext::admin("test");

    {
        let runtime_a = PiRuntime::new_with_worker(session_root.clone(), support::pi_worker_path())
            .expect("bridge must bind");
        let app_a = support::application_with_runtime(
            runtime_a,
            managers.clone(),
            repository.clone(),
            workspace_root.clone(),
        );
        app_a
            .open(&ctx, open_request())
            .await
            .expect("open must succeed against a real pi subprocess");
    }

    let record = repository.0.lock().unwrap().clone().unwrap();
    let session_dir = record
        .runtime
        .state
        .get("pi_session_dir")
        .and_then(Value::as_str)
        .unwrap()
        .to_string();
    seed_valid_session_file(&session_dir, "session.jsonl");

    // Simulate the sandbox itself having been reclaimed independently of the
    // daemon restart: destroy it directly via the manager both `PiRuntime`s
    // share, so `backend_for` will report it as gone.
    managers
        .get(support::LOCAL_BACKEND_ID)
        .unwrap()
        .stop_instance("runtime-1")
        .await
        .expect("stop_instance must succeed while the sandbox is still registered");

    let runtime_b = PiRuntime::new_with_worker(session_root.clone(), support::pi_worker_path())
        .expect("bridge must bind");
    let app_b = support::application_with_runtime(
        runtime_b,
        managers.clone(),
        repository.clone(),
        workspace_root,
    );

    // `SessionSubmission` (the `Ok` type) does not implement `Debug`, so
    // `expect_err` cannot be used directly here.
    let error = match app_b.submit_turn(&ctx, turn_request("hello")).await {
        Ok(_) => panic!("submit_turn must fail closed when the underlying sandbox is gone"),
        Err(error) => error,
    };
    assert!(matches!(error, SessionDomainError::NotFound { .. }));

    let after = repository.0.lock().unwrap().clone().unwrap();
    assert_eq!(
        after.status,
        SessionStatus::Failed,
        "a failed restoration attempt must be persisted onto the row"
    );
    assert!(
        after
            .last_error
            .as_deref()
            .is_some_and(|message| !message.is_empty()),
        "last_error must carry the restoration failure, got {:?}",
        after.last_error
    );
}

/// Closing after restart destroys the provider instance without spawning a
/// worker merely to stop it. Runtime-owned persisted files remain outside
/// the provider lifecycle boundary.
#[tokio::test]
async fn close_after_restart_destroys_provider_without_spawning_pi() {
    let workspace = TempDir::new().expect("tempdir");
    let workspace_root = workspace.path().to_str().unwrap().to_string();
    let session_root = TempDir::new()
        .expect("tempdir for pi session-dir root")
        .keep();
    let managers = support::build_managers();
    let repository = StdArc::new(support::MemoryRepository::default());
    let ctx = SecurityContext::admin("test");

    {
        let runtime_a = PiRuntime::new_with_worker(session_root.clone(), support::pi_worker_path())
            .expect("bridge must bind");
        let app_a = support::application_with_runtime(
            runtime_a,
            managers.clone(),
            repository.clone(),
            workspace_root.clone(),
        );
        app_a
            .open(&ctx, open_request())
            .await
            .expect("open must succeed against a real pi subprocess");
    } // simulated restart: fake_pi killed, sandbox/ledger entry survives.

    let record = repository.0.lock().unwrap().clone().unwrap();
    let session_dir = record
        .runtime
        .state
        .get("pi_session_dir")
        .and_then(Value::as_str)
        .unwrap()
        .to_string();
    assert!(
        Path::new(&session_dir).is_dir(),
        "sanity: the cold start must have created the session dir"
    );

    let runtime_b = PiRuntime::new_with_worker(session_root.clone(), support::pi_worker_path())
        .expect("bridge must bind");
    let app_b = support::application_with_runtime(
        runtime_b,
        managers.clone(),
        repository.clone(),
        workspace_root,
    );

    let response = app_b
        .close(&ctx, "runtime-1", Default::default())
        .await
        .expect("close must succeed via the state-based cleanup special case, without spawning pi");
    assert_eq!(
        response.status,
        session_protocol::SessionLifecycleStatus::Closed
    );

    // Runtime-owned session files are intentionally outside the provider
    // lifecycle boundary; close tombstones the session and stops the worker.
    assert!(Path::new(&session_dir).exists());

    let closed = repository.0.lock().unwrap().clone().unwrap();
    assert_eq!(closed.status, SessionStatus::Closed);
}
