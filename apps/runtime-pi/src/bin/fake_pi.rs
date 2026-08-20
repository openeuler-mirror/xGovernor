//Only for testing purposes
use std::io::{self, BufRead, Write};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

const CANCEL_WINDOW_MS: u64 = 600;

fn emit(value: serde_json::Value) {
    let mut line = value.to_string();
    line.push('\n');
    let mut stdout = io::stdout();
    let _ = stdout.write_all(line.as_bytes());
    let _ = stdout.flush();
}

/// Test-only introspection hook: if this process was launched with
/// `--session-dir <dir>` (as `xgovernor-runtime-pi`'s `PiRuntime::start`
/// always passes it — `docs/pi_session_restore_plan.md` §1.1/decision 1),
/// record the full argv this process actually received into
/// `<dir>/fake_pi_launch_args.json` so contract tests can assert on exactly
/// what `start()` spawned without racing on shared process-global state
/// (each `runtime_id` gets its own session dir, so this is inherently
/// per-instance and safe under `cargo test`'s parallel test execution).
fn record_launch_args_if_session_dir_present() {
    let args: Vec<String> = std::env::args().collect();
    let session_dir = args
        .iter()
        .position(|arg| arg == "--session-dir")
        .and_then(|index| args.get(index + 1));
    if let Some(session_dir) = session_dir {
        let log_path = std::path::Path::new(session_dir).join("fake_pi_launch_args.json");
        let _ = std::fs::write(log_path, serde_json::to_string(&args).unwrap_or_default());
    }
}

fn record_command(command: &serde_json::Value) {
    let args: Vec<String> = std::env::args().collect();
    let Some(session_dir) = args
        .iter()
        .position(|arg| arg == "--session-dir")
        .and_then(|index| args.get(index + 1))
    else {
        return;
    };
    let path = std::path::Path::new(session_dir).join("fake_pi_commands.jsonl");
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(file, "{command}");
    }
}

fn main() {
    record_launch_args_if_session_dir_present();

    let (tx, rx) = mpsc::channel::<serde_json::Value>();
    thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else { break };
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<serde_json::Value>(&line) {
                Ok(value) => {
                    if tx.send(value).is_err() {
                        break;
                    }
                }
                Err(_) => continue,
            }
        }
    });

    // Only one command is ever actively driven at a time (this fake has no
    // concept of concurrent turns), so a single shared receiver handed down
    // into whichever scenario is currently running is safe: nothing else
    // consumes from it while a scenario function is blocked in its own
    // `recv_timeout` loop.
    while let Ok(command) = rx.recv() {
        record_command(&command);
        match command.get("type").and_then(|v| v.as_str()).unwrap_or("") {
            "prompt" => handle_prompt(&command, &rx),
            "set_model" => emit(serde_json::json!({
                "type": "response",
                "command": "set_model",
                "id": command.get("id"),
                "success": true,
                "data": {
                    "provider": command.get("provider"),
                    "id": command.get("modelId")
                }
            })),
            // A stray `abort`/`extension_ui_response` with no scenario
            // currently waiting on it (e.g. arrived after settle) — nothing
            // to do.
            _ => {}
        }
    }
}

fn handle_prompt(command: &serde_json::Value, rx: &mpsc::Receiver<serde_json::Value>) {
    let id = command
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("turn")
        .to_string();
    let message = command
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    emit(serde_json::json!({
        "type": "response",
        "command": "prompt",
        "id": id,
        "success": true
    }));

    if message.starts_with("/xgovernor-model ") {
        return;
    }

    if message == "trigger-interaction" {
        run_interaction_scenario(&id, rx);
    } else {
        run_cancellable_scenario(&message, rx);
    }
}

fn run_interaction_scenario(id: &str, rx: &mpsc::Receiver<serde_json::Value>) {
    let interaction_id = format!("{id}-confirm");
    emit(serde_json::json!({
        "type": "extension_ui_request",
        "id": interaction_id,
        "method": "confirm",
        "params": {"message": "proceed?"}
    }));

    let deadline = Instant::now() + Duration::from_millis(2_000);
    let mut answer_value = serde_json::Value::Null;
    let mut received = false;
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(20)) {
            Ok(value) => {
                let is_match = value.get("type").and_then(|v| v.as_str())
                    == Some("extension_ui_response")
                    && value.get("id").and_then(|v| v.as_str()) == Some(interaction_id.as_str());
                if is_match {
                    answer_value = value
                        .get("value")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    received = true;
                    break;
                }
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }

    let text = if received {
        format!("confirmed: {answer_value}")
    } else {
        "confirmed: <no answer received>".to_string()
    };
    emit(serde_json::json!({
        "type": "message_update",
        "assistantMessageEvent": {"type": "text_delta", "delta": text}
    }));
    emit(serde_json::json!({
        "type": "agent_settled",
        "usage": {"inputTokens": 1, "outputTokens": 1, "totalTokens": 2}
    }));
}

fn run_cancellable_scenario(message: &str, rx: &mpsc::Receiver<serde_json::Value>) {
    emit(serde_json::json!({
        "type": "message_update",
        "assistantMessageEvent": {"type": "text_delta", "delta": format!("echo: {message}")}
    }));

    let activity_id = "call-1";
    emit(serde_json::json!({
        "type": "tool_execution_start",
        "toolCallId": activity_id,
        "toolName": "noop",
        "input": {}
    }));

    let deadline = Instant::now() + Duration::from_millis(CANCEL_WINDOW_MS);
    let mut aborted = false;
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(20)) {
            Ok(value) => {
                if value.get("type").and_then(|v| v.as_str()) == Some("abort") {
                    aborted = true;
                    break;
                }
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }

    emit(serde_json::json!({
        "type": "tool_execution_end",
        "toolCallId": activity_id,
        "toolName": "noop",
        "result": {"ok": true},
        "isError": false
    }));

    if !aborted {
        emit(serde_json::json!({
            "type": "message_update",
            "assistantMessageEvent": {"type": "text_delta", "delta": " done"}
        }));
    }

    emit(serde_json::json!({
        "type": "agent_settled",
        "usage": {"inputTokens": 1, "outputTokens": 1, "totalTokens": 2}
    }));
}
