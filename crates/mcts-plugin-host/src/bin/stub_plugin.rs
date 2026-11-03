use std::io::{self, BufRead, Write};

use serde_json::{json, Value};

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let Ok(line) = line else {
            break;
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let Ok(message) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let id = message.get("id").cloned().unwrap_or(Value::Null);
        let method = message.get("method").and_then(Value::as_str).unwrap_or("");
        let params = message.get("params").cloned().unwrap_or(json!({}));
        let run_id = params.get("run_id").cloned().unwrap_or(Value::Null);
        let mut notifications: Vec<Value> = Vec::new();

        let response = match method {
            "plugin.initialize" => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "plugin_id": params.get("plugin_id").cloned().unwrap_or(Value::Null),
                    "accepted": true
                }
            }),
            "plugin.start" => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "run_id": params.get("run_id").cloned().unwrap_or(Value::Null),
                    "state": "running"
                }
            }),
            "plugin.wait" => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "run_id": run_id.clone(),
                    "sequence": 1,
                    "status": "running"
                }
            }),
            "plugin.shutdown" => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {"ok": true}
            }),
            _ => json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": "-32601", "message": "method not found"}
            }),
        };

        if method == "plugin.start" {
            notifications.push(json!({
                "jsonrpc": "2.0",
                "method": "plugin.started",
                "params": {
                    "plugin_id": "mcts",
                    "run_id": run_id.clone(),
                    "sequence": 1,
                    "summary": "stub started"
                }
            }));
        }

        if method == "plugin.wait" {
            notifications.push(json!({
                "jsonrpc": "2.0",
                "method": "plugin.completed",
                "params": {
                    "plugin_id": "mcts",
                    "run_id": run_id.clone(),
                    "sequence": 2,
                    "result": {
                        "status": "completed",
                        "summary": "stub completed",
                        "answer": "stub answer",
                        "stop_reason": "stub budget",
                        "selected_candidate_id": null,
                        "candidates": [],
                        "evidence": [],
                        "limitations": []
                    }
                }
            }));
        }

        for notification in notifications {
            writeln!(stdout, "{notification}").expect("write stub notification");
            stdout.flush().expect("flush stub notification");
        }

        writeln!(stdout, "{response}").expect("write stub response");
        stdout.flush().expect("flush stub response");

        if method == "plugin.shutdown" {
            break;
        }
    }
}
