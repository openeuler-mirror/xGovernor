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
                    "run_id": params.get("run_id").cloned().unwrap_or(Value::Null),
                    "sequence": 1,
                    "state": "running"
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

        writeln!(stdout, "{response}").expect("write stub response");
        stdout.flush().expect("flush stub response");

        if method == "plugin.shutdown" {
            break;
        }
    }
}
