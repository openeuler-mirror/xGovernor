//! MCP stdio adapter for the run application's five entry actions.
use async_trait::async_trait;
use orchestration_contract::run::*;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const PROTOCOL: &str = "2025-11-25";
const MAX_MESSAGE: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunAction {
    Start,
    Status,
    Cancel,
    Result,
    List,
}

impl RunAction {
    fn name(self) -> &'static str {
        match self {
            Self::Start => "mcts_start",
            Self::Status => "mcts_status",
            Self::Cancel => "mcts_cancel",
            Self::Result => "mcts_result",
            Self::List => "mcts_list",
        }
    }
}

/// The application owns admission, durable acceptance, cancellation and results.
/// Start must return its receipt after acceptance, without waiting for execution.
/// Errors must be safe for display to the invoking agent.
#[async_trait]
pub trait RunClient: Send + Sync {
    async fn start(&self, request: StartRequest) -> Result<StartResponse, String>;
    async fn status(&self, request: StatusRequest) -> Result<StatusResponse, String>;
    async fn cancel(&self, request: CancelRequest) -> Result<CancelResponse, String>;
    async fn result(&self, request: ResultRequest) -> Result<ResultResponse, String>;
    async fn list(&self, request: ListRequest) -> Result<ListResponse, String>;
}

pub struct McpServer<C> {
    client: C,
}

impl<C: RunClient> McpServer<C> {
    pub fn new(client: C) -> Self {
        Self { client }
    }

    /// Handle one classic MCP JSON-RPC message. Notifications have no response.
    pub async fn handle(&self, request: Value) -> Option<Value> {
        let id = request.get("id").cloned();
        if request.get("jsonrpc") != Some(&json!("2.0"))
            || !request.get("method").is_some_and(Value::is_string)
        {
            return Some(rpc_error(
                id.unwrap_or(Value::Null),
                -32600,
                "Invalid request",
            ));
        }
        let id = id?;
        let params = &request["params"];
        let result = match request["method"].as_str().unwrap() {
            "initialize" => {
                let requested = params["protocolVersion"].as_str().unwrap_or(PROTOCOL);
                let version = match requested {
                    "2024-11-05" | "2025-03-26" | "2025-06-18" | PROTOCOL => requested,
                    _ => PROTOCOL,
                };
                json!({
                    "protocolVersion": version,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "xgovernor-mcts", "version": env!("CARGO_PKG_VERSION")},
                    "instructions": "Confirm budget and expected duration before starting. Results contain artifact references; never apply patches automatically."
                })
            }
            "ping" => json!({}),
            "tools/list" => json!({"tools": tools()}),
            "tools/call" => {
                let Some(action) = actions()
                    .into_iter()
                    .find(|a| Some(a.name()) == params["name"].as_str())
                else {
                    return Some(rpc_error(id, -32602, "Unknown tool"));
                };
                let arguments = params.get("arguments").cloned().unwrap_or(json!({}));
                macro_rules! forward {
                    ($method:ident, $request:ty) => {{
                        let Ok(request) = decode::<$request>(arguments) else {
                            return Some(rpc_error(id, -32602, "Invalid tool arguments"));
                        };
                        self.client.$method(request).await.map(encode)
                    }};
                }
                let response = match action {
                    RunAction::Start => forward!(start, StartRequest),
                    RunAction::Status => forward!(status, StatusRequest),
                    RunAction::Cancel => forward!(cancel, CancelRequest),
                    RunAction::Result => forward!(result, ResultRequest),
                    RunAction::List => forward!(list, ListRequest),
                };
                match response {
                    Ok(value) => json!({
                        "content": [{"type": "text", "text": value.to_string()}],
                        "structuredContent": {"result": value},
                        "isError": false
                    }),
                    Err(message) => json!({
                        "content": [{"type": "text", "text": message}], "isError": true
                    }),
                }
            }
            _ => return Some(rpc_error(id, -32601, "Method not found")),
        };
        Some(json!({"jsonrpc": "2.0", "id": id, "result": result}))
    }

    /// Serve newline-delimited MCP messages; stdout contains protocol output only.
    /// EOF closes the adapter and never cancels runs owned by the application.
    pub async fn serve<R, W>(&self, mut input: R, mut output: W) -> std::io::Result<()>
    where
        R: AsyncBufRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        loop {
            let mut bytes = Vec::new();
            let count = (&mut input)
                .take(MAX_MESSAGE + 1)
                .read_until(b'\n', &mut bytes)
                .await?;
            if count == 0 {
                return Ok(());
            }
            if count as u64 > MAX_MESSAGE {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "MCP message exceeds size limit",
                ));
            }
            let response = match serde_json::from_slice(&bytes) {
                Ok(request) => self.handle(request).await,
                Err(_) => Some(rpc_error(Value::Null, -32700, "Parse error")),
            };
            if let Some(response) = response {
                output.write_all(response.to_string().as_bytes()).await?;
                output.write_all(b"\n").await?;
                output.flush().await?;
            }
        }
    }
}

fn decode<T: DeserializeOwned>(arguments: Value) -> Result<T, serde_json::Error> {
    serde_json::from_value(arguments)
}

fn encode<T: Serialize>(response: T) -> Value {
    serde_json::to_value(response).expect("run responses serialize to JSON")
}

fn rpc_error(id: Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn actions() -> [RunAction; 5] {
    [
        RunAction::Start,
        RunAction::Status,
        RunAction::Cancel,
        RunAction::Result,
        RunAction::List,
    ]
}

fn tools() -> Vec<Value> {
    actions().into_iter().map(|action| {
        let (description, schema) = match action {
            RunAction::Start => (
                "Start a background run. Confirm budget and expected duration with the user first. Reuse start_key on retry. Returns durable acceptance, not completion. Tasks must supply their code source; local workspace upload is unsupported.",
                start_schema(),
            ),
            RunAction::List => (
                "List runs using the run application's session filter and pagination.",
                json!({"type": "object", "properties": {
                    "source_session_id": {"type": ["string", "null"]},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 0, "maximum": u32::MAX}
                }, "required": ["limit"]}),
            ),
            action => {
                let mut schema = json!({"type": "object", "properties": {
                    "run_id": {"type": "string"}
                }, "required": ["run_id"]});
                let description = match action {
                    RunAction::Status => {
                        schema["properties"]["after_sequence"] = json!({"type": "integer", "minimum": 0});
                        schema["properties"]["limit"] = json!({"type": "integer", "minimum": 0, "maximum": u32::MAX});
                        schema["required"] = json!(["run_id", "after_sequence", "limit"]);
                        "Read run status and events. Pass next_sequence as after_sequence on subsequent queries."
                    }
                    RunAction::Cancel => {
                        schema["properties"]["reason"] = json!({"type": "string"});
                        schema["required"] = json!(["run_id", "reason"]);
                        "Request cancellation and report whether it has taken effect. Do not infer cancellation from a request receipt."
                    }
                    _ => "Read the run summary and controlled artifact references, including patch and workspace. Never apply patches automatically.",
                };
                (description, schema)
            },
        };
        json!({"name": action.name(), "description": description, "inputSchema": schema})
    }).collect()
}

fn start_schema() -> Value {
    // Inline references because MCP clients differ in support for local $defs.
    // Serde's tagged enums/defaults/options remain defined by the run contract.
    let mut settings = schemars::gen::SchemaSettings::draft07();
    settings.inline_subschemas = true;
    serde_json::to_value(
        settings
            .into_generator()
            .into_root_schema_for::<StartRequest>(),
    )
    .expect("run request schema serializes")
}
