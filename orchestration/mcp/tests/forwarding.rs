use async_trait::async_trait;
use orchestration_contract::{
    execution::{Isolation, NetworkPolicy},
    run::*,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::Mutex;
use tokio::io::BufReader;
use xgovernor_mcp::{McpServer, RunAction, RunClient};

struct LocalClient {
    calls: Mutex<Vec<(RunAction, Value)>>,
    response: Result<Reply, String>,
}

#[derive(Clone, Serialize)]
#[serde(untagged)]
enum Reply {
    Start(StartResponse),
    Status(StatusResponse),
    Cancel(CancelResponse),
    Result(ResultResponse),
    List(ListResponse),
}

#[async_trait]
impl RunClient for &LocalClient {
    async fn start(&self, request: StartRequest) -> Result<StartResponse, String> {
        self.calls
            .lock()
            .unwrap()
            .push((RunAction::Start, json!(request)));
        match self.response.clone()? {
            Reply::Start(response) => Ok(response),
            _ => panic!("unexpected action"),
        }
    }
    async fn status(&self, request: StatusRequest) -> Result<StatusResponse, String> {
        self.calls
            .lock()
            .unwrap()
            .push((RunAction::Status, json!(request)));
        match self.response.clone()? {
            Reply::Status(response) => Ok(response),
            _ => panic!("unexpected action"),
        }
    }
    async fn cancel(&self, request: CancelRequest) -> Result<CancelResponse, String> {
        self.calls
            .lock()
            .unwrap()
            .push((RunAction::Cancel, json!(request)));
        match self.response.clone()? {
            Reply::Cancel(response) => Ok(response),
            _ => panic!("unexpected action"),
        }
    }
    async fn result(&self, request: ResultRequest) -> Result<ResultResponse, String> {
        self.calls
            .lock()
            .unwrap()
            .push((RunAction::Result, json!(request)));
        match self.response.clone()? {
            Reply::Result(response) => Ok(response),
            _ => panic!("unexpected action"),
        }
    }
    async fn list(&self, request: ListRequest) -> Result<ListResponse, String> {
        self.calls
            .lock()
            .unwrap()
            .push((RunAction::List, json!(request)));
        match self.response.clone()? {
            Reply::List(response) => Ok(response),
            _ => panic!("unexpected action"),
        }
    }
}

async fn forward(action: RunAction, name: &str, arguments: impl Serialize, reply: Reply) {
    let arguments = json!(arguments);
    let expected_reply = json!(reply);
    let client = LocalClient {
        calls: Mutex::new(Vec::new()),
        response: Ok(reply),
    };
    let server = McpServer::new(&client);
    let mut messages = vec![json!({"jsonrpc":"2.0", "id":7, "method":"tools/call",
        "params":{"name":name, "arguments":arguments}})];
    if action == RunAction::Start {
        messages.insert(
            0,
            json!({"jsonrpc":"2.0", "id":1, "method":"initialize",
            "params":{"protocolVersion":"2025-11-25", "capabilities":{},
            "clientInfo":{"name":"entry", "version":"1"}}}),
        );
        messages.insert(
            1,
            json!({"jsonrpc":"2.0", "method":"notifications/initialized"}),
        );
        messages.insert(2, json!({"jsonrpc":"2.0", "id":2, "method":"tools/list"}));
    }
    let input = messages
        .iter()
        .map(|v| format!("{v}\n"))
        .collect::<String>();
    let mut output = Vec::new();
    server
        .serve(BufReader::new(input.as_bytes()), &mut output)
        .await
        .unwrap();
    let responses: Vec<Value> = String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    if action == RunAction::Start {
        assert_eq!(responses.len(), 3);
        assert_eq!(responses[0]["result"]["protocolVersion"], "2025-11-25");
        let names: Vec<_> = responses[1]["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            [
                "mcts_start",
                "mcts_status",
                "mcts_cancel",
                "mcts_result",
                "mcts_list"
            ]
        );
    }
    let response = responses.last().unwrap();
    assert_eq!(response["id"], 7);
    assert_eq!(
        response["result"]["structuredContent"]["result"],
        expected_reply
    );
    assert_eq!(response["result"]["isError"], false);
    assert_eq!(*client.calls.lock().unwrap(), vec![(action, arguments)]);
}

fn summary(status: RunStatus) -> RunSummary {
    RunSummary {
        run_id: "run-1".into(),
        status,
        summary: "Selected candidate".into(),
        backend: Some(BackendKind::E2b),
        cleanup: CleanupStatus::Pending,
        source_session: None,
    }
}

#[tokio::test]
async fn start_forwards_acceptance_without_running_the_task() {
    let budget = BudgetLimits {
        max_tokens: 20000,
        max_time_ms: 60000,
        max_cost_microunits: None,
    };
    forward(
        RunAction::Start,
        "mcts_start",
        StartRequest {
            start_key: "stable-key".into(),
            goal: "Fix the task".into(),
            content: "Task description".into(),
            code_source: CodeSource::Task {
                task: "swe_bench".into(),
                reference: "example".into(),
            },
            config: RunConfig {
                budget: budget.clone(),
                network_policy: NetworkPolicy::Offline,
                backend: BackendSelection::Explicit(BackendKind::E2b),
                requirements: ExecutionRequirements {
                    can_reset_history: false,
                    capabilities: Default::default(),
                    isolation: Isolation::Sandbox,
                    joint_snapshots: true,
                    independent_branches: true,
                },
                algorithm: AlgorithmConfig {
                    namespace: "mcts".into(),
                    version: "1".into(),
                    schema: "mcts-v1".into(),
                    parameters: Default::default(),
                },
            },
            approval: StartApproval {
                budget,
                workspace: None,
            },
            source_session: None,
        },
        Reply::Start(StartResponse {
            run_id: "run-1".into(),
            status: RunStatus::Accepted,
        }),
    )
    .await;
}

#[tokio::test]
async fn status_forwards_cursor_and_application_failure() {
    forward(
        RunAction::Status,
        "mcts_status",
        StatusRequest {
            run_id: "run-1".into(),
            after_sequence: 4,
            limit: 10,
        },
        Reply::Status(StatusResponse {
            run: summary(RunStatus::Running),
            events: vec![],
            next_sequence: 4,
            has_more: false,
        }),
    )
    .await;
    let client = LocalClient {
        calls: Mutex::new(Vec::new()),
        response: Err("Run not found".into()),
    };
    let server = McpServer::new(&client);
    let response = server
        .handle(json!({"jsonrpc":"2.0", "id":1, "method":"tools/call",
        "params":{"name":"mcts_status", "arguments":StatusRequest {
            run_id: "missing".into(), after_sequence: 0, limit: 10
        }}}))
        .await
        .unwrap();
    assert_eq!(response["result"]["isError"], true);
    assert_eq!(response["result"]["content"][0]["text"], "Run not found");
}

#[tokio::test]
async fn cancel_preserves_pending_confirmation() {
    forward(
        RunAction::Cancel,
        "mcts_cancel",
        CancelRequest {
            run_id: "run-1".into(),
            reason: "User requested stop".into(),
        },
        Reply::Cancel(CancelResponse {
            run_id: "run-1".into(),
            status: RunStatus::Cancelling,
            effective: false,
            cleanup: CleanupStatus::Pending,
        }),
    )
    .await;
}

#[tokio::test]
async fn result_preserves_summary_and_artifact_references() {
    forward(
        RunAction::Result,
        "mcts_result",
        ResultRequest {
            run_id: "run-1".into(),
        },
        Reply::Result(ResultResponse {
            run: summary(RunStatus::Completed),
            failure: None,
            result: Some(RunResult {
                summary: "Selected candidate".into(),
                answer: "Patch ready".into(),
                stop_reason: "completed".into(),
                selected_candidate_id: Some("candidate-1".into()),
                artifacts: vec![ArtifactRef {
                    artifact_id: "patch".into(),
                    kind: "patch".into(),
                    media_type: "text/x-diff".into(),
                    expires_at: None,
                }],
                continuation: None,
                limitations: vec![],
            }),
        }),
    )
    .await;
}

#[tokio::test]
async fn list_preserves_filters_and_pagination() {
    forward(
        RunAction::List,
        "mcts_list",
        ListRequest {
            source_session_id: Some("session-1".into()),
            cursor: Some("page-2".into()),
            limit: 10,
        },
        Reply::List(ListResponse {
            runs: vec![summary(RunStatus::Running)],
            next_cursor: Some("page-3".into()),
        }),
    )
    .await;
}
