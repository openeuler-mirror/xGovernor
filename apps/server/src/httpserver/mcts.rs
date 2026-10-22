use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use mcts_plugin_host::{
    build_launch_command, FakeApplication, HostCapService, MctsRun, MctsRunOutcome,
    MctsRunRequest, MctsRunStatus, RunMap,
};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct MctsHttpState {
    runs: Arc<Mutex<HashMap<String, MctsRunRecord>>>,
    config: Arc<MctsLaunchConfig>,
}

#[derive(Clone)]
pub struct MctsLaunchConfig {
    python_bin: String,
    repo_root: PathBuf,
    spawn_enabled: bool,
}

impl Default for MctsLaunchConfig {
    fn default() -> Self {
        Self {
            python_bin: std::env::var("MCTS_PYTHON_BIN").unwrap_or_else(|_| "python".to_string()),
            repo_root: std::env::var("MCTS_REPO_ROOT")
                .unwrap_or_else(|_| ".".to_string())
                .into(),
            spawn_enabled: true,
        }
    }
}

impl MctsHttpState {
    pub fn new(config: MctsLaunchConfig) -> Self {
        Self {
            runs: Arc::new(Mutex::new(HashMap::new())),
            config: Arc::new(config),
        }
    }

    pub fn disabled_for_tests() -> Self {
        Self::new(MctsLaunchConfig {
            spawn_enabled: false,
            ..MctsLaunchConfig::default()
        })
    }
}

impl Default for MctsHttpState {
    fn default() -> Self {
        Self::new(MctsLaunchConfig::default())
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct MctsRunRecord {
    run_id: String,
    status: MctsRunStatus,
    request: MctsRunRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outcome: Option<MctsRunOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct MctsRunView {
    run_id: String,
    status: MctsRunStatus,
    request: MctsRunRequest,
    outcome: Option<MctsRunOutcome>,
    error: Option<String>,
}

impl From<MctsRunRecord> for MctsRunView {
    fn from(record: MctsRunRecord) -> Self {
        Self {
            run_id: record.run_id,
            status: record.status,
            request: record.request,
            outcome: record.outcome,
            error: record.error,
        }
    }
}

struct MctsApiError {
    status: StatusCode,
    message: String,
}

impl IntoResponse for MctsApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({"error": self.message})),
        )
            .into_response()
    }
}

pub fn mcts_router(state: MctsHttpState) -> Router {
    Router::new()
        .route("/api/v1/mcts/runs", post(create_run))
        .route("/api/v1/mcts/runs/:run_id", get(get_run))
        .route("/api/v1/mcts/runs/:run_id/cancel", post(cancel_run))
        .with_state(state.clone())
}

const MCTS_PLUGIN_ENV_ALLOWLIST: &[&str] = &[
    "ZSOFT_TASKS_DIR",
    "PYTHONPATH",
    "ZSOFT_TRIAL_SERVER_URL",
    "XIAOO_MCTS_TOKEN_FUSE_LIMIT",
];

fn collect_plugin_envs() -> Vec<(String, String)> {
    MCTS_PLUGIN_ENV_ALLOWLIST
        .iter()
        .filter_map(|key| std::env::var(key).ok().map(|value| ((*key).to_string(), value)))
        .collect()
}

async fn create_run(
    State(state): State<MctsHttpState>,
    Json(mut request): Json<MctsRunRequest>,
) -> Result<(StatusCode, Json<MctsRunView>), MctsApiError> {
    if request.plugin_id != "mcts" {
        return Err(MctsApiError {
            status: StatusCode::BAD_REQUEST,
            message: "plugin_id must be 'mcts'".to_string(),
        });
    }
    if request.mode.trim().is_empty() {
        return Err(MctsApiError {
            status: StatusCode::BAD_REQUEST,
            message: "mode must not be empty".to_string(),
        });
    }
    if request.run_id.trim().is_empty() {
        request.run_id = uuid::Uuid::new_v4().to_string();
    }

    let mut runs = state.runs.lock().map_err(|_| MctsApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "mcts state lock poisoned".to_string(),
    })?;
    if runs.contains_key(&request.run_id) {
        return Err(MctsApiError {
            status: StatusCode::CONFLICT,
            message: format!("run already exists: {}", request.run_id),
        });
    }

    let host_service = if state.config.spawn_enabled {
        let run_map = RunMap::new();
        if let Err(error) = run_map.register_run(&request.run_id) {
            return Err(MctsApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to register MCTS run map: {error}"),
            });
        }
        if let Err(error) = run_map.register_root_state(&request.run_id, &request.root_state_id) {
            return Err(MctsApiError {
                status: StatusCode::BAD_REQUEST,
                message: format!("failed to adopt root state: {error}"),
            });
        }
        Some(Arc::new(HostCapService::new(FakeApplication::new(), run_map)))
    } else {
        None
    };

    let record = MctsRunRecord {
        run_id: request.run_id.clone(),
        status: MctsRunStatus::Accepted,
        request: request.clone(),
        outcome: None,
        error: None,
    };
    runs.insert(record.run_id.clone(), record);
    drop(runs);

    if let Some(host_service) = host_service {
        spawn_mcts_task(state.clone(), request.clone(), host_service);
    }

    Ok((
        StatusCode::CREATED,
        Json(MctsRunView {
            run_id: request.run_id.clone(),
            status: MctsRunStatus::Accepted,
            request,
            outcome: None,
            error: None,
        }),
    ))
}

fn spawn_mcts_task(
    state: MctsHttpState,
    request: MctsRunRequest,
    host_service: Arc<HostCapService<FakeApplication>>,
) {
    let run_id = request.run_id.clone();
    let command = build_launch_command(
        state.config.python_bin.clone(),
        state.config.repo_root.clone(),
        &request,
    );
    let run = MctsRun::new(command, collect_plugin_envs(), request, host_service);
    let runs = state.runs.clone();

    tokio::spawn(async move {
        let outcome = run.execute().await;
        if let Ok(mut records) = runs.lock() {
            if let Some(record) = records.get_mut(&run_id) {
                if record.status == MctsRunStatus::Cancelled {
                    return;
                }
                match outcome {
                    Ok(mcts_outcome) => {
                        record.status = mcts_outcome.status;
                        record.outcome = Some(mcts_outcome);
                    }
                    Err(error) => {
                        record.status = MctsRunStatus::Failed;
                        record.error = Some(error.to_string());
                    }
                }
            }
        }
    });
}

async fn get_run(
    State(state): State<MctsHttpState>,
    Path(run_id): Path<String>,
) -> Result<Json<MctsRunView>, MctsApiError> {
    let runs = state.runs.lock().map_err(|_| MctsApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "mcts state lock poisoned".to_string(),
    })?;
    let record = runs.get(&run_id).ok_or_else(|| MctsApiError {
        status: StatusCode::NOT_FOUND,
        message: format!("run not found: {run_id}"),
    })?;
    Ok(Json(MctsRunView::from(record.clone())))
}

async fn cancel_run(
    State(state): State<MctsHttpState>,
    Path(run_id): Path<String>,
) -> Result<Json<MctsRunView>, MctsApiError> {
    let mut runs = state.runs.lock().map_err(|_| MctsApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "mcts state lock poisoned".to_string(),
    })?;
    let record = runs.get_mut(&run_id).ok_or_else(|| MctsApiError {
        status: StatusCode::NOT_FOUND,
        message: format!("run not found: {run_id}"),
    })?;
    if !matches!(
        record.status,
        MctsRunStatus::Completed | MctsRunStatus::Failed
    ) {
        record.status = MctsRunStatus::Cancelled;
    }
    Ok(Json(MctsRunView::from(record.clone())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn request_json() -> serde_json::Value {
        serde_json::json!({
            "plugin_id": "mcts",
            "mode": "zsoft",
            "task_id": "civetweb-002-websocket-deflate-oob",
            "run_id": "run-mcts-http",
            "root_state_id": "checkpoint-abc",
            "goal": "fix the websocket issue",
            "content": "content",
            "budget": {
                "max_nodes": 5,
                "max_time_ms": 1800000,
                "max_tokens": 3000000
            },
            "granted_capabilities": ["snapshot", "fork", "agent_turn"]
        })
    }

    #[test]
    fn plugin_env_allowlist_excludes_llm_credentials() {
        assert!(
            MCTS_PLUGIN_ENV_ALLOWLIST
                .iter()
                .all(|key| !key.starts_with("XGOVERNOR_LLM_"))
        );
    }

    fn test_router() -> Router {
        mcts_router(MctsHttpState::disabled_for_tests())
    }

    #[test]
    fn parses_valid_request_schema() {
        let request: MctsRunRequest = serde_json::from_value(request_json()).unwrap();
        assert_eq!(request.plugin_id, "mcts");
        assert_eq!(request.budget.max_nodes, 5);
        assert_eq!(request.granted_capabilities.len(), 3);
    }

    #[tokio::test]
    async fn creates_gets_and_cancels_run() {
        let router = test_router();

        let create = router
            .clone()
            .oneshot(
                Request::post("/api/v1/mcts/runs")
                    .header("content-type", "application/json")
                    .body(Body::from(request_json().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);

        let response = router
            .clone()
            .oneshot(
                Request::get("/api/v1/mcts/runs/run-mcts-http")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(status, StatusCode::OK);
        let view: MctsRunView = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(view.status, MctsRunStatus::Accepted);
        assert!(view.outcome.is_none());

        let cancel = router
            .oneshot(
                Request::post("/api/v1/mcts/runs/run-mcts-http/cancel")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancel.status(), StatusCode::OK);
        let bytes = cancel.into_body().collect().await.unwrap().to_bytes();
        let view: MctsRunView = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(view.status, MctsRunStatus::Cancelled);
    }

    #[tokio::test]
    async fn rejects_wrong_plugin_id() {
        let router = test_router();
        let mut body = request_json();
        body["plugin_id"] = serde_json::json!("other");
        let response = router
            .oneshot(
                Request::post("/api/v1/mcts/runs")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn http_route_spawns_real_mock_mcts_process() {
        let Ok(python_bin) = std::env::var("MCTS_PYTHON_BIN") else {
            eprintln!("SKIP: MCTS_PYTHON_BIN is not set");
            return;
        };
        let Ok(repo_root) = std::env::var("MCTS_REPO_ROOT") else {
            eprintln!("SKIP: MCTS_REPO_ROOT is not set");
            return;
        };

        let state = MctsHttpState::new(MctsLaunchConfig {
            python_bin,
            repo_root: repo_root.into(),
            spawn_enabled: true,
        });
        let router = mcts_router(state);
        let run_id = format!("run-http-real-{}", std::process::id());
        let body = serde_json::json!({
            "plugin_id": "mcts",
            "mode": "mock",
            "run_id": run_id,
            "root_state_id": "state-root",
            "goal": "http spawn smoke test",
            "content": "content",
            "budget": {
                "max_nodes": 2,
                "max_time_ms": 15000,
                "max_tokens": 1000
            }
        });

        let create = router
            .clone()
            .oneshot(
                Request::post("/api/v1/mcts/runs")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);

        for _ in 0..40 {
            let response = router
                .clone()
                .oneshot(
                    Request::get(format!("/api/v1/mcts/runs/{run_id}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let view: MctsRunView = serde_json::from_slice(&bytes).unwrap();
            if view.status == MctsRunStatus::Completed {
                assert!(view.outcome.is_some());
                return;
            }
            if matches!(view.status, MctsRunStatus::Failed | MctsRunStatus::Cancelled) {
                panic!("unexpected terminal status: {:?} {:?}", view.status, view.error);
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }

        panic!("MCTS mock run did not complete within the test timeout");
    }
}
