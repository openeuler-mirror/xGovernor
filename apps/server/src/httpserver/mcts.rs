use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Extension;
use axum::{Json, Router};
use mcts_plugin_host::{
    build_launch_command, FakeApplication, HostApplication, HostCapService, MctsRun,
    MctsRunOutcome, MctsRunRequest, MctsRunStatus, RunMap,
};
use serde::{Deserialize, Serialize};
use session_protocol::{SessionCheckpointDeleteRequest, SessionLeaseClaim};
use xgovernor_core::{SecurityContext, SessionApplication};

use super::mcts_host::RealSessionHostApplication;
use super::mcts_root_state::prepare_native_root_state;

#[derive(Clone)]
pub struct MctsHttpState {
    runs: Arc<Mutex<HashMap<String, MctsRunRecord>>>,
    cancellations: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
    config: Arc<MctsLaunchConfig>,
    session: Option<SessionApplication>,
}

#[derive(Clone)]
pub struct MctsLaunchConfig {
    python_bin: String,
    repo_root: PathBuf,
    spawn_enabled: bool,
    base_url: String,
    root_state_token: Option<String>,
}

impl Default for MctsLaunchConfig {
    fn default() -> Self {
        Self {
            python_bin: std::env::var("MCTS_PYTHON_BIN").unwrap_or_else(|_| "python".to_string()),
            repo_root: std::env::var("MCTS_REPO_ROOT")
                .unwrap_or_else(|_| ".".to_string())
                .into(),
            spawn_enabled: true,
            base_url: std::env::var("XGOVERNOR_BASE_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8787".to_string()),
            root_state_token: std::env::var("XGOVERNOR_ADMIN_TOKEN").ok(),
        }
    }
}

impl MctsHttpState {
    pub fn new(config: MctsLaunchConfig) -> Self {
        Self {
            runs: Arc::new(Mutex::new(HashMap::new())),
            cancellations: Arc::new(Mutex::new(HashMap::new())),
            config: Arc::new(config),
            session: None,
        }
    }

    pub fn with_session(config: MctsLaunchConfig, session: SessionApplication) -> Self {
        Self {
            runs: Arc::new(Mutex::new(HashMap::new())),
            cancellations: Arc::new(Mutex::new(HashMap::new())),
            config: Arc::new(config),
            session: Some(session),
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
    #[serde(default)]
    root_state_prepared: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    root_state_id: Option<String>,
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
    root_state_prepared: bool,
    root_state_id: Option<String>,
    outcome: Option<MctsRunOutcome>,
    error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    protocol_jsonl: Option<String>,
}

impl From<MctsRunRecord> for MctsRunView {
    fn from(record: MctsRunRecord) -> Self {
        let protocol_jsonl = record
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.protocol.clone());
        Self {
            run_id: record.run_id,
            status: record.status,
            request: record.request,
            root_state_prepared: record.root_state_prepared,
            root_state_id: record.root_state_id,
            outcome: record.outcome,
            error: record.error,
            protocol_jsonl,
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
    "ZSOFT_BUNDLE_DIR",
    "ZSOFT_BIN",
    "ZSOFT_SERVER_URL",
    "ZSOFT_BENCH_ROOT",
    "PYTHONPATH",
    "ZSOFT_TRIAL_SERVER_URL",
    "XIAOO_MCTS_TOKEN_FUSE_LIMIT",
    "XIAOO_MCTS_MAX_CONCURRENT",
];

fn collect_plugin_envs() -> Vec<(String, String)> {
    MCTS_PLUGIN_ENV_ALLOWLIST
        .iter()
        .filter_map(|key| {
            std::env::var(key)
                .ok()
                .map(|value| ((*key).to_string(), value))
        })
        .collect()
}

async fn create_run(
    State(state): State<MctsHttpState>,
    context: Option<Extension<SecurityContext>>,
    Json(mut request): Json<MctsRunRequest>,
) -> Result<(StatusCode, Json<MctsRunView>), MctsApiError> {
    let ctx = context
        .map(|Extension(ctx)| ctx)
        .unwrap_or_else(|| SecurityContext::admin("system:mcts-worker"));
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

    {
        let runs = state.runs.lock().map_err(|_| MctsApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "mcts state lock poisoned".to_string(),
        })?;
        if runs.contains_key(&request.run_id) {
            return Err(MctsApiError {
                status: StatusCode::CONFLICT,
                message: format!("run already exists: {}", request.run_id),
            });
        }
    }

    let mut root_state_prepared = false;
    if state.config.spawn_enabled && request.root_state_id.is_none() {
        let task_id = request.task_id.as_deref().ok_or_else(|| MctsApiError {
            status: StatusCode::BAD_REQUEST,
            message: "task_id is required when xg prepares the root state".to_string(),
        })?;
        let prepared = prepare_native_root_state(
            &state.config.python_bin,
            &state.config.repo_root,
            &state.config.base_url,
            state.config.root_state_token.as_deref(),
            &request.mode,
            task_id,
            &request.run_id,
        )
        .await
        .map_err(|error| MctsApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("native root-state preparation failed: {error}"),
        })?;
        request.root_state_id = Some(prepared.root_state_id);
        root_state_prepared = true;
    }

    let cancel = Arc::new(AtomicBool::new(false));
    if state.config.spawn_enabled {
        let root_state_id = request
            .root_state_id
            .as_deref()
            .ok_or_else(|| MctsApiError {
                status: StatusCode::BAD_REQUEST,
                message: "root_state_id must not be empty".to_string(),
            })?;
        let run_map = RunMap::new();
        let run_tenant_id = ctx.tenant_id().map(str::to_string);
        if let Err(error) = run_map.register_run_with_tenant(&request.run_id, run_tenant_id) {
            return Err(MctsApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to register MCTS run map: {error}"),
            });
        }
        if let Err(error) = run_map.register_root_state(&request.run_id, root_state_id) {
            return Err(MctsApiError {
                status: StatusCode::BAD_REQUEST,
                message: format!("failed to adopt root state: {error}"),
            });
        }

        if let Some(session) = state.session.clone() {
            let application = RealSessionHostApplication::new(
                session,
                ctx.clone(),
                request.mode.clone(),
                &request.run_id,
                &state.config.repo_root,
                None,
                cancel.clone(),
            )
            .map_err(|error| MctsApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to build real MCTS host application: {error}"),
            })?;
            spawn_mcts_task(
                state.clone(),
                request.clone(),
                Arc::new(HostCapService::new(application, run_map)),
                cancel.clone(),
            );
        } else {
            spawn_mcts_task(
                state.clone(),
                request.clone(),
                Arc::new(HostCapService::new(FakeApplication::new(), run_map)),
                cancel.clone(),
            );
        }
    }

    {
        let mut cancellations = state.cancellations.lock().map_err(|_| MctsApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "mcts cancellation state lock poisoned".to_string(),
        })?;
        cancellations.insert(request.run_id.clone(), cancel);
    }

    let status = if state.config.spawn_enabled {
        MctsRunStatus::Running
    } else {
        MctsRunStatus::Accepted
    };
    let root_state_id = request.root_state_id.clone();
    let record = MctsRunRecord {
        run_id: request.run_id.clone(),
        status,
        request: request.clone(),
        root_state_prepared,
        root_state_id: request.root_state_id.clone(),
        outcome: None,
        error: None,
    };
    {
        let mut runs = state.runs.lock().map_err(|_| MctsApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "mcts state lock poisoned".to_string(),
        })?;
        runs.insert(record.run_id.clone(), record);
    }

    Ok((
        StatusCode::CREATED,
        Json(MctsRunView {
            run_id: request.run_id.clone(),
            status,
            request,
            root_state_prepared,
            root_state_id,
            outcome: None,
            error: None,
            protocol_jsonl: None,
        }),
    ))
}

fn spawn_mcts_task<A: HostApplication + 'static>(
    state: MctsHttpState,
    request: MctsRunRequest,
    host_service: Arc<HostCapService<A>>,
    cancel: Arc<AtomicBool>,
) {
    let run_id = request.run_id.clone();
    let command = build_launch_command(
        state.config.python_bin.clone(),
        state.config.repo_root.clone(),
        &request,
    );
    let run =
        MctsRun::new(command, collect_plugin_envs(), request, host_service).with_cancel(cancel);
    let runs = state.runs.clone();
    let cancellations = state.cancellations.clone();

    tokio::spawn(async move {
        let outcome = run.execute().await;
        let mut prepared_state_id = None;
        if let Ok(mut records) = runs.lock() {
            if let Some(record) = records.get_mut(&run_id) {
                if record.root_state_prepared {
                    prepared_state_id = record.root_state_id.clone();
                }
                if record.status != MctsRunStatus::Cancelled {
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
        }
        if let (Some(state_id), Some(session)) = (prepared_state_id, state.session.clone()) {
            cleanup_prepared_root_state(&session, &run_id, &state_id).await;
        }
        if let Ok(mut map) = cancellations.lock() {
            map.remove(&run_id);
        }
    });
}

async fn cleanup_prepared_root_state(session: &SessionApplication, run_id: &str, state_id: &str) {
    let result = session
        .delete_checkpoint(
            &SecurityContext::admin("system:mcts-worker"),
            SessionCheckpointDeleteRequest {
                checkpoint_id: state_id.to_string(),
                lease: SessionLeaseClaim {
                    client_id: Some(format!("mcts-run:{run_id}")),
                    client_pid: None,
                    client_hostname: None,
                },
            },
        )
        .await;
    if let Err(error) = result {
        tracing::warn!(run_id, state_id, error = %error, "MCTS prepared root state cleanup failed");
    }
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
    if let Ok(cancellations) = state.cancellations.lock() {
        if let Some(cancel) = cancellations.get(&run_id) {
            cancel.store(true, Ordering::SeqCst);
        }
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
                "max_iters": 5,
                "branching": 3,
                "max_time_ms": 1800000,
                "max_tokens": 3000000
            },
            "granted_capabilities": ["snapshot", "fork", "agent_turn"]
        })
    }

    #[test]
    fn plugin_env_allowlist_excludes_llm_credentials() {
        assert!(MCTS_PLUGIN_ENV_ALLOWLIST
            .iter()
            .all(|key| !key.starts_with("XGOVERNOR_LLM_")));
    }

    fn test_router() -> Router {
        mcts_router(MctsHttpState::disabled_for_tests())
    }

    #[test]
    fn parses_valid_request_schema() {
        let request: MctsRunRequest = serde_json::from_value(request_json()).unwrap();
        assert_eq!(request.plugin_id, "mcts");
        assert_eq!(request.budget.max_iters, 5);
        assert_eq!(request.budget.branching, 3);
        assert_eq!(request.granted_capabilities.len(), 3);
    }

    #[test]
    fn parses_request_without_root_state_id() {
        let mut body = request_json();
        body.as_object_mut().unwrap().remove("root_state_id");
        let request: MctsRunRequest = serde_json::from_value(body).unwrap();
        assert_eq!(request.root_state_id, None);
    }

    #[tokio::test]
    async fn creates_run_without_root_state_id_when_spawn_disabled() {
        let router = test_router();
        let mut body = request_json();
        body.as_object_mut().unwrap().remove("root_state_id");

        let response = router
            .oneshot(
                Request::post("/api/v1/mcts/runs")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let view: MctsRunView = serde_json::from_slice(&bytes).unwrap();
        assert!(!view.root_state_prepared);
        assert!(view.root_state_id.is_none());
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
            base_url: "http://127.0.0.1:8787".to_string(),
            root_state_token: None,
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
                "max_iters": 2,
                "branching": 1,
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
            if matches!(
                view.status,
                MctsRunStatus::Failed | MctsRunStatus::Cancelled
            ) {
                panic!(
                    "unexpected terminal status: {:?} {:?}",
                    view.status, view.error
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }

        panic!("MCTS mock run did not complete within the test timeout");
    }
}
