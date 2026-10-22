use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mcts_protocol::messages::JSONRPC_VERSION;
use mcts_protocol::methods::{Budget, PluginInitializeParams, PluginStartParams, PluginWaitParams};
use mcts_protocol::notifications::{
    PluginCancelledParams, PluginCompletedParams, PluginFailedParams,
};
use mcts_protocol::{
    Capability, CompletionResult, Failure, MethodName, NotificationName, Request, Response,
    RpcEnvelope, RpcId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::process::Command;

use crate::error::PluginHostError;
use crate::host_cap_service::{HostApplication, HostCapService};
use crate::llm_policy::LlmPolicy;
use crate::manifest::PLUGIN_ID;
use crate::process::PluginProcess;

pub const DEFAULT_RUN_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_WAIT_POLL_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MctsRunStatus {
    Accepted,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl Default for MctsRunStatus {
    fn default() -> Self {
        Self::Accepted
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LlmConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_base: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MctsRunRequest {
    pub plugin_id: String,
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split: Option<String>,
    pub run_id: String,
    pub root_state_id: String,
    pub goal: String,
    pub content: String,
    pub budget: Budget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub llm: Option<LlmConfig>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub granted_capabilities: BTreeSet<Capability>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MctsRunOutcome {
    pub status: MctsRunStatus,
    pub sequence: u64,
    pub summary: Option<String>,
    pub result: Option<CompletionResult>,
    pub failure: Option<Failure>,
}

#[derive(Debug, Default)]
struct RunProgress {
    status: MctsRunStatus,
    sequence: u64,
    summary: Option<String>,
    result: Option<CompletionResult>,
    failure: Option<Failure>,
}

impl RunProgress {
    fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            MctsRunStatus::Completed | MctsRunStatus::Failed | MctsRunStatus::Cancelled
        )
    }

    fn into_outcome(self) -> MctsRunOutcome {
        MctsRunOutcome {
            status: self.status,
            sequence: self.sequence,
            summary: self.summary,
            result: self.result,
            failure: self.failure,
        }
    }
}

pub struct MctsRun<A: HostApplication> {
    command: Command,
    envs: Vec<(String, String)>,
    request: MctsRunRequest,
    host_cap_service: Arc<HostCapService<A>>,
    llm_policy: LlmPolicy,
    request_timeout: Duration,
    wait_poll_timeout: Duration,
    available_capabilities: BTreeSet<Capability>,
}

impl<A: HostApplication> MctsRun<A> {
    pub fn new(
        command: Command,
        envs: Vec<(String, String)>,
        request: MctsRunRequest,
        host_cap_service: Arc<HostCapService<A>>,
    ) -> Self {
        Self {
            command,
            envs,
            request,
            host_cap_service,
            llm_policy: LlmPolicy::default_for_mcts(),
            request_timeout: DEFAULT_RUN_REQUEST_TIMEOUT,
            wait_poll_timeout: DEFAULT_WAIT_POLL_TIMEOUT,
            available_capabilities: all_capabilities(),
        }
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_wait_poll_timeout(mut self, timeout: Duration) -> Self {
        self.wait_poll_timeout = timeout;
        self
    }

    pub fn with_available_capabilities(mut self, capabilities: BTreeSet<Capability>) -> Self {
        self.available_capabilities = capabilities;
        self
    }

    pub fn with_llm_policy(mut self, llm_policy: LlmPolicy) -> Self {
        self.llm_policy = llm_policy;
        self
    }

    pub fn resolved_llm(&self) -> String {
        self.llm_policy.resolve(
            self.request
                .llm
                .as_ref()
                .and_then(|llm| llm.model.as_deref()),
        )
    }

    pub async fn execute(self) -> Result<MctsRunOutcome, PluginHostError> {
        validate_request(&self.request)?;

        let plugin_id = self.request.plugin_id.clone();
        let run_id = self.request.run_id.clone();
        self.host_cap_service.run_map().register_run(&run_id)?;

        let envs: Vec<(&str, &str)> = self
            .envs
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        self.llm_policy.validate_env(&envs)?;
        let process = PluginProcess::spawn(self.command, &envs).await?;
        let mut progress = RunProgress {
            status: MctsRunStatus::Accepted,
            ..RunProgress::default()
        };

        let initialize_response = request_and_dispatch(
            &process,
            &self.host_cap_service,
            "mcts-run-initialize",
            MethodName::PluginInitialize,
            serde_json::to_value(PluginInitializeParams {
                plugin_id: plugin_id.clone(),
                supported_api_versions: vec!["mcts.rpc.v1".to_string()],
                host_version: "xgovernor-dev".to_string(),
                available_capabilities: self.available_capabilities.clone(),
            })?,
            self.request_timeout,
            &mut progress,
        )
        .await?;
        ensure_response_ok(&initialize_response)?;

        let start_response = request_and_dispatch(
            &process,
            &self.host_cap_service,
            "mcts-run-start",
            MethodName::PluginStart,
            serde_json::to_value(PluginStartParams {
                plugin_id: plugin_id.clone(),
                run_id: run_id.clone(),
                session_id: format!("{run_id}-session"),
                turn_id: format!("{run_id}-turn"),
                root_state_id: self.request.root_state_id.clone(),
                goal: self.request.goal.clone(),
                content: self.request.content.clone(),
                budget: self.request.budget.clone(),
                granted_capabilities: self.request.granted_capabilities.clone(),
            })?,
            self.request_timeout,
            &mut progress,
        )
        .await?;
        ensure_response_ok(&start_response)?;

        if progress.status == MctsRunStatus::Accepted {
            progress.status = MctsRunStatus::Running;
        }

        let mut after_sequence = 0u64;
        while !progress.is_terminal() {
            let wait_response = request_and_dispatch(
                &process,
                &self.host_cap_service,
                "mcts-run-wait",
                MethodName::PluginWait,
                serde_json::to_value(PluginWaitParams {
                    plugin_id: plugin_id.clone(),
                    run_id: run_id.clone(),
                    after_sequence,
                    timeout_ms: self.wait_poll_timeout.as_millis().min(u128::from(u64::MAX)) as u64,
                })?,
                self.request_timeout,
                &mut progress,
            )
            .await?;
            ensure_response_ok(&wait_response)?;

            let snapshot: WaitSnapshot =
                serde_json::from_value(wait_response.result.clone().ok_or_else(|| {
                    PluginHostError::InvalidResponse("missing wait result".to_string())
                })?)?;
            after_sequence = after_sequence.max(snapshot.sequence);

            if snapshot.status == "completed" {
                progress.status = MctsRunStatus::Completed;
            } else if snapshot.status == "failed" {
                progress.status = MctsRunStatus::Failed;
            } else if snapshot.status == "cancelled" {
                progress.status = MctsRunStatus::Cancelled;
            }
        }

        let _ = process.shutdown().await;
        Ok(progress.into_outcome())
    }
}

fn all_capabilities() -> BTreeSet<Capability> {
    [
        Capability::Snapshot,
        Capability::Fork,
        Capability::AgentTurn,
        Capability::Inspect,
        Capability::Exec,
        Capability::FileRead,
        Capability::FileWrite,
        Capability::Diff,
        Capability::Release,
    ]
    .into_iter()
    .collect()
}

pub fn build_launch_command(
    python_bin: impl AsRef<std::ffi::OsStr>,
    repo_root: impl AsRef<std::path::Path>,
    request: &MctsRunRequest,
) -> Command {
    let mut command = Command::new(python_bin);
    match request.mode.as_str() {
        "mock" => {
            command
                .arg("-m")
                .arg("testcases_union.core.main")
                .arg("--mock");
        }
        "zsoft" => {
            command
                .arg("-m")
                .arg("testcases_union.core.main")
                .arg("--mode")
                .arg("zsoft");
            if let Some(task_id) = request.task_id.as_deref() {
                command.arg("--task-id").arg(task_id);
            }
        }
        "sweverified" => {
            command
                .arg("-m")
                .arg("testcases_union.core.main")
                .arg("--mode")
                .arg("sweverified");
            if let Some(instance_id) = request.instance_id.as_deref() {
                command.arg("--instance-id").arg(instance_id);
            }
            if let Some(split) = request.split.as_deref() {
                command.arg("--split").arg(split);
            }
        }
        _ => {}
    }
    command.current_dir(repo_root);
    command
}

fn validate_request(request: &MctsRunRequest) -> Result<(), PluginHostError> {
    if request.plugin_id != PLUGIN_ID {
        return Err(PluginHostError::Manifest(format!(
            "request plugin_id must be {PLUGIN_ID}"
        )));
    }
    if request.mode.trim().is_empty() {
        return Err(PluginHostError::Manifest(
            "mode must not be empty".to_string(),
        ));
    }
    if request.run_id.trim().is_empty() {
        return Err(PluginHostError::Manifest(
            "run_id must not be empty".to_string(),
        ));
    }
    if request.root_state_id.trim().is_empty() {
        return Err(PluginHostError::Manifest(
            "root_state_id must not be empty".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct WaitSnapshot {
    #[allow(dead_code)]
    run_id: String,
    status: String,
    sequence: u64,
}

async fn request_and_dispatch<A: HostApplication>(
    process: &PluginProcess,
    host_cap_service: &Arc<HostCapService<A>>,
    id: &str,
    method: MethodName,
    params: Value,
    timeout: Duration,
    progress: &mut RunProgress,
) -> Result<Response, PluginHostError> {
    let request = RpcEnvelope::Request(Request {
        jsonrpc: JSONRPC_VERSION.to_string(),
        id: RpcId::String(id.to_string()),
        method,
        params,
    });
    process.send(&request).await?;

    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(PluginHostError::DeadlineExceeded)?;
        let message = tokio::time::timeout(remaining, process.next_message())
            .await
            .map_err(|_| PluginHostError::DeadlineExceeded)??;

        match message {
            RpcEnvelope::Response(response) => {
                if id_matches(&response.id, id) {
                    return Ok(response);
                }
            }
            RpcEnvelope::Request(request) => {
                let response = host_cap_service
                    .handle(request.id, request.method, request.params)
                    .await;
                process.send(&RpcEnvelope::Response(response)).await?;
            }
            RpcEnvelope::Notification(notification) => {
                apply_notification(notification.method, notification.params, progress)?;
            }
        }
    }
}

fn id_matches(id: &RpcId, expected: &str) -> bool {
    match id {
        RpcId::String(value) => value == expected,
        RpcId::Number(value) => value.to_string() == expected,
    }
}

fn apply_notification(
    method: NotificationName,
    params: Value,
    progress: &mut RunProgress,
) -> Result<(), PluginHostError> {
    match method {
        NotificationName::PluginStarted => {
            let params: mcts_protocol::notifications::PluginStartedParams =
                serde_json::from_value(params)?;
            progress.status = MctsRunStatus::Running;
            progress.sequence = progress.sequence.max(params.sequence);
            progress.summary = params.summary;
        }
        NotificationName::PluginCompleted => {
            let params: PluginCompletedParams = serde_json::from_value(params)?;
            progress.status = MctsRunStatus::Completed;
            progress.sequence = progress.sequence.max(params.sequence);
            progress.summary = Some("completed".to_string());
            progress.result = Some(params.result);
        }
        NotificationName::PluginCancelled => {
            let params: PluginCancelledParams = serde_json::from_value(params)?;
            progress.status = MctsRunStatus::Cancelled;
            progress.sequence = progress.sequence.max(params.sequence);
            progress.summary = Some(params.summary);
        }
        NotificationName::PluginFailed => {
            let params: PluginFailedParams = serde_json::from_value(params)?;
            progress.status = MctsRunStatus::Failed;
            progress.sequence = progress.sequence.max(params.sequence);
            progress.summary = Some(params.summary);
            progress.failure = Some(params.failure);
        }
        _ => {}
    }
    Ok(())
}

fn ensure_response_ok(response: &Response) -> Result<(), PluginHostError> {
    if response.error.is_some() {
        Err(PluginHostError::InvalidResponse(
            "plugin returned a JSON-RPC error".to_string(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_cap_service::FakeApplication;
    use crate::run_map::RunMap;
    use serde_json::json;

    fn stub_command() -> Command {
        let test_binary = std::env::current_exe().expect("current test binary");
        let stub_binary = test_binary
            .parent()
            .expect("target/deps directory")
            .parent()
            .expect("target directory")
            .join("stub-plugin");
        Command::new(stub_binary)
    }

    fn request() -> MctsRunRequest {
        serde_json::from_value(json!({
            "plugin_id": "mcts",
            "mode": "mock",
            "run_id": "run-mcts-day4",
            "root_state_id": "state-root",
            "goal": "validate lifecycle",
            "content": "content",
            "budget": {"max_nodes": 2, "max_time_ms": 5000, "max_tokens": 1000}
        }))
        .unwrap()
    }

    #[test]
    fn resolves_default_llm_and_builds_mock_command() {
        let mut request = request();
        request.mode = "mock".to_string();
        let run = MctsRun::new(
            Command::new("python"),
            vec![],
            request.clone(),
            host_service(),
        );
        assert_eq!(run.resolved_llm(), "DeepSeek-V4-Pro");

        let command = build_launch_command("python3", "/repo", &request);
        assert!(command
            .as_std()
            .get_program()
            .to_string_lossy()
            .contains("python3"));
    }

    fn host_service() -> Arc<HostCapService<FakeApplication>> {
        let run_map = RunMap::new();
        let service = HostCapService::new(FakeApplication::new(), run_map);
        Arc::new(service)
    }

    #[tokio::test]
    async fn runs_initialize_start_wait_and_reaches_terminal() {
        let run = MctsRun::new(stub_command(), vec![], request(), host_service())
            .with_wait_poll_timeout(Duration::from_millis(100));
        let outcome = run.execute().await.unwrap();
        assert_eq!(outcome.status, MctsRunStatus::Completed);
        assert!(outcome.sequence >= 1);
    }

    #[test]
    fn rejects_non_mcts_plugin_request() {
        let mut bad = request();
        bad.plugin_id = "other".to_string();
        assert!(validate_request(&bad).is_err());
    }

    #[test]
    fn llm_config_has_stable_defaults() {
        let llm: LlmConfig = serde_json::from_value(json!({})).unwrap();
        assert!(llm.provider.is_none());
        assert!(llm.model.is_none());
    }
}
