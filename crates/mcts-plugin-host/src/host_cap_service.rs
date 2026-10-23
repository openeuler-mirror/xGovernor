use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use mcts_protocol::{Failure, MethodName, Response, RpcErrorCode, RpcId};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::PluginHostError;
use crate::manifest::PLUGIN_ID;
use crate::run_map::{JobStatus, RunMap};

#[derive(Debug, Clone, PartialEq)]
pub struct HostJobOutcome {
    pub status: JobStatus,
    pub output: String,
    pub exit_code: Option<i64>,
    pub usage: BTreeMap<String, u64>,
    pub failure: Option<Failure>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostSnapshot {
    pub state_id: String,
    pub expires_at: String,
    /// Internal lease-expiry metadata. Not part of the stdio wire result;
    /// [`HostCapService`] copies it into [`RunMap`] so `apply_plan`
    /// validation can enforce freshness.
    pub expires_at_ms: Option<u64>,
    pub tenant_id: Option<String>,
}

#[async_trait]
pub trait HostApplication: Send + Sync {
    async fn snapshot(&self, run_id: &str, branch_id: &str) -> Result<HostSnapshot, PluginHostError>;
    async fn fork(&self, run_id: &str, parent_state_id: &str) -> Result<String, PluginHostError>;
    async fn agent_turn(
        &self,
        run_id: &str,
        branch_id: &str,
        input: &str,
        profile: Option<&str>,
    ) -> Result<String, PluginHostError>;
    async fn inspect(&self, run_id: &str, branch_id: &str, view: &str) -> Result<String, PluginHostError>;
    async fn exec(
        &self,
        run_id: &str,
        branch_id: &str,
        argv: &[String],
        cwd: &str,
    ) -> Result<String, PluginHostError>;
    async fn file_read(&self, run_id: &str, branch_id: &str, path: &str) -> Result<Vec<u8>, PluginHostError>;
    async fn file_write(
        &self,
        run_id: &str,
        branch_id: &str,
        path: &str,
        data: &[u8],
    ) -> Result<u64, PluginHostError>;
    async fn heartbeat(
        &self,
        run_id: &str,
        branch_id: &str,
        client_id: &str,
    ) -> Result<(), PluginHostError>;
    async fn diff(&self, run_id: &str, branch_id: &str, base_state_id: &str) -> Result<String, PluginHostError>;
    async fn release(&self, run_id: &str, branch_id: &str) -> Result<(), PluginHostError>;
    async fn wait_job(&self, run_id: &str, job_id: &str) -> Result<HostJobOutcome, PluginHostError>;
}

pub struct HostCapService<A: HostApplication> {
    app: Arc<A>,
    run_map: Arc<RunMap>,
}

impl<A: HostApplication> HostCapService<A> {
    pub fn new(app: A, run_map: RunMap) -> Self {
        Self {
            app: Arc::new(app),
            run_map: Arc::new(run_map),
        }
    }

    pub fn run_map(&self) -> Arc<RunMap> {
        self.run_map.clone()
    }

    pub fn application(&self) -> Arc<A> {
        self.app.clone()
    }

    pub async fn handle(&self, id: RpcId, method: MethodName, params: Value) -> Response {
        match method {
            MethodName::HostSnapshot => self.snapshot(id, params).await,
            MethodName::HostFork => self.fork(id, params).await,
            MethodName::HostAgentTurn => self.agent_turn(id, params).await,
            MethodName::HostInspect => self.inspect(id, params).await,
            MethodName::HostExec => self.exec(id, params).await,
            MethodName::HostFileRead => self.file_read(id, params).await,
            MethodName::HostFileWrite => self.file_write(id, params).await,
            MethodName::HostDiff => self.diff(id, params).await,
            MethodName::HostRelease => self.release(id, params).await,
            _ => error_response(id, RpcErrorCode::MethodNotFound as i64, "not a host method"),
        }
    }

    async fn snapshot(&self, id: RpcId, params: Value) -> Response {
        let params = match parse::<HostSnapshotParams>(id.clone(), params) {
            Ok(value) => value,
            Err(response) => return response,
        };
        if let Err(response) = self.validate_plugin_and_run(&params.plugin_id, &params.run_id, id.clone()) {
            return response;
        }
        if let Err(error) = self.run_map.require_branch_in_run(&params.run_id, &params.branch_id) {
            return error_response(id, error.rpc_code(), error.to_string());
        }
        match self.app.snapshot(&params.run_id, &params.branch_id).await {
            Ok(snapshot) => {
                if let Err(error) = self.run_map.add_state_with_metadata(
                    &params.run_id,
                    &snapshot.state_id,
                    &params.branch_id,
                    snapshot.expires_at_ms,
                    snapshot.tenant_id.clone(),
                ) {
                    return error_response(id, error.rpc_code(), error.to_string());
                }
                ok_response(
                    id,
                    json!({"state_id": snapshot.state_id, "expires_at": snapshot.expires_at}),
                )
            }
            Err(error) => error_response(id, error.rpc_code(), error.to_string()),
        }
    }

    async fn fork(&self, id: RpcId, params: Value) -> Response {
        let params = match parse::<HostForkParams>(id.clone(), params) {
            Ok(value) => value,
            Err(response) => return response,
        };
        if let Err(response) = self.validate_plugin_and_run(&params.plugin_id, &params.run_id, id.clone()) {
            return response;
        }
        if let Err(error) = self.run_map.require_state(&params.parent_state_id) {
            return error_response(id, error.rpc_code(), error.to_string());
        }
        match self.app.fork(&params.run_id, &params.parent_state_id).await {
            Ok(branch_id) => {
                if let Err(error) = self.run_map.add_branch(&params.run_id, &branch_id, &params.parent_state_id) {
                    return error_response(id, error.rpc_code(), error.to_string());
                }
                ok_response(id, json!({"branch_id": branch_id}))
            }
            Err(error) => error_response(id, error.rpc_code(), error.to_string()),
        }
    }

    async fn agent_turn(&self, id: RpcId, params: Value) -> Response {
        let params = match parse::<HostAgentTurnParams>(id.clone(), params) {
            Ok(value) => value,
            Err(response) => return response,
        };
        if let Err(response) = self.validate_plugin_and_run(&params.plugin_id, &params.run_id, id.clone()) {
            return response;
        }
        if let Err(error) = self.run_map.require_branch_in_run(&params.run_id, &params.branch_id) {
            return error_response(id, error.rpc_code(), error.to_string());
        }
        match self
            .app
            .agent_turn(&params.run_id, &params.branch_id, &params.input, params.profile.as_deref())
            .await
        {
            Ok(job_id) => {
                if let Err(error) = self.run_map.add_job(&params.run_id, &params.branch_id, &job_id) {
                    return error_response(id, error.rpc_code(), error.to_string());
                }
                ok_response(id, json!({"job_id": job_id, "status": "accepted"}))
            }
            Err(error) => error_response(id, error.rpc_code(), error.to_string()),
        }
    }

    async fn inspect(&self, id: RpcId, params: Value) -> Response {
        let params = match parse::<HostInspectParams>(id.clone(), params) {
            Ok(value) => value,
            Err(response) => return response,
        };
        if let Err(response) = self.validate_plugin_and_run(&params.plugin_id, &params.run_id, id.clone()) {
            return response;
        }
        if let Err(error) = self.run_map.require_branch_in_run(&params.run_id, &params.branch_id) {
            return error_response(id, error.rpc_code(), error.to_string());
        }
        match self.app.inspect(&params.run_id, &params.branch_id, &params.view).await {
            Ok(content) => ok_response(id, json!({"content": content})),
            Err(error) => error_response(id, error.rpc_code(), error.to_string()),
        }
    }

    async fn exec(&self, id: RpcId, params: Value) -> Response {
        let params = match parse::<HostExecParams>(id.clone(), params) {
            Ok(value) => value,
            Err(response) => return response,
        };
        if let Err(response) = self.validate_plugin_and_run(&params.plugin_id, &params.run_id, id.clone()) {
            return response;
        }
        if let Err(error) = self.run_map.require_branch_in_run(&params.run_id, &params.branch_id) {
            return error_response(id, error.rpc_code(), error.to_string());
        }
        match self.app.exec(&params.run_id, &params.branch_id, &params.argv, &params.cwd).await {
            Ok(job_id) => {
                if let Err(error) = self.run_map.add_job(&params.run_id, &params.branch_id, &job_id) {
                    return error_response(id, error.rpc_code(), error.to_string());
                }
                ok_response(id, json!({"job_id": job_id, "status": "accepted"}))
            }
            Err(error) => error_response(id, error.rpc_code(), error.to_string()),
        }
    }

    async fn file_read(&self, id: RpcId, params: Value) -> Response {
        let params = match parse::<HostFileReadParams>(id.clone(), params) {
            Ok(value) => value,
            Err(response) => return response,
        };
        if let Err(response) = self.validate_plugin_and_run(&params.plugin_id, &params.run_id, id.clone()) {
            return response;
        }
        if let Err(error) = self.run_map.require_branch_in_run(&params.run_id, &params.branch_id) {
            return error_response(id, error.rpc_code(), error.to_string());
        }
        let read_result = self.app.file_read(&params.run_id, &params.branch_id, &params.path).await;
        if let Err(error) = &read_result {
            tracing::warn!(
                branch_id = %params.branch_id,
                path = %params.path,
                %error,
                "host.file_read translated to session read_file failed"
            );
        }
        match read_result {
            Ok(content) => ok_response(id, json!({"content_base64": BASE64.encode(content)})),
            Err(error) => error_response(id, error.rpc_code(), error.to_string()),
        }
    }

    async fn file_write(&self, id: RpcId, params: Value) -> Response {
        let params = match parse::<HostFileWriteParams>(id.clone(), params) {
            Ok(value) => value,
            Err(response) => return response,
        };
        if let Err(response) = self.validate_plugin_and_run(&params.plugin_id, &params.run_id, id.clone()) {
            return response;
        }
        if let Err(error) = self.run_map.require_branch_in_run(&params.run_id, &params.branch_id) {
            return error_response(id, error.rpc_code(), error.to_string());
        }
        let data = match BASE64.decode(&params.content_base64) {
            Ok(data) => data,
            Err(error) => {
                return error_response(
                    id,
                    RpcErrorCode::InvalidParams as i64,
                    format!("host.file_write content_base64 is not valid base64: {error}"),
                );
            }
        };
        match self.app.file_write(&params.run_id, &params.branch_id, &params.path, &data).await {
            Ok(bytes_written) => ok_response(id, json!({"written_bytes": bytes_written})),
            Err(error) => error_response(id, error.rpc_code(), error.to_string()),
        }
    }

    async fn diff(&self, id: RpcId, params: Value) -> Response {
        let params = match parse::<HostDiffParams>(id.clone(), params) {
            Ok(value) => value,
            Err(response) => return response,
        };
        if let Err(response) = self.validate_plugin_and_run(&params.plugin_id, &params.run_id, id.clone()) {
            return response;
        }
        if let Err(error) = self.run_map.require_branch_in_run(&params.run_id, &params.branch_id) {
            return error_response(id, error.rpc_code(), error.to_string());
        }
        match self.app.diff(&params.run_id, &params.branch_id, &params.base_state_id).await {
            Ok(diff) => ok_response(id, json!({"content": diff})),
            Err(error) => error_response(id, error.rpc_code(), error.to_string()),
        }
    }

    async fn release(&self, id: RpcId, params: Value) -> Response {
        let params = match parse::<HostReleaseParams>(id.clone(), params) {
            Ok(value) => value,
            Err(response) => return response,
        };
        if let Err(response) = self.validate_plugin_and_run(&params.plugin_id, &params.run_id, id.clone()) {
            return response;
        }
        if let Err(error) = self.run_map.require_branch_in_run(&params.run_id, &params.branch_id) {
            return error_response(id, error.rpc_code(), error.to_string());
        }
        match self.app.release(&params.run_id, &params.branch_id).await {
            Ok(()) => {
                let _ = self.run_map.remove_branch(&params.branch_id);
                ok_response(id, json!({"released": true}))
            }
            Err(error) => error_response(id, error.rpc_code(), error.to_string()),
        }
    }

    fn validate_plugin_and_run(&self, plugin_id: &str, run_id: &str, id: RpcId) -> Result<(), Response> {
        if plugin_id != PLUGIN_ID {
            return Err(error_response(
                id,
                RpcErrorCode::CapabilityDenied as i64,
                format!("host request must target plugin {PLUGIN_ID}"),
            ));
        }
        if let Err(error) = self.run_map.require_run(run_id) {
            return Err(error_response(id, error.rpc_code(), error.to_string()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct FakeApplication {
    state: Arc<Mutex<FakeApplicationState>>,
}

#[derive(Debug, Default)]
struct FakeApplicationState {
    next_branch: u64,
    next_job: u64,
    calls: Vec<String>,
}

impl FakeApplication {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn calls(&self) -> Vec<String> {
        self.state
            .lock()
            .map(|state| state.calls.clone())
            .unwrap_or_default()
    }
}

#[async_trait]
impl HostApplication for FakeApplication {
    async fn snapshot(&self, _run_id: &str, branch_id: &str) -> Result<HostSnapshot, PluginHostError> {
        self.state
            .lock()
            .map(|mut state| {
                state.calls.push(format!("snapshot:{branch_id}"));
                HostSnapshot {
                    state_id: format!("state-{branch_id}"),
                    expires_at: "2026-12-31T23:59:59Z".to_string(),
                    expires_at_ms: None,
                    tenant_id: None,
                }
            })
            .map_err(|_| PluginHostError::Manifest("fake application lock poisoned".to_string()))
    }

    async fn fork(&self, _run_id: &str, parent_state_id: &str) -> Result<String, PluginHostError> {
        self.state
            .lock()
            .map(|mut state| {
                state.next_branch += 1;
                state.calls.push(format!("fork:{parent_state_id}"));
                format!("branch-{}", state.next_branch)
            })
            .map_err(|_| PluginHostError::Manifest("fake application lock poisoned".to_string()))
    }

    async fn agent_turn(
        &self,
        _run_id: &str,
        branch_id: &str,
        input: &str,
        profile: Option<&str>,
    ) -> Result<String, PluginHostError> {
        self.state
            .lock()
            .map(|mut state| {
                state.next_job += 1;
                state
                    .calls
                    .push(format!("agent_turn:{branch_id}:{input}:{profile:?}"));
                format!("job-{}", state.next_job)
            })
            .map_err(|_| PluginHostError::Manifest("fake application lock poisoned".to_string()))
    }

    async fn inspect(&self, _run_id: &str, branch_id: &str, view: &str) -> Result<String, PluginHostError> {
        self.state
            .lock()
            .map(|state| {
                format!("{view}:{branch_id}:{}", state.calls.len())
            })
            .map_err(|_| PluginHostError::Manifest("fake application lock poisoned".to_string()))
    }

    async fn exec(
        &self,
        _run_id: &str,
        branch_id: &str,
        argv: &[String],
        cwd: &str,
    ) -> Result<String, PluginHostError> {
        self.state
            .lock()
            .map(|mut state| {
                state.next_job += 1;
                state
                    .calls
                    .push(format!("exec:{branch_id}:{argv:?}:{cwd}"));
                format!("job-{}", state.next_job)
            })
            .map_err(|_| PluginHostError::Manifest("fake application lock poisoned".to_string()))
    }

    async fn file_read(&self, _run_id: &str, branch_id: &str, path: &str) -> Result<Vec<u8>, PluginHostError> {
        self.state
            .lock()
            .map(|mut state| {
                state.calls.push(format!("file_read:{branch_id}:{path}"));
                b"mock-file-content".to_vec()
            })
            .map_err(|_| PluginHostError::Manifest("fake application lock poisoned".to_string()))
    }

    async fn file_write(
        &self,
        _run_id: &str,
        branch_id: &str,
        path: &str,
        data: &[u8],
    ) -> Result<u64, PluginHostError> {
        self.state
            .lock()
            .map(|mut state| {
                state
                    .calls
                    .push(format!("file_write:{branch_id}:{path}:{}", data.len()));
                data.len() as u64
            })
            .map_err(|_| PluginHostError::Manifest("fake application lock poisoned".to_string()))
    }

    async fn heartbeat(
        &self,
        _run_id: &str,
        branch_id: &str,
        client_id: &str,
    ) -> Result<(), PluginHostError> {
        self.state
            .lock()
            .map(|mut state| {
                state.calls.push(format!("heartbeat:{branch_id}:{client_id}"));
            })
            .map_err(|_| PluginHostError::Manifest("fake application lock poisoned".to_string()))
    }

    async fn diff(&self, _run_id: &str, branch_id: &str, base_state_id: &str) -> Result<String, PluginHostError> {
        self.state
            .lock()
            .map(|mut state| {
                state.calls.push(format!("diff:{branch_id}:{base_state_id}"));
                "mock-diff".to_string()
            })
            .map_err(|_| PluginHostError::Manifest("fake application lock poisoned".to_string()))
    }

    async fn release(&self, _run_id: &str, branch_id: &str) -> Result<(), PluginHostError> {
        self.state
            .lock()
            .map(|mut state| {
                state.calls.push(format!("release:{branch_id}"));
            })
            .map_err(|_| PluginHostError::Manifest("fake application lock poisoned".to_string()))
    }

    async fn wait_job(&self, _run_id: &str, job_id: &str) -> Result<HostJobOutcome, PluginHostError> {
        self.state
            .lock()
            .map(|mut state| {
                state.calls.push(format!("wait_job:{job_id}"));
                HostJobOutcome {
                    status: JobStatus::Completed,
                    output: String::new(),
                    exit_code: Some(0),
                    usage: BTreeMap::from([("total_tokens".to_string(), 0)]),
                    failure: None,
                }
            })
            .map_err(|_| PluginHostError::Manifest("fake application lock poisoned".to_string()))
    }
}

fn ok_response(id: RpcId, result: Value) -> Response {
    Response {
        jsonrpc: "2.0".to_string(),
        id,
        result: Some(result),
        error: None,
    }
}

fn error_response(id: RpcId, code: i64, message: impl Into<String>) -> Response {
    Response {
        jsonrpc: "2.0".to_string(),
        id,
        result: None,
        error: Some(json!({"code": code, "message": message.into()})),
    }
}

fn parse<T: DeserializeOwned>(id: RpcId, params: Value) -> Result<T, Response> {
    serde_json::from_value(params).map_err(|error| {
        error_response(id, RpcErrorCode::InvalidParams as i64, error.to_string())
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct HostSnapshotParams {
    plugin_id: String,
    run_id: String,
    timeout_ms: u64,
    branch_id: String,
    idempotency_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct HostForkParams {
    plugin_id: String,
    run_id: String,
    timeout_ms: u64,
    parent_state_id: String,
    idempotency_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct HostAgentTurnParams {
    plugin_id: String,
    run_id: String,
    timeout_ms: u64,
    branch_id: String,
    input: String,
    #[serde(default)]
    profile: Option<String>,
    idempotency_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct HostInspectParams {
    plugin_id: String,
    run_id: String,
    timeout_ms: u64,
    branch_id: String,
    view: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct HostExecParams {
    plugin_id: String,
    run_id: String,
    timeout_ms: u64,
    branch_id: String,
    argv: Vec<String>,
    cwd: String,
    idempotency_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct HostFileReadParams {
    plugin_id: String,
    run_id: String,
    timeout_ms: u64,
    branch_id: String,
    path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct HostFileWriteParams {
    plugin_id: String,
    run_id: String,
    timeout_ms: u64,
    branch_id: String,
    path: String,
    content_base64: String,
    idempotency_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct HostDiffParams {
    plugin_id: String,
    run_id: String,
    timeout_ms: u64,
    branch_id: String,
    base_state_id: String,
    #[serde(default)]
    idempotency_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct HostReleaseParams {
    plugin_id: String,
    run_id: String,
    timeout_ms: u64,
    branch_id: String,
    idempotency_key: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepare_service() -> (HostCapService<FakeApplication>, FakeApplication) {
        let run_map = RunMap::new();
        run_map.register_run("run-1").unwrap();
        run_map
            .add_branch("run-1", "root-branch", "state-root")
            .unwrap();
        run_map
            .add_state("run-1", "state-root", "root-branch")
            .unwrap();
        let app = FakeApplication::new();
        let service = HostCapService::new(app.clone(), run_map);
        (service, app)
    }

    #[tokio::test]
    async fn dispatches_full_fork_snapshot_agent_exec_release_trace() {
        let (service, app) = prepare_service();

        let fork = service
            .handle(
                RpcId::String("1".into()),
                MethodName::HostFork,
                json!({"plugin_id":"mcts","run_id":"run-1","timeout_ms":1000,"parent_state_id":"state-root","idempotency_key":"k-fork"}),
            )
            .await;
        let branch_id = fork.result.unwrap()["branch_id"].as_str().unwrap().to_string();

        let snapshot = service
            .handle(
                RpcId::String("2".into()),
                MethodName::HostSnapshot,
                json!({"plugin_id":"mcts","run_id":"run-1","timeout_ms":1000,"branch_id":branch_id,"idempotency_key":"k-snap"}),
            )
            .await;
        let snapshot_result = snapshot.result.unwrap();
        let state_id = snapshot_result["state_id"].as_str().unwrap().to_string();
        assert!(state_id.starts_with("state-"));
        assert!(snapshot_result["expires_at"].as_str().is_some());

        let agent = service
            .handle(
                RpcId::String("3".into()),
                MethodName::HostAgentTurn,
                json!({"plugin_id":"mcts","run_id":"run-1","timeout_ms":1000,"branch_id":branch_id,"input":"solve","profile":"solver-step","idempotency_key":"k-agent"}),
            )
            .await;
        assert!(agent.result.unwrap()["job_id"].as_str().is_some());

        let exec = service
            .handle(
                RpcId::String("4".into()),
                MethodName::HostExec,
                json!({"plugin_id":"mcts","run_id":"run-1","timeout_ms":1000,"branch_id":branch_id,"argv":["pytest"],"cwd":"/repo","idempotency_key":"k-exec"}),
            )
            .await;
        assert!(exec.result.unwrap()["job_id"].as_str().is_some());

        let release = service
            .handle(
                RpcId::String("5".into()),
                MethodName::HostRelease,
                json!({"plugin_id":"mcts","run_id":"run-1","timeout_ms":1000,"branch_id":branch_id,"idempotency_key":"k-release"}),
            )
            .await;
        assert!(release.error.is_none());
        assert!(service.run_map().require_branch_in_run("run-1", &branch_id).is_err());

        let calls = app.calls();
        assert!(calls.iter().any(|call| call.starts_with("fork:")));
        assert!(calls.iter().any(|call| call.starts_with("snapshot:")));
        assert!(calls.iter().any(|call| call.starts_with("agent_turn:")));
        assert!(calls.iter().any(|call| call.starts_with("exec:")));
        assert!(calls.iter().any(|call| call.starts_with("release:")));
    }

    #[tokio::test]
    async fn file_read_write_use_protocol_wire_names() {
        let (service, app) = prepare_service();
        let write = service
            .handle(
                RpcId::String("1".into()),
                MethodName::HostFileWrite,
                json!({"plugin_id":"mcts","run_id":"run-1","timeout_ms":1000,"branch_id":"root-branch","path":"/workspace/a.txt","content_base64":"aGVsbG8=","idempotency_key":"k-write"}),
            )
            .await;
        let write_result = write.result.unwrap();
        assert_eq!(write_result["written_bytes"], 5);

        let read = service
            .handle(
                RpcId::String("2".into()),
                MethodName::HostFileRead,
                json!({"plugin_id":"mcts","run_id":"run-1","timeout_ms":1000,"branch_id":"root-branch","path":"/workspace/a.txt"}),
            )
            .await;
        let read_result = read.result.unwrap();
        assert_eq!(read_result["content_base64"], "bW9jay1maWxlLWNvbnRlbnQ=");
        assert!(read_result.get("content").is_none());
        assert!(app
            .calls()
            .iter()
            .any(|call| call == "file_read:root-branch:/workspace/a.txt"));
    }

    #[tokio::test]
    async fn release_uses_released_true_and_diff_uses_content() {
        let (service, _app) = prepare_service();
        let diff = service
            .handle(
                RpcId::String("1".into()),
                MethodName::HostDiff,
                json!({"plugin_id":"mcts","run_id":"run-1","timeout_ms":1000,"branch_id":"root-branch","base_state_id":"state-root"}),
            )
            .await;
        let diff_result = diff.result.unwrap();
        assert!(diff_result["content"].as_str().is_some());
        assert!(diff_result.get("diff").is_none());

        let release = service
            .handle(
                RpcId::String("2".into()),
                MethodName::HostRelease,
                json!({"plugin_id":"mcts","run_id":"run-1","timeout_ms":1000,"branch_id":"root-branch","idempotency_key":"k-release"}),
            )
            .await;
        let release_result = release.result.unwrap();
        assert_eq!(release_result["released"], true);
    }

    #[tokio::test]
    async fn rejects_cross_run_branch_access() {
        let (service, _app) = prepare_service();
        let response = service
            .handle(
                RpcId::String("1".into()),
                MethodName::HostSnapshot,
                json!({"plugin_id":"mcts","run_id":"run-2","timeout_ms":1000,"branch_id":"root-branch","idempotency_key":"k"}),
            )
            .await;
        assert!(response.error.is_some());
    }
}
