use mcts_protocol::messages::JSONRPC_VERSION;
use mcts_protocol::{MethodName, Request, Response, RpcEnvelope, RpcId};
use serde_json::Value;
use std::time::Duration;

use crate::error::PluginHostError;
use crate::process::PluginProcess;

pub struct PluginSession {
    process: PluginProcess,
}

pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

impl PluginSession {
    pub fn new(process: PluginProcess) -> Self {
        Self { process }
    }

    pub fn process(&self) -> &PluginProcess {
        &self.process
    }

    pub async fn request(
        &self,
        id: &str,
        method: MethodName,
        params: Value,
    ) -> Result<Response, PluginHostError> {
        self.request_with_timeout(id, method, params, DEFAULT_REQUEST_TIMEOUT)
            .await
    }

    pub async fn request_with_timeout(
        &self,
        id: &str,
        method: MethodName,
        params: Value,
        timeout: Duration,
    ) -> Result<Response, PluginHostError> {
        let request = RpcEnvelope::Request(Request {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id: RpcId::String(id.to_string()),
            method,
            params,
        });
        self.process.send(&request).await?;

        tokio::time::timeout(timeout, self.wait_for_response(id))
            .await
            .map_err(|_| PluginHostError::DeadlineExceeded)?
    }

    async fn wait_for_response(&self, id: &str) -> Result<Response, PluginHostError> {
        loop {
            match self.process.next_message().await? {
                RpcEnvelope::Response(response) => {
                    let matched = match &response.id {
                        RpcId::String(value) => value == id,
                        RpcId::Number(value) => value.to_string() == id,
                    };
                    if matched {
                        return Ok(response);
                    }
                }
                RpcEnvelope::Notification(_) | RpcEnvelope::Request(_) => {}
            }
        }
    }

    pub async fn initialize(&self, params: Value) -> Result<Response, PluginHostError> {
        self.request("day2-initialize", MethodName::PluginInitialize, params)
            .await
    }

    pub async fn start(&self, params: Value) -> Result<Response, PluginHostError> {
        self.request("day2-start", MethodName::PluginStart, params)
            .await
    }

    pub async fn wait(&self, params: Value) -> Result<Response, PluginHostError> {
        self.request("day2-wait", MethodName::PluginWait, params)
            .await
    }

    pub async fn shutdown(self) -> Result<(), PluginHostError> {
        let _ = self
            .request(
                "day2-shutdown",
                MethodName::PluginShutdown,
                serde_json::json!({"plugin_id": "mcts"}),
            )
            .await;
        self.process.shutdown().await
    }
}
