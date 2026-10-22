use std::sync::Arc;

use mcts_protocol::RpcEnvelope;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::task::JoinHandle;

use crate::error::PluginHostError;
use crate::stdio::StdioTransport;

pub struct PluginProcess {
    child: Option<Child>,
    transport: Arc<StdioTransport>,
    stderr_task: Option<JoinHandle<()>>,
}

async fn log_stderr(stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();
    while let Some(line) = lines.next_line().await.unwrap_or(None) {
        tracing::warn!(target: "mcts.plugin.stderr", line);
    }
}

impl PluginProcess {
    pub async fn spawn(
        mut command: Command,
        envs: &[(&str, &str)],
    ) -> Result<Self, PluginHostError> {
        Self::spawn_inner(&mut command, envs).await
    }

    /// Spawn the plugin with an empty inherited environment, then inject only
    /// the caller-provided allow-list. This prevents xg model/token/tenant
    /// credentials from leaking into the plugin process.
    pub async fn spawn_isolated(
        command: &mut Command,
        envs: &[(&str, &str)],
    ) -> Result<Self, PluginHostError> {
        command.env_clear();
        Self::spawn_inner(command, envs).await
    }

    async fn spawn_inner(
        command: &mut Command,
        envs: &[(&str, &str)],
    ) -> Result<Self, PluginHostError> {
        command.stdin(std::process::Stdio::piped());
        command.stdout(std::process::Stdio::piped());
        command.stderr(std::process::Stdio::piped());
        for (key, value) in envs {
            command.env(key, value);
        }

        let mut child = command.spawn()?;
        let stdin: ChildStdin = child
            .stdin
            .take()
            .ok_or_else(|| PluginHostError::NotRunning)?;
        let stdout: ChildStdout = child
            .stdout
            .take()
            .ok_or_else(|| PluginHostError::NotRunning)?;
        let stderr = child.stderr.take();
        let stderr_task = stderr.map(|stderr| tokio::spawn(log_stderr(stderr)));
        let transport = StdioTransport::new(stdin, stdout);

        Ok(Self {
            child: Some(child),
            transport,
            stderr_task,
        })
    }

    pub async fn send(&self, message: &RpcEnvelope) -> Result<(), PluginHostError> {
        self.transport.send(message).await
    }

    pub async fn next_message(&self) -> Result<RpcEnvelope, PluginHostError> {
        self.transport.next_message().await
    }

    pub async fn shutdown(mut self) -> Result<(), PluginHostError> {
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
        if let Some(mut child) = self.child.take() {
            let _ = child.wait().await;
        }
        if let Some(task) = self.stderr_task.take() {
            let _ = task.await;
        }
        Ok(())
    }
}

impl Drop for PluginProcess {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
    }
}
