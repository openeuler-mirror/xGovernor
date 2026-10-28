use std::sync::Arc;

use mcts_protocol::RpcEnvelope;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::Mutex;

use crate::error::PluginHostError;

pub struct StdioTransport {
    writer: Mutex<ChildStdin>,
    reader: Mutex<BufReader<ChildStdout>>,
}

impl StdioTransport {
    pub fn new(stdin: ChildStdin, stdout: ChildStdout) -> Arc<Self> {
        Arc::new(Self {
            writer: Mutex::new(stdin),
            reader: Mutex::new(BufReader::new(stdout)),
        })
    }

    pub async fn send(&self, message: &RpcEnvelope) -> Result<(), PluginHostError> {
        let mut payload = serde_json::to_vec(message)?;
        payload.push(b'\n');
        let mut writer = self.writer.lock().await;
        writer.write_all(&payload).await?;
        writer.flush().await?;
        Ok(())
    }

    pub async fn next_message(&self) -> Result<RpcEnvelope, PluginHostError> {
        loop {
            let mut line = String::new();
            let mut reader = self.reader.lock().await;
            let read = reader.read_line(&mut line).await?;
            if read == 0 {
                return Err(PluginHostError::Eof);
            }

            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            return Ok(serde_json::from_str(trimmed)?);
        }
    }
}
