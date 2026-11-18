//! Entry client: durable acceptance followed by a detached worker process.
use crate::{Result, RunManager};
use async_trait::async_trait;
use orchestration_contract::run::*;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use xgovernor_mcp::RunClient;

pub struct Service {
    pub manager: RunManager,
    pub configuration: PathBuf,
    pub executable: PathBuf,
}

impl Service {
    pub fn start_run(&self, request: StartRequest) -> Result<StartResponse> {
        let response = self.manager.start_run(request)?;
        if response.status == RunStatus::Accepted {
            // Multiple identical entry requests may spawn workers; the durable
            // claim admits exactly one. Children have no entry-owned stdio.
            let mut command = Command::new(&self.executable);
            command
                .arg("--config")
                .arg(&self.configuration)
                .arg("worker")
                .arg(&response.run_id)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            // Start a new session as well as disconnecting stdio: terminal
            // hangup and entry-process group signals must not stop this run.
            unsafe {
                command.pre_exec(|| {
                    if libc::setsid() == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let child = command.spawn()?;
            std::thread::spawn(move || {
                let mut child = child;
                let _ = child.wait();
            });
        }
        Ok(response)
    }
}

#[async_trait]
impl RunClient for Service {
    async fn start(&self, r: StartRequest) -> std::result::Result<StartResponse, String> {
        self.start_run(r).map_err(|e| {
            e.downcast_ref::<RunError>()
                .map_or_else(|| e.to_string(), RunError::json)
        })
    }
    async fn status(&self, r: StatusRequest) -> std::result::Result<StatusResponse, String> {
        self.manager.status_run(r).map_err(|e| e.to_string())
    }
    async fn cancel(&self, r: CancelRequest) -> std::result::Result<CancelResponse, String> {
        self.manager.cancel_run(r).map_err(|e| e.to_string())
    }
    async fn result(&self, r: ResultRequest) -> std::result::Result<ResultResponse, String> {
        self.manager.result_run(r).map_err(|e| e.to_string())
    }
    async fn list(&self, r: ListRequest) -> std::result::Result<ListResponse, String> {
        self.manager.list_runs(r).map_err(|e| e.to_string())
    }
}
