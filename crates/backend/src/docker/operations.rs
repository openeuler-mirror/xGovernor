use super::{
    args,
    control::{Control, InFlight},
    DockerConfig, WORKSPACE,
};
use crate::execution::OPERATION_CONTEXT;
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use operation_protocol::{
    capability::{exec::*, export::*, filesystem::*, path::*, search::*},
    *,
};
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub(super) struct DockerBackend {
    config: Arc<DockerConfig>,
    container: String,
    workspace: BackendPath,
    home: BackendPath,
    control: Arc<Control>,
}
fn failure(message: impl ToString) -> OperationError {
    OperationError::Transport {
        message: message.to_string(),
    }
}
impl DockerBackend {
    pub(super) fn new(config: Arc<DockerConfig>, container: String, control: Arc<Control>) -> Self {
        Self {
            config,
            container,
            control,
            workspace: BackendPath(WORKSPACE.into()),
            home: BackendPath("/home/agent".into()),
        }
    }
    async fn call(&self, mut request: Value) -> Result<Value, OperationError> {
        let epoch = self.control.generation.load(Ordering::SeqCst);
        let _guard = self.control.gate.lock().await;
        if epoch != self.control.generation.load(Ordering::SeqCst) {
            return Err(failure(
                "operation belongs to a retired execution generation",
            ));
        }
        if let Ok(context) = OPERATION_CONTEXT.try_with(Clone::clone) {
            let turn = self.control.turn.lock().unwrap();
            if !turn
                .as_ref()
                .is_some_and(|(id, cancelled)| id == &context.turn_id && !cancelled)
            {
                return Err(failure("stale or cancelled tool context"));
            }
            request["operation_id"] = json!(context.operation_id);
            request["turn_id"] = json!(context.turn_id);
        }
        if self.control.state().map_err(failure)? != "ready" {
            return Err(failure(
                "container unavailable: cleanup or resource protection pending",
            ));
        }
        request["limits"] = json!(self.config.limits);
        request["protocol"] = json!(2);
        let timeout = request["timeout_ms"]
            .as_u64()
            .unwrap_or(self.config.limits.timeout_ms);
        let changed = self.control.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        self.control.set("executing").map_err(failure)?;
        let mut flight = InFlight {
            control: self.control.clone(),
            finished: false,
        };
        let argv = args(&[
            "exec",
            "-i",
            &self.container,
            "/usr/local/bin/python3",
            "-I",
            "/usr/local/lib/xgovernor/helper.py",
        ]);
        let output = tokio::select! {
            output=self.config.run(&argv,Some(serde_json::to_vec(&request).map_err(failure)?),timeout.saturating_add(15_000)) => output.map_err(failure),
            _=&mut changed => Err(OperationError::ExecutionInterrupted {message:"container operation cancelled".into(),
                stdout:vec![],stderr:vec![],state:ExecutionState::RunningOrCompleted}),
        };
        let response = output.and_then(|output| {
            if !output.status.success() {
                return Err(failure(String::from_utf8_lossy(&output.stderr)));
            }
            serde_json::from_slice::<Value>(&output.stdout).map_err(failure)
        });
        let interrupted = epoch != self.control.generation.load(Ordering::SeqCst);
        let reset = interrupted
            || response.as_ref().map_or(true, |r| {
                r["value"]["timed_out"] == true || r["code"] == "failed"
            });
        if reset {
            // A failed cleanup leaves durable dirty state and blocks new work.
            if self.control.state().map_err(failure)? == "executing" {
                self.control.set("cleanup").map_err(failure)?;
            }
            self.control.recover().await.map_err(failure)?;
        } else {
            self.control.set("ready").map_err(failure)?;
        }
        flight.finished = true;
        if interrupted {
            return Err(OperationError::ExecutionInterrupted {
                message: "container operation cancelled".into(),
                stdout: vec![],
                stderr: vec![],
                state: ExecutionState::RunningOrCompleted,
            });
        }
        let response = response?;
        if response["ok"] == true {
            return Ok(response["value"].clone());
        }
        let path = request["path"].as_str().unwrap_or("").to_string();
        Err(match response["code"].as_str() {
            Some("not_found") => OperationError::NotFound { path },
            Some("already_exists") => OperationError::AlreadyExists { path },
            Some("permission_denied") => OperationError::PermissionDenied { path },
            Some("not_directory") => OperationError::NotDirectory { path },
            Some("not_file") => OperationError::NotFile { path },
            _ => OperationError::ExecutionFailed {
                message: response["message"]
                    .as_str()
                    .unwrap_or("invalid helper response")
                    .into(),
            },
        })
    }
    pub(super) async fn health(&self) -> Result<(), OperationError> {
        let hello = self.call(json!({"op":"hello"})).await?;
        if hello["protocol"] != 2 || hello["uid"] != 10001 {
            return Err(failure("incompatible Docker helper"));
        }
        let stat = self.stat(&self.workspace).await?;
        if stat.exists && stat.kind == Some(PathKind::Directory) {
            Ok(())
        } else {
            Err(failure("Docker workspace missing"))
        }
    }
}
#[async_trait]
impl OperationBackend for DockerBackend {
    fn backend_id(&self) -> &str {
        "docker"
    }
    fn capabilities(&self) -> OperationBackendCapabilities {
        OperationBackendCapabilities {
            supports_atomic_write: true,
            supports_grep: true,
            supports_export_file: true,
            supports_lsp: false,
        }
    }
    fn paths(&self) -> &dyn OperationPathResolver {
        self
    }
    fn files(&self) -> &dyn OperationFileSystem {
        self
    }
    fn search(&self) -> &dyn OperationSearch {
        self
    }
    fn exec(&self) -> &dyn OperationExec {
        self
    }
    fn export(&self) -> &dyn OperationExport {
        self
    }
    fn execution_control(&self) -> Option<&dyn OperationExecutionControl> {
        Some(self)
    }
    async fn shutdown(&self) -> Result<(), OperationError> {
        self.control.set("closing").map_err(failure)?;
        self.control.invalidate();
        Ok(())
    }
}
#[async_trait]
impl OperationExecutionControl for DockerBackend {
    async fn begin_turn(&self, turn_id: &str) -> Result<(), OperationError> {
        let _guard = self.control.gate.lock().await;
        if self.control.state().map_err(failure)? != "ready" {
            return Err(failure("Docker cleanup or resource protection pending"));
        }
        *self.control.turn.lock().unwrap() = Some((turn_id.into(), false));
        Ok(())
    }
    fn block_turn(&self, turn_id: &str) -> Result<(), OperationError> {
        let mut turn = self.control.turn.lock().unwrap();
        if let Some((id, cancelled)) = turn.as_mut() {
            if id == turn_id {
                *cancelled = true;
                self.control.invalidate();
            }
        }
        Ok(())
    }
    async fn cancel_turn(&self, turn_id: &str) -> Result<(), OperationError> {
        self.control.cancel(Some(turn_id)).await.map_err(failure)
    }
    async fn finish_turn(&self, turn_id: &str) -> Result<(), OperationError> {
        let _guard = self.control.gate.lock().await;
        if matches!(
            self.control.state().map_err(failure)?.as_str(),
            "executing" | "cleanup"
        ) {
            self.control.recover().await.map_err(failure)?;
        }
        let mut turn = self.control.turn.lock().unwrap();
        if turn.as_ref().is_some_and(|(id, _)| id == turn_id) {
            *turn = None;
        }
        Ok(())
    }
}
#[async_trait]
impl OperationExec for DockerBackend {
    fn default_shell(&self) -> Option<&str> {
        Some("/bin/bash")
    }
    async fn exec(&self, request: ExecRequest) -> Result<ExecResult, OperationError> {
        let timeout = request.timeout_ms.unwrap_or(self.config.limits.timeout_ms);
        if !(1..=self.config.limits.max_timeout_ms).contains(&timeout)
            || request.command.is_empty()
            || request.extra.is_some()
        {
            return Err(OperationError::Unsupported {
                message: "invalid timeout, empty command or unsupported exec extra".into(),
            });
        }
        let env: std::collections::BTreeMap<_, _> =
            request.env.unwrap_or_default().into_iter().collect();
        let value = self.call(json!({"op":"exec", "command":request.command,"args":request.args,"shell":request.shell,
            "cwd":request.cwd.map(|p|p.0),"env":env,"timeout_ms":timeout})).await?;
        Ok(ExecResult {
            stdout: STANDARD
                .decode(
                    value["stdout"]
                        .as_str()
                        .ok_or_else(|| failure("missing stdout"))?,
                )
                .map_err(failure)?,
            stderr: STANDARD
                .decode(
                    value["stderr"]
                        .as_str()
                        .ok_or_else(|| failure("missing stderr"))?,
                )
                .map_err(failure)?,
            exit_code: value["exit_code"].as_i64().map(|i| i as i32),
            timed_out: value["timed_out"]
                .as_bool()
                .ok_or_else(|| failure("missing timeout result"))?,
        })
    }
}
#[async_trait]
impl OperationPathResolver for DockerBackend {
    fn workspace_root(&self) -> &BackendPath {
        &self.workspace
    }
    fn home_dir(&self) -> Option<&BackendPath> {
        Some(&self.home)
    }
    async fn resolve_path(
        &self,
        request: ResolvePathRequest,
    ) -> Result<BackendPath, OperationError> {
        let base = match request.base {
            ResolveBase::WorkspaceRoot => self.workspace.0.clone(),
            ResolveBase::HomeDir => self.home.0.clone(),
            ResolveBase::Explicit(path) => path.0,
        };
        let value = self
            .call(json!({"op":"resolve","base":base,"path":request.raw_path}))
            .await?;
        Ok(BackendPath(
            value
                .as_str()
                .ok_or_else(|| failure("invalid path result"))?
                .into(),
        ))
    }
}
#[async_trait]
impl OperationFileSystem for DockerBackend {
    async fn stat(&self, path: &BackendPath) -> Result<PathStat, OperationError> {
        let value = self.call(json!({"op":"stat","path":path.0})).await?;
        Ok(PathStat {
            exists: value["exists"].as_bool().unwrap_or(false),
            kind: match value["kind"].as_str() {
                Some("file") => Some(PathKind::File),
                Some("directory") => Some(PathKind::Directory),
                Some("symlink") => Some(PathKind::Symlink),
                Some(_) => Some(PathKind::Other),
                None => None,
            },
            size_bytes: value["size"].as_u64(),
            modified_at: None,
        })
    }
    async fn read_bytes(&self, request: ReadBytesRequest) -> Result<Vec<u8>, OperationError> {
        let value = self
            .call(json!({"op":"read","path":request.path.0}))
            .await?;
        STANDARD
            .decode(
                value
                    .as_str()
                    .ok_or_else(|| failure("invalid read result"))?,
            )
            .map_err(failure)
    }
    async fn write_bytes(
        &self,
        request: WriteBytesRequest,
    ) -> Result<WriteBytesOutcome, OperationError> {
        if request.content.len() > self.config.limits.file_bytes {
            return Err(OperationError::Unsupported {
                message: "file exceeds configured limit".into(),
            });
        }
        let mode = match request.mode {
            WriteMode::Create => "create",
            WriteMode::Overwrite => "overwrite",
            WriteMode::AtomicOverwrite => "atomic",
        };
        let value = self.call(json!({"op":"write","path":request.path.0,"content":STANDARD.encode(request.content),"mode":mode})).await?;
        Ok(WriteBytesOutcome {
            path: request.path,
            created: value["created"]
                .as_bool()
                .ok_or_else(|| failure("invalid write result"))?,
        })
    }
    async fn create_dir_all(&self, path: &BackendPath) -> Result<(), OperationError> {
        self.call(json!({"op":"mkdir","path":path.0})).await?;
        Ok(())
    }
    async fn temp_path(&self, request: TempPathRequest) -> Result<BackendPath, OperationError> {
        let kind = match request.kind {
            TempPathKind::File => "file",
            TempPathKind::Directory => "directory",
        };
        let value = self.call(json!({"op":"temp","kind":kind,"parent":request.preferred_parent.map(|p|p.0),"prefix":request.prefix,"suffix":request.suffix})).await?;
        Ok(BackendPath(
            value
                .as_str()
                .ok_or_else(|| failure("invalid temp result"))?
                .into(),
        ))
    }
}
#[async_trait]
impl OperationSearch for DockerBackend {
    async fn glob(&self, request: GlobRequest) -> Result<Vec<BackendPath>, OperationError> {
        let value = self.call(json!({"op":"glob","pattern":request.pattern,"base":request.base_dir.unwrap_or_else(||self.workspace.clone()).0,"limit":request.limit})).await?;
        let paths: Vec<String> = serde_json::from_value(value).map_err(failure)?;
        Ok(paths.into_iter().map(BackendPath).collect())
    }
    async fn grep(&self, request: GrepRequest) -> Result<GrepResult, OperationError> {
        let mode = match request.mode {
            GrepMode::FilesWithMatches => "files",
            GrepMode::Content => "content",
            GrepMode::Count => "count",
        };
        let value = self.call(json!({"op":"grep","query":request.query,"base":request.base_dir.0,"include":request.include,"mode":mode,"limit":request.head_limit})).await?;
        Ok(GrepResult {
            entries: serde_json::from_value(value).map_err(failure)?,
        })
    }
}
struct FileExport {
    meta: ExportedFileMeta,
    data: Vec<u8>,
}
#[async_trait]
impl ExportedFileHandle for FileExport {
    fn metadata(&self) -> &ExportedFileMeta {
        &self.meta
    }
    async fn open_read(&self) -> Result<ExportedFileReader, OperationError> {
        Ok(Box::new(std::io::Cursor::new(self.data.clone())))
    }
}
#[async_trait]
impl OperationExport for DockerBackend {
    async fn export_file(
        &self,
        request: ExportFileRequest,
    ) -> Result<SharedExportedFileHandle, OperationError> {
        let data = self
            .read_bytes(ReadBytesRequest {
                path: request.path.clone(),
            })
            .await?;
        Ok(Arc::new(FileExport {
            meta: ExportedFileMeta {
                file_name: request
                    .preferred_name
                    .unwrap_or_else(|| request.path.0.rsplit('/').next().unwrap_or("file").into()),
                size_bytes: Some(data.len() as u64),
                media_type: None,
            },
            data,
        }))
    }
}
