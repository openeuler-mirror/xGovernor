use super::*;
use operation_protocol::{
    capability::{exec::ExecRequest as OperationExecRequest, export::ExportFileRequest},
    BackendPath,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

#[derive(Clone, Serialize, Deserialize)]
struct LeaseRecord {
    expires_at: String,
    reclaiming: bool,
}
impl OwnBackend {
    pub(super) fn run_live(&self) -> BackendResult<bool> {
        Ok(self
            .0
            .store
            .get::<LeaseRecord>("lease", "run")?
            .is_some_and(|l| {
                !l.reclaiming
                    && chrono::DateTime::parse_from_rfc3339(&l.expires_at)
                        .is_ok_and(|expiry| expiry > chrono::Utc::now())
            }))
    }
    pub(super) fn require_live(&self) -> BackendResult<()> {
        if self.run_live()? {
            Ok(())
        } else {
            Err(error(
                ErrorCode::RunNotFound,
                "run lease missing, expired or reclaiming",
            ))
        }
    }
    pub(super) fn start_reaper(&self) {
        let weak = Arc::downgrade(&self.0);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                let backend = OwnBackend(inner);
                if backend
                    .0
                    .store
                    .get::<LeaseRecord>("lease", "run")
                    .ok()
                    .flatten()
                    .is_some()
                    && matches!(backend.run_live(), Ok(false))
                {
                    if backend
                        .reclaim_resources(Duration::from_secs(30), true)
                        .await
                        .is_ok_and(|r| r.status == ReclaimStatus::Complete)
                    {
                        break;
                    }
                }
            }
        });
    }
    /// Register an administrator-resolved OCI image to an E2B template. Building
    /// and publishing the template is a host provisioning action, never an
    /// algorithm-supplied shell command or a silent substitute environment.
    pub fn register_image_template(&self, image: &str, template: &str) -> BackendResult<()> {
        if image.is_empty() || template.is_empty() {
            return Err(error(
                ErrorCode::InvalidParams,
                "image and template required",
            ));
        }
        if let Some(old) = self.0.store.get::<String>("image", image)? {
            if old != template {
                return Err(error(
                    ErrorCode::IdempotencyConflict,
                    "image mapping changed",
                ));
            }
        }
        self.0.store.put("image", image, &template)
    }
    pub(super) async fn renew_run_lease_impl(
        &self,
        r: RenewRunLeaseRequest,
    ) -> BackendResult<RunLease> {
        self.check(&r.context.call, Some(Capability::RenewRunLease))?;
        if r.ttl_ms == 0 || r.ttl_ms > 86_400_000 {
            return Err(error(
                ErrorCode::InvalidParams,
                "lease ttl must be 1..86400000 ms",
            ));
        }
        let b = self.clone();
        let request = r.clone();
        match self
            .mutation(&r.context, "host.renew_run_lease", &r, async move {
                if b.0.store.get::<LeaseRecord>("lease", "run")?.is_some() {
                    b.require_live()?;
                }
                let lease = RunLease {
                    expires_at: (chrono::Utc::now()
                        + chrono::Duration::milliseconds(request.ttl_ms as i64))
                    .to_rfc3339(),
                };
                b.0.store.put(
                    "lease",
                    "run",
                    &LeaseRecord {
                        expires_at: lease.expires_at.clone(),
                        reclaiming: false,
                    },
                )?;
                Ok(MutationResult::RenewRunLease(lease))
            })
            .await?
        {
            MutationResult::RenewRunLease(v) => Ok(v),
            _ => Err(internal("invalid lease result")),
        }
    }
    pub(super) async fn prepare_root_impl(&self, r: PrepareRootRequest) -> BackendResult<Snapshot> {
        self.check(&r.context.call, Some(Capability::PrepareRoot))?;
        if r.root.network_policy != self.0.policy {
            return Err(error(
                ErrorCode::CapabilityDenied,
                "root policy differs from run admission",
            ));
        }
        let cwd = Path::new(&r.root.working_directory);
        if !cwd.is_absolute()
            || cwd
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(error(
                ErrorCode::InvalidParams,
                "absolute normalized working directory required",
            ));
        }
        let b = self.clone();
        let request = r.clone();
        match self.mutation(&r.context, "host.prepare_root", &r, async move {
            let branch = id();
            match &request.root.workspace {
                WorkspaceSource::Package { .. } => return Err(error(ErrorCode::Unsupported, "workspace packages are unsupported").into()),
                WorkspaceSource::Snapshot { state_id } => {
                    let state = b.state(state_id)?;
                    let checkpoint = b.0.repository.get_checkpoint(&state.checkpoint).await.map_err(internal)?
                        .ok_or_else(|| error(ErrorCode::StateExpired, "root snapshot missing"))?;
                    if checkpoint.workspace.root != request.root.working_directory {
                        return Err(error(ErrorCode::InvalidParams, "snapshot working directory differs").into());
                    }
                    b.load(state, &branch, true).await?;
                }
                WorkspaceSource::Git { repository, commit } => {
                    if !repository.starts_with("https://") || repository.contains('@') || repository.contains('?')
                        || !matches!(commit.len(), 40 | 64) || !commit.bytes().all(|c| c.is_ascii_hexdigit()) {
                        return Err(error(ErrorCode::InvalidParams, "HTTPS repository and immutable Git commit required").into());
                    }
                    let template = match &request.root.environment {
                        EnvironmentRef::Template(t) => t.clone(),
                        EnvironmentRef::Image(image) => b.0.store.get::<String>("image", image)?
                            .ok_or_else(|| error(ErrorCode::Unsupported, "image must first be built as a registered E2B template"))?,
                    };
                    if template.is_empty() { return Err(error(ErrorCode::InvalidParams, "template required").into()); }
                    b.0.store.put("branch", &branch, &BranchRecord { released: false, output: String::new(), root: true })?;
                    b.0.app.open(&b.0.owner, session::SessionOpenRequest {
                        runtime_id: Some(branch.clone()), runtime_kind: None, conversation_id: branch.clone(), sender_id: b.0.owner.principal.clone(),
                        workspace: session::WorkspaceSpec::Git { url: repository.clone(), reference: Some(commit.clone()), subdirectory: None },
                        deployment: session::DeploymentProfile { options: json!({"provider_options":{"template_id":template,"workspace_root":request.root.working_directory,"allow_internet_access":b.0.policy == NetworkPolicy::AllowInternet}}), ..Default::default() },
                        requested_capabilities: Default::default(), llm: b.0.llm.clone(), ext: b.0.bootstrap.clone(), lease: b.0.lease.clone(),
                    }).await.map_err(internal)?;
                    // An offline template contains the repository and dependencies.
                    // Missing objects fail offline; only an admitted online root may fetch.
                    let backend = b.0.manager.backend_for(&branch).map_err(internal)?;
                    let script = r#"import subprocess,sys
repo,commit,online=sys.argv[1:]
def git(*args): return subprocess.run(['git',*args],capture_output=True)
if git('cat-file','-e',commit+'^{commit}').returncode:
 if online!='true' or git('fetch','--no-tags',repo,commit).returncode: raise SystemExit(2)
if git('checkout','--detach','--force',commit).returncode: raise SystemExit(3)
if git('rev-parse','HEAD').stdout.decode().strip().lower()!=commit.lower(): raise SystemExit(4)
"#;
                    let result = backend.exec().exec(OperationExecRequest { command: "python3".into(), args: vec!["-c".into(), script.into(), repository.clone(), commit.clone(), (b.0.policy == NetworkPolicy::AllowInternet).to_string()], cwd: Some(BackendPath(request.root.working_directory.clone())), timeout_ms: Some(request.context.call.timeout_ms), ..Default::default() }).await.map_err(internal)?;
                    if result.exit_code != Some(0) || result.timed_out { return Err(error(ErrorCode::InvalidParams, "template Git commit preparation failed").into()); }
                }
            }
            b.0.store.put("branch", &branch, &BranchRecord { released: false, output: String::new(), root: true })?;
            let snapshot = b.save_snapshot(&branch).await?;
            b.stop_branch(&branch).await?;
            Ok(MutationResult::PrepareRoot(snapshot))
        }).await? { MutationResult::PrepareRoot(v) => Ok(v), _ => Err(internal("invalid prepared root result")) }
    }
    async fn remaining_resources(&self) -> BackendResult<Vec<ResourceIdentity>> {
        let mut remaining = Vec::new();
        let branches: BTreeSet<_> = self
            .0
            .store
            .list::<BranchRecord>("branch")?
            .into_iter()
            .map(|(id, b)| {
                if !b.released {
                    remaining.push(ResourceIdentity {
                        kind: "branch".into(),
                        id: id.clone(),
                    });
                }
                id
            })
            .collect();
        for (state, record) in self.0.store.list::<StateRecord>("state")? {
            if self
                .0
                .repository
                .get_checkpoint(&record.checkpoint)
                .await
                .map_err(internal)?
                .is_some()
            {
                remaining.push(ResourceIdentity {
                    kind: "snapshot".into(),
                    id: state,
                });
            }
        }
        let states: BTreeSet<_> = self
            .0
            .store
            .list::<StateRecord>("state")?
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        for (intent, body) in self
            .0
            .repository
            .operation_intents()
            .await
            .map_err(internal)?
        {
            if body["checkpoint"]
                .as_str()
                .is_some_and(|t| states.contains(t))
                || body["target"]
                    .as_str()
                    .is_some_and(|t| branches.contains(t))
                || body["source"]
                    .as_str()
                    .is_some_and(|t| branches.contains(t))
            {
                remaining.push(ResourceIdentity {
                    kind: "intent".into(),
                    id: intent,
                });
            }
        }
        for (job, record) in self.0.store.list::<JobRecord>("job")? {
            if !matches!(record.state, JobState::Completed(_) | JobState::Failed(_)) {
                remaining.push(ResourceIdentity {
                    kind: "job".into(),
                    id: job,
                });
            }
        }
        remaining.extend(
            self.0
                .store
                .unresolved()?
                .into_iter()
                .map(|id| ResourceIdentity {
                    kind: "operation".into(),
                    id,
                }),
        );
        Ok(remaining)
    }
    async fn reclaim_resources(
        &self,
        budget: Duration,
        expired_only: bool,
    ) -> BackendResult<RunReclaimed> {
        let _serial = self.0.reclaim_gate.lock().await;
        let lease = self
            .0
            .store
            .get::<LeaseRecord>("lease", "run")?
            .unwrap_or(LeaseRecord {
                expires_at: chrono::Utc::now().to_rfc3339(),
                reclaiming: true,
            });
        if expired_only && self.run_live()? {
            return Ok(RunReclaimed {
                status: ReclaimStatus::Unknown,
                remaining: self.remaining_resources().await?,
                message: "run lease was renewed".into(),
            });
        }
        let deadline = tokio::time::Instant::now() + budget;
        let Some(_gate) = lock_reclamation(
            &self.0.store,
            &self.0.run_gate,
            &lease,
            expired_only,
            deadline,
        )
        .await?
        else {
            return Ok(RunReclaimed {
                status: ReclaimStatus::Unknown,
                remaining: self.remaining_resources().await?,
                message: "in-flight mutations have not stopped or run lease changed".into(),
            });
        };
        let mut message = String::new();
        loop {
            match self.cleanup().await {
                Ok(()) => break,
                Err(e) => message = format!("{e:?}"),
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let remaining = self.remaining_resources().await?;
        if remaining.iter().all(|r| r.kind == "operation") {
            let proof = async {
                // Fork persists its branch before dispatch. Under the exclusive
                // run gate, this set includes every possible fork target, even
                // when an older operation has no direct target association.
                for (branch, _) in self.0.store.list::<BranchRecord>("branch")? {
                    let instance = self
                        .0
                        .manager
                        .pending_snapshot_load_instance(&branch)
                        .await
                        .map_err(internal)?
                        .ok_or_else(|| {
                            error(ErrorCode::BranchBusy, "fork target absence is unconfirmed")
                        })?;
                    self.0
                        .manager
                        .abort_pending_snapshot_load(&branch, Some(instance))
                        .await
                        .map_err(internal)?;
                }
                Ok(())
            };
            let proof = async {
                tokio::time::timeout_at(deadline, proof)
                    .await
                    .map_err(|_| {
                        error(ErrorCode::DeadlineExceeded, "reclamation proof timed out")
                    })?
            };
            if let Err(e) = settle_reclaimed_forks(&self.0.store, proof).await {
                message = format!("{e:?}");
            }
        }
        let remaining = self.remaining_resources().await?;
        Ok(RunReclaimed {
            status: if remaining.is_empty() {
                ReclaimStatus::Complete
            } else {
                ReclaimStatus::Unknown
            },
            remaining,
            message,
        })
    }
    pub(super) async fn reclaim_run_impl(
        &self,
        r: ReclaimRunRequest,
    ) -> BackendResult<RunReclaimed> {
        self.check(&r.context.call, Some(Capability::ReclaimRun))?;
        let b = self.clone();
        let budget = Duration::from_millis(r.context.call.timeout_ms.saturating_sub(100));
        match self
            .mutation(&r.context, "host.reclaim_run", &r, async move {
                b.reclaim_resources(budget, false)
                    .await
                    .map(MutationResult::ReclaimRun)
                    .map_err(Into::into)
            })
            .await?
        {
            MutationResult::ReclaimRun(v) => Ok(v),
            _ => Err(internal("invalid reclaim result")),
        }
    }
    pub(super) async fn export_workspace_impl(
        &self,
        r: ExportWorkspaceRequest,
    ) -> BackendResult<WorkspaceExported> {
        self.check(&r.context.call, Some(Capability::ExportWorkspace))?;
        if r.max_bytes == 0 {
            return Err(error(
                ErrorCode::InvalidParams,
                "positive export limit required",
            ));
        }
        let b = self.clone();
        let request = r.clone();
        match self
            .mutation(&r.context, "host.export_workspace", &r, async move {
                let (branch, temporary) = match &request.source {
                    WorkspaceRef::Branch(branch) => {
                        b.branch(branch)?;
                        (branch.clone(), false)
                    }
                    WorkspaceRef::Snapshot(state) => {
                        let branch = id();
                        b.load(b.state(state)?, &branch, true).await?;
                        (branch, true)
                    }
                };
                let _guard = b.lock(&branch)?;
                let result = b.export_tar(&branch, &request).await;
                if temporary {
                    b.stop_branch(&branch).await?;
                }
                result
                    .map(MutationResult::ExportWorkspace)
                    .map_err(Into::into)
            })
            .await?
        {
            MutationResult::ExportWorkspace(v) => Ok(v),
            _ => Err(internal("invalid export result")),
        }
    }
    async fn export_tar(
        &self,
        branch: &str,
        request: &ExportWorkspaceRequest,
    ) -> BackendResult<WorkspaceExported> {
        let backend = self.0.manager.backend_for(branch).map_err(internal)?;
        let script = include_str!("workspace_tar.py");
        let result = backend
            .exec()
            .exec(OperationExecRequest {
                command: "python3".into(),
                args: vec![
                    "-c".into(),
                    script.into(),
                    request.max_bytes.to_string(),
                    request.include_vcs_metadata.to_string(),
                ],
                cwd: Some(backend.paths().workspace_root().clone()),
                timeout_ms: Some(request.context.call.timeout_ms),
                ..Default::default()
            })
            .await
            .map_err(internal)?;
        if result.exit_code == Some(42) {
            return Err(error(
                ErrorCode::ExportTooLarge,
                "complete workspace archive exceeds limit",
            ));
        }
        if result.exit_code != Some(0) || result.timed_out {
            return Err(internal("workspace archive failed"));
        }
        let manifest: serde_json::Value =
            serde_json::from_slice(&result.stdout).map_err(internal)?;
        let path = manifest["path"]
            .as_str()
            .ok_or_else(|| internal("missing archive path"))?;
        let result = async {
            let handle = backend
                .export()
                .export_file(ExportFileRequest {
                    path: BackendPath(path.into()),
                    preferred_name: None,
                })
                .await
                .map_err(internal)?;
            let mut reader = handle.open_read().await.map_err(internal)?;
            let mut destination =
                Destination::new(Path::new(&request.run_directory), Path::new(&request.path))?;
            let mut size = 0;
            let mut hash = Sha256::new();
            let mut buffer = [0u8; 65536];
            loop {
                let count = reader.read(&mut buffer).await.map_err(internal)?;
                if count == 0 {
                    break;
                }
                size += count as u64;
                if size > request.max_bytes {
                    return Err(error(
                        ErrorCode::ExportTooLarge,
                        "complete workspace archive exceeds limit",
                    ));
                }
                use std::io::Write;
                destination
                    .file
                    .write_all(&buffer[..count])
                    .map_err(internal)?;
                hash.update(&buffer[..count]);
            }
            if manifest["size"].as_u64() != Some(size) {
                return Err(internal("incomplete workspace transfer"));
            }
            destination.publish()?;
            Ok(WorkspaceExported {
                size_bytes: size,
                file_count: manifest["files"]
                    .as_u64()
                    .ok_or_else(|| internal("missing file count"))?,
                sha256: format!("{:x}", hash.finalize()),
            })
        }
        .await;
        let _ = backend
            .exec()
            .exec(OperationExecRequest {
                command: "rm".into(),
                args: vec!["-f".into(), "--".into(), path.into()],
                timeout_ms: Some(10000),
                ..Default::default()
            })
            .await;
        result
    }
}

/// Descriptor-relative publication pins every directory and refuses symlink
/// traversal, including destination changes between validation and publication.
struct Destination {
    directory: std::fs::File,
    temporary: std::ffi::CString,
    name: std::ffi::CString,
    file: std::fs::File,
}
impl Destination {
    fn new(root: &Path, relative: &Path) -> BackendResult<Self> {
        use std::{
            ffi::CString,
            os::{
                fd::{AsRawFd, FromRawFd},
                unix::ffi::OsStrExt,
            },
        };
        let parts: Vec<_> = relative.components().collect();
        if parts.is_empty()
            || parts
                .iter()
                .any(|c| !matches!(c, std::path::Component::Normal(_)))
        {
            return Err(error(
                ErrorCode::InvalidParams,
                "export path must be relative without traversal",
            ));
        }
        if !root.is_absolute() {
            return Err(error(
                ErrorCode::InvalidParams,
                "absolute export root required",
            ));
        }
        let root_parts: Vec<_> = root
            .components()
            .filter(|c| !matches!(c, std::path::Component::RootDir))
            .collect();
        if root_parts
            .iter()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
        {
            return Err(error(
                ErrorCode::InvalidParams,
                "normalized export root required",
            ));
        }
        let root = CString::new("/").unwrap();
        let fd = unsafe {
            libc::open(
                root.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(error(
                ErrorCode::InvalidParams,
                "export root is not a real directory",
            ));
        }
        let mut directory = unsafe { std::fs::File::from_raw_fd(fd) };
        for (index, part) in root_parts
            .iter()
            .chain(parts[..parts.len() - 1].iter())
            .enumerate()
        {
            let name = CString::new(part.as_os_str().as_bytes()).map_err(internal)?;
            if index >= root_parts.len() {
                unsafe {
                    libc::mkdirat(directory.as_raw_fd(), name.as_ptr(), 0o700);
                }
            }
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if fd < 0 {
                return Err(error(
                    ErrorCode::InvalidParams,
                    "export path traverses a symlink or invalid directory",
                ));
            }
            directory = unsafe { std::fs::File::from_raw_fd(fd) };
        }
        let temporary = CString::new(format!(".workspace-{}", id())).unwrap();
        let name = CString::new(parts.last().unwrap().as_os_str().as_bytes()).map_err(internal)?;
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                temporary.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(internal(std::io::Error::last_os_error()));
        }
        Ok(Self {
            directory,
            temporary,
            name,
            file: unsafe { std::fs::File::from_raw_fd(fd) },
        })
    }
    fn publish(&mut self) -> BackendResult<()> {
        use std::os::fd::AsRawFd;
        self.file.sync_all().map_err(internal)?;
        if unsafe {
            libc::linkat(
                self.directory.as_raw_fd(),
                self.temporary.as_ptr(),
                self.directory.as_raw_fd(),
                self.name.as_ptr(),
                0,
            )
        } != 0
        {
            return Err(error(
                ErrorCode::InvalidParams,
                "export destination already exists or cannot be published",
            ));
        }
        self.directory.sync_all().map_err(internal)
    }
}
impl Drop for Destination {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        unsafe {
            libc::unlinkat(self.directory.as_raw_fd(), self.temporary.as_ptr(), 0);
        }
    }
}

// Renewals hold a read guard through their durable write. Check only after all
// admitted mutations have stopped, and mark reclamation under the same gate.
async fn lock_reclamation<'a>(
    store: &store::Store,
    gate: &'a RwLock<()>,
    expected: &LeaseRecord,
    expired_only: bool,
    deadline: tokio::time::Instant,
) -> BackendResult<Option<tokio::sync::RwLockWriteGuard<'a, ()>>> {
    let Ok(guard) = tokio::time::timeout_at(deadline, gate.write()).await else {
        return Ok(None);
    };
    if let Some(current) = store.get::<LeaseRecord>("lease", "run")? {
        if current.reclaiming != expected.reclaiming
            || current.expires_at != expected.expires_at
            || (expired_only
                && !current.reclaiming
                && chrono::DateTime::parse_from_rfc3339(&current.expires_at)
                    .is_ok_and(|expiry| expiry > chrono::Utc::now()))
        {
            return Ok(None);
        }
    }
    store.put(
        "lease",
        "run",
        &LeaseRecord {
            expires_at: expected.expires_at.clone(),
            reclaiming: true,
        },
    )?;
    Ok(Some(guard))
}

// Resource reclamation does not establish whether the original fork succeeded.
// Keep its idempotent result and resolution intact; persist only cleanup proof.
async fn settle_reclaimed_forks(
    store: &store::Store,
    proof: impl Future<Output = BackendResult<()>>,
) -> BackendResult<()> {
    let forks: Vec<_> = store
        .unresolved()?
        .into_iter()
        .filter(|id| id.starts_with("host.fork:"))
        .collect();
    if !forks.is_empty() {
        proof.await?;
        for identity in forks {
            store.put("reclaimed_operation", &identity, &true)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn renewal_while_reclamation_waits_cancels_stale_cleanup() {
        let store = store::Store::open(Path::new(":memory:")).unwrap();
        let gate = RwLock::new(());
        let expired = LeaseRecord {
            expires_at: (chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339(),
            reclaiming: false,
        };
        store.put("lease", "run", &expired).unwrap();
        let held = gate.read().await;
        let mut reclaim = Box::pin(lock_reclamation(
            &store,
            &gate,
            &expired,
            true,
            tokio::time::Instant::now() + Duration::from_secs(1),
        ));
        std::future::poll_fn(|cx| {
            assert!(reclaim.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let renewed = LeaseRecord {
            expires_at: (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339(),
            reclaiming: false,
        };
        store.put("lease", "run", &renewed).unwrap();
        drop(held);
        assert!(
            reclaim.await.unwrap().is_none(),
            "stale reclamation must not enter cleanup"
        );
        assert!(
            !store
                .get::<LeaseRecord>("lease", "run")
                .unwrap()
                .unwrap()
                .reclaiming
        );
    }
    #[tokio::test]
    async fn reclaimed_fork_requires_platform_proof_and_preserves_unknown_result() {
        let path = std::env::temp_dir().join(format!("own-reclamation-{}.sqlite", id()));
        let op = OperationIdentity {
            run_id: "run".into(),
            method: "host.fork".into(),
            idempotency_key: "author".into(),
        };
        let original = Err(unknown(&op));
        {
            let store = store::Store::open(&path).unwrap();
            store
                .begin(&op.method, &op.idempotency_key, json!({}))
                .unwrap();
            store.finish(&op, &original).unwrap();
            store
                .begin("host.snapshot", "unresolved", json!({}))
                .unwrap();
        }
        let store = store::Store::open(&path).unwrap();
        assert!(
            settle_reclaimed_forks(&store, async { Err(internal("platform query failed")) })
                .await
                .is_err()
        );
        assert!(store
            .unresolved()
            .unwrap()
            .contains(&"host.fork:author".into()));
        settle_reclaimed_forks(&store, async { Ok(()) })
            .await
            .unwrap();
        assert_eq!(
            store.unresolved().unwrap(),
            vec!["host.snapshot:unresolved"]
        );
        assert!(matches!(
            store.resolution(&op).unwrap(),
            OperationResolution::Unknown
        ));
        assert_eq!(
            store
                .begin(&op.method, &op.idempotency_key, json!({}))
                .unwrap(),
            Some(original)
        );
        drop(store);
        let reopened = store::Store::open(&path).unwrap();
        assert_eq!(
            reopened.unresolved().unwrap(),
            vec!["host.snapshot:unresolved"]
        );
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }
}
