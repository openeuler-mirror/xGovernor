//! Reusable execution-interface cases. Call each with a fresh run fixture.
#[cfg(feature = "test-support")]
pub mod memory;

use orchestration_contract::execution::*;

pub struct Fixture<'a> {
    pub backend: &'a dyn ExecutionBackend,
    pub context: CallContext,
    pub root_state_id: String,
    pub network_policy: NetworkPolicy,
    pub test_path: String,
    pub exec_argv: Vec<String>,
    pub cancellable_argv: Vec<String>,
    pub cwd: String,
    pub turn_input: String,
    pub lease_ttl_ms: u64,
    pub reclaim_wait_ms: u64,
}

impl Fixture<'_> {
    pub fn mutation(&self, key: &str) -> MutationContext {
        MutationContext {
            call: self.context.clone(),
            idempotency_key: key.into(),
        }
    }

    pub fn fork_request(&self, key: &str, state: &str) -> ForkRequest {
        ForkRequest {
            context: self.mutation(key),
            parent_state_id: state.into(),
            network_policy: self.network_policy,
            inherit_history: true,
        }
    }

    async fn branch(&self) -> String {
        let caps = self.backend.capabilities();
        assert!(caps.network_policies.contains(&self.network_policy));
        for capability in [
            Capability::Snapshot,
            Capability::Fork,
            Capability::AgentTurn,
            Capability::CancelJob,
            Capability::Inspect,
            Capability::Exec,
            Capability::FileRead,
            Capability::FileWrite,
            Capability::Diff,
            Capability::Release,
        ] {
            assert!(caps.capabilities.contains(&capability));
        }
        self.backend
            .fork(self.fork_request("setup", &self.root_state_id))
            .await
            .unwrap()
            .branch_id
    }

    async fn read(&self, branch: &str) -> FileContent {
        self.backend
            .file_read(FileReadRequest {
                context: self.context.clone(),
                branch_id: branch.into(),
                path: self.test_path.clone(),
            })
            .await
            .unwrap()
    }

    async fn write(&self, branch: &str, key: &str, content: &str) -> FileWritten {
        self.backend
            .file_write(FileWriteRequest {
                context: self.mutation(key),
                branch_id: branch.into(),
                path: self.test_path.clone(),
                content_base64: content.into(),
            })
            .await
            .unwrap()
    }

    async fn release(&self, branch: &str, key: &str) {
        assert!(
            self.backend
                .release(ReleaseRequest {
                    context: self.mutation(key),
                    branch_id: branch.into(),
                })
                .await
                .unwrap()
                .released
        );
    }

    async fn terminal(&self, job_id: &str) -> JobState {
        // Every wait is bounded by CallContext, and the complete case is bounded
        // as well. A backend must not keep a test alive through empty polls.
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_millis(self.context.timeout_ms);
        loop {
            let state = self
                .backend
                .wait_job(self.context.clone(), job_id)
                .await
                .unwrap();
            if matches!(state, JobState::Completed(_) | JobState::Failed(_)) {
                return state;
            }
            assert!(std::time::Instant::now() < deadline, "job did not stop");
        }
    }
}

pub async fn snapshot(f: &Fixture<'_>) {
    let branch = f.branch().await;
    f.write(&branch, "write", "b25l").await;
    let state = f
        .backend
        .snapshot(SnapshotRequest {
            context: f.mutation("snapshot"),
            branch_id: branch.clone(),
        })
        .await
        .unwrap();
    f.release(&branch, "release").await;
    let restored = f
        .backend
        .fork(f.fork_request("restore", &state.state_id))
        .await
        .unwrap();
    assert_eq!(f.read(&restored.branch_id).await.content_base64, "b25l");
    f.release(&restored.branch_id, "release-restored").await;
}

pub async fn fork(f: &Fixture<'_>) {
    let request = f.fork_request("fork", &f.root_state_id);
    let first = f.backend.fork(request.clone()).await.unwrap();
    assert_eq!(f.backend.fork(request.clone()).await.unwrap(), first);
    let mut changed = request;
    changed.inherit_history = false;
    assert!(matches!(
        f.backend.fork(changed).await,
        Err(BackendError::Rejected {
            code: ErrorCode::IdempotencyConflict,
            ..
        })
    ));
    let second = f
        .backend
        .fork(f.fork_request("fork-second", &f.root_state_id))
        .await
        .unwrap();
    assert_ne!(first.branch_id, second.branch_id);
    f.write(&first.branch_id, "first-write", "b25l").await;
    f.write(&second.branch_id, "second-write", "dHdv").await;
    assert_eq!(f.read(&first.branch_id).await.content_base64, "b25l");
    f.release(&first.branch_id, "release-first").await;
    f.release(&second.branch_id, "release-second").await;
}

pub async fn agent_turn(f: &Fixture<'_>) {
    let branch = f.branch().await;
    let receipt = f
        .backend
        .agent_turn(AgentTurnRequest {
            context: f.mutation("turn"),
            branch_id: branch.clone(),
            input: f.turn_input.clone(),
            profile: None,
        })
        .await
        .unwrap();
    let JobState::Completed(result) = f.terminal(&receipt.job_id).await else {
        panic!("Agent turn failed");
    };
    assert!(!result.output.is_empty());
    assert_eq!(result.exit_code, None);
    f.release(&branch, "release").await;
}

pub async fn cancel_job(f: &Fixture<'_>) {
    let branch = f.branch().await;
    let job = f
        .backend
        .exec(ExecRequest {
            context: f.mutation("exec"),
            branch_id: branch.clone(),
            argv: f.cancellable_argv.clone(),
            cwd: f.cwd.clone(),
        })
        .await
        .unwrap();
    let request = CancelJobRequest {
        context: f.mutation("cancel"),
        job_id: job.job_id.clone(),
        reason: "stop compatibility command".into(),
    };
    let response = f.backend.cancel_job(request.clone()).await.unwrap();
    assert_eq!(response.job_id, job.job_id);
    let JobState::Failed(failure) = f.terminal(&job.job_id).await else {
        panic!("cancellation did not fail the active command");
    };
    assert_eq!(failure.failure.code, "cancelled");
    assert_eq!(
        f.backend.cancel_job(request).await.unwrap().job_id,
        job.job_id
    );
    f.release(&branch, "release").await;
}

pub async fn inspect(f: &Fixture<'_>) {
    let branch = f.branch().await;
    f.write(&branch, "write", "b25l").await;
    let _observation = f
        .backend
        .inspect(InspectRequest {
            context: f.context.clone(),
            branch_id: branch.clone(),
            view: InspectView::Summary,
        })
        .await
        .unwrap();
    assert_eq!(f.read(&branch).await.content_base64, "b25l");
    f.release(&branch, "release").await;
}

pub async fn exec(f: &Fixture<'_>) {
    let branch = f.branch().await;
    let receipt = f
        .backend
        .exec(ExecRequest {
            context: f.mutation("exec"),
            branch_id: branch.clone(),
            argv: f.exec_argv.clone(),
            cwd: f.cwd.clone(),
        })
        .await
        .unwrap();
    let JobState::Completed(result) = f.terminal(&receipt.job_id).await else {
        panic!("command execution failed");
    };
    assert_eq!(result.exit_code, Some(7));
    assert!(result.output.contains("compat-exec"));
    f.release(&branch, "release").await;
}

pub async fn file_read(f: &Fixture<'_>) {
    let branch = f.branch().await;
    f.write(&branch, "write", "AP9h").await;
    assert_eq!(f.read(&branch).await.content_base64, "AP9h");
    f.release(&branch, "release").await;
}

pub async fn file_write(f: &Fixture<'_>) {
    let branch = f.branch().await;
    f.write(&branch, "write-original", "b25l").await;
    let written = f.write(&branch, "write-replace", "dHdv").await;
    assert_eq!(written.written_bytes, 3);
    assert_eq!(f.read(&branch).await.content_base64, "dHdv");
    f.release(&branch, "release").await;
}

pub async fn diff(f: &Fixture<'_>) {
    let branch = f.branch().await;
    f.write(&branch, "write", "b25l").await;
    let result = f
        .backend
        .diff(DiffRequest {
            context: f.context.clone(),
            branch_id: branch.clone(),
            base_state_id: f.root_state_id.clone(),
        })
        .await
        .unwrap();
    assert!(result.content.contains(&f.test_path));
    assert!(result.content.contains("+one"));
    f.release(&branch, "release").await;
}

pub async fn release(f: &Fixture<'_>) {
    let branch = f.branch().await;
    f.release(&branch, "release").await;
    f.release(&branch, "release-again").await;
    assert!(matches!(
        f.backend
            .inspect(InspectRequest {
                context: f.context.clone(),
                branch_id: branch,
                view: InspectView::Summary,
            })
            .await,
        Err(BackendError::Rejected {
            code: ErrorCode::BranchNotFound,
            ..
        })
    ));
}

/// Fixture must inject loss of the first fork result after its side effect.
pub async fn outcome_unknown(f: &Fixture<'_>) {
    let request = f.fork_request("uncertain-fork", &f.root_state_id);
    let Err(BackendError::OutcomeUnknown { operation, .. }) = f.backend.fork(request.clone()).await
    else {
        panic!("fixture did not inject an unknown fork outcome");
    };
    assert_eq!(operation.run_id, f.context.run_id);
    assert_eq!(operation.method, "host.fork");
    assert_eq!(operation.idempotency_key, "uncertain-fork");
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_millis(f.context.timeout_ms);
    let branch = loop {
        match f
            .backend
            .reconcile(f.context.clone(), operation.clone())
            .await
            .unwrap()
        {
            OperationResolution::Succeeded(MutationResult::Fork(branch)) => break branch,
            OperationResolution::Pending | OperationResolution::Unknown => {
                assert!(std::time::Instant::now() < deadline, "unresolved fork");
            }
            other => panic!("lost successful fork resolved incorrectly: {other:?}"),
        }
    };
    assert_eq!(f.backend.fork(request).await.unwrap(), branch);
    f.release(&branch.branch_id, "release").await;
}

pub async fn prepare_root(f: &Fixture<'_>) {
    let request = PrepareRootRequest {
        context: f.mutation("prepare"),
        root: RootDescription {
            environment: EnvironmentRef::Template("compat".into()),
            workspace: WorkspaceSource::Snapshot {
                state_id: f.root_state_id.clone(),
            },
            working_directory: f.cwd.clone(),
            network_policy: f.network_policy,
        },
    };
    let root = f.backend.prepare_root(request.clone()).await.unwrap();
    assert_eq!(f.backend.prepare_root(request).await.unwrap(), root);
    let branch = f
        .backend
        .fork(f.fork_request("prepared", &root.state_id))
        .await
        .unwrap();
    f.write(&branch.branch_id, "change", "b25l").await;
    f.release(&branch.branch_id, "release-prepared").await;
    let restored = f
        .backend
        .fork(f.fork_request("prepared-again", &root.state_id))
        .await
        .unwrap();
    assert!(f
        .backend
        .file_read(FileReadRequest {
            context: f.context.clone(),
            branch_id: restored.branch_id.clone(),
            path: f.test_path.clone()
        })
        .await
        .is_err());
    f.release(&restored.branch_id, "release-restored").await;
}

/// Short real-time lease test; the backend must perform orphan cleanup without
/// another manager request. Intended for an isolated, disposable fixture run.
pub async fn renew_run_lease(f: &Fixture<'_>) {
    let request = RenewRunLeaseRequest {
        context: f.mutation("lease"),
        ttl_ms: f.lease_ttl_ms,
    };
    let lease = f.backend.renew_run_lease(request.clone()).await.unwrap();
    assert_eq!(f.backend.renew_run_lease(request).await.unwrap(), lease);
    let branch = f.branch().await;
    std::thread::sleep(std::time::Duration::from_millis(f.reclaim_wait_ms));
    assert!(matches!(
        f.backend
            .inspect(InspectRequest {
                context: f.context.clone(),
                branch_id: branch,
                view: InspectView::Summary
            })
            .await,
        Err(BackendError::Rejected {
            code: ErrorCode::BranchNotFound,
            ..
        })
    ));
}

pub async fn reclaim_run(f: &Fixture<'_>) {
    let branch = f.branch().await;
    let job = f
        .backend
        .exec(ExecRequest {
            context: f.mutation("active"),
            branch_id: branch.clone(),
            argv: f.cancellable_argv.clone(),
            cwd: f.cwd.clone(),
        })
        .await
        .unwrap();
    let request = ReclaimRunRequest {
        context: f.mutation("reclaim"),
    };
    let result = f.backend.reclaim_run(request.clone()).await.unwrap();
    assert_eq!(result.status, ReclaimStatus::Complete);
    assert!(result.remaining.is_empty());
    assert_eq!(f.backend.reclaim_run(request).await.unwrap(), result);
    assert!(matches!(f.terminal(&job.job_id).await, JobState::Failed(_)));
    assert!(f
        .backend
        .inspect(InspectRequest {
            context: f.context.clone(),
            branch_id: branch,
            view: InspectView::Summary
        })
        .await
        .is_err());
}

pub async fn export_workspace(f: &Fixture<'_>, run_directory: &str) -> WorkspaceExported {
    let branch = f.branch().await;
    f.write(&branch, "write", "b25l").await;
    let request = ExportWorkspaceRequest {
        context: f.mutation("too-large"),
        source: WorkspaceRef::Branch(branch.clone()),
        run_directory: run_directory.into(),
        path: "selected.tar".into(),
        max_bytes: 1,
        include_vcs_metadata: false,
    };
    assert!(matches!(
        f.backend.export_workspace(request.clone()).await,
        Err(BackendError::Rejected {
            code: ErrorCode::ExportTooLarge,
            ..
        })
    ));
    assert!(!std::path::Path::new(run_directory)
        .join("selected.tar")
        .exists());
    let request = ExportWorkspaceRequest {
        context: f.mutation("export"),
        max_bytes: 1024 * 1024,
        ..request
    };
    let result = f.backend.export_workspace(request.clone()).await.unwrap();
    assert_eq!(f.backend.export_workspace(request).await.unwrap(), result);
    assert_eq!(result.file_count, 1);
    f.release(&branch, "release").await;
    result
}
