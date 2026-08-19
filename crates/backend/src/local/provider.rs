use crate::local::error::LocalBuildError;
use crate::local::factory::local_backend_with_isolation;
use crate::OperationAttach;
use async_trait::async_trait;
use operation_protocol::OperationBackend;
use provider_protocol::{
    Provider, ProviderCapabilities, ProviderCheckpointRequest, ProviderControlError,
    ProviderCreateRequest, ProviderDeleteOutcome, ProviderDeleteRequest, ProviderEndpoint,
    ProviderInspectRequest, ProviderInstance, ProviderInstanceStatus, ProviderKind,
    ProviderLifecycle, ProviderLifecycleOperation, ProviderLifecycleStateMachine,
    ProviderLoadRequest, ProviderLoadSource, ProviderOperationCapabilities, ProviderPauseRequest,
    ProviderResourceAllocation, ProviderSnapshot, ProviderSnapshotId,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// Options carried inside [`ProviderCreateRequest::provider_options`] /
/// [`ProviderLoadRequest::provider_options`] for the local provider. This
/// plays the role xiaoO's `LocalBackendOptions` played for `build_backend`,
/// but is scoped to what `create` needs since there is no generic
/// `OperationBackendConfig` dispatch here.
#[derive(Debug, Deserialize, serde::Serialize, Clone)]
#[serde(deny_unknown_fields)]
struct LocalProviderOptions {
    workspace_root: String,
    #[serde(default)]
    home_dir: Option<String>,
    #[serde(default)]
    temp_root: Option<String>,
    #[serde(default)]
    default_shell: Option<String>,
    #[serde(default)]
    isolation: Option<Value>,
}

struct LocalInstanceRecord {
    instance: ProviderInstance,
    backend: Arc<dyn OperationBackend>,
    workspace_root_host: PathBuf,
}

pub struct LocalProvider {
    kind: ProviderKind,
    registry: Mutex<HashMap<String, LocalInstanceRecord>>,
    next_sequence: AtomicU64,
}

impl LocalProvider {
    pub fn new() -> Self {
        Self {
            kind: ProviderKind("local".to_string()),
            registry: Mutex::new(HashMap::new()),
            next_sequence: AtomicU64::new(0),
        }
    }

    fn lock_registry(
        &self,
    ) -> Result<MutexGuard<'_, HashMap<String, LocalInstanceRecord>>, ProviderControlError> {
        self.registry
            .lock()
            .map_err(|_| ProviderControlError::Transport {
                message: "local provider registry lock poisoned".to_string(),
            })
    }

    fn next_instance_id(&self) -> String {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        format!("local-{}-{sequence}", std::process::id())
    }
}

impl Default for LocalProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Construct a fresh, empty [`LocalProvider`].
pub fn local_provider() -> LocalProvider {
    LocalProvider::new()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn local_build_error_to_control_error(
    kind: &ProviderKind,
    error: LocalBuildError,
) -> ProviderControlError {
    match error {
        LocalBuildError::InvalidConfig { message } => {
            ProviderControlError::InvalidRequest { message }
        }
        LocalBuildError::Unsupported { message } => ProviderControlError::ProviderFailure {
            provider: kind.clone(),
            message,
            details: Value::Null,
        },
    }
}

impl Provider for LocalProvider {
    fn kind(&self) -> &ProviderKind {
        &self.kind
    }

    fn lifecycle(&self) -> &dyn ProviderLifecycle {
        self
    }
}

#[async_trait]
impl ProviderLifecycle for LocalProvider {
    async fn create(
        &self,
        request: ProviderCreateRequest,
    ) -> Result<ProviderInstance, ProviderControlError> {
        let options: LocalProviderOptions =
            serde_json::from_value(request.provider_options.clone()).map_err(|error| {
                ProviderControlError::InvalidRequest {
                    message: format!("invalid local provider_options: {error}"),
                }
            })?;

        let workspace_root_host = PathBuf::from(options.workspace_root.as_str());
        let backend = local_backend_with_isolation(
            workspace_root_host.clone(),
            options.home_dir.as_deref().map(PathBuf::from),
            options.temp_root.as_deref().map(PathBuf::from),
            options.default_shell.clone(),
            options.isolation.clone(),
        )
        .map_err(|error| local_build_error_to_control_error(&self.kind, error))?;

        let state = ProviderLifecycleStateMachine::begin(None, ProviderLifecycleOperation::Create)?;
        let state = ProviderLifecycleStateMachine::complete_success(
            state,
            ProviderLifecycleOperation::Create,
        )?;

        let now = now_ms();
        let instance_id = self.next_instance_id();
        let mut lifecycle_capabilities = BTreeSet::new();
        lifecycle_capabilities.insert(provider_protocol::ProviderCapability::Pause);

        let instance = ProviderInstance {
            backend_id: request.backend_id,
            provider: self.kind.clone(),
            instance_id: provider_protocol::ProviderInstanceId(instance_id.clone()),
            state,
            endpoint: Some(ProviderEndpoint::Local),
            snapshot: None,
            capabilities: ProviderCapabilities {
                lifecycle: lifecycle_capabilities,
                operation_plane: ProviderOperationCapabilities {
                    exec: true,
                    file_read: true,
                    file_write: true,
                    search: true,
                    export_file: true,
                    lsp: true,
                    network: true,
                },
            },
            resources: ProviderResourceAllocation::default(),
            metadata: json!({ "provider_options": options }),
            created_at_ms: now,
            updated_at_ms: now,
        };

        let mut registry = self.lock_registry()?;
        registry.insert(
            instance_id,
            LocalInstanceRecord {
                instance: instance.clone(),
                backend,
                workspace_root_host,
            },
        );

        Ok(instance)
    }

    async fn load(
        &self,
        request: ProviderLoadRequest,
    ) -> Result<ProviderInstance, ProviderControlError> {
        let instance_id = match &request.source {
            ProviderLoadSource::Instance(id) => id.0.clone(),
            ProviderLoadSource::Snapshot(_) | ProviderLoadSource::SerializedHandle(_) => {
                return Err(ProviderControlError::UnsupportedCapability {
                    provider: self.kind.clone(),
                    capability: "load_from_snapshot_or_serialized_handle".to_string(),
                });
            }
        };

        let mut registry = self.lock_registry()?;
        let record = registry.get_mut(instance_id.as_str()).ok_or_else(|| {
            ProviderControlError::NotFound {
                resource_ref: instance_id.clone(),
            }
        })?;

        let next_state = ProviderLifecycleStateMachine::begin(
            Some(record.instance.state),
            ProviderLifecycleOperation::Load,
        )?;
        let next_state = ProviderLifecycleStateMachine::complete_success(
            next_state,
            ProviderLifecycleOperation::Load,
        )?;

        record.instance.state = next_state;
        record.instance.updated_at_ms = now_ms();
        Ok(record.instance.clone())
    }

    async fn pause(
        &self,
        request: ProviderPauseRequest,
    ) -> Result<ProviderSnapshot, ProviderControlError> {
        let instance_id = request.instance_id.0.clone();
        let mut registry = self.lock_registry()?;
        let record = registry.get_mut(instance_id.as_str()).ok_or_else(|| {
            ProviderControlError::NotFound {
                resource_ref: instance_id.clone(),
            }
        })?;

        let next_state = ProviderLifecycleStateMachine::begin(
            Some(record.instance.state),
            ProviderLifecycleOperation::Pause,
        )?;
        let next_state = ProviderLifecycleStateMachine::complete_success(
            next_state,
            ProviderLifecycleOperation::Pause,
        )?;
        record.instance.state = next_state;
        record.instance.updated_at_ms = now_ms();

        let snapshot = ProviderSnapshot {
            snapshot_id: ProviderSnapshotId(format!("{instance_id}-snapshot")),
            provider: self.kind.clone(),
            source_instance_id: Some(request.instance_id),
            serialized_handle: None,
            metadata: json!({ "instance_id": instance_id }),
            created_at_ms: now_ms(),
        };
        record.instance.snapshot = Some(snapshot.clone());
        Ok(snapshot)
    }

    async fn checkpoint(
        &self,
        _request: ProviderCheckpointRequest,
    ) -> Result<ProviderSnapshot, ProviderControlError> {
        Err(ProviderControlError::UnsupportedCapability {
            provider: self.kind.clone(),
            capability: "checkpoint".to_string(),
        })
    }

    async fn delete(
        &self,
        request: ProviderDeleteRequest,
    ) -> Result<ProviderDeleteOutcome, ProviderControlError> {
        let Some(instance_id) = request.instance_id.clone() else {
            // The local provider has no snapshot store independent of a live
            // instance, so a snapshot-only delete request is not something
            // it can satisfy — reject explicitly rather than silently no-op.
            if request.snapshot_id.is_some() {
                return Err(ProviderControlError::UnsupportedCapability {
                    provider: self.kind.clone(),
                    capability: "delete_by_snapshot_id".to_string(),
                });
            }
            return Err(ProviderControlError::InvalidRequest {
                message: "delete requires instance_id for the local provider".to_string(),
            });
        };

        let mut registry = self.lock_registry()?;
        let deleted = if let Some(record) = registry.get_mut(instance_id.0.as_str()) {
            let next_state = ProviderLifecycleStateMachine::begin(
                Some(record.instance.state),
                ProviderLifecycleOperation::Delete,
            )?;
            let _ = ProviderLifecycleStateMachine::complete_success(
                next_state,
                ProviderLifecycleOperation::Delete,
            )?;
            registry.remove(instance_id.0.as_str());
            true
        } else {
            false
        };

        Ok(ProviderDeleteOutcome {
            backend_id: request.backend_id,
            provider: self.kind.clone(),
            instance_id: Some(instance_id),
            deleted,
            retained_snapshots: Vec::new(),
            deleted_snapshots: Vec::new(),
            correlation: request.correlation,
        })
    }

    async fn inspect(
        &self,
        request: ProviderInspectRequest,
    ) -> Result<ProviderInstanceStatus, ProviderControlError> {
        let Some(instance_id) = request.instance_id.clone() else {
            return Err(ProviderControlError::InvalidRequest {
                message: "inspect requires instance_id for the local provider".to_string(),
            });
        };

        let mut registry = self.lock_registry()?;
        let record = registry.get_mut(instance_id.0.as_str()).ok_or_else(|| {
            ProviderControlError::NotFound {
                resource_ref: instance_id.0.clone(),
            }
        })?;

        // Best-effort liveness check: if the workspace directory has
        // disappeared from disk out-of-band (e.g. the user deleted it), the
        // tracked in-memory state would otherwise stay stale at
        // Active/Paused forever, since local has no separate health signal.
        let mut last_error = None;
        if !record.workspace_root_host.exists() {
            record.instance.state = provider_protocol::ProviderLifecycleState::Failed;
            last_error = Some(format!(
                "workspace_root no longer exists: {}",
                record.workspace_root_host.display()
            ));
        }
        record.instance.updated_at_ms = now_ms();

        Ok(ProviderInstanceStatus {
            backend_id: record.instance.backend_id.clone(),
            provider: self.kind.clone(),
            instance_id: Some(record.instance.instance_id.clone()),
            state: record.instance.state,
            endpoint: record.instance.endpoint.clone(),
            snapshot: record.instance.snapshot.clone(),
            capabilities: record.instance.capabilities.clone(),
            resources: record.instance.resources,
            last_error,
            metadata: record.instance.metadata.clone(),
            updated_at_ms: record.instance.updated_at_ms,
        })
    }

    /// Snapshot of this process's in-memory registry. Faithful by
    /// construction: `LocalProvider` has no state outside this registry, so
    /// there is nothing else to "list" here — a freshly constructed
    /// provider (as happens on every daemon restart) always returns an
    /// empty list, even though the underlying workspace directories on disk
    /// (and thus the instances themselves, in the sense that matters to a
    /// caller) generally do survive a restart. `attach()`'s doc comment
    /// covers how that survivorship is recovered on demand from
    /// `ProviderInstance.metadata`, without this method's answer changing —
    /// `list_instances` intentionally still only reflects the live
    /// registry, not what could be reconstructed (see
    /// `xgovernor_manager::InstanceManager::reconcile`'s doc, in
    /// `crates/manager`, for why reconcile's own re-attach step is what
    /// actually repopulates this list across a restart, rather than this
    /// method growing reconstruction logic of its own).
    async fn list_instances(&self) -> Result<Vec<ProviderInstance>, ProviderControlError> {
        let registry = self.lock_registry()?;
        Ok(registry
            .values()
            .map(|record| record.instance.clone())
            .collect())
    }
}

#[async_trait]
impl OperationAttach for LocalProvider {
    /// Fast path: the instance is still in this process's in-memory
    /// registry (the common case — no restart happened since `create()`).
    ///
    /// Slow path (post-restart reattach, Fix B / Finding 1 root cause B):
    /// a freshly constructed `LocalProvider` (as happens on every daemon
    /// restart, see `list_instances`'s doc comment) always starts with an
    /// empty registry, so a registry miss does not necessarily mean the
    /// instance is gone — it may simply not have been re-registered yet
    /// after a restart. Unlike e2b, there is no remote control-plane call
    /// needed to check liveness here: a "local instance" is nothing more
    /// than a workspace directory on the host filesystem plus a sandboxing
    /// policy (bubblewrap/seatbelt) applied per-operation, so recovering it
    /// is exactly the same synchronous, side-effect-free construction
    /// `create()` did originally, replayed from the `provider_options` that
    /// `create()` already persisted onto `instance.metadata` (see
    /// `LocalProviderOptions`'s `Serialize` derive and `create()`). If the
    /// workspace directory itself is gone, this fails closed with
    /// `NotFound` — matching `inspect()`'s existing liveness check — rather
    /// than silently fabricating a backend over a directory that no longer
    /// exists.
    async fn attach(
        &self,
        instance: &ProviderInstance,
    ) -> Result<Arc<dyn OperationBackend>, ProviderControlError> {
        {
            let registry = self.lock_registry()?;
            if let Some(record) = registry.get(instance.instance_id.0.as_str()) {
                return Ok(Arc::clone(&record.backend));
            }
        }

        let options: LocalProviderOptions = instance
            .metadata
            .get("provider_options")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| ProviderControlError::ProviderFailure {
                provider: self.kind.clone(),
                message: format!(
                    "cannot reattach local instance {}: stored provider_options is malformed: {error}",
                    instance.instance_id.0
                ),
                details: Value::Null,
            })?
            .ok_or_else(|| ProviderControlError::NotFound {
                resource_ref: instance.instance_id.0.clone(),
            })?;

        let workspace_root_host = PathBuf::from(options.workspace_root.as_str());
        if !workspace_root_host.exists() {
            return Err(ProviderControlError::NotFound {
                resource_ref: instance.instance_id.0.clone(),
            });
        }

        let backend = local_backend_with_isolation(
            workspace_root_host.clone(),
            options.home_dir.as_deref().map(PathBuf::from),
            options.temp_root.as_deref().map(PathBuf::from),
            options.default_shell.clone(),
            options.isolation.clone(),
        )
        .map_err(|error| local_build_error_to_control_error(&self.kind, error))?;

        let mut registry = self.lock_registry()?;
        registry.insert(
            instance.instance_id.0.clone(),
            LocalInstanceRecord {
                instance: instance.clone(),
                backend: Arc::clone(&backend),
                workspace_root_host,
            },
        );

        Ok(backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use operation_protocol::capability::exec::ExecRequest;
    use provider_protocol::{
        BackendId, ProviderInstanceId, ProviderLifecycleReason, ProviderLifecycleState,
    };

    fn test_workspace() -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "xgovernor-local-provider-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(root.as_path()).unwrap();
        root
    }

    fn create_request(workspace_root: &PathBuf) -> ProviderCreateRequest {
        ProviderCreateRequest {
            backend_id: BackendId("local".to_string()),
            owner_ref: "test-owner".to_string(),
            reason: ProviderLifecycleReason::Acquire,
            resource_limits: Default::default(),
            provider_options: json!({ "workspace_root": workspace_root.to_string_lossy() }),
            correlation: Value::Null,
        }
    }

    #[tokio::test]
    async fn create_then_inspect_reports_active() {
        let provider = LocalProvider::new();
        let workspace = test_workspace();

        let instance = provider
            .lifecycle()
            .create(create_request(&workspace))
            .await
            .unwrap();
        assert_eq!(instance.state, ProviderLifecycleState::Active);

        let status = provider
            .lifecycle()
            .inspect(ProviderInspectRequest {
                backend_id: instance.backend_id.clone(),
                instance_id: Some(instance.instance_id.clone()),
                reason: ProviderLifecycleReason::Reconcile,
                correlation: Value::Null,
            })
            .await
            .unwrap();
        assert_eq!(status.state, ProviderLifecycleState::Active);
        assert!(status.last_error.is_none());

        let _ = std::fs::remove_dir_all(workspace.as_path());
    }

    #[tokio::test]
    async fn attach_returns_operation_backend_after_create() {
        let provider = LocalProvider::new();
        let workspace = test_workspace();

        let instance = provider
            .lifecycle()
            .create(create_request(&workspace))
            .await
            .unwrap();

        let backend = provider.attach(&instance).await.unwrap();
        assert_eq!(backend.backend_id(), "local");

        let _ = std::fs::remove_dir_all(workspace.as_path());
    }

    /// End-to-end smoke test: drive the full control-plane -> operation-plane
    /// path a real caller would use — create an instance through
    /// `ProviderLifecycle`, attach it to get an `Arc<dyn OperationBackend>`,
    /// and actually run a shell command through it. This is the concrete
    /// proof that the `provider-protocol` wiring and the local operation
    /// backend are connected end to end, not just that the types line up.
    #[tokio::test]
    async fn end_to_end_exec_through_attached_backend() {
        let provider = LocalProvider::new();
        let workspace = test_workspace();

        let instance = provider
            .lifecycle()
            .create(create_request(&workspace))
            .await
            .unwrap();
        assert_eq!(instance.state, ProviderLifecycleState::Active);

        let backend = provider.attach(&instance).await.unwrap();
        let result = backend
            .exec()
            .exec(ExecRequest {
                command: "echo".to_string(),
                args: vec!["hello-from-xgovernor".to_string()],
                shell: None,
                cwd: None,
                timeout_ms: Some(5_000),
                env: None,
            })
            .await
            .unwrap();

        assert_eq!(result.exit_code, Some(0));
        assert!(String::from_utf8_lossy(&result.stdout).contains("hello-from-xgovernor"));

        let outcome = provider
            .lifecycle()
            .delete(ProviderDeleteRequest {
                backend_id: instance.backend_id.clone(),
                instance_id: Some(instance.instance_id.clone()),
                snapshot_id: None,
                reason: ProviderLifecycleReason::Release,
                correlation: Value::Null,
            })
            .await
            .unwrap();
        assert!(outcome.deleted);

        let _ = std::fs::remove_dir_all(workspace.as_path());
    }

    #[tokio::test]
    async fn pause_then_load_returns_to_active() {
        let provider = LocalProvider::new();
        let workspace = test_workspace();

        let instance = provider
            .lifecycle()
            .create(create_request(&workspace))
            .await
            .unwrap();

        let snapshot = provider
            .lifecycle()
            .pause(ProviderPauseRequest {
                backend_id: instance.backend_id.clone(),
                instance_id: instance.instance_id.clone(),
                mode: provider_protocol::ProviderPauseMode::Suspend,
                reason: ProviderLifecycleReason::Release,
                correlation: Value::Null,
            })
            .await
            .unwrap();
        assert_eq!(
            snapshot.source_instance_id,
            Some(instance.instance_id.clone())
        );

        let reloaded = provider
            .lifecycle()
            .load(ProviderLoadRequest {
                backend_id: instance.backend_id.clone(),
                owner_ref: "test-owner".to_string(),
                source: ProviderLoadSource::Instance(instance.instance_id.clone()),
                reason: ProviderLifecycleReason::Restore,
                resource_limits: Default::default(),
                provider_options: Value::Null,
                correlation: Value::Null,
            })
            .await
            .unwrap();
        assert_eq!(reloaded.state, ProviderLifecycleState::Active);

        let _ = std::fs::remove_dir_all(workspace.as_path());
    }

    #[tokio::test]
    async fn delete_removes_instance_and_is_idempotent() {
        let provider = LocalProvider::new();
        let workspace = test_workspace();

        let instance = provider
            .lifecycle()
            .create(create_request(&workspace))
            .await
            .unwrap();

        let outcome = provider
            .lifecycle()
            .delete(ProviderDeleteRequest {
                backend_id: instance.backend_id.clone(),
                instance_id: Some(instance.instance_id.clone()),
                snapshot_id: None,
                reason: ProviderLifecycleReason::Release,
                correlation: Value::Null,
            })
            .await
            .unwrap();
        assert!(outcome.deleted);

        let outcome_again = provider
            .lifecycle()
            .delete(ProviderDeleteRequest {
                backend_id: instance.backend_id.clone(),
                instance_id: Some(instance.instance_id.clone()),
                snapshot_id: None,
                reason: ProviderLifecycleReason::Release,
                correlation: Value::Null,
            })
            .await
            .unwrap();
        assert!(!outcome_again.deleted);

        let _ = std::fs::remove_dir_all(workspace.as_path());
    }

    #[tokio::test]
    async fn delete_by_snapshot_id_only_is_unsupported() {
        let provider = LocalProvider::new();

        let error = provider
            .lifecycle()
            .delete(ProviderDeleteRequest {
                backend_id: BackendId("local".to_string()),
                instance_id: None,
                snapshot_id: Some(ProviderSnapshotId("some-snapshot".to_string())),
                reason: ProviderLifecycleReason::Release,
                correlation: Value::Null,
            })
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ProviderControlError::UnsupportedCapability { .. }
        ));
    }

    #[tokio::test]
    async fn inspect_missing_instance_returns_not_found() {
        let provider = LocalProvider::new();

        let error = provider
            .lifecycle()
            .inspect(ProviderInspectRequest {
                backend_id: BackendId("local".to_string()),
                instance_id: Some(ProviderInstanceId("does-not-exist".to_string())),
                reason: ProviderLifecycleReason::Reconcile,
                correlation: Value::Null,
            })
            .await
            .unwrap_err();

        assert!(matches!(error, ProviderControlError::NotFound { .. }));
    }

    #[tokio::test]
    async fn inspect_reports_failed_when_workspace_root_disappears() {
        let provider = LocalProvider::new();
        let workspace = test_workspace();

        let instance = provider
            .lifecycle()
            .create(create_request(&workspace))
            .await
            .unwrap();

        std::fs::remove_dir_all(workspace.as_path()).unwrap();

        let status = provider
            .lifecycle()
            .inspect(ProviderInspectRequest {
                backend_id: instance.backend_id.clone(),
                instance_id: Some(instance.instance_id.clone()),
                reason: ProviderLifecycleReason::Reconcile,
                correlation: Value::Null,
            })
            .await
            .unwrap();

        assert_eq!(status.state, ProviderLifecycleState::Failed);
        assert!(status.last_error.is_some());
    }
}
