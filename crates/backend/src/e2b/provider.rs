//! E2B cloud sandbox provider: implements [`Provider`] / [`ProviderLifecycle`]
//! against the real E2B platform API (sandbox creation, envd bootstrap of the
//! workspace/temp roots, and snapshot-based pause/load), and bridges to the
//! operation plane via [`OperationAttach`].
//!
//! Scope of this port (see task history): bootstrap/skill archive injection
//! (xiaoO's `install_bootstrap_archive`) is ported in `bootstrap.rs` and
//! wired into `provision_sandbox()` below — `create()` accepts a caller-
//! supplied list of already-resolved skill directory paths and only handles
//! archiving/uploading/extracting; it has no `SkillsConfig`/skill-selection
//! logic of its own (that stays a caller concern, split across the xiaoO/
//! xGovernor repo boundary). Pause/load use the real E2B snapshot API (not a
//! local-style state-only placeholder): `pause()` creates a real E2B
//! snapshot and deletes the underlying sandbox (E2B has no "suspend in
//! place"), and `load()` always creates a brand-new sandbox from a
//! template/snapshot id — there is no way to resume the exact same sandbox
//! process in E2B's model.

use super::backend::{
    configured_activity_refresh_throttle, envd_host, join_url, normalize_backend_path,
    parse_error_message, shell_quote, E2bBackendState, E2bLifecycle, E2bOperationBackend,
    DEFAULT_API_BASE, DEFAULT_DOMAIN, DEFAULT_ENVD_PORT, DEFAULT_HOME_DIR, DEFAULT_SHELL,
    DEFAULT_TEMPLATE_ID, DEFAULT_TEMP_ROOT, DEFAULT_TIMEOUT_SECS, DEFAULT_WORKSPACE_ROOT,
    E2B_PROVIDER_KIND,
};
use super::bootstrap::{apply_e2b_bootstrap, E2bBootstrapPlan};
use super::error::E2bFailure;
use super::exec::{E2bExec, E2bExecFailure, E2bExecOutput};
use crate::OperationAttach;
use async_trait::async_trait;
use operation_protocol::{BackendPath, OperationBackend};
use provider_protocol::{
    BackendId, Provider, ProviderCapabilities, ProviderCapability, ProviderCheckpointRequest,
    ProviderControlError, ProviderCreateRequest, ProviderDeleteOutcome, ProviderDeleteRequest,
    ProviderEndpoint, ProviderInspectRequest, ProviderInstance, ProviderInstanceId,
    ProviderInstanceStatus, ProviderKind, ProviderLifecycle, ProviderLifecycleOperation,
    ProviderLifecycleState, ProviderLifecycleStateMachine, ProviderLoadRequest, ProviderLoadSource,
    ProviderOperationCapabilities, ProviderPauseRequest, ProviderResourceAllocation,
    ProviderResourceLimits, ProviderSnapshot, ProviderSnapshotId,
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

const E2B_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const E2B_ENVD_SCHEME_ENV: &str = "E2B_ENVD_SCHEME";
const DEFAULT_ENVD_SCHEME: &str = "https";
const WORKSPACE_INIT_MAX_ATTEMPTS: usize = 6;
const WORKSPACE_INIT_BASE_DELAY_MS: u64 = 200;
const WORKSPACE_INIT_MAX_DELAY_MS: u64 = 2_000;

/// Options carried inside [`ProviderCreateRequest::provider_options`] /
/// [`ProviderLoadRequest::provider_options`] for the e2b provider. Mechanical
/// port of xiaoO's `E2bProviderOptions`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct E2bProviderOptions {
    api_key: Option<String>,
    api_key_env: Option<String>,
    #[serde(alias = "apiBase", alias = "api_url", alias = "apiUrl")]
    api_base: Option<String>,
    #[serde(alias = "sandbox_domain", alias = "sandboxDomain")]
    domain: Option<String>,
    #[serde(alias = "templateID", alias = "template")]
    template_id: Option<String>,
    #[serde(alias = "timeout")]
    timeout_secs: Option<u64>,
    secure: Option<bool>,
    #[serde(alias = "allowInternetAccess")]
    allow_internet_access: Option<bool>,
    #[serde(alias = "autoPause")]
    auto_pause: Option<bool>,
    #[serde(alias = "autoResume")]
    auto_resume: Option<bool>,
    #[serde(alias = "envdPort")]
    envd_port: Option<u16>,
    #[serde(alias = "envdScheme")]
    envd_scheme: Option<String>,
    #[serde(alias = "workspaceRoot", alias = "remoteWorkspaceRoot")]
    workspace_root: Option<String>,
    #[serde(alias = "homeDir")]
    home_dir: Option<String>,
    #[serde(alias = "tempRoot")]
    temp_root: Option<String>,
    #[serde(alias = "defaultShell")]
    default_shell: Option<String>,
    /// `multipart` for legacy envd; omitted or any other value keeps octet-stream.
    #[serde(alias = "envdFileUpload")]
    envd_file_upload: Option<String>,
    username: Option<String>,
    metadata: Option<BTreeMap<String, String>>,
    #[serde(alias = "envVars")]
    env_vars: Option<BTreeMap<String, String>>,
    network: Option<Value>,
    mcp: Option<Value>,
    #[serde(alias = "volumeMounts")]
    volume_mounts: Option<Value>,
    /// Optional workspace/skill archive injection, applied once right after
    /// the sandbox's workspace/temp roots are created. Provider-only
    /// packaging/upload: `skill_dirs` must already be the caller's final,
    /// resolved list of directories to inject — this provider does not
    /// select, dedupe, or parse skill manifests itself.
    bootstrap: Option<E2bBootstrapOptions>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct E2bBootstrapOptions {
    workspace: Option<PathBuf>,
    #[serde(default, alias = "skillDirs")]
    skill_dirs: Vec<PathBuf>,
    #[serde(alias = "skillsRoot")]
    skills_root: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateSandboxResponse {
    #[serde(rename = "sandboxID")]
    sandbox_id: String,
    #[serde(rename = "templateID")]
    template_id: String,
    #[serde(rename = "envdAccessToken")]
    envd_access_token: Option<String>,
    #[serde(rename = "trafficAccessToken")]
    traffic_access_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateSnapshotResponse {
    #[serde(rename = "snapshotID")]
    snapshot_id: String,
    #[serde(default)]
    names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct E2bConnectionOptions {
    api_base: String,
    sandbox_domain: String,
}

/// A live, attached e2b sandbox: the state driving HTTP/envd calls, and the
/// operation-plane backend wrapping it.
struct E2bLiveHandle {
    state: Arc<E2bBackendState>,
    backend: Arc<dyn OperationBackend>,
}

struct E2bInstanceRecord {
    instance: ProviderInstance,
    /// The provider_options last used to stand up (or reload) this instance.
    /// Retained after the sandbox is deleted (e.g. on pause) since pause/
    /// delete requests carry no `provider_options` of their own — reload/
    /// snapshot/delete calls need the api key & connection settings again.
    provider_options: Value,
    /// `None` when the instance has no live sandbox (e.g. paused).
    live: Option<E2bLiveHandle>,
}

pub struct E2bProvider {
    kind: ProviderKind,
    registry: Mutex<HashMap<String, E2bInstanceRecord>>,
}

fn configured_default_timeout_secs() -> u64 {
    match std::env::var("XGOVERNOR_E2B_TIMEOUT_SECS") {
        Ok(value) => match value.parse::<u64>() {
            Ok(secs) if secs > 0 => secs,
            _ => {
                tracing::warn!(value = %value, default_secs = DEFAULT_TIMEOUT_SECS, "invalid XGOVERNOR_E2B_TIMEOUT_SECS; using default");
                DEFAULT_TIMEOUT_SECS
            }
        },
        Err(_) => DEFAULT_TIMEOUT_SECS,
    }
}

impl E2bProvider {
    pub fn new() -> Self {
        Self {
            kind: ProviderKind(E2B_PROVIDER_KIND.to_string()),
            registry: Mutex::new(HashMap::new()),
        }
    }

    fn lock_registry(
        &self,
    ) -> Result<MutexGuard<'_, HashMap<String, E2bInstanceRecord>>, ProviderControlError> {
        self.registry
            .lock()
            .map_err(|_| ProviderControlError::Transport {
                message: "e2b provider registry lock poisoned".to_string(),
            })
    }

    /// Create a brand-new E2B sandbox and register it. Used by `create()`
    /// (operation = `Create`, no template override) and by both branches of
    /// `load()` (operation = `Load`, `template_override` set to the
    /// snapshot/template id being loaded from).
    #[allow(clippy::too_many_arguments)]
    async fn provision_sandbox(
        &self,
        backend_id: BackendId,
        owner_ref: String,
        resource_limits: ProviderResourceLimits,
        provider_options_value: Value,
        template_override: Option<String>,
        operation: ProviderLifecycleOperation,
    ) -> Result<(ProviderInstance, E2bLiveHandle, Value), ProviderControlError> {
        let options = parse_options(&provider_options_value)?;
        let api_key = resolve_api_key(&options)?;
        let http = new_e2b_http_client()?;
        let connection = resolve_connection_options(&options)?;
        let api_base = connection.api_base;
        let sandbox_domain = connection.sandbox_domain;

        let template_id = template_override.unwrap_or_else(|| {
            options
                .template_id
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(DEFAULT_TEMPLATE_ID)
                .to_string()
        });

        let workspace_root = backend_path(
            options
                .workspace_root
                .as_deref()
                .unwrap_or(DEFAULT_WORKSPACE_ROOT),
        )?;
        let home_dir = options
            .home_dir
            .as_deref()
            .map(backend_path)
            .transpose()?
            .or_else(|| Some(BackendPath(DEFAULT_HOME_DIR.to_string())));
        let temp_root = backend_path(options.temp_root.as_deref().unwrap_or(DEFAULT_TEMP_ROOT))?;
        let envd_port = options.envd_port.unwrap_or(DEFAULT_ENVD_PORT);
        let envd_scheme = resolve_envd_scheme(&options)?;
        let timeout_secs = options
            .timeout_secs
            .or_else(|| resource_limits.timeout_ms.map(|ms| ms / 1000))
            .unwrap_or_else(configured_default_timeout_secs);

        let created = create_e2b_sandbox(
            &http,
            api_base.as_str(),
            api_key.as_str(),
            template_id.as_str(),
            timeout_secs,
            &options,
            backend_id.0.as_str(),
            owner_ref.as_str(),
        )
        .await?;

        let now = now_ms();
        let endpoint = provider_handle(
            &created,
            envd_port,
            envd_scheme.as_str(),
            sandbox_domain.as_str(),
        );

        let state = Arc::new_cyclic(|self_weak| E2bBackendState {
            backend_id: backend_id.0.clone(),
            api_base,
            api_key,
            sandbox_id: created.sandbox_id.clone(),
            sandbox_domain,
            envd_access_token: created.envd_access_token.clone(),
            envd_port,
            envd_scheme,
            workspace_root: workspace_root.clone(),
            home_dir,
            temp_root,
            default_shell: Some(
                options
                    .default_shell
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| DEFAULT_SHELL.to_string()),
            ),
            username: options
                .username
                .clone()
                .filter(|value| !value.trim().is_empty()),
            envd_file_upload_multipart: options
                .envd_file_upload
                .as_deref()
                .is_some_and(|value| value.trim().eq_ignore_ascii_case("multipart")),
            http: http.clone(),
            lifecycle: Mutex::new(E2bLifecycle::Active),
            timeout_secs,
            activity_refresh_throttle: configured_activity_refresh_throttle(),
            last_refresh: Mutex::new(Instant::now()),
            self_weak: self_weak.clone(),
        });
        let backend: Arc<dyn OperationBackend> =
            Arc::new(E2bOperationBackend::new(Arc::clone(&state)));

        if let Err(error) = ensure_remote_roots(&state).await {
            return Err(self
                .abort_provisioning(
                    &backend_id,
                    &resource_limits,
                    &provider_options_value,
                    &state,
                    &backend,
                    "workspace init failed",
                    error.to_string(),
                )
                .await
                .unwrap_or(error));
        }

        let bootstrap_summary = if let Some(bootstrap) = options.bootstrap.as_ref() {
            let remote_skills_root = bootstrap
                .skills_root
                .clone()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| {
                    let home = state
                        .home_dir
                        .as_ref()
                        .map(|path| path.0.as_str())
                        .unwrap_or(DEFAULT_HOME_DIR);
                    format!("{home}/.xgovernor/skills")
                });
            let plan = E2bBootstrapPlan {
                workspace: bootstrap.workspace.clone(),
                skill_dirs: bootstrap.skill_dirs.clone(),
                remote_skills_root,
            };
            match apply_e2b_bootstrap(&state, &plan).await {
                Ok(summary) => Some(summary),
                Err(error) => {
                    return Err(self
                        .abort_provisioning(
                            &backend_id,
                            &resource_limits,
                            &provider_options_value,
                            &state,
                            &backend,
                            "bootstrap injection failed",
                            error.to_string(),
                        )
                        .await
                        .unwrap_or(ProviderControlError::ProviderFailure {
                            provider: self.kind.clone(),
                            message: format!(
                                "e2b sandbox {} bootstrap injection failed: {error}",
                                state.sandbox_id
                            ),
                            details: Value::Null,
                        }))
                }
            }
        } else {
            None
        };

        let lifecycle_state = ProviderLifecycleStateMachine::begin(None, operation)?;
        let lifecycle_state =
            ProviderLifecycleStateMachine::complete_success(lifecycle_state, operation)?;

        let mut lifecycle_capabilities = BTreeSet::new();
        lifecycle_capabilities.insert(ProviderCapability::Pause);
        lifecycle_capabilities.insert(ProviderCapability::Snapshot);

        let mut metadata = metadata_for_instance(
            &provider_options_value,
            &created,
            &options,
            owner_ref.as_str(),
        );
        if let Some(summary) = bootstrap_summary {
            if let Value::Object(object) = &mut metadata {
                object.insert(
                    "bootstrap".to_string(),
                    json!({
                        "archive_sha256": summary.archive_sha256,
                        "archive_size_bytes": summary.archive_size_bytes,
                        "remote_workspace_root": summary.remote_workspace_root,
                        "remote_skills_root": summary.remote_skills_root,
                        "skills": summary
                            .skills
                            .iter()
                            .map(|skill| json!({
                                "source": skill.source.display().to_string(),
                                "remote_dir": skill.remote_dir,
                            }))
                            .collect::<Vec<_>>(),
                    }),
                );
            }
        }

        let instance = ProviderInstance {
            backend_id,
            provider: self.kind.clone(),
            instance_id: ProviderInstanceId(created.sandbox_id.clone()),
            state: lifecycle_state,
            endpoint: Some(endpoint),
            snapshot: None,
            capabilities: ProviderCapabilities {
                lifecycle: lifecycle_capabilities,
                operation_plane: ProviderOperationCapabilities {
                    exec: true,
                    file_read: true,
                    file_write: true,
                    search: true,
                    export_file: true,
                    lsp: false,
                    network: true,
                },
            },
            resources: ProviderResourceAllocation {
                vcpu_count: resource_limits.vcpu_count,
                memory_mb: resource_limits.memory_mb,
                disk_mb: resource_limits.disk_mb,
            },
            metadata,
            created_at_ms: now,
            updated_at_ms: now,
        };

        Ok((
            instance,
            E2bLiveHandle { state, backend },
            provider_options_value,
        ))
    }

    async fn reattach_from_persisted_instance(
        &self,
        instance: &ProviderInstance,
    ) -> Result<(Arc<E2bBackendState>, Arc<dyn OperationBackend>, Value), ProviderControlError>
    {
        let provider_options_value = instance
            .metadata
            .as_object()
            .and_then(|object| object.get("provider_options"))
            .cloned()
            .unwrap_or(Value::Null);
        let options = parse_options(&provider_options_value)?;
        let api_key = resolve_api_key(&options)?;
        let connection = resolve_connection_options(&options)?;
        let http = new_e2b_http_client()?;

        let detail = fetch_sandbox_detail(
            &http,
            connection.api_base.as_str(),
            api_key.as_str(),
            instance.instance_id.0.as_str(),
        )
        .await?;
        if detail.state != "running" {
            // Not actually alive (paused/killed/unknown) despite the
            // caller's own liveness check — treat exactly like a registry
            // miss with no platform match. Fail-closed, matches
            // `list_instances`'s "running" -> Active mapping.
            return Err(ProviderControlError::NotFound {
                resource_ref: instance.instance_id.0.clone(),
            });
        }

        let workspace_root = backend_path(
            options
                .workspace_root
                .as_deref()
                .unwrap_or(DEFAULT_WORKSPACE_ROOT),
        )?;
        let home_dir = options
            .home_dir
            .as_deref()
            .map(backend_path)
            .transpose()?
            .or_else(|| Some(BackendPath(DEFAULT_HOME_DIR.to_string())));
        let temp_root = backend_path(options.temp_root.as_deref().unwrap_or(DEFAULT_TEMP_ROOT))?;
        let envd_port = options.envd_port.unwrap_or(DEFAULT_ENVD_PORT);
        let envd_scheme = resolve_envd_scheme(&options)?;
        // The platform's actual remaining deadline isn't re-fetched here
        // (`SandboxDetailResponse` doesn't carry it) — re-asserting the
        // originally-requested timeout on the next keep-alive refresh is
        // safe regardless of how much of the previous deadline was left.
        let timeout_secs = options
            .timeout_secs
            .unwrap_or_else(configured_default_timeout_secs);

        let state = Arc::new_cyclic(|self_weak| E2bBackendState {
            backend_id: instance.backend_id.0.clone(),
            api_base: connection.api_base,
            api_key,
            sandbox_id: instance.instance_id.0.clone(),
            sandbox_domain: connection.sandbox_domain,
            envd_access_token: detail.envd_access_token,
            envd_port,
            envd_scheme,
            workspace_root,
            home_dir,
            temp_root,
            default_shell: Some(
                options
                    .default_shell
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| DEFAULT_SHELL.to_string()),
            ),
            username: options
                .username
                .clone()
                .filter(|value| !value.trim().is_empty()),
            envd_file_upload_multipart: options
                .envd_file_upload
                .as_deref()
                .is_some_and(|value| value.trim().eq_ignore_ascii_case("multipart")),
            http,
            lifecycle: Mutex::new(E2bLifecycle::Active),
            timeout_secs,
            activity_refresh_throttle: configured_activity_refresh_throttle(),
            last_refresh: Mutex::new(Instant::now()),
            self_weak: self_weak.clone(),
        });
        let backend: Arc<dyn OperationBackend> =
            Arc::new(E2bOperationBackend::new(Arc::clone(&state)));

        Ok((state, backend, provider_options_value))
    }

    /// Handles a fatal provisioning failure (workspace init or bootstrap
    /// injection) after a sandbox has already been created on the E2B
    /// platform: attempts to delete the sandbox to avoid leaking it, and
    /// only registers a `Failed` ghost record (recoverable via `delete()`)
    /// if that cleanup delete *also* fails. Returns `Some` with a
    /// `ProviderFailure` describing both errors when a ghost record was
    /// registered; returns `None` when cleanup succeeded, so the caller
    /// should surface its own original error instead (the sandbox is
    /// already gone, nothing to recover).
    async fn abort_provisioning(
        &self,
        backend_id: &BackendId,
        resource_limits: &ProviderResourceLimits,
        provider_options_value: &Value,
        state: &Arc<E2bBackendState>,
        backend: &Arc<dyn OperationBackend>,
        reason: &str,
        error_message: String,
    ) -> Option<ProviderControlError> {
        let cleanup_error = match state.delete_sandbox().await {
            Ok(()) => return None,
            Err(error) => error.to_string(),
        };

        tracing::error!(
            sandbox_id = %state.sandbox_id,
            reason,
            init_error = %error_message,
            cleanup_error = %cleanup_error,
            "e2b sandbox leaked: provisioning failed and cleanup delete also failed; registering a failed ghost record so it can be retried via delete()"
        );

        let now = now_ms();
        let mut lifecycle_capabilities = BTreeSet::new();
        lifecycle_capabilities.insert(ProviderCapability::Pause);
        lifecycle_capabilities.insert(ProviderCapability::Snapshot);
        let ghost = ProviderInstance {
            backend_id: backend_id.clone(),
            provider: self.kind.clone(),
            instance_id: ProviderInstanceId(state.sandbox_id.clone()),
            state: ProviderLifecycleState::Failed,
            endpoint: None,
            snapshot: None,
            capabilities: ProviderCapabilities {
                lifecycle: lifecycle_capabilities,
                operation_plane: ProviderOperationCapabilities {
                    exec: false,
                    file_read: false,
                    file_write: false,
                    search: false,
                    export_file: false,
                    lsp: false,
                    network: false,
                },
            },
            resources: ProviderResourceAllocation {
                vcpu_count: resource_limits.vcpu_count,
                memory_mb: resource_limits.memory_mb,
                disk_mb: resource_limits.disk_mb,
            },
            metadata: json!({
                "provider": "e2b",
                "sandbox_id": state.sandbox_id,
                "leaked": true,
                "reason": reason,
                "init_error": error_message,
                "cleanup_error": cleanup_error,
            }),
            created_at_ms: now,
            updated_at_ms: now,
        };
        let leaked_instance_id = ghost.instance_id.clone();

        if let Ok(mut registry) = self.lock_registry() {
            registry.insert(
                leaked_instance_id.0.clone(),
                E2bInstanceRecord {
                    instance: ghost,
                    provider_options: provider_options_value.clone(),
                    live: Some(E2bLiveHandle {
                        state: Arc::clone(state),
                        backend: Arc::clone(backend),
                    }),
                },
            );
        }

        Some(ProviderControlError::ProviderFailure {
            provider: self.kind.clone(),
            message: format!(
                "e2b sandbox {} creation aborted ({reason}: {error_message}) and cleanup delete also failed ({cleanup_error}); the sandbox was NOT deleted on the E2B platform and has been registered under instance_id {} so it can be retried via delete()",
                state.sandbox_id, leaked_instance_id.0
            ),
            details: json!({
                "leaked_sandbox_id": state.sandbox_id,
                "recoverable_instance_id": leaked_instance_id.0,
            }),
        })
    }

    /// Delete a snapshot that has no live instance backing it. When the
    /// current process still has a registry record, reuse its connection
    /// options; after a daemon restart, fall back to the normal E2B
    /// environment/default resolution so a checkpoint persisted in SQLite
    /// remains user-deletable.
    async fn delete_snapshot_only(
        &self,
        backend_id: BackendId,
        snapshot_id: ProviderSnapshotId,
        correlation: Value,
    ) -> Result<ProviderDeleteOutcome, ProviderControlError> {
        let provider_options = {
            let registry = self.lock_registry()?;
            let found = registry
                .values()
                .find(|record| {
                    record
                        .instance
                        .snapshot
                        .as_ref()
                        .is_some_and(|snapshot| snapshot.snapshot_id == snapshot_id)
                })
                .map(|record| record.provider_options.clone());
            if found.is_none()
                && std::env::var("E2B_API_KEY")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .is_none()
            {
                return Err(ProviderControlError::NotFound {
                    resource_ref: snapshot_id.0.clone(),
                });
            }
            found.unwrap_or(Value::Null)
        };

        let options = parse_options(&provider_options)?;
        let api_key = resolve_api_key(&options)?;
        let connection = resolve_connection_options(&options)?;
        let http = new_e2b_http_client()?;
        let deleted = delete_snapshot(
            &http,
            connection.api_base.as_str(),
            api_key.as_str(),
            snapshot_id.0.as_str(),
        )
        .await?;

        let (deleted_snapshots, retained_snapshots) = if deleted {
            let mut registry = self.lock_registry()?;
            for record in registry.values_mut() {
                if record
                    .instance
                    .snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.snapshot_id == snapshot_id)
                {
                    record.instance.snapshot = None;
                    record.instance.updated_at_ms = now_ms();
                }
            }
            (vec![snapshot_id], Vec::new())
        } else {
            (Vec::new(), vec![snapshot_id])
        };

        Ok(ProviderDeleteOutcome {
            backend_id,
            provider: self.kind.clone(),
            instance_id: None,
            deleted: false,
            retained_snapshots,
            deleted_snapshots,
            correlation,
        })
    }

    /// Delete an e2b sandbox by its raw platform sandbox id, without
    /// requiring a matching record in this process's in-memory registry.
    ///
    /// This process's registry is purely in-memory (see task history: no
    /// cross-process/persisted tracking), so a sandbox created before a
    /// daemon restart — or by a different process entirely — leaves no
    /// trace here even though it may still be running (and billing) on the
    /// E2B platform. This method lets a caller that independently knows the
    /// `sandbox_id` (e.g. from its own durable store) and holds matching
    /// credentials reclaim it directly, mirroring xiaoO's
    /// `delete_sandbox_by_id`. `provider_options` must resolve to the same
    /// api_key/connection shape accepted by `create()`. Idempotent: both a
    /// successful delete and an already-gone (404) sandbox return `Ok(())`.
    pub async fn reclaim_orphan_sandbox(
        &self,
        provider_options: Value,
        sandbox_id: &str,
    ) -> Result<(), ProviderControlError> {
        let sandbox_id = sandbox_id.trim();
        if sandbox_id.is_empty() {
            return Err(ProviderControlError::InvalidRequest {
                message: "e2b sandbox id cannot be empty".to_string(),
            });
        }

        let options = parse_options(&provider_options)?;
        let api_key = resolve_api_key(&options)?;
        let connection = resolve_connection_options(&options)?;
        let http = new_e2b_http_client()?;

        delete_sandbox_by_id(
            &http,
            connection.api_base.as_str(),
            api_key.as_str(),
            sandbox_id,
        )
        .await
    }
}

impl Default for E2bProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Construct a fresh, empty [`E2bProvider`].
pub fn e2b_provider() -> E2bProvider {
    E2bProvider::new()
}

impl Provider for E2bProvider {
    fn kind(&self) -> &ProviderKind {
        &self.kind
    }

    fn lifecycle(&self) -> &dyn ProviderLifecycle {
        self
    }
}

#[async_trait]
impl ProviderLifecycle for E2bProvider {
    async fn create(
        &self,
        request: ProviderCreateRequest,
    ) -> Result<ProviderInstance, ProviderControlError> {
        let (instance, live, provider_options) = self
            .provision_sandbox(
                request.backend_id,
                request.owner_ref,
                request.resource_limits,
                request.provider_options,
                None,
                ProviderLifecycleOperation::Create,
            )
            .await?;

        let mut registry = self.lock_registry()?;
        registry.insert(
            instance.instance_id.0.clone(),
            E2bInstanceRecord {
                instance: instance.clone(),
                provider_options,
                live: Some(live),
            },
        );
        Ok(instance)
    }

    async fn load(
        &self,
        request: ProviderLoadRequest,
    ) -> Result<ProviderInstance, ProviderControlError> {
        let ProviderLoadRequest {
            backend_id,
            owner_ref,
            source,
            resource_limits,
            provider_options,
            ..
        } = request;

        match source {
            ProviderLoadSource::Instance(id) => {
                let (current_state, snapshot, stored_provider_options) = {
                    let registry = self.lock_registry()?;
                    let record = registry.get(id.0.as_str()).ok_or_else(|| {
                        ProviderControlError::NotFound {
                            resource_ref: id.0.clone(),
                        }
                    })?;
                    (
                        record.instance.state,
                        record.instance.snapshot.clone(),
                        record.provider_options.clone(),
                    )
                };
                let _ = ProviderLifecycleStateMachine::begin(
                    Some(current_state),
                    ProviderLifecycleOperation::Load,
                )?;
                let snapshot = snapshot.ok_or_else(|| ProviderControlError::Conflict {
                    message: format!("instance {} has no snapshot available to reload from", id.0),
                })?;

                let effective_options = if provider_options.is_null() {
                    stored_provider_options
                } else {
                    provider_options
                };

                let (instance, live, provider_options) = self
                    .provision_sandbox(
                        backend_id,
                        owner_ref,
                        resource_limits,
                        effective_options,
                        Some(snapshot.snapshot_id.0.clone()),
                        ProviderLifecycleOperation::Load,
                    )
                    .await?;

                let mut registry = self.lock_registry()?;
                registry.remove(id.0.as_str());
                registry.insert(
                    instance.instance_id.0.clone(),
                    E2bInstanceRecord {
                        instance: instance.clone(),
                        provider_options,
                        live: Some(live),
                    },
                );
                Ok(instance)
            }
            ProviderLoadSource::Snapshot(snapshot_id) => {
                let (instance, live, provider_options) = self
                    .provision_sandbox(
                        backend_id,
                        owner_ref,
                        resource_limits,
                        provider_options,
                        Some(snapshot_id.0.clone()),
                        ProviderLifecycleOperation::Load,
                    )
                    .await?;

                let mut registry = self.lock_registry()?;
                registry.insert(
                    instance.instance_id.0.clone(),
                    E2bInstanceRecord {
                        instance: instance.clone(),
                        provider_options,
                        live: Some(live),
                    },
                );
                Ok(instance)
            }
            ProviderLoadSource::SerializedHandle(_) => {
                Err(ProviderControlError::UnsupportedCapability {
                    provider: self.kind.clone(),
                    capability: "load_from_serialized_handle".to_string(),
                })
            }
        }
    }

    async fn pause(
        &self,
        request: ProviderPauseRequest,
    ) -> Result<ProviderSnapshot, ProviderControlError> {
        let instance_id = request.instance_id.0.clone();
        let (current_state, sandbox_id, api_base, api_key) = {
            let registry = self.lock_registry()?;
            let record = registry.get(instance_id.as_str()).ok_or_else(|| {
                ProviderControlError::NotFound {
                    resource_ref: instance_id.clone(),
                }
            })?;
            let live = record
                .live
                .as_ref()
                .ok_or_else(|| ProviderControlError::Conflict {
                    message: format!("instance {instance_id} has no live sandbox to pause"),
                })?;
            (
                record.instance.state,
                live.state.sandbox_id.clone(),
                live.state.api_base.clone(),
                live.state.api_key.clone(),
            )
        };

        let _ = ProviderLifecycleStateMachine::begin(
            Some(current_state),
            ProviderLifecycleOperation::Pause,
        )?;

        let http = new_e2b_http_client()?;
        let snapshot_result = create_snapshot(
            &http,
            api_base.as_str(),
            api_key.as_str(),
            sandbox_id.as_str(),
        )
        .await?;

        let snapshot = ProviderSnapshot {
            snapshot_id: ProviderSnapshotId(snapshot_result.snapshot_id),
            provider: self.kind.clone(),
            source_instance_id: Some(ProviderInstanceId(instance_id.clone())),
            serialized_handle: None,
            metadata: json!({ "sandbox_id": sandbox_id, "names": snapshot_result.names }),
            created_at_ms: now_ms(),
        };

        let live = {
            let mut registry = self.lock_registry()?;
            let record = registry.get_mut(instance_id.as_str()).ok_or_else(|| {
                ProviderControlError::NotFound {
                    resource_ref: instance_id.clone(),
                }
            })?;
            record
                .live
                .take()
                .ok_or_else(|| ProviderControlError::Conflict {
                    message: format!("instance {instance_id} has no live sandbox to pause"),
                })?
        };

        if let Err(error) = live.state.delete_sandbox().await {
            tracing::error!(
                instance_id = %instance_id,
                sandbox_id = %sandbox_id,
                snapshot_id = %snapshot.snapshot_id.0,
                error = %error,
                "e2b pause: snapshot created but sandbox delete failed; snapshot retained on the instance to avoid an orphaned resource"
            );
            let mut registry = self.lock_registry()?;
            if let Some(record) = registry.get_mut(instance_id.as_str()) {
                record.live = Some(live);
                record.instance.snapshot = Some(snapshot.clone());
                record.instance.updated_at_ms = now_ms();
            }
            return Err(ProviderControlError::ProviderFailure {
                provider: self.kind.clone(),
                message: format!(
                    "e2b sandbox snapshot {} succeeded but delete of sandbox {sandbox_id} failed: {error}; the snapshot was retained on instance {instance_id} (visible via inspect/delete) instead of being discarded",
                    snapshot.snapshot_id.0
                ),
                details: json!({
                    "orphaned_snapshot_id": snapshot.snapshot_id.0,
                    "sandbox_id": sandbox_id,
                    "instance_id": instance_id,
                }),
            });
        }

        let next_state = ProviderLifecycleStateMachine::begin(
            Some(current_state),
            ProviderLifecycleOperation::Pause,
        )?;
        let next_state = ProviderLifecycleStateMachine::complete_success(
            next_state,
            ProviderLifecycleOperation::Pause,
        )?;

        let mut registry = self.lock_registry()?;
        if let Some(record) = registry.get_mut(instance_id.as_str()) {
            record.instance.state = next_state;
            record.instance.snapshot = Some(snapshot.clone());
            record.instance.updated_at_ms = now_ms();
        }
        Ok(snapshot)
    }

    async fn checkpoint(
        &self,
        request: ProviderCheckpointRequest,
    ) -> Result<ProviderSnapshot, ProviderControlError> {
        let instance_id = request.instance_id.0.clone();
        let (sandbox_id, api_base, api_key) = {
            let registry = self.lock_registry()?;
            let record = registry.get(instance_id.as_str()).ok_or_else(|| {
                ProviderControlError::NotFound {
                    resource_ref: instance_id.clone(),
                }
            })?;
            let live = record
                .live
                .as_ref()
                .ok_or_else(|| ProviderControlError::Conflict {
                    message: format!("instance {instance_id} has no live sandbox to checkpoint"),
                })?;
            (
                live.state.sandbox_id.clone(),
                live.state.api_base.clone(),
                live.state.api_key.clone(),
            )
        };
        let http = new_e2b_http_client()?;
        let snapshot_result = create_snapshot(
            &http,
            api_base.as_str(),
            api_key.as_str(),
            sandbox_id.as_str(),
        )
        .await?;
        Ok(ProviderSnapshot {
            snapshot_id: ProviderSnapshotId(snapshot_result.snapshot_id),
            provider: self.kind.clone(),
            source_instance_id: Some(request.instance_id),
            serialized_handle: None,
            metadata: json!({ "sandbox_id": sandbox_id, "names": snapshot_result.names }),
            created_at_ms: now_ms(),
        })
    }

    async fn delete(
        &self,
        request: ProviderDeleteRequest,
    ) -> Result<ProviderDeleteOutcome, ProviderControlError> {
        let Some(instance_id) = request.instance_id.clone() else {
            // No instance_id: the only thing left to satisfy is a
            // snapshot-only delete. Scoped to snapshots visible in the
            // current in-memory registry (see task history) — no
            // cross-process/persisted snapshot tracking.
            return match request.snapshot_id.clone() {
                Some(snapshot_id) => {
                    self.delete_snapshot_only(request.backend_id, snapshot_id, request.correlation)
                        .await
                }
                None => Err(ProviderControlError::InvalidRequest {
                    message: "delete requires instance_id or snapshot_id for the e2b provider"
                        .to_string(),
                }),
            };
        };

        let record = {
            let mut registry = self.lock_registry()?;
            registry.remove(instance_id.0.as_str())
        };

        let Some(mut record) = record else {
            return Ok(ProviderDeleteOutcome {
                backend_id: request.backend_id,
                provider: self.kind.clone(),
                instance_id: Some(instance_id),
                deleted: false,
                retained_snapshots: Vec::new(),
                deleted_snapshots: Vec::new(),
                correlation: request.correlation,
            });
        };

        let next_state = ProviderLifecycleStateMachine::begin(
            Some(record.instance.state),
            ProviderLifecycleOperation::Delete,
        )?;

        if let Some(live) = record.live.take() {
            if let Err(error) = live.state.delete_sandbox().await {
                record.live = Some(live);
                let mut registry = self.lock_registry()?;
                registry.insert(instance_id.0.clone(), record);
                return Err(ProviderControlError::ProviderFailure {
                    provider: self.kind.clone(),
                    message: format!("failed to delete e2b sandbox: {error}"),
                    details: Value::Null,
                });
            }
        }

        let _ = ProviderLifecycleStateMachine::complete_success(
            next_state,
            ProviderLifecycleOperation::Delete,
        )?;

        let retained_snapshots = record
            .instance
            .snapshot
            .map(|snapshot| vec![snapshot.snapshot_id])
            .unwrap_or_default();

        Ok(ProviderDeleteOutcome {
            backend_id: request.backend_id,
            provider: self.kind.clone(),
            instance_id: Some(instance_id),
            deleted: true,
            retained_snapshots,
            deleted_snapshots: Vec::new(),
            correlation: request.correlation,
        })
    }

    /// Unlike every other accessor here, this deliberately re-verifies
    /// against the platform rather than trusting the in-memory registry
    /// alone — it is the single source of truth `check_alive`
    /// (`RuntimeAdapter::check_alive`, used by the core-layer reclaim sweep)
    /// relies on to notice a sandbox the platform already killed via its
    /// idle `timeout` (`backend.rs`'s `touch_activity`/`refresh_timeout`
    /// push that deadline out on real activity, but a session left idle
    /// past it is gone with no signal to this process otherwise). When the
    /// instance has no live sandbox at all (e.g. paused — snapshotted and
    /// deleted by design), there is nothing to re-verify: the registry's
    /// own state already correctly reflects that, so no network call is
    /// made and `Err(NotFound)` is reserved for "the platform confirms this
    /// sandbox id no longer exists", not "there was never one to check".
    async fn inspect(
        &self,
        request: ProviderInspectRequest,
    ) -> Result<ProviderInstanceStatus, ProviderControlError> {
        let Some(instance_id) = request.instance_id.clone() else {
            return Err(ProviderControlError::InvalidRequest {
                message: "inspect requires instance_id for the e2b provider".to_string(),
            });
        };

        let (instance, live) = {
            let registry = self.lock_registry()?;
            let record = registry.get(instance_id.0.as_str()).ok_or_else(|| {
                ProviderControlError::NotFound {
                    resource_ref: instance_id.0.clone(),
                }
            })?;
            let live = record.live.as_ref().map(|live| {
                (
                    live.state.sandbox_id.clone(),
                    live.state.api_base.clone(),
                    live.state.api_key.clone(),
                )
            });
            (record.instance.clone(), live)
        };

        if let Some((sandbox_id, api_base, api_key)) = live {
            let http = new_e2b_http_client()?;
            // Propagates `ProviderControlError::NotFound` on a 404 straight
            // through — that is exactly the "platform reclaimed this
            // sandbox" signal the caller needs; any other error (transport,
            // non-404 failure) also propagates so a network blip is never
            // mistaken for reclaim.
            fetch_sandbox_detail(
                &http,
                api_base.as_str(),
                api_key.as_str(),
                sandbox_id.as_str(),
            )
            .await?;
        }

        Ok(ProviderInstanceStatus {
            backend_id: instance.backend_id,
            provider: self.kind.clone(),
            instance_id: Some(instance.instance_id),
            state: instance.state,
            endpoint: instance.endpoint,
            snapshot: instance.snapshot,
            capabilities: instance.capabilities,
            resources: instance.resources,
            last_error: None,
            metadata: instance.metadata,
            updated_at_ms: instance.updated_at_ms,
        })
    }

    async fn list_instances(&self) -> Result<Vec<ProviderInstance>, ProviderControlError> {
        let options = parse_options(&Value::Null)?;
        let api_key = resolve_api_key(&options)?;
        let connection = resolve_connection_options(&options)?;
        let http = new_e2b_http_client()?;

        // Deliberately the SAME v1 REST surface as create/delete/snapshot
        // (`/sandboxes`, no `/v2` prefix), so a self-hosted or older E2B
        // deployment that only implements the v1 API still answers this
        // call. `state=running` filters server-side; the v1 listing does
        // not paginate (no `X-Next-Token`), so the whole list comes back
        // in one response.
        let response = http
            .get(join_url(connection.api_base.as_str(), "/sandboxes"))
            .header("X-API-Key", api_key.as_str())
            .query(&[("state", "running")])
            .send()
            .await
            .map_err(|error| {
                let failure = E2bFailure::from_reqwest("failed to list e2b sandboxes", &error);
                ProviderControlError::Transport {
                    message: failure.message,
                }
            })?;

        if response.status() != reqwest::StatusCode::OK {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            let message = parse_error_message(text.as_str()).unwrap_or(text);
            let failure = E2bFailure::from_status("e2b list sandboxes", status, message);
            return Err(ProviderControlError::ProviderFailure {
                provider: ProviderKind(E2B_PROVIDER_KIND.to_string()),
                message: failure.message,
                details: Value::Null,
            });
        }

        let entries: Vec<ListedSandboxEntry> =
            response
                .json()
                .await
                .map_err(|error| ProviderControlError::Transport {
                    message: format!("failed to decode e2b list sandboxes response: {error}"),
                })?;

        Ok(entries
            .into_iter()
            .map(|entry| provider_instance_from_listed_sandbox(&self.kind, entry))
            .collect())
    }
}

/// The single-sandbox `GET /sandboxes/{sandboxID}` response (`SandboxDetail`
/// in the platform's OpenAPI spec) — deliberately narrower than the full
/// schema, mirroring `ListedSandboxEntry`'s approach: only what
/// `reattach_from_persisted_instance` actually uses. Unlike the `GET
/// /sandboxes` *list* response (`ListedSandboxEntry`), this one includes
/// `envdAccessToken` — confirmed against the platform's published OpenAPI
/// spec, which lists `envdAccessToken` on `SandboxDetail`/`Sandbox` but not
/// on `ListedSandbox`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SandboxDetailResponse {
    state: String,
    #[serde(rename = "envdAccessToken")]
    envd_access_token: Option<String>,
}

async fn fetch_sandbox_detail(
    http: &reqwest::Client,
    api_base: &str,
    api_key: &str,
    sandbox_id: &str,
) -> Result<SandboxDetailResponse, ProviderControlError> {
    let response = http
        .get(join_url(
            api_base,
            format!("/sandboxes/{}", encode_path_segment(sandbox_id)).as_str(),
        ))
        .header("X-API-Key", api_key)
        .send()
        .await
        .map_err(|error| {
            let failure = E2bFailure::from_reqwest("failed to fetch e2b sandbox detail", &error);
            ProviderControlError::Transport {
                message: failure.message,
            }
        })?;

    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(ProviderControlError::NotFound {
            resource_ref: sandbox_id.to_string(),
        });
    }
    if response.status() != reqwest::StatusCode::OK {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        let message = parse_error_message(text.as_str()).unwrap_or(text);
        let failure = E2bFailure::from_status("e2b sandbox detail", status, message);
        return Err(ProviderControlError::ProviderFailure {
            provider: ProviderKind(E2B_PROVIDER_KIND.to_string()),
            message: failure.message,
            details: Value::Null,
        });
    }

    response
        .json()
        .await
        .map_err(|error| ProviderControlError::Transport {
            message: format!("failed to decode e2b sandbox detail response: {error}"),
        })
}

#[async_trait]
impl OperationAttach for E2bProvider {
    async fn attach(
        &self,
        instance: &ProviderInstance,
    ) -> Result<Arc<dyn OperationBackend>, ProviderControlError> {
        {
            let registry = self.lock_registry()?;
            if let Some(record) = registry.get(instance.instance_id.0.as_str()) {
                let live = record
                    .live
                    .as_ref()
                    .ok_or_else(|| ProviderControlError::Conflict {
                        message: format!("instance {} is not active", instance.instance_id.0),
                    })?;
                return Ok(Arc::clone(&live.backend));
            }
        }

        // Registry miss: not necessarily wrong, most commonly a freshly
        // restarted process whose in-memory registry is empty reattaching
        // to a sandbox that the caller has already confirmed is still alive
        // on the platform (via `list_instances`). Rebuild the backend state
        // from what the ledger persisted rather than failing — see
        // `reattach_from_persisted_instance`'s doc comment for what is and
        // isn't recoverable this way.
        let (state, backend, provider_options_value) =
            self.reattach_from_persisted_instance(instance).await?;

        let mut registry = self.lock_registry()?;
        registry.insert(
            instance.instance_id.0.clone(),
            E2bInstanceRecord {
                instance: instance.clone(),
                provider_options: provider_options_value,
                live: Some(E2bLiveHandle {
                    state,
                    backend: Arc::clone(&backend),
                }),
            },
        );
        Ok(backend)
    }
}

/// One entry from the v1 `GET /sandboxes` listing — the platform's real
/// "what's running" view. Field set is deliberately narrower than the full
/// `ListedSandbox` schema: only what `provider_instance_from_listed_sandbox`
/// below actually uses (`endAt`/`envdVersion`/`alias`/`volumeMounts` are
/// ignored).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListedSandboxEntry {
    #[serde(rename = "sandboxID")]
    sandbox_id: String,
    #[serde(rename = "templateID")]
    template_id: String,
    started_at: Option<String>,
    cpu_count: Option<u32>,
    memory_mb: Option<u64>,
    disk_size_mb: Option<u64>,
    state: String,
    #[serde(default)]
    metadata: Option<BTreeMap<String, String>>,
}

fn provider_instance_from_listed_sandbox(
    kind: &ProviderKind,
    entry: ListedSandboxEntry,
) -> ProviderInstance {
    let backend_id = entry
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("xgovernor_backend_id"))
        .cloned()
        .unwrap_or_else(|| format!("e2b-unmanaged:{}", entry.sandbox_id));

    let state = match entry.state.as_str() {
        "running" => ProviderLifecycleState::Active,
        "paused" => ProviderLifecycleState::Paused,
        _ => ProviderLifecycleState::Unknown,
    };

    let mut metadata = Map::new();
    metadata.insert("template_id".to_string(), Value::String(entry.template_id));
    if let Some(started_at) = entry.started_at {
        metadata.insert("started_at".to_string(), Value::String(started_at));
    }
    if let Some(platform_metadata) = entry.metadata {
        metadata.insert(
            "platform_metadata".to_string(),
            serde_json::to_value(platform_metadata).unwrap_or(Value::Null),
        );
    }

    let now = now_ms();
    ProviderInstance {
        backend_id: BackendId(backend_id),
        provider: kind.clone(),
        instance_id: ProviderInstanceId(entry.sandbox_id),
        state,
        endpoint: None,
        snapshot: None,
        capabilities: ProviderCapabilities::default(),
        resources: ProviderResourceAllocation {
            vcpu_count: entry.cpu_count,
            memory_mb: entry.memory_mb,
            disk_mb: entry.disk_size_mb,
        },
        metadata: Value::Object(metadata),
        created_at_ms: now,
        updated_at_ms: now,
    }
}

fn parse_options(value: &Value) -> Result<E2bProviderOptions, ProviderControlError> {
    let value = if value.is_null() {
        Value::Object(Map::new())
    } else {
        value.clone()
    };
    serde_json::from_value(value).map_err(|error| ProviderControlError::InvalidRequest {
        message: format!("invalid e2b provider_options: {error}"),
    })
}

fn resolve_connection_options(
    options: &E2bProviderOptions,
) -> Result<E2bConnectionOptions, ProviderControlError> {
    let api_url_env = std::env::var("E2B_API_URL").ok();
    let domain_env = std::env::var("E2B_DOMAIN").ok();
    resolve_connection_options_from_values(options, api_url_env.as_deref(), domain_env.as_deref())
}

fn resolve_envd_scheme(options: &E2bProviderOptions) -> Result<String, ProviderControlError> {
    let env_value = std::env::var(E2B_ENVD_SCHEME_ENV).ok();
    resolve_envd_scheme_from_value(options, env_value.as_deref())
}

fn resolve_envd_scheme_from_value(
    options: &E2bProviderOptions,
    env_value: Option<&str>,
) -> Result<String, ProviderControlError> {
    let scheme = non_empty(options.envd_scheme.as_deref())
        .or_else(|| non_empty(env_value))
        .unwrap_or(DEFAULT_ENVD_SCHEME)
        .to_ascii_lowercase();
    match scheme.as_str() {
        "http" | "https" => Ok(scheme),
        _ => Err(ProviderControlError::InvalidRequest {
            message: format!("invalid e2b envd scheme {scheme:?}; expected 'http' or 'https'"),
        }),
    }
}

fn resolve_connection_options_from_values(
    options: &E2bProviderOptions,
    api_url_env: Option<&str>,
    domain_env: Option<&str>,
) -> Result<E2bConnectionOptions, ProviderControlError> {
    let raw_domain = non_empty(options.domain.as_deref())
        .or_else(|| non_empty(domain_env))
        .unwrap_or(DEFAULT_DOMAIN);
    let sandbox_domain = normalize_sandbox_domain(raw_domain)?;
    let api_base = non_empty(options.api_base.as_deref())
        .or_else(|| non_empty(api_url_env))
        .map(str::to_string)
        .unwrap_or_else(|| {
            if sandbox_domain == DEFAULT_DOMAIN {
                DEFAULT_API_BASE.to_string()
            } else {
                format!("https://api.{sandbox_domain}")
            }
        });

    Ok(E2bConnectionOptions {
        api_base,
        sandbox_domain,
    })
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn normalize_sandbox_domain(value: &str) -> Result<String, ProviderControlError> {
    let domain = value.trim().trim_end_matches('.');
    if domain.is_empty()
        || domain.contains("://")
        || domain.contains('/')
        || domain.contains('?')
        || domain.contains('#')
    {
        return Err(ProviderControlError::InvalidRequest {
            message: format!(
                "invalid e2b domain {value:?}; expected a hostname without scheme or path"
            ),
        });
    }
    Ok(domain.to_string())
}

fn resolve_api_key(options: &E2bProviderOptions) -> Result<String, ProviderControlError> {
    if let Some(api_key) = options
        .api_key
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return Ok(api_key.to_string());
    }

    let env_name = options
        .api_key_env
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("E2B_API_KEY");
    std::env::var(env_name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ProviderControlError::InvalidRequest {
            message: format!("e2b provider requires api_key or non-empty env var {env_name}"),
        })
}

fn backend_path(value: &str) -> Result<BackendPath, ProviderControlError> {
    normalize_backend_path(std::path::Path::new(value)).map_err(|error| {
        ProviderControlError::InvalidRequest {
            message: error.to_string(),
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn create_e2b_sandbox(
    http: &reqwest::Client,
    api_base: &str,
    api_key: &str,
    template_id: &str,
    timeout_secs: u64,
    options: &E2bProviderOptions,
    backend_id: &str,
    owner_ref: &str,
) -> Result<CreateSandboxResponse, ProviderControlError> {
    let mut body = Map::new();
    body.insert(
        "templateID".to_string(),
        Value::String(template_id.to_string()),
    );
    body.insert("timeout".to_string(), json!(timeout_secs));
    body.insert("secure".to_string(), json!(options.secure.unwrap_or(true)));
    if let Some(value) = options.allow_internet_access {
        body.insert("allow_internet_access".to_string(), json!(value));
    }
    if let Some(value) = options.auto_pause {
        body.insert("autoPause".to_string(), json!(value));
    }
    if let Some(value) = options.auto_resume {
        body.insert("autoResume".to_string(), json!({ "enabled": value }));
    }
    let metadata = platform_metadata(options, backend_id, owner_ref);
    if !metadata.is_empty() {
        body.insert(
            "metadata".to_string(),
            serde_json::to_value(metadata).unwrap(),
        );
    }
    if let Some(env_vars) = options
        .env_vars
        .as_ref()
        .filter(|values| !values.is_empty())
    {
        body.insert(
            "envVars".to_string(),
            serde_json::to_value(env_vars).unwrap(),
        );
    }
    if let Some(network) = options.network.clone() {
        body.insert("network".to_string(), network);
    }
    if let Some(mcp) = options.mcp.clone() {
        body.insert("mcp".to_string(), mcp);
    }
    if let Some(volume_mounts) = options.volume_mounts.clone() {
        body.insert("volumeMounts".to_string(), volume_mounts);
    }

    let started_at = Instant::now();
    let response = http
        .post(join_url(api_base, "/sandboxes"))
        .header("X-API-Key", api_key)
        .json(&Value::Object(body))
        .send()
        .await
        .map_err(|error| {
            let failure = E2bFailure::from_reqwest("failed to create e2b sandbox", &error);
            failure.log(
                "create_sandbox",
                None,
                None,
                started_at.elapsed().as_millis(),
            );
            ProviderControlError::Transport {
                message: failure.message,
            }
        })?;

    if response.status() != reqwest::StatusCode::CREATED {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        let message = parse_error_message(text.as_str()).unwrap_or(text);
        let failure = E2bFailure::from_status("e2b create sandbox", status, message.clone());
        failure.log(
            "create_sandbox",
            None,
            None,
            started_at.elapsed().as_millis(),
        );
        return Err(ProviderControlError::ProviderFailure {
            provider: ProviderKind(E2B_PROVIDER_KIND.to_string()),
            message: failure.message,
            details: Value::Null,
        });
    }

    response
        .json::<CreateSandboxResponse>()
        .await
        .map_err(|error| ProviderControlError::Transport {
            message: format!("failed to decode e2b create sandbox response: {error}"),
        })
}

async fn create_snapshot(
    http: &reqwest::Client,
    api_base: &str,
    api_key: &str,
    sandbox_id: &str,
) -> Result<CreateSnapshotResponse, ProviderControlError> {
    let response = http
        .post(join_url(
            api_base,
            format!("/sandboxes/{sandbox_id}/snapshots").as_str(),
        ))
        .header("X-API-Key", api_key)
        .json(&Value::Object(Map::new()))
        .send()
        .await
        .map_err(|error| ProviderControlError::Transport {
            message: format!("failed to create e2b snapshot: {error}"),
        })?;

    if response.status() != reqwest::StatusCode::CREATED {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        let message = parse_error_message(text.as_str()).unwrap_or(text);
        return Err(ProviderControlError::ProviderFailure {
            provider: ProviderKind(E2B_PROVIDER_KIND.to_string()),
            message: format!("e2b create snapshot failed with HTTP {status}: {message}"),
            details: Value::Null,
        });
    }

    response
        .json::<CreateSnapshotResponse>()
        .await
        .map_err(|error| ProviderControlError::Transport {
            message: format!("failed to decode e2b create snapshot response: {error}"),
        })
}

/// Delete an e2b snapshot (backed by a platform "template"). Returns `Ok(true)`
/// when the snapshot was deleted, `Ok(false)` when it was already gone
/// (HTTP 404) — mirrors the create-sandbox/delete-sandbox precedent of
/// treating "already absent" as a successful no-op rather than an error.
async fn delete_snapshot(
    http: &reqwest::Client,
    api_base: &str,
    api_key: &str,
    snapshot_id: &str,
) -> Result<bool, ProviderControlError> {
    let response = http
        .delete(join_url(
            api_base,
            format!("/templates/{}", encode_path_segment(snapshot_id)).as_str(),
        ))
        .header("X-API-Key", api_key)
        .send()
        .await
        .map_err(|error| ProviderControlError::Transport {
            message: format!("failed to delete e2b snapshot: {error}"),
        })?;

    match response.status() {
        reqwest::StatusCode::NO_CONTENT => Ok(true),
        reqwest::StatusCode::NOT_FOUND => Ok(false),
        status => {
            let text = response.text().await.unwrap_or_default();
            let message = parse_error_message(text.as_str()).unwrap_or(text);
            Err(ProviderControlError::ProviderFailure {
                provider: ProviderKind(E2B_PROVIDER_KIND.to_string()),
                message: format!("e2b delete snapshot failed with HTTP {status}: {message}"),
                details: Value::Null,
            })
        }
    }
}

/// Delete an e2b sandbox by its raw platform sandbox id, independent of any
/// in-memory registry record. Idempotent: both `204 No Content` and
/// `404 Not Found` are treated as success (the sandbox is gone either way).
/// Ported from xiaoO's `delete_sandbox_by_id`, which existed specifically so
/// a process other than the sandbox's original owner could reclaim it after
/// the owner died without a live in-memory handle to call `shutdown()` on.
async fn delete_sandbox_by_id(
    http: &reqwest::Client,
    api_base: &str,
    api_key: &str,
    sandbox_id: &str,
) -> Result<(), ProviderControlError> {
    let response = http
        .delete(join_url(
            api_base,
            format!("/sandboxes/{}", encode_path_segment(sandbox_id)).as_str(),
        ))
        .header("X-API-Key", api_key)
        .send()
        .await
        .map_err(|error| ProviderControlError::Transport {
            message: format!("failed to delete e2b sandbox: {error}"),
        })?;

    match response.status() {
        reqwest::StatusCode::NO_CONTENT | reqwest::StatusCode::NOT_FOUND => Ok(()),
        status => {
            let text = response.text().await.unwrap_or_default();
            let message = parse_error_message(text.as_str()).unwrap_or(text);
            Err(ProviderControlError::ProviderFailure {
                provider: ProviderKind(E2B_PROVIDER_KIND.to_string()),
                message: format!("e2b delete sandbox failed with HTTP {status}: {message}"),
                details: Value::Null,
            })
        }
    }
}

/// Percent-encode a single path segment (snapshot/template id) for use in a
/// URL path. Ported from xiaoO's `encode_path_segment`.
fn encode_path_segment(value: &str) -> String {
    value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
                (byte as char).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}

fn platform_metadata(
    options: &E2bProviderOptions,
    backend_id: &str,
    owner_ref: &str,
) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    if let Some(values) = &options.metadata {
        metadata.extend(values.clone());
    }
    metadata.insert("xgovernor_backend_id".to_string(), backend_id.to_string());
    metadata.insert("xgovernor_owner_ref".to_string(), owner_ref.to_string());
    metadata
}

fn metadata_for_instance(
    provider_options: &Value,
    sandbox: &CreateSandboxResponse,
    options: &E2bProviderOptions,
    owner_ref: &str,
) -> Value {
    let mut object = Map::new();
    object.insert("provider".to_string(), Value::String("e2b".to_string()));
    object.insert(
        "sandbox_id".to_string(),
        Value::String(sandbox.sandbox_id.clone()),
    );
    object.insert(
        "template_id".to_string(),
        Value::String(sandbox.template_id.clone()),
    );
    object.insert(
        "owner_ref".to_string(),
        Value::String(owner_ref.to_string()),
    );
    if sandbox.traffic_access_token.is_some() {
        object.insert(
            "traffic_access_token_present".to_string(),
            Value::Bool(true),
        );
    }
    object.insert(
        "provider_options".to_string(),
        redacted_provider_options(provider_options, options),
    );
    Value::Object(object)
}

fn redacted_provider_options(provider_options: &Value, options: &E2bProviderOptions) -> Value {
    let mut object = provider_options.as_object().cloned().unwrap_or_default();
    object.remove("api_key");
    object.remove("env_vars");
    object.remove("envVars");
    if options.api_key.is_some() {
        object.insert("api_key_configured".to_string(), Value::Bool(true));
    }
    Value::Object(object)
}

fn provider_handle(
    sandbox: &CreateSandboxResponse,
    envd_port: u16,
    envd_scheme: &str,
    sandbox_domain: &str,
) -> ProviderEndpoint {
    ProviderEndpoint::Handle {
        value: json!({
            "provider": "e2b",
            "sandbox_id": sandbox.sandbox_id.clone(),
            "envd_host": envd_host(envd_port, sandbox.sandbox_id.as_str(), sandbox_domain),
            "envd_port": envd_port,
            "envd_scheme": envd_scheme,
        }),
    }
}

async fn ensure_remote_roots(state: &Arc<E2bBackendState>) -> Result<(), ProviderControlError> {
    let exec = E2bExec::new(Arc::clone(state));
    let script = format!(
        "mkdir -p {} {}",
        shell_quote(state.workspace_root.0.as_str()),
        shell_quote(state.temp_root.0.as_str()),
    );
    let output = retry_workspace_initialization(
        state.sandbox_id.as_str(),
        || exec.run_shell_script_detailed(script.as_str(), None),
        tokio::time::sleep,
    )
    .await
    .map_err(|failure| ProviderControlError::Transport {
        message: failure.message().to_string(),
    })?;
    if output.exit_code == Some(0) {
        return Ok(());
    }
    Err(ProviderControlError::ProviderFailure {
        provider: ProviderKind(E2B_PROVIDER_KIND.to_string()),
        message: String::from_utf8_lossy(output.stderr.as_slice()).to_string(),
        details: Value::Null,
    })
}

async fn retry_workspace_initialization<Attempt, AttemptFuture, Sleep, SleepFuture>(
    sandbox_id: &str,
    mut operation: Attempt,
    mut sleep: Sleep,
) -> Result<E2bExecOutput, E2bExecFailure>
where
    Attempt: FnMut() -> AttemptFuture,
    AttemptFuture: Future<Output = Result<E2bExecOutput, E2bExecFailure>>,
    Sleep: FnMut(Duration) -> SleepFuture,
    SleepFuture: Future<Output = ()>,
{
    for attempt in 1..=WORKSPACE_INIT_MAX_ATTEMPTS {
        match operation().await {
            Ok(output) => return Ok(output),
            Err(error) if error.retryable() && attempt < WORKSPACE_INIT_MAX_ATTEMPTS => {
                let delay = workspace_init_backoff(attempt);
                tracing::warn!(
                    operation = "initialize_workspace",
                    sandbox_id,
                    attempt,
                    max_attempts = WORKSPACE_INIT_MAX_ATTEMPTS,
                    retry_delay_ms = delay.as_millis(),
                    error = %error.message(),
                    "E2B workspace initialization failed; retrying"
                );
                sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("workspace initialization retry loop always returns")
}

fn workspace_init_backoff(attempt: usize) -> Duration {
    let exponent = u32::try_from(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
    let base = WORKSPACE_INIT_BASE_DELAY_MS
        .saturating_mul(2u64.saturating_pow(exponent))
        .min(WORKSPACE_INIT_MAX_DELAY_MS);
    let spread = base / 5;
    let jitter = if spread == 0 {
        0
    } else {
        rand::random::<u64>() % (spread.saturating_mul(2).saturating_add(1))
    };
    Duration::from_millis(base.saturating_sub(spread).saturating_add(jitter))
}

fn new_e2b_http_client() -> Result<reqwest::Client, ProviderControlError> {
    reqwest::Client::builder()
        .connect_timeout(E2B_CONNECT_TIMEOUT)
        .build()
        .map_err(|error| ProviderControlError::Transport {
            message: format!("failed to build e2b HTTP client: {error}"),
        })
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use operation_protocol::capability::exec::ExecRequest;
    use operation_protocol::capability::filesystem::{WriteBytesRequest, WriteMode};
    use provider_protocol::{BackendId, ProviderLifecycleReason};
    use std::cell::Cell;
    use std::future::ready;

    /// End-to-end smoke test against the real E2B platform: creates a
    /// sandbox via the provider-protocol control plane, attaches the
    /// operation plane, runs a structured grep, then tears the sandbox down.
    /// Requires network access and a real `E2B_API_KEY`, so it's ignored by
    /// default.
    #[tokio::test]
    #[ignore = "requires E2B_API_KEY and creates a real E2B sandbox"]
    async fn live_create_attach_exec_and_delete() {
        assert!(
            std::env::var_os("E2B_API_KEY").is_some(),
            "E2B_API_KEY must be set"
        );

        let provider = E2bProvider::new();
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let backend_id = BackendId(format!("e2b-live-grep:{suffix}"));

        let instance = provider
            .lifecycle()
            .create(ProviderCreateRequest {
                backend_id: backend_id.clone(),
                owner_ref: format!("e2b-live-grep-owner:{suffix}"),
                reason: ProviderLifecycleReason::Acquire,
                resource_limits: Default::default(),
                provider_options: json!({
                    "api_key_env": "E2B_API_KEY",
                    "template_id": "base",
                    "timeout_secs": 300,
                    "default_shell": "/bin/sh"
                }),
                correlation: Value::Null,
            })
            .await
            .expect("create live E2B backend");

        let backend = provider
            .attach(&instance)
            .await
            .expect("attach to live E2B backend");
        let smoke_path = BackendPath(format!("{DEFAULT_WORKSPACE_ROOT}/grep-smoke.py"));

        let smoke_result: Result<String, String> = async {
            backend
                .files()
                .write_bytes(WriteBytesRequest {
                    path: smoke_path,
                    content: b"watt = watts = W = Quantity(\"watt\")\n".to_vec(),
                    mode: WriteMode::Overwrite,
                })
                .await
                .map_err(|error| format!("write smoke fixture: {error}"))?;

            let output = backend
                .exec()
                .exec(ExecRequest {
                    command: "grep".to_string(),
                    args: vec![
                        "-P".to_string(),
                        "-n".to_string(),
                        "-e".to_string(),
                        r"W\s*=".to_string(),
                        "grep-smoke.py".to_string(),
                    ],
                    shell: None,
                    cwd: Some(BackendPath(DEFAULT_WORKSPACE_ROOT.to_string())),
                    timeout_ms: Some(30_000),
                    env: None,
                    extra: None,
                })
                .await
                .map_err(|error| format!("execute structured grep: {error}"))?;
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr);
            if output.exit_code != Some(0) {
                return Err(format!(
                    "grep exited with {:?}; stderr: {stderr}",
                    output.exit_code
                ));
            }
            if !stdout.contains("1:watt = watts = W = Quantity") {
                return Err(format!("unexpected grep stdout: {stdout:?}"));
            }
            Ok(stdout)
        }
        .await;

        let cleanup_result = provider
            .lifecycle()
            .delete(ProviderDeleteRequest {
                backend_id,
                instance_id: Some(instance.instance_id.clone()),
                snapshot_id: None,
                reason: ProviderLifecycleReason::UserRequested,
                correlation: Value::Null,
            })
            .await;
        cleanup_result.expect("delete live E2B sandbox");
        let stdout = smoke_result.expect("structured grep smoke");
        eprintln!("live E2B structured grep passed: {}", stdout.trim());
    }

    /// End-to-end smoke test for the git-sandbox path
    /// `apps/runtime-mock::GitSandboxWorkspaceEnvironment` /
    /// `apps/runtime-mock::MockRuntime` exercise against a real e2b sandbox:
    /// creates a sandbox with `allow_internet_access` on, `git clone`s a real
    /// public repo into `DEFAULT_WORKSPACE_ROOT` via the operation-plane
    /// `exec`, verifies the clone landed with `git rev-parse HEAD`, then tears
    /// the sandbox down. This used to live in `apps/runtime-e2b`'s own test
    /// module (exercised through `RuntimeAdapter::start`/`stop`) before that
    /// crate was folded into `apps/runtime-mock`; it is rewritten here in raw
    /// `provider_protocol` calls, matching this module's own
    /// `live_create_attach_exec_and_delete` above, so this crate's live e2b
    /// coverage does not depend on `crates/manager` or `xgovernor-core`
    /// wiring at all — only on the `Provider`/`OperationAttach` contract this
    /// file itself implements. Requires `E2B_API_KEY` and outbound network
    /// access, so it stays `#[ignore]`d like its neighbor.
    #[tokio::test]
    #[ignore = "requires E2B_API_KEY and creates a real E2B sandbox with network access"]
    async fn live_git_clone_into_a_real_e2b_sandbox() {
        assert!(
            std::env::var_os("E2B_API_KEY").is_some(),
            "E2B_API_KEY must be set"
        );

        let provider = E2bProvider::new();
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let backend_id = BackendId(format!("e2b-live-git-clone:{suffix}"));

        let instance = provider
            .lifecycle()
            .create(ProviderCreateRequest {
                backend_id: backend_id.clone(),
                owner_ref: format!("e2b-live-git-clone-owner:{suffix}"),
                reason: ProviderLifecycleReason::Acquire,
                resource_limits: Default::default(),
                provider_options: json!({
                    "api_key_env": "E2B_API_KEY",
                    "template_id": "base",
                    "timeout_secs": 300,
                    "default_shell": "/bin/sh",
                    "allow_internet_access": true
                }),
                correlation: Value::Null,
            })
            .await
            .expect("create live E2B backend");

        let backend = provider
            .attach(&instance)
            .await
            .expect("attach to live E2B backend");

        let clone_result: Result<String, String> = async {
            let clone_output = backend
                .exec()
                .exec(ExecRequest {
                    command: "git".to_string(),
                    args: vec![
                        "clone".to_string(),
                        "https://github.com/octocat/Hello-World.git".to_string(),
                        DEFAULT_WORKSPACE_ROOT.to_string(),
                    ],
                    shell: None,
                    cwd: None,
                    timeout_ms: Some(120_000),
                    env: None,
                    extra: None,
                })
                .await
                .map_err(|error| format!("git clone exec failed: {error}"))?;
            if clone_output.exit_code != Some(0) {
                return Err(format!(
                    "git clone exited with {:?}; stderr: {}",
                    clone_output.exit_code,
                    String::from_utf8_lossy(&clone_output.stderr)
                ));
            }

            let rev_parse_output = backend
                .exec()
                .exec(ExecRequest {
                    command: "git".to_string(),
                    args: vec![
                        "-C".to_string(),
                        DEFAULT_WORKSPACE_ROOT.to_string(),
                        "rev-parse".to_string(),
                        "HEAD".to_string(),
                    ],
                    shell: None,
                    cwd: None,
                    timeout_ms: Some(10_000),
                    env: None,
                    extra: None,
                })
                .await
                .map_err(|error| format!("git rev-parse exec failed: {error}"))?;
            if rev_parse_output.exit_code != Some(0) {
                return Err(format!(
                    "git rev-parse exited with {:?}; stderr: {}",
                    rev_parse_output.exit_code,
                    String::from_utf8_lossy(&rev_parse_output.stderr)
                ));
            }
            Ok(String::from_utf8_lossy(&rev_parse_output.stdout).into_owned())
        }
        .await;

        let cleanup_result = provider
            .lifecycle()
            .delete(ProviderDeleteRequest {
                backend_id,
                instance_id: Some(instance.instance_id.clone()),
                snapshot_id: None,
                reason: ProviderLifecycleReason::UserRequested,
                correlation: Value::Null,
            })
            .await;
        cleanup_result.expect("delete live E2B sandbox");
        let head_sha = clone_result.expect("git clone + rev-parse smoke");
        eprintln!("live E2B git clone passed: HEAD={}", head_sha.trim());
    }

    #[tokio::test]
    async fn workspace_initialization_retries_transient_failures() {
        let attempts = Cell::new(0usize);
        let sleeps = Cell::new(0usize);

        let output = retry_workspace_initialization(
            "sandbox-test",
            || {
                let attempt = attempts.get() + 1;
                attempts.set(attempt);
                ready(if attempt < 3 {
                    Err(E2bExecFailure::retryable_for_test("temporary reset"))
                } else {
                    Ok(E2bExecOutput {
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                        exit_code: Some(0),
                        timed_out: false,
                    })
                })
            },
            |_| {
                sleeps.set(sleeps.get() + 1);
                ready(())
            },
        )
        .await
        .expect("third attempt should succeed");

        assert_eq!(output.exit_code, Some(0));
        assert_eq!(attempts.get(), 3);
        assert_eq!(sleeps.get(), 2);
    }

    #[test]
    fn workspace_backoff_is_bounded_and_jittered() {
        for attempt in 1..=WORKSPACE_INIT_MAX_ATTEMPTS {
            let delay_ms = workspace_init_backoff(attempt).as_millis() as u64;
            let exponent = u32::try_from(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
            let base = WORKSPACE_INIT_BASE_DELAY_MS
                .saturating_mul(2u64.saturating_pow(exponent))
                .min(WORKSPACE_INIT_MAX_DELAY_MS);
            assert!(delay_ms >= base - base / 5);
            assert!(delay_ms <= base + base / 5);
        }
    }

    #[test]
    fn redacts_direct_api_key_from_metadata() {
        let options = parse_options(&json!({
            "api_key": "secret",
            "template_id": "base",
            "envVars": {"TOKEN": "secret"}
        }))
        .expect("options");

        let redacted = redacted_provider_options(
            &json!({
                "api_key": "secret",
                "template_id": "base",
                "envVars": {"TOKEN": "secret"}
            }),
            &options,
        );

        let object = redacted.as_object().expect("object");
        assert!(!object.contains_key("api_key"));
        assert!(!object.contains_key("envVars"));
        assert_eq!(object.get("api_key_configured"), Some(&Value::Bool(true)));
    }

    #[test]
    fn default_template_is_base() {
        let options = parse_options(&json!({})).expect("options");
        assert_eq!(
            options
                .template_id
                .as_deref()
                .unwrap_or(DEFAULT_TEMPLATE_ID),
            "base"
        );
    }

    #[test]
    fn connection_options_default_to_e2b_cloud() {
        let options = parse_options(&json!({})).expect("options");
        let connection = resolve_connection_options_from_values(&options, None, None)
            .expect("connection options");

        assert_eq!(connection.api_base, DEFAULT_API_BASE);
        assert_eq!(connection.sandbox_domain, DEFAULT_DOMAIN);
    }

    #[test]
    fn connection_options_derive_api_url_from_self_hosted_domain() {
        let options = parse_options(&json!({})).expect("options");
        let connection = resolve_connection_options_from_values(
            &options,
            None,
            Some(" self-hosted.example.com. "),
        )
        .expect("connection options");

        assert_eq!(connection.api_base, "https://api.self-hosted.example.com");
        assert_eq!(connection.sandbox_domain, "self-hosted.example.com");
    }

    #[test]
    fn connection_options_accept_api_url_and_domain_environment_values() {
        let options = parse_options(&json!({})).expect("options");
        let connection = resolve_connection_options_from_values(
            &options,
            Some(" https://control.self-hosted.example.com/ "),
            Some("self-hosted.example.com"),
        )
        .expect("connection options");

        assert_eq!(
            connection.api_base,
            "https://control.self-hosted.example.com/"
        );
        assert_eq!(connection.sandbox_domain, "self-hosted.example.com");
    }

    #[test]
    fn explicit_connection_options_override_environment_values() {
        let options = parse_options(&json!({
            "apiUrl": "https://control.internal.example.com/",
            "sandboxDomain": "sandboxes.internal.example.com"
        }))
        .expect("options");
        let connection = resolve_connection_options_from_values(
            &options,
            Some("https://api.from-env.example.com"),
            Some("from-env.example.com"),
        )
        .expect("connection options");

        assert_eq!(connection.api_base, "https://control.internal.example.com/");
        assert_eq!(connection.sandbox_domain, "sandboxes.internal.example.com");
    }

    #[test]
    fn envd_scheme_defaults_to_https_and_accepts_environment_http() {
        let options = parse_options(&json!({})).expect("options");
        assert_eq!(
            resolve_envd_scheme_from_value(&options, None).expect("default scheme"),
            "https"
        );
        assert_eq!(
            resolve_envd_scheme_from_value(&options, Some(" HTTP ")).expect("environment scheme"),
            "http"
        );
    }

    #[test]
    fn explicit_envd_scheme_overrides_environment_and_invalid_values_are_rejected() {
        let options = parse_options(&json!({ "envd_scheme": "https" })).expect("options");
        assert_eq!(
            resolve_envd_scheme_from_value(&options, Some("http")).expect("explicit scheme"),
            "https"
        );

        let options = parse_options(&json!({})).expect("options");
        let error = resolve_envd_scheme_from_value(&options, Some("ftp"))
            .expect_err("unsupported scheme must be rejected");
        let ProviderControlError::InvalidRequest { message } = error else {
            panic!("expected InvalidRequest error, got {error:?}");
        };
        assert!(message.contains("expected 'http' or 'https'"));
    }

    #[test]
    fn rejects_domain_with_scheme() {
        let options = parse_options(&json!({
            "domain": "https://self-hosted.example.com"
        }))
        .expect("options");
        let error = resolve_connection_options_from_values(&options, None, None)
            .expect_err("domain with a scheme must be rejected");

        let ProviderControlError::InvalidRequest { message } = error else {
            panic!("expected InvalidRequest error, got {error:?}");
        };
        assert!(message.contains("hostname without scheme or path"));
    }

    #[test]
    fn encodes_template_id_as_single_path_segment() {
        assert_eq!(
            encode_path_segment("team/fork-test:default"),
            "team%2Ffork-test%3Adefault"
        );
    }

    #[tokio::test]
    async fn delete_by_snapshot_id_reports_not_found_when_no_registry_record_matches() {
        let provider = E2bProvider::new();

        let error = provider
            .lifecycle()
            .delete(ProviderDeleteRequest {
                backend_id: BackendId("e2b".to_string()),
                instance_id: None,
                snapshot_id: Some(ProviderSnapshotId("does-not-exist".to_string())),
                reason: ProviderLifecycleReason::UserRequested,
                correlation: Value::Null,
            })
            .await
            .unwrap_err();

        assert!(matches!(error, ProviderControlError::NotFound { .. }));
    }

    #[tokio::test]
    async fn delete_without_instance_id_or_snapshot_id_is_invalid_request() {
        let provider = E2bProvider::new();

        let error = provider
            .lifecycle()
            .delete(ProviderDeleteRequest {
                backend_id: BackendId("e2b".to_string()),
                instance_id: None,
                snapshot_id: None,
                reason: ProviderLifecycleReason::UserRequested,
                correlation: Value::Null,
            })
            .await
            .unwrap_err();

        assert!(matches!(error, ProviderControlError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn reclaim_orphan_sandbox_rejects_empty_sandbox_id() {
        let provider = E2bProvider::new();

        let error = provider
            .reclaim_orphan_sandbox(json!({"api_key": "test-key"}), "   ")
            .await
            .unwrap_err();

        assert!(matches!(error, ProviderControlError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn reclaim_orphan_sandbox_requires_resolvable_credentials() {
        // No api_key in provider_options, and api_key_env points at a
        // variable name that should never be set in any test environment
        // (rather than relying on the ambient absence of E2B_API_KEY, which
        // a developer running live tests locally might have exported). This
        // exercises the same credential-resolution path `create()` uses,
        // without needing network access, proving reclaim doesn't depend on
        // any in-memory registry record for its provider_options.
        let provider = E2bProvider::new();

        let error = provider
            .reclaim_orphan_sandbox(
                json!({"api_key_env": "XGOVERNOR_TEST_UNSET_E2B_API_KEY_MARKER"}),
                "sandbox-orphan-1",
            )
            .await
            .unwrap_err();

        assert!(matches!(error, ProviderControlError::InvalidRequest { .. }));
    }

    #[test]
    fn provider_handle_uses_configured_sandbox_domain() {
        let sandbox = CreateSandboxResponse {
            sandbox_id: "sandbox-test".to_string(),
            template_id: "base".to_string(),
            envd_access_token: Some("access-token".to_string()),
            traffic_access_token: None,
        };
        let endpoint = provider_handle(&sandbox, 49_983, "https", "self-hosted.example.com");
        let ProviderEndpoint::Handle { value } = endpoint else {
            panic!("expected provider handle");
        };

        assert_eq!(
            value["envd_host"],
            "49983-sandbox-test.self-hosted.example.com"
        );
    }

    fn listed_entry(
        sandbox_id: &str,
        state: &str,
        metadata: Option<BTreeMap<String, String>>,
    ) -> ListedSandboxEntry {
        ListedSandboxEntry {
            sandbox_id: sandbox_id.to_string(),
            template_id: "base".to_string(),
            started_at: Some("2026-08-14T09:17:29.660909037Z".to_string()),
            cpu_count: Some(2),
            memory_mb: Some(512),
            disk_size_mb: Some(23_301),
            state: state.to_string(),
            metadata,
        }
    }

    fn e2b_kind() -> ProviderKind {
        ProviderKind(E2B_PROVIDER_KIND.to_string())
    }

    #[test]
    fn listed_sandbox_recovers_backend_id_from_platform_metadata() {
        let mut platform_metadata = BTreeMap::new();
        platform_metadata.insert(
            "xgovernor_backend_id".to_string(),
            "e2b-live-abc".to_string(),
        );
        platform_metadata.insert("xgovernor_owner_ref".to_string(), "owner-abc".to_string());

        let instance = provider_instance_from_listed_sandbox(
            &e2b_kind(),
            listed_entry("sandbox-1", "running", Some(platform_metadata)),
        );

        assert_eq!(instance.instance_id.0, "sandbox-1");
        assert_eq!(instance.backend_id.0, "e2b-live-abc");
        assert_eq!(instance.state, ProviderLifecycleState::Active);
        assert_eq!(instance.resources.vcpu_count, Some(2));
        assert_eq!(instance.resources.memory_mb, Some(512));
        assert_eq!(instance.resources.disk_mb, Some(23_301));

        let Value::Object(metadata) = instance.metadata else {
            panic!("listed-sandbox metadata must be an object");
        };
        assert_eq!(
            metadata.get("template_id"),
            Some(&Value::String("base".to_string()))
        );
        assert_eq!(
            metadata.get("started_at"),
            Some(&Value::String("2026-08-14T09:17:29.660909037Z".to_string()))
        );
        assert_eq!(
            metadata["platform_metadata"]["xgovernor_backend_id"],
            Value::String("e2b-live-abc".to_string())
        );
    }

    #[test]
    fn listed_sandbox_without_backend_metadata_gets_synthetic_unmanaged_id() {
        let instance = provider_instance_from_listed_sandbox(
            &e2b_kind(),
            listed_entry("sandbox-2", "running", None),
        );

        assert_eq!(instance.instance_id.0, "sandbox-2");
        assert_eq!(instance.backend_id.0, "e2b-unmanaged:sandbox-2");
    }

    #[test]
    fn listed_sandbox_maps_paused_and_unknown_states() {
        let paused = provider_instance_from_listed_sandbox(
            &e2b_kind(),
            listed_entry("sandbox-3", "paused", None),
        );
        assert_eq!(paused.state, ProviderLifecycleState::Paused);

        let unknown = provider_instance_from_listed_sandbox(
            &e2b_kind(),
            listed_entry("sandbox-4", "terminating", None),
        );
        assert_eq!(unknown.state, ProviderLifecycleState::Unknown);
    }

    #[tokio::test]
    #[ignore = "requires E2B_API_KEY and creates real E2B sandboxes"]
    async fn live_list_instances_and_reconcile_against_remote_platform() {
        assert!(
            std::env::var_os("E2B_API_KEY").is_some(),
            "E2B_API_KEY must be set"
        );

        let provider = Arc::new(E2bProvider::new());
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let backend_id = BackendId(format!("e2b-live-list:{suffix}"));
        let options = parse_options(&Value::Null).expect("default options");
        let api_key = resolve_api_key(&options).expect("resolve E2B_API_KEY");
        let connection = resolve_connection_options(&options).expect("connection options");
        let http = new_e2b_http_client().expect("http client");

        // The whole verification runs inside one inner block whose failure
        // returns an Err instead of panicking; cleanup below then always
        // runs, so a failed assertion can never leak the two real sandboxes
        // this test creates.
        let mut managed_id: Option<String> = None;
        let mut unmanaged_id: Option<String> = None;
        let verification: Result<(), String> = async {
            // 1. Managed sandbox via the control plane: platform metadata
            //    gets `xgovernor_backend_id`, which the listing must recover.
            let managed = provider
                .lifecycle()
                .create(ProviderCreateRequest {
                    backend_id: backend_id.clone(),
                    owner_ref: format!("e2b-live-list-owner:{suffix}"),
                    reason: ProviderLifecycleReason::Acquire,
                    resource_limits: Default::default(),
                    provider_options: json!({
                        "api_key_env": "E2B_API_KEY",
                        "template_id": "base",
                        "timeout_secs": 300,
                    }),
                    correlation: Value::Null,
                })
                .await
                .map_err(|error| format!("create managed live E2B sandbox: {error}"))?;
            managed_id = Some(managed.instance_id.0.clone());

            // 2. Unmanaged sandbox via a raw platform call: no xgovernor
            //    metadata, so the listing must classify it as unmanaged.
            let response = http
                .post(join_url(connection.api_base.as_str(), "/sandboxes"))
                .header("X-API-Key", api_key.as_str())
                .json(&json!({ "templateID": "base", "timeout": 120 }))
                .send()
                .await
                .map_err(|error| format!("create unmanaged live E2B sandbox: {error}"))?;
            if response.status() != reqwest::StatusCode::CREATED {
                return Err(format!(
                    "unmanaged create failed with {}: {}",
                    response.status(),
                    response.text().await.unwrap_or_default()
                ));
            }
            let unmanaged: CreateSandboxResponse = response
                .json()
                .await
                .map_err(|error| format!("decode unmanaged create response: {error}"))?;
            unmanaged_id = Some(unmanaged.sandbox_id.clone());

            // 3. Remote listing sees both sandboxes, with correct mapping.
            //    The platform list is eventually consistent right after
            //    create, so poll briefly.
            let managed_id = managed_id.as_deref().expect("managed id set");
            let unmanaged_id = unmanaged_id.as_deref().expect("unmanaged id set");
            let deadline = Instant::now() + Duration::from_secs(30);
            let listed = loop {
                let listed = provider
                    .lifecycle()
                    .list_instances()
                    .await
                    .map_err(|error| format!("list_instances against the real platform: {error}"))?;
                let has_managed = listed.iter().any(|i| i.instance_id.0 == managed_id);
                let has_unmanaged = listed.iter().any(|i| i.instance_id.0 == unmanaged_id);
                if has_managed && has_unmanaged {
                    break listed;
                }
                if Instant::now() >= deadline {
                    return Err(format!(
                        "sandboxes did not appear in list_instances within 30s; got {listed:?}"
                    ));
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            };

            let listed_managed = listed
                .iter()
                .find(|i| i.instance_id.0 == managed_id)
                .expect("managed sandbox present in listing");
            if listed_managed.backend_id != backend_id {
                return Err(format!(
                    "backend_id must be recovered from platform metadata; expected {backend_id:?}, got {:?}",
                    listed_managed.backend_id
                ));
            }
            if listed_managed.state != ProviderLifecycleState::Active {
                return Err(format!("listed managed state: {:?}", listed_managed.state));
            }
            let Value::Object(metadata) = &listed_managed.metadata else {
                return Err(format!(
                    "listed managed sandbox metadata must be an object: {listed_managed:?}"
                ));
            };
            // The platform resolves the template alias ("base") to its
            // internal template id, so only presence is asserted here.
            let Some(Value::String(template_id)) = metadata.get("template_id") else {
                return Err(format!("listed managed template_id missing: {metadata:?}"));
            };
            if template_id.is_empty() {
                return Err("listed managed template_id is empty".to_string());
            }
            if !metadata.contains_key("started_at") {
                return Err(format!("listed managed started_at missing: {metadata:?}"));
            }
            if metadata["platform_metadata"]["xgovernor_backend_id"]
                != Value::String(backend_id.0.clone())
            {
                return Err(format!(
                    "platform_metadata xgovernor_backend_id mismatch: {metadata:?}"
                ));
            }

            let listed_unmanaged = listed
                .iter()
                .find(|i| i.instance_id.0 == unmanaged_id)
                .expect("unmanaged sandbox present in listing");
            if listed_unmanaged.backend_id.0 != format!("e2b-unmanaged:{unmanaged_id}") {
                return Err(format!(
                    "unmanaged sandbox backend_id mismatch: expected e2b-unmanaged:{unmanaged_id}, got {}",
                    listed_unmanaged.backend_id.0
                ));
            }

            // 4. Cross-restart re-attach: a fresh provider with an empty
            //    in-memory registry must still be able to attach to the
            //    still-live remote sandbox, by fetching a fresh envd access
            //    token via `GET /sandboxes/{sandboxID}` and reconstructing
            //    `provider_options` from what `create()` persisted onto
            //    `instance.metadata`. Real E2E testing against real pi +
            //    DeepSeek + E2B (2026-08-17) found this path was previously
            //    entirely unimplemented — `attach()` was registry-only, so
            //    every restart made `submit_turn` wrongly report
            //    `pi_sandbox_gone` for sandboxes that were, in fact, still
            //    alive. This assertion is the regression guard for that.
            provider
                .attach(&managed)
                .await
                .map_err(|error| format!("same-process attach must succeed: {error}"))?;

            let restarted = E2bProvider::new();
            let reattached = restarted
                .attach(&managed)
                .await
                .map_err(|error| format!("cross-restart attach must succeed: {error}"))?;

            // Not just "attach returned Ok" — the reattached backend must
            // actually be usable: exec a real command through it.
            let output = reattached
                .exec()
                .exec(ExecRequest {
                    command: "echo".to_string(),
                    args: vec!["reattached".to_string()],
                    shell: None,
                    cwd: None,
                    timeout_ms: Some(15_000),
                    env: None,
            extra: None,
                })
                .await
                .map_err(|error| format!("exec through reattached backend: {error}"))?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            if !stdout.contains("reattached") {
                return Err(format!(
                    "reattached backend exec produced unexpected stdout: {stdout:?}"
                ));
            }

            Ok(())
        }
        .await;

        // Cleanup runs unconditionally, so even a failed verification cannot
        // leak sandboxes. Failures are collected and reported, not silently
        // swallowed.
        let mut cleanup_errors: Vec<String> = Vec::new();
        if let Some(managed_id) = managed_id.as_deref() {
            if let Err(error) = provider
                .lifecycle()
                .delete(ProviderDeleteRequest {
                    backend_id: backend_id.clone(),
                    instance_id: Some(ProviderInstanceId(managed_id.to_string())),
                    snapshot_id: None,
                    reason: ProviderLifecycleReason::UserRequested,
                    correlation: Value::Null,
                })
                .await
            {
                cleanup_errors.push(format!("delete managed sandbox {managed_id}: {error}"));
            }
        }
        if let Some(unmanaged_id) = unmanaged_id.as_deref() {
            match http
                .delete(join_url(
                    connection.api_base.as_str(),
                    format!("/sandboxes/{unmanaged_id}").as_str(),
                ))
                .header("X-API-Key", api_key.as_str())
                .send()
                .await
            {
                Ok(response)
                    if response.status() == reqwest::StatusCode::NO_CONTENT
                        || response.status() == reqwest::StatusCode::NOT_FOUND => {}
                Ok(response) => cleanup_errors.push(format!(
                    "delete unmanaged sandbox {unmanaged_id}: HTTP {}",
                    response.status()
                )),
                Err(error) => {
                    cleanup_errors
                        .push(format!("delete unmanaged sandbox {unmanaged_id}: {error}"));
                }
            }
        }

        match (verification, cleanup_errors.is_empty()) {
            (Err(error), _) => panic!("live list/reconcile verification failed: {error}"),
            (Ok(()), false) => panic!(
                "live list/reconcile verification passed but sandbox cleanup failed: {}",
                cleanup_errors.join("; ")
            ),
            (Ok(()), true) => {}
        }
    }
}
