use crate::local::backend::{LocalBackendState, LocalOperationBackend};
use crate::local::error::LocalBuildError;
use crate::local::policy::{LocalBackendPolicy, LocalIsolationOptions};
use operation_protocol::{BackendPath, OperationBackend};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Build a [`LocalOperationBackend`] with no sandbox isolation configured.
///
/// This is a thin convenience wrapper over [`local_backend_with_isolation`]
/// with `isolation: None`. Kept separate from xiaoO's generic,
/// config-driven `build_backend` (which dispatched across multiple backend
/// kinds via a serialized `OperationBackendConfig`) — that dispatch pattern
/// is out of scope for this phase, which is local-only.
pub fn local_backend(
    workspace_root: PathBuf,
    home_dir: Option<PathBuf>,
    temp_root: Option<PathBuf>,
    default_shell: Option<String>,
) -> Result<Arc<dyn OperationBackend>, LocalBuildError> {
    local_backend_with_isolation(workspace_root, home_dir, temp_root, default_shell, None)
}

pub fn local_backend_with_isolation(
    workspace_root: PathBuf,
    home_dir: Option<PathBuf>,
    temp_root: Option<PathBuf>,
    default_shell: Option<String>,
    isolation: Option<serde_json::Value>,
) -> Result<Arc<dyn OperationBackend>, LocalBuildError> {
    let workspace_root_host = absolute_dir(
        "workspace_root",
        workspace_root
            .to_str()
            .ok_or_else(|| LocalBuildError::InvalidConfig {
                message: format!(
                    "workspace_root is not valid utf-8: {}",
                    workspace_root.display()
                ),
            })?,
    )?;
    let workspace_root = backend_path_from_host_path(workspace_root_host.as_path())?;

    let home_dir_host = home_dir
        .map(|path| {
            let text = path
                .to_str()
                .ok_or_else(|| LocalBuildError::InvalidConfig {
                    message: format!("home_dir is not valid utf-8: {}", path.display()),
                })?;
            absolute_dir("home_dir", text)
        })
        .transpose()?;
    let home_dir = home_dir_host
        .as_ref()
        .map(|path| backend_path_from_host_path(path.as_path()))
        .transpose()?;
    let isolation = isolation
        .map(serde_json::from_value::<LocalIsolationOptions>)
        .transpose()
        .map_err(|error| LocalBuildError::InvalidConfig {
            message: format!("invalid local backend isolation options: {error}"),
        })?;
    let workspace_scoped = matches!(
        isolation.as_ref(),
        Some(LocalIsolationOptions::Workspace { .. })
    );
    let temp_root_host = if workspace_scoped {
        // Strict workspace mode owns its scratch space. Ignoring a caller
        // supplied `temp_root` here is deliberate: a shared host temp
        // directory would be reachable by every session.
        let temp = workspace_root_host.join(".xgovernor").join("tmp");
        ensure_workspace_dir(temp.as_path())?;
        let canonical = std::fs::canonicalize(temp.as_path()).map_err(|error| {
            LocalBuildError::InvalidConfig {
                message: format!(
                    "failed to resolve workspace temp directory {}: {error}",
                    temp.display()
                ),
            }
        })?;
        if !canonical.starts_with(workspace_root_host.as_path()) {
            return Err(LocalBuildError::InvalidConfig {
                message: format!(
                    "workspace temp directory {} resolves outside the workspace {}",
                    canonical.display(),
                    workspace_root_host.display()
                ),
            });
        }
        canonical
    } else {
        temp_root.unwrap_or_else(std::env::temp_dir)
    };
    let policy = LocalBackendPolicy::from_isolation_options(
        isolation,
        workspace_root_host.as_path(),
        temp_root_host.as_path(),
    )?;

    Ok(Arc::new(LocalOperationBackend::new(Arc::new(
        LocalBackendState {
            backend_id: "local".to_string(),
            workspace_root,
            workspace_root_host,
            home_dir,
            home_dir_host,
            temp_root_host,
            default_shell,
            policy,
        },
    ))))
}

/// Creates `path` (and missing parents) without following symlinks. Existing
/// entries must be real directories; otherwise the strict workspace temp
/// root could be redirected outside the workspace by a planted symlink.
fn ensure_workspace_dir(path: &Path) -> Result<(), LocalBuildError> {
    if let Some(parent) = path.parent() {
        if parent != Path::new(std::path::MAIN_SEPARATOR_STR) {
            ensure_workspace_dir(parent)?;
        }
    }
    match std::fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = std::fs::symlink_metadata(path).map_err(|error| {
                LocalBuildError::InvalidConfig {
                    message: format!(
                        "failed to inspect workspace temp directory {}: {error}",
                        path.display()
                    ),
                }
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(LocalBuildError::InvalidConfig {
                    message: format!(
                        "workspace temp path {} must be a real directory, not a symlink/file",
                        path.display()
                    ),
                });
            }
            Ok(())
        }
        Err(error) => Err(LocalBuildError::InvalidConfig {
            message: format!(
                "failed to create workspace temp directory {}: {error}",
                path.display()
            ),
        }),
    }
}

pub(crate) fn absolute_dir(
    field_name: &str,
    value: &str,
) -> Result<std::path::PathBuf, LocalBuildError> {
    let path = Path::new(value);
    if !path.is_absolute() {
        return Err(LocalBuildError::InvalidConfig {
            message: format!("{field_name} must be an absolute path: {value}"),
        });
    }
    // Canonicalize once so the policy roots, the sandbox `--chdir` target and
    // the workspace temp directory all agree on the same spelling (notably
    // `/var` vs `/private/var` on macOS).
    let path = std::fs::canonicalize(path).map_err(|error| LocalBuildError::InvalidConfig {
        message: format!("failed to read {field_name}: {error}"),
    })?;
    let metadata =
        std::fs::metadata(path.as_path()).map_err(|error| LocalBuildError::InvalidConfig {
            message: format!("failed to read {field_name}: {error}"),
        })?;
    if !metadata.is_dir() {
        return Err(LocalBuildError::InvalidConfig {
            message: format!("{field_name} must point to a directory: {value}"),
        });
    }
    Ok(path)
}

pub(crate) fn backend_path_from_host_path(path: &Path) -> Result<BackendPath, LocalBuildError> {
    let text = path
        .to_str()
        .ok_or_else(|| LocalBuildError::InvalidConfig {
            message: format!("path is not valid utf-8: {}", path.display()),
        })?;
    Ok(BackendPath(text.to_string()))
}
