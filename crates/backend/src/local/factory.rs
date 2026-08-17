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
    let temp_root_host = temp_root.unwrap_or_else(std::env::temp_dir);
    let isolation = isolation
        .map(serde_json::from_value::<LocalIsolationOptions>)
        .transpose()
        .map_err(|error| LocalBuildError::InvalidConfig {
            message: format!("invalid local backend isolation options: {error}"),
        })?;
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
    let metadata = std::fs::metadata(path).map_err(|error| LocalBuildError::InvalidConfig {
        message: format!("failed to read {field_name}: {error}"),
    })?;
    if !metadata.is_dir() {
        return Err(LocalBuildError::InvalidConfig {
            message: format!("{field_name} must point to a directory: {value}"),
        });
    }
    Ok(path.to_path_buf())
}

pub(crate) fn backend_path_from_host_path(path: &Path) -> Result<BackendPath, LocalBuildError> {
    let text = path
        .to_str()
        .ok_or_else(|| LocalBuildError::InvalidConfig {
            message: format!("path is not valid utf-8: {}", path.display()),
        })?;
    Ok(BackendPath(text.to_string()))
}
