//! Behavior tests for the portable local isolation modes.
//!
//! `workspace` is the opt-in "modify only inside the tool workspace" mode: the
//! provider resolves it to seatbelt/bubblewrap, confines file/search APIs to
//! the workspace, pins exec cwd, and redirects temp state into a per-session
//! directory under the workspace. Omitting `isolation` (or selecting
//! `unrestricted`) must preserve the historical cross-workspace behavior.

use backend::local::local_backend_with_isolation;
use operation_protocol::capability::{
    exec::ExecRequest,
    filesystem::{ReadBytesRequest, TempPathKind, TempPathRequest, WriteBytesRequest, WriteMode},
};
use operation_protocol::{BackendPath, OperationBackend, OperationError};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let temp = tempfile::tempdir().expect("tempdir");
    let workspace = temp.path().join("workspace");
    let outside = temp.path().join("outside");
    std::fs::create_dir_all(workspace.as_path()).unwrap();
    std::fs::create_dir_all(outside.as_path()).unwrap();
    let workspace = std::fs::canonicalize(workspace.as_path()).unwrap();
    let outside = std::fs::canonicalize(outside.as_path()).unwrap();
    (temp, workspace, outside)
}

fn workspace_backend(
    workspace: &Path,
) -> Result<Arc<dyn OperationBackend>, backend::local::LocalBuildError> {
    local_backend_with_isolation(
        workspace.to_path_buf(),
        None,
        None,
        Some("/bin/sh".to_string()),
        Some(json!({ "kind": "workspace" })),
    )
}

fn unrestricted_backend(
    workspace: &Path,
    isolation: Option<serde_json::Value>,
) -> Arc<dyn OperationBackend> {
    local_backend_with_isolation(
        workspace.to_path_buf(),
        None,
        None,
        Some("/bin/sh".to_string()),
        isolation,
    )
    .expect("unrestricted backend")
}

fn assert_denied<T>(result: Result<T, OperationError>) {
    match result {
        Err(OperationError::SandboxPolicyDenied { .. }) => {}
        Err(other) => panic!("expected SandboxPolicyDenied, got: {other:?}"),
        Ok(_) => panic!("expected SandboxPolicyDenied, got Ok"),
    }
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}

fn exec_sh(
    backend: &dyn OperationBackend,
    cwd: &Path,
    command: &str,
) -> Result<operation_protocol::capability::exec::ExecResult, OperationError> {
    let runtime = runtime();
    runtime.block_on(backend.exec().exec(ExecRequest {
        command: command.to_string(),
        args: Vec::new(),
        shell: Some("/bin/sh".to_string()),
        cwd: Some(BackendPath(cwd.to_string_lossy().into_owned())),
        timeout_ms: Some(10_000),
        env: None,
        extra: None,
    }))
}

fn exec_sh_without_cwd(
    backend: &dyn OperationBackend,
    command: &str,
) -> Result<operation_protocol::capability::exec::ExecResult, OperationError> {
    let runtime = runtime();
    runtime.block_on(backend.exec().exec(ExecRequest {
        command: command.to_string(),
        args: Vec::new(),
        shell: Some("/bin/sh".to_string()),
        cwd: None,
        timeout_ms: Some(10_000),
        env: None,
        extra: None,
    }))
}

fn has_bwrap() -> bool {
    #[cfg(target_os = "linux")]
    {
        std::env::var_os("PATH")
            .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join("bwrap").is_file()))
            .unwrap_or(false)
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// The CI/sandboxed test runner may forbid nested OS sandboxes (on macOS
/// `sandbox-exec` can fail with `sandbox_apply: Operation not permitted`).
/// Exec-confinement assertions only run when the platform sandbox can
/// actually be applied; the policy/config assertions always run.
fn os_sandbox_exec_available() -> bool {
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", "(version 1)(allow default)", "/usr/bin/true"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }
    #[cfg(target_os = "linux")]
    {
        if !has_bwrap() {
            return false;
        }
        std::process::Command::new("bwrap")
            .args(["--ro-bind", "/", "/", "--", "/bin/true"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        false
    }
}

#[test]
fn workspace_mode_confines_file_api_exec_and_temp() {
    if !cfg!(any(target_os = "macos", target_os = "linux")) {
        return;
    }
    if cfg!(target_os = "linux") && !has_bwrap() {
        return; // covered by workspace_mode_fails_closed_without_bwrap
    }

    let (_temp, workspace, outside) = fixture();
    let backend = workspace_backend(workspace.as_path()).expect("workspace backend");

    // File API is confined to the workspace.
    let inside = workspace.join("inside.txt");
    runtime()
        .block_on(backend.files().write_bytes(WriteBytesRequest {
            path: BackendPath(inside.to_string_lossy().into_owned()),
            content: b"inside".to_vec(),
            mode: WriteMode::AtomicOverwrite,
        }))
        .expect("write inside workspace");
    assert_denied(
        runtime().block_on(backend.files().write_bytes(WriteBytesRequest {
            path: BackendPath(outside.join("denied.txt").to_string_lossy().into_owned()),
            content: b"no".to_vec(),
            mode: WriteMode::AtomicOverwrite,
        })),
    );
    assert!(!outside.join("denied.txt").exists());
    assert_denied(
        runtime().block_on(backend.files().read_bytes(ReadBytesRequest {
            path: BackendPath(outside.join("denied.txt").to_string_lossy().into_owned()),
        })),
    );

    // Temp state lives under the workspace, not in the shared host temp root.
    let temp_path = runtime()
        .block_on(backend.files().temp_path(TempPathRequest {
            kind: TempPathKind::File,
            preferred_parent: None,
            prefix: None,
            suffix: None,
        }))
        .expect("workspace temp path");
    let temp_path = PathBuf::from(temp_path.0);
    let expected_temp = workspace.join(".xgovernor").join("tmp");
    assert!(
        temp_path.starts_with(&expected_temp),
        "temp path {} is not under {}",
        temp_path.display(),
        expected_temp.display()
    );
    assert!(temp_path.is_file());

    // exec cwd is confined even before any process is spawned.
    assert_denied(exec_sh(backend.as_ref(), outside.as_path(), "true"));

    if os_sandbox_exec_available() {
        // The OS sandbox must also stop a command whose cwd is inside the
        // workspace from writing to an arbitrary host path.
        let escaped = outside.join("exec-escaped.txt");
        let result = exec_sh(
            backend.as_ref(),
            workspace.as_path(),
            format!("printf escaped > {}", shell_quote(escaped.as_path())).as_str(),
        )
        .expect("exec result");
        assert_ne!(result.exit_code, Some(0));
        assert!(!escaped.exists(), "exec escaped the workspace sandbox");

        // TMPDIR is redirected into the session workspace.
        let tmpdir = exec_sh(
            backend.as_ref(),
            workspace.as_path(),
            "printf %s \"$TMPDIR\"",
        )
        .expect("print TMPDIR");
        assert_eq!(
            tmpdir.exit_code,
            Some(0),
            "TMPDIR probe failed: stderr={}",
            String::from_utf8_lossy(tmpdir.stderr.as_slice())
        );
        assert_eq!(
            String::from_utf8_lossy(tmpdir.stdout.as_slice()).trim_end(),
            expected_temp.to_string_lossy()
        );

        // The working directory is the workspace itself; the per-session
        // temp directory is only TMPDIR, never the exec cwd. This also covers
        // the cwd-less request path, which is pinned to the workspace.
        let pwd = exec_sh_without_cwd(backend.as_ref(), "pwd").expect("print cwd");
        assert_eq!(
            pwd.exit_code,
            Some(0),
            "cwd probe failed: stderr={}",
            String::from_utf8_lossy(pwd.stderr.as_slice())
        );
        assert_eq!(
            String::from_utf8_lossy(pwd.stdout.as_slice()).trim_end(),
            workspace.to_string_lossy()
        );
    }
}

#[test]
fn unrestricted_mode_preserves_cross_workspace_access() {
    let (_temp, workspace, outside) = fixture();
    for isolation in [None, Some(json!({ "kind": "unrestricted" }))] {
        let backend = unrestricted_backend(workspace.as_path(), isolation);

        let target = outside.join("unrestricted.txt");
        runtime()
            .block_on(backend.files().write_bytes(WriteBytesRequest {
                path: BackendPath(target.to_string_lossy().into_owned()),
                content: b"outside".to_vec(),
                mode: WriteMode::AtomicOverwrite,
            }))
            .expect("unrestricted write outside workspace");
        assert_eq!(std::fs::read(target.as_path()).unwrap(), b"outside");

        let result = exec_sh(backend.as_ref(), outside.as_path(), "true")
            .expect("unrestricted exec outside cwd");
        assert_eq!(result.exit_code, Some(0));
    }
}

#[cfg(unix)]
#[test]
fn workspace_mode_rejects_symlinked_temp_parent() {
    let (_temp, workspace, outside) = fixture();
    std::os::unix::fs::symlink(outside.as_path(), workspace.join(".xgovernor").as_path()).unwrap();

    let error = match workspace_backend(workspace.as_path()) {
        Ok(_) => panic!("workspace temp parent must not be a symlink"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        backend::local::LocalBuildError::InvalidConfig { .. }
    ));
    assert!(!outside.join("tmp").exists());
}

#[cfg(target_os = "linux")]
#[test]
fn workspace_mode_fails_closed_without_bubblewrap() {
    if has_bwrap() {
        return;
    }
    let (_temp, workspace, _outside) = fixture();
    let error = match workspace_backend(workspace.as_path()) {
        Ok(_) => panic!("workspace mode must fail closed without bwrap"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        backend::local::LocalBuildError::Unsupported { .. }
    ));
}
