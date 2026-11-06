use base64::Engine;
use session_protocol::{
    SessionExecRequest, SessionExecResult, SessionFileReadRequest, SessionFileWriteRequest,
    SessionLeaseClaim,
};
use uuid::Uuid;
use xgovernor_core::{SecurityContext, SessionApplication};

use super::mcts::MctsApiError;
use super::mcts_context::{
    MctsCodeSource, MctsTurnDirective, MAX_FILE_PATHS, MAX_TEXT_FILE_BYTES, MAX_TEXT_TOTAL_BYTES,
};

const WORKSPACE_ROOT: &str = "/home/user/workspace";
const REPO_DIR: &str = "/home/user/workspace/repo";
const FILE_MATERIALIZE_TIMEOUT_MS: u64 = 20_000;
const GIT_MATERIALIZE_TIMEOUT_MS: u64 = 120_000;

/// Materialize the consolidated code source into `/home/user/workspace/repo`
/// inside the running session, before the MCTS root-state checkpoint is taken.
/// A `None` source deliberately mirrors P5: checkpoint the session in place
/// without touching the workspace.
pub(super) async fn materialize(
    app: &SessionApplication,
    ctx: &SecurityContext,
    runtime_id: &str,
    lease: &SessionLeaseClaim,
    directive: &MctsTurnDirective,
) -> Result<(), MctsApiError> {
    let Some(source) = directive.code_source.as_ref() else {
        return Ok(());
    };
    match source.kind.as_str() {
        "file" => materialize_file(app, ctx, runtime_id, lease, source).await,
        "text" => materialize_text(app, ctx, runtime_id, lease, source).await,
        "git" => materialize_git(app, ctx, runtime_id, lease, source).await,
        "empty" => ensure_repo_dir(app, ctx, runtime_id, lease).await,
        other => Err(MctsApiError::bad_request(format!(
            "unsupported code_source kind: {other}"
        ))),
    }
}

async fn materialize_file(
    app: &SessionApplication,
    ctx: &SecurityContext,
    runtime_id: &str,
    lease: &SessionLeaseClaim,
    source: &MctsCodeSource,
) -> Result<(), MctsApiError> {
    if source.paths.is_empty() || source.paths.len() > MAX_FILE_PATHS {
        return Err(MctsApiError::bad_request(format!(
            "file code_source requires between 1 and {MAX_FILE_PATHS} paths"
        )));
    }
    ensure_repo_dir(app, ctx, runtime_id, lease).await?;
    for raw in &source.paths {
        let rel = workspace_relative(raw)?;
        // Existence probe first; a missing path must fail before checkpointing.
        app.read_file(
            ctx,
            SessionFileReadRequest {
                runtime_id: runtime_id.to_string(),
                path: rel.clone(),
                lease: lease.clone(),
            },
        )
        .await
        .map_err(|error| {
            MctsApiError::bad_request(format!("file code_source path not found ({raw}): {error}"))
        })?;
        // Already inside the unified repo? Nothing to copy.
        if rel == "repo" || rel.strip_prefix("repo/").is_some() {
            continue;
        }
        let src = format!("{WORKSPACE_ROOT}/{rel}");
        let dest = format!("{REPO_DIR}/{rel}");
        if let Some((parent, _)) = dest.rsplit_once('/') {
            if parent != WORKSPACE_ROOT {
                exec_ok(
                    app,
                    ctx,
                    runtime_id,
                    lease,
                    FILE_MATERIALIZE_TIMEOUT_MS,
                    &["mkdir", "-p", parent],
                )
                .await?;
            }
        }
        exec_ok(
            app,
            ctx,
            runtime_id,
            lease,
            FILE_MATERIALIZE_TIMEOUT_MS,
            &["cp", "-a", &src, &dest],
        )
        .await?;
    }
    Ok(())
}

async fn materialize_text(
    app: &SessionApplication,
    ctx: &SecurityContext,
    runtime_id: &str,
    lease: &SessionLeaseClaim,
    source: &MctsCodeSource,
) -> Result<(), MctsApiError> {
    if source.files.is_empty() {
        return Err(MctsApiError::bad_request(
            "text code_source requires at least one file",
        ));
    }
    let mut total_bytes = 0usize;
    for (path, content) in &source.files {
        if content.len() > MAX_TEXT_FILE_BYTES {
            return Err(MctsApiError::bad_request(format!(
                "text code_source file exceeds {MAX_TEXT_FILE_BYTES} bytes: {path}"
            )));
        }
        total_bytes = total_bytes.saturating_add(content.len());
    }
    if total_bytes > MAX_TEXT_TOTAL_BYTES {
        return Err(MctsApiError::bad_request(format!(
            "text code_source total exceeds {MAX_TEXT_TOTAL_BYTES} bytes"
        )));
    }

    ensure_repo_dir(app, ctx, runtime_id, lease).await?;
    for (path, content) in &source.files {
        let rel = workspace_relative(path)?;
        let rel = rel.strip_prefix("repo/").unwrap_or(&rel);
        let dest = format!("repo/{rel}");
        let content_base64 = base64::engine::general_purpose::STANDARD.encode(content.as_bytes());
        app.write_file(
            ctx,
            SessionFileWriteRequest {
                runtime_id: runtime_id.to_string(),
                path: dest.clone(),
                content_base64,
                create_parents: true,
                lease: lease.clone(),
            },
        )
        .await
        .map_err(|error| {
            MctsApiError::bad_request(format!("text code_source write failed ({dest}): {error}"))
        })?;

        // Read back and verify the bytes made it to disk before checkpointing.
        let read_back = app
            .read_file(
                ctx,
                SessionFileReadRequest {
                    runtime_id: runtime_id.to_string(),
                    path: dest.clone(),
                    lease: lease.clone(),
                },
            )
            .await
            .map_err(|error| {
                MctsApiError::bad_request(format!(
                    "text code_source verification read failed ({dest}): {error}"
                ))
            })?;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(read_back.content_base64)
            .map_err(|error| {
                MctsApiError::bad_request(format!(
                    "text code_source verification decode failed ({dest}): {error}"
                ))
            })?;
        if String::from_utf8_lossy(&decoded) != content.as_str() {
            return Err(MctsApiError::bad_request(format!(
                "text code_source verification mismatch: {dest}"
            )));
        }
    }
    Ok(())
}

async fn materialize_git(
    app: &SessionApplication,
    ctx: &SecurityContext,
    runtime_id: &str,
    lease: &SessionLeaseClaim,
    source: &MctsCodeSource,
) -> Result<(), MctsApiError> {
    let url = source
        .url
        .as_deref()
        .filter(|url| url.starts_with("https://"))
        .ok_or_else(|| MctsApiError::bad_request("git code_source requires an https:// url"))?;
    let git_ref = source.git_ref.as_deref().unwrap_or("main");
    if git_ref.is_empty() || git_ref.contains(['\'', '"', ' ']) {
        return Err(MctsApiError::bad_request("git code_source has an invalid ref"));
    }
    let subdir = source.subdir.as_deref().unwrap_or("");
    if subdir.starts_with('/') || subdir.contains("..") || subdir.contains('\'') {
        return Err(MctsApiError::bad_request("git code_source has an invalid subdir"));
    }

    ensure_repo_dir(app, ctx, runtime_id, lease).await?;
    let tmp = format!("/tmp/mcts-clone-{}", Uuid::new_v4());
    exec_ok(
        app,
        ctx,
        runtime_id,
        lease,
        GIT_MATERIALIZE_TIMEOUT_MS,
        &["rm", "-rf", &tmp],
    )
    .await?;
    exec_ok(
        app,
        ctx,
        runtime_id,
        lease,
        GIT_MATERIALIZE_TIMEOUT_MS,
        &["git", "clone", "--depth", "1", "--branch", git_ref, url, &tmp],
    )
    .await?;

    let clone_src = if subdir.is_empty() {
        tmp.clone()
    } else {
        format!("{tmp}/{subdir}")
    };
    // `cp -a` preserves layout without creating symlinks outside the repo.
    exec_ok(
        app,
        ctx,
        runtime_id,
        lease,
        GIT_MATERIALIZE_TIMEOUT_MS,
        &[
            "bash",
            "-lc",
            &format!(
                "mkdir -p {REPO_DIR}; cp -a \"{clone_src}/.\" \"{REPO_DIR}/\"; rm -rf \"{tmp}\""
            ),
        ],
    )
    .await
}

async fn ensure_repo_dir(
    app: &SessionApplication,
    ctx: &SecurityContext,
    runtime_id: &str,
    lease: &SessionLeaseClaim,
) -> Result<(), MctsApiError> {
    exec_ok(
        app,
        ctx,
        runtime_id,
        lease,
        FILE_MATERIALIZE_TIMEOUT_MS,
        &["mkdir", "-p", REPO_DIR],
    )
    .await
}

/// Converts an LLM-supplied path to a path relative to `/home/user/workspace`.
/// Absolute paths are only accepted when they are already inside the
/// workspace root; anything escaping it is rejected.
fn workspace_relative(raw: &str) -> Result<String, MctsApiError> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(MctsApiError::bad_request("code_source path must not be empty"));
    }
    let rel = if raw == WORKSPACE_ROOT {
        return Err(MctsApiError::bad_request(
            "code_source path must name a file, not the workspace root",
        ));
    } else if let Some(stripped) = raw.strip_prefix(&format!("{WORKSPACE_ROOT}/")) {
        stripped.to_string()
    } else if raw.starts_with('/') {
        return Err(MctsApiError::bad_request(format!(
            "code_source path must be inside {WORKSPACE_ROOT}: {raw}"
        )));
    } else {
        raw.to_string()
    };
    let rel = rel.trim_start_matches("./");
    if rel.is_empty() || rel == "." || rel.starts_with("..") {
        return Err(MctsApiError::bad_request(format!(
            "code_source path escapes the workspace: {raw}"
        )));
    }
    Ok(rel.to_string())
}

async fn exec_capture(
    app: &SessionApplication,
    ctx: &SecurityContext,
    runtime_id: &str,
    lease: &SessionLeaseClaim,
    timeout_ms: u64,
    args: &[&str],
) -> Result<SessionExecResult, MctsApiError> {
    let command = args.iter().map(|arg| arg.to_string()).collect();
    app.exec(
        ctx,
        SessionExecRequest {
            runtime_id: runtime_id.to_string(),
            command,
            cwd: Some(WORKSPACE_ROOT.to_string()),
            env: Default::default(),
            timeout_ms: Some(timeout_ms),
            lease: lease.clone(),
        },
    )
    .await
    .map_err(|error| MctsApiError::bad_request(format!("materialize exec failed: {error}")))
}

async fn exec_ok(
    app: &SessionApplication,
    ctx: &SecurityContext,
    runtime_id: &str,
    lease: &SessionLeaseClaim,
    timeout_ms: u64,
    args: &[&str],
) -> Result<(), MctsApiError> {
    let display = args.join(" ");
    let result = exec_capture(app, ctx, runtime_id, lease, timeout_ms, args).await?;
    if result.timed_out {
        return Err(MctsApiError::bad_request(format!(
            "materialize timed out running `{display}`"
        )));
    }
    if result.exit_code != Some(0) {
        let tail = result
            .stderr
            .trim()
            .lines()
            .rev()
            .take(5)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("; ");
        let detail = if tail.is_empty() {
            "(no stderr)"
        } else {
            &tail
        };
        return Err(MctsApiError::bad_request(format!(
            "materialize failed running `{display}`: {detail}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_paths_are_normalized_and_gated() {
        assert_eq!(workspace_relative("repo/calc.py").unwrap(), "repo/calc.py");
        assert_eq!(
            workspace_relative("/home/user/workspace/calc.py").unwrap(),
            "calc.py"
        );
        assert!(workspace_relative("/tmp/evil.py").is_err());
        assert!(workspace_relative("../evil.py").is_err());
        assert!(workspace_relative("/home/user/workspace").is_err());
    }
}
