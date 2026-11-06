use backend::process_group;
use std::path::Path;
use xgovernor_core::SessionDomainError;

pub(crate) const WORKER_PGID_FILE: &str = "worker.pgid";

fn unavailable(message: impl Into<String>) -> SessionDomainError {
    SessionDomainError::Unavailable {
        message: message.into(),
    }
}

/// Records the current worker's process-group id for a future restart. Best
/// effort: failing to write the marker must not fail the session start.
pub(crate) async fn record_worker_pgid(session_dir: &Path, pgid: i32) {
    if pgid <= 0 {
        return;
    }
    let path = session_dir.join(WORKER_PGID_FILE);
    let temp = session_dir.join(format!(".{WORKER_PGID_FILE}.tmp"));
    let result = async {
        tokio::fs::write(&temp, format!("{pgid}\n")).await?;
        tokio::fs::rename(&temp, &path).await
    }
    .await;
    if let Err(error) = result {
        let _ = tokio::fs::remove_file(&temp).await;
        tracing::warn!(
            path = %path.display(),
            %pgid,
            %error,
            "failed to persist Pi worker process-group marker"
        );
    }
}

pub(crate) async fn clear_worker_pgid(session_dir: &Path) {
    let path = session_dir.join(WORKER_PGID_FILE);
    match tokio::fs::remove_file(&path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "failed to remove Pi worker pgid marker");
        }
    }
}

/// Kills a worker group left over from a previous daemon process, if and only
/// if it is still the group belonging to this session. Must run before
/// touching the session JSONL or spawning a replacement worker.
pub(crate) async fn reap_orphan_worker_group(session_dir: &Path) -> Result<(), SessionDomainError> {
    let path = session_dir.join(WORKER_PGID_FILE);
    let contents = match tokio::fs::read_to_string(&path).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(unavailable(format!(
                "pi_worker_orphaned: failed to read {}: {error}",
                path.display()
            )))
        }
    };
    let pgid = match contents.trim().parse::<i32>() {
        Ok(pgid) if pgid > 0 => pgid,
        _ => {
            clear_worker_pgid(session_dir).await;
            return Ok(());
        }
    };

    if !process_group::process_group_exists(pgid) {
        clear_worker_pgid(session_dir).await;
        return Ok(());
    }

    match worker_group_matches_session(pgid, session_dir) {
        Ok(true) => {
            tracing::warn!(
                pgid,
                session_dir = %session_dir.display(),
                "reaping orphaned Pi worker process group before restoration"
            );
            process_group::terminate_process_group(pgid).map_err(|message| {
                unavailable(format!(
                    "pi_worker_orphaned: could not terminate group {pgid}: {message}"
                ))
            })?;
            clear_worker_pgid(session_dir).await;
            Ok(())
        }
        Ok(false) => {
            // The pgid was reused by something that is not this session's Pi
            // tree; never kill it. The marker is stale.
            tracing::warn!(
                pgid,
                session_dir = %session_dir.display(),
                "stale Pi worker pgid marker no longer matches this session; dropping it"
            );
            clear_worker_pgid(session_dir).await;
            Ok(())
        }
        Err(message) => {
            // Process inspection can be unavailable in restricted containers
            // (e.g. `ps`/procfs denied by the sandbox). Losing the ability to
            // verify is not a reason to brick an otherwise recoverable
            // session; keep the marker, proceed with restoration, and let a
            // later restart retry the check. The error is loud because an
            // orphaned nested `pi` may still be running in this window.
            tracing::warn!(
                pgid,
                session_dir = %session_dir.display(),
                error = %message,
                "cannot verify Pi worker orphan group; proceeding without killing it"
            );
            Ok(())
        }
    }
}

fn worker_group_matches_session(pgid: i32, session_dir: &Path) -> Result<bool, String> {
    let commands = group_commands(pgid)?;
    let needle = format!("--session-dir {}", session_dir.to_string_lossy());
    Ok(commands.iter().any(|command| command.contains(&needle)))
}

#[cfg(target_os = "linux")]
fn group_commands(pgid: i32) -> Result<Vec<String>, String> {
    let mut commands = Vec::new();
    let entries = std::fs::read_dir("/proc").map_err(|error| error.to_string())?;
    for entry in entries {
        let entry = entry.map_err(|error| error.to_string())?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if name.parse::<u32>().is_err() {
            continue;
        }
        let dir = entry.path();
        let Ok(stat) = std::fs::read_to_string(dir.join("stat")) else {
            continue;
        };
        // Field 5 of /proc/<pid>/stat is the process group; the first four
        // fields are pid, comm, state and ppid. `comm` can contain spaces, so
        // anchor after its closing ')'.
        let Some((_, after_comm)) = stat.rsplit_once(')') else {
            continue;
        };
        let mut fields = after_comm.split_whitespace();
        let _state = fields.next();
        let _ppid = fields.next();
        let Ok(group) = fields.next().unwrap_or("").parse::<i32>() else {
            continue;
        };
        if group != pgid {
            continue;
        }
        let Ok(raw) = std::fs::read(dir.join("cmdline")) else {
            continue;
        };
        let command = raw
            .split(|byte| *byte == 0)
            .filter(|part| !part.is_empty())
            .map(|part| String::from_utf8_lossy(part).into_owned())
            .collect::<Vec<_>>()
            .join(" ");
        if !command.is_empty() {
            commands.push(command);
        }
    }
    Ok(commands)
}

#[cfg(target_os = "macos")]
fn group_commands(pgid: i32) -> Result<Vec<String>, String> {
    let output = std::process::Command::new("ps")
        .args(["-axww", "-o", "pgid=,command="])
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!("ps exited with {}", output.status));
    }
    let mut commands = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let line = line.trim_start();
        let Some((group, command)) = line.split_once(|character: char| character.is_whitespace())
        else {
            continue;
        };
        let Ok(group) = group.parse::<i32>() else {
            continue;
        };
        if group == pgid && !command.trim().is_empty() {
            commands.push(command.trim().to_string());
        }
    }
    Ok(commands)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn group_commands(_pgid: i32) -> Result<Vec<String>, String> {
    Err("process inspection is only supported on Linux and macOS".to_string())
}
