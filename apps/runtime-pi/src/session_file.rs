//! Pure helpers for locating and validating pi's own JSONL session files
//! inside a `--session-dir` directory, used by `PiRuntime::start()`'s
//! restoration path (`docs/pi_session_restore_plan.md` §1.3).
//!
//! Per Phase 0's F14 finding, `pi` batches an entire turn's writes to disk
//! atomically — nothing for an in-flight turn appears on disk until the
//! model reaches a terminal `stopReason`, so SIGKILLing `pi` mid-turn never
//! leaves a half-written turn behind. This demotes this module's job from
//! "trim a possibly-corrupt tail" (the original plan) down to "confirm the
//! latest file is exactly what F14 promises it will be" before trusting it
//! enough to hand to a freshly spawned `pi --session <path>`. Nothing here
//! ever repairs or rewrites a session file — a file that fails validation is
//! reported, never touched.

use std::path::{Path, PathBuf};

/// Why [`latest_complete_turn_file`] refused to trust a session directory.
/// The `Display` impl is embedded (with a `pi_session_state_lost` prefix,
/// per `docs/pi_session_restore_plan.md` §1.4) into the `SessionDomainError`
/// the caller returns.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SessionFileError {
    DirectoryMissing(PathBuf),
    NoSessionFiles(PathBuf),
    Io { path: PathBuf, message: String },
    Empty(PathBuf),
    LastLineNotJson { path: PathBuf, message: String },
    LastLineNotCompleteAssistantMessage { path: PathBuf },
}

impl std::fmt::Display for SessionFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DirectoryMissing(dir) => {
                write!(f, "session directory {} does not exist", dir.display())
            }
            Self::NoSessionFiles(dir) => {
                write!(f, "no .jsonl session files found in {}", dir.display())
            }
            Self::Io { path, message } => {
                write!(f, "failed to read session file {}: {message}", path.display())
            }
            Self::Empty(path) => write!(f, "session file {} is empty", path.display()),
            Self::LastLineNotJson { path, message } => write!(
                f,
                "last line of session file {} did not parse as JSON: {message}",
                path.display()
            ),
            Self::LastLineNotCompleteAssistantMessage { path } => write!(
                f,
                "last line of session file {} is not a complete assistant message with a \
                 non-pending stopReason",
                path.display()
            ),
        }
    }
}

/// Finds the most recently modified `*.jsonl` file directly inside
/// `session_dir` (pi lays these out flat when given an explicit
/// `--session-dir` — F13) and validates that its last non-blank line is a
/// fully-formed, terminal assistant message (F14: pi writes an entire turn
/// atomically, so a valid last line means the file holds no half-written
/// turn). Returns the file's path on success; otherwise a
/// [`SessionFileError`] describing exactly what did not hold.
pub(crate) fn latest_complete_turn_file(session_dir: &Path) -> Result<PathBuf, SessionFileError> {
    if !session_dir.is_dir() {
        return Err(SessionFileError::DirectoryMissing(session_dir.to_path_buf()));
    }

    let entries = std::fs::read_dir(session_dir).map_err(|error| SessionFileError::Io {
        path: session_dir.to_path_buf(),
        message: error.to_string(),
    })?;

    let mut candidates: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| SessionFileError::Io {
            path: session_dir.to_path_buf(),
            message: error.to_string(),
        })?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .map_err(|error| SessionFileError::Io {
                path: path.clone(),
                message: error.to_string(),
            })?;
        candidates.push((modified, path));
    }

    let (_, latest) = candidates
        .into_iter()
        .max_by_key(|(modified, _)| *modified)
        .ok_or_else(|| SessionFileError::NoSessionFiles(session_dir.to_path_buf()))?;

    validate_last_line_is_complete_turn(&latest)?;
    Ok(latest)
}

fn validate_last_line_is_complete_turn(path: &Path) -> Result<(), SessionFileError> {
    let contents = std::fs::read_to_string(path).map_err(|error| SessionFileError::Io {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    let last_line = contents
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .ok_or_else(|| SessionFileError::Empty(path.to_path_buf()))?;

    let value: serde_json::Value =
        serde_json::from_str(last_line).map_err(|error| SessionFileError::LastLineNotJson {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;

    let is_complete_assistant_message = value.get("type").and_then(serde_json::Value::as_str)
        == Some("message")
        && value
            .get("message")
            .and_then(|message| message.get("role"))
            .and_then(serde_json::Value::as_str)
            == Some("assistant")
        && value
            .get("message")
            .and_then(|message| message.get("stopReason"))
            .and_then(serde_json::Value::as_str)
            .map(|reason| reason != "pending")
            .unwrap_or(false);

    if !is_complete_assistant_message {
        return Err(SessionFileError::LastLineNotCompleteAssistantMessage {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_jsonl(dir: &Path, name: &str, lines: &[&str]) -> PathBuf {
        let path = dir.join(name);
        let mut file = std::fs::File::create(&path).unwrap();
        for line in lines {
            writeln!(file, "{line}").unwrap();
        }
        path
    }

    #[test]
    fn a_well_formed_file_with_a_terminal_assistant_message_passes() {
        let dir = tempfile::tempdir().unwrap();
        write_jsonl(
            dir.path(),
            "session.jsonl",
            &[
                r#"{"type":"header"}"#,
                r#"{"type":"message","message":{"role":"user","content":"hi"}}"#,
                r#"{"type":"message","message":{"role":"assistant","content":"hello","stopReason":"end_turn"}}"#,
            ],
        );
        let found =
            latest_complete_turn_file(dir.path()).expect("should find and validate the file");
        assert_eq!(found, dir.path().join("session.jsonl"));
    }

    #[test]
    fn an_empty_file_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        write_jsonl(dir.path(), "session.jsonl", &[]);
        let error = latest_complete_turn_file(dir.path()).unwrap_err();
        assert!(matches!(error, SessionFileError::Empty(_)));
    }

    #[test]
    fn a_last_line_that_fails_to_parse_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        write_jsonl(
            dir.path(),
            "session.jsonl",
            &[r#"{"type":"header"}"#, "not json at all {{{"],
        );
        let error = latest_complete_turn_file(dir.path()).unwrap_err();
        assert!(matches!(error, SessionFileError::LastLineNotJson { .. }));
    }

    #[test]
    fn a_last_line_that_is_a_user_message_not_assistant_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        write_jsonl(
            dir.path(),
            "session.jsonl",
            &[
                r#"{"type":"message","message":{"role":"assistant","content":"prior","stopReason":"end_turn"}}"#,
                r#"{"type":"message","message":{"role":"user","content":"another question"}}"#,
            ],
        );
        let error = latest_complete_turn_file(dir.path()).unwrap_err();
        assert!(matches!(
            error,
            SessionFileError::LastLineNotCompleteAssistantMessage { .. }
        ));
    }

    #[test]
    fn a_last_line_with_pending_stop_reason_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        write_jsonl(
            dir.path(),
            "session.jsonl",
            &[r#"{"type":"message","message":{"role":"assistant","content":"partial","stopReason":"pending"}}"#],
        );
        let error = latest_complete_turn_file(dir.path()).unwrap_err();
        assert!(matches!(
            error,
            SessionFileError::LastLineNotCompleteAssistantMessage { .. }
        ));
    }

    #[test]
    fn picks_the_most_recently_modified_jsonl_file_when_several_exist() {
        let dir = tempfile::tempdir().unwrap();
        write_jsonl(
            dir.path(),
            "older.jsonl",
            &[r#"{"type":"message","message":{"role":"assistant","content":"old","stopReason":"end_turn"}}"#],
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
        let newer = write_jsonl(
            dir.path(),
            "newer.jsonl",
            &[r#"{"type":"message","message":{"role":"assistant","content":"new","stopReason":"end_turn"}}"#],
        );
        let found = latest_complete_turn_file(dir.path()).unwrap();
        assert_eq!(found, newer);
    }

    #[test]
    fn a_missing_directory_is_reported_distinctly() {
        let error =
            latest_complete_turn_file(Path::new("/nonexistent/pi-session-dir-xyz")).unwrap_err();
        assert!(matches!(error, SessionFileError::DirectoryMissing(_)));
    }

    #[test]
    fn a_directory_with_no_jsonl_files_is_reported_distinctly() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("not-a-session.txt"), "irrelevant").unwrap();
        let error = latest_complete_turn_file(dir.path()).unwrap_err();
        assert!(matches!(error, SessionFileError::NoSessionFiles(_)));
    }
}
