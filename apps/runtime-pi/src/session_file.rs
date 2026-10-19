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
//!
//! `pi` may append bookkeeping records after a completed turn — `compaction`
//! (context-compaction summaries), `model_change`, `thinking_level_change` —
//! none of which are `message` records. Validation therefore anchors on the
//! last `message`-typed record (which must be a complete, non-pending
//! assistant message) and tolerates trailing bookkeeping records after it;
//! only a physically half-written trailing line (failed JSON parse) is
//! treated as truncation.

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
                write!(
                    f,
                    "failed to read session file {}: {message}",
                    path.display()
                )
            }
            Self::Empty(path) => write!(f, "session file {} is empty", path.display()),
            Self::LastLineNotJson { path, message } => write!(
                f,
                "last line of session file {} did not parse as JSON: {message}",
                path.display()
            ),
            Self::LastLineNotCompleteAssistantMessage { path } => write!(
                f,
                "last message record of session file {} is not a complete assistant message \
                 with a non-pending stopReason",
                path.display()
            ),
        }
    }
}

/// Finds the most recently modified `*.jsonl` file directly inside
/// `session_dir` (pi lays these out flat when given an explicit
/// `--session-dir` — F13) and validates that its last `message`-typed record
/// is a fully-formed, terminal assistant message (F14: pi writes an entire
/// turn atomically, so a valid last message record means the file holds no
/// half-written turn). Trailing non-`message` bookkeeping records that `pi`
/// appends after a completed turn — e.g. `compaction` summaries,
/// `model_change`, `thinking_level_change` — are tolerated after it. Returns
/// the file's path on success; otherwise a [`SessionFileError`] describing
/// exactly what did not hold.
pub(crate) fn latest_complete_turn_file(session_dir: &Path) -> Result<PathBuf, SessionFileError> {
    if !session_dir.is_dir() {
        return Err(SessionFileError::DirectoryMissing(
            session_dir.to_path_buf(),
        ));
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
    let mut lines = contents
        .lines()
        .rev()
        .filter(|line| !line.trim().is_empty());
    let last_line = lines
        .next()
        .ok_or_else(|| SessionFileError::Empty(path.to_path_buf()))?;

    // Truncation gate: the physical last line must parse as JSON. pi appends
    // whole turns atomically (F14), so a half-written trailing line means the
    // process was killed mid-append.
    serde_json::from_str::<serde_json::Value>(last_line).map_err(|error| {
        SessionFileError::LastLineNotJson {
            path: path.to_path_buf(),
            message: error.to_string(),
        }
    })?;

    // Anchor on the last `message`-typed record: pi may legitimately append
    // bookkeeping records after a completed turn (`compaction`,
    // `model_change`, `thinking_level_change`, ...), which are not messages
    // and must not fail validation. A file whose last message record is not a
    // complete assistant message still fails, as does one with no message
    // records at all.
    for line in std::iter::once(last_line).chain(lines) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            // Only the physical last line must parse (checked above); an
            // unparseable line in the middle cannot be a truncation tail.
            continue;
        };
        if value.get("type").and_then(serde_json::Value::as_str) != Some("message") {
            continue;
        }
        let is_complete_assistant_message = value
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
        return Ok(());
    }
    Err(SessionFileError::LastLineNotCompleteAssistantMessage {
        path: path.to_path_buf(),
    })
}
