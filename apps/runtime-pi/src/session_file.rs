//! Pure helpers for locating and validating pi's own JSONL session files
//! inside a `--session-dir` directory, used by `PiRuntime::start()`'s
//! restoration path (`docs/pi_session_restore_plan.md` §1.3).
//!
//! `pi` persists each message as its `message_end` arrives, so a process
//! killed mid-turn can leave the newest file ending on that turn's user
//! message, a tool result, a `pending` assistant message, or a physically
//! half-written JSON line. A restart cannot resume that in-flight turn, but it
//! can safely discard its tail: [`latest_complete_turn_file_repairing_crash_tail`]
//! truncates back to the last completed assistant turn and re-validates,
//! preserving all completed history. Non-tail errors (I/O, missing directory)
//! still fail closed.

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
                "last message record of session file {} is not a completed assistant turn \
                 (stopReason is empty, pending, or toolUse)",
                path.display()
            ),
        }
    }
}

/// Finds the most recently modified `*.jsonl` file directly inside
/// `session_dir` (pi lays these out flat when given an explicit
/// `--session-dir` — F13) and validates that its last `message`-typed record
/// is a fully-formed, terminal assistant message. Trailing non-`message`
/// bookkeeping records that `pi` appends after a completed turn — e.g.
/// `compaction` summaries, `model_change`, `thinking_level_change` — are
/// tolerated after it. Returns the file's path on success; otherwise a
/// [`SessionFileError`] describing exactly what did not hold. This strict
/// variant does not repair; restore paths should use
/// [`latest_complete_turn_file_repairing_crash_tail`].
pub(crate) fn latest_complete_turn_file(session_dir: &Path) -> Result<PathBuf, SessionFileError> {
    let latest = latest_session_file(session_dir)?;
    validate_last_line_is_complete_turn(&latest)?;
    Ok(latest)
}

pub(crate) fn latest_complete_turn_file_repairing_crash_tail(
    session_dir: &Path,
) -> Result<Option<PathBuf>, SessionFileError> {
    match latest_complete_turn_file(session_dir) {
        Ok(path) => Ok(Some(path)),
        Err(SessionFileError::NoSessionFiles(_)) => Ok(None),
        Err(error) if is_crash_tail_error(&error) => {
            truncate_to_last_complete_turn(session_dir)?;
            match latest_complete_turn_file(session_dir) {
                Ok(path) => Ok(Some(path)),
                Err(SessionFileError::NoSessionFiles(_)) => Ok(None),
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

fn is_crash_tail_error(error: &SessionFileError) -> bool {
    matches!(
        error,
        SessionFileError::Empty(_)
            | SessionFileError::LastLineNotJson { .. }
            | SessionFileError::LastLineNotCompleteAssistantMessage { .. }
    )
}

/// Newest `*.jsonl` in `session_dir`, with no completeness judgement. Used by
/// [`latest_complete_turn_file`] and by [`truncate_to_last_complete_turn`],
/// which must be able to find the file it is about to repair.
fn latest_session_file(session_dir: &Path) -> Result<PathBuf, SessionFileError> {
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

    candidates
        .into_iter()
        .max_by_key(|(modified, _)| *modified)
        .map(|(_, path)| path)
        .ok_or_else(|| SessionFileError::NoSessionFiles(session_dir.to_path_buf()))
}

pub(crate) fn truncate_to_last_complete_turn(session_dir: &Path) -> Result<(), SessionFileError> {
    let path = latest_session_file(session_dir)?;
    // Work on raw bytes: a SIGKILL can also cut a multi-byte UTF-8 sequence
    // in half, and that tail must not make the whole repair fail.
    let contents = std::fs::read(&path).map_err(|error| SessionFileError::Io {
        path: path.clone(),
        message: error.to_string(),
    })?;

    let mut cut = 0usize;
    let mut offset = 0usize;
    for line in contents.split_inclusive(|byte| *byte == b'\n') {
        offset += line.len();
        let trimmed = trim_ascii_line(line);
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(trimmed) else {
            continue;
        };
        if value.get("type").and_then(serde_json::Value::as_str) != Some("message") {
            continue;
        }
        let message = &value["message"];
        if message.get("role").and_then(serde_json::Value::as_str) != Some("assistant") {
            continue;
        }
        let stop_reason = message
            .get("stopReason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        // `toolUse` is a completed assistant *message* but not a completed
        // turn: the next entries are its tool results. Cutting there would
        // leave a dangling tool call for the resumed model.
        if stop_reason.is_empty() || stop_reason == "pending" || stop_reason == "toolUse" {
            continue;
        }
        cut = offset;
    }

    if cut == 0 {
        return std::fs::remove_file(&path).map_err(|error| SessionFileError::Io {
            path,
            message: error.to_string(),
        });
    }
    if cut < contents.len() {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|error| SessionFileError::Io {
                path: path.clone(),
                message: error.to_string(),
            })?;
        file.set_len(cut as u64)
            .map_err(|error| SessionFileError::Io {
                path,
                message: error.to_string(),
            })?;
    }
    Ok(())
}

fn trim_ascii_line(line: &[u8]) -> &[u8] {
    let start = line
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(line.len());
    let end = line
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map_or(start, |index| index + 1);
    &line[start..end]
}

fn validate_last_line_is_complete_turn(path: &Path) -> Result<(), SessionFileError> {
    // Raw bytes, not `read_to_string`: a crash can cut a UTF-8 sequence in
    // half, and that must be reported as a repairable JSON tail rather than an
    // opaque I/O error.
    let contents = std::fs::read(path).map_err(|error| SessionFileError::Io {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    let mut lines = contents
        .split(|byte| *byte == b'\n')
        .map(trim_ascii_line)
        .filter(|line| !line.is_empty())
        .rev();
    let last_line = lines
        .next()
        .ok_or_else(|| SessionFileError::Empty(path.to_path_buf()))?;

    // Truncation gate: the physical last line must parse as JSON. A
    // half-written trailing line means the process was killed mid-append.
    let last_value = serde_json::from_slice::<serde_json::Value>(last_line).map_err(|error| {
        SessionFileError::LastLineNotJson {
            path: path.to_path_buf(),
            message: error.to_string(),
        }
    })?;

    // Anchor on the last `message`-typed record: pi may legitimately append
    // bookkeeping records after a completed turn (`compaction`,
    // `model_change`, `thinking_level_change`, ...), which are not messages
    // and must not fail validation. A file whose last message record is not a
    // completed assistant turn still fails, as does one with no message
    // records at all. `toolUse` is a completed assistant *message* but not a
    // completed turn — the next entries are its tool results — so resuming
    // there would leave a dangling tool call.
    let candidates = std::iter::once(last_value)
        .chain(lines.filter_map(|line| serde_json::from_slice::<serde_json::Value>(line).ok()));
    for value in candidates {
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
                .map(|reason| !reason.is_empty() && reason != "pending" && reason != "toolUse")
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
