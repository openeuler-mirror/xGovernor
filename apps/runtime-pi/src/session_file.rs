//! Pure helpers for locating and validating pi's own JSONL session files
//! inside a `--session-dir` directory, used by `PiRuntime::start()`'s
//! restoration path (`docs/pi_session_restore_plan.md` §1.3).
//!
//! `pi` persists each message as its `message_end` arrives, so a process
//! killed mid-turn can leave the newest file ending on that turn's user
//! message, a tool result, or a `pending` assistant message. The normal
//! restore path treats that as `pi_session_state_lost` rather than handing a
//! half-finished turn to a new process. This module's job is therefore to
//! confirm the latest file ends on a completed assistant turn before trusting
//! it enough to spawn `pi --session <path>`. The normal path never repairs or
//! rewrites a session file — a file that fails validation is reported, never
//! touched. [`truncate_to_last_complete_turn`] is the one explicit exception,
//! reserved for the forced-cancel restart path where this process itself
//! killed `pi` mid-turn.
//!
//! `pi` may append bookkeeping records after a completed turn — `compaction`
//! (context-compaction summaries), `model_change`, `thinking_level_change` —
//! none of which are `message` records. Validation therefore anchors on the
//! last `message`-typed record (which must be a completed assistant turn: a
//! non-empty `stopReason` other than `pending` or `toolUse`) and tolerates
//! trailing bookkeeping records after it; only a physically half-written
//! trailing line (failed JSON parse) is treated as truncation.

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
/// is a fully-formed, terminal assistant message (F14: pi writes an entire
/// turn atomically, so a valid last message record means the file holds no
/// half-written turn). Trailing non-`message` bookkeeping records that `pi`
/// appends after a completed turn — e.g. `compaction` summaries,
/// `model_change`, `thinking_level_change` — are tolerated after it. Returns
/// the file's path on success; otherwise a [`SessionFileError`] describing
/// exactly what did not hold.
pub(crate) fn latest_complete_turn_file(session_dir: &Path) -> Result<PathBuf, SessionFileError> {
    let latest = latest_session_file(session_dir)?;
    validate_last_line_is_complete_turn(&latest)?;
    Ok(latest)
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
    let contents = std::fs::read_to_string(&path).map_err(|error| SessionFileError::Io {
        path: path.clone(),
        message: error.to_string(),
    })?;

    let mut cut = 0usize;
    let mut offset = 0usize;
    for line in contents.split_inclusive('\n') {
        offset += line.len();
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
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

    // Truncation gate: the physical last line must parse as JSON. A
    // half-written trailing line means the process was killed mid-append.
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
    // completed assistant turn still fails, as does one with no message
    // records at all. `toolUse` is a completed assistant *message* but not a
    // completed turn — the next entries are its tool results — so resuming
    // there would leave a dangling tool call.
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

/// Maximum characters retained per exported user/assistant message. The host
/// only needs enough text to consolidate a task description, so capping each
/// message keeps the `/mcts` extraction payload bounded.
const MESSAGE_TEXT_LIMIT: usize = 8_000;

/// Reads the newest session `*.jsonl` and returns the most recent user and
/// assistant text messages (oldest first, at most `limit`). Unparseable or
/// trailing bookkeeping lines are skipped; a missing session directory or
/// zero session files is reported as-is.
pub(crate) fn recent_turn_texts(
    session_dir: &Path,
    limit: usize,
) -> Result<Vec<(String, String)>, SessionFileError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let path = latest_session_file(session_dir)?;
    let contents = std::fs::read_to_string(&path).map_err(|error| SessionFileError::Io {
        path: path.clone(),
        message: error.to_string(),
    })?;

    let mut collected: Vec<(String, String)> = Vec::new();
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
            continue;
        };
        if value.get("type").and_then(serde_json::Value::as_str) != Some("message") {
            continue;
        }
        let Some(message) = value.get("message") else {
            continue;
        };
        let Some(role) = message.get("role").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if role != "user" && role != "assistant" {
            continue;
        }
        let Some(text) = message_text(message.get("content")) else {
            continue;
        };
        let text = truncate_chars(&text, MESSAGE_TEXT_LIMIT);
        if text.trim().is_empty() {
            continue;
        }
        collected.push((role.to_string(), text));
    }

    if collected.len() > limit {
        collected.drain(..collected.len() - limit);
    }
    Ok(collected)
}

/// Decodes a `message.content` value into plain text. `pi` writes either a
/// plain string (`"hi"`) or an array of content blocks (`{"type":"text",
/// "text":"..."}`); tool-use blocks carry no prose and are ignored.
fn message_text(content: Option<&serde_json::Value>) -> Option<String> {
    let content = content?;
    match content {
        serde_json::Value::String(text) => {
            let text = text.trim();
            if text.is_empty() {
                None
            } else {
                Some(text.to_string())
            }
        }
        serde_json::Value::Array(blocks) => {
            let mut parts = Vec::new();
            for block in blocks {
                let text = block
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .or_else(|| block.get("content").and_then(serde_json::Value::as_str));
                if let Some(text) = text {
                    let text = text.trim();
                    if !text.is_empty() {
                        parts.push(text);
                    }
                }
            }
            if parts.is_empty() {
                None
            } else {
                Some(parts.join("\n"))
            }
        }
        _ => None,
    }
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        text.to_string()
    } else {
        text.chars().take(max_chars).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_recent_user_and_assistant_text_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.jsonl");
        let lines = [
            json!({"type":"message","message":{"role":"user","content":"first"}}),
            json!({"type":"message","message":{"role":"assistant","content":[]}}),
            json!({"type":"message","message":{"role":"user","content":[{"type":"text","text":"second"}]}}),
            json!({"type":"message","message":{"role":"assistant","content":[{"type":"text","text":"done"},{"type":"tool_use","call_id":"c","tool_name":"bash","input":{}}],"stopReason":"end_turn"}}),
        ];
        std::fs::write(&path, lines.into_iter().map(|v| v.to_string()).collect::<Vec<_>>().join("\n") + "\n").unwrap();

        let texts = recent_turn_texts(dir.path(), 2).unwrap();
        assert_eq!(texts.len(), 2);
        assert_eq!(texts[0], ("user".to_string(), "second".to_string()));
        assert_eq!(texts[1], ("assistant".to_string(), "done".to_string()));
    }
}
