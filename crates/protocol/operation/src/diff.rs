//! Pure, stateless line-diff statistics for file content changes.
//!
//! This module is a computation helper only: no state, no I/O, no notion of
//! tool-call ids or turn/session bookkeeping. It exists because computing a
//! meaningful line-diff requires the actual file bytes, and only whatever is
//! attached to a live [`crate::OperationBackend`] can read those (a remote
//! session client has no direct access to sandbox-side files). Whoever
//! performs a write via [`crate::capability::filesystem::OperationFileSystem`]
//! and wants to report a diff stat — e.g. to populate a `session-protocol`
//! `ToolActivity` event's `ext` bag under the well-known `"file_change"` key
//! — can call [`FileChangeDelta::from_contents`] directly.
//!
//! Deliberately excluded from this module: reading files, resolving
//! workspace-relative paths, and tracking per-call-id baselines across a
//! tool's Running -> Completed lifecycle. Those are bookkeeping concerns for
//! whatever orchestrates tool execution (the not-yet-built agent execution
//! layer), not for this protocol crate.

/// Line-level additions/deletions between two versions of a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChangeDelta {
    pub path: String,
    pub additions: u32,
    pub deletions: u32,
}

impl FileChangeDelta {
    /// Computes a delta from optional before/after content for `path`.
    /// Returns `None` when there is no effective change (identical content,
    /// or both sides absent) so callers can skip emitting a no-op event.
    pub fn from_contents(
        path: impl Into<String>,
        before: Option<&str>,
        after: Option<&str>,
    ) -> Option<Self> {
        let (additions, deletions) = match (before, after) {
            (Some(before), Some(after)) => line_change_counts(before, after),
            (None, Some(after)) => (text_line_count(after), 0),
            (Some(before), None) => (0, text_line_count(before)),
            (None, None) => (0, 0),
        };

        if additions == 0 && deletions == 0 {
            return None;
        }

        Some(Self {
            path: path.into(),
            additions,
            deletions,
        })
    }
}

/// Computes `(additions, deletions)` between two full file contents using a
/// line-level longest-common-subsequence diff. Falls back to a cheap
/// prefix/suffix comparison for very large files (where the LCS DP table
/// would be too large), at the cost of coarser stats in the middle of the
/// file.
pub fn line_change_counts(before: &str, after: &str) -> (u32, u32) {
    if before == after {
        return (0, 0);
    }

    let before_lines = text_lines(before);
    let after_lines = text_lines(after);
    if before_lines.is_empty() {
        return (after_lines.len() as u32, 0);
    }
    if after_lines.is_empty() {
        return (0, before_lines.len() as u32);
    }

    let cell_count = before_lines.len().saturating_mul(after_lines.len());
    let common = if cell_count > 20_000 {
        coarse_common_line_count(&before_lines, &after_lines)
    } else {
        lcs_line_count(&before_lines, &after_lines)
    };

    (
        after_lines.len().saturating_sub(common) as u32,
        before_lines.len().saturating_sub(common) as u32,
    )
}

fn lcs_line_count(before_lines: &[&str], after_lines: &[&str]) -> usize {
    let mut previous = vec![0usize; after_lines.len() + 1];
    let mut current = vec![0usize; after_lines.len() + 1];
    for before_line in before_lines {
        for (after_index, after_line) in after_lines.iter().enumerate() {
            current[after_index + 1] = if before_line == after_line {
                previous[after_index] + 1
            } else {
                current[after_index].max(previous[after_index + 1])
            };
        }
        std::mem::swap(&mut previous, &mut current);
        current.fill(0);
    }
    previous[after_lines.len()]
}

fn coarse_common_line_count(before_lines: &[&str], after_lines: &[&str]) -> usize {
    let mut prefix = 0usize;
    while prefix < before_lines.len().min(after_lines.len())
        && before_lines[prefix] == after_lines[prefix]
    {
        prefix += 1;
    }

    let mut suffix = 0usize;
    while suffix < before_lines.len().saturating_sub(prefix)
        && suffix < after_lines.len().saturating_sub(prefix)
        && before_lines[before_lines.len() - 1 - suffix]
            == after_lines[after_lines.len() - 1 - suffix]
    {
        suffix += 1;
    }
    prefix + suffix
}

fn text_line_count(text: &str) -> u32 {
    text_lines(text).len() as u32
}

fn text_lines(text: &str) -> Vec<&str> {
    if text.is_empty() {
        Vec::new()
    } else {
        text.lines().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_content_has_no_change() {
        assert_eq!(line_change_counts("a\nb\n", "a\nb\n"), (0, 0));
    }

    #[test]
    fn new_file_counts_all_lines_as_additions() {
        assert_eq!(line_change_counts("", "a\nb\nc"), (3, 0));
    }

    #[test]
    fn deleted_file_counts_all_lines_as_deletions() {
        assert_eq!(line_change_counts("a\nb\nc", ""), (0, 3));
    }

    #[test]
    fn single_line_replacement_counts_one_addition_and_one_deletion() {
        let before = "one\ntwo\nthree\nfour\nfive\n";
        let after = "one\ntwo\nTHREE\nfour\nfive\n";
        assert_eq!(line_change_counts(before, after), (1, 1));
    }

    #[test]
    fn large_file_falls_back_to_coarse_prefix_suffix_comparison() {
        // 200 x 200 lines = 40_000 cells, above the 20_000 LCS threshold.
        let before_lines: Vec<String> = (0..200).map(|i| format!("line-{i}")).collect();
        let mut after_lines = before_lines.clone();
        after_lines[100] = "changed".to_string();
        let before = before_lines.join("\n");
        let after = after_lines.join("\n");
        let (additions, deletions) = line_change_counts(&before, &after);
        assert_eq!(additions, 1);
        assert_eq!(deletions, 1);
    }

    #[test]
    fn from_contents_returns_none_for_no_op_change() {
        assert!(FileChangeDelta::from_contents("a.txt", Some("same"), Some("same")).is_none());
        assert!(FileChangeDelta::from_contents("a.txt", None, None).is_none());
    }

    #[test]
    fn from_contents_reports_new_file_as_pure_addition() {
        let delta = FileChangeDelta::from_contents("a.txt", None, Some("x\ny"))
            .expect("delta should be computed");
        assert_eq!(delta.path, "a.txt");
        assert_eq!(delta.additions, 2);
        assert_eq!(delta.deletions, 0);
    }

    #[test]
    fn from_contents_reports_deleted_file_as_pure_deletion() {
        let delta = FileChangeDelta::from_contents("a.txt", Some("x\ny\nz"), None)
            .expect("delta should be computed");
        assert_eq!(delta.additions, 0);
        assert_eq!(delta.deletions, 3);
    }
}
