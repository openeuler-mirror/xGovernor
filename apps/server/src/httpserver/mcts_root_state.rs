//! Native root-state provisioning for the xg `/mcts` endpoint.
//!
//! When a `/mcts` request omits `root_state_id`, the server shells out to the
//! MCTS repository's non-interactive `xg_root_state_prepare` CLI. That CLI
//! opens a zsoft sandbox, materializes the public task bundle, creates a
//! checkpoint and verifies it can be loaded. Its stdout is intentionally a
//! single JSON object so the host can parse it without scraping diagnostics.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use tokio::process::Command;

const ROOT_STATE_PREPARE_TIMEOUT: Duration = Duration::from_secs(1800);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedRootState {
    pub root_state_id: String,
    pub checkpoint_id: String,
    pub output_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
struct PreparedRootStateWire {
    root_state_id: String,
    #[serde(default)]
    checkpoint_id: Option<String>,
    #[serde(default)]
    output_dir: Option<String>,
}

pub(crate) async fn prepare_native_root_state(
    python_bin: &str,
    repo_root: &Path,
    base_url: &str,
    token: Option<&str>,
    mode: &str,
    task_id: &str,
    run_id: &str,
) -> Result<PreparedRootState, String> {
    let output_dir = std::env::temp_dir().join(format!("xg-mcts-root-{run_id}"));
    std::fs::create_dir_all(&output_dir)
        .map_err(|error| format!("failed to create root-state output dir: {error}"))?;
    let prepare_module = std::env::var("MCTS_ROOT_STATE_PREPARE_MODULE")
        .unwrap_or_else(|_| "src.harness.xg_root_state_prepare".to_string());

    let mut command = Command::new(python_bin);
    command
        .current_dir(repo_root)
        .arg("-m")
        .arg(&prepare_module)
        .arg("--mode")
        .arg(mode)
        .arg("--task-id")
        .arg(task_id)
        .arg("--base-url")
        .arg(base_url)
        .arg("--runtime-kind")
        .arg("pi")
        .arg("--output-dir")
        .arg(&output_dir);
    if let Some(token) = token {
        command.arg("--token").arg(token);
    }

    let output = tokio::time::timeout(ROOT_STATE_PREPARE_TIMEOUT, command.output())
        .await
        .map_err(|_| "root-state prepare CLI timed out".to_string())?
        .map_err(|error| format!("failed to launch root-state prepare CLI: {error}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(format!(
            "root-state prepare exited with {}: stdout={} stderr={}",
            output.status,
            stdout.trim(),
            stderr.trim()
        ));
    }

    parse_prepared_stdout(&String::from_utf8_lossy(&output.stdout)).map(|mut prepared| {
        prepared.output_dir = output_dir;
        prepared
    })
}

pub(crate) fn parse_prepared_stdout(stdout: &str) -> Result<PreparedRootState, String> {
    let raw: PreparedRootStateWire = serde_json::from_str(stdout.trim())
        .map_err(|error| format!("root-state prepare stdout is not valid JSON: {error}"))?;
    if raw.root_state_id.trim().is_empty() {
        return Err("root-state prepare returned an empty root_state_id".to_string());
    }
    let checkpoint_id = raw
        .checkpoint_id
        .unwrap_or_else(|| raw.root_state_id.clone());
    Ok(PreparedRootState {
        root_state_id: raw.root_state_id,
        checkpoint_id,
        output_dir: raw
            .output_dir
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_success_stdout() {
        let parsed = parse_prepared_stdout(
            r#"{"root_state_id":"ckpt-1","checkpoint_id":"ckpt-1","output_dir":"/tmp/out"}"#,
        )
        .unwrap();
        assert_eq!(parsed.root_state_id, "ckpt-1");
        assert_eq!(parsed.checkpoint_id, "ckpt-1");
        assert_eq!(parsed.output_dir, PathBuf::from("/tmp/out"));
    }

    #[test]
    fn checkpoint_id_defaults_to_root_state_id() {
        let parsed = parse_prepared_stdout(r#"{"root_state_id":"ckpt-1"}"#).unwrap();
        assert_eq!(parsed.checkpoint_id, "ckpt-1");
    }

    #[test]
    fn rejects_non_json_stdout() {
        assert!(parse_prepared_stdout("not-json").is_err());
    }

    #[test]
    fn rejects_empty_root_state_id() {
        assert!(parse_prepared_stdout(r#"{"root_state_id":""}"#).is_err());
    }
}
