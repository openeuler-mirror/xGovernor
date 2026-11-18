//! File configuration contains descriptors, never model credentials.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use orchestration_contract::execution::{
    BackendCapabilities, EnvironmentRef, NetworkPolicy, RootDescription, WorkspaceSource,
};
use orchestration_contract::run::{
    BackendKind, BackendSelection, CodeSource, RunError, RunErrorCode, StartRequest,
};
use serde::{Deserialize, Serialize};

use crate::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub runs_dir: PathBuf,
    pub algorithms_dir: PathBuf,
    #[serde(default = "default_python")]
    pub python_executable: PathBuf,
    #[serde(default)]
    pub swebench_python: Option<PathBuf>,
    pub models: BTreeMap<String, ModelDescriptor>,
    #[serde(default)]
    pub backends: Vec<BackendDescriptor>,
    #[serde(default = "default_tasks")]
    pub tasks: BTreeMap<String, TaskDescriptor>,
    #[serde(default = "default_lease")]
    pub lease_ttl_ms: u64,
    /// Maximum job and selected-result delivery call duration, clamped to the run deadline.
    #[serde(default = "default_job_timeout")]
    pub job_timeout_ms: u64,
    #[serde(default = "default_export_limit")]
    pub export_max_bytes: u64,
    /// Distribution root containing plugins/opencode and skills/mcts.
    pub package_directory: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDescriptor {
    pub provider: String,
    pub model: String,
    pub api_base: String,
    /// Name resolved by the host adapter, never passed to an algorithm process.
    pub credential_ref: String,
}

impl Configuration {
    /// Explicit path; no legacy environment or config.env fallback.
    pub fn load(path: &Path) -> Result<Self> {
        let mut config: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        let parent = std::fs::canonicalize(path)?
            .parent()
            .ok_or("configuration has no parent")?
            .to_path_buf();
        for entry in [&mut config.runs_dir, &mut config.algorithms_dir] {
            if entry.is_relative() {
                *entry = parent.join(&entry);
            }
        }
        if config.python_executable.is_relative()
            && config.python_executable.components().count() > 1
        {
            config.python_executable = parent.join(&config.python_executable);
        }
        if let Some(directory) = &mut config.package_directory {
            if directory.is_relative() {
                *directory = parent.join(&directory);
            }
        }
        if config.job_timeout_ms == 0 {
            return Err("job_timeout_ms must be positive".into());
        }
        if config.lease_ttl_ms < 300 {
            return Err("lease ttl must be at least 300ms".into());
        }
        Ok(config)
    }

    pub(crate) fn operation_timeout_ms(&self, method: &str, remaining_ms: u64) -> u64 {
        let limit = match method {
            "host.agent_turn" | "host.exec" => self.job_timeout_ms,
            "host.prepare_root" | "host.fork" | "host.snapshot" => 120_000,
            _ => 30_000,
        };
        remaining_ms.min(limit).max(1)
    }

    pub fn algorithm_entry(&self, namespace: &str) -> Result<PathBuf> {
        if namespace.is_empty()
            || !namespace
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        {
            return Err("invalid algorithm namespace".into());
        }
        Ok(self.algorithms_dir.join(namespace).join("__main__.py"))
    }
}

/// Actual provider/Agent capabilities supplied by the configured backend.
/// External backends must supply their own declaration; the host does not
/// invent guarantees on their behalf.
#[derive(Debug, Clone)]
pub struct BackendRegistration {
    pub kind: BackendKind,
    pub capabilities: BackendCapabilities,
}

pub fn select_backend(
    request: &StartRequest,
    backends: &[BackendRegistration],
) -> Result<BackendKind> {
    let selected = match request.config.backend {
        BackendSelection::Explicit(kind) => backends.iter().find(|b| b.kind == kind),
        BackendSelection::Auto => [
            BackendKind::Thinkprocess,
            BackendKind::E2b,
            BackendKind::Docker,
            BackendKind::Worktree,
        ]
        .iter()
        .find_map(|kind| backends.iter().find(|b| b.kind == *kind)),
    }
    .ok_or_else(|| {
        RunError::new(
            RunErrorCode::BackendUnavailable,
            "requested backend is not configured",
        )
    })?;
    let caps = &selected.capabilities;
    let needs = &request.config.requirements;
    if !needs.capabilities.is_subset(&caps.capabilities)
        || !caps
            .network_policies
            .contains(&request.config.network_policy)
        || !caps.isolation.satisfies(needs.isolation)
        || (needs.joint_snapshots && !caps.joint_snapshots)
        || (needs.independent_branches && !caps.independent_branches)
        || (needs.can_reset_history && !caps.can_reset_history)
    {
        return Err(RunError::new(
            RunErrorCode::CapabilityMismatch,
            "selected backend cannot satisfy run requirements",
        )
        .into());
    }
    Ok(selected.kind)
}

/// The common StartRequest already contains a required effective policy.
/// Human-facing callers use this resolver before constructing that request;
/// a supplied policy in the common request is the explicit override.
pub fn network_policy(
    source: &CodeSource,
    explicit: Option<NetworkPolicy>,
) -> Result<NetworkPolicy> {
    if let Some(policy) = explicit {
        return Ok(policy);
    }
    match source {
        CodeSource::Task { task, .. } => match task.as_str() {
            "swe_bench" | "swe-bench" | "sweverified" => Ok(NetworkPolicy::Offline),
            "deepswe" | "deep_swe" => Ok(NetworkPolicy::AllowInternet),
            _ => Err("task has no registered network default; specify a policy".into()),
        },
        _ => Err("code source requires an explicit network policy".into()),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendDescriptor {
    pub kind: BackendKind,
    pub driver: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub e2b: Option<E2bDescriptor>,
}

/// Connection settings for the own-backend assembly; contains no credential.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct E2bDescriptor {
    pub api_url: String,
    pub domain: String,
    pub envd_scheme: String,
    pub credential_ref: String,
    pub timeout_secs: u64,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_opencode")]
    pub opencode_executable: PathBuf,
    #[serde(default)]
    pub image_templates: BTreeMap<String, String>,
}

fn default_model() -> String {
    "solver".into()
}
fn default_opencode() -> PathBuf {
    "/usr/local/bin/opencode".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskDescriptor {
    /// Image naming pattern with {reference} replaced by the task instance ID.
    pub image: String,
    pub working_directory: String,
    pub default_network_policy: NetworkPolicy,
    #[serde(default)]
    pub instances: BTreeMap<String, GitWorkspace>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitWorkspace {
    pub repository: String,
    pub commit: String,
}

fn default_job_timeout() -> u64 {
    1_800_000
}

fn default_lease() -> u64 {
    30_000
}
fn default_python() -> PathBuf {
    "python3".into()
}
fn default_export_limit() -> u64 {
    256 * 1024 * 1024
}
fn default_tasks() -> BTreeMap<String, TaskDescriptor> {
    [
        (
            "swe_bench",
            "swebench/sweb.eval.x86_64.{reference}:latest",
            NetworkPolicy::Offline,
        ),
        (
            "deepswe",
            "deepswe/{reference}:latest",
            NetworkPolicy::AllowInternet,
        ),
    ]
    .into_iter()
    .map(|(name, image, policy)| {
        (
            name.into(),
            TaskDescriptor {
                image: image.into(),
                working_directory: "/testbed".into(),
                default_network_policy: policy,
                instances: BTreeMap::new(),
            },
        )
    })
    .collect()
}

impl Configuration {
    pub fn root_description(&self, request: &StartRequest) -> Result<RootDescription> {
        let (environment, workspace, working_directory) = match &request.code_source {
            CodeSource::Snapshot { state_id } => (
                EnvironmentRef::Template("existing-snapshot".into()),
                WorkspaceSource::Snapshot {
                    state_id: state_id.clone(),
                },
                "/testbed".into(),
            ),
            CodeSource::Task { task, reference } => {
                let task = self
                    .tasks
                    .get(task)
                    .ok_or("task description is not configured")?;
                let source = task
                    .instances
                    .get(reference)
                    .ok_or("task repository and immutable commit are not configured")?;
                if source.commit.len() != 40
                    || !source.commit.bytes().all(|b| b.is_ascii_hexdigit())
                {
                    return Err("task commit must be a full immutable Git SHA".into());
                }
                (
                    EnvironmentRef::Image(task.image.replace("{reference}", reference)),
                    WorkspaceSource::Git {
                        repository: source.repository.clone(),
                        commit: source.commit.clone(),
                    },
                    task.working_directory.clone(),
                )
            }
            CodeSource::WorkspacePackage { .. } => {
                return Err("workspace upload is unsupported".into())
            }
        };
        Ok(RootDescription {
            environment,
            workspace,
            working_directory,
            network_policy: request.config.network_policy,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_deadlines_obey_configuration_and_remaining_budget() {
        let mut config: Configuration = serde_json::from_value(serde_json::json!({
            "runs_dir": "runs", "algorithms_dir": "algorithms", "models": {}
        }))
        .unwrap();
        assert_eq!(
            config.operation_timeout_ms("host.agent_turn", 3_600_000),
            1_800_000
        );
        config.job_timeout_ms = 2_400_000;
        assert_eq!(
            config.operation_timeout_ms("host.exec", 3_600_000),
            2_400_000
        );
        assert_eq!(
            config.operation_timeout_ms("host.agent_turn", 900_000),
            900_000
        );
    }
}
