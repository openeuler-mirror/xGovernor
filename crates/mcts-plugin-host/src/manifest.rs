use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::PluginHostError;

pub const PLUGIN_ID: &str = "mcts";
pub const PROFILE_OWNER: &str = "xgovernor";
pub const TASK_PLUGIN_PREFIX: &str = "res/task_plugins/";

fn default_true() -> bool {
    true
}

fn ensure_task_plugins_path(value: &str) -> Result<String, PluginHostError> {
    let normalized = value.replace('\\', "/");
    let path = Path::new(&normalized);
    if path.is_absolute() || normalized.starts_with('/') {
        return Err(PluginHostError::Manifest(format!(
            "template path must be relative: {value}"
        )));
    }
    if path.components().any(|component| {
        matches!(component, Component::ParentDir | Component::RootDir | Component::Prefix(_))
    }) {
        return Err(PluginHostError::Manifest(format!(
            "template path must not contain '..': {value}"
        )));
    }
    if !normalized.starts_with(TASK_PLUGIN_PREFIX) {
        return Err(PluginHostError::Manifest(format!(
            "template path must stay under {TASK_PLUGIN_PREFIX}: {value}"
        )));
    }
    Ok(normalized)
}

fn ensure_relative_resource_root(value: &str) -> Result<String, PluginHostError> {
    let normalized = value.replace('\\', "/");
    let path = Path::new(&normalized);
    if path.is_absolute() || normalized.starts_with('/') {
        return Err(PluginHostError::Manifest(
            "resource_root must be relative".to_string(),
        ));
    }
    if path.components().any(|component| {
        matches!(component, Component::ParentDir | Component::RootDir | Component::Prefix(_))
    }) {
        return Err(PluginHostError::Manifest(
            "resource_root must not contain '..'".to_string(),
        ));
    }
    Ok(normalized)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginManifest {
    pub plugin_id: String,
    pub resource_root: String,
    pub modes: BTreeMap<String, ModeManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModeManifest {
    pub task_plugin_root: String,
    pub profiles: BTreeMap<String, ProfileManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileManifest {
    pub role: String,
    pub owner: String,
    pub source: String,
    #[serde(default = "default_true")]
    pub tools_enabled: bool,
}

impl PluginManifest {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, PluginHostError> {
        let text = std::fs::read_to_string(path)?;
        let manifest: PluginManifest = serde_json::from_str(&text)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), PluginHostError> {
        if self.plugin_id != PLUGIN_ID {
            return Err(PluginHostError::Manifest(format!(
                "plugin_id must be {PLUGIN_ID}"
            )));
        }
        ensure_relative_resource_root(&self.resource_root)?;

        for (mode_id, mode) in &self.modes {
            ensure_task_plugins_path(&mode.task_plugin_root)?;
            for (profile_id, profile) in &mode.profiles {
                if profile.owner != PROFILE_OWNER {
                    return Err(PluginHostError::Manifest(format!(
                        "profile {mode_id}.{profile_id} must be owned by {PROFILE_OWNER}"
                    )));
                }
                if profile.role.trim().is_empty() {
                    return Err(PluginHostError::Manifest(format!(
                        "profile {mode_id}.{profile_id} needs a role"
                    )));
                }
                ensure_task_plugins_path(&profile.source)?;
            }
        }
        Ok(())
    }

    pub fn mode(&self, mode: &str) -> Result<&ModeManifest, PluginHostError> {
        self.modes.get(mode).ok_or_else(|| {
            PluginHostError::Manifest(format!("unknown MCTS mode: {mode}"))
        })
    }

    pub fn profile(&self, mode: &str, profile: &str) -> Result<&ProfileManifest, PluginHostError> {
        let mode_config = self.mode(mode)?;
        mode_config.profiles.get(profile).ok_or_else(|| {
            PluginHostError::Manifest(format!("unknown profile {profile} for mode {mode}"))
        })
    }

    pub fn resolve_profile_path(
        &self,
        mode: &str,
        profile: &str,
        repo_root: &Path,
    ) -> Result<PathBuf, PluginHostError> {
        let profile = self.profile(mode, profile)?;
        let resource_root = ensure_relative_resource_root(&self.resource_root)?;
        let source = ensure_task_plugins_path(&profile.source)?;
        Ok(repo_root.join(resource_root).join(source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_manifest() -> PluginManifest {
        serde_json::from_value(serde_json::json!({
            "plugin_id": "mcts",
            "resource_root": "src",
            "modes": {
                "zsoft": {
                    "task_plugin_root": "res/task_plugins/zsoft",
                    "profiles": {
                        "solver-step": {
                            "role": "solver_step",
                            "owner": "xgovernor",
                            "source": "res/task_plugins/zsoft/prompts/prompt_solver_step.txt"
                        }
                    }
                }
            }
        }))
        .unwrap()
    }

    #[test]
    fn accepts_valid_manifest() {
        valid_manifest().validate().unwrap();
    }

    #[test]
    fn rejects_absolute_source() {
        let mut manifest = valid_manifest();
        manifest.modes.get_mut("zsoft").unwrap()
            .profiles.get_mut("solver-step").unwrap().source = "/etc/passwd".into();
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn rejects_parent_traversal_source() {
        let mut manifest = valid_manifest();
        manifest.modes.get_mut("zsoft").unwrap()
            .profiles.get_mut("solver-step").unwrap().source = "../prompts.txt".into();
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn rejects_plugin_owned_profile() {
        let mut manifest = valid_manifest();
        manifest.modes.get_mut("zsoft").unwrap()
            .profiles.get_mut("solver-step").unwrap().owner = "plugin".into();
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn defaults_tools_enabled_to_true() {
        assert!(valid_manifest().profile("zsoft", "solver-step").unwrap().tools_enabled);
    }

    #[test]
    fn accepts_explicit_tools_disabled() {
        let manifest: PluginManifest = serde_json::from_value(serde_json::json!({
            "plugin_id": "mcts",
            "resource_root": "src",
            "modes": {
                "zsoft": {
                    "task_plugin_root": "res/task_plugins/zsoft",
                    "profiles": {
                        "memory-merge": {
                            "role": "memory_merge",
                            "owner": "xgovernor",
                            "source": "res/task_plugins/common/prompts/prompt_memory_merge.txt",
                            "tools_enabled": false
                        }
                    }
                }
            }
        }))
        .unwrap();
        assert!(!manifest.profile("zsoft", "memory-merge").unwrap().tools_enabled);
    }
}
