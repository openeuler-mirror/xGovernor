use std::path::PathBuf;

use serde_json::Value;

use crate::error::PluginHostError;
use crate::manifest::PluginManifest;

pub struct ProfileRegistry {
    manifest: PluginManifest,
    repo_root: PathBuf,
}

impl ProfileRegistry {
    pub fn new(manifest: PluginManifest, repo_root: impl Into<PathBuf>) -> Self {
        Self {
            manifest,
            repo_root: repo_root.into(),
        }
    }

    pub fn resolve(&self, mode: &str, profile: &str, payload: &Value) -> Result<String, PluginHostError> {
        let path = self.manifest.resolve_profile_path(mode, profile, &self.repo_root)?;
        let template = std::fs::read_to_string(path)?;
        Ok(render_template(&template, payload))
    }

    pub fn template_path(&self, mode: &str, profile: &str) -> Result<PathBuf, PluginHostError> {
        self.manifest.resolve_profile_path(mode, profile, &self.repo_root)
    }
}

fn render_template(template: &str, payload: &Value) -> String {
    let Some(values) = payload.as_object() else {
        return template.to_string();
    };
    let mut rendered = template.to_string();
    for (key, value) in values {
        let placeholder = format!("{{{{{key}}}}}");
        let text = match value {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        rendered = rendered.replace(&placeholder, &text);
    }
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use tempfile::TempDir;

    fn manifest() -> PluginManifest {
        serde_json::from_value(json!({
            "plugin_id": "mcts",
            "resource_root": "testcases_union",
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
    fn resolves_profile_and_renders_payload() {
        let temp = TempDir::new().unwrap();
        let prompt_path = temp
            .path()
            .join("testcases_union/res/task_plugins/zsoft/prompts/prompt_solver_step.txt");
        fs::create_dir_all(prompt_path.parent().unwrap()).unwrap();
        fs::write(&prompt_path, "hello {{goal}}").unwrap();

        let registry = ProfileRegistry::new(manifest(), temp.path());
        let resolved = registry
            .resolve("zsoft", "solver-step", &json!({"goal": "fix bug"}))
            .unwrap();
        assert_eq!(resolved, "hello fix bug");
    }

    #[test]
    fn rejects_unknown_profile() {
        let temp = TempDir::new().unwrap();
        let registry = ProfileRegistry::new(manifest(), temp.path());
        assert!(registry.resolve("zsoft", "missing", &json!({})).is_err());
    }
}
