use crate::error::PluginHostError;

pub const FORBIDDEN_LLM_ENV_PREFIXES: &[&str] = &[
    "XGOVERNOR_LLM_PROVIDER",
    "XGOVERNOR_LLM_MODEL",
    "XGOVERNOR_LLM_API_BASE",
    "XGOVERNOR_LLM_KEY_ENV",
];

#[derive(Debug, Clone)]
pub struct LlmPolicy {
    default_model: String,
}

impl LlmPolicy {
    pub fn new(default_model: impl Into<String>) -> Self {
        Self {
            default_model: default_model.into(),
        }
    }

    pub fn resolve(&self, explicit_override: Option<&str>) -> String {
        explicit_override
            .map(str::to_string)
            .unwrap_or_else(|| self.default_model.clone())
    }

    pub fn validate_env(&self, envs: &[(&str, &str)]) -> Result<(), PluginHostError> {
        for (key, _value) in envs {
            if FORBIDDEN_LLM_ENV_PREFIXES.contains(key) {
                return Err(PluginHostError::Manifest(format!(
                    "MCTS plugin environment must not contain LLM credential: {key}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_explicit_override_when_present() {
        let policy = LlmPolicy::new("default-mcts");
        assert_eq!(policy.resolve(Some("explicit-model")), "explicit-model");
        assert_eq!(policy.resolve(None), "default-mcts");
    }

    #[test]
    fn rejects_llm_credentials_in_plugin_env() {
        let policy = LlmPolicy::new("default-mcts");
        assert!(policy.validate_env(&[("MCTS_DEBUG", "1")]).is_ok());
        assert!(
            policy
                .validate_env(&[("XGOVERNOR_LLM_MODEL", "secret-model")])
                .is_err()
        );
    }
}
