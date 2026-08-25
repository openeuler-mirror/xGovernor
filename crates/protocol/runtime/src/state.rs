use crate::RuntimeError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Runtime-owned state quarantined behind a runtime kind and schema version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeStateSnapshot {
    pub runtime_kind: String,
    pub schema_version: u32,
    #[serde(default)]
    pub state: Value,
}

impl RuntimeStateSnapshot {
    pub fn try_new<T: Serialize>(
        runtime_kind: impl Into<String>,
        schema_version: u32,
        state: &T,
    ) -> Result<Self, RuntimeError> {
        let state = serde_json::to_value(state).map_err(|error| RuntimeError::StateCorrupt {
            message: format!("runtime state could not be serialized: {error}"),
        })?;
        Ok(Self {
            runtime_kind: runtime_kind.into(),
            schema_version,
            state,
        })
    }

    pub fn validate(
        &self,
        expected_runtime_kind: &str,
        supported_schema_version: u32,
    ) -> Result<(), RuntimeError> {
        if self.runtime_kind != expected_runtime_kind {
            return Err(RuntimeError::StateCorrupt {
                message: format!(
                    "runtime state belongs to '{}' instead of '{}'",
                    self.runtime_kind, expected_runtime_kind
                ),
            });
        }
        if self.schema_version != supported_schema_version {
            return Err(RuntimeError::StateCorrupt {
                message: format!(
                    "unsupported {expected_runtime_kind} state schema version {}; expected {}",
                    self.schema_version, supported_schema_version
                ),
            });
        }
        Ok(())
    }

    pub fn decode<T: DeserializeOwned>(
        &self,
        expected_runtime_kind: &str,
        supported_schema_version: u32,
    ) -> Result<T, RuntimeError> {
        self.validate(expected_runtime_kind, supported_schema_version)?;
        serde_json::from_value(self.state.clone()).map_err(|error| RuntimeError::StateCorrupt {
            message: format!("invalid {expected_runtime_kind} runtime state: {error}"),
        })
    }
}
