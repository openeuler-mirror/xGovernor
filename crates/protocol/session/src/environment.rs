use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SessionSandboxCapability {
    Exec,
    FileRead,
    FileWrite,
    Pause,
    Snapshot,
    Network,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SessionRuntimeCapability {
    Interaction,
    Steering,
    Fork,
    Checkpoint,
    StateExport,
    ModelOverride,
    ReasoningControl,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SessionCapabilities {
    #[serde(default, with = "sandbox_capability_set")]
    pub sandbox: BTreeSet<SessionSandboxCapability>,
    #[serde(default, with = "runtime_capability_set")]
    pub runtime: BTreeSet<SessionRuntimeCapability>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SessionCapabilityRequest {
    #[serde(default)]
    pub sandbox: BTreeSet<SessionSandboxCapability>,
    #[serde(default)]
    pub runtime: BTreeSet<SessionRuntimeCapability>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkspaceSpec {
    #[default]
    DaemonDefault,
    LocalPath {
        path: String,
    },
    Git {
        url: String,
        #[serde(default)]
        reference: Option<String>,
        #[serde(default)]
        subdirectory: Option<String>,
    },
    Shared {
        workspace_ref: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DeploymentProfile {
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub resource_class: Option<String>,
    #[serde(default)]
    pub options: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceAccessMode {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceState {
    pub workspace_id: String,
    pub root: String,
    pub access: WorkspaceAccessMode,
    #[serde(default)]
    pub revision: Option<String>,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationBoundary {
    Host,
    Process,
    Container,
    VirtualMachine,
    Remote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkIsolation {
    None,
    Restricted,
    Isolated,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IsolationState {
    pub boundary: IsolationBoundary,
    pub workspace_access: WorkspaceAccessMode,
    pub network: NetworkIsolation,
    #[serde(default, with = "sandbox_capability_set")]
    pub effective_capabilities: BTreeSet<SessionSandboxCapability>,
    #[serde(default)]
    pub metadata: Value,
}

macro_rules! tolerant_capability_set {
    ($module:ident, $capability:ty) => {
        mod $module {
            use super::*;

            pub fn serialize<S>(
                capabilities: &BTreeSet<$capability>,
                serializer: S,
            ) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let mut sequence = serializer.serialize_seq(Some(capabilities.len()))?;
                for capability in capabilities {
                    sequence.serialize_element(capability)?;
                }
                sequence.end()
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<BTreeSet<$capability>, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct CapabilityVisitor;

                impl<'de> Visitor<'de> for CapabilityVisitor {
                    type Value = BTreeSet<$capability>;

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str("a sequence of capability names")
                    }

                    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
                    where
                        A: SeqAccess<'de>,
                    {
                        let mut capabilities = BTreeSet::new();
                        while let Some(value) = sequence.next_element::<Value>()? {
                            if let Ok(capability) = serde_json::from_value::<$capability>(value) {
                                capabilities.insert(capability);
                            }
                        }
                        Ok(capabilities)
                    }
                }

                deserializer.deserialize_seq(CapabilityVisitor)
            }
        }
    };
}

tolerant_capability_set!(sandbox_capability_set, SessionSandboxCapability);
tolerant_capability_set!(runtime_capability_set, SessionRuntimeCapability);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unknown_response_capabilities_are_treated_as_absent() {
        let capabilities: SessionCapabilities = serde_json::from_value(json!({
            "sandbox": ["exec", "future_sandbox_capability"],
            "runtime": ["fork", "future_runtime_capability"]
        }))
        .unwrap();
        assert_eq!(
            capabilities.sandbox,
            [SessionSandboxCapability::Exec].into_iter().collect()
        );
        assert_eq!(
            capabilities.runtime,
            [SessionRuntimeCapability::Fork].into_iter().collect()
        );
    }

    #[test]
    fn unknown_requested_capability_is_rejected() {
        assert!(serde_json::from_value::<SessionCapabilityRequest>(json!({
            "runtime": ["future_runtime_capability"]
        }))
        .is_err());
    }
}

/// Request-only LLM configuration. `api_key` must be consumed during request
/// handling and must never be projected into a descriptor or persistent state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct LlmOverrideRequest {
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub api_base: Option<String>,
    #[serde(default)]
    pub api_key_env: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
}

/// Safe response/persistence projection: a key value cannot be represented.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedLlmDescriptor {
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub api_base: Option<String>,
    pub credential_source: String,
}
