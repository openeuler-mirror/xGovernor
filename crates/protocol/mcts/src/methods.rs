use std::collections::BTreeSet;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ProtocolError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    Snapshot,
    Fork,
    AgentTurn,
    Inspect,
    Exec,
    FileRead,
    FileWrite,
    Diff,
    Release,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MethodName {
    #[serde(rename = "plugin.initialize")]
    PluginInitialize,
    #[serde(rename = "plugin.start")]
    PluginStart,
    #[serde(rename = "plugin.wait")]
    PluginWait,
    #[serde(rename = "plugin.cancel")]
    PluginCancel,
    #[serde(rename = "plugin.shutdown")]
    PluginShutdown,
    #[serde(rename = "host.snapshot")]
    HostSnapshot,
    #[serde(rename = "host.fork")]
    HostFork,
    #[serde(rename = "host.agent_turn")]
    HostAgentTurn,
    #[serde(rename = "host.inspect")]
    HostInspect,
    #[serde(rename = "host.exec")]
    HostExec,
    #[serde(rename = "host.file_read")]
    HostFileRead,
    #[serde(rename = "host.file_write")]
    HostFileWrite,
    #[serde(rename = "host.diff")]
    HostDiff,
    #[serde(rename = "host.release")]
    HostRelease,
}

impl FromStr for MethodName {
    type Err = ProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "plugin.initialize" => Ok(MethodName::PluginInitialize),
            "plugin.start" => Ok(MethodName::PluginStart),
            "plugin.wait" => Ok(MethodName::PluginWait),
            "plugin.cancel" => Ok(MethodName::PluginCancel),
            "plugin.shutdown" => Ok(MethodName::PluginShutdown),
            "host.snapshot" => Ok(MethodName::HostSnapshot),
            "host.fork" => Ok(MethodName::HostFork),
            "host.agent_turn" => Ok(MethodName::HostAgentTurn),
            "host.inspect" => Ok(MethodName::HostInspect),
            "host.exec" => Ok(MethodName::HostExec),
            "host.file_read" => Ok(MethodName::HostFileRead),
            "host.file_write" => Ok(MethodName::HostFileWrite),
            "host.diff" => Ok(MethodName::HostDiff),
            "host.release" => Ok(MethodName::HostRelease),
            _ => Err(ProtocolError::UnknownMethod(value.to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    pub max_iters: u64,
    pub branching: u64,
    pub max_time_ms: u64,
    pub max_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginInitializeParams {
    pub plugin_id: String,
    pub supported_api_versions: Vec<String>,
    pub host_version: String,
    pub available_capabilities: BTreeSet<Capability>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginStartParams {
    pub plugin_id: String,
    pub run_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub root_state_id: String,
    pub goal: String,
    pub content: String,
    pub budget: Budget,
    pub granted_capabilities: BTreeSet<Capability>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginWaitParams {
    pub plugin_id: String,
    pub run_id: String,
    pub after_sequence: u64,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostForkParams {
    pub plugin_id: String,
    pub run_id: String,
    pub timeout_ms: u64,
    pub parent_state_id: String,
    pub idempotency_key: String,
    #[serde(default)]
    pub offline: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostAgentTurnParams {
    pub plugin_id: String,
    pub run_id: String,
    pub timeout_ms: u64,
    pub branch_id: String,
    pub input: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestParams {
    PluginInitialize(PluginInitializeParams),
    PluginStart(PluginStartParams),
    PluginWait(PluginWaitParams),
    HostFork(HostForkParams),
    HostAgentTurn(HostAgentTurnParams),
    Generic(Value),
}

pub fn validate_request_params(method: &str, params: &Value) -> Result<(), ProtocolError> {
    match method {
        "plugin.initialize" => {
            serde_json::from_value::<PluginInitializeParams>(params.clone())?;
        }
        "plugin.start" => {
            serde_json::from_value::<PluginStartParams>(params.clone())?;
        }
        "plugin.wait" => {
            serde_json::from_value::<PluginWaitParams>(params.clone())?;
        }
        "host.fork" => {
            serde_json::from_value::<HostForkParams>(params.clone())?;
        }
        "host.agent_turn" => {
            serde_json::from_value::<HostAgentTurnParams>(params.clone())?;
        }
        _ => {}
    }
    Ok(())
}
