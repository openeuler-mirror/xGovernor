use std::collections::BTreeMap;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::completion::CompletionResult;
use crate::error::ProtocolError;
use crate::messages::Failure;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotificationName {
    #[serde(rename = "plugin.started")]
    PluginStarted,
    #[serde(rename = "plugin.progress")]
    PluginProgress,
    #[serde(rename = "plugin.completed")]
    PluginCompleted,
    #[serde(rename = "plugin.cancelled")]
    PluginCancelled,
    #[serde(rename = "plugin.failed")]
    PluginFailed,
    #[serde(rename = "host.job.started")]
    HostJobStarted,
    #[serde(rename = "host.job.output_delta")]
    HostJobOutputDelta,
    #[serde(rename = "host.job.tool_activity")]
    HostJobToolActivity,
    #[serde(rename = "host.job.completed")]
    HostJobCompleted,
    #[serde(rename = "host.job.failed")]
    HostJobFailed,
}

impl FromStr for NotificationName {
    type Err = ProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "plugin.started" => Ok(NotificationName::PluginStarted),
            "plugin.progress" => Ok(NotificationName::PluginProgress),
            "plugin.completed" => Ok(NotificationName::PluginCompleted),
            "plugin.cancelled" => Ok(NotificationName::PluginCancelled),
            "plugin.failed" => Ok(NotificationName::PluginFailed),
            "host.job.started" => Ok(NotificationName::HostJobStarted),
            "host.job.output_delta" => Ok(NotificationName::HostJobOutputDelta),
            "host.job.tool_activity" => Ok(NotificationName::HostJobToolActivity),
            "host.job.completed" => Ok(NotificationName::HostJobCompleted),
            "host.job.failed" => Ok(NotificationName::HostJobFailed),
            _ => Err(ProtocolError::UnknownNotification(value.to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginStartedParams {
    pub plugin_id: String,
    pub run_id: String,
    pub sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginProgressParams {
    pub plugin_id: String,
    pub run_id: String,
    pub sequence: u64,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expanded_nodes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_branches: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub best_candidate_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<BTreeMap<String, u64>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginCompletedParams {
    pub plugin_id: String,
    pub run_id: String,
    pub sequence: u64,
    pub result: CompletionResult,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginCancelledParams {
    pub plugin_id: String,
    pub run_id: String,
    pub sequence: u64,
    pub reason: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub findings: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_input: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginFailedParams {
    pub plugin_id: String,
    pub run_id: String,
    pub sequence: u64,
    pub failure: Failure,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub findings: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_input: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostJobStartedParams {
    pub run_id: String,
    pub job_id: String,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostJobOutputDeltaParams {
    pub run_id: String,
    pub job_id: String,
    pub sequence: u64,
    pub delta: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostJobToolActivityParams {
    pub run_id: String,
    pub job_id: String,
    pub sequence: u64,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostJobCompletedParams {
    pub run_id: String,
    pub job_id: String,
    pub sequence: u64,
    pub output: String,
    pub usage: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostJobFailedParams {
    pub run_id: String,
    pub job_id: String,
    pub sequence: u64,
    pub failure: Failure,
    pub usage: BTreeMap<String, u64>,
}

pub fn validate_notification_params(
    method: &str,
    params: &Value,
) -> Result<(), ProtocolError> {
    match method {
        "plugin.started" => {
            serde_json::from_value::<PluginStartedParams>(params.clone())?;
        }
        "plugin.progress" => {
            serde_json::from_value::<PluginProgressParams>(params.clone())?;
        }
        "plugin.completed" => {
            let value = serde_json::from_value::<PluginCompletedParams>(params.clone())?;
            value.result.validate()?;
        }
        "plugin.cancelled" => {
            serde_json::from_value::<PluginCancelledParams>(params.clone())?;
        }
        "plugin.failed" => {
            serde_json::from_value::<PluginFailedParams>(params.clone())?;
        }
        "host.job.started" => {
            serde_json::from_value::<HostJobStartedParams>(params.clone())?;
        }
        "host.job.output_delta" => {
            serde_json::from_value::<HostJobOutputDeltaParams>(params.clone())?;
        }
        "host.job.tool_activity" => {
            serde_json::from_value::<HostJobToolActivityParams>(params.clone())?;
        }
        "host.job.completed" => {
            serde_json::from_value::<HostJobCompletedParams>(params.clone())?;
        }
        "host.job.failed" => {
            serde_json::from_value::<HostJobFailedParams>(params.clone())?;
        }
        _ => {}
    }
    Ok(())
}
