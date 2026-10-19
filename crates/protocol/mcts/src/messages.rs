use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::error::ProtocolError;
use crate::methods::{validate_request_params, MethodName};
use crate::notifications::{validate_notification_params, NotificationName};

pub const JSONRPC_VERSION: &str = "2.0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum RpcId {
    String(String),
    Number(i64),
}

impl<'de> Deserialize<'de> for RpcId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::String(value) if !value.is_empty() => Ok(RpcId::String(value)),
            Value::Number(value) if value.as_i64().is_some() => {
                Ok(RpcId::Number(value.as_i64().unwrap()))
            }
            _ => Err(serde::de::Error::custom(
                "request id must be a non-empty string or integer",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Request {
    pub jsonrpc: String,
    pub id: RpcId,
    pub method: MethodName,
    pub params: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRequest {
    jsonrpc: String,
    id: RpcId,
    method: String,
    params: Value,
}

impl<'de> Deserialize<'de> for Request {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawRequest::deserialize(deserializer)?;
        if raw.jsonrpc != JSONRPC_VERSION {
            return Err(serde::de::Error::custom("jsonrpc must be '2.0'"));
        }
        let method = MethodName::from_str(&raw.method)
            .map_err(|_| serde::de::Error::custom("unsupported request method"))?;
        validate_request_params(&raw.method, &raw.params)
            .map_err(serde::de::Error::custom)?;
        Ok(Request {
            jsonrpc: raw.jsonrpc,
            id: raw.id,
            method,
            params: raw.params,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Notification {
    pub jsonrpc: String,
    pub method: NotificationName,
    pub params: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawNotification {
    jsonrpc: String,
    method: String,
    params: Value,
}

impl<'de> Deserialize<'de> for Notification {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawNotification::deserialize(deserializer)?;
        if raw.jsonrpc != JSONRPC_VERSION {
            return Err(serde::de::Error::custom("jsonrpc must be '2.0'"));
        }
        let method = NotificationName::from_str(&raw.method)
            .map_err(|_| serde::de::Error::custom("unsupported notification method"))?;
        validate_notification_params(&raw.method, &raw.params)
            .map_err(serde::de::Error::custom)?;
        Ok(Notification {
            jsonrpc: raw.jsonrpc,
            method,
            params: raw.params,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Failure {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub jsonrpc: String,
    pub id: RpcId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RpcEnvelope {
    Request(Request),
    Notification(Notification),
    Response(Response),
}

impl fmt::Display for MethodName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = serde_json::to_value(self).map_err(|_| fmt::Error)?;
        write!(f, "{}", value.as_str().unwrap_or_default())
    }
}

impl From<ProtocolError> for serde_json::Error {
    fn from(value: ProtocolError) -> Self {
        serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            value.to_string(),
        ))
    }
}
