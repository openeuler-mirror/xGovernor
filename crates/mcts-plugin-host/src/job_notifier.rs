//! Builds host.job.* notifications sent from xg back to the MCTS plugin.
//!
//! The plugin polls a local job registry that is populated exclusively by
//! these notifications, so completed/failed events must carry the terminal
//! usage map used for token accounting.

use std::collections::BTreeMap;

use mcts_protocol::messages::{JSONRPC_VERSION, Notification, RpcEnvelope};
use mcts_protocol::notifications::NotificationName;
use mcts_protocol::Failure;
use serde_json::{json, Value};

pub struct JobNotifier;

impl JobNotifier {
    pub fn started(run_id: &str, job_id: &str, sequence: u64) -> RpcEnvelope {
        notification(
            NotificationName::HostJobStarted,
            json!({ "run_id": run_id, "job_id": job_id, "sequence": sequence }),
        )
    }

    pub fn output_delta(
        run_id: &str,
        job_id: &str,
        sequence: u64,
        delta: &str,
        stream: Option<&str>,
    ) -> RpcEnvelope {
        let mut params = serde_json::Map::new();
        params.insert("run_id".into(), json!(run_id));
        params.insert("job_id".into(), json!(job_id));
        params.insert("sequence".into(), json!(sequence));
        params.insert("delta".into(), json!(delta));
        if let Some(stream) = stream {
            params.insert("stream".into(), json!(stream));
        }
        notification(NotificationName::HostJobOutputDelta, Value::Object(params))
    }

    pub fn tool_activity(
        run_id: &str,
        job_id: &str,
        sequence: u64,
        summary: &str,
    ) -> RpcEnvelope {
        notification(
            NotificationName::HostJobToolActivity,
            json!({ "run_id": run_id, "job_id": job_id, "sequence": sequence, "summary": summary }),
        )
    }

    pub fn completed(
        run_id: &str,
        job_id: &str,
        sequence: u64,
        output: &str,
        usage: BTreeMap<String, u64>,
        exit_code: Option<i64>,
        truncated: Option<bool>,
    ) -> RpcEnvelope {
        let mut params = serde_json::Map::new();
        params.insert("run_id".into(), json!(run_id));
        params.insert("job_id".into(), json!(job_id));
        params.insert("sequence".into(), json!(sequence));
        params.insert("output".into(), json!(output));
        params.insert("usage".into(), serde_json::to_value(usage).expect("usage serializes"));
        if let Some(exit_code) = exit_code {
            params.insert("exit_code".into(), json!(exit_code));
        }
        if let Some(truncated) = truncated {
            params.insert("truncated".into(), json!(truncated));
        }
        notification(NotificationName::HostJobCompleted, Value::Object(params))
    }

    pub fn failed(
        run_id: &str,
        job_id: &str,
        sequence: u64,
        failure: Failure,
        usage: BTreeMap<String, u64>,
    ) -> RpcEnvelope {
        notification(
            NotificationName::HostJobFailed,
            json!({
                "run_id": run_id,
                "job_id": job_id,
                "sequence": sequence,
                "failure": {"code": failure.code, "message": failure.message},
                "usage": usage,
            }),
        )
    }
}

/// Zero-token usage map for the synchronous mock host.
pub fn empty_usage() -> BTreeMap<String, u64> {
    BTreeMap::from([("total_tokens".to_string(), 0u64)])
}

fn notification(method: NotificationName, params: Value) -> RpcEnvelope {
    RpcEnvelope::Notification(Notification {
        jsonrpc: JSONRPC_VERSION.to_string(),
        method,
        params,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcts_protocol::notifications::{
        HostJobCompletedParams, HostJobFailedParams, HostJobOutputDeltaParams,
        HostJobStartedParams, HostJobToolActivityParams,
    };

    #[test]
    fn host_job_notifications_started_roundtrips() {
        let envelope = JobNotifier::started("run-1", "job-1", 1);
        let RpcEnvelope::Notification(notification) = envelope else {
            panic!("expected notification");
        };
        assert_eq!(notification.method, NotificationName::HostJobStarted);
        let params: HostJobStartedParams = serde_json::from_value(notification.params).unwrap();
        assert_eq!(params.run_id, "run-1");
        assert_eq!(params.job_id, "job-1");
        assert_eq!(params.sequence, 1);
    }

    #[test]
    fn host_job_notifications_completed_carries_usage() {
        let usage = BTreeMap::from([("total_tokens".to_string(), 42u64)]);
        let envelope = JobNotifier::completed("run-1", "job-1", 2, "out", usage, Some(0), Some(false));
        let RpcEnvelope::Notification(notification) = envelope else {
            panic!("expected notification");
        };
        assert_eq!(notification.method, NotificationName::HostJobCompleted);
        let params: HostJobCompletedParams = serde_json::from_value(notification.params).unwrap();
        assert_eq!(params.usage.get("total_tokens"), Some(&42));
        assert_eq!(params.output, "out");
    }

    #[test]
    fn host_job_notifications_failed_carries_usage() {
        let failure = Failure { code: "exec-failed".into(), message: "boom".into() };
        let envelope = JobNotifier::failed(
            "run-1",
            "job-1",
            3,
            failure,
            BTreeMap::from([("total_tokens".to_string(), 7u64)]),
        );
        let RpcEnvelope::Notification(notification) = envelope else {
            panic!("expected notification");
        };
        assert_eq!(notification.method, NotificationName::HostJobFailed);
        let params: HostJobFailedParams = serde_json::from_value(notification.params).unwrap();
        assert_eq!(params.failure.code, "exec-failed");
        assert_eq!(params.usage.get("total_tokens"), Some(&7));
    }

    #[test]
    fn host_job_notifications_output_delta_roundtrips() {
        let envelope = JobNotifier::output_delta("run-1", "job-1", 2, "hello", Some("stdout"));
        let RpcEnvelope::Notification(notification) = envelope else {
            panic!("expected notification");
        };
        assert_eq!(notification.method, NotificationName::HostJobOutputDelta);
        let params: HostJobOutputDeltaParams = serde_json::from_value(notification.params).unwrap();
        assert_eq!(params.delta, "hello");
        assert_eq!(params.stream.as_deref(), Some("stdout"));
    }

    #[test]
    fn host_job_notifications_tool_activity_roundtrips() {
        let envelope = JobNotifier::tool_activity("run-1", "job-1", 2, "ran pytest");
        let RpcEnvelope::Notification(notification) = envelope else {
            panic!("expected notification");
        };
        assert_eq!(notification.method, NotificationName::HostJobToolActivity);
        let params: HostJobToolActivityParams = serde_json::from_value(notification.params).unwrap();
        assert_eq!(params.summary, "ran pytest");
    }
}
