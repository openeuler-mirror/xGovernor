#![cfg(unix)]

#[path = "../examples/support/lifecycle.rs"]
mod lifecycle;

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::{json, Value};
use tempfile::TempDir;
use xgovernor_thinkthread_client::{sdk, ManagementConfig};

struct Fixture {
    root: TempDir,
    config: ManagementConfig,
}

impl Fixture {
    fn new(scenario: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let host = root.path().join("management-host");
        std::fs::write(&host, include_bytes!("support/lifecycle_host.py")).unwrap();
        std::fs::set_permissions(&host, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = ManagementConfig {
            host_executable: host,
            state_root: root.path().join("state"),
            runtime_root: root.path().join("runtime"),
            config_root: root.path().join("config"),
            cache_root: root.path().join("cache"),
            working_directory: root.path().join("work"),
            fs_root: root.path().join("fs"),
            home_directory: root.path().join("home"),
            executable_search_path: vec![PathBuf::from("/usr/bin")],
            base_environment: BTreeMap::from([
                ("XG_TEST_SCENARIO".into(), scenario.into()),
                (
                    "XG_TEST_TRACE".into(),
                    root.path().join("trace").to_str().unwrap().into(),
                ),
                ("MODEL_API_KEY".into(), "synthetic-private-key-value".into()),
            ]),
            connect_timeout: Duration::from_secs(2),
        };
        Self { root, config }
    }
    fn create(&self) -> sdk::ThinkThreadCreateRequest {
        serde_json::from_value(json!({
            "agent": {"kind":"profile","name":"fixture"},
            "fs": self.config.fs_root,
        }))
        .unwrap()
    }
    fn trace(&self) -> Vec<Value> {
        std::fs::read_to_string(self.root.path().join("trace"))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
    async fn run(&self) -> (Value, Vec<Value>) {
        let mut journal = Vec::new();
        let report = lifecycle::bounded_probe(&self.config, self.create(), 1000, &mut |v| {
            journal.push(v.clone());
            Ok(())
        })
        .await;
        (report, journal)
    }
}

#[tokio::test]
async fn runs_two_typed_turns_and_only_removes_the_created_root() {
    let f = Fixture::new("ok");
    let (report, journal) = f.run().await;
    assert_eq!(report["status"], "passed_lifecycle_subset");
    assert_eq!(report["cleanup"], "owned_root_absent");
    assert_eq!(report["scope"], "lifecycle_only");
    let trace = f.trace();
    let methods: Vec<_> = trace
        .iter()
        .map(|v| v["method"].as_str().unwrap())
        .collect();
    assert_eq!(
        methods
            .iter()
            .filter(|m| **m == "thinkthread.start")
            .count(),
        2
    );
    assert_eq!(methods.iter().filter(|m| **m == "message.send").count(), 2);
    assert_eq!(
        methods.iter().filter(|m| **m == "thinkthread.stop").count(),
        2
    );
    assert_eq!(
        methods
            .iter()
            .filter(|m| **m == "thinkthread.remove")
            .count(),
        1
    );
    assert!(!methods
        .iter()
        .any(|m| m.contains("list") || m.contains("force")));
    for row in trace
        .iter()
        .filter(|v| v["params"]["operationId"].is_string())
    {
        let id = row["params"]["operationId"].as_str().unwrap();
        assert!(journal.iter().any(|snap| {
            snap["operations"]
                .as_array()
                .unwrap()
                .iter()
                .any(|op| op["operation_id"] == id && op["delivery"] == "pending")
        }));
        if row["method"] == "thinkthread.stop" || row["method"] == "thinkthread.remove" {
            assert_eq!(
                row["params"]["thinkthreadIds"],
                json!(["fixture-owned-root"])
            );
            assert_eq!(row["params"]["force"], false);
        }
    }
}

#[tokio::test]
async fn truncated_reply_is_a_failure_and_is_never_scored_as_complete() {
    let f = Fixture::new("truncated");
    let (r, _) = f.run().await;
    assert_eq!(r["status"], "failed");
    assert_eq!(
        r["check_failure"],
        "message_not_complete_exact_untruncated_reply"
    );
    assert!(!f
        .trace()
        .iter()
        .any(|v| v["method"] == "thinkthread.remove"));
}

#[tokio::test]
async fn foreign_terminal_cannot_finish_the_owned_message() {
    let (r, _) = Fixture::new("foreign-terminal").run().await;
    assert_eq!(r["check_failure"], "message_identity_mismatch");
}

#[tokio::test]
async fn queued_wait_timeout_is_not_a_completed_turn() {
    let (r, _) = Fixture::new("queued-timeout").run().await;
    assert_eq!(
        r["check_failure"],
        "message_not_complete_exact_untruncated_reply"
    );
    assert_eq!(r["status"], "failed");
}

#[tokio::test]
async fn repeated_message_identity_is_rejected() {
    let (r, _) = Fixture::new("duplicate-message").run().await;
    assert_eq!(
        r["check_failure"],
        "message_admission_truncated_or_duplicate"
    );
}

#[tokio::test]
async fn partial_batch_failure_does_not_authorize_remove() {
    let f = Fixture::new("partial-stop");
    let (r, _) = f.run().await;
    assert_eq!(r["cleanup_failure"], "stop_batch_target_not_proven");
    assert_eq!(r["status"], "failed");
    assert!(!f
        .trace()
        .iter()
        .any(|v| v["method"] == "thinkthread.remove"));
}

#[tokio::test]
async fn deletion_requires_absence_in_a_separate_public_query() {
    let (r, _) = Fixture::new("not-absent").run().await;
    assert_eq!(r["cleanup_failure"], "remove_not_absent");
    assert_ne!(r["cleanup"], "owned_root_absent");
}

#[tokio::test]
async fn unknown_create_is_journaled_and_never_retried_or_globally_discovered() {
    let f = Fixture::new("drop-create");
    let (r, journal) = f.run().await;
    assert_eq!(r["failure"]["delivery"], "sent_unknown");
    assert!(r.get("owned_root").is_none());
    assert_eq!(
        f.trace()
            .iter()
            .filter(|v| v["method"] == "thinkthread.create")
            .count(),
        1
    );
    assert_eq!(r["operations"][0]["delivery"], "pending");
    assert_eq!(
        r["operations"][0]["operation_id"],
        journal[0]["operations"][0]["operation_id"]
    );
}

#[tokio::test]
async fn incomplete_or_recovered_root_is_not_a_ready_proof() {
    let (r, _) = Fixture::new("needs-recovery").run().await;
    assert_eq!(
        r["check_failure"],
        "native_root_identity_state_or_recovery_mismatch"
    );
}

#[tokio::test]
async fn reply_content_and_environment_credentials_are_not_projected() {
    let (r, journal) = Fixture::new("secret-reply").run().await;
    let output = serde_json::to_string(&(r, journal)).unwrap();
    assert!(!output.contains("synthetic-private-key-value"));
    assert!(!output.contains("Reply with exactly"));
    assert!(!output.contains("replyText"));
}

#[tokio::test]
async fn durable_terminal_read_must_match_the_waited_projection() {
    let (r, _) = Fixture::new("changed-terminal").run().await;
    assert_eq!(r["check_failure"], "terminal_projection_changed");
}

#[tokio::test]
async fn journal_failure_prevents_the_next_mutation() {
    let f = Fixture::new("ok");
    let r =
        lifecycle::bounded_probe(&f.config, f.create(), 1000, &mut |_| Err("journal_write")).await;
    assert_eq!(r["check_failure"], "journal_write");
    assert!(!f
        .trace()
        .iter()
        .any(|v| v["method"] == "thinkthread.create"));
}

#[tokio::test]
async fn invalid_budget_blocks_before_host_start() {
    let f = Fixture::new("ok");
    let r = lifecycle::bounded_probe(&f.config, f.create(), 0, &mut |_| Ok(())).await;
    assert_eq!(r["status"], "blocked");
    assert!(f.trace().is_empty());
}

#[tokio::test]
async fn operation_response_must_match_the_journaled_identity() {
    let (r, _) = Fixture::new("wrong-operation").run().await;
    assert_eq!(r["check_failure"], "operation_identity_mismatch");
}

#[tokio::test]
async fn accepted_operation_must_have_the_digest_of_the_journaled_request() {
    let (r, _) = Fixture::new("wrong-digest").run().await;
    assert_eq!(r["check_failure"], "operation_digest_mismatch");
    assert_eq!(r["operations"][0]["delivery"], "pending");
}

#[tokio::test]
async fn accepted_envelope_with_failed_stop_status_does_not_authorize_remove() {
    let f = Fixture::new("misreported-stop");
    let (r, _) = f.run().await;
    assert_eq!(r["cleanup_failure"], "stop_batch_target_not_proven");
    assert_eq!(r["status"], "failed");
    assert!(!f
        .trace()
        .iter()
        .any(|v| v["method"] == "thinkthread.remove"));
}

#[tokio::test]
async fn already_stopped_with_content_change_does_not_authorize_remove() {
    let f = Fixture::new("changed-noop-stop");
    let (r, _) = f.run().await;
    assert_eq!(r["cleanup_failure"], "stop_batch_target_not_proven");
    assert_eq!(r["status"], "failed");
    assert!(!f
        .trace()
        .iter()
        .any(|v| v["method"] == "thinkthread.remove"));
}
