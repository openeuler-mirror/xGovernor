#![cfg(unix)]

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tempfile::TempDir;
use xgovernor_thinkthread_client::{sdk, ConnectError, ManagementConfig};

struct HostFixture {
    root: TempDir,
    config: ManagementConfig,
}

impl HostFixture {
    fn new(scenario: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let host = root.path().join("management-host");
        std::fs::write(&host, include_bytes!("support/management_host.py")).unwrap();
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
                ("XG_TEST_SCENARIO".to_owned(), scenario.to_owned()),
                (
                    "XG_TEST_TRACE".to_owned(),
                    root.path()
                        .join("trace.ndjson")
                        .to_str()
                        .unwrap()
                        .to_owned(),
                ),
                (
                    "XG_TEST_MODEL_KEY".to_owned(),
                    "synthetic-private-value".to_owned(),
                ),
            ]),
            connect_timeout: Duration::from_secs(5),
        };
        Self { root, config }
    }

    fn trace(&self) -> Vec<Value> {
        std::fs::read_to_string(self.root.path().join("trace.ndjson"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn methods(&self) -> Vec<String> {
        self.trace()
            .iter()
            .filter_map(|value| value["method"].as_str().map(str::to_owned))
            .collect()
    }
}

#[test]
fn dependency_matches_the_approved_contract() {
    assert_eq!(sdk::MANAGEMENT_PROTOCOL_VERSION, 2);
    assert_eq!(
        sdk::MANAGEMENT_CONTRACT_FINGERPRINT,
        xgovernor_thinkthread_client::EXPECTED_CONTRACT_FINGERPRINT
    );
}

#[tokio::test]
async fn binds_explicit_roots_and_freezes_only_the_selected_environment() {
    let mut fixture = HostFixture::new("ok");
    let client = fixture.config.connect().await.unwrap();
    let context = client.context().clone();
    assert_eq!(context.state_root, fixture.config.state_root);
    assert_eq!(context.runtime_root, fixture.config.runtime_root);
    assert_eq!(context.config_root, fixture.config.config_root);
    assert_eq!(context.cache_root, fixture.config.cache_root);
    assert_eq!(context.fs_root, fixture.config.fs_root);
    assert_eq!(context.working_directory, fixture.config.working_directory);
    assert_eq!(
        context.home_directory,
        Some(fixture.config.home_directory.clone())
    );
    assert_eq!(
        context.executable_search_path,
        fixture.config.executable_search_path
    );
    let selected_keys: Vec<_> = fixture.config.base_environment.keys().cloned().collect();
    assert_eq!(context.environment_keys, selected_keys);

    fixture.config.state_root = fixture.root.path().join("other-state");
    fixture
        .config
        .base_environment
        .insert("LATE_KEY".to_owned(), "late".to_owned());
    assert_eq!(client.context(), &context);
    client.close().await.unwrap();

    let trace = fixture.trace();
    let env_keys: Vec<_> = trace[0]["envKeys"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .filter(|key| *key != "LC_CTYPE")
        .map(str::to_owned)
        .collect();
    assert_eq!(env_keys, selected_keys);
    assert_eq!(trace[2]["params"], serde_json::to_value(context).unwrap());
    assert!(!serde_json::to_string(&trace)
        .unwrap()
        .contains("synthetic-private-value"));
    assert_eq!(fixture.methods(), ["handshake", "context.bind"]);
    for name in [
        "state",
        "runtime",
        "config",
        "cache",
        "fs",
        "work",
        "home",
        "other-state",
    ] {
        assert!(!fixture.root.path().join(name).exists());
    }
}

#[test]
fn diagnostics_do_not_contain_environment_values() {
    let fixture = HostFixture::new("ok");
    let debug = format!("{:?}", fixture.config);
    assert!(debug.contains("XG_TEST_MODEL_KEY"));
    assert!(!debug.contains("synthetic-private-value"));
}

#[tokio::test]
async fn invalid_paths_are_rejected_before_starting_the_host() {
    for field in 0..9 {
        let mut fixture = HostFixture::new("ok");
        let relative = PathBuf::from("relative");
        match field {
            0 => fixture.config.host_executable = relative,
            1 => fixture.config.state_root = relative,
            2 => fixture.config.runtime_root = relative,
            3 => fixture.config.config_root = relative,
            4 => fixture.config.cache_root = relative,
            5 => fixture.config.working_directory = relative,
            6 => fixture.config.fs_root = relative,
            7 => fixture.config.home_directory = relative,
            _ => fixture.config.executable_search_path = vec![relative],
        }
        assert!(matches!(
            fixture.config.connect().await,
            Err(ConnectError::Configuration { .. })
        ));
        assert!(!fixture.root.path().join("trace.ndjson").exists());
    }
    let mut fixture = HostFixture::new("ok");
    fixture.config.state_root = fixture.root.path().join("state/../sibling");
    assert!(fixture.config.validate().is_err());
}

#[tokio::test]
async fn invalid_environment_and_zero_timeout_fail_before_start() {
    for (key, value) in [
        ("", "value"),
        ("1KEY", "value"),
        ("INVALID=KEY", "value"),
        ("THINKTHREAD_CONTROL_TOKEN", "synthetic-secret"),
        ("MODEL_KEY", "synthetic-secret\0tail"),
    ] {
        let mut fixture = HostFixture::new("ok");
        fixture
            .config
            .base_environment
            .insert(key.to_owned(), value.to_owned());
        let error = fixture.config.connect().await.unwrap_err();
        assert!(matches!(error, ConnectError::Configuration { .. }));
        assert!(!format!("{error:?}").contains("synthetic-secret"));
        assert!(!fixture.root.path().join("trace.ndjson").exists());
    }
    let mut fixture = HostFixture::new("ok");
    fixture.config.connect_timeout = Duration::ZERO;
    assert!(matches!(
        fixture.config.connect().await,
        Err(ConnectError::Configuration { .. })
    ));
    assert!(!fixture.root.path().join("trace.ndjson").exists());
}

#[tokio::test]
async fn missing_explicit_host_does_not_fall_back_to_discovery() {
    let mut fixture = HostFixture::new("ok");
    fixture.config.host_executable = fixture.root.path().join("missing-host");
    assert!(matches!(
        fixture.config.connect().await,
        Err(ConnectError::Sdk(sdk::ManagementError::Host(_)))
    ));
    assert!(!fixture.root.path().join("trace.ndjson").exists());
}

#[tokio::test]
async fn incompatible_handshakes_and_refused_context_never_expose_operations() {
    for scenario in ["bad-version", "bad-fingerprint", "bind-refused"] {
        let fixture = HostFixture::new(scenario);
        assert!(matches!(
            fixture.config.connect().await,
            Err(ConnectError::Sdk(sdk::ManagementError::Protocol(_)))
        ));
        let expected = if scenario == "bind-refused" {
            vec!["handshake", "context.bind"]
        } else {
            vec!["handshake"]
        };
        assert_eq!(fixture.methods(), expected);
    }
}

#[tokio::test]
async fn eof_and_oversized_frames_preserve_transport_failure() {
    for scenario in ["eof", "oversized"] {
        let fixture = HostFixture::new(scenario);
        let error = fixture.config.connect().await.unwrap_err();
        match error {
            ConnectError::Sdk(error) => assert_eq!(error.delivery(), Some(sdk::Delivery::NotSent)),
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(fixture.methods(), ["handshake"]);
    }
}

#[tokio::test]
async fn disconnected_mutation_requires_explicit_reconnect_and_same_operation_replay() {
    let mut fixture = HostFixture::new("drop-send");
    let client = fixture.config.connect().await.unwrap();
    let operation_id = "c702673c-2a77-4f1c-b403-9cc688fab456".to_owned();
    let request = sdk::MessageSendRequest {
        recipient_thinkthread_id: "fixture-recipient".to_owned(),
        text: "fixture prompt".to_owned(),
    };
    let error = client
        .messages()
        .send_with_operation(operation_id.clone(), request.clone())
        .await
        .unwrap_err();
    assert_eq!(error.delivery(), Some(sdk::Delivery::SentUnknown));
    let error = client
        .messages()
        .send_with_operation(operation_id.clone(), request.clone())
        .await
        .unwrap_err();
    assert_eq!(error.delivery(), Some(sdk::Delivery::NotSent));
    client.close().await.unwrap();
    assert_eq!(
        fixture.methods(),
        ["handshake", "context.bind", "message.send"]
    );

    fixture
        .config
        .base_environment
        .insert("XG_TEST_SCENARIO".to_owned(), "ok".to_owned());
    let client = fixture.config.connect().await.unwrap();
    let admitted = client
        .messages()
        .send_with_operation(operation_id.clone(), request)
        .await
        .unwrap();
    assert_eq!(admitted.operation.operation_id, operation_id);
    assert_eq!(admitted.message.state, sdk::MessageState::Queued);
    client.close().await.unwrap();
    let sends: Vec<_> = fixture
        .trace()
        .into_iter()
        .filter(|value| value["method"] == "message.send")
        .collect();
    assert_eq!(sends.len(), 2);
    assert_eq!(sends[0]["params"], sends[1]["params"]);
    assert_eq!(sends[0]["params"]["operationId"], json!(operation_id));
}

#[tokio::test]
async fn queued_cancel_rejection_is_not_reported_as_success() {
    let fixture = HostFixture::new("ok");
    let client = fixture.config.connect().await.unwrap();
    let error = client
        .messages()
        .cancel(sdk::MessageTargetRequest {
            recipient_thinkthread_id: "fixture-recipient".to_owned(),
            message_id: "fixture-message".to_owned(),
        })
        .await
        .unwrap_err();
    match error {
        sdk::ManagementError::Rejected(body) => {
            assert_eq!(body.code, "MessageAlreadyDispatched");
            assert_eq!(body.delivery, sdk::Delivery::Accepted);
            assert!(!body.retryable);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    client.close().await.unwrap();
    assert_eq!(
        fixture.methods(),
        ["handshake", "context.bind", "message.cancel"]
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn connection_timeout_reclaims_the_host_process() {
    let mut fixture = HostFixture::new("hang");
    fixture.config.connect_timeout = Duration::from_secs(1);
    assert!(matches!(
        fixture.config.connect().await,
        Err(ConnectError::Timeout)
    ));
    let pid = fixture.trace()[0]["pid"].as_u64().unwrap();
    let process = PathBuf::from(format!("/proc/{pid}"));
    let deadline = Instant::now() + Duration::from_secs(2);
    while process.exists() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !process.exists(),
        "timed-out Management Host was not reaped"
    );
    assert_eq!(fixture.methods(), ["handshake"]);
}

#[test]
fn agent_process_context_is_denied_before_starting_the_host() {
    if std::env::var_os("XG_TEST_AGENT_PROBE").is_some() {
        let fixture = HostFixture::new("ok");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        assert!(matches!(
            runtime.block_on(fixture.config.connect()),
            Err(ConnectError::Sdk(sdk::ManagementError::Configuration(_)))
        ));
        assert!(!fixture.root.path().join("trace.ndjson").exists());
        return;
    }
    for marker in [
        "THINKTHREAD_CONTROL_SOCKET",
        "THINKTHREAD_CONTROL_TOKEN",
        "THINKTHREAD_AGENT_CONTROL_SOCKET",
    ] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "agent_process_context_is_denied_before_starting_the_host",
            ])
            .env("XG_TEST_AGENT_PROBE", "1")
            .env(marker, "synthetic-agent-marker")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "agent context probe failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
