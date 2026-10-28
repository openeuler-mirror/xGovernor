use std::fs;
use std::path::PathBuf;

use mcts_protocol::RpcEnvelope;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn read_fixture(name: &str) -> String {
    fs::read_to_string(fixture_dir().join(name)).unwrap()
}

#[test]
fn valid_fixtures_deserialize_and_reserialize() {
    let valid = [
        "host.agent_turn.request.json",
        "host.fork.request.json",
        "host.job.completed.notification.json",
        "plugin.completed.notification.json",
        "plugin.initialize.request.json",
        "plugin.start.request.json",
        "plugin.wait.request.json",
    ];

    for name in valid {
        let text = read_fixture(name);
        let message: RpcEnvelope = serde_json::from_str(&text).unwrap_or_else(|error| {
            panic!("failed to deserialize {name}: {error}")
        });
        let value = serde_json::to_value(&message).unwrap();
        let reparsed: RpcEnvelope =
            serde_json::from_value(value).unwrap_or_else(|error| {
                panic!("failed to reserialize {name}: {error}")
            });
        assert_eq!(message, reparsed, "round-trip mismatch for {name}");
    }
}

#[test]
fn invalid_fixture_is_rejected() {
    let text = read_fixture("invalid.missing_timeout.json");
    let result = serde_json::from_str::<RpcEnvelope>(&text);
    assert!(result.is_err(), "invalid fixture should be rejected");
}
