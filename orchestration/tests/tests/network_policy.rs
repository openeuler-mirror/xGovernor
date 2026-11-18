use orchestration_contract::execution::{ForkRequest, NetworkPolicy};
use orchestration_contract::rpc::{HostCall, HostForkParams};
use orchestration_contract::run::RunConfig;
use serde_json::json;

fn recorded_config(policy: NetworkPolicy) -> RunConfig {
    serde_json::from_value(json!({
        "budget": {"max_tokens": 1000, "max_time_ms": 30000},
        "network_policy": policy,
        "backend": {"selection": "explicit", "backend": "e2b"},
        "requirements": {
            "capabilities": [], "isolation": "sandbox",
            "joint_snapshots": true, "independent_branches": true
        },
        "algorithm": {
            "namespace": "mcts", "version": "1", "schema": "config-v1",
            "parameters": {}
        }
    }))
    .unwrap()
}

fn wire_fork() -> serde_json::Value {
    json!({
        "plugin_id": "mcts", "run_id": "run", "timeout_ms": 30000,
        "idempotency_key": "fork", "parent_state_id": "root"
    })
}

#[test]
fn backend_fork_requires_recorded_network_policy() {
    let mut params = wire_fork();
    params["inherit_history"] = json!(true);
    // The backend cannot accidentally accept the permissive legacy wire input.
    assert!(serde_json::from_value::<ForkRequest>(params.clone()).is_err());
    params["offline"] = json!(false);
    assert!(serde_json::from_value::<ForkRequest>(params).is_err());

    let wire: HostForkParams = serde_json::from_value(wire_fork()).unwrap();
    let mut config = recorded_config(NetworkPolicy::Offline);
    let request = wire.clone().into_backend_request(&config);
    assert_eq!(request.network_policy, NetworkPolicy::Offline);
    // A persisted, approved override is the authority, not a task-name heuristic.
    config.network_policy = NetworkPolicy::AllowInternet;
    assert_eq!(
        wire.into_backend_request(&config).network_policy,
        NetworkPolicy::AllowInternet
    );
}

#[test]
fn algorithm_offline_flag_can_only_tighten_recorded_policy() {
    let config = recorded_config(NetworkPolicy::Offline);
    let mut params = wire_fork();
    for offline in [None, Some(false)] {
        if let Some(value) = offline {
            params["offline"] = json!(value);
        }
        let call: HostCall =
            serde_json::from_value(json!({"method": "host.fork", "params": params})).unwrap();
        let HostCall::Fork(wire) = call else {
            panic!("expected fork mapping");
        };
        assert_eq!(
            wire.into_backend_request(&config).network_policy,
            NetworkPolicy::Offline
        );
    }
    params["offline"] = json!(true);
    let wire: HostForkParams = serde_json::from_value(params).unwrap();
    assert_eq!(
        wire.into_backend_request(&recorded_config(NetworkPolicy::AllowInternet))
            .network_policy,
        NetworkPolicy::Offline
    );
}

#[test]
fn evaluation_forks_use_recorded_policy_without_history_exception() {
    let wire: HostForkParams = serde_json::from_value(wire_fork()).unwrap();
    let config = recorded_config(NetworkPolicy::Offline);
    // Scoring starts an empty conversation; official evaluation may inherit it.
    let score = config.fork_request(wire.context.clone(), "candidate".into(), false);
    let official = config.fork_request(wire.context, "selected".into(), true);
    assert_eq!(score.network_policy, NetworkPolicy::Offline);
    assert_eq!(official.network_policy, NetworkPolicy::Offline);
}
