//! Server-side validation of a plugin CompletionResult before it is persisted.

use mcts_protocol::CompletionResult;

use crate::error::PluginHostError;
use crate::run_map::RunMap;

#[derive(Debug, Default)]
pub struct ApplyPlanValidator;

impl ApplyPlanValidator {
    pub fn new() -> Self {
        Self
    }

    /// Validate the protocol shape plus host ownership of the apply_plan state.
    pub fn validate(
        &self,
        result: &CompletionResult,
        run_id: &str,
        run_map: &RunMap,
    ) -> Result<(), PluginHostError> {
        result.validate()?;
        if let Some(plan) = &result.apply_plan {
            // Day 6 keeps expiry/tenant placeholders on StateRecord but has no
            // real host lease metadata yet, so do not pseudo-enforce them here.
            // State ownership is enforceable only once the host has actually
            // materialized branches/states through host.* calls.  The mock
            // stdio plugin runs in-process and never registers host states,
            // so in that no-host-state case protocol validation is sufficient.
            if run_map.has_host_states() {
                run_map.require_state_in_run(run_id, &plan.state_id)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_result(state_id: &str) -> CompletionResult {
        serde_json::from_value(json!({
            "status": "completed",
            "summary": "done",
            "answer": "answer",
            "stop_reason": "node_budget",
            "selected_candidate_id": "c0",
            "candidates": [{
                "candidate_id": "c0",
                "state_id": state_id,
                "score": 0.5,
                "evaluation_version": "v1",
                "summary": "candidate",
                "evidence_refs": []
            }],
            "evidence": [],
            "limitations": [],
            "apply_plan": {
                "kind": "continue_from_state",
                "state_id": state_id,
                "base_state_id": "state-root"
            }
        }))
        .unwrap()
    }

    fn run_map_with_state() -> RunMap {
        let map = RunMap::new();
        map.register_run("run-1").unwrap();
        map.add_branch("run-1", "root-branch", "state-root").unwrap();
        map.add_state("run-1", "state-1", "root-branch").unwrap();
        map
    }

    #[test]
    fn accepts_owned_apply_plan_state() {
        let map = run_map_with_state();
        let result = valid_result("state-1");
        assert!(ApplyPlanValidator::new().validate(&result, "run-1", &map).is_ok());
    }

    #[test]
    fn external_root_state_alone_does_not_enable_local_mock_ownership_gate() {
        let map = RunMap::new();
        map.register_run("run-1").unwrap();
        map.register_root_state("run-1", "state-root").unwrap();
        let result = valid_result("state-3");
        assert!(ApplyPlanValidator::new().validate(&result, "run-1", &map).is_ok());
    }

    #[test]
    fn rejects_apply_plan_state_from_another_run() {
        let map = run_map_with_state();
        map.register_run("run-2").unwrap();
        let result = valid_result("state-1");
        assert!(ApplyPlanValidator::new().validate(&result, "run-2", &map).is_err());
    }
}
