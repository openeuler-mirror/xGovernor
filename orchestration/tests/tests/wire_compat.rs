//! Use the migrated algorithm's real manifest and result validator.
use orchestration_contract::{execution::*, rpc::*};
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

fn algorithm(script: &str, input: Option<&str>) -> String {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../algorithms/mcts");
    let setup = "import sys,types,json\np=types.ModuleType('xgovernor_mcts');p.__path__=[sys.argv[1]];sys.modules[p.__name__]=p\n";
    let mut child = Command::new("python3")
        .arg("-c")
        .arg(format!("{setup}{script}"))
        .arg(directory)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    if let Some(input) = input {
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn real_initialize_preserves_optional_job_progress() {
    let sample=algorithm("from xgovernor_mcts.bridge.lifecycle import RunRegistry\nr=RunRegistry()\nprint(json.dumps(r.initialize({'plugin_id':'mcts','supported_api_versions':['mcts.rpc.v1'],'host_version':'1','available_capabilities':list(r.manifest.required_capabilities)})))",None);
    let result: InitializeResult = serde_json::from_str(&sample).unwrap();
    assert!(result
        .optional_host_capabilities
        .contains(&WireCapability::JobProgress));
    let encoded = serde_json::to_value(result).unwrap();
    algorithm("from xgovernor_mcts.engine.mcts_protocol import validate_result\nvalidate_result('plugin.initialize',json.load(sys.stdin))", Some(&encoded.to_string()));
}

#[test]
fn cancel_results_pass_existing_host_client_validator() {
    let responses = [
        CancelJobResponse {
            job_id: "active".into(),
            status: CancelJobStatus::Stopped,
        },
        CancelJobResponse {
            job_id: "finished".into(),
            status: CancelJobStatus::Completed,
        },
    ]
    .map(WireCancelJobResponse::from);
    let input = serde_json::to_string(&responses).unwrap();
    algorithm("from xgovernor_mcts.engine.mcts_protocol import validate_result\nvalues=json.load(sys.stdin)\nfor value in values: validate_result('host.cancel_job',value)\nassert [value['status'] for value in values]==['failed','completed']",Some(&input));
}
