use serde_json::Value;
use std::collections::BTreeSet;
use std::process::Command;

#[test]
fn public_contract_has_only_the_approved_dependencies() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--manifest-path"])
        .arg(format!("{manifest_dir}/Cargo.toml"))
        .output()
        .expect("cargo metadata must run");
    assert!(output.status.success(), "cargo metadata failed");

    let metadata: Value = serde_json::from_slice(&output.stdout).expect("metadata JSON");
    let package = metadata["packages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|package| package["name"] == "session-protocol")
        .expect("session-protocol package");
    let actual = package["dependencies"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|dependency| dependency["kind"].is_null() || dependency["kind"] == "normal")
        .map(|dependency| dependency["name"].as_str().unwrap())
        .collect::<BTreeSet<_>>();
    let approved = ["serde", "serde_json", "thiserror"].into_iter().collect();
    assert_eq!(actual, approved, "wire contract dependency policy changed");
}
