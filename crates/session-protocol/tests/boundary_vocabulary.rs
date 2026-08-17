use std::fs;
use std::path::Path;

#[test]
fn session_contract_contains_no_implementation_crate_vocabulary() {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let forbidden = [
        "agent_types",
        "agent-types",
        "xiaoo_core",
        "xiaoo-core",
        "operation_backend",
        "operation-backend",
        "provider_protocol",
        "provider-protocol",
    ];
    for entry in fs::read_dir(source).expect("session source directory") {
        let path = entry.expect("source entry").path();
        if path.extension().and_then(|value| value.to_str()) != Some("rs") {
            continue;
        }
        let contents = fs::read_to_string(&path).expect("session source");
        for word in forbidden {
            assert!(
                !contents.contains(word),
                "{} leaked into {}",
                word,
                path.display()
            );
        }
    }
}
