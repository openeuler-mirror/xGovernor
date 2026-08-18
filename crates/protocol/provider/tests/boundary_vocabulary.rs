use std::fs;
use std::path::Path;

#[test]
fn provider_contract_contains_no_business_or_runtime_vocabulary() {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let forbidden = ["session_id", "conversation_id", "sender_id", "runtime_id"];
    for entry in fs::read_dir(source).expect("provider source directory") {
        let path = entry.expect("source entry").path();
        if path.extension().and_then(|value| value.to_str()) != Some("rs") {
            continue;
        }
        let contents = fs::read_to_string(&path).expect("provider source");
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
