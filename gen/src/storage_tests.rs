use crate::parse_models;
use serde_json::Value;

#[test]
fn local_server_declarations_are_rejected_from_the_shared_cases() {
    let cases: Vec<Value> = serde_json::from_str(include_str!("../../fixtures/storage.json")).unwrap();
    for case in cases {
        let storage = case["models"][0]["storage"].as_str().unwrap();
        let member = case["declaration"].as_str().unwrap();
        let source = format!("model StorageRow, storage: :{storage} do\n\nfield :record, type: :string\n\n{member}\nend");
        let result = parse_models(&source);
        if case["error"] == true {
            let errors = result.expect_err(member);
            assert!(errors.iter().any(|error| error.message.contains("local model has no server")), "{member}: {errors:?}");
        } else {
            result.unwrap();
        }
    }
}
