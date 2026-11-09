use super::*;
use serde_json::json;

const HASH: &str = "abc123";

fn wire(request_id: &str, sequence: Option<i64>, body: Value) -> Value {
    json!({ "requestId": request_id, "sequence": sequence, "body": body })
}

fn hash_only(operation: &str, variables: Value) -> Value {
    json!({ "operationName": operation, "variables": variables, "extensions": { "persistedQuery": { "version": 1, "sha256Hash": HASH } } })
}

fn full(operation: &str, variables: Value) -> Value {
    json!({ "operationName": operation, "variables": variables, "query": "mutation M { m }", "extensions": { "persistedQuery": { "version": 1, "sha256Hash": HASH } } })
}

fn request(scope: &str, request_id: &str, sequence: Option<i64>, body: Value) -> Request {
    Request::from_wire(scope, &wire(request_id, sequence, body))
}

fn code_of(body: &Value) -> String {
    body["errors"][0]["extensions"]["code"].as_str().or_else(|| body["error"].as_str()).unwrap_or_default().to_string()
}

#[test]
fn server_contract_pair_memory() {
    let mut contract: Contract<String> = Contract::new();
    contract.register(HASH);
    let first = request("a", "r1", Some(1), hash_only("M", json!({ "x": 1 })));
    assert!(matches!(contract.admit(&first), Admission::Fresh));
    contract.settle(&first, "answer".into());
    let again = request("a", "r1", Some(1), hash_only("M", json!({ "x": 1 })));
    assert!(matches!(contract.admit(&again), Admission::Stored(answer) if answer == "answer"));
    let other_body = request("a", "r1", Some(1), hash_only("M", json!({ "x": 2 })));
    assert!(matches!(contract.admit(&other_body), Admission::TwoBodies));
    let other_scope = request("b", "r1", Some(1), hash_only("M", json!({ "x": 2 })));
    assert!(!matches!(contract.admit(&other_scope), Admission::TwoBodies | Admission::Stored(_)));
    assert_eq!(contract.last(), 1);
}

#[test]
fn server_contract_sequence_spend() {
    let mut contract: Contract<String> = Contract::new();
    contract.register(HASH);
    let gap = request("a", "r1", Some(2), hash_only("M", json!({})));
    assert!(matches!(contract.admit(&gap), Admission::Refused(body) if code_of(&body) == "SEQUENCE_CONFLICT"));
    assert_eq!(contract.last(), 0);
    let next = request("a", "r2", Some(1), hash_only("M", json!({})));
    assert!(matches!(contract.admit(&next), Admission::Fresh));
    assert_eq!(contract.last(), 0);
    contract.settle(&next, "one".into());
    assert_eq!(contract.last(), 1);
    let read = request("a", "r3", None, hash_only("Q", json!({})));
    assert!(matches!(contract.admit(&read), Admission::Fresh));
    contract.settle(&read, "read".into());
    assert_eq!(contract.last(), 1);
}

#[test]
fn server_contract_document_registry() {
    let mut contract: Contract<String> = Contract::new();
    let miss = request("a", "r1", Some(1), hash_only("M", json!({})));
    assert!(matches!(contract.admit(&miss), Admission::Refused(body) if code_of(&body) == "PERSISTED_QUERY_NOT_FOUND"));
    assert_eq!(contract.last(), 0);
    let document = request("a", "r2", Some(1), full("M", json!({})));
    assert!(matches!(contract.admit(&document), Admission::Fresh));
    contract.settle(&document, "one".into());
    let known = request("a", "r3", Some(2), hash_only("M", json!({})));
    assert!(matches!(contract.admit(&known), Admission::Fresh));
    contract.forget();
    let forgotten = request("a", "r4", Some(2), hash_only("M", json!({})));
    assert!(matches!(contract.admit(&forgotten), Admission::Refused(body) if code_of(&body) == "PERSISTED_QUERY_NOT_FOUND"));
    let unsequenced = request("a", "r5", None, hash_only("Q", json!({})));
    assert!(matches!(contract.admit(&unsequenced), Admission::Refused(_)));
}

#[test]
fn server_contract_unscripted_request() {
    let mut contract: Contract<String> = Contract::new();
    assert!(contract.take("M").is_none());
    contract.script("M", "first".into());
    contract.script("M", "second".into());
    assert_eq!(contract.take("M").as_deref(), Some("first"));
    assert_eq!(contract.take("M").as_deref(), Some("second"));
    assert!(contract.take("M").is_none());
    assert!(contract.take("Other").is_none());
    contract.standing("always".into());
    assert_eq!(contract.take("Other").as_deref(), Some("always"));
    assert_eq!(contract.take("Other").as_deref(), Some("always"));
}
