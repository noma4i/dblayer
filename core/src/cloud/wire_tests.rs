use serde_json::{json, Value};

use super::{CloudChange, CloudError};

fn change(field: &str, value: Value) -> CloudChange {
    CloudChange { model: "notes".into(), row_id: "n1".into(), field: field.into(), clock_ms: 1_700_000_000_123, clock_count: 7, replica: "r1".into(), value }
}

#[test]
fn blob_round_trips_every_value_shape() {
    for value in [json!(null), json!(true), json!(-5), json!(1.0), json!(2.5), json!("text"), json!({"a": [1, {"b": null}], "c": "d"}), json!([1, "x"])] {
        let sent = change("title", value);
        assert_eq!(CloudChange::from_blob(&sent.blob().bytes).unwrap(), sent);
    }
}

#[test]
fn blob_bytes_are_stable_and_carry_the_format_version() {
    let blob = change("title", json!({"b": 1, "a": 2})).blob();
    assert_eq!(
        String::from_utf8(blob.bytes).unwrap(),
        r#"{"v":1,"model":"notes","row_id":"n1","field":"title","clock_ms":1700000000123,"clock_count":7,"replica":"r1","value":{"a":2,"b":1}}"#
    );
}

#[test]
fn blob_key_is_the_hash_of_replica_model_row_and_field() {
    let first = change("title", json!("a")).blob().key;
    assert_eq!(first.len(), 64);
    assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()));
    assert_eq!(first, CloudChange { value: json!("b"), clock_ms: 9, clock_count: 1, ..change("title", json!("a")) }.blob().key);
    let others = [change("body", json!("a")), CloudChange { replica: "r2".into(), ..change("title", json!("a")) }, CloudChange { row_id: "n2".into(), ..change("title", json!("a")) }, CloudChange { model: "tasks".into(), ..change("title", json!("a")) }];
    for other in others {
        assert_ne!(first, other.blob().key);
    }
}

#[test]
fn unknown_format_version_and_garbage_are_rejected() {
    let future = br#"{"v":2,"model":"notes","row_id":"n1","field":"title","clock_ms":1,"clock_count":0,"replica":"r1","value":null}"#;
    for bytes in [&future[..], b"not json", b"{}"] {
        assert!(matches!(CloudChange::from_blob(bytes), Err(CloudError::Rejected { .. })), "{}", String::from_utf8_lossy(bytes));
    }
}
