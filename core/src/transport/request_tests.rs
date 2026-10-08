use super::*;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Serialize)]
struct ProbeVariables {
    id: String,
    #[serde(skip_serializing_if = "Nullable::is_omitted")]
    name: Nullable<String>,
}

fn encode(name: Nullable<String>) -> String {
    serde_json::to_string(&ProbeVariables { id: "1".into(), name }).unwrap()
}

#[test]
fn omitted_skips_key() {
    assert_eq!(encode(Nullable::Omitted), r#"{"id":"1"}"#);
}

#[test]
fn null_writes_null() {
    assert_eq!(encode(Nullable::Null), r#"{"id":"1","name":null}"#);
}

#[test]
fn value_writes_value() {
    assert_eq!(encode(Nullable::Value("Ann".into())), r#"{"id":"1","name":"Ann"}"#);
}

#[test]
fn error_decodes_signature_code() {
    let error: GraphQLError =
        serde_json::from_str(r#"{"message":"x","extensions":{"code":"SIGNATURE_EXPIRED"}}"#).unwrap();
    assert_eq!(error.code(), Some("SIGNATURE_EXPIRED"));
}

#[test]
fn error_without_string_code_has_no_code() {
    let missing: GraphQLError = serde_json::from_str(r#"{"message":"x"}"#).unwrap();
    let numeric: GraphQLError = serde_json::from_str(r#"{"message":"x","extensions":{"code":5}}"#).unwrap();
    assert_eq!(missing.code(), None);
    assert_eq!(numeric.code(), None);
}

#[derive(Deserialize, Debug, PartialEq)]
struct Ping {
    ping: String,
}

#[test]
fn response_data_decodes_into_typed_value() {
    let response: Response = serde_json::from_value(json!({"data": {"ping": "pong"}})).unwrap();
    assert!(response.errors.is_empty());
    assert_eq!(response.decode::<Ping>().unwrap(), Some(Ping { ping: "pong".into() }));
}

#[test]
fn response_without_data_decodes_to_none_and_bad_shape_is_decode_error() {
    let empty: Response = serde_json::from_value(json!({"errors": [{"message": "boom"}]})).unwrap();
    assert_eq!(empty.decode::<Ping>().unwrap(), None);
    assert_eq!(empty.errors.len(), 1);
    let bad = Response::data(json!({"ping": 1}));
    assert!(matches!(bad.decode::<Ping>(), Err(TransportError::Decode(_))));
}

#[test]
fn request_keeps_document_name_and_variables() {
    let request = GraphQLRequest::new("query Ping { ping }", "Ping", json!({"a": 1}));
    assert_eq!(request.operation_name, "Ping");
    assert_eq!(request.document, "query Ping { ping }");
    assert_eq!(request.variables, json!({"a": 1}));
}
