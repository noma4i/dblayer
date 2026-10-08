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
fn nullable_variables_skip_write_null_or_write_the_value() {
    let cases = [
        ("omitted", Nullable::Omitted, r#"{"id":"1"}"#),
        ("null", Nullable::Null, r#"{"id":"1","name":null}"#),
        ("value", Nullable::Value("Ann".into()), r#"{"id":"1","name":"Ann"}"#),
    ];
    for (case, name, expected) in cases {
        assert_eq!(encode(name), expected, "{case}");
    }
}

#[test]
fn error_code_is_read_only_from_a_string_extension() {
    let cases = [
        ("string code", r#"{"message":"x","extensions":{"code":"SIGNATURE_EXPIRED"}}"#, Some("SIGNATURE_EXPIRED")),
        ("no extensions", r#"{"message":"x"}"#, None),
        ("numeric code", r#"{"message":"x","extensions":{"code":5}}"#, None),
    ];
    for (case, body, expected) in cases {
        let error: GraphQLError = serde_json::from_str(body).unwrap();
        assert_eq!(error.code(), expected, "{case}");
    }
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
