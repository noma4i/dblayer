use serde_json::{json, Value};

use super::fake_server_tests::{open_request, seal_response};
use super::vectors_tests::*;
use super::*;

fn plain_from_ruby() -> Value {
    serde_json::from_str(super::vectors_tests::request_plain()).unwrap()
}

fn request_plain() -> RequestPlain {
    RequestPlain {
        audience: origin().to_string(),
        kid: pin().to_string(),
        path: "/api/graphql".to_string(),
        request_id: "11111111-2222-4333-8444-555555555555".to_string(),
        issued_at: 1_800_000_000,
        response_key: published_response_key().to_string(),
        session: Some(super::jwe::RequestSession { credential: "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8".to_string(), sequence: Some(2) }),
        body: json!({ "query": "mutation { x }" }),
    }
}

#[test]
fn responses_from_the_ruby_server_open_with_the_request_response_key() {
    let key = plain_from_ruby()["responseKey"].as_str().unwrap().to_string();
    let request_id = plain_from_ruby()["requestId"].as_str().unwrap().to_string();

    let accepted = open_response(response_ok(), &key).unwrap();
    assert_eq!(accepted, ResponsePlain { request_id: request_id.clone(), sequence: Some(1), body: json!({ "data": { "x": { "ok": true } } }) });
    let refused = open_response(response_conflict(), &key).unwrap();
    assert_eq!(refused, ResponsePlain { request_id, sequence: None, body: json!({ "error": "SEQUENCE_CONFLICT" }) });
    let published = open_response(published_response(), published_response_key()).unwrap();
    assert_eq!(published, ResponsePlain { request_id: "11111111-2222-4333-8444-555555555555".to_string(), sequence: Some(2), body: json!({ "revoked": true }) });
}

#[test]
fn a_response_is_refused_when_key_header_or_ciphertext_do_not_match() {
    let key = plain_from_ruby()["responseKey"].as_str().unwrap().to_string();
    let parts: Vec<&str> = response_ok().split('.').collect();
    let header = |json: &str| base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, json);
    let tampered_tag = format!("{}.{}.{}.{}.{}", parts[0], parts[1], parts[2], parts[3], "A".repeat(parts[4].len()));
    let extra_header = format!("{}.{}.{}.{}.{}", header(r#"{"alg":"dir","enc":"A256GCM","zip":"DEF"}"#), parts[1], parts[2], parts[3], parts[4]);
    let other_alg = format!("{}.{}.{}.{}.{}", header(r#"{"alg":"A256KW","enc":"A256GCM"}"#), parts[1], parts[2], parts[3], parts[4]);
    let with_key_segment = format!("{}.AAAA.{}.{}.{}", parts[0], parts[2], parts[3], parts[4]);
    let cases: [(&str, &str); 7] = [
        (response_ok(), "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"),
        (&tampered_tag, &key),
        (&extra_header, &key),
        (&other_alg, &key),
        (&with_key_segment, &key),
        ("a.b.c", &key),
        (response_ok(), "short"),
    ];
    for (token, response_key) in cases {
        assert!(matches!(open_response(token, response_key), Err(crate::transport::encrypted::jwe::JweError::Invalid(_))), "{token}");
    }
}

#[test]
fn a_sealed_request_opens_on_the_server_side_with_exactly_the_ten_fields() {
    let token = seal_request(&ServerKey::parse(public_key_der_b64(), pin()).unwrap(), &request_plain()).unwrap();

    assert_eq!(token.split('.').count(), 5);
    let opened = open_request(&token, "/api/graphql", 1_800_000_000).unwrap();
    assert_eq!(opened, json!({
        "audience": origin(), "kid": pin(), "method": "POST", "path": "/api/graphql",
        "requestId": "11111111-2222-4333-8444-555555555555", "issuedAt": 1_800_000_000, "responseKey": published_response_key(),
        "credential": "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8", "sequence": 2, "body": { "query": "mutation { x }" }
    }));
    let mut read = request_plain();
    read.session.as_mut().unwrap().sequence = None;
    let token = seal_request(&ServerKey::parse(public_key_der_b64(), pin()).unwrap(), &read).unwrap();
    assert!(open_request(&token, "/api/graphql", 1_800_000_000).unwrap()["sequence"].is_null());
}

#[test]
fn a_sessionless_request_has_exactly_eight_fields() {
    let plain = RequestPlain { session: None, ..request_plain() };
    let token = seal_request(&ServerKey::parse(public_key_der_b64(), pin()).unwrap(), &plain).unwrap();
    let opened = super::fake_server_tests::decrypt_request(&token).unwrap();
    assert_eq!(opened, json!({
        "audience": origin(), "kid": pin(), "method": "POST", "path": "/api/graphql",
        "requestId": "11111111-2222-4333-8444-555555555555", "issuedAt": 1_800_000_000,
        "responseKey": published_response_key(), "body": { "query": "mutation { x }" }
    }));
}
