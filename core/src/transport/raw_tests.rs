use super::*;
use serde_json::json;

fn failure(kind: RawFailureKind, code: Option<i64>) -> RawFailure { RawFailure { kind, code, message: "text".into() } }

/// P-SESS-14: one raw platform failure gets one class, whichever platform reported it.
#[test]
fn sess_transport_classify_table() {
    use ExchangeKind::{Mutation, Post, Query};
    let interrupted = || TransportError::Interrupted("text".into());
    let network = || TransportError::Network("text".into());
    let table = [
        (failure(RawFailureKind::Cancelled, None), [interrupted(), interrupted(), interrupted()]),
        (failure(RawFailureKind::Tls, Some(-1202)), [0; 3].map(|_| TransportError::Security { code: SecurityCode::TlsTrust, detail: "text".into() })),
        (failure(RawFailureKind::Unreachable, Some(-1009)), [network(), network(), network()]),
        (failure(RawFailureKind::Timeout, Some(-1001)), [network(), interrupted(), network()]),
        (failure(RawFailureKind::Io, None), [network(), interrupted(), network()]),
        (failure(RawFailureKind::Protocol, None), [network(), interrupted(), network()]),
        (failure(RawFailureKind::Protocol, Some(403)), [0; 3].map(|_| TransportError::Http { status: 403, body: "text".into() })),
        (failure(RawFailureKind::Protocol, Some(70000)), [network(), interrupted(), network()]),
    ];
    for (raw, [query, mutation, post]) in table {
        assert_eq!(classify(&raw, Query), query, "{raw:?} query");
        assert_eq!(classify(&raw, Mutation), mutation, "{raw:?} mutation");
        assert_eq!(classify(&raw, Post), post, "{raw:?} post");
    }
}

#[test]
fn a_graphql_answer_is_its_body_on_2xx_and_http_otherwise() {
    assert_eq!(graphql_text(RawOutcome::response(204, "{}"), ExchangeKind::Query), Ok("{}".into()));
    assert_eq!(graphql_text(RawOutcome::response(302, "moved"), ExchangeKind::Query), Err(TransportError::Http { status: 302, body: "moved".into() }));
    assert_eq!(graphql_text(RawOutcome::response(500, "x"), ExchangeKind::Mutation), Err(TransportError::Http { status: 500, body: "x".into() }));
}

#[test]
fn the_graphql_call_carries_the_body_the_json_header_and_the_core_timeouts() {
    let request = GraphQLRequest::new("query Q { a }", "Q", json!({"id": 1}));
    let call = graphql_call("https://api.test/graphql", &request);
    assert_eq!((call.url.as_str(), call.method.as_str()), ("https://api.test/graphql", "POST"));
    assert_eq!(serde_json::from_str::<serde_json::Value>(&call.body).unwrap(), json!({"query": "query Q { a }", "operationName": "Q", "variables": {"id": 1}}));
    assert_eq!(call.headers.get("Content-Type").map(String::as_str), Some("application/json"));
    assert_eq!((call.connect_ms, call.request_ms), (10_000, 30_000));
}

#[test]
fn the_encrypted_call_joins_origin_and_path_and_refuses_a_path_that_leaves_the_origin() {
    let call = encrypted_call("https://api.test/", "/api/session", "jwe".into(), NetworkPurpose::Transfer).unwrap();
    assert_eq!((call.url.as_str(), call.body.as_str(), call.purpose), ("https://api.test/api/session", "jwe", NetworkPurpose::Transfer));
    assert_eq!(call.headers.get("Content-Type").map(String::as_str), Some("application/jose"));
    assert_eq!(call.headers.get("Accept").map(String::as_str), Some(""));
    for path in ["//evil.test/x", "/a?x=1", "/a#f", "api/session", "/a/../b", "/a b"] {
        assert!(encrypted_call("https://api.test", path, String::new(), NetworkPurpose::Standard).is_err(), "{path}");
    }
}

#[test]
fn the_graphql_endpoint_needs_an_http_scheme_and_a_host() {
    assert!(graphql_endpoint("https://api.test/graphql").is_ok());
    for endpoint in ["ftp://api.test", "https://", "https:///graphql", "api.test/graphql", "https://api test"] {
        assert!(graphql_endpoint(endpoint).is_err(), "{endpoint}");
    }
}
