use super::*;
use serde_json::json;

fn failure(domain: &str, code: Option<i64>) -> RawFailure { RawFailure { domain: domain.into(), code, message: "text".into() } }

/// P-SESS-14: one raw platform failure gets one class, whichever platform reported it.
#[test]
fn sess_transport_classify_table() {
    use ExchangeKind::{Mutation, Post, Query};
    let interrupted = || TransportError::Interrupted("text".into());
    let network = || TransportError::Network("text".into());
    let tls = || TransportError::Security { code: SecurityCode::TlsTrust, detail: "text".into() };
    let url = "NSURLErrorDomain";
    let mut table = vec![
        (failure("Swift.CancellationError", Some(1)), [interrupted(), interrupted(), interrupted()]),
        (failure("java.util.concurrent.CancellationException", None), [interrupted(), interrupted(), interrupted()]),
        (failure("kotlinx.coroutines.JobCancellationException", None), [interrupted(), interrupted(), interrupted()]),
        (failure(url, Some(-999)), [interrupted(), interrupted(), interrupted()]),
        (failure("javax.net.ssl.SSLHandshakeException", None), [tls(), tls(), tls()]),
        (failure("javax.net.ssl.SSLPeerUnverifiedException", None), [tls(), tls(), tls()]),
        (failure("java.net.UnknownHostException", None), [network(), network(), network()]),
        (failure("java.net.ConnectException", None), [network(), network(), network()]),
        (failure("java.net.NoRouteToHostException", None), [network(), network(), network()]),
        (failure("java.net.SocketTimeoutException", None), [network(), interrupted(), network()]),
        (failure(url, Some(-1001)), [network(), interrupted(), network()]),
        (failure(url, Some(-1005)), [network(), interrupted(), network()]),
        (failure("java.io.IOException", None), [network(), interrupted(), network()]),
        (failure("NSCocoaErrorDomain", Some(260)), [network(), interrupted(), network()]),
        (failure("url", None), [network(), interrupted(), network()]),
        (failure(url, None), [network(), interrupted(), network()]),
        (failure(TIMEOUT_DOMAIN, None), [network(), interrupted(), network()]),
    ];
    table.extend([-1200, -1201, -1202, -1203, -1204, -1205, -1206].map(|code| (failure(url, Some(code)), [tls(), tls(), tls()])));
    table.extend([-1003, -1004, -1006, -1009, -1018, -1020].map(|code| (failure(url, Some(code)), [network(), network(), network()])));
    for (raw, [query, mutation, post]) in table {
        assert_eq!(classify(&raw, Query), query, "{raw:?} query");
        assert_eq!(classify(&raw, Mutation), mutation, "{raw:?} mutation");
        assert_eq!(classify(&raw, Post), post, "{raw:?} post");
    }
}

#[test]
fn a_call_the_http_client_cancelled_is_interrupted_whatever_the_exchange() {
    let cancelled = RawFailure { domain: "java.io.IOException".into(), code: None, message: "Canceled".into() };
    for kind in [ExchangeKind::Query, ExchangeKind::Mutation, ExchangeKind::Post] {
        assert_eq!(classify(&cancelled, kind), TransportError::Interrupted("Canceled".into()));
    }
}

#[test]
fn a_refused_cable_upgrade_is_http_by_its_status_and_any_other_failure_by_its_raw_facts() {
    let refused = failure("java.net.ProtocolException", None);
    assert_eq!(classify_upgrade(&refused, Some(403)), TransportError::Http { status: 403, body: "text".into() });
    for status in [None, Some(101), Some(700)] {
        assert_eq!(classify_upgrade(&refused, status), TransportError::Network("text".into()), "{status:?}");
    }
    assert_eq!(classify_upgrade(&failure("NSURLErrorDomain", Some(-1202)), None), TransportError::Security { code: SecurityCode::TlsTrust, detail: "text".into() });
}

#[test]
fn the_merged_headers_keep_the_app_fields_let_the_library_win_and_set_one_user_agent() {
    let app = HashMap::from([("X-App".to_string(), "a".to_string()), ("uSeR-aGeNt".to_string(), "Reader".to_string()), ("accept".to_string(), "text/html".to_string())]);
    let own = HashMap::from([("Accept".to_string(), String::new())]);
    let merged = merge_headers(app, own);
    assert_eq!(merged.get("X-App").map(String::as_str), Some("a"));
    assert_eq!(merged.get("Accept").map(String::as_str), Some(""));
    assert_eq!(merged.len(), 3);
    assert_eq!(merged.get("User-Agent").map(String::as_str), Some(user_agent(Some("Reader".into())).as_str()));
    assert_eq!(merge_headers(HashMap::new(), HashMap::new()).get("User-Agent").map(String::as_str), Some(user_agent(None).as_str()));
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
    let call = graphql_call("https://api.test/graphql", &request, HashMap::new());
    assert_eq!((call.url.as_str(), call.method.as_str()), ("https://api.test/graphql", "POST"));
    assert_eq!(serde_json::from_str::<serde_json::Value>(&call.body).unwrap(), json!({"query": "query Q { a }", "operationName": "Q", "variables": {"id": 1}}));
    assert_eq!(call.headers.get("Content-Type").map(String::as_str), Some("application/json"));
    assert_eq!((call.connect_ms, call.request_ms), (10_000, 30_000));
}

#[test]
fn the_encrypted_call_joins_origin_and_path_and_refuses_a_path_that_leaves_the_origin() {
    let call = encrypted_call("https://api.test/", "/api/session", "jwe".into(), NetworkPurpose::Transfer, HashMap::new()).unwrap();
    assert_eq!((call.url.as_str(), call.body.as_str(), call.purpose), ("https://api.test/api/session", "jwe", NetworkPurpose::Transfer));
    assert_eq!(call.headers.get("Content-Type").map(String::as_str), Some("application/jose"));
    assert_eq!(call.headers.get("Accept").map(String::as_str), Some(""));
    for path in ["//evil.test/x", "/a?x=1", "/a#f", "api/session", "/a/../b", "/a b"] {
        assert!(encrypted_call("https://api.test", path, String::new(), NetworkPurpose::Standard, HashMap::new()).is_err(), "{path}");
    }
}

#[test]
fn the_graphql_endpoint_needs_an_http_scheme_and_a_host() {
    assert!(graphql_endpoint("https://api.test/graphql").is_ok());
    for endpoint in ["ftp://api.test", "https://", "https:///graphql", "api.test/graphql", "https://api test"] {
        assert!(graphql_endpoint(endpoint).is_err(), "{endpoint}");
    }
}

#[tokio::test]
async fn a_request_the_platform_never_answers_ends_as_a_timeout_after_the_request_timeout() {
    let clock = std::sync::Arc::new(crate::runtime::ManualClock::default());
    let waiting = crate::task::spawn(crate::task::Carry::Detached, {
        let clock = clock.clone();
        async move { within_request(clock.as_ref(), REQUEST_TIMEOUT_MS, std::future::pending()).await }
    });
    crate::test_wait::yield_until("the request ends on the clock", || { clock.advance(std::time::Duration::from_millis(REQUEST_TIMEOUT_MS)); waiting.is_finished() }).await;
    let RawOutcome::Failed { failure } = waiting.await.unwrap() else { panic!("a timeout is a failure") };
    assert_eq!(failure.kind(), RawFailureKind::Timeout);
    assert_eq!(classify(&failure, ExchangeKind::Mutation), TransportError::Interrupted("the request timed out".into()));
}
