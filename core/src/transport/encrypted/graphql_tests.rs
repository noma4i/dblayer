use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use super::exchange_tests::Client;
use super::fake_server_tests::{FakeServer, MemoryCredentials};
use super::graphql::{graphql_body, response_of};
use super::vectors_tests::*;
use super::*;
use crate::store::Store;
use crate::pacing::ManualClock;
use crate::transport::{GraphQLRequest, MockTransport, Transport};

async fn transport(rest: MockTransport) -> (Arc<EncryptedTransport>, Arc<FakeServer>, Arc<Client>) {
    let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
    let server = FakeServer::new(clock.clone());
    let config = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() };
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let client = Client::new(&config, server.clone(), Arc::new(MemoryCredentials::default()), db, clock, Arc::new(rest)).unwrap();
    client.start_owners();
    client.session_call("social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    let transport = client.transport();
    (transport, server, client)
}

#[tokio::test]
async fn encrypted_subscriptions_do_not_send_credentials_or_queries_to_v2() {
    let rest = MockTransport::new();
    let (transport, _, _client) = transport(rest.clone()).await;
    let _subscription = transport.subscribe(GraphQLRequest::new("subscription Changes { recordEvents { changes { entityId } } }", "Changes", json!({}))).await.unwrap();
    assert_eq!(rest.call_count("Changes"), 0);
}

#[test]
fn the_body_carries_the_sha256_hash_and_the_document_only_when_asked_and_omits_empty_fields() {
    let request = GraphQLRequest::new("query Me { me { id } }", "Me", json!({ "a": 1 }));

    let body = graphql_body(&request, true);
    assert!(graphql_body(&request, false).get("query").is_none());

    assert_eq!(body["query"], "query Me { me { id } }");
    assert_eq!((body["operationName"].clone(), body["variables"].clone()), (json!("Me"), json!({ "a": 1 })));
    assert_eq!(body["extensions"]["persistedQuery"]["version"], 1);
    assert_eq!(body["extensions"]["persistedQuery"]["sha256Hash"], "2e3e49f19828396c35f7f9415f5558239d37f5d85b674851f58bf43337f5aaab");
    let bare = graphql_body(&GraphQLRequest::new("query { x }", "", serde_json::Value::Null), true);
    assert!(bare.get("operationName").is_none() && bare.get("variables").is_none());
}

#[test]
fn an_answer_maps_to_a_response_and_a_server_code_becomes_a_graphql_error() {
    let data = response_of(Answer { sequence: None, body: json!({ "data": { "x": 1 } }) }).unwrap();
    assert_eq!(data.data, Some(json!({ "x": 1 })));

    let refused = response_of(Answer { sequence: None, body: json!({ "error": "INTERNAL_SERVER_ERROR" }) }).unwrap();
    assert_eq!((refused.data, refused.errors[0].code()), (None, Some("INTERNAL_SERVER_ERROR")));
    let failed = response_of(Answer { sequence: Some(2), body: json!({ "errors": [{ "message": "boom", "extensions": { "code": "PERSISTED_QUERY_NOT_FOUND" } }] }) }).unwrap();
    assert_eq!(failed.errors[0].code(), Some("PERSISTED_QUERY_NOT_FOUND"));
}

#[tokio::test]
async fn queries_and_mutations_use_the_encrypted_client() {
    let rest = MockTransport::new();
    let (transport, server, _client) = transport(rest.clone()).await;
    server.register("query Q { q }");
    server.register("mutation M { m }");

    let read = transport.query(GraphQLRequest::new("query Q { q }", "Q", json!({}))).await.unwrap();
    let written = transport.mutate(GraphQLRequest::new("mutation M { m }", "M", json!({}))).await.unwrap();

    assert_eq!(read.data, Some(json!({ "read": "query Q { q }" })));
    assert_eq!(written.data, Some(json!({ "done": "mutation M { m }" })));
    let sequences: Vec<_> = server.seen().iter().map(|request| request["sequence"].clone()).collect();
    assert_eq!(sequences, [json!(1), serde_json::Value::Null, json!(2)]);
    assert!(rest.calls().is_empty());
}
