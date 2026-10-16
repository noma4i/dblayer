use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use super::fake_server_tests::{FakeServer, MemoryCredentials};
use super::graphql::{graphql_body, response_of};
use super::vectors::*;
use super::*;
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::transport::{CallKind, GraphQLRequest, MockTransport, Reply, Transport};

async fn transport(rest: MockTransport) -> (EncryptedTransport, Arc<FakeServer>) {
    let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
    let server = FakeServer::new(clock.clone());
    let config = EncryptedConfig { origin: ORIGIN.to_string(), server_key: PUBLIC_KEY_DER_B64.to_string(), pin: PIN.to_string() };
    let client = EncryptedClient::new(&config, server.clone(), Arc::new(MemoryCredentials::default()), Db::open_in_memory(Vec::new(), None).unwrap(), clock).unwrap();
    client.mutate("/api/v3/auth/provider", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    (EncryptedTransport::new(Arc::new(client), Arc::new(rest)), server)
}

#[test]
fn the_body_carries_the_document_with_its_sha256_hash_and_omits_empty_fields() {
    let request = GraphQLRequest::new("query Me { me { id } }", "Me", json!({ "a": 1 }));

    let body = graphql_body(&request);

    assert_eq!(body["query"], "query Me { me { id } }");
    assert_eq!((body["operationName"].clone(), body["variables"].clone()), (json!("Me"), json!({ "a": 1 })));
    assert_eq!(body["extensions"]["persistedQuery"]["version"], 1);
    assert_eq!(body["extensions"]["persistedQuery"]["sha256Hash"], "2e3e49f19828396c35f7f9415f5558239d37f5d85b674851f58bf43337f5aaab");
    let bare = graphql_body(&GraphQLRequest::new("query { x }", "", serde_json::Value::Null));
    assert!(bare.get("operationName").is_none() && bare.get("variables").is_none());
}

#[test]
fn an_answer_maps_to_a_response_and_a_server_error_code_becomes_a_graphql_error() {
    let data = response_of(Answer { sequence: None, body: json!({ "data": { "x": 1 } }) }).unwrap();
    assert_eq!(data.data, Some(json!({ "x": 1 })));

    let refused = response_of(Answer { sequence: None, body: json!({ "error": "UNAUTHENTICATED" }) }).unwrap();
    assert_eq!((refused.data, refused.errors[0].code()), (None, Some("UNAUTHENTICATED")));
    let failed = response_of(Answer { sequence: Some(2), body: json!({ "errors": [{ "message": "boom", "extensions": { "code": "PERSISTED_QUERY_NOT_FOUND" } }] }) }).unwrap();
    assert_eq!(failed.errors[0].code(), Some("PERSISTED_QUERY_NOT_FOUND"));
}

#[tokio::test]
async fn queries_and_mutations_go_through_the_encrypted_client_and_subscriptions_and_uploads_stay_with_the_platform() {
    let rest = MockTransport::new();
    rest.enqueue("Watch", Reply::data(json!({ "w": 1 })));
    let (transport, server) = transport(rest.clone()).await;

    let read = transport.query(GraphQLRequest::new("query Q { q }", "Q", json!({}))).await.unwrap();
    let written = transport.mutate(GraphQLRequest::new("mutation M { m }", "M", json!({}))).await.unwrap();
    let _ = transport.subscribe(GraphQLRequest::new("subscription Watch { w }", "Watch", json!({}))).await;

    assert_eq!(read.data, Some(json!({ "read": "query Q { q }" })));
    assert_eq!(written.data, Some(json!({ "done": "mutation M { m }" })));
    let sequences: Vec<_> = server.seen().iter().map(|request| request["sequence"].clone()).collect();
    assert_eq!(sequences, [json!(1), serde_json::Value::Null, json!(2)]);
    assert_eq!(rest.calls().iter().map(|call| call.kind).collect::<Vec<_>>(), [CallKind::Subscription]);
}
