use serde_json::json;

use crate::transport::{CallKind, Reply};

use super::rig::*;
use super::{OperationDef, SyncError};

fn registered() -> Rig {
    let rig = rig();
    rig.sync.register(OperationDef::new("QrCodeCreate", "mutation QrCodeCreate($input: CreateQrCodeInput!) { qrCodeCreate(input: $input) { url } }", "qrCodeCreate"));
    rig.sync.register(OperationDef::new("Pricing", "query Pricing { pricing { plan } }", "pricing"));
    rig
}

#[tokio::test]
async fn an_admission_from_another_database_cannot_start_a_server_call() {
    let other = rig();
    let rig = registered();
    let admitted = other.db.admit().unwrap();
    let result = rig.sync.run(&admitted, "Pricing", json!({})).await;
    assert!(matches!(result, Err(SyncError::Cancelled)));
    assert!(rig.transport.calls().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_run_sends_the_variables_and_returns_the_value_at_the_root() {
    let rig = registered();
    rig.transport.enqueue("QrCodeCreate", Reply::data(json!({"qrCodeCreate": {"url": "https://q"}})));
    let value = rig.sync.run(&rig.sync.inner.db.admit().unwrap(), "QrCodeCreate", json!({"input": {"kind": "chat"}})).await.unwrap();
    assert_eq!(value, json!({"url": "https://q"}));
    let calls = rig.transport.calls();
    assert_eq!(calls.last().unwrap().variables, json!({"input": {"kind": "chat"}}));
    assert_eq!(calls.last().unwrap().kind, CallKind::Mutation);
}

#[tokio::test(start_paused = true)]
async fn a_run_of_a_query_uses_the_query_call_and_an_unknown_name_fails() {
    let rig = registered();
    rig.transport.enqueue("Pricing", Reply::data(json!({"pricing": {"plan": "gold"}})));
    assert_eq!(rig.sync.run(&rig.sync.inner.db.admit().unwrap(), "Pricing", json!({})).await.unwrap(), json!({"plan": "gold"}));
    assert_eq!(rig.transport.calls().last().unwrap().kind, CallKind::Query);
    assert!(matches!(rig.sync.run(&rig.sync.inner.db.admit().unwrap(), "Nope", json!({})).await, Err(SyncError::UnknownOperation(_))));
}

#[tokio::test(start_paused = true)]
async fn a_run_answered_with_errors_fails_with_every_message() {
    let rig = registered();
    rig.transport.enqueue("QrCodeCreate", Reply::graphql_messages(&["first", "second"]));
    let error = rig.sync.run(&rig.sync.inner.db.admit().unwrap(), "QrCodeCreate", json!({})).await.unwrap_err();
    assert_eq!(error.to_string(), "graphql: first\nsecond");
}

#[tokio::test(start_paused = true)]
async fn an_aggregate_run_returns_the_nested_value_and_writes_no_rows() {
    let rig = registered();
    rig.sync.register(OperationDef::new("ChatMedia", "query ChatMedia($chatId: ID!, $upToSequence: Int) { node(id: $chatId) { ... on Chat { id mediaLibrary(upToSequence: $upToSequence) { visualCount latestSequence } } } }", "node"));
    let before = rig.db.revision_seq().unwrap();
    rig.transport.enqueue("ChatMedia", Reply::data(json!({"node": {"id": "c1", "mediaLibrary": {"visualCount": 3, "latestSequence": 120}}})));
    let value = rig.sync.run(&rig.sync.inner.db.admit().unwrap(), "ChatMedia", json!({"chatId": "c1", "upToSequence": 120})).await.unwrap();
    assert_eq!(value["mediaLibrary"], json!({"visualCount": 3, "latestSequence": 120}));
    assert_eq!(rig.transport.calls().last().unwrap().variables, json!({"chatId": "c1", "upToSequence": 120}));
    assert_eq!(rig.db.revision_seq().unwrap(), before);
    assert_eq!(member_ids(&rig, "moments", "feed", &params(json!({}))), Vec::<String>::new());
}
