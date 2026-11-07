use super::*;
use serde_json::json;
use std::time::Duration;
use tokio::time::Instant;

fn ping() -> GraphQLRequest {
    GraphQLRequest::new("query Ping { ping }", "Ping", json!({"n": 1}))
}

#[tokio::test]
async fn replies_are_scripted_by_operation_name_and_unscripted_operations_fail() {
    assert_eq!(MockTransport::new().query(ping()).await, Err(TransportError::Unscripted("Ping".into())), "unscripted");

    let scripted = |reply: Reply| {
        let mock = MockTransport::new();
        mock.enqueue("Ping", reply);
        mock
    };
    let data = scripted(Reply::data(json!({"ping": "pong"}))).query(ping()).await.unwrap();
    assert_eq!(data.data, Some(json!({"ping": "pong"})), "data reply");
    let failed = scripted(Reply::fail(TransportError::Network("down".into()))).query(ping()).await;
    assert_eq!(failed, Err(TransportError::Network("down".into())), "scripted error");
    let rejected = scripted(Reply::graphql_error("nope", "FORBIDDEN")).query(ping()).await.unwrap();
    assert_eq!(rejected.errors[0].code(), Some("FORBIDDEN"), "graphql error");
    assert!(rejected.data.is_none());
}

#[tokio::test]
async fn queue_pops_in_order_and_last_reply_repeats() {
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::data(json!(1)));
    mock.enqueue("Ping", Reply::data(json!(2)));
    let data = |response: Response| response.data.unwrap();
    assert_eq!(data(mock.query(ping()).await.unwrap()), json!(1));
    assert_eq!(data(mock.query(ping()).await.unwrap()), json!(2));
    assert_eq!(data(mock.query(ping()).await.unwrap()), json!(2));
}

#[test]
fn delay_waits_on_paused_clock_without_real_sleep() { crate::test_wait::paused(async {
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::data(json!(1)).after(Duration::from_secs(5)));
    let started = Instant::now();
    mock.query(ping()).await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(5));
}) }

#[tokio::test]
async fn journal_records_calls_in_order_with_kind_and_variables() {
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::data(json!(1)));
    mock.enqueue("Rename", Reply::data(json!(2)));
    mock.query(ping()).await.unwrap();
    mock.mutate(GraphQLRequest::new("mutation Rename { r }", "Rename", json!({"id": "a"}))).await.unwrap();
    let calls = mock.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!((calls[0].kind, calls[0].operation_name.as_str()), (RecordedKind::Query, "Ping"));
    assert_eq!((calls[1].kind, calls[1].operation_name.as_str()), (RecordedKind::Mutation, "Rename"));
    assert_eq!(calls[1].variables, json!({"id": "a"}));
    assert_eq!(mock.call_count("Ping"), 1);
}
