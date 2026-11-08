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
async fn queue_pops_in_order_and_an_empty_queue_refuses() {
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::data(json!(1)));
    mock.enqueue("Ping", Reply::data(json!(2)));
    let data = |response: Response| response.data.unwrap();
    assert_eq!(data(mock.query(ping()).await.unwrap()), json!(1));
    assert_eq!(data(mock.query(ping()).await.unwrap()), json!(2));
    assert_eq!(mock.query(ping()).await, Err(TransportError::Unscripted("Ping".into())));
}

#[tokio::test]
async fn a_repeated_pair_gets_the_reply_or_refusal_of_the_server_and_a_new_pair_the_next_one() {
    let by = |request_id: u64, attempt: u64| ping().by(super::WireIds::new(super::Author::Outbox, request_id, attempt));
    let refused = TransportError::Http { status: 503, body: "busy".into() };
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::fail(refused.clone()));
    mock.enqueue("Ping", Reply::data(json!(2)));
    assert_eq!(mock.mutate(by(1, 1)).await, Err(refused.clone()));
    assert_eq!(mock.mutate(by(1, 2)).await, Err(refused), "the repeat of the pair");
    assert_eq!(mock.mutate(by(2, 1)).await.unwrap().data, Some(json!(2)), "the new pair");
    assert_eq!(mock.mutate(by(2, 2)).await.unwrap().data, Some(json!(2)), "the repeat of the new pair");
    assert_eq!(mock.mutate(by(3, 1)).await, Err(TransportError::Unscripted("Ping".into())), "an unrecorded pair");
    let other_author = ping().by(super::WireIds::new(super::Author::Window, 1, 1));
    assert_eq!(mock.mutate(other_author).await, Err(TransportError::Unscripted("Ping".into())), "the same id of another author is a new pair");
}

#[tokio::test]
async fn a_transport_failure_without_an_answer_is_not_recorded_for_the_pair() {
    let by = |attempt: u64| ping().by(super::WireIds::new(super::Author::Outbox, 1, attempt));
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::fail(TransportError::Network("down".into())));
    mock.enqueue("Ping", Reply::fail(TransportError::Interrupted("cancelled".into())));
    mock.enqueue("Ping", Reply::data(json!(3)));
    assert_eq!(mock.mutate(by(1)).await, Err(TransportError::Network("down".into())));
    assert_eq!(mock.mutate(by(2)).await, Err(TransportError::Interrupted("cancelled".into())), "the repeat takes the next reply of the queue");
    assert_eq!(mock.mutate(by(3)).await.unwrap().data, Some(json!(3)));
    assert_eq!(mock.mutate(by(4)).await.unwrap().data, Some(json!(3)), "an answer of the server is recorded");
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
