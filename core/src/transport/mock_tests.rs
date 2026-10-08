use super::*;
use serde_json::json;
use std::time::Duration;
use tokio::time::Instant;

fn ping() -> GraphQLRequest {
    GraphQLRequest::new("query Ping { ping }", "Ping", json!({"n": 1}))
}

#[tokio::test]
async fn scripted_response_is_returned_by_operation_name() {
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::data(json!({"ping": "pong"})));
    let response = mock.query(ping()).await.unwrap();
    assert_eq!(response.data, Some(json!({"ping": "pong"})));
}

#[tokio::test]
async fn unscripted_operation_fails_with_unscripted_error() {
    let mock = MockTransport::new();
    assert_eq!(mock.query(ping()).await, Err(TransportError::Unscripted("Ping".into())));
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

#[tokio::test]
async fn scripted_error_is_returned() {
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::fail(TransportError::Network("down".into())));
    assert_eq!(mock.query(ping()).await, Err(TransportError::Network("down".into())));
}

#[tokio::test]
async fn graphql_errors_travel_in_the_response() {
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::graphql_error("nope", "FORBIDDEN"));
    let response = mock.query(ping()).await.unwrap();
    assert_eq!(response.errors[0].code(), Some("FORBIDDEN"));
    assert!(response.data.is_none());
}

#[tokio::test(start_paused = true)]
async fn delay_waits_on_paused_clock_without_real_sleep() {
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::data(json!(1)).after(Duration::from_secs(5)));
    let started = Instant::now();
    mock.query(ping()).await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(5));
}

#[tokio::test]
async fn journal_records_calls_in_order_with_kind_and_variables() {
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::data(json!(1)));
    mock.enqueue("Rename", Reply::data(json!(2)));
    mock.query(ping()).await.unwrap();
    mock.mutate(GraphQLRequest::new("mutation Rename { r }", "Rename", json!({"id": "a"}))).await.unwrap();
    let calls = mock.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!((calls[0].kind, calls[0].operation_name.as_str()), (CallKind::Query, "Ping"));
    assert_eq!((calls[1].kind, calls[1].operation_name.as_str()), (CallKind::Mutation, "Rename"));
    assert_eq!(calls[1].variables, json!({"id": "a"}));
    assert_eq!(mock.call_count("Ping"), 1);
}

#[tokio::test]
async fn subscription_delivers_scripted_and_emitted_events_then_breaks_on_disconnect() {
    let mock = MockTransport::new();
    mock.script_subscription("Tick", vec![SubscriptionEvent::Ready]);
    let mut subscription = mock.subscribe(GraphQLRequest::new("subscription Tick { t }", "Tick", json!({}))).await.unwrap();
    assert_eq!(subscription.next().await, Some(SubscriptionEvent::Ready));
    mock.emit("Tick", SubscriptionEvent::Next(Response::data(json!({"t": 1}))));
    assert_eq!(subscription.next().await, Some(SubscriptionEvent::Next(Response::data(json!({"t": 1})))));
    mock.emit(
        "Tick",
        SubscriptionEvent::Failed(SubscriptionFailure::new(SubscriptionFailureKind::Transport, "lost")),
    );
    assert!(matches!(subscription.next().await, Some(SubscriptionEvent::Failed(_))));
    mock.disconnect("Tick");
    assert_eq!(subscription.next().await, None);
    assert_eq!(mock.call_count("Tick"), 1);
}

#[tokio::test]
async fn cancelling_subscription_is_counted_once() {
    let mock = MockTransport::new();
    let subscription = mock.subscribe(GraphQLRequest::new("subscription Tick { t }", "Tick", json!({}))).await.unwrap();
    subscription.cancel();
    drop(subscription);
    assert_eq!(mock.cancellations("Tick"), 1);
}

#[tokio::test]
async fn mock_is_usable_as_dyn_transport() {
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::data(json!(1)));
    let transport: std::sync::Arc<dyn Transport> = std::sync::Arc::new(mock);
    assert!(transport.query(ping()).await.is_ok());
}
