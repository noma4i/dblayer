use super::*;
use crate::transport::{GraphQLRequest, SubscriptionEvent};
use serde_json::json;

#[tokio::test(start_paused = true)]
async fn multiplexes_registered_subscriptions_and_cancels_only_the_selected_id() {
    let mut session = CableSession::new();
    let mut subscriptions = Vec::new();
    let fixtures: Vec<serde_json::Value> = serde_json::from_str(include_str!("../../../../../fixtures/v3-cable/subscriptions.json")).unwrap();
    for fixture in &fixtures {
        let request = GraphQLRequest::new(fixture["query"].as_str().unwrap(), fixture["operationName"].as_str().unwrap(), fixture["variables"].clone());
        let (id, subscription) = session.subscribe(request);
        subscriptions.push((id, subscription));
    }
    let frames = session.ready();
    assert_eq!(frames.len(), 4);
    for (((id, subscription), frame), fixture) in subscriptions.iter_mut().zip(&frames).zip(&fixtures) {
        assert_eq!(frame["t"], "subscribe");
        assert_eq!(frame["id"], *id);
        session.receive(json!({"t":"subscribed", "id":id})).unwrap();
        assert!(matches!(subscription.next().await, Some(SubscriptionEvent::Ready)));
        session.receive(json!({"t":"next", "id":id, "result":fixture["result"]})).unwrap();
        let Some(SubscriptionEvent::Next(response)) = subscription.next().await else { panic!("missing next") };
        assert_eq!(response.data.unwrap(), fixture["result"]["data"]);
    }
    let id = subscriptions[0].0.clone();
    assert_eq!(session.unsubscribe(&id), Some(json!({"t":"unsubscribe","id":id})));
    assert!(subscriptions[0].1.next().await.is_none());
    assert_eq!(session.ready().len(), 3);
}

#[tokio::test(start_paused = true)]
async fn ping_deadline_exists_only_for_live_subscriptions_and_expires_without_pong() {
    use std::time::Duration;
    let mut session = CableSession::new();
    for _ in 0..4 {
        tokio::time::advance(Duration::from_secs(25)).await;
        assert!(session.deadline().is_none());
    }
    let (id, _subscription) = session.subscribe(GraphQLRequest::new("subscription { viewerChanges { __typename } }", "Changes", json!({})));
    session.ready();
    let deadline = session.deadline().unwrap();
    assert_eq!(deadline - tokio::time::Instant::now(), Duration::from_secs(25));
    tokio::time::sleep_until(deadline).await;
    let ping = session.tick().unwrap().unwrap();
    assert_eq!(ping["t"], "ping");
    assert_eq!(session.deadline().unwrap() - tokio::time::Instant::now(), Duration::from_secs(20));
    tokio::time::advance(Duration::from_secs(20)).await;
    assert_eq!(session.tick(), Err(CableError::Closed));
    session.unsubscribe(&id);
    assert!(session.deadline().is_none());
}

#[tokio::test(start_paused = true)]
async fn revoke_finishes_every_subscription_and_unavailable_keeps_them_for_retry() {
    for (code, reason, terminal) in [(1000, "LOGGED_OUT", true), (4403, "ACCOUNT_LOCKED", true), (4503, "SERVER_RESTART", false)] {
        let mut session = CableSession::new();
        let (_, mut subscription) = session.subscribe(GraphQLRequest::new("subscription { viewerChanges { __typename } }", "Changes", json!({})));
        let close = session.closed(code, reason);
        assert_eq!(close.code, code);
        assert_eq!(close.terminal, terminal);
        if terminal {
            let Some(SubscriptionEvent::Failed(failure)) = subscription.next().await else { panic!("terminal failure") };
            assert!(matches!(failure.error, crate::transport::TransportError::Session { .. }));
            assert!(subscription.next().await.is_none());
            assert!(session.ready().is_empty());
        } else {
            assert_eq!(session.ready().len(), 1);
        }
    }
}

#[test]
fn failed_openings_increase_backoff_without_spending_attempts_offline() {
    use std::time::Duration;
    let mut retry = CableSession::new();
    assert_eq!(retry.failed_open(true), Some(Duration::from_secs(1)));
    assert_eq!(retry.failed_open(false), None);
    assert_eq!(retry.failed_open(true), Some(Duration::from_secs(2)));
    assert_eq!(retry.failed_open(true), Some(Duration::from_secs(4)));
    retry.ready();
    assert_eq!(retry.failed_open(true), Some(Duration::from_secs(1)));
}
