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

#[tokio::test(start_paused = true)]
async fn failed_openings_increase_backoff_only_with_online_subscribers() {
    let mut retry = CableSession::new();
    assert_eq!(retry.failed_open(true), None);
    let request = GraphQLRequest::new("subscription { viewerChanges { __typename } }", "Changes", json!({}));
    let (_, subscription) = retry.subscribe(request.clone());
    assert!(within_jitter(retry.failed_open(true).unwrap(), 1));
    assert_eq!(retry.failed_open(false), None);
    assert!(within_jitter(retry.failed_open(true).unwrap(), 2));
    assert!(within_jitter(retry.failed_open(true).unwrap(), 4));
    retry.ready();
    retry.receive(json!({"t":"subscribed", "id":"1"})).unwrap();
    assert!(within_jitter(retry.failed_open(true).unwrap(), 1));
    subscription.cancel();
    retry.cancelled().await.unwrap();
    assert_eq!(retry.failed_open(true), None);
    let (_, _subscription) = retry.subscribe(request);
    assert!(within_jitter(retry.failed_open(true).unwrap(), 2));
    retry.closed(4401, "UNAUTHENTICATED");
    assert_eq!(retry.failed_open(true), None);
}

fn within_jitter(delay: std::time::Duration, ceiling_secs: u64) -> bool {
    let ceiling = std::time::Duration::from_secs(ceiling_secs);
    delay <= ceiling && delay >= ceiling.mul_f64(0.8)
}

fn ready_then_close(session: &mut CableSession, frame: Option<serde_json::Value>) -> std::time::Duration {
    session.ready();
    if let Some(frame) = frame { session.receive(frame).unwrap(); }
    session.closed(1006, "");
    session.failed_open(true).unwrap()
}

#[tokio::test(start_paused = true)]
async fn closes_right_after_ready_grow_jittered_delays_up_to_the_cap() {
    let mut session = CableSession::new();
    let _subscription = session.subscribe(GraphQLRequest::new("subscription { viewerChanges { __typename } }", "Changes", json!({})));
    let mut previous = std::time::Duration::ZERO;
    let mut below_ceiling = false;
    for ceiling in [1u64, 2, 4, 8, 16, 30, 30, 30] {
        let delay = ready_then_close(&mut session, None);
        assert!(within_jitter(delay, ceiling), "{delay:?} outside jitter band of {ceiling}s");
        if ceiling < 30 { assert!(delay > previous, "{delay:?} did not grow past {previous:?}"); }
        below_ceiling |= delay < std::time::Duration::from_secs(ceiling);
        previous = delay;
    }
    assert!(below_ceiling, "delays carry no jitter");
}

#[tokio::test(start_paused = true)]
async fn first_server_frame_after_ready_resets_attempts_and_other_frames_do_not() {
    for (frame, resets) in [
        (Some(json!({"t":"subscribed", "id":"1"})), true),
        (Some(json!({"t":"next", "id":"1", "result":{"data":{}}})), true),
        (Some(json!({"t":"pong", "ts":1})), true),
        (None, false),
    ] {
        let mut session = CableSession::new();
        let _subscription = session.subscribe(GraphQLRequest::new("subscription { viewerChanges { __typename } }", "Changes", json!({})));
        for _ in 0..3 { ready_then_close(&mut session, None); }
        let delay = ready_then_close(&mut session, frame.clone());
        let (first, second) = if resets { (1, 2) } else { (8, 16) };
        assert!(within_jitter(delay, first), "{frame:?}: {delay:?}");
        let delay = ready_then_close(&mut session, None);
        assert!(within_jitter(delay, second), "{frame:?}: {delay:?}");
    }
}
