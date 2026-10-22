use super::*;
use crate::transport::{GraphQLRequest, SubscriptionEvent};
use serde_json::json;

#[tokio::test(start_paused = true)]
async fn inv_wait_05_frames_postpone_the_single_deadline_without_timer_wakes() {
    use std::sync::{Arc, atomic::AtomicUsize};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use super::wire_tests::{FakeSocket, FakeConnect, Peer};
    let (incoming, receiver) = mpsc::unbounded_channel();
    let (outgoing, sent) = mpsc::unbounded_channel();
    let socket = Arc::new(FakeSocket { incoming: tokio::sync::Mutex::new(receiver), outgoing, closes: AtomicUsize::new(0) });
    let connect = Arc::new(FakeConnect { socket, opens: AtomicUsize::new(0) });
    let (kat, config, challenge, credential) = super::link_tests::inputs();
    let cable = CableSubscriptions::new(config.clone(), Arc::new(move || Ok(credential.clone())), connect, crate::transport::Connectivity::new(true), Arc::new(crate::pacing::TestClock::default()));
    let mut subscription = cable.subscribe(GraphQLRequest::new("subscription { sample { id } }", "Sample", json!({}))).await.unwrap();
    let mut peer = Peer { incoming, outgoing: sent, send_key: [0; 32], receive_key: [0; 32], sequence: 0 };
    peer.handshake(&kat, &config, challenge).await;
    let subscribed = peer.next().await;
    peer.emit(json!({"t":"subscribed", "id":subscribed["id"]}));
    assert!(matches!(subscription.next().await, Some(SubscriptionEvent::Ready)));
    for number in 0..10 {
        tokio::time::advance(Duration::from_secs(3)).await;
        peer.emit(json!({"t":"next", "id":subscribed["id"], "result":{"data":{"sample":{"id":number}}}}));
        assert!(matches!(subscription.next().await, Some(SubscriptionEvent::Next(_))));
        assert_eq!(cable.wake_count(), 0, "traffic caused a timer wake at frame {number}");
    }
    tokio::time::advance(Duration::from_secs(25)).await;
    assert_eq!(peer.next().await["t"], "ping");
    assert_eq!(cable.wake_count(), 1, "silence must fire exactly one timer");
}

#[tokio::test(start_paused = true)]
async fn invalid_frames_preserve_failure_history() {
    let mut failures = Vec::new();
    for body in [
        json!({"t":"next", "id":"1", "result":{"errors":42}}),
        json!({"t":"next", "id":"1", "result":{}}),
        json!({"t":"next", "id":"missing", "result":{"data":{}}}),
        json!({"t":"subscribed", "id":"missing"}),
        json!({"t":"pong", "ts":999}),
    ] {
        let mut session = CableSession::new(std::sync::Arc::new(crate::pacing::TestClock::default()));
        let _subscription = session.subscribe(GraphQLRequest::new("subscription { x }", "X", json!({})));
        for _ in 0..3 { ready_then_close(&mut session, None); }
        session.ready();
        tokio::time::advance(std::time::Duration::from_secs(25)).await;
        session.tick().unwrap().unwrap();
        let deadline = session.deadline();
        let _ = session.receive(body.clone());
        let preserved_deadline = session.deadline() == deadline;
        session.closed(4400, "");
        let delay = session.failed_open(true).unwrap();
        if !within_jitter(delay, 8) || !preserved_deadline {
            failures.push(format!("{body}: delay {delay:?}, preserved deadline {preserved_deadline}"));
        }
    }
    assert!(failures.is_empty(), "invalid frames reset failure history: {failures:?}");
}

#[tokio::test(start_paused = true)]
async fn independent_cable_sessions_do_not_retry_in_lockstep() {
    let mut sessions: Vec<_> = (0..3).map(|_| {
        let mut session = CableSession::new(std::sync::Arc::new(crate::pacing::TestClock::default()));
        let subscription = session.subscribe(GraphQLRequest::new("subscription { x }", "X", json!({})));
        (session, subscription)
    }).collect();
    let delays: Vec<_> = sessions.iter_mut().map(|(session, _)| session.failed_open(true).unwrap()).collect();
    assert!(delays.windows(2).any(|pair| pair[0] != pair[1]), "independent sessions use identical delays: {delays:?}");
}

#[tokio::test(start_paused = true)]
async fn multiplexes_registered_subscriptions_and_cancels_only_the_selected_id() {
    let mut session = CableSession::new(std::sync::Arc::new(crate::pacing::TestClock::default()));
    let mut subscriptions = Vec::new();
    let fixtures: Vec<serde_json::Value> = serde_json::from_str(include_str!("../../../../../fixtures/change-events/subscriptions.json")).unwrap();
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
    let mut session = CableSession::new(std::sync::Arc::new(crate::pacing::TestClock::default()));
    for _ in 0..4 {
        tokio::time::advance(Duration::from_secs(25)).await;
        assert!(session.deadline().is_none());
    }
    let (id, _subscription) = session.subscribe(GraphQLRequest::new("subscription { recordEvents { __typename } }", "Changes", json!({})));
    session.ready();
    let deadline = session.deadline().unwrap();
    assert_eq!(deadline - session.now(), Duration::from_secs(25));
    tokio::time::sleep(deadline - session.now()).await;
    let ping = session.tick().unwrap().unwrap();
    assert_eq!(ping["t"], "ping");
    assert_eq!(session.deadline().unwrap() - session.now(), Duration::from_secs(20));
    tokio::time::advance(Duration::from_secs(20)).await;
    assert_eq!(session.tick(), Err(CableError::Closed));
    session.unsubscribe(&id);
    assert!(session.deadline().is_none());
}

#[tokio::test(start_paused = true)]
async fn revoke_finishes_every_subscription_and_unavailable_keeps_them_for_retry() {
    for (code, reason, terminal) in [(1000, "LOGGED_OUT", true), (4403, "ACCOUNT_LOCKED", true), (4503, "SERVER_RESTART", false)] {
        let mut session = CableSession::new(std::sync::Arc::new(crate::pacing::TestClock::default()));
        let (_, mut subscription) = session.subscribe(GraphQLRequest::new("subscription { recordEvents { __typename } }", "Changes", json!({})));
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
    let mut retry = CableSession::new(std::sync::Arc::new(crate::pacing::TestClock::default()));
    assert_eq!(retry.failed_open(true), None);
    let request = GraphQLRequest::new("subscription { recordEvents { __typename } }", "Changes", json!({}));
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
    let mut session = CableSession::new(std::sync::Arc::new(crate::pacing::TestClock::default()));
    let _subscription = session.subscribe(GraphQLRequest::new("subscription { recordEvents { __typename } }", "Changes", json!({})));
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
        let mut session = CableSession::new(std::sync::Arc::new(crate::pacing::TestClock::default()));
        let _subscription = session.subscribe(GraphQLRequest::new("subscription { recordEvents { __typename } }", "Changes", json!({})));
        for _ in 0..3 { ready_then_close(&mut session, None); }
        session.ready();
        if frame.as_ref().is_some_and(|body| body["t"] == "pong") {
            tokio::time::advance(std::time::Duration::from_secs(25)).await;
            session.tick().unwrap().unwrap();
        }
        if let Some(frame) = frame.clone() { session.receive(frame).unwrap(); }
        session.closed(1006, "");
        let delay = session.failed_open(true).unwrap();
        let (first, second) = if resets { (1, 2) } else { (8, 16) };
        assert!(within_jitter(delay, first), "{frame:?}: {delay:?}");
        let delay = ready_then_close(&mut session, None);
        assert!(within_jitter(delay, second), "{frame:?}: {delay:?}");
    }
}
