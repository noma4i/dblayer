use super::*;
use crate::test_wait::within;
use crate::transport::{Connectivity, GraphQLRequest, SubscriptionEvent};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

fn new_session() -> CableSession {
    CableSession::new(Arc::new(crate::pacing::TestClock::default()), Connectivity::new(true))
}

fn request() -> GraphQLRequest {
    GraphQLRequest::new("subscription { recordEvents { __typename } }", "Changes", json!({}))
}

fn backoff_delay(session: &CableSession) -> Duration {
    session.deadline().unwrap() - session.now()
}

async fn reopen(session: &mut CableSession) {
    tokio::time::advance(backoff_delay(session)).await;
    session.tick();
    assert!(session.is_connecting());
}

fn ready(session: &mut CableSession) -> Vec<serde_json::Value> {
    session.opened();
    session.take_frames()
}

fn within_jitter(delay: Duration, ceiling_secs: u64) -> bool {
    let ceiling = Duration::from_secs(ceiling_secs);
    delay <= ceiling && delay >= ceiling.mul_f64(0.8)
}

async fn ready_then_close(session: &mut CableSession) -> Duration {
    ready(session);
    session.closed(1006, "");
    let delay = backoff_delay(session);
    reopen(session).await;
    delay
}

#[tokio::test(start_paused = true)]
async fn inv_wait_05_frames_postpone_the_single_deadline_without_timer_wakes() {
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::mpsc;
    use super::wire_tests::{FakeSocket, FakeConnect, Peer};
    let (incoming, receiver) = mpsc::unbounded_channel();
    let (outgoing, sent) = mpsc::unbounded_channel();
    let socket = Arc::new(FakeSocket { incoming: tokio::sync::Mutex::new(receiver), outgoing, closes: AtomicUsize::new(0) });
    let connect = Arc::new(FakeConnect { socket, opens: AtomicUsize::new(0) });
    let (kat, config, challenge, credential) = super::link_tests::inputs();
    let cable = CableSubscriptions::new(config.clone(), Arc::new(move |_| Ok(credential.clone())), connect, crate::transport::Connectivity::new(true), Arc::new(crate::pacing::TestClock::default()));
    let mut subscription = cable.subscribe(&test_admission(), GraphQLRequest::new("subscription { sample { id } }", "Sample", json!({}))).await.unwrap();
    let mut peer = Peer { incoming, outgoing: sent, send_key: [0; 32], receive_key: [0; 32], sequence: 0 };
    peer.handshake(&kat, &config, challenge).await;
    let subscribed = within(peer.next()).await;
    peer.emit(json!({"t":"subscribed", "id":subscribed["id"]}));
    assert!(matches!(within(subscription.next()).await, Some(SubscriptionEvent::Ready)));
    for number in 0..10 {
        tokio::time::advance(Duration::from_secs(3)).await;
        peer.emit(json!({"t":"next", "id":subscribed["id"], "result":{"data":{"sample":{"id":number}}}}));
        assert!(matches!(within(subscription.next()).await, Some(SubscriptionEvent::Next(_))));
        assert_eq!(cable.wake_count(), 0, "traffic caused a timer wake at frame {number}");
    }
    tokio::time::advance(Duration::from_secs(25)).await;
    assert_eq!(within(peer.next()).await["t"], "ping");
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
        let mut session = new_session();
        let _subscription = session.subscribe(test_admission(), GraphQLRequest::new("subscription { x }", "X", json!({})));
        for _ in 0..3 { ready_then_close(&mut session).await; }
        ready(&mut session);
        tokio::time::advance(Duration::from_secs(25)).await;
        session.tick();
        let deadline = session.deadline();
        let _ = session.receive(body.clone());
        let preserved_deadline = session.deadline() == deadline;
        session.closed(4400, "");
        let delay = backoff_delay(&session);
        if !within_jitter(delay, 8) || !preserved_deadline {
            failures.push(format!("{body}: delay {delay:?}, preserved deadline {preserved_deadline}"));
        }
    }
    assert!(failures.is_empty(), "invalid frames reset failure history: {failures:?}");
}

#[tokio::test(start_paused = true)]
async fn independent_cable_sessions_do_not_retry_in_lockstep() {
    let mut sessions: Vec<_> = (0..3).map(|_| {
        let mut session = new_session();
        let subscription = session.subscribe(test_admission(), GraphQLRequest::new("subscription { x }", "X", json!({})));
        (session, subscription)
    }).collect();
    let delays: Vec<_> = sessions.iter_mut().map(|(session, _)| { session.open_failed(); backoff_delay(session) }).collect();
    assert!(delays.windows(2).any(|pair| pair[0] != pair[1]), "independent sessions use identical delays: {delays:?}");
}

#[tokio::test(start_paused = true)]
async fn multiplexes_registered_subscriptions_and_cancels_only_the_selected_id() {
    let mut session = new_session();
    let mut subscriptions = Vec::new();
    let fixtures: Vec<serde_json::Value> = serde_json::from_str(include_str!("../../../../../fixtures/change-events/subscriptions.json")).unwrap();
    for fixture in &fixtures {
        let request = GraphQLRequest::new(fixture["query"].as_str().unwrap(), fixture["operationName"].as_str().unwrap(), fixture["variables"].clone());
        let (id, subscription) = session.subscribe(test_admission(), request);
        subscriptions.push((id, subscription));
    }
    let frames = ready(&mut session);
    assert_eq!(frames.len(), 4);
    for (((id, subscription), frame), fixture) in subscriptions.iter_mut().zip(&frames).zip(&fixtures) {
        assert_eq!(frame["t"], "subscribe");
        assert_eq!(frame["id"], *id);
        session.receive(json!({"t":"subscribed", "id":id})).unwrap();
        assert!(matches!(within(subscription.next()).await, Some(SubscriptionEvent::Ready)));
        session.receive(json!({"t":"next", "id":id, "result":fixture["result"]})).unwrap();
        let Some(SubscriptionEvent::Next(response)) = within(subscription.next()).await else { panic!("missing next") };
        assert_eq!(response.data.unwrap(), fixture["result"]["data"]);
    }
    let id = subscriptions[0].0.clone();
    session.unsubscribe(&id);
    assert_eq!(session.take_frames(), vec![json!({"t":"unsubscribe","id":id})]);
    assert!(within(subscriptions[0].1.next()).await.is_none());
    session.closed(1006, "");
    reopen(&mut session).await;
    assert_eq!(ready(&mut session).len(), 3);
}

#[tokio::test(start_paused = true)]
async fn ping_deadline_exists_only_for_live_subscriptions_and_expires_without_pong() {
    let mut session = new_session();
    for _ in 0..4 {
        tokio::time::advance(Duration::from_secs(25)).await;
        assert!(session.deadline().is_none());
    }
    let (id, _subscription) = session.subscribe(test_admission(), GraphQLRequest::new("subscription { recordEvents { __typename } }", "Changes", json!({})));
    ready(&mut session);
    let deadline = session.deadline().unwrap();
    assert_eq!(deadline - session.now(), Duration::from_secs(25));
    tokio::time::sleep(deadline - session.now()).await;
    session.tick();
    assert_eq!(session.take_frames()[0]["t"], "ping");
    assert_eq!(session.deadline().unwrap() - session.now(), Duration::from_secs(20));
    tokio::time::advance(Duration::from_secs(20)).await;
    session.tick();
    assert!(!session.is_open());
    session.unsubscribe(&id);
    assert!(session.deadline().is_none());
}

#[tokio::test(start_paused = true)]
async fn revoke_finishes_every_subscription_and_unavailable_keeps_them_for_retry() {
    for (code, reason, terminal) in [(1000, "LOGGED_OUT", true), (4403, "ACCOUNT_LOCKED", true), (4503, "SERVER_RESTART", false)] {
        let mut session = new_session();
        let (_, mut subscription) = session.subscribe(test_admission(), request());
        session.closed(code, reason);
        assert_eq!(session.deadline().is_none(), terminal, "{code} {reason}");
        if terminal {
            let next = tokio::time::timeout(Duration::from_secs(1), subscription.next()).await.expect("terminal failure delivered");
            let Some(SubscriptionEvent::Failed(failure)) = next else { panic!("terminal failure") };
            let close = failure.close.clone().unwrap();
            assert_eq!((close.code, close.terminal), (code, true));
            assert!(matches!(failure.error, crate::transport::TransportError::Session { .. }));
            assert!(tokio::time::timeout(Duration::from_secs(1), subscription.next()).await.expect("subscription ends").is_none());
            assert!(ready(&mut session).is_empty());
            let (_, mut late) = session.subscribe(test_admission(), request());
            assert!(matches!(tokio::time::timeout(Duration::from_secs(1), late.next()).await.expect("late failure delivered"), Some(SubscriptionEvent::Failed(_))));
            assert!(!session.is_connecting());
            session.resume();
            let _signed_in = session.subscribe(test_admission(), request());
            assert!(session.is_connecting());
        } else {
            reopen(&mut session).await;
            assert_eq!(ready(&mut session).len(), 1);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn offline_creates_no_work_and_online_resumes_the_demanded_connection() {
    let online = Connectivity::new(false);
    let mut session = CableSession::new(Arc::new(crate::pacing::TestClock::default()), online.clone());
    let _subscription = session.subscribe(test_admission(), request());
    assert!(!session.is_connecting() && session.deadline().is_none());
    online.set_online(true);
    session.connectivity();
    assert!(session.is_connecting());
    ready(&mut session);
    online.set_online(false);
    session.connectivity();
    assert!(!session.is_open() && session.deadline().is_none());
    online.set_online(true);
    session.connectivity();
    assert!(session.is_connecting());
}

#[tokio::test(start_paused = true)]
async fn failed_openings_increase_backoff_only_with_online_subscribers() {
    let online = Connectivity::new(true);
    let mut retry = CableSession::new(Arc::new(crate::pacing::TestClock::default()), online.clone());
    retry.open_failed();
    assert_eq!(retry.deadline(), None);
    let (_, subscription) = retry.subscribe(test_admission(), request());
    retry.open_failed();
    assert!(within_jitter(backoff_delay(&retry), 1));
    reopen(&mut retry).await;
    online.set_online(false);
    retry.connectivity();
    retry.open_failed();
    assert_eq!(retry.deadline(), None);
    online.set_online(true);
    retry.connectivity();
    retry.open_failed();
    assert!(within_jitter(backoff_delay(&retry), 2));
    reopen(&mut retry).await;
    retry.open_failed();
    assert!(within_jitter(backoff_delay(&retry), 4));
    reopen(&mut retry).await;
    ready(&mut retry);
    retry.receive(json!({"t":"subscribed", "id":"1"})).unwrap();
    retry.closed(1006, "");
    assert!(within_jitter(backoff_delay(&retry), 1));
    reopen(&mut retry).await;
    subscription.cancel();
    retry.cancelled().await;
    retry.open_failed();
    assert_eq!(retry.deadline(), None);
    let (_, _subscription) = retry.subscribe(test_admission(), request());
    retry.open_failed();
    assert!(within_jitter(backoff_delay(&retry), 2));
    reopen(&mut retry).await;
    retry.closed(4401, "UNAUTHENTICATED");
    retry.open_failed();
    assert_eq!(retry.deadline(), None);
}

#[tokio::test(start_paused = true)]
async fn closes_right_after_ready_grow_jittered_delays_up_to_the_cap() {
    let mut session = new_session();
    let _subscription = session.subscribe(test_admission(), request());
    let mut previous = Duration::ZERO;
    let mut below_ceiling = false;
    for ceiling in [1u64, 2, 4, 8, 16, 30, 30, 30] {
        let delay = ready_then_close(&mut session).await;
        assert!(within_jitter(delay, ceiling), "{delay:?} outside jitter band of {ceiling}s");
        if ceiling < 30 { assert!(delay > previous, "{delay:?} did not grow past {previous:?}"); }
        below_ceiling |= delay < Duration::from_secs(ceiling);
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
        let mut session = new_session();
        let _subscription = session.subscribe(test_admission(), request());
        for _ in 0..3 { ready_then_close(&mut session).await; }
        ready(&mut session);
        if frame.as_ref().is_some_and(|body| body["t"] == "pong") {
            tokio::time::advance(Duration::from_secs(25)).await;
            session.tick();
        }
        if let Some(frame) = frame.clone() { session.receive(frame).unwrap(); }
        session.closed(1006, "");
        let delay = backoff_delay(&session);
        reopen(&mut session).await;
        let (first, second) = if resets { (1, 2) } else { (8, 16) };
        assert!(within_jitter(delay, first), "{frame:?}: {delay:?}");
        let delay = ready_then_close(&mut session).await;
        assert!(within_jitter(delay, second), "{frame:?}: {delay:?}");
    }
}
