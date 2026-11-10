//! `FeedIo` on `DetScheduler` with a scripted cable line. Requirement -> observed value -> assert:
//! feed_io_frames_commit: the handshake, the subscribe frame with the first send counter, an opened frame as `FrameOpened`, an event reaches the Store as a `Commit`.
//! feed_io_unsubscribe_closes: the last `Want-` seals the unsubscribe frame with the next counter and closes the line, no frame reaches the owner after it.
//! feed_io_broken_next_drops: a `next` that is no response is a protocol error, the owner opens another connection and nothing lands.
//! feed_io_close_cancels_handshake: `Close` of a connection that waits in its handshake gives the call its cancel token, the call ends by itself.
//! feed_io_counter_mismatch_closes: a frame with a skipped counter closes the line and the owner opens another connection.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::feed::FramedEvent;
use super::rig::{collector, pump, runtime, saw_input, BatchPeer, Batches};
use super::{FeedCodec, FeedIo};
use crate::owners::contract::Online;
use crate::owners::read::deliveries::Deliveries;
use crate::owners::read::feed::{self as f, Feed, FeedInput, Mode};
use crate::owners::read::windows::{StaleKeys, Windows};
use crate::owners::store::StoreWriter;
use crate::spec::support::{Discard, Sink};
use crate::runtime::{Address, DetScheduler, EpochStamp};
use crate::transport::{CableLine, CableLines, GraphQLRequest, Incoming, Response, TransportError};

const EPOCH: u64 = 1;

struct Models;

impl FeedCodec for Models {
    fn request(&self, channel: &str, _variables: serde_json::Value) -> GraphQLRequest { GraphQLRequest::new("subscription Feed", "Feed", json!({ "channel": channel })) }

    fn events(&self, _channel: &str, response: Response) -> Vec<FramedEvent> {
        let Some(data) = response.data else { return Vec::new() };
        match (data["model"].as_str(), data["payload"].as_str()) {
            (Some(model), Some(payload)) => vec![FramedEvent { model: model.into(), payload: payload.into(), reread: StaleKeys::none(), refused: false }],
            _ => Vec::new(),
        }
    }
}

/// The end of a read of the line (a frame, a close or an abort of the task) frees its lock: the pumps look again.
struct WakeOnDrop;

impl Drop for WakeOnDrop {
    fn drop(&mut self) { crate::runtime::wake_pumps(); }
}

struct Line {
    incoming: tokio::sync::Mutex<mpsc::UnboundedReceiver<Incoming>>,
    sent: Arc<Mutex<Vec<(u64, Value)>>>,
    closed: Arc<AtomicUsize>,
    cancels: Arc<AtomicUsize>,
}

#[async_trait]
impl CableLine for Line {
    async fn send(&self, counter: u64, body: Value) -> Result<(), TransportError> {
        self.sent.lock().unwrap_or_else(PoisonError::into_inner).push((counter, body));
        crate::runtime::wake_pumps();
        Ok(())
    }

    async fn next(&self) -> Incoming {
        let _wake = WakeOnDrop;
        crate::test_wait::within(async { self.incoming.lock().await.recv().await }).await.unwrap_or(Incoming::Closed { code: None, reason: None, failure: None })
    }

    async fn close(&self) { self.closed.fetch_add(1, Ordering::SeqCst); crate::runtime::wake_pumps(); }

    async fn cancel(&self) { self.cancels.fetch_add(1, Ordering::SeqCst); crate::runtime::wake_pumps(); }
}

struct Lines(Mutex<VecDeque<Arc<Line>>>);

#[async_trait]
impl CableLines for Lines {
    async fn connect(&self, _: &crate::transport::CancelToken) -> Result<Arc<dyn CableLine>, crate::transport::CableConnectError> {
        let line = self.0.lock().unwrap_or_else(PoisonError::into_inner).pop_front().ok_or_else(|| TransportError::Network("no line".into()))?;
        Ok(line)
    }
}

struct Peer {
    frames: mpsc::UnboundedSender<Incoming>,
    sent: Arc<Mutex<Vec<(u64, Value)>>>,
    closed: Arc<AtomicUsize>,
    cancels: Arc<AtomicUsize>,
}

fn line() -> (Arc<Line>, Peer) {
    let (frames, incoming) = mpsc::unbounded_channel();
    let (sent, closed, cancels) = (Arc::new(Mutex::new(Vec::new())), Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    (Arc::new(Line { incoming: tokio::sync::Mutex::new(incoming), sent: sent.clone(), closed: closed.clone(), cancels: cancels.clone() }), Peer { frames, sent, closed, cancels })
}

fn confirmed(counter: u64) -> Incoming { Incoming::Frame { counter, body: json!({"t":"subscribed", "id":"1"}) } }

fn next(counter: u64, payload: &str) -> Incoming { Incoming::Frame { counter, body: json!({"t":"next", "id":"1", "result":{"data":{"model":"Chat", "payload":payload}}}) } }

#[test]
fn feed_close_reaches_the_delivery_stream() {
    use crate::owners::read::deliveries::{DeliveryInput, DeliveriesState, DeliveryPeers, Kind, Value, View};
    for (code, failure, intended, broken) in [(Some(4001), None, false, false), (None, Some(crate::transport::RawFailure::of_kind(crate::transport::RawFailureKind::Io, "lost")), false, false), (None, None, true, false), (None, None, false, true)] {
        let (line, peer) = line();
        let runtime = runtime();
        let (_, rest) = collector();
        let io = FeedIo::new(Arc::new(Lines(Mutex::new(vec![line].into()))), Arc::new(Models), runtime.handle().clone(), rest, super::rig::open_gate(1), Arc::new(|| 0));
        let mut det = DetScheduler::new(1);
        let store = det.add::<BatchPeer>("store", Batches::default(), Discard);
        let windows = det.add::<Sink<Windows>>("windows", Default::default(), Discard);
        let feed_at = Address::<Feed>::new(2);
        let deliveries_at = Address::<Deliveries>::new(3);
        let peers = f::FeedPeers { store: Address::new(store.index()), windows: Address::new(windows.index()), deliveries: deliveries_at };
        let feed = det.add::<Feed>("feed", f::FeedState::new(EPOCH, peers, [("Chat".to_string(), 0)].into(), [("Chat".to_string(), vec!["1".to_string()])].into()), io);
        let deliveries = det.add::<Deliveries>("deliveries", DeliveriesState::new(1, DeliveryPeers { windows: Address::new(windows.index()), feed: feed_at }), Discard);
        det.start_epoch(EPOCH);
        let view = View { kind: Kind::CableCloses, model: String::new(), tables: Vec::new(), local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: super::windows::WindowKey { model: String::new(), name: String::new() }, subject: String::new(), query: None };
        det.post(deliveries, EpochStamp::Epoch(EPOCH), DeliveryInput::Open { stream: 7, view, epoch: EPOCH, identity: true, valid: true, now: 0, revision: 0 });
        det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::Online { online: Online(true), online_seq: 1, now: crate::runtime::Time(0) });
        det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantPlus { model: "Chat".into(), id: 1, stream: false, scoped: Vec::new(), now: crate::runtime::Time(0) });
        pump(&mut det, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
        if intended || broken {
            if broken { peer.frames.send(confirmed(9)).unwrap(); } else { det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantMinus { model: "Chat".into(), id: 1, now: crate::runtime::Time(0) }); }
            if broken { pump(&mut det, |det| det.state(feed).is_some_and(|state| matches!(state.mode(), Mode::Draining { .. }))); }
            det.run_until_quiet(10_000).unwrap();
            det.tick(crate::runtime::Time(crate::owners::read::CLOSE_DRAIN.as_millis() as u64));
        } else {
            peer.frames.send(Incoming::Closed { code, reason: Some("closed".into()), failure }).unwrap();
        }
        pump(&mut det, |det| det.state(deliveries).is_some_and(|state| state.journal(7).iter().any(|value| matches!(value, Value::CableClose { .. }))));
        let journal = det.state(deliveries).unwrap().journal(7);
        let events: Vec<_> = journal.iter().filter_map(|value| if let Value::CableClose { event, .. } = value { Some(event) } else { None }).collect();
        assert_eq!(events.len(), 1);
        assert_eq!((events[0].code, events[0].intended), (code, intended));
        let expected = if intended || broken { f::CableCloseInitiator::Client } else if code.is_some() { f::CableCloseInitiator::Server } else { f::CableCloseInitiator::Network };
        assert_eq!(events[0].initiator, expected);
        if expected == f::CableCloseInitiator::Network { assert_eq!(events[0].failure.as_ref().unwrap().message, "lost"); }
    }
}

fn rig(lines: Vec<Arc<Line>>) -> (DetScheduler, Address<Feed>, Batches, tokio::runtime::Runtime) { rig_over(lines, super::rig::open_gate(1)) }

fn rig_over(lines: Vec<Arc<Line>>, gate: Arc<crate::runtime::EpochGate>) -> (DetScheduler, Address<Feed>, Batches, tokio::runtime::Runtime) {
    let runtime = runtime();
    let batches = Batches::default();
    let (_, rest) = collector();
    let io = FeedIo::new(Arc::new(Lines(Mutex::new(lines.into()))), Arc::new(Models), runtime.handle().clone(), rest, gate, Arc::new(|| 0));
    let mut det = DetScheduler::new(1);
    let store = det.add::<BatchPeer>("store", batches.clone(), Discard);
    let windows = det.add::<Sink<Windows>>("windows", Default::default(), Discard);
    let deliveries = det.add::<Sink<Deliveries>>("deliveries", Default::default(), Discard);
    let peers = f::FeedPeers { store: Address::<StoreWriter>::new(store.index()), windows: Address::<Windows>::new(windows.index()), deliveries: Address::<Deliveries>::new(deliveries.index()) };
    let feed = det.add::<Feed>("feed", f::FeedState::new(EPOCH, peers, [("Chat".to_string(), 0)].into(), [("Chat".to_string(), vec!["1".to_string()])].into()), io);
    det.start_epoch(EPOCH);
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::Online { online: Online(true), online_seq: 1, now: crate::runtime::Time(0) });
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantPlus { model: "Chat".into(), id: 1, stream: false, scoped: Vec::new(), now: crate::runtime::Time(0) });
    (det, feed, batches, runtime)
}

#[test]
fn feed_io_frames_commit() {
    let (line, peer) = line();
    let (mut det, feed, batches, _runtime) = rig(vec![line]);
    pump(&mut det, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
    peer.frames.send(confirmed(1)).unwrap();
    peer.frames.send(next(2, "hello")).unwrap();
    let (seen, sent_frames) = (batches.clone(), peer.sent.clone());
    pump(&mut det, |_| !seen.all().is_empty() && !sent_frames.lock().unwrap_or_else(PoisonError::into_inner).is_empty());
    let landed = batches.all();
    assert_eq!(landed.len(), 1);
    assert_eq!(landed[0].rows, [("Chat".to_string(), "hello".to_string())]);
    assert_eq!(landed[0].stamp, crate::runtime::EpochStamp::Epoch(EPOCH));
    let sent = peer.sent.lock().unwrap_or_else(PoisonError::into_inner).clone();
    assert_eq!(sent.len(), 1);
    assert_eq!((sent[0].0, sent[0].1["t"].as_str(), sent[0].1["id"].as_str(), sent[0].1["variables"]["channel"].as_str()), (f::FIRST_SEND_COUNTER, Some("subscribe"), Some("1"), Some("1")), "the subscribe frame is sealed with the first send counter");
    assert_eq!((saw_input(&det, "HandshakeDone"), saw_input(&det, "FrameOpened")), (1, 2), "one handshake, the frames confirmed and next");
}

#[test]
fn feed_io_close_drains_then_cancels_once() {
    let (line, peer) = line();
    let (mut det, feed, _batches, _runtime) = rig(vec![line]);
    pump(&mut det, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
    peer.frames.send(confirmed(1)).unwrap();
    det.close();
    let closed = peer.closed.clone();
    pump(&mut det, |_| closed.load(Ordering::SeqCst) == 1);
    assert_eq!((peer.cancels.load(Ordering::SeqCst), saw_input(&det, "Drain")), (0, 0), "the queue is sent and the line is closed, the drain runs");
    let half = crate::owners::read::CLOSE_DRAIN.as_millis() as u64 / 2;
    det.tick(crate::runtime::Time(half));
    det.run_until_quiet(10_000).expect("quiet");
    assert_eq!((peer.cancels.load(Ordering::SeqCst), saw_input(&det, "Drain")), (0, 0), "half of the drain: no cancel");
    det.tick(crate::runtime::Time(2 * half));
    let cancels = peer.cancels.clone();
    pump(&mut det, |_| cancels.load(Ordering::SeqCst) == 1);
    assert_eq!((peer.closed.load(Ordering::SeqCst), saw_input(&det, "Drain"), det.state(feed).map(|state| state.mode())), (1, 1, Some(Mode::Ended)), "one close, one drain tick, one cancel");
}

#[test]
fn feed_io_unsubscribe_closes() {
    let (line, peer) = line();
    let (mut det, feed, _batches, _runtime) = rig(vec![line.clone()]);
    pump(&mut det, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantMinus { model: "Chat".into(), id: 1, now: crate::runtime::Time(0) });
    let closed = peer.closed.clone();
    pump(&mut det, |_| closed.load(Ordering::SeqCst) >= 1);
    let sent = peer.sent.lock().unwrap_or_else(PoisonError::into_inner).clone();
    assert_eq!(sent.iter().map(|(counter, body)| (*counter, body["t"].as_str().unwrap_or(""))).collect::<Vec<_>>(), [(0, "subscribe"), (1, "unsubscribe")]);
    let before = saw_input(&det, "FrameOpened");
    pump(&mut det, |_| line.incoming.try_lock().is_ok());
    let _ = peer.frames.send(next(1, "late"));
    pump(&mut det, |_| true);
    assert_eq!(saw_input(&det, "FrameOpened"), before, "no frame reaches the owner after the line closed");
}

#[test]
fn feed_io_counter_mismatch_closes() {
    let (first, peer) = line();
    let (second, _other) = line();
    let (mut det, feed, batches, _runtime) = rig(vec![first, second]);
    pump(&mut det, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
    peer.frames.send(next(5, "skipped")).unwrap();
    let closed = peer.closed.clone();
    pump(&mut det, |_| closed.load(Ordering::SeqCst) >= 1);
    assert!(det.state(feed).is_some_and(|state| matches!(state.mode(), Mode::Draining { then: f::Resume::Reopen, .. })), "the broken connection drains before another opens");
    det.tick(crate::runtime::Time(crate::owners::read::CLOSE_DRAIN.as_millis() as u64));
    pump(&mut det, |det| det.state(feed).is_some_and(|state| matches!(state.mode(), Mode::Live | Mode::Opening { .. }) && state.active_connection() != Some(1)));
    let cancels = peer.cancels.clone();
    pump(&mut det, |_| cancels.load(Ordering::SeqCst) == 1);
    assert_eq!((peer.closed.load(Ordering::SeqCst), peer.cancels.load(Ordering::SeqCst)), (1, 1), "the drain ended with one close and one cancel");
    assert!(batches.all().is_empty(), "the frame with a skipped counter does not reach the Store");
}

#[test]
fn feed_io_broken_next_drops() {
    let (first, peer) = line();
    let (second, _other) = line();
    let (mut det, feed, batches, _runtime) = rig(vec![first, second]);
    pump(&mut det, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
    peer.frames.send(Incoming::Frame { counter: 1, body: json!({"t":"next", "id":"1", "result":{}}) }).unwrap();
    pump(&mut det, |det| saw_input(det, "ProtocolError") == 1);
    det.tick(crate::runtime::Time(crate::owners::read::CLOSE_DRAIN.as_millis() as u64));
    pump(&mut det, |det| det.state(feed).is_some_and(|state| matches!(state.mode(), Mode::Live | Mode::Opening { .. }) && state.active_connection() != Some(1)));
    assert!(batches.all().is_empty());
    assert!(saw_input(&det, "ProtocolError") == 1, "the broken frame is an explicit protocol error");
}

fn two_channel_rig(lines: Vec<Arc<Line>>) -> (DetScheduler, Address<Feed>, Batches, tokio::runtime::Runtime) {
    let runtime = runtime();
    let batches = Batches::default();
    let (_, rest) = collector();
    let io = FeedIo::new(Arc::new(Lines(Mutex::new(lines.into()))), Arc::new(Models), runtime.handle().clone(), rest, super::rig::open_gate(1), Arc::new(|| 0));
    let mut det = DetScheduler::new(1);
    let store = det.add::<BatchPeer>("store", batches.clone(), Discard);
    let windows = det.add::<Sink<Windows>>("windows", Default::default(), Discard);
    let deliveries = det.add::<Sink<Deliveries>>("deliveries", Default::default(), Discard);
    let peers = f::FeedPeers { store: Address::<StoreWriter>::new(store.index()), windows: Address::<Windows>::new(windows.index()), deliveries: Address::<Deliveries>::new(deliveries.index()) };
    let feed = det.add::<Feed>("feed", f::FeedState::new(EPOCH, peers, [("Chat".to_string(), 0), ("Room".to_string(), 0)].into(), [("Chat".to_string(), vec!["1".to_string()]), ("Room".to_string(), vec!["2".to_string()])].into()), io);
    det.start_epoch(EPOCH);
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::Online { online: Online(true), online_seq: 1, now: crate::runtime::Time(0) });
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantPlus { model: "Chat".into(), id: 1, stream: false, scoped: Vec::new(), now: crate::runtime::Time(0) });
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantPlus { model: "Room".into(), id: 1, stream: false, scoped: Vec::new(), now: crate::runtime::Time(0) });
    (det, feed, batches, runtime)
}

#[test]
fn feed_io_graphql_errors_drop() {
    for (name, result, errored) in [
        ("errors_only", json!({"errors":[{"message":"denied"}]}), true),
        ("data_with_errors", json!({"data":{"model":"Chat", "payload":"p"}, "errors":[{"message":"denied"}]}), true),
        ("data", json!({"data":{"model":"Chat", "payload":"p"}}), false),
    ] {
        let (first, peer) = line();
        let (second, _other) = line();
        let (mut det, feed, batches, _runtime) = two_channel_rig(vec![first, second]);
        pump(&mut det, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
        peer.frames.send(confirmed(1)).unwrap();
        peer.frames.send(Incoming::Frame { counter: 2, body: json!({"t":"next", "id":"1", "result":result}) }).unwrap();
        if errored {
            pump(&mut det, |det| saw_input(det, "FrameOpened") >= 2 || saw_input(det, "ProtocolError") >= 1);
            assert_eq!(saw_input(&det, "ProtocolError"), 0, "{name}: a next with errors ends only its channel, not the connection");
            assert_eq!(det.state(feed).map(|state| state.active_connection()), Some(Some(1)), "{name}: the connection and the other channel stay");
            assert!(batches.all().is_empty(), "{name}: no event reaches the Store");
        } else {
            let seen = batches.clone();
            pump(&mut det, |_| !seen.all().is_empty());
            assert_eq!(saw_input(&det, "ProtocolError"), 0, "{name}: a clean frame is no error");
        }
    }
}

struct Hanging(Arc<AtomicUsize>, Arc<AtomicUsize>);

#[async_trait]
impl CableLines for Hanging {
    async fn connect(&self, cancel: &crate::transport::CancelToken) -> Result<Arc<dyn CableLine>, crate::transport::CableConnectError> {
        self.1.fetch_add(1, Ordering::SeqCst);
        crate::runtime::wake_pumps();
        cancel.cancelled().await;
        self.0.fetch_add(1, Ordering::SeqCst);
        crate::runtime::wake_pumps();
        Err(TransportError::Interrupted("cancelled".into()).into())
    }
}

#[test]
fn feed_io_close_cancels_handshake() {
    let runtime = runtime();
    let (dropped, started) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (_, rest) = collector();
    let io = FeedIo::new(Arc::new(Hanging(dropped.clone(), started.clone())), Arc::new(Models), runtime.handle().clone(), rest, super::rig::open_gate(1), Arc::new(|| 0));
    let mut det = DetScheduler::new(1);
    let store = det.add::<BatchPeer>("store", Batches::default(), Discard);
    let windows = det.add::<Sink<Windows>>("windows", Default::default(), Discard);
    let deliveries = det.add::<Sink<Deliveries>>("deliveries", Default::default(), Discard);
    let peers = f::FeedPeers { store: Address::<StoreWriter>::new(store.index()), windows: Address::<Windows>::new(windows.index()), deliveries: Address::<Deliveries>::new(deliveries.index()) };
    let feed = det.add::<Feed>("feed", f::FeedState::new(EPOCH, peers, [("Chat".to_string(), 0)].into(), [("Chat".to_string(), vec!["1".to_string()])].into()), io);
    det.start_epoch(EPOCH);
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::Online { online: Online(true), online_seq: 1, now: crate::runtime::Time(0) });
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantPlus { model: "Chat".into(), id: 1, stream: false, scoped: Vec::new(), now: crate::runtime::Time(0) });
    let began = started.clone();
    pump(&mut det, |_| began.load(Ordering::SeqCst) >= 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 0, "the open waits");
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantMinus { model: "Chat".into(), id: 1, now: crate::runtime::Time(0) });
    let seen = dropped.clone();
    pump(&mut det, |_| seen.load(Ordering::SeqCst) >= 1);
}

#[test]
fn feed_io_foreign_subscription_is_ignored() {
    let (line, peer) = line();
    let (mut det, feed, batches, _runtime) = rig(vec![line]);
    pump(&mut det, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
    peer.frames.send(confirmed(1)).unwrap();
    peer.frames.send(Incoming::Frame { counter: 2, body: json!({"t":"error", "id":"2", "code":"BAD", "retryable":false}) }).unwrap();
    peer.frames.send(Incoming::Frame { counter: 3, body: json!({"t":"next", "id":"2", "result":{"data":{"model":"Chat", "payload":"theirs"}}}) }).unwrap();
    peer.frames.send(Incoming::Frame { counter: 4, body: json!({"t":"complete", "id":"2"}) }).unwrap();
    peer.frames.send(next(5, "mine")).unwrap();
    let seen = batches.clone();
    pump(&mut det, |_| !seen.all().is_empty());
    assert_eq!(batches.all()[0].rows, [("Chat".to_string(), "mine".to_string())]);
    assert_eq!((saw_input(&det, "ProtocolError"), det.state(feed).map(|state| state.active_connection())), (0, Some(Some(1))), "frames of a channel this connection never subscribed neither end nor break it");
}

/// P-RT-14 for Feed: a connection requested after the close of the epoch never reaches the lines of the old transport.
#[test]
fn rt_prop_close_against_query_no_old_transport_feed() {
    let runtime = runtime();
    let (dropped, started) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (_, rest) = collector();
    let gate = super::rig::open_gate(1);
    let io = FeedIo::new(Arc::new(Hanging(dropped, started.clone())), Arc::new(Models), runtime.handle().clone(), rest, gate.clone(), Arc::new(|| 0));
    let mut det = DetScheduler::new(1);
    let store = det.add::<BatchPeer>("store", Batches::default(), Discard);
    let windows = det.add::<Sink<Windows>>("windows", Default::default(), Discard);
    let deliveries = det.add::<Sink<Deliveries>>("deliveries", Default::default(), Discard);
    let peers = f::FeedPeers { store: Address::<StoreWriter>::new(store.index()), windows: Address::<Windows>::new(windows.index()), deliveries: Address::<Deliveries>::new(deliveries.index()) };
    let feed = det.add::<Feed>("feed", f::FeedState::new(EPOCH, peers, [("Chat".to_string(), 0)].into(), [("Chat".to_string(), vec!["1".to_string()])].into()), io);
    det.start_epoch(EPOCH);
    gate.close(EPOCH);
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::Online { online: Online(true), online_seq: 1, now: crate::runtime::Time(0) });
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantPlus { model: "Chat".into(), id: 1, stream: false, scoped: Vec::new(), now: crate::runtime::Time(0) });
    pump(&mut det, |det| saw_input(det, "Dropped") >= 1);
    assert_eq!(started.load(Ordering::SeqCst), 0, "a connect reached the old lines after the close");
}

/// P-RT-14 for Feed with an open cable: a frame sent after the close of the epoch never reaches the old connection.
#[test]
fn rt_prop_close_against_send_no_old_connection_feed() {
    let (line, peer) = line();
    let gate = super::rig::open_gate(1);
    let (mut det, feed, _batches, _runtime) = rig_over(vec![line], gate.clone());
    let sent = peer.sent.clone();
    pump(&mut det, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live) && sent.lock().unwrap_or_else(PoisonError::into_inner).len() == 1);
    gate.close(EPOCH);
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantMinus { model: "Chat".into(), id: 1, now: crate::runtime::Time(0) });
    pump(&mut det, |det| saw_input(det, "Dropped") >= 1);
    let late: Vec<_> = peer.sent.lock().unwrap_or_else(PoisonError::into_inner).iter().skip(1).map(|(counter, body)| (*counter, body["t"].as_str().unwrap_or("").to_string())).collect();
    assert!(late.is_empty(), "late frames reached the old connection after the close: {late:?}");
}
