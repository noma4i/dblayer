//! `FeedIo` on `DetScheduler` with a scripted cable line. Requirement -> observed value -> assert:
//! feed_io_frames_commit: the handshake, the subscribe frame with the first send counter, an opened frame as `FrameOpened`, an event reaches the Store as a `Commit`.
//! feed_io_unsubscribe_closes: the last `Want-` seals the unsubscribe frame with the next counter and closes the line, no frame reaches the owner after it.
//! feed_io_broken_next_drops: a `next` that is no response is a protocol error, the owner opens another connection and nothing lands.
//! feed_io_close_cancels_handshake: `Close` of a connection that waits in its handshake ends the open (the future is dropped).
//! feed_io_counter_mismatch_closes: a frame with a skipped counter closes the line and the owner opens another connection.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::feed::FramedEvent;
use super::rig::{collector, pump, pump_for, runtime, saw_input, BatchPeer, Batches};
use super::{FeedCodec, FeedIo};
use crate::owners::contract::Online;
use crate::owners::read::deliveries::Deliveries;
use crate::owners::read::feed::{self as f, Feed, FeedInput, Mode};
use crate::owners::read::windows::{StaleKeys, Windows};
use crate::owners::store::StoreWriter;
use crate::owners::support::{Discard, Sink};
use crate::runtime::{Address, DetScheduler, EpochStamp};
use crate::transport::{CableLine, CableLines, GraphQLRequest, Incoming, Response, TransportError};

const EPOCH: u64 = 1;

struct Models;

impl FeedCodec for Models {
    fn request(&self, channel: &str) -> GraphQLRequest { GraphQLRequest::new("subscription Feed", "Feed", json!({ "channel": channel })) }

    fn events(&self, _channel: &str, response: Response) -> Vec<FramedEvent> {
        let Some(data) = response.data else { return Vec::new() };
        match (data["model"].as_str(), data["payload"].as_str()) {
            (Some(model), Some(payload)) => vec![FramedEvent { model: model.into(), payload: payload.into(), reread: StaleKeys::none(), refused: false }],
            _ => Vec::new(),
        }
    }
}

struct Line {
    incoming: tokio::sync::Mutex<mpsc::UnboundedReceiver<Incoming>>,
    sent: Arc<Mutex<Vec<(u64, Value)>>>,
    closed: Arc<AtomicUsize>,
}

#[async_trait]
impl CableLine for Line {
    async fn send(&self, counter: u64, body: Value) -> Result<(), TransportError> {
        self.sent.lock().unwrap_or_else(PoisonError::into_inner).push((counter, body));
        Ok(())
    }

    async fn next(&self) -> Incoming { self.incoming.lock().await.recv().await.unwrap_or(Incoming::Closed { code: 1006, reason: String::new() }) }

    async fn close(&self) { self.closed.fetch_add(1, Ordering::SeqCst); }
}

struct Lines(Mutex<VecDeque<Arc<Line>>>);

#[async_trait]
impl CableLines for Lines {
    async fn connect(&self) -> Result<Arc<dyn CableLine>, TransportError> {
        let line = self.0.lock().unwrap_or_else(PoisonError::into_inner).pop_front().ok_or_else(|| TransportError::Network("no line".into()))?;
        Ok(line)
    }
}

struct Peer {
    frames: mpsc::UnboundedSender<Incoming>,
    sent: Arc<Mutex<Vec<(u64, Value)>>>,
    closed: Arc<AtomicUsize>,
}

fn line() -> (Arc<Line>, Peer) {
    let (frames, incoming) = mpsc::unbounded_channel();
    let (sent, closed) = (Arc::new(Mutex::new(Vec::new())), Arc::new(AtomicUsize::new(0)));
    (Arc::new(Line { incoming: tokio::sync::Mutex::new(incoming), sent: sent.clone(), closed: closed.clone() }), Peer { frames, sent, closed })
}

fn confirmed(counter: u64) -> Incoming { Incoming::Frame { counter, body: json!({"t":"subscribed", "id":"1"}) } }

fn next(counter: u64, payload: &str) -> Incoming { Incoming::Frame { counter, body: json!({"t":"next", "id":"1", "result":{"data":{"model":"Chat", "payload":payload}}}) } }

fn rig(lines: Vec<Arc<Line>>) -> (DetScheduler, Address<Feed>, Batches, tokio::runtime::Runtime) {
    let runtime = runtime();
    let batches = Batches::default();
    let (_, rest) = collector();
    let io = FeedIo::new(Arc::new(Lines(Mutex::new(lines.into()))), Arc::new(Models), runtime.handle().clone(), rest, (Arc::new(crate::pacing::SystemClock), super::rig::open_gate(1)));
    let mut det = DetScheduler::new(1);
    let store = det.add::<BatchPeer>("store", batches.clone(), Discard);
    let windows = det.add::<Sink<Windows>>("windows", Default::default(), Discard);
    let deliveries = det.add::<Sink<Deliveries>>("deliveries", Default::default(), Discard);
    let peers = f::FeedPeers { store: Address::<StoreWriter>::new(store.index()), windows: Address::<Windows>::new(windows.index()), deliveries: Address::<Deliveries>::new(deliveries.index()) };
    let feed = det.add::<Feed>("feed", f::FeedState::new(EPOCH, peers, [("Chat".to_string(), 0)].into(), [("Chat".to_string(), vec!["1".to_string()])].into()), io);
    det.start_epoch(EPOCH);
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::Online(Online(true)));
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantPlus { model: "Chat".into(), id: 1, stream: false });
    (det, feed, batches, runtime)
}

#[test]
fn feed_io_frames_commit() {
    let (line, peer) = line();
    let (mut det, feed, batches, _runtime) = rig(vec![line]);
    pump(&mut det, feed, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
    peer.frames.send(confirmed(1)).unwrap();
    peer.frames.send(next(2, "hello")).unwrap();
    let seen = batches.clone();
    pump(&mut det, feed, |_| !seen.all().is_empty());
    let landed = batches.all();
    assert_eq!(landed.len(), 1);
    assert_eq!(landed[0].rows, [("Chat".to_string(), "hello".to_string())]);
    assert_eq!(landed[0].epoch, EPOCH);
    let sent = peer.sent.lock().unwrap_or_else(PoisonError::into_inner).clone();
    assert_eq!(sent.len(), 1);
    assert_eq!((sent[0].0, sent[0].1["t"].as_str(), sent[0].1["id"].as_str(), sent[0].1["variables"]["channel"].as_str()), (f::FIRST_SEND_COUNTER, Some("subscribe"), Some("1"), Some("1")), "the subscribe frame is sealed with the first send counter");
    assert!(saw_input(&det, "HandshakeDone") >= 1 && saw_input(&det, "FrameOpened") >= 1);
}

#[test]
fn feed_io_unsubscribe_closes() {
    let (line, peer) = line();
    let (mut det, feed, _batches, runtime) = rig(vec![line]);
    pump(&mut det, feed, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantMinus { model: "Chat".into(), id: 1 });
    let closed = peer.closed.clone();
    pump(&mut det, feed, |_| closed.load(Ordering::SeqCst) >= 1);
    let sent = peer.sent.lock().unwrap_or_else(PoisonError::into_inner).clone();
    assert_eq!(sent.iter().map(|(counter, body)| (*counter, body["t"].as_str().unwrap_or(""))).collect::<Vec<_>>(), [(0, "subscribe"), (1, "unsubscribe")]);
    let before = saw_input(&det, "FrameOpened");
    let _ = peer.frames.send(next(1, "late"));
    runtime.block_on(async { tokio::time::sleep(Duration::from_millis(50)).await });
    pump_for(&mut det, feed, Duration::from_millis(300));
    assert_eq!(saw_input(&det, "FrameOpened"), before, "no frame reaches the owner after the line closed");
}

#[test]
fn feed_io_counter_mismatch_closes() {
    let (first, peer) = line();
    let (second, _other) = line();
    let (mut det, feed, batches, _runtime) = rig(vec![first, second]);
    pump(&mut det, feed, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
    peer.frames.send(next(5, "skipped")).unwrap();
    let closed = peer.closed.clone();
    pump(&mut det, feed, |_| closed.load(Ordering::SeqCst) >= 1);
    pump(&mut det, feed, |det| det.state(feed).is_some_and(|state| matches!(state.mode(), Mode::Live | Mode::Opening { .. }) && state.active_connection() != Some(1)));
    assert!(batches.all().is_empty(), "the frame with a skipped counter does not reach the Store");
}

#[test]
fn feed_io_broken_next_drops() {
    let (first, peer) = line();
    let (second, _other) = line();
    let (mut det, feed, batches, _runtime) = rig(vec![first, second]);
    pump(&mut det, feed, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
    peer.frames.send(Incoming::Frame { counter: 1, body: json!({"t":"next", "id":"1", "result":{}}) }).unwrap();
    pump(&mut det, feed, |det| det.state(feed).is_some_and(|state| matches!(state.mode(), Mode::Live | Mode::Opening { .. }) && state.active_connection() != Some(1)));
    assert!(batches.all().is_empty());
    assert!(saw_input(&det, "ProtocolError") >= 1, "the broken frame is an explicit protocol error");
}

#[test]
fn feed_io_graphql_errors_drop() {
    let cases = [
        ("errors_only", json!({"errors":[{"message":"denied"}]}), true),
        ("data_with_errors", json!({"data":{"model":"Chat", "payload":"p"}, "errors":[{"message":"denied"}]}), true),
        ("data", json!({"data":{"model":"Chat", "payload":"p"}}), false),
    ];
    for (name, result, drops) in cases {
        let (first, peer) = line();
        let (second, _other) = line();
        let (mut det, feed, batches, _runtime) = rig(vec![first, second]);
        pump(&mut det, feed, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
        peer.frames.send(confirmed(1)).unwrap();
        peer.frames.send(Incoming::Frame { counter: 2, body: json!({"t":"next", "id":"1", "result":result}) }).unwrap();
        if drops {
            pump(&mut det, feed, |det| det.state(feed).is_some_and(|state| matches!(state.mode(), Mode::Live | Mode::Opening { .. }) && state.active_connection() != Some(1)));
            assert!(saw_input(&det, "ProtocolError") >= 1, "{name}: the error frame is an explicit protocol error");
            assert!(batches.all().is_empty(), "{name}: no event reaches the Store");
        } else {
            let seen = batches.clone();
            pump(&mut det, feed, |_| !seen.all().is_empty());
            assert_eq!(saw_input(&det, "ProtocolError"), 0, "{name}: a clean frame is no error");
        }
    }
}

struct Hanging(Arc<AtomicUsize>, Arc<AtomicUsize>);

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); }
}

#[async_trait]
impl CableLines for Hanging {
    async fn connect(&self) -> Result<Arc<dyn CableLine>, TransportError> {
        let _guard = DropCount(self.0.clone());
        self.1.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }
}

#[test]
fn feed_io_close_cancels_handshake() {
    let runtime = runtime();
    let (dropped, started) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (_, rest) = collector();
    let io = FeedIo::new(Arc::new(Hanging(dropped.clone(), started.clone())), Arc::new(Models), runtime.handle().clone(), rest, (Arc::new(crate::pacing::SystemClock), super::rig::open_gate(1)));
    let mut det = DetScheduler::new(1);
    let store = det.add::<BatchPeer>("store", Batches::default(), Discard);
    let windows = det.add::<Sink<Windows>>("windows", Default::default(), Discard);
    let deliveries = det.add::<Sink<Deliveries>>("deliveries", Default::default(), Discard);
    let peers = f::FeedPeers { store: Address::<StoreWriter>::new(store.index()), windows: Address::<Windows>::new(windows.index()), deliveries: Address::<Deliveries>::new(deliveries.index()) };
    let feed = det.add::<Feed>("feed", f::FeedState::new(EPOCH, peers, [("Chat".to_string(), 0)].into(), [("Chat".to_string(), vec!["1".to_string()])].into()), io);
    det.start_epoch(EPOCH);
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::Online(Online(true)));
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantPlus { model: "Chat".into(), id: 1, stream: false });
    let began = started.clone();
    pump(&mut det, feed, |_| began.load(Ordering::SeqCst) >= 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 0, "the open waits");
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantMinus { model: "Chat".into(), id: 1 });
    let seen = dropped.clone();
    pump(&mut det, feed, |_| seen.load(Ordering::SeqCst) >= 1);
}

#[test]
fn feed_io_foreign_subscription_is_ignored() {
    let (line, peer) = line();
    let (mut det, feed, batches, _runtime) = rig(vec![line]);
    pump(&mut det, feed, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
    peer.frames.send(confirmed(1)).unwrap();
    peer.frames.send(Incoming::Frame { counter: 2, body: json!({"t":"error", "id":"2", "code":"BAD", "retryable":false}) }).unwrap();
    peer.frames.send(Incoming::Frame { counter: 3, body: json!({"t":"next", "id":"2", "result":{"data":{"model":"Chat", "payload":"theirs"}}}) }).unwrap();
    peer.frames.send(Incoming::Frame { counter: 4, body: json!({"t":"complete", "id":"2"}) }).unwrap();
    peer.frames.send(next(5, "mine")).unwrap();
    let seen = batches.clone();
    pump(&mut det, feed, |_| !seen.all().is_empty());
    assert_eq!(batches.all()[0].rows, [("Chat".to_string(), "mine".to_string())]);
    assert_eq!((saw_input(&det, "ProtocolError"), det.state(feed).map(|state| state.active_connection())), (0, Some(Some(1))), "frames of a channel this connection never subscribed neither end nor break it");
}
