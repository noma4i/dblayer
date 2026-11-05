//! `FeedIo` on `DetScheduler` with the mock transport. Requirement -> observed value -> assert:
//! feed_io_frames_commit: subscription frames are inputs of their connection, an event reaches the Store as a `Commit`.
//! feed_io_unsubscribe_closes: `Unsubscribe` closes the subscription and no frame reaches the owner after it.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use super::rig::{admit, collector, pump, pump_for, runtime, saw_input, BatchPeer, Batches};
use super::{FeedCodec, FeedIo};
use crate::owners::read::deliveries::Deliveries;
use crate::owners::read::feed::{self as f, Feed, FeedInput, Mode};
use crate::owners::read::windows::Windows;
use crate::owners::store::StoreWriter;
use crate::owners::support::{Discard, Sink};
use crate::runtime::{Address, DetScheduler, EpochStamp};
use crate::transport::{GraphQLRequest, MockTransport, Response, SubscriptionEvent};

const EPOCH: u64 = 1;

struct Models;

impl FeedCodec for Models {
    fn request(&self, models: &[String]) -> GraphQLRequest { GraphQLRequest::new("subscription Feed", "Feed", json!({ "models": models })) }

    fn event(&self, response: Response) -> Option<(String, String)> {
        let data = response.data?;
        Some((data["model"].as_str()?.to_string(), data["payload"].as_str()?.to_string()))
    }
}

fn event(payload: &str) -> SubscriptionEvent { SubscriptionEvent::Next(Response::data(json!({ "model": "Chat", "payload": payload }))) }

fn rig(script: Vec<SubscriptionEvent>) -> (DetScheduler, Address<Feed>, MockTransport, Batches, tokio::runtime::Runtime) {
    let runtime = runtime();
    let mock = MockTransport::new();
    mock.script_subscription("Feed", script);
    let batches = Batches::default();
    let (_, rest) = collector();
    let io = FeedIo::new(Arc::new(mock.clone()), Arc::new(Models), admit(), runtime.handle().clone(), rest);
    let mut det = DetScheduler::new(1);
    let store = det.add::<BatchPeer>("store", batches.clone(), Discard);
    let windows = det.add::<Sink<Windows>>("windows", Default::default(), Discard);
    let deliveries = det.add::<Sink<Deliveries>>("deliveries", Default::default(), Discard);
    let peers = f::FeedPeers { store: Address::<StoreWriter>::new(store.index()), windows: Address::<Windows>::new(windows.index()), deliveries: Address::<Deliveries>::new(deliveries.index()) };
    let feed = det.add::<Feed>("feed", f::FeedState::new(EPOCH, true, peers), io);
    det.start_epoch(EPOCH);
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantPlus { model: "Chat".into(), debounce_ms: 0, delivery: None });
    (det, feed, mock, batches, runtime)
}

#[test]
fn feed_io_frames_commit() {
    let (mut det, feed, _mock, batches, _runtime) = rig(vec![SubscriptionEvent::Ready, event("hello")]);
    let seen = batches.clone();
    pump(&mut det, feed, |_| !seen.all().is_empty());
    let landed = batches.all();
    assert_eq!(landed.len(), 1);
    assert_eq!(landed[0].rows, [("Chat".to_string(), "hello".to_string())]);
    assert_eq!(landed[0].epoch, EPOCH);
    assert!(saw_input(&det, "Frame") >= 2, "Ready and the event came as frames");
}

#[test]
fn feed_io_unsubscribe_closes() {
    let (mut det, feed, mock, _batches, runtime) = rig(vec![SubscriptionEvent::Ready]);
    pump(&mut det, feed, |det| det.state(feed).is_some_and(|state| state.mode() == Mode::Live));
    det.post(feed, EpochStamp::Epoch(EPOCH), FeedInput::WantMinus { model: "Chat".into(), delivery: None });
    pump(&mut det, feed, |det| det.trace().iter().any(|entry| entry.effects.contains("Unsubscribe")));
    runtime.block_on(async { tokio::time::timeout(Duration::from_secs(10), mock.await_cancellations("Feed", 1)).await.expect("the subscription was cancelled") });
    let before = saw_input(&det, "Frame");
    mock.emit("Feed", event("late"));
    pump_for(&mut det, feed, Duration::from_millis(500));
    assert_eq!(saw_input(&det, "Frame"), before, "no frame reaches the owner after Unsubscribe");
}
