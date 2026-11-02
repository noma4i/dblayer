use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::load_tests::held_rig;
use super::{LiveChange, QueryListener, StateSubscription};
use crate::test_wait::{recv_within, within_secs, yield_until};

struct Offline {
    sync: crate::sync::Sync,
    calls: AtomicUsize,
    seen: Mutex<Vec<u64>>,
}

impl QueryListener for Offline {
    fn changed(&self, change: LiveChange) {
        self.seen.lock().unwrap().push(change.seq);
        if self.calls.fetch_add(1, Ordering::SeqCst) < 4 {
            self.sync.set_online(false);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn state_callback_reenters_owner_on_the_state_follower() {
    let (sender, receiver) = tokio::sync::watch::channel(0u32);
    let sender = Arc::new(sender);
    let (heard, deliveries) = std::sync::mpsc::channel();
    let publisher = sender.clone();
    let subscription = StateSubscription::follow(receiver, move |state| {
        if state == 0 { publisher.send_replace(1); }
        heard.send(state).unwrap();
    }).unwrap();
    assert_eq!(recv_within(&deliveries), 0);
    assert_eq!(recv_within(&deliveries), 1);
    subscription.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn state_callback_reenters_owner_on_the_relation_follower() {
    let (db, _transport, hold) = held_rig().await;
    let listener = Arc::new(Offline { sync: db.sync().unwrap().clone(), calls: AtomicUsize::new(0), seen: Mutex::new(Vec::new()) });
    let live = db.live_query(r#"{"model":"Moment","relation":"feed","params":{}}"#.into(), listener.clone());
    yield_until("the listener never saw a delivery", || listener.calls.load(Ordering::SeqCst) > 0).await;
    hold.add_permits(1);
    let _ = within_secs(30, live.wait_for_initial_load()).await;
    let seen = listener.seen.lock().unwrap().clone();
    assert!(seen.windows(2).all(|pair| pair[0] < pair[1]), "deliveries went back: {seen:?}");
    live.cancel();
}
