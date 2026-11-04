use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use super::node_tests::{data, node_rig};
use super::{RefreshError, RefreshSink};
use crate::test_wait::{within, yield_until};

type Told = Arc<Mutex<Vec<(String, Option<RefreshError>)>>>;

fn sink() -> (RefreshSink, Told) {
    let told: Told = Arc::default();
    let into = told.clone();
    (Arc::new(move |id, error| into.lock().unwrap().push((id.to_string(), error))), told)
}

async fn told(told: &Told, count: usize) -> Vec<Option<RefreshError>> {
    within(async { while told.lock().unwrap().len() < count { tokio::time::sleep(Duration::from_millis(10)).await; } }).await;
    told.lock().unwrap().iter().map(|(_, error)| error.clone()).collect()
}

fn ids(id: &str) -> Vec<String> {
    vec![id.to_string()]
}

#[tokio::test(start_paused = true)]
async fn two_readers_of_one_entity_send_one_request_and_a_stale_read_of_a_fresh_row_sends_none() {
    let rig = node_rig();
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c1", "title": "one"}})).after(Duration::from_secs(1)));
    let (sink, got) = sink();
    rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c1"), super::EntityFetch::Force, sink.clone()).unwrap();
    rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c1"), super::EntityFetch::Force, sink.clone()).unwrap();
    assert_eq!(told(&got, 2).await, [None, None]);
    assert_eq!(rig.transport.call_count("ChatNode"), 1);
    rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c1"), super::EntityFetch::Stale, sink.clone()).unwrap();
    assert_eq!(told(&got, 3).await.len(), 3);
    assert_eq!(rig.transport.call_count("ChatNode"), 1);
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c1", "title": "two"}})));
    rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c1"), super::EntityFetch::Force, sink).unwrap();
    told(&got, 4).await;
    assert_eq!(rig.transport.call_count("ChatNode"), 2);
}

#[tokio::test(start_paused = true)]
async fn a_stale_read_of_a_missing_row_sends_one_request() {
    let rig = node_rig();
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c2", "title": "x"}})));
    let (sink, got) = sink();
    rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c2"), super::EntityFetch::Stale, sink).unwrap();
    told(&got, 1).await;
    assert_eq!(rig.transport.call_count("ChatNode"), 1);
}

#[tokio::test(start_paused = true)]
async fn an_offline_refresh_reports_offline_and_repeats_once_when_the_network_returns() {
    let rig = node_rig();
    rig.sync.set_online(false);
    let (sink, got) = sink();
    rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c1"), super::EntityFetch::Force, sink).unwrap();
    assert_eq!(told(&got, 1).await, [Some(RefreshError::Offline)]);
    assert_eq!(rig.transport.call_count("ChatNode"), 0);
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c1", "title": "back"}})));
    rig.sync.set_online(true);
    assert_eq!(told(&got, 2).await, [Some(RefreshError::Offline), None]);
    assert_eq!(rig.transport.call_count("ChatNode"), 1);
}

#[tokio::test(start_paused = true)]
async fn a_model_without_a_node_read_is_refused() {
    let rig = super::rig::rig();
    let (sink, _) = sink();
    assert!(rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "moments", &ids("m1"), super::EntityFetch::Force, sink).is_err());
}

#[tokio::test(start_paused = true)]
async fn entity_fetch_presence_modes() {
    for (mode, present, negative, expected) in [
        (super::EntityFetch::IfMissing, true, false, 0),
        (super::EntityFetch::IfMissing, false, false, 1),
        (super::EntityFetch::IfMissing, false, true, 1),
        (super::EntityFetch::Stale, true, false, 1),
        (super::EntityFetch::Force, true, false, 1),
    ] {
        let rig = node_rig();
        if negative {
            rig.transport.enqueue("ChatNode", data(json!({"node": null})));
            rig.sync.node(&rig.sync.inner.db.admit().unwrap(), "chats", "node", "c1", true).await.unwrap();
        }
        if present { super::rig::put(&rig, "chats", json!({"id":"c1", "title":"from list"})); }
        let before = rig.transport.call_count("ChatNode");
        rig.transport.enqueue("ChatNode", data(json!({"node":{"id":"c1", "title":"node"}})));
        let (sink, got) = sink();
        rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c1"), mode, sink).unwrap();
        assert_eq!(told(&got, 1).await, [None]);
        assert_eq!(rig.transport.call_count("ChatNode") - before, expected, "{mode:?}, present={present}, negative={negative}");
    }
}

#[tokio::test(start_paused = true)]
async fn entity_if_missing_online() {
    for arriving in [false, true] {
        let rig = node_rig();
        rig.sync.set_online(false);
        let (sink, got) = sink();
        rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c1"), super::EntityFetch::IfMissing, sink).unwrap();
        assert_eq!(told(&got, 1).await, [Some(RefreshError::Offline)]);
        assert_eq!(rig.transport.call_count("ChatNode"), 0);
        if arriving { super::rig::put(&rig, "chats", json!({"id":"c1", "title":"list while offline"})); }
        rig.transport.enqueue("ChatNode", data(json!({"node":{"id":"c1", "title":"online"}})));
        rig.sync.set_online(true);
        assert_eq!(told(&got, 2).await.last(), Some(&None));
        assert_eq!(rig.transport.call_count("ChatNode"), usize::from(!arriving));
    }
}

#[tokio::test(start_paused = true)]
async fn entity_if_missing_read_error() {
    let rig = node_rig();
    rig.db.shared.store.fail_next_entity_reads(1);
    let (sink, got) = sink();
    rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c1"), super::EntityFetch::IfMissing, sink).unwrap();
    assert!(matches!(&told(&got, 1).await[0], Some(RefreshError::Failed(_))));
    assert_eq!(rig.transport.call_count("ChatNode"), 0);
}

#[tokio::test(start_paused = true)]
async fn entity_if_missing_shared() {
    let rig = node_rig();
    let (sink, got) = sink();
    rig.transport.enqueue("ChatNode", data(json!({"node":{"id":"c1", "title":"shared"}})).after(Duration::from_secs(1)));
    rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c1"), super::EntityFetch::IfMissing, sink.clone()).unwrap();
    rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c1"), super::EntityFetch::IfMissing, sink.clone()).unwrap();
    assert_eq!(told(&got, 2).await, [None, None]);
    assert_eq!(rig.transport.call_count("ChatNode"), 1);
    rig.sync.set_online(false);
    let cancelled = rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c2"), super::EntityFetch::IfMissing, sink.clone()).unwrap();
    told(&got, 3).await;
    rig.sync.cancel_entity_refresh(&cancelled);
    assert!(rig.sync.inner.entity_due.lock().unwrap().is_empty());
    rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c3"), super::EntityFetch::IfMissing, sink).unwrap();
    told(&got, 4).await;
    rig.db.reset_all().unwrap();
    rig.sync.set_online(true);
    tokio::task::yield_now().await;
    assert_eq!(rig.transport.call_count("ChatNode"), 1);
    assert_eq!(got.lock().unwrap().len(), 4);
}

struct EntityClock {
    clock: crate::pacing::TestClock,
    sleeps: std::sync::atomic::AtomicUsize,
    wakes: std::sync::atomic::AtomicUsize,
}
impl crate::pacing::Clock for EntityClock {
    fn now(&self) -> Duration { crate::pacing::Clock::now(&self.clock) }
    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output=()> + Send + '_>> {
        use std::sync::atomic::Ordering;
        self.sleeps.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { crate::pacing::Clock::sleep(&self.clock, duration).await; self.wakes.fetch_add(1, Ordering::SeqCst); })
    }
}

#[tokio::test]
async fn entity_if_missing_idle() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let rig = node_rig();
    let clock = Arc::new(EntityClock { clock: rig.clock.clone(), sleeps: AtomicUsize::new(0), wakes: AtomicUsize::new(0) });
    let sync = super::Sync::new(rig.db.clone(), Arc::new(rig.transport.clone()), clock.clone(), Default::default());
    sync.register(super::OperationDef::from_document("ChatNode", "query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title } } }").unwrap());
    sync.set_online(false);
    let (sink, got) = sink();
    sync.refresh_entities(&sync.inner.db.admit().unwrap(), "chats", &ids("c1"), super::EntityFetch::IfMissing, sink).unwrap();
    told(&got, 1).await;
    let before = (clock.sleeps.load(Ordering::SeqCst), clock.wakes.load(Ordering::SeqCst));
    rig.db.shared.store.fail_next_entity_reads(1);
    tokio::time::pause();
    rig.clock.advance(Duration::from_secs(10));
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    assert_eq!(before, (clock.sleeps.load(Ordering::SeqCst), clock.wakes.load(Ordering::SeqCst)));
    assert_eq!(got.lock().unwrap().len(), 1);
    assert_eq!(rig.transport.call_count("ChatNode"), 0);
    sync.set_online(true);
    assert!(matches!(&told(&got, 2).await[1], Some(RefreshError::Failed(_))));
}

struct DropProbe(std::sync::Weak<super::Inner>, Arc<Mutex<Vec<bool>>>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        let free = self.0.upgrade().is_some_and(|inner| inner.entity_due.try_lock().is_ok());
        self.1.lock().unwrap().push(free);
    }
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_parked_refresh_releases_its_sink_outside_the_due_lock() {
    let rig = node_rig();
    rig.sync.set_online(false);
    let seen: Arc<Mutex<Vec<bool>>> = Arc::default();
    let probe = DropProbe(Arc::downgrade(&rig.sync.inner), seen.clone());
    let sink: RefreshSink = Arc::new(move |_, _| { let _keep = &probe; });
    let cancel = rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c1"), super::EntityFetch::Force, sink.clone()).unwrap();
    yield_until("the refresh parked", || !rig.sync.inner.entity_due.lock().unwrap().is_empty()).await;
    yield_until("the task released its copy", || Arc::strong_count(&sink) == 2).await;
    drop(sink);
    assert!(seen.lock().unwrap().is_empty());
    rig.sync.cancel_entity_refresh(&cancel);
    assert_eq!(*seen.lock().unwrap(), [true]);
}

#[tokio::test(start_paused = true)]
async fn a_pending_refresh_does_not_keep_the_database_alive() {
    let rig = node_rig();
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c1", "title": "slow"}})).after(Duration::from_secs(3600)));
    let (sink, _) = sink();
    rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c1"), super::EntityFetch::Force, sink.clone()).unwrap();
    yield_until("the request is in flight", || rig.transport.call_count("ChatNode") == 1).await;
    assert!(Arc::strong_count(&rig.sync.inner) >= 2, "the task holds the inner state");
    rig.sync.close();
    yield_until("the task ended with the close", || Arc::strong_count(&rig.sync.inner) == 1).await;
    assert!(matches!(rig.sync.inner.db.admit(), Err(crate::db::DbError::NoSession { .. })));
    for _ in 0..8 { tokio::task::yield_now().await; }
    assert_eq!(rig.transport.call_count("ChatNode"), 1, "a closed database sends nothing");
}

#[tokio::test(start_paused = true)]
async fn dropping_the_last_owner_ends_a_pending_refresh() {
    let rig = node_rig();
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c1", "title": "slow"}})).after(Duration::from_secs(3600)));
    let (sink, _) = sink();
    rig.sync.refresh_entities(&rig.sync.inner.db.admit().unwrap(), "chats", &ids("c1"), super::EntityFetch::Force, sink).unwrap();
    yield_until("the request is in flight", || rig.transport.call_count("ChatNode") == 1).await;
    let owner = Arc::downgrade(&rig.sync.inner);
    let sync = rig.sync;
    drop(sync);
    yield_until("the last drop released the database", || owner.upgrade().is_none()).await;
}
