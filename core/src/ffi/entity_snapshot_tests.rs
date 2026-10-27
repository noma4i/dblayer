use super::*;
use crate::db::fixtures::put;
use crate::test_wait::within;
use serde_json::json;
use tokio::sync::mpsc;

struct Listener(mpsc::UnboundedSender<WireEntitySnapshot>);
impl EntityListener for Listener {
    fn changed(&self, delivery: EntityDelivery) { for snapshot in delivery.snapshots { let _ = self.0.send(snapshot); } }
}
async fn next(receiver: &mut mpsc::UnboundedReceiver<WireEntitySnapshot>, phase: EntityStage) -> WireEntitySnapshot {
    within(async { loop { let snapshot = receiver.recv().await.expect("entity source remains alive"); if snapshot.phase == phase { return snapshot; } } }).await
}
fn request(id: &str, lease: u64) -> EntityRequest { EntityRequest { id: id.into(), lease, fetch: EntityFetch::None } }
fn release(id: &str, lease: u64) -> EntityLease { EntityLease { id: id.into(), lease } }

#[tokio::test]
async fn entity_snapshot_order() {
    let (db, _) = super::super::load_tests::rig().await;
    put(&db.db, "Moment", json!({"id":"m1", "text":"cold"}));
    let old_row = db.db.find("Moment", &RowId::new("m1").unwrap()).unwrap().unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let live = db.live_entities("Moment".into(), Arc::new(Listener(sender))).unwrap();
    live.retain(vec![request("m1", 1)]).unwrap();
    let cold = next(&mut receiver, EntityStage::Present).await;
    assert!(cold.settled && !cold.is_stale && cold.value.is_some());
    let mut revisions = vec![cold.revision];
    let old_observer_lease = locked(&live.state).entries["m1"].observer_lease;
    live.release(vec![release("m1", 1)]).unwrap();
    assert_eq!(db.entity_cache.count("Moment", live.now()), 1);
    live.retain(vec![request("m1", 2)]).unwrap();
    let retained = next(&mut receiver, EntityStage::Present).await;
    assert!(retained.is_stale && retained.value == cold.value && retained.lease == 2);
    revisions.push(retained.revision);
    let fresh = next(&mut receiver, EntityStage::Present).await;
    assert!(!fresh.is_stale);
    revisions.push(fresh.revision);
    live.changed(&EntityChange { revision: i64::MAX, epoch: live.epoch(), leases: BTreeMap::from([(old_row.id.clone(), old_observer_lease)]), rows: vec![old_row.clone()], removed: vec![], failure: None });
    assert!(receiver.try_recv().is_err(), "an old acquisition cannot publish to a new lease");
    let current_lease = locked(&live.state).entries["m1"].observer_lease;
    live.changed(&EntityChange { revision: fresh.revision, epoch: live.epoch(), leases: BTreeMap::from([(old_row.id.clone(), current_lease)]), rows: vec![], removed: vec![], failure: Some("read failed".into()) });
    let error = next(&mut receiver, EntityStage::Failed { message: "read failed".into() }).await;
    assert!(matches!(error.phase, EntityStage::Failed { .. }) && error.value == fresh.value && error.settled);
    revisions.push(error.revision);
    let generation = live.generation();
    db.logout().await.unwrap();
    let removed = next(&mut receiver, EntityStage::Absent).await;
    assert!(removed.value.is_none() && removed.generation > generation);
    revisions.push(removed.revision);
    put(&db.db, "Moment", json!({"id":"m1", "text":"new account"}));
    let current = next(&mut receiver, EntityStage::Present).await;
    revisions.push(current.revision);
    assert!(revisions.windows(2).all(|pair| pair[0] <= pair[1]), "{revisions:?}");
    let mut state = locked(&live.state);
    let mut single = Entry::new(3, true, live.generation(), live.epoch());
    let first = single.verdict.feed;
    single.accept(current.revision, current.value.clone(), Some(first));
    single.verdict = single.machine.reset();
    single.clear_value();
    single.verdict = single.machine.begin_feed();
    let second = single.verdict.feed;
    state.singleton = Some(Singleton { entry: single, query: String::new(), subscription: None });
    drop(state);
    live.single_changed(first, LiveChange::whole(i64::MAX, current.value.clone().into_iter().collect()));
    assert!(receiver.try_recv().is_err());
    live.single_changed(second, LiveChange::whole(current.revision, current.value.into_iter().collect()));
    let singleton = next(&mut receiver, EntityStage::Present).await;
    assert_eq!(singleton.revision, current.revision);
    assert_eq!(singleton.lease, 3);
    live.shutdown();
}

#[tokio::test]
async fn entity_initial_local() {
    let (db, transport) = super::super::load_tests::rig().await;
    put(&db.db, "Moment", json!({"id":"m1", "text":"local"}));
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let live = db.live_entities("Moment".into(), Arc::new(Listener(sender))).unwrap();
    live.retain(vec![request("m1", 1), request("missing", 2)]).unwrap();
    let mut values = BTreeMap::new();
    for _ in 0..2 { let snapshot = within(receiver.recv()).await.unwrap(); assert!(snapshot.settled); values.insert(snapshot.id.clone(), snapshot); }
    assert_eq!(values["m1"].phase, EntityStage::Present);
    assert!(values["m1"].value.is_some());
    assert_eq!(values["missing"].phase, EntityStage::Absent);
    assert!(values["missing"].value.is_none());
    assert_eq!(transport.queries.load(std::sync::atomic::Ordering::SeqCst), 0);
    live.shutdown();
}

#[tokio::test]
async fn entity_retention_budget() {
    let cache = super::super::entity_pool::EntityCache::new();
    let row = || CachedEntity { row: None, revision: 1, generation: 1, epoch: 0 };
    for id in 0..201 { cache.keep("A", &id.to_string(), row(), 0); }
    assert_eq!(cache.count("A", 0), 200);
    assert!(cache.take("A", "0", 0, 1, 0).is_none());
    assert!(cache.take("A", "1", 120_000, 1, 0).is_none());
    assert_eq!(cache.count("A", 120_000), 0);
    cache.keep("A", "generation", row(), 120_001);
    assert!(cache.take("A", "generation", 120_002, 2, 0).is_none());
    let (db, _) = super::super::load_tests::rig().await;
    put(&db.db, "Moment", json!({"id":"large", "text":"bytes"}));
    let stored = db.db.find("Moment", &RowId::new("large").unwrap()).unwrap().unwrap();
    let mut wire = WireModel::of(db.db.spec("Moment").unwrap()).row(&stored);
    wire.estimated_bytes = 5 * 1024 * 1024;
    for model in ["A", "B"] {
        cache.keep(model, "large", CachedEntity { row: Some(wire.clone()), revision: 1, generation: 1, epoch: 0 }, 120_003);
    }
    assert_eq!(cache.count("A", 120_003), 0);
    assert_eq!(cache.count("B", 120_003), 1);
    wire.estimated_bytes = 9 * 1024 * 1024;
    cache.keep("C", "oversized", CachedEntity { row: Some(wire), revision: 1, generation: 1, epoch: 0 }, 120_003);
    assert_eq!(cache.count("C", 120_003), 0);
}

struct IdleClock {
    clock: crate::pacing::ManualClock,
    sleeps: std::sync::atomic::AtomicUsize,
    wakes: std::sync::atomic::AtomicUsize,
}
impl crate::pacing::Clock for IdleClock {
    fn now(&self) -> std::time::Duration { crate::pacing::Clock::now(&self.clock) }
    fn sleep(&self, duration: std::time::Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output=()> + Send + '_>> {
        use std::sync::atomic::Ordering;
        self.sleeps.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { crate::pacing::Clock::sleep(&self.clock, duration).await; self.wakes.fetch_add(1, Ordering::SeqCst); })
    }
}

#[tokio::test]
async fn entity_idle_work() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    let (sample, _) = super::super::load_tests::rig().await;
    let models = serde_json::to_string(&vec![sample.db.spec("Moment").unwrap().clone()]).unwrap();
    let clock = Arc::new(IdleClock { clock: crate::pacing::ManualClock::new(Duration::from_secs(1000)), sleeps: AtomicUsize::new(0), wakes: AtomicUsize::new(0) });
    let db = Database::start_with(super::super::Config { path: None, data_version: None, cache_rows: 32 }, models, None, clock.clone(), |_, _| Ok((None, None))).unwrap();
    put(&db.db, "Moment", json!({"id":"m1", "text":"idle"}));
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let live = db.live_entities("Moment".into(), Arc::new(Listener(sender))).unwrap();
    live.retain(vec![request("m1", 1)]).unwrap();
    next(&mut receiver, EntityStage::Present).await;
    live.release(vec![release("m1", 1)]).unwrap();
    let before = db.db.counters();
    let sleepers = (clock.sleeps.load(Ordering::SeqCst), clock.wakes.load(Ordering::SeqCst));
    tokio::time::pause();
    clock.clock.advance(Duration::from_secs(130));
    tokio::time::advance(Duration::from_secs(130)).await;
    tokio::task::yield_now().await;
    let after = db.db.counters();
    assert_eq!((after.entity_facts_told, after.rows_loaded, after.observer_calls, after.transport_calls), (before.entity_facts_told, before.rows_loaded, before.observer_calls, before.transport_calls));
    assert_eq!(sleepers, (clock.sleeps.load(Ordering::SeqCst), clock.wakes.load(Ordering::SeqCst)));
    assert!(receiver.try_recv().is_err());
    live.shutdown();
}
