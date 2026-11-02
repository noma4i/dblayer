use super::*;
use crate::db::fixtures::put;
use crate::test_wait::{mutex_free, within_secs, yield_until, ProbeWaker};
use serde_json::json;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicBool;
use std::task::{Context, Waker};

#[derive(Clone, Copy, Debug)]
enum Trigger { Changed, Refreshed, Reset, SingleChanged, RetainCached, Shutdown, DecodeFailed }

#[derive(Clone, Copy, Debug)]
enum Reenter { Retain, Release, Reset, Shutdown, DecodeFailed }

const TRIGGERS: [Trigger; 7] = [Trigger::Changed, Trigger::Refreshed, Trigger::Reset, Trigger::SingleChanged, Trigger::RetainCached, Trigger::Shutdown, Trigger::DecodeFailed];
const REENTERS: [Reenter; 5] = [Reenter::Retain, Reenter::Release, Reenter::Reset, Reenter::Shutdown, Reenter::DecodeFailed];

#[derive(Default)]
struct Probe {
    live: OnceLock<Weak<LiveEntities>>,
    op: Mutex<Option<(Reenter, u64)>>,
    seen: Mutex<Vec<EntityDelivery>>,
    guarded: AtomicU64,
    retained: Mutex<Option<bool>>,
}

impl EntityListener for Probe {
    fn changed(&self, delivery: EntityDelivery) {
        let first = delivery.snapshots[0].clone();
        locked(&self.seen).push(delivery);
        let Some(live) = self.live.get().and_then(Weak::upgrade) else { return };
        if !(mutex_free(&live.state) && mutex_free(&live.outbox)) { self.guarded.fetch_add(1, Ordering::SeqCst); }
        let armed = locked(&self.op).take();
        let Some((op, lease)) = armed else { return };
        match op {
            Reenter::Retain => *locked(&self.retained) = Some(live.retain(vec![request("m2", 9)]).is_ok()),
            Reenter::Release => live.release(vec![release("m1", lease)]).unwrap(),
            Reenter::Reset => live.reset(),
            Reenter::Shutdown => live.shutdown(),
            Reenter::DecodeFailed => live.decode_failed(EntityLease { id: first.id.clone(), lease: first.lease }, first.revision, "reentered".into()),
        }
    }
}

fn request(id: &str, lease: u64) -> EntityRequest { EntityRequest { id: id.into(), lease, fetch: EntityFetch::None } }
fn release(id: &str, lease: u64) -> EntityLease { EntityLease { id: id.into(), lease } }

fn has(probe: &Probe, wanted: impl Fn(&WireEntitySnapshot) -> bool) -> bool {
    locked(&probe.seen).iter().flat_map(|delivery| delivery.snapshots.iter()).any(wanted)
}

async fn reenter_case(trigger: Trigger, op: Reenter) {
    let label = format!("{trigger:?} then {op:?}");
    let (db, _) = super::super::load_tests::rig().await;
    put(&db.db, "Moment", json!({"id":"m1", "text":"a"}));
    let row = db.db.find("Moment", &RowId::new("m1").unwrap()).unwrap().unwrap();
    let probe = Arc::new(Probe::default());
    let live = db.live_entities("Moment".into(), probe.clone()).unwrap();
    probe.live.set(Arc::downgrade(&live)).unwrap();
    live.retain(vec![request("m1", 1)]).unwrap();
    yield_until("the first snapshot", || has(&probe, |snapshot| snapshot.phase == EntityStage::Present)).await;
    let present = locked(&probe.seen).iter().flat_map(|delivery| delivery.snapshots.clone()).find(|snapshot| snapshot.phase == EntityStage::Present).unwrap();
    let mut lease = 1;
    if matches!(trigger, Trigger::RetainCached) {
        live.release(vec![release("m1", 1)]).unwrap();
        lease = 2;
    }
    let feed = matches!(trigger, Trigger::SingleChanged).then(|| {
        let single = Entry::new(3, true, live.generation(), live.epoch());
        let feed = single.verdict.feed;
        locked(&live.state).singleton = Some(Singleton { entry: single, query: String::new(), subscription: None });
        feed
    });
    let before = locked(&probe.seen).len();
    *locked(&probe.op) = Some((op, lease));
    let fired = live.clone();
    let value = present.value.clone();
    within_secs(30, tokio::task::spawn_blocking(move || match trigger {
        Trigger::Changed => {
            let observer_lease = locked(&fired.state).entries["m1"].observer_lease;
            fired.changed(&EntityChange { revision: present.revision + 1, epoch: fired.epoch(), leases: BTreeMap::from([(row.id.clone(), observer_lease)]), rows: vec![row], removed: vec![], failure: None });
        }
        Trigger::Refreshed => fired.refreshed("m1", 1, fired.generation(), None),
        Trigger::Reset => fired.reset(),
        Trigger::SingleChanged => fired.single_changed(feed.unwrap(), LiveChange::whole(present.revision, value.into_iter().collect())),
        Trigger::RetainCached => fired.retain(vec![request("m1", 2)]).unwrap(),
        Trigger::Shutdown => fired.shutdown(),
        Trigger::DecodeFailed => fired.decode_failed(EntityLease { id: "m1".into(), lease: 1 }, present.revision, "first".into()),
    })).await.unwrap_or_else(|error| panic!("{label}: the call did not return: {error}"));
    yield_until("the reentrant call ran", || locked(&probe.op).is_none()).await;
    assert_eq!(probe.guarded.load(Ordering::SeqCst), 0, "{label}: a callback ran under a library lock");
    let closed = matches!(trigger, Trigger::Shutdown);
    match op {
        Reenter::Reset if !closed => yield_until("the reset snapshot", || has(&probe, |snapshot| snapshot.phase == EntityStage::Opening)).await,
        Reenter::Shutdown => yield_until("the closed snapshot", || has(&probe, |snapshot| snapshot.phase == EntityStage::Closed)).await,
        Reenter::DecodeFailed if !closed => yield_until("the failure snapshot", || has(&probe, |snapshot| snapshot.phase == EntityStage::Failed { message: "reentered".into() })).await,
        _ => {}
    }
    assert!(locked(&probe.seen).len() > before, "{label}: the trigger delivered nothing");
    {
        let state = locked(&live.state);
        match op {
            Reenter::Retain if !closed => {
                assert_eq!(*locked(&probe.retained), Some(true), "{label}");
                assert!(state.entries["m2"].lease == 9 && state.entries["m2"].observer_lease != 0, "{label}: lease not recorded");
            }
            Reenter::Retain => assert_eq!(*locked(&probe.retained), Some(false), "{label}: a closed source accepted a lease"),
            Reenter::Release => assert!(!state.entries.contains_key("m1"), "{label}: the lease stayed"),
            _ => {}
        }
    }
    let mut last: BTreeMap<(String, u64), i64> = BTreeMap::new();
    for snapshot in locked(&probe.seen).iter().flat_map(|delivery| delivery.snapshots.iter()) {
        let seen = last.entry((snapshot.id.clone(), snapshot.lease)).or_insert(snapshot.revision);
        assert!(*seen <= snapshot.revision, "{label}: revision went back for {}", snapshot.id);
        *seen = snapshot.revision;
    }
    live.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn entity_callbacks_reenter() {
    for trigger in TRIGGERS {
        for op in REENTERS { reenter_case(trigger, op).await; }
    }
}

fn watched(token: &CancelToken, probe: &Arc<ProbeWaker>) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> {
    let token = token.clone();
    let mut waiting: std::pin::Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async move { token.cancelled().await });
    let waker = Waker::from(probe.clone());
    assert!(waiting.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    waiting
}

#[tokio::test(flavor = "multi_thread")]
async fn entity_cleanup_runs_unlocked() {
    for label in ["release", "replace", "reset", "shutdown", "drop"] {
        let (db, _) = super::super::load_tests::rig().await;
        put(&db.db, "Moment", json!({"id":"m1", "text":"a"}));
        let (sender, _receiver) = std::sync::mpsc::channel::<EntityDelivery>();
        struct Quiet(Mutex<std::sync::mpsc::Sender<EntityDelivery>>);
        impl EntityListener for Quiet { fn changed(&self, delivery: EntityDelivery) { let _ = locked(&self.0).send(delivery); } }
        let live = db.live_entities("Moment".into(), Arc::new(Quiet(Mutex::new(sender)))).unwrap();
        live.retain(vec![request("m1", 1)]).unwrap();
        let token = CancelToken::new();
        locked(&live.state).entries.get_mut("m1").unwrap().cancel = Some(token.clone());
        let weak = Arc::downgrade(&live);
        let probe = ProbeWaker::new(move || weak.upgrade().is_none_or(|live| mutex_free(&live.state)));
        let _waiting = watched(&token, &probe);
        match label {
            "release" => live.release(vec![release("m1", 1)]).unwrap(),
            "replace" => live.retain(vec![request("m1", 2)]).unwrap(),
            "reset" => live.reset(),
            "shutdown" => live.shutdown(),
            _ => drop(live),
        }
        assert!(token.is_cancelled(), "{label}: the refresh stayed live");
        assert_eq!((probe.wakes(), probe.violations()), (1, 0), "{label}: the cancel woke under the state guard");
    }
}

struct PanicOnce { armed: AtomicBool, seen: Mutex<Vec<EntityDelivery>> }
impl EntityListener for PanicOnce {
    fn changed(&self, delivery: EntityDelivery) {
        locked(&self.seen).push(delivery);
        if self.armed.swap(false, Ordering::SeqCst) { panic!("listener failed"); }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn entity_callback_panic_keeps_delivering() {
    let (db, _) = super::super::load_tests::rig().await;
    put(&db.db, "Moment", json!({"id":"m1", "text":"a"}));
    let listener = Arc::new(PanicOnce { armed: AtomicBool::new(false), seen: Mutex::default() });
    let live = db.live_entities("Moment".into(), listener.clone()).unwrap();
    live.retain(vec![request("m1", 1)]).unwrap();
    yield_until("the first snapshot", || !locked(&listener.seen).is_empty()).await;
    let revision = locked(&listener.seen)[0].snapshots[0].revision;
    let before = locked(&listener.seen).len();
    listener.armed.store(true, Ordering::SeqCst);
    let panicked = std::panic::catch_unwind(AssertUnwindSafe(|| live.decode_failed(EntityLease { id: "m1".into(), lease: 1 }, revision, "boom".into())));
    assert!(panicked.is_err());
    assert!(mutex_free(&live.state) && !locked(&live.outbox).draining, "a panicking listener left the source blocked");
    live.decode_failed(EntityLease { id: "m1".into(), lease: 1 }, revision, "after".into());
    let seen = locked(&listener.seen);
    assert_eq!(seen.len(), before + 2);
    assert_eq!(seen[before + 1].snapshots[0].phase, EntityStage::Failed { message: "after".into() });
    drop(seen);
    live.shutdown();
}
