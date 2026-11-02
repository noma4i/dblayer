use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use serde_json::Map;
use tokio::sync::{mpsc, Notify};

use super::load_machine::{Event, LoadError, LoadPhase};
use super::load_owner::{FlightTicket, LoadOwner, RelationWatch, Target};
use super::SyncError;
use crate::test_wait::{join_within, spin_until, within_secs, ForeignWaker, ProbeWaker};

const WINDOW: &str = "moments|feed";

fn target() -> Target {
    Target { model: "moments".to_string(), relation: "feed".to_string(), params: Map::new() }
}

fn observed() -> (Arc<LoadOwner>, RelationWatch) {
    let owner = Arc::new(LoadOwner::default());
    let mut watch = owner.observe(WINDOW, target(), None);
    drop(watch.borrow_and_update());
    (owner, watch)
}

fn poll_once<F: Future>(future: &mut std::pin::Pin<Box<F>>, waker: Arc<impl Wake + Send + Sync + 'static>) -> Poll<F::Output> {
    future.as_mut().poll(&mut Context::from_waker(&Waker::from(waker)))
}

fn failed(flight: u64) -> Event {
    Event::Failed { window: WINDOW.to_string(), flight, error: LoadError::internal() }
}

#[test]
fn load_wake_has_no_guards() {
    type Trigger = fn(&Arc<LoadOwner>, u64);
    let cases: [(&str, bool, Trigger); 5] = [
        ("a flight starts", false, |owner, _| { owner.begin_flight(WINDOW, target(), false, None); }),
        ("a flight lands", true, |owner, flight| owner.submit(Event::Landed { window: WINDOW.to_string(), flight, stored: None })),
        ("a flight is abandoned", true, |owner, flight| owner.submit(Event::Abandoned { window: WINDOW.to_string(), flight })),
        ("a flight fails", true, |owner, flight| owner.submit(failed(flight))),
        ("the owner closes", true, |owner, _| owner.submit(Event::Close)),
    ];
    for (name, started, trigger) in cases {
        let (owner, mut watch) = observed();
        let flight = if started { owner.begin_flight(WINDOW, target(), false, None) } else { 0 };
        drop(watch.borrow_and_update());
        let probe = ProbeWaker::new({
            let owner = owner.clone();
            move || owner.guards_free()
        });
        let mut changed = Box::pin(watch.changed());
        assert!(poll_once(&mut changed, probe.clone()).is_pending(), "{name}: nothing published yet");
        trigger(&owner, flight);
        assert_eq!((probe.wakes(), probe.violations()), (1, 0), "{name}: one wake with the owner guards free");
        assert!(poll_once(&mut changed, probe.clone()).is_ready(), "{name}: the waiter sees the new state");
    }
}

#[tokio::test]
async fn load_wake_has_no_guards_when_the_members_chain_ends() {
    let owner = Arc::new(LoadOwner::default());
    let mut reader = owner.members(WINDOW, |result| {
        drop(result);
        tokio::spawn(std::future::pending::<()>()).abort_handle()
    });
    let probe = ProbeWaker::new({
        let owner = owner.clone();
        move || owner.guards_free()
    });
    let mut finished = Box::pin(reader.finished());
    assert!(poll_once(&mut finished, probe.clone()).is_pending());
    FlightTicket::new(&owner, WINDOW, target(), false, None).landed(None);
    assert_eq!((probe.wakes(), probe.violations()), (1, 0), "the chain ends with its result sender dropped outside the guards");
    assert_eq!(poll_once(&mut finished, probe.clone()), Poll::Ready(Err(SyncError::Cancelled)));
}

struct Reenter {
    owner: Arc<LoadOwner>,
    entered: AtomicUsize,
    flights_seen: AtomicUsize,
}

impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.owner.submit(failed(0));
        self.flights_seen.store(self.owner.flights(WINDOW), Ordering::SeqCst);
    }
}

#[test]
fn load_wake_reenters_the_owner() {
    let (owner, mut watch) = observed();
    let waker = Arc::new(Reenter { owner: owner.clone(), entered: AtomicUsize::new(0), flights_seen: AtomicUsize::new(usize::MAX) });
    let mut changed = Box::pin(watch.changed());
    assert!(poll_once(&mut changed, waker.clone()).is_pending());
    let starter = {
        let owner = owner.clone();
        std::thread::spawn(move || owner.begin_flight(WINDOW, target(), false, None))
    };
    assert_eq!(join_within(starter), 0);
    assert_eq!(waker.entered.load(Ordering::SeqCst), 1, "the start published once and woke the waiter");
    assert_eq!(waker.flights_seen.load(Ordering::SeqCst), 0, "the wake saw the flight ended by its own call");
    drop(changed);
    let state = watch.borrow().clone();
    assert_eq!((state.phase, state.error.map(|error| error.kind)), (LoadPhase::Failed, Some("internal")), "the nested end published after the start");
}

#[test]
fn load_wake_blocked_in_a_foreign_waiter_does_not_stop_the_owner() {
    let (owner, mut watch) = observed();
    let foreign = Arc::new(Mutex::new(()));
    let waker = ForeignWaker::new(foreign.clone());
    let mut changed = Box::pin(watch.changed());
    assert!(poll_once(&mut changed, waker.clone()).is_pending());
    let held = foreign.lock().unwrap();
    let starter = {
        let owner = owner.clone();
        std::thread::spawn(move || owner.begin_flight(WINDOW, target(), false, None))
    };
    spin_until("the start reached the foreign waiter", || waker.entered());
    assert!(owner.guards_free(), "the owner guards are free while the foreign wake is blocked");
    let other = {
        let owner = owner.clone();
        std::thread::spawn(move || { owner.submit(failed(0)); owner.flights(WINDOW) })
    };
    assert_eq!(join_within(other), 0, "a second operation completes while the first wake is blocked");
    drop(held);
    assert_eq!(join_within(starter), 0);
    drop(changed);
    assert_eq!(watch.borrow().phase, LoadPhase::Failed, "the queued publication followed the first one");
}

struct Terminal(mpsc::UnboundedSender<()>);

impl Drop for Terminal {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[tokio::test]
async fn last_member_cancel_settles_load() {
    let owner = Arc::new(LoadOwner::default());
    let started = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Notify::new());
    let (ended_tx, mut ended) = mpsc::unbounded_channel::<()>();
    let open = |window: &str| {
        owner.members(window, |result| {
            started.fetch_add(1, Ordering::SeqCst);
            let (terminal, gate) = (Terminal(ended_tx.clone()), gate.clone());
            tokio::spawn(async move {
                let _terminal = terminal;
                within_secs(30, gate.notified()).await;
                result.send_replace(Some(Ok(())));
            })
            .abort_handle()
        })
    };

    let (first, mut second) = (open("shared"), open("shared"));
    assert_eq!((started.load(Ordering::SeqCst), owner.members_readers("shared")), (1, 2), "two readers share one chain");
    drop(first);
    assert_eq!(owner.members_readers("shared"), 1, "cancelling one reader keeps the chain");
    gate.notify_one();
    assert_eq!(within_secs(30, second.finished()).await, Ok(()), "the other reader gets the result");
    assert!(within_secs(30, ended.recv()).await.is_some(), "the finished chain ended once");
    assert!(ended.try_recv().is_err(), "no second terminal outcome");
    drop(second);
    assert_eq!(owner.members_readers("shared"), 0);

    let last = open("alone");
    assert_eq!((started.load(Ordering::SeqCst), owner.members_readers("alone")), (2, 1));
    drop(last);
    assert_eq!(owner.members_readers("alone"), 0, "the census returns to zero");
    assert!(within_secs(30, ended.recv()).await.is_some(), "the abandoned chain was aborted and ended once");
    assert!(ended.try_recv().is_err(), "no second terminal outcome");

    let reopened = open("alone");
    assert_eq!((started.load(Ordering::SeqCst), owner.members_readers("alone")), (3, 1), "a reopen starts one new chain");
    drop(reopened);
    assert!(within_secs(30, ended.recv()).await.is_some());
    assert_eq!(owner.members_readers("alone"), 0);
}
