use super::listeners::{Listener, Mailbox, Subscriber};
use super::watch::{Body, Loaded, RowsBody, Version};
use crate::identity::RowId;
use std::sync::Arc;
use crate::test_wait::join_within;

fn version(revision: i64) -> Arc<Version> {
    Arc::new(Version { revision, epoch: 0, body: Body::Rows(Box::new(RowsBody::new(Vec::new()))) })
}

#[test]
fn a_mailbox_keeps_only_the_latest_version_and_all_replaced_pairs() {
    let mut mailbox = Mailbox::default();
    let pair = (RowId::new("temp-1").unwrap(), RowId::new("m1").unwrap());
    assert!(mailbox.push(version(1), &[], Loaded::default()));
    assert!(!mailbox.push(version(2), std::slice::from_ref(&pair), Loaded::default()));
    assert!(!mailbox.push(version(3), &[], Loaded::default()));
    let taken = mailbox.take().unwrap();
    assert_eq!(taken.latest.revision, 3);
    assert_eq!(taken.steps, 3);
    assert_eq!(taken.replaced, vec![pair]);
    assert!(mailbox.take().is_none());
}

#[test]
fn a_mailbox_asks_for_a_new_run_only_when_it_is_not_scheduled() {
    let mut mailbox = Mailbox::default();
    assert!(mailbox.push(version(1), &[], Loaded::default()));
    mailbox.take().unwrap();
    assert!(!mailbox.push(version(2), &[], Loaded::default()));
    mailbox.finish();
    assert!(mailbox.push(version(3), &[], Loaded::default()));
}

#[test]
fn inv_panic_01_a_listener_worker_that_cannot_start_is_counted_and_never_runs_the_callback_inline() {
    use super::fixtures::{booted, calls, recorder};
    use crate::query::Query;
    use crate::test_wait::spin_until;
    use std::sync::atomic::Ordering;
    let db = booted();
    db.shared.executor.refuse_spawn.store(true, Ordering::SeqCst);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    spin_until("the refused start is counted", || db.counters().listener_spawn_failures == 1);
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(calls(&log), Vec::<usize>::new());
}

#[test]
fn a_paused_mailbox_merges_every_push_and_owes_exactly_one_run() {
    let mut mailbox = Mailbox::default();
    mailbox.pause();
    for revision in 1..=5 {
        assert!(!mailbox.push(version(revision), &[], Loaded::default()), "no run is asked for while paused");
    }
    assert!(mailbox.resume(), "resume asks for the owed run");
    assert!(!mailbox.resume(), "and only once");
    let taken = mailbox.take().unwrap();
    assert_eq!((taken.latest.revision, taken.steps), (5, 5), "one delivery with the latest version");
    assert!(taken.delivered.is_none());
}

#[test]
fn a_paused_mailbox_with_nothing_owed_asks_for_no_run_on_resume() {
    let mut mailbox = Mailbox::default();
    mailbox.pause();
    assert!(!mailbox.resume());
}

#[test]
fn cancelling_while_paused_leaves_nothing_owed() {
    let mut mailbox = Mailbox::default();
    mailbox.pause();
    mailbox.push(version(1), &[], Loaded::default());
    mailbox.forget_owed();
    assert!(!mailbox.resume());
    assert!(mailbox.take().is_none());
}

const WAIT: std::time::Duration = std::time::Duration::from_secs(10);

fn hold_before_the_call(call_state: &super::listeners::CallState) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (reached, at_the_call) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    *call_state.before_enter.lock().unwrap() = Some(Box::new(move || {
        reached.send(()).unwrap();
        released.recv_timeout(WAIT).unwrap();
    }));
    (at_the_call, release)
}

#[test]
fn inv_observe_08_a_rows_call_that_passed_the_cancel_check_does_not_start_after_the_cancel_returns() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let db = super::fixtures::booted();
    db.shared.executor.refuse_spawn.store(true, Ordering::SeqCst);
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let subscriber = Subscriber::new(Listener::Rows(Box::new(move |_| { counted.fetch_add(1, Ordering::SeqCst); })), db.shared.counters.clone());
    let (at_the_call, release) = hold_before_the_call(&subscriber.call_state);
    let delivering = {
        let (subscriber, executor) = (subscriber.clone(), db.shared.executor.clone());
        std::thread::spawn(move || {
            subscriber.post(&executor, version(1), &[], Loaded::default());
            super::listeners::Runnable::run(&*subscriber);
        })
    };
    at_the_call.recv_timeout(WAIT).unwrap();
    subscriber.cancel();
    release.send(()).unwrap();
    join_within(delivering);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(db.counters().observer_calls, 0);
}

#[test]
fn listener_thread_creation_failure() {
    use super::fixtures::booted;
    use crate::test_wait::recv_within;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc::channel;
    let db = booted();
    let executor = db.shared.executor.clone();
    executor.refuse_spawn.store(true, Ordering::SeqCst);

    let (row_sink, row_revisions) = channel();
    let rows = Subscriber::new(Listener::Rows(Box::new(move |change| row_sink.send(change.revision).unwrap())), db.shared.counters.clone());
    rows.post(&executor, version(1), &[], Loaded::default());
    assert_eq!(db.counters().listener_spawn_failures, 1, "the refused start is counted");
    assert!(row_revisions.try_recv().is_err(), "no callback runs on the posting thread");
    assert!(rows.idle(), "a refused start leaves no subscriber waiting for a worker");

    executor.refuse_spawn.store(false, Ordering::SeqCst);
    rows.post(&executor, version(2), &[], Loaded::default());
    assert_eq!(recv_within(&row_revisions), 2, "a live worker serves the subscriber once the start is allowed");
    assert_eq!(db.counters().listener_spawn_failures, 1, "only the start that left a subscriber without a worker is counted");
}

struct Capture {
    shared: std::sync::Weak<super::open::Shared>,
    seen: Arc<std::sync::Mutex<Vec<bool>>>,
}

impl Drop for Capture {
    fn drop(&mut self) {
        let Some(shared) = self.shared.upgrade() else { return };
        let held = super::open::observers_held();
        if !held {
            drop(shared.observers());
        }
        self.seen.lock().unwrap().push(!held);
    }
}

#[test]
fn listener_capture_drop_reenters() {
    use super::fixtures::booted;
    use super::observe::ObserverHandle;
    use super::open::locked;
    use super::Db;
    use crate::query::Query;
    use crate::test_wait::spin_until;
    type Observe = fn(&Db, Capture) -> ObserverHandle;
    let kinds: [(&str, Observe); 2] = [
        ("rows", |db, capture| db.observe(Query::new("moments"), move |_| { let _keep = &capture; }).unwrap()),
        ("count", |db, capture| db.observe_count(Query::new("moments"), move |_| { let _keep = &capture; }).unwrap()),
    ];
    for (kind, observe) in kinds {
        let db = booted();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let handle = observe(&db, Capture { shared: Arc::downgrade(&db.shared), seen: seen.clone() });
        let registry = locked(&db.shared.observers);
        drop(handle);
        assert!(seen.lock().unwrap().is_empty(), "{kind}: the registry owns the capture while the cancel waits");
        drop(registry);
        drop(db.shared.observers());
        spin_until("the capture is destroyed", || !seen.lock().unwrap().is_empty());
        assert_eq!(*seen.lock().unwrap(), vec![true], "{kind}: the capture is destroyed with no registry lock held and may enter the registry");
    }
}
