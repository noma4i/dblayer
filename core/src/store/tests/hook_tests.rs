use super::fixtures::*;
use super::*;
use std::sync::mpsc;
use crate::test_wait::{join_within, recv_within};

fn written(store: &Arc<Store>, row_id: &str) {
    store.commit(envelope(store, vec![upsert("messages", message(row_id, "c1"))], vec![])).unwrap();
}

fn completes(work: impl FnOnce() + Send + 'static) {
    let (done, finished) = mpsc::channel();
    std::thread::spawn(move || {
        work();
        let _ = done.send(());
    });
    recv_within(&finished);
}

#[test]
fn inv_conc_01_a_store_hook_may_call_the_store_it_was_announced_from() {
    let (store, _) = booted_store();
    let store = Arc::new(store);
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (inside, counter) = (store.clone(), calls.clone());
    store.set_hook(Box::new(move |_| {
        inside.revision_seq().unwrap();
        inside.counters();
        inside.generation();
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    })).unwrap();
    let worker = store.clone();
    completes(move || {
        written(&worker, "m1");
        worker.erase_models(worker.generation().next(), &["messages"].map(String::from)).unwrap();
        worker.erase_account(worker.generation().next(), false).unwrap();
    });
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3, "one announcement for the commit and one for each reset");
}

#[test]
fn inv_conc_01_events_reach_the_hook_in_commit_order_while_an_earlier_hook_is_still_running() {
    let (store, _) = booted_store();
    let store = Arc::new(store);
    let revisions = Arc::new(Mutex::new(Vec::new()));
    let (entered, first_entered) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    let log = revisions.clone();
    store.set_hook(Box::new(move |event| {
        let StoreEvent::Committed(result) = event else { return };
        let first = {
            let mut seen = log.lock().unwrap();
            seen.push(result.revision);
            seen.len() == 1
        };
        if first {
            entered.send(()).unwrap();
            recv_within(&released.lock().unwrap());
        }
    })).unwrap();
    let earlier = store.clone();
    let first = std::thread::spawn(move || written(&earlier, "m1"));
    recv_within(&first_entered);
    let reached = Arc::new(crate::test_wait::Flag::default());
    let commits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    store.set_commit_log({ let (reached, commits) = (reached.clone(), commits.clone()); Box::new(move |_| { if commits.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 { reached.set(); } }) }).unwrap();
    let later = store.clone();
    let second = std::thread::spawn(move || written(&later, "m2"));
    reached.wait_blocking("the later commit never reached the store");
    assert_eq!(revisions.lock().unwrap().len(), 1, "the later commit waits for the earlier hook");
    release.send(()).unwrap();
    join_within(first);
    join_within(second);
    let seen = revisions.lock().unwrap().clone();
    assert!(seen.len() == 2 && seen[0] < seen[1], "{seen:?}");
}

#[test]
fn inv_conc_02_no_store_lock_is_held_while_the_hook_calls_into_other_components() {
    let (store, _) = booted_store();
    let store = Arc::new(store);
    let probes = Arc::new(Mutex::new(Vec::new()));
    let (inside, log) = (store.clone(), probes.clone());
    store.set_hook(Box::new(move |_| {
        log.lock().unwrap().push((inside.writer_is_free(), inside.models.try_write().is_ok()));
    })).unwrap();
    written(&store, "m1");
    store.erase_models(store.generation().next(), &["messages"].map(String::from)).unwrap();
    store.erase_account(store.generation().next(), false).unwrap();
    assert_eq!(*probes.lock().unwrap(), vec![(true, true); 3]);
}

fn deferred<'a>(store: &'a Store, row_id: &str) -> Committed<'a> {
    let planned = envelope(store, vec![upsert("messages", message(row_id, "c1"))], vec![]);
    store.commit_deferred(planned).unwrap()
}

#[test]
fn a_deferred_commit_announces_only_when_the_caller_publishes_in_commit_order() {
    let (store, _) = booted_store();
    let announced = Arc::new(Mutex::new(Vec::new()));
    let log = announced.clone();
    store.set_hook(Box::new(move |event| {
        if let StoreEvent::Committed(result) = event {
            log.lock().unwrap().push(result.revision);
        }
    })).unwrap();
    let caller_state = Mutex::new(());
    let guard = caller_state.lock().unwrap();
    let first = deferred(&store, "m1");
    let second = deferred(&store, "m2");
    assert!(announced.lock().unwrap().is_empty(), "nothing is announced while the caller holds its guard");
    assert_eq!(store.revision_seq().unwrap(), second.result().revision.unwrap(), "the commits are readable before they are announced");
    drop(guard);
    let (first, second) = (first.publish(), second.publish());
    assert_eq!(*announced.lock().unwrap(), vec![first.revision, second.revision]);
}

#[test]
fn a_dropped_deferred_commit_still_announces_so_the_order_never_stalls() {
    let (store, _) = booted_store();
    let store = Arc::new(store);
    let announced = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = announced.clone();
    store.set_hook(Box::new(move |event| {
        if matches!(event, StoreEvent::Committed(_)) {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    })).unwrap();
    drop(deferred(&store, "m1"));
    let later = store.clone();
    completes(move || written(&later, "m2"));
    assert_eq!(announced.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[test]
fn generation_is_readable_while_the_writer_is_held() {
    let (store, _) = booted_store();
    let store = Arc::new(store);
    let writer = store.lock();
    let reader = store.clone();
    let generation = crate::test_wait::join_within(std::thread::spawn(move || reader.generation()));
    assert_eq!(generation, writer.generation);
}
