use super::fixtures::*;
use super::*;
use std::sync::mpsc;
use std::time::Duration;

fn written(store: &Arc<Store>, row_id: &str) {
    store.commit(envelope(store, vec![upsert("messages", message(row_id, "c1"))], vec![])).unwrap();
}

fn within_five_seconds(work: impl FnOnce() + Send + 'static) {
    let (done, finished) = mpsc::channel();
    std::thread::spawn(move || {
        work();
        let _ = done.send(());
    });
    assert_eq!(finished.recv_timeout(Duration::from_secs(5)), Ok(()), "the call never returned: the hook waits for a store lock");
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
    }));
    let worker = store.clone();
    within_five_seconds(move || {
        written(&worker, "m1");
        worker.reset_cache_only(&["messages"]).unwrap();
        worker.reset_incompatible().unwrap();
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
            released.lock().unwrap().recv().unwrap();
        }
    }));
    let earlier = store.clone();
    let first = std::thread::spawn(move || written(&earlier, "m1"));
    first_entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let later = store.clone();
    let second = std::thread::spawn(move || written(&later, "m2"));
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(revisions.lock().unwrap().len(), 1, "the later commit waits for the earlier hook");
    release.send(()).unwrap();
    first.join().unwrap();
    second.join().unwrap();
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
    }));
    written(&store, "m1");
    store.reset_cache_only(&["messages"]).unwrap();
    store.reset_incompatible().unwrap();
    assert_eq!(*probes.lock().unwrap(), vec![(true, true); 3]);
}
