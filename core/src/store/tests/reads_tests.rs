use std::sync::{Arc, Mutex};

use crate::test_wait::join_within;
use super::reads::{ReadEvent, Reads};

fn recording() -> (Arc<Reads>, Arc<Mutex<Vec<ReadEvent>>>) {
    let reads = Reads::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    reads.attach(move |event| log.lock().unwrap().push(event)).unwrap();
    (reads, seen)
}

#[test]
fn close_names_the_unfinished_reads_before_their_finish_and_refuses_new_ones() {
    let (reads, seen) = recording();
    let done = reads.admit().unwrap();
    drop(done);
    let held = reads.admit().unwrap();
    reads.close();
    assert!(reads.admit().is_none());
    drop(held);
    assert_eq!(*seen.lock().unwrap(), vec![ReadEvent::Closed(vec![2]), ReadEvent::Finished(2)]);
}

#[test]
fn reads_that_finished_before_the_close_leave_no_event() {
    let (reads, seen) = recording();
    drop(reads.admit().unwrap());
    reads.close();
    assert_eq!(*seen.lock().unwrap(), vec![ReadEvent::Closed(vec![])]);
}

#[test]
fn threads_that_finish_while_the_close_runs_keep_the_order_of_the_queue() {
    for _ in 0..200 {
        let (reads, seen) = recording();
        let guards: Vec<_> = (0..4).map(|_| reads.admit().unwrap()).collect();
        let closer = { let reads = reads.clone(); std::thread::spawn(move || reads.close()) };
        let finishers: Vec<_> = guards.into_iter().map(|guard| std::thread::spawn(move || drop(guard))).collect();
        join_within(closer);
        finishers.into_iter().for_each(|finisher| join_within(finisher));
        let events = seen.lock().unwrap().clone();
        let Some(ReadEvent::Closed(named)) = events.first().cloned() else { panic!("Closed is first: {events:?}") };
        let finished: Vec<u64> = events[1..].iter().map(|event| match event { ReadEvent::Finished(id) => *id, ReadEvent::Closed(_) => panic!("one Closed") }).collect();
        let mut sorted = finished.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, named, "every named id finishes once: {events:?}");
    }
}
