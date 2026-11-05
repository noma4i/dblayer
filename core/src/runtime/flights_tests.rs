use super::sync::{Arc, AtomicUsize, Ordering};
use super::{Flights, Running};

fn counted(cancelled: &Arc<AtomicUsize>) -> Running {
    let cancelled = cancelled.clone();
    Running::new(move || {
        cancelled.fetch_add(1, Ordering::SeqCst);
    })
}

#[test]
fn a_replaced_task_is_cancelled_once_and_a_finished_one_is_not_cancelled() {
    let (replaced, finished) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let flights = Flights::<u32>::default();
    flights.put(1, counted(&replaced));
    flights.put(1, counted(&replaced));
    assert_eq!((replaced.load(Ordering::SeqCst), flights.len()), (1, 1));
    flights.cancel(&1);
    assert_eq!((replaced.load(Ordering::SeqCst), flights.len()), (2, 0));
    flights.put(2, counted(&finished));
    flights.done(&2);
    assert_eq!((finished.load(Ordering::SeqCst), flights.len()), (0, 0));
    flights.cancel(&2);
    assert_eq!(finished.load(Ordering::SeqCst), 0);
}

#[cfg(dblayer_loom)]
mod model {
    use loom::thread;

    use super::*;

    #[test]
    fn loom_put_against_put_and_cancel_cancels_each_task_once_and_never_under_the_lock() {
        loom::model(|| {
            let (first, second) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
            let flights = Flights::<u32>::default();
            let aware = |counter: &Arc<AtomicUsize>| {
                let (counter, flights) = (counter.clone(), flights.clone());
                Running::new(move || {
                    flights.len();
                    counter.fetch_add(1, Ordering::SeqCst);
                })
            };
            let (a, b) = (aware(&first), aware(&second));
            let putting = [(a, 0u8), (b, 1u8)].map(|(running, _)| {
                let flights = flights.clone();
                thread::spawn(move || flights.put(7, running))
            });
            let cancelling = {
                let flights = flights.clone();
                thread::spawn(move || flights.cancel(&7))
            };
            putting.into_iter().for_each(|put| put.join().unwrap());
            cancelling.join().unwrap();
            let (first, second) = (first.load(Ordering::SeqCst), second.load(Ordering::SeqCst));
            assert!(first <= 1 && second <= 1, "a task was cancelled twice: {first} {second}");
            assert_eq!(first + second + flights.len(), 2, "every task is cancelled or still held");
            flights.cancel(&7);
        });
    }

    #[test]
    fn loom_done_against_cancel_never_cancels_a_finished_task_twice() {
        loom::model(|| {
            let cancelled = Arc::new(AtomicUsize::new(0));
            let flights = Flights::<u32>::default();
            flights.put(1, counted(&cancelled));
            let done = {
                let flights = flights.clone();
                thread::spawn(move || flights.done(&1))
            };
            let cancel = {
                let flights = flights.clone();
                thread::spawn(move || flights.cancel(&1))
            };
            done.join().unwrap();
            cancel.join().unwrap();
            assert!(cancelled.load(Ordering::SeqCst) <= 1);
            assert_eq!(flights.len(), 0);
        });
    }
}
