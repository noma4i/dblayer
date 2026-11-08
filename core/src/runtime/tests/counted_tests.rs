use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use super::toys_tests::*;
use super::*;

struct Parked {
    waker: Arc<Mutex<Option<Waker>>>,
    wake_on_drop: bool,
}

impl Future for Parked {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        *self.waker.lock().unwrap() = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for Parked {
    fn drop(&mut self) {
        if self.wake_on_drop { if let Some(waker) = self.waker.lock().unwrap().take() { waker.wake(); } }
    }
}

/// A counted task that is gone takes no count back: a wake that comes after its poll, while its future drops, or after it still leaves the barrier idle.
#[test]
fn a_wake_that_comes_after_the_end_of_a_counted_task_leaves_the_barrier_idle() {
    for (wake_on_drop, wake_after) in [(true, false), (false, true)] {
        let mut builder = RuntimeBuilder::for_tests();
        let handle = builder.handle();
        builder.add::<Toy>("toy", ToyState::default(), ToyRunner::default());
        let runtime = builder.start().unwrap();
        let waker = Arc::new(Mutex::new(None));
        let mut task = Box::pin(counted(Some(handle.launch()), Parked { waker: waker.clone(), wake_on_drop }));
        assert_eq!(handle.pending(), 1, "the launch counts the task before its first poll");
        assert!(task.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        assert_eq!(handle.pending(), 0, "a task that waits for the outside holds no count");
        drop(task);
        let late = waker.lock().unwrap().take();
        if wake_after { late.expect("the task kept the waker").wake(); }
        assert_eq!(handle.pending(), 0, "a wake of a task that is gone (wake_on_drop {wake_on_drop}) held the barrier");
        runtime.barrier();
    }
}
