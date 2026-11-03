use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use tokio::sync::oneshot;

use super::{Cause, Outcome, ResumeOwner, ResumeReport, SyncError};
use crate::sync::locked;
use crate::sync::rig::*;
use crate::test_wait::{join_within, spin_until, within_secs};

struct Logged {
    name: &'static str,
    log: Arc<Mutex<Vec<&'static str>>>,
    gate: Option<Arc<Mutex<()>>>,
    entered: AtomicBool,
}

impl Logged {
    fn new(name: &'static str, log: &Arc<Mutex<Vec<&'static str>>>, gate: Option<&Arc<Mutex<()>>>) -> Arc<Self> {
        Arc::new(Self { name, log: log.clone(), gate: gate.cloned(), entered: AtomicBool::new(false) })
    }
}

impl Wake for Logged {
    fn wake(self: Arc<Self>) {
        self.entered.store(true, Ordering::SeqCst);
        if let Some(gate) = &self.gate { drop(locked(gate)); }
        locked(&self.log).push(self.name);
    }
}

fn poll_once(receiver: &mut std::pin::Pin<Box<oneshot::Receiver<Outcome>>>, waker: Arc<Logged>) -> Poll<Result<Outcome, oneshot::error::RecvError>> {
    receiver.as_mut().poll(&mut Context::from_waker(&Waker::from(waker)))
}

#[test]
fn resume_wakes_follow_the_state_order_while_one_wake_is_blocked() {
    let owner = Arc::new(ResumeOwner::new());
    let (first_sender, first_receiver) = oneshot::channel();
    let (second_sender, second_receiver) = oneshot::channel();
    let (runner, _) = owner.request(Cause::Explicit, Some(first_sender)).expect("the first request starts a runner");
    assert!(owner.request(Cause::Explicit, Some(second_sender)).is_none(), "the second request queues behind the runner");
    let log = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(Mutex::new(()));
    let first = Logged::new("first", &log, Some(&gate));
    let (mut first_receiver, mut second_receiver) = (Box::pin(first_receiver), Box::pin(second_receiver));
    assert!(poll_once(&mut first_receiver, first.clone()).is_pending());
    assert!(poll_once(&mut second_receiver, Logged::new("second", &log, None)).is_pending());

    let blocked = gate.lock().unwrap();
    let first_pass = std::thread::spawn({
        let owner = owner.clone();
        move || owner.finish(runner, Ok(ResumeReport::default()))
    });
    spin_until("the first answer reached its blocked wake", || first.entered.load(Ordering::SeqCst));
    let second_pass = std::thread::spawn({
        let owner = owner.clone();
        move || owner.finish(runner, Ok(ResumeReport::default()))
    });
    join_within(second_pass);
    assert!(locked(&log).is_empty(), "the later answer waits for the earlier wake instead of overtaking it");
    drop(blocked);
    join_within(first_pass);
    assert_eq!(*locked(&log), ["first", "second"]);
    assert!(matches!(poll_once(&mut first_receiver, Logged::new("again", &log, None)), Poll::Ready(Ok(Ok(_)))));
    assert!(matches!(poll_once(&mut second_receiver, Logged::new("again", &log, None)), Poll::Ready(Ok(Ok(_)))));
}

#[tokio::test]
async fn resume_drop_completes_waiters() {
    let rig = rig();
    let inner = rig.sync.inner.clone();
    let (current_sender, current_receiver) = oneshot::channel();
    let (queued_sender, queued_receiver) = oneshot::channel();
    let runner = inner.begin_resume(Cause::Explicit, Some(current_sender)).expect("the first request starts a runner");
    assert!(inner.begin_resume(Cause::Explicit, Some(queued_sender)).is_none());
    drop(runner);
    assert_eq!(within_secs(30, current_receiver).await, Ok(Err(SyncError::Cancelled)));
    assert_eq!(within_secs(30, queued_receiver).await, Ok(Err(SyncError::Cancelled)));
    within_secs(30, inner.db.shared.store.lifecycle.settled()).await;
    assert!(inner.begin_resume(Cause::Event, None).is_some(), "the owner accepts a new resume after the abandoned one");
}
