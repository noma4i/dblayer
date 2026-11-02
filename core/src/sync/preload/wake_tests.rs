use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use super::{AppLoadHold, Inner};
use crate::sync::locked;
use crate::sync::rig::*;
use crate::test_wait::{join_within, within_secs, yield_until};

struct ReenterOnWake {
    inner: Arc<Inner>,
    holds: Mutex<Vec<AppLoadHold>>,
    wakes: AtomicUsize,
}

impl Wake for ReenterOnWake {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        let released = locked(&self.holds).pop();
        drop(released);
        let taken = self.inner.app_load();
        locked(&self.holds).push(taken);
    }
}

#[tokio::test]
async fn preload_hold_release_is_reentrant() {
    let rig = rig();
    let inner = rig.sync.inner.clone();
    let (first, second) = (inner.app_load(), inner.app_load());
    let waker = Arc::new(ReenterOnWake { inner: inner.clone(), holds: Mutex::new(vec![second]), wakes: AtomicUsize::new(0) });
    let mut busy = inner.preload.busy.subscribe();
    let mut changed = Box::pin(busy.changed());
    assert!(changed.as_mut().poll(&mut Context::from_waker(&Waker::from(waker.clone()))).is_pending());

    let release = std::thread::spawn(move || drop(first));
    join_within(release);

    assert_eq!(waker.wakes.load(Ordering::SeqCst), 1, "the release woke the waiter once");
    assert!(matches!(changed.as_mut().poll(&mut Context::from_waker(&Waker::from(waker.clone()))), Poll::Ready(Ok(()))));
    drop(changed);
    assert_eq!(inner.preload.app_loads(), 1, "the waiter released one hold and took one");
    assert_eq!(*busy.borrow(), 1, "the published count follows the state order");
    assert!(inner.preload.effects.is_idle());
    locked(&waker.holds).clear();
    assert_eq!(inner.preload.app_loads(), 0);
    assert_eq!(*busy.borrow(), 0);
}

#[tokio::test]
async fn app_loads_settled_waits_for_the_last_hold() {
    let rig = rig();
    let inner = rig.sync.inner.clone();
    let (first, second) = (inner.app_load(), inner.app_load());
    let settled = tokio::spawn({
        let inner = inner.clone();
        async move { inner.app_loads_settled().await }
    });
    drop(first);
    yield_until("the settle waiter registered", || inner.preload.busy.receiver_count() > 0).await;
    assert!(!settled.is_finished(), "a hold is still open, preload keeps yielding");
    drop(second);
    within_secs(30, settled).await.expect("the settle waiter ends with the last hold");
}
