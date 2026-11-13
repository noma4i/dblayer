use std::collections::BTreeSet;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// The order in which the calls of one database started: a place is taken in O(1) on the thread of the caller and passes when its call reaches an owner or ends.
#[derive(Default)]
pub struct Order {
    state: std::sync::Mutex<Places>,
}

#[derive(Default)]
struct Places {
    next: u64,
    waiting: BTreeSet<u64>,
    wakers: Vec<Waker>,
}

pub struct Place {
    order: Arc<Order>,
    number: u64,
}

thread_local! { static ENTERING: std::cell::RefCell<Option<Arc<Place>>> = const { std::cell::RefCell::new(None) }; }

fn held(state: &std::sync::Mutex<Places>) -> std::sync::MutexGuard<'_, Places> { state.lock().unwrap_or_else(std::sync::PoisonError::into_inner) }

impl Order {
    pub fn take(self: &Arc<Self>) -> Arc<Place> {
        let mut places = held(&self.state);
        places.next += 1;
        let number = places.next;
        places.waiting.insert(number);
        Arc::new(Place { order: self.clone(), number })
    }
}

impl Place {
    /// Pending until every smaller place has passed.
    pub fn turn(&self) -> impl std::future::Future<Output = ()> + '_ {
        std::future::poll_fn(move |cx: &mut Context<'_>| {
            let mut places = held(&self.order.state);
            if places.waiting.range(..self.number).next().is_none() { return Poll::Ready(()); }
            if !places.wakers.iter().any(|known| known.will_wake(cx.waker())) { places.wakers.push(cx.waker().clone()); }
            Poll::Pending
        })
    }

    pub fn pass(&self) {
        let woken = {
            let mut places = held(&self.order.state);
            if !places.waiting.remove(&self.number) { return; }
            std::mem::take(&mut places.wakers)
        };
        woken.into_iter().for_each(Waker::wake);
    }
}

impl Drop for Place {
    fn drop(&mut self) { self.pass(); }
}

/// Polls `poll` with `place` as the place of the call that enters an owner from this thread.
pub fn polling_place<R>(place: Option<&Arc<Place>>, poll: impl FnOnce() -> R) -> R {
    let previous = ENTERING.replace(place.cloned());
    let result = poll();
    ENTERING.set(previous);
    result
}

/// The call polled on this thread sent its first input to an owner.
pub(super) fn entered_owner() {
    if let Some(place) = ENTERING.with(|entering| entering.borrow().clone()) { place.pass(); }
}
