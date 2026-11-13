use std::collections::BTreeSet;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// The order in which the calls of one database started: a place is taken in O(1) on the thread of the caller and passes when its call reaches an owner or ends.
#[derive(Default)]
pub struct CallOrder {
    state: std::sync::Mutex<Places>,
}

#[derive(Default)]
struct Places {
    next: u64,
    waiting: BTreeSet<u64>,
    wakers: Vec<Waker>,
}

pub struct Place {
    order: Arc<CallOrder>,
    number: u64,
}

thread_local! { static ENTERING: std::cell::RefCell<Option<Arc<Place>>> = const { std::cell::RefCell::new(None) }; }

fn held(state: &std::sync::Mutex<Places>) -> std::sync::MutexGuard<'_, Places> { state.lock().unwrap_or_else(std::sync::PoisonError::into_inner) }

impl CallOrder {
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

/// The task of a bridge call and the place its drop takes: the place taken on the thread of the drop passes when the task is gone, so the next call of the database enters after the consequences of the dropped one.
#[derive(Default)]
pub struct DropPlace(std::sync::Mutex<DropState>);

#[derive(Default)]
enum DropState {
    #[default]
    Running,
    Gone,
    Held(Arc<Place>),
}

impl DropPlace {
    /// Takes the place of the drop unless the task is gone already or holds one.
    pub fn hold(&self, order: &Arc<CallOrder>) {
        let mut state = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*state, DropState::Running) { *state = DropState::Held(order.take()); }
    }

    /// The task is gone: its drop place passes.
    pub fn gone(&self) {
        let state = std::mem::replace(&mut *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner), DropState::Gone);
        if let DropState::Held(place) = state { place.pass(); }
    }
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
