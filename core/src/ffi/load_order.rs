use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

#[derive(Default)]
struct Turns {
    issued: u64,
    floor: u64,
    done: BTreeSet<u64>,
    /// Leaves of callers that gave up while earlier turns were still to register: each runs once the turns through its ticket registered.
    leaves: Vec<(u64, Box<dyn FnOnce() + Send>)>,
}

#[derive(Default)]
pub(super) struct LoadOrder {
    turns: Mutex<Turns>,
    moved: Notify,
}

pub(super) struct Turn {
    order: Arc<LoadOrder>,
    ticket: u64,
}

impl Drop for Turn {
    fn drop(&mut self) { self.order.finish(self.ticket); }
}

impl LoadOrder {
    pub(super) fn begin(self: &Arc<Self>) -> Turn {
        let mut turns = crate::lock::lock(&self.turns);
        turns.issued += 1;
        Turn { order: self.clone(), ticket: turns.issued }
    }

    pub(super) fn issued(&self) -> u64 { crate::lock::lock(&self.turns).issued }

    pub(super) fn registered_through(&self, upto: u64) -> bool { crate::lock::lock(&self.turns).floor >= upto }

    /// Runs `leave` at once when the turns through `upto` registered, else in the `Drop` of the turn that completes them.
    pub(super) fn leave_after(&self, upto: u64, leave: impl FnOnce() + Send + 'static) {
        let mut turns = crate::lock::lock(&self.turns);
        if turns.floor < upto {
            turns.leaves.push((upto, Box::new(leave)));
            return;
        }
        drop(turns);
        leave();
    }

    pub(super) async fn wait_through(&self, upto: u64) {
        loop {
            let moved = self.moved.notified();
            if self.registered_through(upto) { return; }
            moved.await;
        }
    }

    fn finish(&self, ticket: u64) {
        let due = {
            let mut turns = crate::lock::lock(&self.turns);
            turns.done.insert(ticket);
            let mut next = turns.floor + 1;
            while turns.done.remove(&next) {
                turns.floor = next;
                next += 1;
            }
            let floor = turns.floor;
            let (due, kept) = std::mem::take(&mut turns.leaves).into_iter().partition(|(upto, _)| *upto <= floor);
            turns.leaves = kept;
            due
        };
        crate::lock::effect(|| self.moved.notify_waiters());
        for (_, leave) in due {
            leave();
        }
    }
}

impl Turn {
    pub(super) async fn wait(&self) { self.order.wait_through(self.ticket - 1).await; }
}
