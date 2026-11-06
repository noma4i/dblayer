use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

#[derive(Default)]
struct Turns {
    issued: u64,
    floor: u64,
    done: BTreeSet<u64>,
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

    pub(super) async fn wait_through(&self, upto: u64) {
        loop {
            let moved = self.moved.notified();
            if self.registered_through(upto) { return; }
            moved.await;
        }
    }

    fn finish(&self, ticket: u64) {
        {
            let mut turns = crate::lock::lock(&self.turns);
            turns.done.insert(ticket);
            let mut next = turns.floor + 1;
            while turns.done.remove(&next) {
                turns.floor = next;
                next += 1;
            }
        }
        crate::lock::effect(|| self.moved.notify_waiters());
    }
}

impl Turn {
    pub(super) async fn wait(&self) { self.order.wait_through(self.ticket - 1).await; }
}
