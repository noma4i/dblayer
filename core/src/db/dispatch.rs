use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

use super::lock::{lock as held, wait};

/// Owned work that the outermost operation runs after it released every guard, gate and announcement turn.
#[must_use]
#[derive(Default)]
pub(crate) struct Effects(Vec<Box<dyn FnOnce() + Send>>);

impl Effects {
    pub(crate) fn push(&mut self, effect: impl FnOnce() + Send + 'static) {
        self.0.push(Box::new(effect));
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn run(self) {
        super::lock::assert_no_library_lock("Effects::run");
        for effect in self.0 {
            super::lock::effect(effect);
        }
    }
}

#[derive(Default)]
pub(crate) struct OrderedEffects {
    ordered: Mutex<Ordered>,
    settled: Condvar,
}

#[derive(Default)]
struct Ordered {
    batches: VecDeque<Effects>,
    running: bool,
    runner: Option<std::thread::ThreadId>,
    pushed: u64,
    done: u64,
}

struct Unwinding<'a> {
    effects: &'a OrderedEffects,
    armed: bool,
}

impl Drop for Unwinding<'_> {
    fn drop(&mut self) {
        if self.armed {
            let mut ordered = held(&self.effects.ordered);
            ordered.running = false;
            ordered.runner = None;
            ordered.done += 1;
            drop(ordered);
            self.effects.settled.notify_all();
        }
    }
}

impl OrderedEffects {

    #[cfg(test)]
    pub(crate) fn push(&self, batch: Effects) {
        self.push_ticket(batch);
    }

    /// Queues a batch and returns its place in the queue, which `run_until` waits for.
    pub(crate) fn push_ticket(&self, batch: Effects) -> u64 {
        let mut ordered = held(&self.ordered);
        if batch.is_empty() { return ordered.pushed; }
        ordered.batches.push_back(batch);
        ordered.pushed += 1;
        ordered.pushed
    }

    /// The place of the last queued batch: waiting for it waits for everything queued so far.
    pub(crate) fn last_ticket(&self) -> u64 {
        held(&self.ordered).pushed
    }

    pub(crate) fn run(&self) {
        super::lock::assert_no_library_lock("OrderedEffects::run");
        {
            let mut ordered = held(&self.ordered);
            if ordered.running || ordered.batches.is_empty() { return; }
            ordered.running = true;
            ordered.runner = Some(std::thread::current().id());
        }
        let mut unwinding = Unwinding { effects: self, armed: true };
        loop {
            let batch = {
                let mut ordered = held(&self.ordered);
                let Some(batch) = ordered.batches.pop_front() else {
                    ordered.running = false;
                    ordered.runner = None;
                    unwinding.armed = false;
                    drop(ordered);
                    self.settled.notify_all();
                    return;
                };
                batch
            };
            batch.run();
            held(&self.ordered).done += 1;
            self.settled.notify_all();
        }
    }

    /// Returns once the batch with this ticket ran. A call from inside a running batch returns false at once, since that batch must finish first.
    pub(crate) fn run_until(&self, ticket: u64) -> bool {
        self.run();
        let mut ordered = held(&self.ordered);
        while ordered.done < ticket {
            if ordered.runner == Some(std::thread::current().id()) { return false; }
            if !ordered.running && !ordered.batches.is_empty() {
                drop(ordered);
                self.run();
                ordered = held(&self.ordered);
                continue;
            }
            ordered = wait(&self.settled, ordered);
        }
        true
    }

    #[cfg(test)]
    pub(crate) fn is_idle(&self) -> bool {
        self.ordered.try_lock().is_ok_and(|ordered| !ordered.running && ordered.batches.is_empty())
    }

}
