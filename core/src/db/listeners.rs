use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::identity::RowId;
use crate::pacing::Counters;

use super::open::locked;
use super::diff::Diff;
use super::dispatch::{mark_core_thread, Busy};
use super::watch::{Body, Change, CountChange, Version};

const MAX_WORKERS: usize = 64;
pub(crate) const MAX_REPLACED_PAIRS: usize = 10_000;

fn held<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) struct Taken {
    pub latest: Arc<Version>,
    pub delivered: Option<Arc<Version>>,
    pub replaced: Vec<(RowId, RowId)>,
    pub overflowed: bool,
    pub steps: usize,
}

#[derive(Default)]
pub(crate) struct Mailbox {
    latest: Option<Arc<Version>>,
    replaced: Vec<(RowId, RowId)>,
    overflowed: bool,
    steps: usize,
    scheduled: bool,
    delivered: Option<Arc<Version>>,
}

impl Mailbox {
    pub(crate) fn push(&mut self, version: Arc<Version>, replaced: &[(RowId, RowId)]) -> bool {
        self.latest = Some(version);
        for (from, to) in replaced {
            self.record_replacement(from, to);
        }
        self.steps += 1;
        !std::mem::replace(&mut self.scheduled, true)
    }

    fn record_replacement(&mut self, from: &RowId, to: &RowId) {
        if self.overflowed {
            return;
        }
        match self.replaced.iter_mut().find(|(_, last)| last == from) {
            Some(pair) => pair.1 = to.clone(),
            None => self.replaced.push((from.clone(), to.clone())),
        }
        if self.replaced.len() > MAX_REPLACED_PAIRS {
            self.replaced.clear();
            self.overflowed = true;
        }
    }

    pub(crate) fn take(&mut self) -> Option<Taken> {
        let latest = self.latest.take()?;
        Some(Taken {
            latest,
            delivered: self.delivered.clone(),
            replaced: std::mem::take(&mut self.replaced),
            overflowed: std::mem::take(&mut self.overflowed),
            steps: std::mem::take(&mut self.steps),
        })
    }

    pub(crate) fn finish(&mut self) {
        self.scheduled = false;
    }

    fn drop_pending(&mut self) {
        self.latest = None;
        self.replaced.clear();
        self.overflowed = false;
        self.steps = 0;
    }
}

pub(crate) enum Listener {
    Rows(Box<dyn Fn(&Change) + Send + Sync>),
    Count(Box<dyn Fn(&CountChange) + Send + Sync>),
}

pub(crate) struct Subscriber {
    cancelled: AtomicBool,
    mailbox: Mutex<Mailbox>,
    listener: Listener,
    counters: Arc<Mutex<Counters>>,
}

impl Subscriber {
    pub(crate) fn new(listener: Listener, counters: Arc<Mutex<Counters>>) -> Arc<Self> {
        Arc::new(Self { cancelled: AtomicBool::new(false), mailbox: Mutex::new(Mailbox::default()), listener, counters })
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub(crate) fn post(self: &Arc<Self>, executor: &Arc<Executor>, version: Arc<Version>, replaced: &[(RowId, RowId)]) {
        let needs_run = held(&self.mailbox).push(version, replaced);
        if needs_run {
            executor.schedule(self.clone());
        }
    }

    fn run(&self) {
        loop {
            let taken = {
                let mut mailbox = held(&self.mailbox);
                match mailbox.take() {
                    Some(taken) => taken,
                    None => {
                        mailbox.finish();
                        return;
                    }
                }
            };
            if self.cancelled.load(Ordering::SeqCst) {
                let mut mailbox = held(&self.mailbox);
                mailbox.drop_pending();
                mailbox.finish();
                return;
            }
            self.deliver(&taken);
            held(&self.mailbox).delivered = Some(taken.latest);
        }
    }

    fn deliver(&self, taken: &Taken) {
        let outcome = match (&self.listener, &taken.latest.body) {
            (Listener::Rows(call), Body::Rows(body)) => {
                let diff = match &taken.delivered {
                    None => Some(Diff::inserting(&body.entries)),
                    Some(previous) => self.rows_diff(previous, taken),
                };
                diff.map(|diff| {
                    let change = Change { revision: taken.latest.revision, rows: body.rows.clone(), related: body.related.clone(), diff };
                    catch_unwind(AssertUnwindSafe(|| call(&change)))
                })
            }
            (Listener::Count(call), Body::Count(count)) => {
                let changed = !matches!(taken.delivered.as_deref(), Some(Version { body: Body::Count(previous), .. }) if previous == count);
                changed.then(|| {
                    let change = CountChange { revision: taken.latest.revision, count: *count };
                    catch_unwind(AssertUnwindSafe(|| call(&change)))
                })
            }
            _ => None,
        };
        if outcome.is_some() {
            locked(&self.counters).observer_calls += 1;
        }
    }

    fn rows_diff(&self, previous: &Version, taken: &Taken) -> Option<Diff> {
        let (Body::Rows(old), Body::Rows(new)) = (&previous.body, &taken.latest.body) else { return None };
        if previous.position() >= taken.latest.position() {
            return None;
        }
        let diff = match &new.step {
            Some(step) if taken.steps == 1 && step.from == previous.position() => step.diff.clone(),
            _ => Diff::between(&old.entries, &new.entries, if taken.overflowed { &[] } else { &taken.replaced }),
        };
        (!diff.is_empty()).then_some(diff)
    }
}

#[derive(Default)]
struct Queue {
    ready: VecDeque<Arc<Subscriber>>,
    idle: usize,
    workers: usize,
    closed: bool,
}

pub(crate) struct Executor {
    queue: Mutex<Queue>,
    wake: Condvar,
    busy: Arc<Busy>,
}

impl Executor {
    pub(crate) fn new(busy: Arc<Busy>) -> Arc<Self> {
        Arc::new(Self { queue: Mutex::new(Queue::default()), wake: Condvar::new(), busy })
    }

    fn schedule(self: &Arc<Self>, subscriber: Arc<Subscriber>) {
        self.busy.enter();
        let mut queue = held(&self.queue);
        queue.ready.push_back(subscriber);
        if queue.ready.len() > queue.idle && queue.workers < MAX_WORKERS {
            queue.workers += 1;
            let executor = self.clone();
            std::thread::Builder::new().name("dblayer-listener".to_string()).spawn(move || executor.work()).expect("listener thread starts");
        } else {
            self.wake.notify_one();
        }
    }

    fn work(&self) {
        mark_core_thread();
        loop {
            let next = {
                let mut queue = held(&self.queue);
                loop {
                    if let Some(subscriber) = queue.ready.pop_front() {
                        break Some(subscriber);
                    }
                    if queue.closed {
                        break None;
                    }
                    queue.idle += 1;
                    queue = self.wake.wait(queue).unwrap_or_else(|poisoned| poisoned.into_inner());
                    queue.idle -= 1;
                }
            };
            let Some(subscriber) = next else { return };
            subscriber.run();
            self.busy.leave(1);
        }
    }

    pub(crate) fn close(&self) {
        held(&self.queue).closed = true;
        self.wake.notify_all();
    }
}
