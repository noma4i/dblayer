use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::identity::RowId;
use super::row::Row;
use crate::pacing::Counters;

use super::open::locked;
use super::diff::Diff;
use super::dispatch::{mark_core_thread, Busy};
use super::watch::{changed_rows, Body, Change, CountChange, Loaded, Related, RowsBody, Version};

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
    pub loaded: Loaded,
}

#[derive(Default)]
pub(crate) struct Mailbox {
    latest: Option<Arc<Version>>,
    loaded: Loaded,
    replaced: Vec<(RowId, RowId)>,
    overflowed: bool,
    steps: usize,
    scheduled: bool,
    delivered: Option<Arc<Version>>,
}

impl Mailbox {
    pub(crate) fn push(&mut self, version: Arc<Version>, replaced: &[(RowId, RowId)], loaded: Loaded) -> bool {
        self.latest = Some(version);
        self.loaded.rows.extend(loaded.rows);
        for (name, parents) in loaded.related {
            self.loaded.related.entry(name).or_default().extend(parents);
        }
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
            loaded: std::mem::take(&mut self.loaded),
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
        self.loaded = Loaded::default();
    }
}

pub(crate) trait Runnable: Send + Sync {
    fn run(&self);
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

impl Runnable for Subscriber {
    fn run(&self) {
        self.run_deliveries();
    }
}

impl Subscriber {
    pub(crate) fn new(listener: Listener, counters: Arc<Mutex<Counters>>) -> Arc<Self> {
        Arc::new(Self { cancelled: AtomicBool::new(false), mailbox: Mutex::new(Mailbox::default()), listener, counters })
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub(crate) fn post(self: &Arc<Self>, executor: &Arc<Executor>, version: Arc<Version>, replaced: &[(RowId, RowId)], loaded: Loaded) {
        let needs_run = held(&self.mailbox).push(version, replaced, loaded);
        if needs_run {
            executor.schedule(self.clone());
        }
    }

    fn run_deliveries(&self) {
        loop {
            let mut taken = {
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
            self.deliver(&mut taken);
            held(&self.mailbox).delivered = Some(taken.latest);
        }
    }

    fn deliver(&self, taken: &mut Taken) {
        let outcome = match (&self.listener, &taken.latest.body) {
            (Listener::Rows(call), Body::Rows(body)) => {
                let first = taken.delivered.is_none();
                let diff = match &taken.delivered {
                    None => Some(Diff::inserting(&body.entries)),
                    Some(previous) => self.rows_diff(previous, taken, body),
                };
                diff.map(|diff| {
                    let (rows, related) = if first { whole_rows(body, &mut taken.loaded) } else { changed_rows(&diff, &mut taken.loaded.rows, &mut taken.loaded.related) };
                    locked(&self.counters).rows_delivered += rows.len();
                    let change = Change { revision: taken.latest.revision, rows, related, diff, full: first, size: body.entries.len() };
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

    fn rows_diff(&self, previous: &Version, taken: &Taken, new: &RowsBody) -> Option<Diff> {
        let Body::Rows(old) = &previous.body else { return None };
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

fn whole_rows(body: &RowsBody, loaded: &mut Loaded) -> (Vec<Row>, Related) {
    let rows = body.entries.iter().filter_map(|entry| loaded.rows.remove(entry.id.as_str())).collect();
    (rows, std::mem::take(&mut loaded.related))
}

#[derive(Default)]
struct Queue {
    ready: VecDeque<Arc<dyn Runnable>>,
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

    pub(crate) fn schedule(self: &Arc<Self>, subscriber: Arc<dyn Runnable>) {
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
