use std::collections::{HashSet, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::identity::RowId;
use super::row::Row;
use crate::pacing::Counters;

use super::open::locked;
use super::DbError;
use super::diff::Diff;
use super::dispatch::mark_core_thread;
use super::threads::{Busy, LiveThreads};
use super::watch::{changed_rows, Body, Change, CountChange, Loaded, Related, RowsBody, Step, Version};

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
    pub touched: Option<HashSet<String>>,
    pub loaded: Loaded,
}

#[derive(Default)]
pub(crate) struct Mailbox {
    latest: Option<Arc<Version>>,
    loaded: Loaded,
    replaced: Vec<(RowId, RowId)>,
    overflowed: bool,
    steps: usize,
    touched: HashSet<String>,
    unmergeable: bool,
    scheduled: bool,
    delivered: Option<Arc<Version>>,
    restarted: bool,
    paused: bool,
    owes_run: bool,
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
        match self.latest.as_ref().map(|latest| &latest.body) {
            Some(Body::Rows(rows)) if matches!(rows.step, Some(Step { touched: Some(_), .. })) => {
                if let Some(Step { touched: Some(ids), .. }) = &rows.step {
                    self.touched.extend(ids.iter().cloned());
                }
            }
            _ => self.unmergeable = true,
        }
        self.steps += 1;
        self.wants_run()
    }

    pub(crate) fn pause(&mut self) {
        self.paused = true;
    }


    #[cfg(test)]
    pub(crate) fn resume(&mut self) -> bool {
        self.paused = false;
        std::mem::take(&mut self.owes_run) && !std::mem::replace(&mut self.scheduled, true)
    }

    pub(crate) fn forget_owed(&mut self) {
        if std::mem::take(&mut self.owes_run) {
            self.drop_pending();
        }
    }

    fn wants_run(&mut self) -> bool {
        if self.paused {
            self.owes_run = true;
            return false;
        }
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
        self.restarted = false;
        let latest = self.latest.take()?;
        Some(Taken {
            latest,
            delivered: self.delivered.clone(),
            replaced: std::mem::take(&mut self.replaced),
            overflowed: std::mem::take(&mut self.overflowed),
            steps: std::mem::take(&mut self.steps),
            touched: (!std::mem::take(&mut self.unmergeable)).then(|| std::mem::take(&mut self.touched)),
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
        self.touched.clear();
        self.unmergeable = false;
        self.loaded = Loaded::default();
    }
}

pub(crate) trait Runnable: Send + Sync {
    fn run(&self);
    fn stranded(&self);
}

pub(crate) enum Listener {
    Rows(Box<dyn Fn(&Change) + Send + Sync>),
    Count(Box<dyn Fn(&CountChange) + Send + Sync>),
}

/// A listener call starts only while the subscription is live: starting a call and cancelling are atomic against each other.
pub(crate) struct CallState {
    state: AtomicU8,
    #[cfg(test)]
    pub(crate) before_enter: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl CallState {
    const IDLE: u8 = 0;
    const CALLING: u8 = 1;
    const CANCELLED: u8 = 2;

    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU8::new(Self::IDLE),
            #[cfg(test)]
            before_enter: Mutex::new(None),
        }
    }

    pub(crate) fn cancel(&self) {
        self.state.store(Self::CANCELLED, Ordering::SeqCst);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::SeqCst) == Self::CANCELLED
    }

    pub(crate) fn call(&self, listener: impl FnOnce()) -> bool {
        #[cfg(test)]
        if let Some(hook) = held(&self.before_enter).take() {
            hook();
        }
        if self.state.compare_exchange(Self::IDLE, Self::CALLING, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return false;
        }
        let outcome = catch_unwind(AssertUnwindSafe(listener));
        let _ = self.state.compare_exchange(Self::CALLING, Self::IDLE, Ordering::SeqCst, Ordering::SeqCst);
        drop(outcome);
        true
    }
}

pub(crate) struct Subscriber {
    pub(crate) call_state: CallState,
    mailbox: Mutex<Mailbox>,
    listener: Listener,
    counters: Arc<Mutex<Counters>>,
}

impl Runnable for Subscriber {
    fn run(&self) {
        self.run_deliveries();
    }

    fn stranded(&self) {
        locked(&self.counters).listener_spawn_failures += 1;
        let mut mailbox = held(&self.mailbox);
        mailbox.finish();
    }
}

impl Subscriber {
    pub(crate) fn new(listener: Listener, counters: Arc<Mutex<Counters>>) -> Arc<Self> {
        Arc::new(Self { call_state: CallState::new(), mailbox: Mutex::new(Mailbox::default()), listener, counters })
    }

    pub(crate) fn cancel(&self) {
        self.call_state.cancel();
        held(&self.mailbox).forget_owed();
    }

    pub(crate) fn pause(&self) {
        held(&self.mailbox).pause();
    }

    pub(crate) fn idle(&self) -> bool {
        let mailbox = held(&self.mailbox);
        !mailbox.scheduled && !mailbox.owes_run
    }

    pub(crate) fn post(self: &Arc<Self>, executor: &Arc<Executor>, version: Arc<Version>, replaced: &[(RowId, RowId)], loaded: Loaded) {
        let needs_run = held(&self.mailbox).push(version, replaced, loaded);
        if needs_run {
            executor.schedule(self.clone());
        }
    }

    fn run_deliveries(&self) {
        loop {
            let taken = {
                let mut mailbox = held(&self.mailbox);
                let Some(taken) = mailbox.take() else {
                    mailbox.finish();
                    return;
                };
                taken
            };
            if self.call_state.is_cancelled() {
                let mut mailbox = held(&self.mailbox);
                mailbox.drop_pending();
                mailbox.finish();
                return;
            }
            let mut taken = taken;
            self.deliver(&mut taken);
            let mut mailbox = held(&self.mailbox);
            if !mailbox.restarted {
                mailbox.delivered = Some(taken.latest);
            }
        }
    }

    fn rows_change(&self, taken: &mut Taken, body: &RowsBody) -> Option<Change> {
        let first = taken.delivered.is_none();
        let diff = match &taken.delivered {
            None => Some(Diff::inserting(&body.entries(), if taken.overflowed { &[] } else { &taken.replaced })),
            Some(previous) => self.rows_diff(previous, taken, body),
        };
        diff.map(|diff| {
            let (rows, related) = if first { whole_rows(body, &mut taken.loaded) } else { changed_rows(&diff, &mut taken.loaded.rows, &mut taken.loaded.related) };
            locked(&self.counters).rows_delivered += rows.len();
            Change { revision: taken.latest.revision, rows, related, diff, full: first, size: body.len(), window: body.window.as_ref().map(|part| part.edge) }
        })
    }

    fn count_change(&self, taken: &Taken, count: i64) -> Option<CountChange> {
        let changed = !matches!(taken.delivered.as_deref(), Some(Version { body: Body::Count(previous), .. }) if *previous == count);
        changed.then(|| CountChange { revision: taken.latest.revision, count })
    }

    pub(crate) fn deliver(&self, taken: &mut Taken) {
        let latest = taken.latest.clone();
        let outcome = match (&self.listener, &latest.body) {
            (Listener::Rows(call), Body::Rows(body)) => self.rows_change(taken, body).map(|change| self.call_state.call(|| call(&change))),
            (Listener::Count(call), Body::Count(count)) => self.count_change(taken, *count).map(|change| self.call_state.call(|| call(&change))),
            _ => None,
        };
        if outcome == Some(true) {
            locked(&self.counters).observer_calls += 1;
        }
    }

    pub(crate) fn rows_diff(&self, previous: &Version, taken: &Taken, new: &RowsBody) -> Option<Diff> {
        let Body::Rows(old) = &previous.body else { return None };
        if previous.position() >= taken.latest.position() && !new.reshaped_from(old) {
            return None;
        }
        let diff = match &new.step {
            Some(step) if taken.steps == 1 && step.from == previous.position() => step.diff.clone(),
            _ => {
                let replaced = if taken.overflowed { &[] } else { &taken.replaced[..] };
                match (&taken.touched, old.ranking(), new.ranking()) {
                    (Some(touched), Some(before), Some(after)) => Diff::stepping(before, after, touched, replaced),
                    _ => Diff::between(&old.entries()[..], &new.entries()[..], replaced),
                }
            }
        };
        (!diff.is_empty() || new.edge_differs(old)).then_some(diff)
    }
}

fn whole_rows(body: &RowsBody, loaded: &mut Loaded) -> (Vec<Row>, Related) {
    let rows = body.entries().iter().filter_map(|entry| loaded.rows.remove(entry.id.as_str())).collect();
    (rows, std::mem::take(&mut loaded.related))
}

#[derive(Default)]
struct Queue {
    ready: VecDeque<(Arc<dyn Runnable>, Busy)>,
    idle: usize,
    workers: usize,
    closed: bool,
}

pub(crate) struct Executor {
    queue: Mutex<Queue>,
    wake: Condvar,
    threads: Arc<LiveThreads>,
    #[cfg(test)]
    pub(crate) refuse_spawn: std::sync::atomic::AtomicBool,
}

impl Executor {
    pub(crate) fn new(threads: Arc<LiveThreads>) -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(Queue::default()),
            wake: Condvar::new(),
            threads,
            #[cfg(test)]
            refuse_spawn: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub(crate) fn schedule(self: &Arc<Self>, subscriber: Arc<dyn Runnable>) {
        let mut queue = held(&self.queue);
        queue.ready.push_back((subscriber, self.threads.busy()));
        if queue.ready.len() <= queue.idle || queue.workers >= MAX_WORKERS {
            self.wake.notify_one(); // lock-ok: condvar schedules an OS thread
            return;
        }
        queue.workers += 1;
        let Err(_) = self.spawn_worker(self.clone()) else { return };
        queue.workers -= 1;
        if queue.workers > 0 {
            self.wake.notify_one(); // lock-ok: condvar schedules an OS thread
            return;
        }
        let stranded = queue.ready.pop_back();
        drop(queue);
        if let Some((subscriber, busy)) = stranded {
            subscriber.stranded();
            drop(busy);
        }
    }

    fn spawn_worker(&self, executor: Arc<Self>) -> Result<(), String> {
        #[cfg(test)]
        if self.refuse_spawn.load(Ordering::SeqCst) {
            return Err("refused by the test".into());
        }
        super::threads::spawn_named(crate::task::Carry::Detached, "dblayer-listener", move || executor.work()).map_err(|error| match error {
            DbError::ThreadSpawn { detail, .. } => detail,
            other => other.to_string(),
        })
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
            let Some((subscriber, busy)) = next else { return };
            subscriber.run();
            drop(busy);
        }
    }

    pub(crate) fn close(&self) {
        held(&self.queue).closed = true;
        self.wake.notify_all();
    }
}
