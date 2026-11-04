use std::cell::Cell;
use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex, Weak};

use crate::commit::{CommitResult, ScopeRef};
use crate::identity::{ModelKey, RowId, RowRef};

use super::error::DbError;
use super::open::{Db, Shared};
use super::reads::CompletedRead;

pub(crate) const MAX_PENDING: usize = 256;
pub(crate) const MAX_REPLACED_LOG: usize = 4096;

thread_local! {
    static CORE_THREAD: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn mark_core_thread() {
    CORE_THREAD.with(|flag| flag.set(true));
}

thread_local! {
    static OWNER_THREAD: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn on_owner_thread() -> bool {
    OWNER_THREAD.with(Cell::get)
}

fn on_core_thread() -> bool {
    CORE_THREAD.with(Cell::get)
}

use super::lock::{lock as held, wait};

/// Owned work that the outermost operation runs after it released every guard, gate and announcement turn.
#[must_use]
#[derive(Default)]
pub(crate) struct Effects(Vec<Box<dyn FnOnce() + Send>>);

impl Effects {
    pub(crate) fn push(&mut self, effect: impl FnOnce() + Send + 'static) {
        self.0.push(Box::new(effect));
    }

    pub(crate) fn release(&mut self, value: impl Send + 'static) {
        self.push(move || drop(value));
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

    pub(crate) fn under<S, R>(&self, state: &Mutex<S>, apply: impl FnOnce(&mut S, &mut Effects) -> R) -> R {
        let mut effects = Effects::default();
        let result = {
            let mut guard = held(state);
            let result = apply(&mut guard, &mut effects);
            self.push(effects);
            result
        };
        self.run();
        result
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

    #[cfg(test)]
    pub(crate) fn is_free(&self) -> bool {
        crate::test_wait::mutex_free(&self.ordered)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Event {
    pub revision: i64,
    pub epoch: u64,
    pub models: BTreeSet<ModelKey>,
    pub scopes: BTreeSet<ScopeRef>,
    pub rows: BTreeSet<RowRef>,
    pub members: BTreeSet<RowRef>,
    pub replaced: Vec<(RowId, RowId)>,
    pub reset_all: bool,
    pub reset_models: BTreeSet<ModelKey>,
}

impl Event {
    pub(crate) fn of_commit(result: &CommitResult) -> Self {
        Self {
            revision: result.revision.unwrap_or_default(),
            models: result.changed_models(),
            scopes: result.scopes.clone(),
            rows: result.rows.iter().map(|change| change.row.clone()).collect(),
            members: result.members.clone(),
            replaced: result.replaced.iter().map(|(from, to)| (from.id.clone(), to.clone())).collect(),
            ..Self::default()
        }
    }

    pub(crate) fn of_reset(revision: i64, models: Option<&[String]>) -> Self {
        Self {
            revision,
            reset_all: models.is_none(),
            reset_models: models.into_iter().flatten().map(|model| ModelKey::new(model)).collect(),
            ..Self::default()
        }
    }

    pub(crate) fn merge(mut self, later: Event) -> Event {
        self.revision = self.revision.max(later.revision);
        self.epoch = self.epoch.max(later.epoch);
        self.models.extend(later.models);
        self.scopes.extend(later.scopes);
        self.rows.extend(later.rows);
        self.members.extend(later.members);
        self.replaced.extend(later.replaced);
        self.reset_all |= later.reset_all;
        self.reset_models.extend(later.reset_models);
        self
    }
}

#[derive(Default)]
pub(crate) struct Progress {
    generation: Mutex<u64>,
    changed: Condvar,
}

impl Progress {
    pub(crate) fn advance(&self) {
        *held(&self.generation) += 1;
        self.changed.notify_all();
    }

    fn generation(&self) -> u64 {
        *held(&self.generation)
    }

    fn wait_after(&self, seen: u64) {
        let mut generation = held(&self.generation);
        while *generation == seen {
            generation = wait(&self.changed, generation);
        }
    }
}

#[derive(Default)]
struct Pending {
    events: VecDeque<Event>,
    replaced: Vec<(i64, RowId, RowId)>,
    dropped_through: i64,
    reads: VecDeque<CompletedRead>,
    processing_through: Option<i64>,
    closed: bool,
    #[cfg(test)]
    paused: bool,
}

#[cfg(test)]
pub(crate) struct OwnerPaused(Arc<Dispatch>);

#[cfg(test)]
impl Drop for OwnerPaused {
    fn drop(&mut self) {
        held(&self.0.pending).paused = false;
        self.0.wake.notify_all();
    }
}

pub(crate) enum Work {
    Read(Box<CompletedRead>),
    Batch(Vec<Event>),
}

pub(crate) struct Dispatch {
    pending: Mutex<Pending>,
    wake: Condvar,
    pub progress: Arc<Progress>,
}

impl Dispatch {
    pub(crate) fn new() -> Self {
        Self { pending: Mutex::new(Pending::default()), wake: Condvar::new(), progress: Arc::new(Progress::default()) }
    }

    pub(crate) fn log_replaced(&self, revision: i64, pairs: impl Iterator<Item = (RowId, RowId)>) {
        let mut pending = held(&self.pending);
        pending.replaced.extend(pairs.map(|(from, to)| (revision, from, to)));
        if pending.replaced.len() > MAX_REPLACED_LOG {
            let excess = pending.replaced.len() - MAX_REPLACED_LOG;
            pending.dropped_through = pending.replaced.drain(..excess).map(|(revision, _, _)| revision).max().unwrap_or_default().max(pending.dropped_through);
        }
    }

    pub(crate) fn push(&self, event: Event) {
        let mut pending = held(&self.pending);
        pending.events.push_back(event);
        if pending.events.len() > MAX_PENDING {
            let first = pending.events.pop_front().expect("queue is not empty");
            let second = pending.events.pop_front().expect("queue holds two events");
            pending.events.push_front(first.merge(second));
        }
        self.progress.advance();
        self.wake.notify_one(); // lock-ok: condvar schedules an OS thread
    }

    pub(crate) fn push_read(&self, result: CompletedRead) {
        held(&self.pending).reads.push_back(result);
        self.wake.notify_one(); // lock-ok: pending guard ended at statement
    }

    pub(crate) fn next_work(&self) -> Option<Work> {
        let mut pending = held(&self.pending);
        loop {
            #[cfg(test)]
            if pending.paused {
                pending = wait(&self.wake, pending);
                continue;
            }
            if let Some(result) = pending.reads.pop_front() {
                pending.processing_through = Some(i64::MIN);
                return Some(Work::Read(Box::new(result)));
            }
            if !pending.events.is_empty() {
                let batch: Vec<Event> = pending.events.drain(..).collect();
                pending.processing_through = batch.iter().map(|event| event.revision).max();
                return Some(Work::Batch(batch));
            }
            if pending.closed {
                return None;
            }
            pending = wait(&self.wake, pending);
        }
    }

    pub(crate) fn finished_work(&self) {
        held(&self.pending).processing_through = None;
        self.progress.advance();
        #[cfg(test)]
        self.wake.notify_all();
    }

    #[cfg(test)]
    pub(crate) fn pause_owner(self: &Arc<Self>) -> OwnerPaused {
        let mut pending = held(&self.pending);
        while pending.processing_through.is_some() {
            pending = wait(&self.wake, pending);
        }
        pending.paused = true;
        OwnerPaused(self.clone())
    }

    pub(crate) fn quiet_through(&self, revision: i64) -> bool {
        let pending = held(&self.pending);
        pending.reads.is_empty() && pending.processing_through.is_none_or(|through| through > revision) && pending.events.iter().all(|event| event.revision > revision)
    }

    pub(crate) fn replaced_between(&self, after: i64, upto: i64) -> Vec<(RowId, RowId)> {
        let pending = held(&self.pending);
        if pending.dropped_through > after {
            return Vec::new();
        }
        pending.replaced.iter().filter(|(revision, _, _)| *revision > after && *revision <= upto).map(|(_, from, to)| (from.clone(), to.clone())).collect()
    }

    #[cfg(test)]
    pub(crate) fn queued(&self) -> usize {
        held(&self.pending).events.len()
    }

    pub(crate) fn close(&self) {
        held(&self.pending).closed = true;
        self.wake.notify_all();
    }

    pub(crate) fn start(threads: &Arc<super::threads::LiveThreads>, dispatch: Arc<Dispatch>, shared: Weak<Shared>) -> Result<(), DbError> {
        threads.start(crate::task::Carry::Detached, "dblayer-dispatch", move || {
                mark_core_thread();
                OWNER_THREAD.with(|flag| flag.set(true));
                while let Some(work) = dispatch.next_work() {
                    if let Some(shared) = shared.upgrade() {
                        match work {
                            Work::Read(result) => shared.land_read(*result),
                            Work::Batch(batch) => shared.refresh_groups(&batch),
                        }
                    }
                    dispatch.finished_work();
                }
            })
    }
}

impl Db {
    /// Returns when the owner has handled every commit up to `revision`, every group has read it and every subscriber has taken the result. It reads the mark that the bridge reads as `rows_at`.
    #[cfg(any(test, feature = "stress"))]
    pub(crate) fn delivered(&self) -> bool {
        if self.shared.store.lifecycle.snapshot().closed { return true; }
        let revision = self.revision_seq().unwrap_or(0);
        self.shared.dispatch.quiet_through(revision) && self.shared.reached_through(revision)
    }

    #[cfg(any(test, feature = "stress"))]
    pub(crate) fn await_delivered(&self) {
        self.await_revision(self.revision_seq().unwrap_or(0));
    }

    pub fn await_revision(&self, revision: i64) {
        if on_core_thread() {
            return;
        }
        loop {
            let seen = self.shared.dispatch.progress.generation();
            if self.shared.dispatch.quiet_through(revision) && self.shared.reached_through(revision) {
                return;
            }
            self.shared.dispatch.progress.wait_after(seen);
        }
    }
}
