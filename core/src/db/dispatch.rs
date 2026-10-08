use std::cell::Cell;
use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};

use crate::commit::{CommitResult, ScopeRef};
use crate::identity::{ModelKey, RowId, RowRef};

use super::open::{Db, Shared};

pub(crate) const MAX_PENDING: usize = 256;

thread_local! {
    static CORE_THREAD: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn mark_core_thread() {
    CORE_THREAD.with(|flag| flag.set(true));
}

fn on_core_thread() -> bool {
    CORE_THREAD.with(Cell::get)
}

fn held<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Event {
    pub revision: i64,
    pub models: BTreeSet<ModelKey>,
    pub scopes: BTreeSet<ScopeRef>,
    pub rows: BTreeSet<RowRef>,
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
        self.models.extend(later.models);
        self.scopes.extend(later.scopes);
        self.rows.extend(later.rows);
        self.replaced.extend(later.replaced);
        self.reset_all |= later.reset_all;
        self.reset_models.extend(later.reset_models);
        self
    }
}

#[derive(Default)]
pub(crate) struct Busy {
    count: Mutex<usize>,
    zero: Condvar,
}

impl Busy {
    pub(crate) fn enter(&self) {
        *held(&self.count) += 1;
    }

    pub(crate) fn leave(&self, units: usize) {
        let mut count = held(&self.count);
        *count -= units;
        if *count == 0 {
            self.zero.notify_all();
        }
    }

    pub(crate) fn wait_idle(&self) {
        let mut count = held(&self.count);
        while *count > 0 {
            count = self.zero.wait(count).unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

#[derive(Default)]
struct Pending {
    events: VecDeque<Event>,
    closed: bool,
}

pub(crate) struct Dispatch {
    pending: Mutex<Pending>,
    wake: Condvar,
    pub busy: Arc<Busy>,
}

impl Dispatch {
    pub(crate) fn new() -> Self {
        Self { pending: Mutex::new(Pending::default()), wake: Condvar::new(), busy: Arc::new(Busy::default()) }
    }

    pub(crate) fn push(&self, event: Event) {
        let mut pending = held(&self.pending);
        self.busy.enter();
        pending.events.push_back(event);
        if pending.events.len() > MAX_PENDING {
            let first = pending.events.pop_front().expect("queue is not empty");
            let second = pending.events.pop_front().expect("queue holds two events");
            pending.events.push_front(first.merge(second));
            self.busy.leave(1);
        }
        self.wake.notify_one();
    }

    pub(crate) fn next_batch(&self) -> Option<Vec<Event>> {
        let mut pending = held(&self.pending);
        loop {
            if !pending.events.is_empty() {
                return Some(pending.events.drain(..).collect());
            }
            if pending.closed {
                return None;
            }
            pending = self.wake.wait(pending).unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    pub(crate) fn close(&self) {
        held(&self.pending).closed = true;
        self.wake.notify_all();
    }

    pub(crate) fn start(dispatch: Arc<Dispatch>, shared: Weak<Shared>) {
        std::thread::Builder::new()
            .name("dblayer-dispatch".to_string())
            .spawn(move || {
                mark_core_thread();
                while let Some(batch) = dispatch.next_batch() {
                    let units = batch.len();
                    if let Some(shared) = shared.upgrade() {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| shared.refresh_groups(&batch)));
                    }
                    dispatch.busy.leave(units);
                }
            })
            .expect("dispatch thread starts");
    }
}

impl Db {
    pub fn settle(&self) {
        if on_core_thread() {
            return;
        }
        self.shared.dispatch.busy.wait_idle();
    }
}
