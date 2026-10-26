use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, Weak};

use crate::identity::{ModelKey, RowId};
use crate::pacing::Counters;

use super::diff::Stamp;
use super::dispatch::Event;
use super::error::DbError;
use super::listeners::{Executor, Runnable};
use super::open::{locked, Db, Shared};
use super::observe::Observers;
use super::reads::{Position, ReadJob, ReadRows, ReadTarget};
use super::row::Row;


#[derive(Debug, Clone)]
pub struct EntityChange {
    pub revision: i64,
    pub rows: Vec<Row>,
    pub removed: Vec<RowId>,
    pub failure: Option<String>,
}

struct Known {
    position: Position,
    stamp: Option<Stamp>,
    told: bool,
}

pub(crate) struct EntityGroup {
    model: ModelKey,
    known: HashMap<RowId, Known>,
    pub(crate) dirty: BTreeSet<RowId>,
    pending: BTreeSet<RowId>,
    reading: bool,
    counted: bool,
    subscriber: Arc<EntitySubscriber>,
}

impl EntityGroup {
    pub(crate) fn resets(&self, event: &Event) -> bool {
        event.reset_all || event.reset_models.contains(&self.model)
    }

    pub(crate) fn touched(&self, batch: &[Event]) -> BTreeSet<RowId> {
        let mut ids = self.dirty.clone();
        for event in batch {
            if self.resets(event) {
                ids.extend(self.known.keys().cloned());
                continue;
            }
            ids.extend(event.rows.iter().filter(|row| row.model == self.model && self.known.contains_key(&row.id)).map(|row| row.id.clone()));
        }
        ids
    }

    pub(crate) fn idle(&self) -> bool {
        !self.reading && self.pending.is_empty() && !locked(&self.subscriber.pending).scheduled
    }

    pub(crate) fn holds_ids(&self) -> bool {
        !self.known.is_empty()
    }

    pub(crate) fn unused(&self) -> bool {
        self.known.is_empty() && !self.reading
    }

    pub(crate) fn model(&self) -> &ModelKey {
        &self.model
    }

    pub(crate) fn new(model: ModelKey, subscriber: Arc<EntitySubscriber>) -> Self {
        Self { model, known: HashMap::new(), dirty: BTreeSet::new(), pending: BTreeSet::new(), reading: false, counted: false, subscriber }
    }

    pub(crate) fn ids(&self) -> impl Iterator<Item = &RowId> {
        self.known.keys()
    }

    fn apply_read(&mut self, position: Position, loaded: &HashMap<String, Row>, ids: impl Iterator<Item = RowId>) -> Vec<(RowId, Position, Option<Row>)> {
        let mut changes = Vec::new();
        for id in ids {
            let Some(known) = self.known.get_mut(&id) else { continue };
            if position <= known.position {
                locked(&self.subscriber.counters).entity_position_skips += 1;
                continue;
            }
            known.position = position;
            let row = loaded.get(id.as_str()).cloned();
            let stamp = row.as_ref().map(|row| Stamp::of(row, Vec::new()));
            if known.told && stamp == known.stamp {
                continue;
            }
            known.told = true;
            known.stamp = stamp;
            changes.push((id, position, row));
        }
        changes
    }
}

#[derive(Default)]
struct Pending {
    rows: BTreeMap<RowId, Row>,
    removed: BTreeSet<RowId>,
    revision: i64,
    failure: Option<String>,
    scheduled: bool,
}

pub(crate) struct EntitySubscriber {
    pub(crate) call_state: super::listeners::CallState,
    pending: Mutex<Pending>,
    listener: Box<dyn Fn(&EntityChange) + Send + Sync>,
    counters: Arc<Mutex<Counters>>,
}

impl EntitySubscriber {
    pub(crate) fn post(self: &Arc<Self>, executor: &Arc<Executor>, changes: Vec<(RowId, Position, Option<Row>)>) {
        if changes.is_empty() {
            return;
        }
        let needs_run = {
            let mut pending = locked(&self.pending);
            for (id, position, row) in changes {
                pending.revision = pending.revision.max(position.0);
                match row {
                    Some(row) => {
                        pending.removed.remove(&id);
                        pending.rows.insert(id, row);
                    }
                    None => {
                        pending.rows.remove(&id);
                        pending.removed.insert(id);
                    }
                }
            }
            !std::mem::replace(&mut pending.scheduled, true)
        };
        if needs_run {
            executor.schedule(self.clone());
        }
    }
}

impl EntitySubscriber {
    pub(crate) fn post_failure(self: &Arc<Self>, executor: &Arc<Executor>, error: &DbError) {
        let needs_run = {
            let mut pending = locked(&self.pending);
            pending.failure = Some(error.to_string());
            !std::mem::replace(&mut pending.scheduled, true)
        };
        if needs_run {
            executor.schedule(self.clone());
        }
    }

    fn discard(&self, ids: &[RowId]) {
        let mut pending = locked(&self.pending);
        for id in ids {
            pending.rows.remove(id);
            pending.removed.remove(id);
        }
    }
}

impl Runnable for EntitySubscriber {
    fn run(&self) {
        loop {
            let change = {
                let mut pending = locked(&self.pending);
                if pending.rows.is_empty() && pending.removed.is_empty() && pending.failure.is_none() {
                    pending.scheduled = false;
                    return;
                }
                let rows = std::mem::take(&mut pending.rows).into_values().collect();
                let removed = std::mem::take(&mut pending.removed).into_iter().collect();
                EntityChange { revision: std::mem::take(&mut pending.revision), rows, removed, failure: pending.failure.take() }
            };
            if self.call_state.is_cancelled() {
                continue;
            }
            let mut returned = false;
            if self.call_state.call(|| {
                (self.listener)(&change);
                returned = true;
            }) && returned
            {
                let mut counters = locked(&self.counters);
                counters.observer_calls += 1;
                counters.entity_facts_told += change.rows.len() + change.removed.len();
            }
        }
    }
}

pub struct EntityObserver {
    shared: Weak<Shared>,
    key: String,
    model: ModelKey,
    pub(super) subscriber: Arc<EntitySubscriber>,
}

impl EntityObserver {
    pub fn retain(&self, ids: &[RowId]) -> Result<(), DbError> {
        let Some(shared) = self.shared.upgrade() else { return Ok(()) };
        let mut observers = shared.observers();
        let group = observers.group_entities(&self.key, &self.model, &self.subscriber);
        let model = self.model.clone();
        let fresh: BTreeSet<RowId> = ids.iter().filter(|id| !group.known.contains_key(*id) || group.dirty.contains(*id)).cloned().collect();
        if fresh.is_empty() {
            observers.drop_unused_entities(&self.key);
            return Ok(());
        }
        for id in &fresh {
            group.known.entry(id.clone()).or_insert(Known { position: (i64::MIN, 0), stamp: None, told: false });
        }
        group.pending.extend(fresh.iter().cloned());
        observers.index_rows(&self.key, &model, fresh.iter());
        shared.pump_entities(&mut observers, &self.key);
        Ok(())
    }

    pub fn release(&self, ids: &[RowId]) {
        let Some(shared) = self.shared.upgrade() else { return };
        let mut observers = shared.observers();
        let Some(group) = observers.entities.get_mut(&self.key) else { return };
        let model = group.model.clone();
        let gone: Vec<RowId> = ids.iter().filter(|id| group.known.remove(*id).is_some()).cloned().collect();
        for id in &gone {
            group.dirty.remove(id);
            group.pending.remove(id);
        }
        let subscriber = group.subscriber.clone();
        observers.unindex_rows(&self.key, &model, gone.iter());
        observers.drop_unused_entities(&self.key);
        subscriber.discard(&gone);
    }
}

impl Drop for EntityObserver {
    fn drop(&mut self) {
        self.subscriber.call_state.cancel();
        let Some(shared) = self.shared.upgrade() else { return };
        shared.observers().remove_entities(&self.key);
    }
}

impl Shared {
    fn read_entities<'a>(&self, model: &ModelKey, ids: impl Iterator<Item = &'a RowId>) -> Result<(Position, HashMap<String, Row>), DbError> {
        #[cfg(test)]
        if let Some(error) = self.store.injected_entity_read_failure() {
            return Err(error.into());
        }
        let wanted: Vec<String> = ids.map(RowId::to_string).collect();
        let epoch = self.ephemeral.lock().epoch;
        self.read(&[model.as_str()], |reader| Ok(((reader.revision_seq()?, epoch), self.load(reader, model.as_str(), &wanted)?)))
    }

    pub(crate) fn refresh_entities(&self, key: &str, model: &ModelKey, ids: BTreeSet<RowId>) {
        let mut observers = self.observers();
        let Some(group) = observers.entities.get_mut(key).filter(|group| group.model == *model) else { return };
        group.pending.extend(ids.into_iter().filter(|id| group.known.contains_key(id)));
        group.counted = true;
        self.pump_entities(&mut observers, key);
    }

    pub(crate) fn pump_entities(&self, observers: &mut Observers, key: &str) {
        let Some(group) = observers.entities.get_mut(key) else { return };
        if group.reading || group.pending.is_empty() {
            return;
        }
        let ids = std::mem::take(&mut group.pending);
        let counted = std::mem::take(&mut group.counted);
        let target = ReadTarget::Entities { key: key.to_string(), ids: ids.clone() };
        let model = group.model.clone();
        let wanted = ids.clone();
        let run = move |shared: &Shared| {
            if counted {
                locked(&shared.counters).observation_reads += 1;
            }
            let (position, loaded) = shared.read_entities(&model, wanted.iter())?;
            Ok(ReadRows::Entities { position, loaded })
        };
        if self.reads.submit(self, ReadJob { target, run: Box::new(run) }) {
            group.reading = true;
            self.dispatch.progress.entity_read_started();
        } else {
            group.pending = ids;
            group.counted = counted;
            observers.defer_read(key);
        }
    }

    pub(crate) fn apply_entities(&self, key: &str, ids: &BTreeSet<RowId>, read: Result<(Position, HashMap<String, Row>), DbError>) {
        let _finished = ReadFinished(&self.dispatch.progress);
        if read.is_err() {
            locked(&self.counters).entity_reads_failed += 1;
        }
        let mut observers = self.observers();
        let Some(group) = observers.entities.get_mut(key) else { return };
        group.reading = false;
        let (position, loaded) = match read {
            Ok(loaded) => loaded,
            Err(error) => {
                group.dirty.extend(ids.iter().filter(|id| group.known.contains_key(*id)).cloned());
                group.subscriber.clone().post_failure(&self.executor, &error);
                self.pump_entities(&mut observers, key);
                observers.drop_unused_entities(key);
                return;
            }
        };
        group.dirty.retain(|id| !ids.contains(id));
        let changes = group.apply_read(position, &loaded, ids.iter().cloned());
        group.subscriber.clone().post(&self.executor, changes);
        self.pump_entities(&mut observers, key);
        observers.drop_unused_entities(key);
    }
}

struct ReadFinished<'a>(&'a super::dispatch::Progress);

impl Drop for ReadFinished<'_> {
    fn drop(&mut self) {
        self.0.entity_read_finished();
    }
}

impl Db {
    pub fn observe_entities(&self, model: &str, listener: impl Fn(&EntityChange) + Send + Sync + 'static) -> Result<EntityObserver, DbError> {
        self.shared.spec(model)?;
        let subscriber = Arc::new(EntitySubscriber { call_state: super::listeners::CallState::new(), pending: Mutex::new(Pending::default()), listener: Box::new(listener), counters: self.shared.counters.clone() });
        let key = self.shared.observers().entities_key();
        Ok(EntityObserver { shared: Arc::downgrade(&self.shared), key, model: ModelKey::new(model), subscriber })
    }
}
