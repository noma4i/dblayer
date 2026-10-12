use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::identity::{ModelKey, RowId};
use crate::pacing::Counters;

use super::diff::Stamp;
use super::dispatch::Event;
use super::error::DbError;
use super::listeners::{Executor, Runnable};
use super::open::{locked, Db, Shared};
use super::row::Row;

type Position = (i64, u64);

#[derive(Debug, Clone)]
pub struct EntityChange {
    pub revision: i64,
    pub rows: Vec<Row>,
    pub removed: Vec<RowId>,
}

struct Known {
    position: Position,
    stamp: Option<Stamp>,
    told: bool,
}

pub(crate) struct EntityGroup {
    model: ModelKey,
    known: HashMap<RowId, Known>,
    subscriber: Arc<EntitySubscriber>,
}

impl EntityGroup {
    pub(crate) fn resets(&self, event: &Event) -> bool {
        event.reset_all || event.reset_models.contains(&self.model)
    }

    pub(crate) fn touched(&self, batch: &[Event]) -> BTreeSet<RowId> {
        let mut ids = BTreeSet::new();
        for event in batch {
            if self.resets(event) {
                ids.extend(self.known.keys().cloned());
                continue;
            }
            ids.extend(event.rows.iter().filter(|row| row.model == self.model && self.known.contains_key(&row.id)).map(|row| row.id.clone()));
        }
        ids
    }

    pub(crate) fn model(&self) -> &ModelKey {
        &self.model
    }

    pub(crate) fn new(model: ModelKey, subscriber: Arc<EntitySubscriber>) -> Self {
        Self { model, known: HashMap::new(), subscriber }
    }

    pub(crate) fn ids(&self) -> impl Iterator<Item = &RowId> {
        self.known.keys()
    }

    fn settle(&mut self, position: Position, loaded: &HashMap<String, Row>, ids: impl Iterator<Item = RowId>) -> Vec<(RowId, Position, Option<Row>)> {
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
    scheduled: bool,
}

pub(crate) struct EntitySubscriber {
    cancelled: AtomicBool,
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
                if pending.rows.is_empty() && pending.removed.is_empty() {
                    pending.scheduled = false;
                    return;
                }
                let rows = std::mem::take(&mut pending.rows).into_values().collect();
                let removed = std::mem::take(&mut pending.removed).into_iter().collect();
                EntityChange { revision: std::mem::take(&mut pending.revision), rows, removed }
            };
            if self.cancelled.load(Ordering::SeqCst) {
                continue;
            }
            let delivered = catch_unwind(AssertUnwindSafe(|| (self.listener)(&change)));
            if delivered.is_ok() {
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
    subscriber: Arc<EntitySubscriber>,
}

impl EntityObserver {
    pub fn retain(&self, ids: &[RowId]) -> Result<(), DbError> {
        let Some(shared) = self.shared.upgrade() else { return Ok(()) };
        let mut observers = locked(&shared.observers);
        let Some(group) = observers.entities.get(&self.key) else { return Ok(()) };
        let model = group.model.clone();
        let fresh: BTreeSet<RowId> = ids.iter().filter(|id| !group.known.contains_key(*id)).cloned().collect();
        if fresh.is_empty() {
            return Ok(());
        }
        let (position, loaded) = shared.read_entities(&model, fresh.iter())?;
        let group = observers.entities.get_mut(&self.key).expect("group checked above");
        for id in &fresh {
            group.known.insert(id.clone(), Known { position: (i64::MIN, 0), stamp: None, told: false });
        }
        let changes = group.settle(position, &loaded, fresh.iter().cloned());
        let subscriber = group.subscriber.clone();
        observers.index_rows(&self.key, &model, fresh.iter());
        subscriber.post(&shared.executor, changes);
        Ok(())
    }

    pub fn release(&self, ids: &[RowId]) {
        let Some(shared) = self.shared.upgrade() else { return };
        let mut observers = locked(&shared.observers);
        let Some(group) = observers.entities.get_mut(&self.key) else { return };
        let model = group.model.clone();
        let gone: Vec<RowId> = ids.iter().filter(|id| group.known.remove(*id).is_some()).cloned().collect();
        let subscriber = group.subscriber.clone();
        observers.unindex_rows(&self.key, &model, gone.iter());
        subscriber.discard(&gone);
    }
}

impl Drop for EntityObserver {
    fn drop(&mut self) {
        self.subscriber.cancelled.store(true, Ordering::SeqCst);
        let Some(shared) = self.shared.upgrade() else { return };
        locked(&shared.observers).remove_entities(&self.key);
    }
}

impl Shared {
    fn read_entities<'a>(&self, model: &ModelKey, ids: impl Iterator<Item = &'a RowId>) -> Result<(Position, HashMap<String, Row>), DbError> {
        let wanted: Vec<String> = ids.map(RowId::to_string).collect();
        let epoch = self.ephemeral.lock().epoch;
        self.read(|reader| Ok(((reader.revision_seq()?, epoch), self.load(reader, model.as_str(), &wanted)?)))
    }

    pub(crate) fn refresh_entities(&self, key: &str, model: &ModelKey, ids: BTreeSet<RowId>) {
        locked(&self.counters).observation_reads += 1;
        let Ok((position, loaded)) = self.read_entities(model, ids.iter()) else {
            locked(&self.counters).entity_reads_failed += 1;
            return;
        };
        let mut observers = locked(&self.observers);
        let Some(group) = observers.entities.get_mut(key) else { return };
        let changes = group.settle(position, &loaded, ids.into_iter());
        group.subscriber.clone().post(&self.executor, changes);
    }
}

impl Db {
    pub fn observe_entities(&self, model: &str, listener: impl Fn(&EntityChange) + Send + Sync + 'static) -> Result<EntityObserver, DbError> {
        self.shared.spec(model)?;
        let subscriber = Arc::new(EntitySubscriber { cancelled: AtomicBool::new(false), pending: Mutex::new(Pending::default()), listener: Box::new(listener), counters: self.shared.counters.clone() });
        let key = locked(&self.shared.observers).add_entities(ModelKey::new(model), subscriber.clone());
        Ok(EntityObserver { shared: Arc::downgrade(&self.shared), key, subscriber })
    }
}
