use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Weak};

use crate::identity::{RowId, RowRef};
use crate::identity::ModelKey;
use crate::query::{Query, Where};
use serde_json::Value;

use super::dispatch::Event;
use super::entities::{EntityGroup, EntitySubscriber};
use super::error::DbError;
use super::listeners::{Listener, Subscriber};
use super::open::{locked, Db, Shared};
use super::watch::{Body, Change, CountChange, Deps, Evaluated, Loaded, Step, Subject, Version};

const MAX_PROBED_ROWS: usize = 8;

struct Group {
    subject: Subject,
    deps: Deps,
    at: i64,
    epoch: u64,
    version: Arc<Version>,
    subscribers: Vec<(u64, Arc<Subscriber>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObserverCensus {
    pub observed_queries: usize,
    pub subscriptions: usize,
}

#[derive(Default)]
pub(crate) struct Observers {
    next_id: u64,
    groups: HashMap<String, Group>,
    by_row: HashMap<RowRef, BTreeSet<String>>,
    pub(crate) entities: HashMap<String, EntityGroup>,
    pub(crate) warm_ids: HashSet<u64>,
}

impl Observers {
    #[cfg(test)]
    pub(crate) fn keyed_rows(&self) -> usize {
        self.by_row.len()
    }

    pub(crate) fn add_entities(&mut self, model: ModelKey, subscriber: Arc<EntitySubscriber>) -> String {
        let key = format!("entities:{}", self.next_id);
        self.next_id += 1;
        self.entities.insert(key.clone(), EntityGroup::new(model, subscriber));
        key
    }

    pub(crate) fn index_rows<'a>(&mut self, key: &str, model: &ModelKey, ids: impl Iterator<Item = &'a RowId>) {
        for id in ids {
            self.by_row.entry(RowRef::new(model.clone(), id.clone())).or_default().insert(key.to_string());
        }
    }

    pub(crate) fn unindex_rows<'a>(&mut self, key: &str, model: &ModelKey, ids: impl Iterator<Item = &'a RowId>) {
        for id in ids {
            let row = RowRef::new(model.clone(), id.clone());
            if let Some(keys) = self.by_row.get_mut(&row) {
                keys.remove(key);
                if keys.is_empty() {
                    self.by_row.remove(&row);
                }
            }
        }
    }

    pub(crate) fn remove_entities(&mut self, key: &str) {
        let Some(group) = self.entities.remove(key) else { return };
        let ids: Vec<RowId> = group.ids().cloned().collect();
        self.unindex_rows(key, group.model(), ids.iter());
    }

    fn add_group(&mut self, key: &str, group: Group) {
        if group.deps.keyed() {
            for row in &group.deps.rows {
                self.by_row.entry(row.clone()).or_default().insert(key.to_string());
            }
        }
        self.groups.insert(key.to_string(), group);
    }

    fn remove_group(&mut self, key: &str) {
        let Some(group) = self.groups.remove(key) else { return };
        if !group.deps.keyed() {
            return;
        }
        for row in &group.deps.rows {
            if let Some(keys) = self.by_row.get_mut(row) {
                keys.remove(key);
                if keys.is_empty() {
                    self.by_row.remove(row);
                }
            }
        }
    }

    fn touched_ids(&self, key: &str, batch: &[Event]) -> Option<HashSet<String>> {
        let group = self.groups.get(key)?;
        let Subject::Rows(query) = &group.subject else { return None };
        if !query.include.is_empty() {
            return None;
        }
        let mut ids = HashSet::new();
        for event in batch.iter().filter(|event| event.revision > group.at || event.epoch > group.epoch) {
            if event.reset_all || !group.deps.reset_models.is_disjoint(&event.reset_models) {
                return None;
            }
            ids.extend(event.rows.iter().filter(|row| row.model == query.model).map(|row| row.id.to_string()));
        }
        Some(ids)
    }

    fn candidates(&self, batch: &[Event]) -> BTreeSet<String> {
        let mut keys: BTreeSet<String> = BTreeSet::new();
        for event in batch {
            for row in &event.rows {
                if let Some(found) = self.by_row.get(row) {
                    keys.extend(found.iter().cloned());
                }
            }
            if event.reset_all || !event.reset_models.is_empty() {
                keys.extend(self.groups.iter().filter(|(_, group)| group.deps.keyed() && group.deps.hit(event)).map(|(key, _)| key.clone()));
                keys.extend(self.entities.iter().filter(|(_, group)| group.resets(event)).map(|(key, _)| key.clone()));
            }
        }
        for (key, group) in &self.groups {
            if !group.deps.keyed() {
                keys.insert(key.clone());
            }
        }
        keys
    }

    pub(crate) fn query_count(&self) -> usize {
        self.groups.values().filter(|group| group.subscribers.iter().any(|(id, _)| !self.warm_ids.contains(id))).count()
    }

    pub(crate) fn subscription_count(&self) -> usize {
        self.groups.values().flat_map(|group| &group.subscribers).filter(|(id, _)| !self.warm_ids.contains(id)).count() + self.entities.len()
    }
}

pub struct ObserverHandle {
    shared: Weak<Shared>,
    group: String,
    id: u64,
    subscriber: Arc<Subscriber>,
}

impl ObserverHandle {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
}

impl Drop for ObserverHandle {
    fn drop(&mut self) {
        self.subscriber.cancel();
        let Some(shared) = self.shared.upgrade() else { return };
        let mut observers = locked(&shared.observers);
        observers.warm_ids.remove(&self.id);
        let Some(group) = observers.groups.get_mut(&self.group) else { return };
        group.subscribers.retain(|(id, _)| *id != self.id);
        if group.subscribers.is_empty() {
            observers.remove_group(&self.group);
        }
    }
}

struct Due {
    key: String,
    subject: Subject,
    version: Arc<Version>,
    touched: Option<HashSet<String>>,
    reached: Option<(i64, u64)>,
}

impl Shared {
    pub(crate) fn refresh_groups(&self, batch: &[Event]) {
        let (due, touched): (Vec<Due>, Vec<(String, ModelKey, BTreeSet<RowId>)>) = {
            let observers = locked(&self.observers);
            let keys = observers.candidates(batch);
            let touched = observers
                .entities
                .iter()
                .filter(|(key, group)| keys.contains(*key) || !group.dirty.is_empty())
                .map(|(key, group)| (key.clone(), group.model().clone(), group.touched(batch)))
                .filter(|(_, _, ids)| !ids.is_empty())
                .collect();
            let due = keys
                .into_iter()
                .filter_map(|key| observers.groups.get(&key).map(|group| (key, group)))
                .filter(|(_, group)| batch.iter().any(|event| (event.revision > group.at || event.epoch > group.epoch) && group.deps.hit(event)))
                .map(|(key, group)| Due {
                    touched: observers.touched_ids(&key, batch),
                    reached: (!batch.iter().any(|event| event.epoch > group.epoch)).then(|| (batch.iter().map(|event| event.revision).max().unwrap_or(0), group.epoch)),
                    subject: group.subject.clone(),
                    version: group.version.clone(),
                    key,
                })
                .collect();
            (due, touched)
        };
        for (key, model, ids) in touched {
            self.refresh_entities(&key, &model, ids);
        }
        for due in due {
            self.publish_group(&due.key, &due.subject, &due.version, due.touched.as_ref().map(|touched| (touched, due.reached)));
        }
    }

    fn cannot_enter(&self, query: &Query, previous: &Version, touched: &HashSet<String>) -> Result<bool, DbError> {
        let Body::Rows(body) = &previous.body else { return Ok(false) };
        let plain = query.include.is_empty() && query.scope.is_none() && query.limit.is_none() && query.offset.is_none();
        if !plain || touched.is_empty() || touched.len() > MAX_PROBED_ROWS || body.entries.iter().any(|entry| touched.contains(entry.id.as_str())) {
            return Ok(false);
        }
        let ids = touched.iter().map(|id| Value::from(id.as_str())).collect();
        let probe = Query { order: Vec::new(), r#where: Some(Where::AllOf { clauses: query.r#where.iter().cloned().chain([Where::in_list("id", ids)]).collect() }), ..query.clone() };
        self.read(&[probe.model.as_str()], |reader| Ok(self.query_keys(reader, &probe)?.is_empty()))
    }

    fn publish_group(&self, key: &str, subject: &Subject, previous: &Arc<Version>, step: Option<(&HashSet<String>, Option<(i64, u64)>)>) {
        if let (Subject::Rows(query), Some((touched, Some((revision, epoch))))) = (subject, step) {
            if self.cannot_enter(query, previous, touched).unwrap_or(false) {
                let mut observers = locked(&self.observers);
                if let Some(group) = observers.groups.get_mut(key).filter(|group| Arc::ptr_eq(&group.version, previous)) {
                    group.at = group.at.max(revision);
                    group.epoch = group.epoch.max(epoch);
                }
                return;
            }
        }
        let touched = step.map(|(touched, _)| touched);
        locked(&self.counters).observation_reads += 1;
        let evaluated = match (subject, touched) {
            (Subject::Rows(query), Some(touched)) if query.include.is_empty() => self.evaluate_step(query, previous, touched),
            _ => self.evaluate(subject),
        };
        let Ok(Evaluated { version: next, loaded }) = evaluated else { return };
        let (revision, epoch) = next.position();
        let replaced = self.dispatch.replaced_between(previous.position().0, revision);
        let replaced = replaced.as_slice();
        let published = next.succeeding(previous, replaced, loaded).map(Arc::new);
        let mut observers = locked(&self.observers);
        let Some(group) = observers.groups.get_mut(key) else { return };
        if (group.at >= revision && group.epoch >= epoch) || !Arc::ptr_eq(&group.version, previous) {
            return;
        }
        group.at = revision;
        group.epoch = epoch;
        let Some(version) = published else { return };
        group.version = version.clone();
        let supply = match &version.body {
            Body::Rows(body) => body.step.as_ref().map(Step::supply).unwrap_or_default(),
            Body::Count(_) => Loaded::default(),
        };
        for (_, subscriber) in &group.subscribers {
            subscriber.post(&self.executor, version.clone(), replaced, supply.clone());
        }
    }
}

impl Db {
    fn subscribe(&self, subject: Subject, listener: Listener) -> Result<ObserverHandle, DbError> {
        let key = subject.key();
        let deps = self.shared.deps(&subject)?;
        let subscriber = Subscriber::new(listener, self.shared.counters.clone());
        let current = locked(&self.shared.observers).groups.get(&key).map(|group| (group.at, group.version.clone()));
        if let Some((_, previous)) = current.filter(|(at, _)| self.shared.store.revision_seq().is_ok_and(|latest| latest > *at)) {
            self.shared.publish_group(&key, &subject, &previous, None);
        }
        let mut observers = locked(&self.shared.observers);
        let id = observers.next_id;
        observers.next_id += 1;
        let joined = observers.groups.get(&key).map(|group| (group.version.clone(), group.subject.clone()));
        let (version, loaded) = match joined {
            Some((version, group_subject)) => {
                let loaded = self.shared.load_whole(&group_subject, &version)?;
                if let Some(group) = observers.groups.get_mut(&key) {
                    group.subscribers.push((id, subscriber.clone()));
                }
                (version, loaded)
            }
            None => {
                locked(&self.shared.counters).observation_reads += 1;
                let Evaluated { version, loaded } = self.shared.evaluate(&subject)?;
                let version = Arc::new(version);
                let group = Group { subject, deps, at: version.revision, epoch: version.epoch, version: version.clone(), subscribers: vec![(id, subscriber.clone())] };
                observers.add_group(&key, group);
                (version, loaded)
            }
        };
        subscriber.post(&self.shared.executor, version, &[], loaded);
        drop(observers);
        Ok(ObserverHandle { shared: Arc::downgrade(&self.shared), group: key, id, subscriber })
    }

    pub fn observe(&self, query: Query, listener: impl Fn(&Change) + Send + Sync + 'static) -> Result<ObserverHandle, DbError> {
        self.subscribe(Subject::Rows(query), Listener::Rows(Box::new(listener)))
    }

    pub fn observe_row(&self, model: &str, id: &RowId, listener: impl Fn(&Change) + Send + Sync + 'static) -> Result<ObserverHandle, DbError> {
        self.subscribe(Subject::Row(RowRef::new(ModelKey::new(model), id.clone())), Listener::Rows(Box::new(listener)))
    }

    pub fn observe_count(&self, query: Query, listener: impl Fn(&CountChange) + Send + Sync + 'static) -> Result<ObserverHandle, DbError> {
        self.subscribe(Subject::Count(query), Listener::Count(Box::new(listener)))
    }
}
