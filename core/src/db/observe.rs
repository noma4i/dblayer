use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::identity::{RowId, RowRef};
use crate::identity::ModelKey;
use crate::query::Query;

use super::dispatch::Event;
use super::error::DbError;
use super::listeners::{Listener, Subscriber};
use super::open::{locked, Db, Shared};
use super::watch::{Change, CountChange, Deps, Subject, Version};

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
}

impl Observers {
    pub(crate) fn query_count(&self) -> usize {
        self.groups.len()
    }

    pub(crate) fn subscription_count(&self) -> usize {
        self.groups.values().map(|group| group.subscribers.len()).sum()
    }
}

pub struct ObserverHandle {
    shared: Weak<Shared>,
    group: String,
    id: u64,
    subscriber: Arc<Subscriber>,
}

impl Drop for ObserverHandle {
    fn drop(&mut self) {
        self.subscriber.cancel();
        let Some(shared) = self.shared.upgrade() else { return };
        let mut observers = locked(&shared.observers);
        let Some(group) = observers.groups.get_mut(&self.group) else { return };
        group.subscribers.retain(|(id, _)| *id != self.id);
        if group.subscribers.is_empty() {
            observers.groups.remove(&self.group);
        }
    }
}

impl Shared {
    pub(crate) fn refresh_groups(&self, batch: &[Event]) {
        let due: Vec<(String, Subject, i64, Arc<Version>)> = {
            let observers = locked(&self.observers);
            observers
                .groups
                .iter()
                .filter(|(_, group)| batch.iter().any(|event| (event.revision > group.at || event.epoch > group.epoch) && group.deps.hit(event)))
                .map(|(key, group)| (key.clone(), group.subject.clone(), group.at, group.version.clone()))
                .collect()
        };
        for (key, subject, at, previous) in due {
            locked(&self.counters).observation_reads += 1;
            let Ok(next) = self.evaluate(&subject) else { continue };
            let (revision, epoch) = next.position();
            let replaced: Vec<(RowId, RowId)> = batch.iter().filter(|event| event.revision > at).flat_map(|event| event.replaced.iter().cloned()).collect();
            let published = next.succeeding(&previous, &replaced).map(Arc::new);
            let mut observers = locked(&self.observers);
            let Some(group) = observers.groups.get_mut(&key) else { continue };
            if (group.at >= revision && group.epoch >= epoch) || !Arc::ptr_eq(&group.version, &previous) {
                continue;
            }
            group.at = revision;
            group.epoch = epoch;
            let Some(version) = published else { continue };
            group.version = version.clone();
            for (_, subscriber) in &group.subscribers {
                subscriber.post(&self.executor, version.clone(), &replaced);
            }
        }
    }
}

impl Db {
    fn subscribe(&self, subject: Subject, listener: Listener) -> Result<ObserverHandle, DbError> {
        let key = subject.key();
        let deps = self.shared.deps(&subject)?;
        let subscriber = Subscriber::new(listener, self.shared.counters.clone());
        let mut observers = locked(&self.shared.observers);
        let id = observers.next_id;
        observers.next_id += 1;
        let version = match observers.groups.get_mut(&key) {
            Some(group) => {
                group.subscribers.push((id, subscriber.clone()));
                group.version.clone()
            }
            None => {
                locked(&self.shared.counters).observation_reads += 1;
                let version = Arc::new(self.shared.evaluate(&subject)?);
                let group = Group { subject, deps, at: version.revision, epoch: version.epoch, version: version.clone(), subscribers: vec![(id, subscriber.clone())] };
                observers.groups.insert(key.clone(), group);
                version
            }
        };
        subscriber.post(&self.shared.executor, version, &[]);
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
