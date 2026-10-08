use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Weak};

use crate::identity::ModelKey;
use crate::query::Query;

use super::error::DbError;
use super::lookup::RowKey;
use super::open::{locked, Db, Shared};
use super::row::Row;

type Callback = Arc<dyn Fn(&[Row]) + Send + Sync>;

struct Group {
    query: Query,
    keys: Vec<RowKey>,
    rows: Vec<Row>,
    subscribers: Vec<(u64, Callback)>,
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

pub struct Subscription {
    shared: Weak<Shared>,
    group: String,
    id: u64,
}

impl Drop for Subscription {
    fn drop(&mut self) {
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
    fn refresh(&self, query: &Query, previous: Option<&[RowKey]>) -> Result<Option<(Vec<RowKey>, Vec<Row>)>, DbError> {
        self.read(|reader| {
            let keys = self.query_keys(reader, query)?;
            if previous == Some(keys.as_slice()) {
                return Ok(None);
            }
            let rows = self.rows_for_keys(reader, query.model.as_str(), &keys)?;
            Ok(Some((keys, rows)))
        })
    }

    pub(crate) fn notify(&self, changed: &BTreeSet<ModelKey>) {
        if changed.is_empty() {
            return;
        }
        let mut deliveries: Vec<(Vec<Callback>, Vec<Row>)> = Vec::new();
        {
            let mut observers = locked(&self.observers);
            for group in observers.groups.values_mut().filter(|group| changed.contains(&group.query.model)) {
                self.counters.count_read();
                let Ok(Some((keys, rows))) = self.refresh(&group.query, Some(&group.keys)) else { continue };
                group.keys = keys;
                group.rows = rows.clone();
                deliveries.push((group.subscribers.iter().map(|(_, callback)| callback.clone()).collect(), rows));
            }
        }
        for (callbacks, rows) in deliveries {
            for callback in callbacks {
                callback(&rows);
                self.counters.count_call();
            }
        }
    }
}

impl Db {
    pub fn observe(&self, query: Query, callback: impl Fn(&[Row]) + Send + Sync + 'static) -> Result<Subscription, DbError> {
        self.shared.spec(query.model.as_str())?;
        let group_key = serde_json::to_string(&query).expect("query serializes");
        let callback: Callback = Arc::new(callback);
        let (id, rows) = {
            let mut observers = locked(&self.shared.observers);
            let id = observers.next_id;
            let rows = match observers.groups.get_mut(&group_key) {
                Some(group) => {
                    group.subscribers.push((id, callback.clone()));
                    group.rows.clone()
                }
                None => {
                    self.shared.counters.count_read();
                    let (keys, rows) = self.shared.refresh(&query, None)?.expect("first read has no previous keys");
                    let group = Group { query, keys, rows: rows.clone(), subscribers: vec![(id, callback.clone())] };
                    observers.groups.insert(group_key.clone(), group);
                    rows
                }
            };
            observers.next_id += 1;
            (id, rows)
        };
        callback(&rows);
        self.shared.counters.count_call();
        Ok(Subscription { shared: Arc::downgrade(&self.shared), group: group_key, id })
    }
}
