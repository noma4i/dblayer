use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Weak};

use crate::identity::{RowId, RowRef};
use crate::identity::ModelKey;
use crate::query::{Query, Where};
use serde_json::Value;

use super::dispatch::Event;
use super::entities::{EntityGroup, EntitySubscriber};
use super::error::DbError;
use super::listeners::{Feed, Listener, Subscriber};
use super::open::{locked, Db, Shared};
use super::reads::{ReadJob, ReadResult, ReadRows, ReadTarget};
use super::window::WindowSeed;
use super::watch::{Body, Change, CountChange, Deps, Evaluated, Loaded, Step, Subject, Version};

const MAX_PROBED_ROWS: usize = 8;

type Subscribers = Vec<(u64, Arc<Subscriber>)>;

#[derive(Default)]
struct GroupFacts {
    reading: bool,
    again: bool,
    read_error: Option<DbError>,
    joining: Subscribers,
    grows: VecDeque<(usize, usize)>,
    seen: i64,
}

struct Group {
    subject: Subject,
    deps: Deps,
    at: i64,
    epoch: u64,
    token: u64,
    version: Arc<Version>,
    subscribers: Subscribers,
    facts: GroupFacts,
}

struct Opening {
    subject: Subject,
    deps: Deps,
    token: u64,
    waiting: Subscribers,
    reading: bool,
    missed: bool,
    read_error: Option<DbError>,
    grows: VecDeque<(usize, usize)>,
    seen: i64,
}

pub(crate) struct Stepping {
    touched: HashSet<String>,
    reached: Option<(i64, u64)>,
    horizon: (i64, u64),
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
    opening: HashMap<String, Opening>,
    by_row: HashMap<RowRef, BTreeSet<String>>,
    pub(crate) entities: HashMap<String, EntityGroup>,
    pub(crate) warm_ids: HashSet<u64>,
}

impl Observers {
    /// The revision that a group has read, or None while it still has work. Subscribers are told this revision, and `Db::await_revision` waits for it.
    fn mark(group: &Group) -> Option<i64> {
        let facts = &group.facts;
        let idle = !facts.reading && facts.read_error.is_none() && !facts.again && facts.grows.is_empty() && facts.joining.is_empty();
        idle.then(|| facts.seen.max(group.at))
    }

    fn reached_through(&self, revision: i64) -> bool {
        let groups = self.groups.values().all(|group| {
            let read = if group.facts.read_error.is_some() { !group.facts.reading } else { Self::mark(group).is_some_and(|reached| reached >= revision) };
            read && group.subscribers.iter().chain(&group.facts.joining).all(|(_, subscriber)| subscriber.idle())
        });
        let opening = self.opening.values().all(|opening| opening.read_error.is_some() && !opening.reading && opening.waiting.iter().all(|(_, subscriber)| subscriber.idle()));
        let entities = self.entities.values().all(EntityGroup::idle);
        groups && opening && entities
    }

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
        let (Subject::Rows(query) | Subject::Window(query, _, _)) = &group.subject else { return None };
        if !query.include.is_empty() {
            return None;
        }
        let mut ids = HashSet::new();
        for event in batch.iter().filter(|event| event.revision > group.at || event.epoch > group.epoch) {
            if event.reset_all || !group.deps.reset_models.is_disjoint(&event.reset_models) {
                return None;
            }
            ids.extend(event.rows.iter().chain(&event.members).filter(|row| row.model == query.model).map(|row| row.id.to_string()));
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

    fn next(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    pub(crate) fn query_count(&self) -> usize {
        let waiting = |subscribers: &Subscribers| subscribers.iter().any(|(id, _)| !self.warm_ids.contains(id));
        self.groups.values().filter(|group| waiting(&group.subscribers) || waiting(&group.facts.joining)).count() + self.opening.values().filter(|opening| waiting(&opening.waiting)).count()
    }

    pub(crate) fn subscription_count(&self) -> usize {
        let live = |subscribers: &Subscribers| subscribers.iter().filter(|(id, _)| !self.warm_ids.contains(id)).count();
        self.groups.values().map(|group| live(&group.subscribers) + live(&group.facts.joining)).sum::<usize>() + self.opening.values().map(|opening| live(&opening.waiting)).sum::<usize>() + self.entities.len()
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

impl ObserverHandle {
    pub(crate) fn grow(&self, before: usize, after: usize) {
        if let Some(shared) = self.shared.upgrade() {
            let mut observers = shared.observers();
            if let Some(group) = observers.groups.get_mut(&self.group) {
                group.facts.grows.push_back((before, after));
                shared.pump(&mut observers, &self.group);
            } else if let Some(opening) = observers.opening.get_mut(&self.group) {
                opening.grows.push_back((before, after));
            }
        }
    }
}

impl Drop for ObserverHandle {
    fn drop(&mut self) {
        self.subscriber.cancel();
        let Some(shared) = self.shared.upgrade() else { return };
        let mut observers = shared.observers();
        observers.warm_ids.remove(&self.id);
        if let Some(group) = observers.groups.get_mut(&self.group) {
            group.subscribers.retain(|(id, _)| *id != self.id);
            group.facts.joining.retain(|(id, _)| *id != self.id);
            if group.subscribers.is_empty() && group.facts.joining.is_empty() {
                observers.remove_group(&self.group);
            }
        } else if let Some(opening) = observers.opening.get_mut(&self.group) {
            opening.waiting.retain(|(id, _)| *id != self.id);
            if opening.waiting.is_empty() {
                observers.opening.remove(&self.group);
            }
        }
    }
}


struct Due {
    key: String,
    step: Option<Stepping>,
}

impl Shared {
    pub(crate) fn refresh_groups(&self, batch: &[Event]) {
        let (due, touched): (Vec<Due>, Vec<(String, ModelKey, BTreeSet<RowId>)>) = {
            let mut observers = self.observers();
            let top = batch.iter().map(|event| event.revision).max().unwrap_or(0);
            for opening in observers.opening.values_mut() {
                opening.missed |= batch.iter().any(|event| opening.deps.hit(event));
                opening.seen = opening.seen.max(top);
            }
            for group in observers.groups.values_mut() {
                group.facts.seen = group.facts.seen.max(top);
            }
            let mut keys = observers.candidates(batch);
            keys.extend(observers.groups.iter().filter(|(_, group)| group.facts.read_error.is_some()).map(|(key, _)| key.clone()));
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
                .filter(|(_, group)| group.facts.read_error.is_some() || batch.iter().any(|event| (event.revision > group.at || event.epoch > group.epoch) && group.deps.hit(event)))
                .map(|(key, group)| Due {
                    step: if group.facts.read_error.is_some() {
                        None
                    } else {
                        observers.touched_ids(&key, batch).map(|touched| Stepping {
                            touched,
                            reached: (!batch.iter().any(|event| event.epoch > group.epoch)).then(|| (batch.iter().map(|event| event.revision).max().unwrap_or(0), group.epoch)),
                            horizon: (batch.iter().map(|event| event.revision).max().unwrap_or(0).max(group.at), batch.iter().map(|event| event.epoch).max().unwrap_or(0).max(group.epoch)),
                        })
                    },
                    key,
                })
                .collect();
            (due, touched)
        };
        for (key, model, ids) in touched {
            self.contained(&key, || self.refresh_entities(&key, &model, ids));
        }
        for due in due {
            self.contained(&due.key.clone(), || {
                let mut observers = self.observers();
                self.schedule(&mut observers, due);
            });
        }
        let mut observers = self.observers();
        let idle: Vec<String> = observers.groups.keys().cloned().collect();
        for key in idle {
            self.pump(&mut observers, &key);
        }
        let retry: Vec<String> = observers.opening.iter().filter(|(_, opening)| !opening.reading && opening.read_error.is_some()).map(|(key, _)| key.clone()).collect();
        for key in retry {
            self.issue_open(&mut observers, &key);
        }
    }

    fn schedule(&self, observers: &mut Observers, due: Due) {
        let Some(group) = observers.groups.get_mut(&due.key) else { return };
        if group.facts.reading {
            group.facts.again = true;
            return;
        }
        self.issue_publish(observers, &due.key, due.step, None);
    }

    fn issue_publish(&self, observers: &mut Observers, key: &str, step: Option<Stepping>, grow: Option<(usize, usize)>) {
        let Some(group) = observers.groups.get_mut(key) else { return };
        group.facts.reading = true;
        let (subject, previous, token) = (group.subject.clone(), group.version.clone(), group.token);
        let target = ReadTarget::Publish { key: key.to_string(), token, previous: previous.clone(), grow };
        self.reads.submit(self, ReadJob { target, run: Box::new(move |shared| shared.read_publish(&subject, &previous, step, grow)) });
    }

    fn issue_join(&self, observers: &mut Observers, key: &str) {
        let Some(group) = observers.groups.get_mut(key) else { return };
        group.facts.reading = true;
        let (subject, version, token) = (group.subject.clone(), group.version.clone(), group.token);
        let joiners = group.facts.joining.iter().map(|(id, _)| *id).collect();
        let target = ReadTarget::Join { key: key.to_string(), token, joiners };
        self.reads.submit(self, ReadJob { target, run: Box::new(move |shared| shared.load_whole(&subject, &version).map(ReadRows::Whole)) });
    }

    fn issue_open(&self, observers: &mut Observers, key: &str) {
        let seed = match observers.opening.get(key) {
            Some(Opening { subject: Subject::Window(query, _, _), .. }) => observers.groups.values().find_map(|group| WindowSeed::of(query, &group.subject, &group.version)),
            _ => None,
        };
        let Some(opening) = observers.opening.get_mut(key) else { return };
        opening.reading = true;
        let subject = opening.subject.clone();
        let target = ReadTarget::Open { key: key.to_string(), token: opening.token };
        self.reads.submit(
            self,
            ReadJob {
                target,
                run: Box::new(move |shared| {
                    locked(&shared.counters).observation_reads += 1;
                    shared.evaluate_seeded(&subject, seed.as_ref()).map(ReadRows::Evaluated)
                }),
            },
        );
    }

    fn pump(&self, observers: &mut Observers, key: &str) {
        let Some(group) = observers.groups.get_mut(key) else {
            if observers.opening.get(key).is_some_and(|opening| !opening.reading && opening.read_error.is_none()) {
                self.issue_open(observers, key);
            }
            return;
        };
        if group.facts.reading || group.facts.read_error.is_some() {
            return;
        }
        if std::mem::take(&mut group.facts.again) {
            self.issue_publish(observers, key, None, None);
        } else if let Some(grow) = group.facts.grows.pop_front() {
            let step = Stepping { touched: HashSet::new(), reached: None, horizon: group.version.position() };
            self.issue_publish(observers, key, Some(step), Some(grow));
        } else if !group.facts.joining.is_empty() {
            self.issue_join(observers, key);
        } else if let Some(reach) = Observers::mark(group) {
            for (_, subscriber) in &group.subscribers {
                subscriber.reach(&self.executor, reach);
            }
        }
    }

    pub(crate) fn reached_through(&self, revision: i64) -> bool {
        let observers = self.observers();
        observers.reached_through(revision) && self.dispatch.progress.entity_reads() == 0
    }

    fn tell_failure(&self, observers: &Observers, key: &str, error: &DbError) {
        let waiting: Vec<&Arc<Subscriber>> = match (observers.groups.get(key), observers.opening.get(key)) {
            (Some(group), _) => group.subscribers.iter().chain(&group.facts.joining).map(|(_, subscriber)| subscriber).collect(),
            (None, Some(opening)) => opening.waiting.iter().map(|(_, subscriber)| subscriber).collect(),
            (None, None) => Vec::new(),
        };
        for subscriber in waiting {
            subscriber.fail(&self.executor, error.to_string());
        }
    }

    fn read_publish(&self, subject: &Subject, previous: &Arc<Version>, step: Option<Stepping>, grow: Option<(usize, usize)>) -> Result<ReadRows, DbError> {
        if let (Subject::Rows(query), Some(Stepping { touched, reached: Some((revision, epoch)), .. })) = (subject, &step) {
            if self.cannot_enter(query, previous, touched).unwrap_or(false) {
                return Ok(ReadRows::Skipped { revision: *revision, epoch: *epoch });
            }
        }
        locked(&self.counters).observation_reads += 1;
        let evaluated = match (subject, &step) {
            (Subject::Rows(query), Some(step)) if query.include.is_empty() => self.evaluate_step(query, previous, &step.touched, step.horizon),
            (Subject::Window(query, _, _), _) => {
                let horizon = step.as_ref().map_or_else(|| previous.position(), |step| step.horizon);
                self.window_next(query, previous, step.as_ref().map(|step| &step.touched), horizon, grow.unwrap_or((0, 0)))
            }
            _ => self.evaluate(subject),
        };
        evaluated.map(ReadRows::Evaluated)
    }

    fn cannot_enter(&self, query: &Query, previous: &Version, touched: &HashSet<String>) -> Result<bool, DbError> {
        let Body::Rows(body) = &previous.body else { return Ok(false) };
        let plain = query.include.is_empty() && query.scope.is_none() && query.limit.is_none() && query.offset.is_none();
        if !plain || touched.is_empty() || touched.len() > MAX_PROBED_ROWS || touched.iter().any(|id| body.entry(id).is_some()) {
            return Ok(false);
        }
        let ids = touched.iter().map(|id| Value::from(id.as_str())).collect();
        let probe = Query { order: Vec::new(), r#where: Some(Where::AllOf { clauses: query.r#where.iter().cloned().chain([Where::in_list("id", ids)]).collect() }), ..query.clone() };
        self.read(&[probe.model.as_str()], |reader| Ok(self.query_keys(reader, &probe)?.is_empty()))
    }

    pub(crate) fn land_read(&self, result: ReadResult) {
        let key = result.key().to_string();
        self.contained(&key, || self.apply(result));
    }

    fn contained(&self, key: &str, run: impl FnOnce()) {
        if catch_unwind(AssertUnwindSafe(run)).is_ok() {
            return;
        }
        locked(&self.counters).owner_panics += 1;
        let mut observers = self.observers();
        let error = DbError::Internal(format!("panic while handling {key}"));
        self.tell_failure(&observers, key, &error);
        if let Some(group) = observers.groups.get_mut(key) {
            group.facts.reading = false;
            group.facts.again = false;
            group.facts.read_error = Some(error);
        } else if let Some(opening) = observers.opening.get_mut(key) {
            opening.reading = false;
            opening.read_error = Some(error);
        }
    }

    fn apply(&self, result: ReadResult) {
        match result {
            ReadResult::ReadOk { target, rows } => match (target, rows) {
                (ReadTarget::Publish { key, token, previous, grow }, rows) => self.apply_publish(&key, token, &previous, grow, rows),
                (ReadTarget::Join { key, token, joiners }, ReadRows::Whole(loaded)) => self.apply_join(&key, token, &joiners, loaded),
                (ReadTarget::Open { key, token }, ReadRows::Evaluated(evaluated)) => self.apply_open(&key, token, evaluated),
                (ReadTarget::Entities { key, ids }, ReadRows::Entities { position, loaded }) => self.apply_entities(&key, &ids, Ok((position, loaded))),
                _ => {}
            },
            ReadResult::ReadFailed { target, error } => {
                match target {
                    ReadTarget::Entities { key, ids } => return self.apply_entities(&key, &ids, Err(error)),
                    ReadTarget::Publish { key, token, .. } | ReadTarget::Join { key, token, .. } => {
                        locked(&self.counters).query_reads_failed += 1;
                        let mut observers = self.observers();
                        if observers.groups.get(&key).is_some_and(|group| group.token == token) {
                            self.tell_failure(&observers, &key, &error);
                        }
                        if let Some(group) = observers.groups.get_mut(&key).filter(|group| group.token == token) {
                            group.facts.reading = false;
                            group.facts.read_error = Some(error);
                        }
                    }
                    ReadTarget::Open { key, token } => {
                        locked(&self.counters).query_reads_failed += 1;
                        let mut observers = self.observers();
                        if observers.opening.get(&key).is_some_and(|opening| opening.token == token) {
                            self.tell_failure(&observers, &key, &error);
                        }
                        if let Some(opening) = observers.opening.get_mut(&key).filter(|opening| opening.token == token) {
                            opening.reading = false;
                            opening.read_error = Some(error);
                        }
                    }
                }
            }
        }
    }

    fn apply_publish(&self, key: &str, token: u64, previous: &Arc<Version>, grow: Option<(usize, usize)>, rows: ReadRows) {
        #[cfg(test)]
        self.panic_at(key);
        let evaluated = match rows {
            ReadRows::Evaluated(Evaluated { version: next, loaded }) => {
                let replaced = self.dispatch.replaced_between(previous.position().0, next.revision);
                let position = next.position();
                let published = next.succeeding(previous, replaced.as_slice(), loaded).map(Arc::new);
                Some((position, published, replaced))
            }
            ReadRows::Skipped { revision, epoch } => {
                let mut observers = self.observers();
                if let Some(group) = observers.groups.get_mut(key).filter(|group| group.token == token) {
                    if Arc::ptr_eq(&group.version, previous) {
                        group.at = group.at.max(revision);
                        group.epoch = group.epoch.max(epoch);
                    }
                    group.facts.reading = false;
                    group.facts.read_error = None;
                    self.pump(&mut observers, key);
                }
                return;
            }
            _ => return,
        };
        let Some(((revision, epoch), published, replaced)) = evaluated else { return };
        let mut observers = self.observers();
        let Some(group) = observers.groups.get_mut(key).filter(|group| group.token == token) else { return };
        group.facts.reading = false;
        group.facts.read_error = None;
        let behind = group.at >= revision && group.epoch >= epoch && grow.is_none();
        if !behind && Arc::ptr_eq(&group.version, previous) {
            group.at = revision;
            group.epoch = epoch;
            if let Some(version) = published {
                group.version = version.clone();
                if version.announces(previous) {
                    let supply = match &version.body {
                        Body::Rows(body) => body.step.as_ref().map(Step::supply).unwrap_or_default(),
                        Body::Count(_) => Loaded::default(),
                    };
                    for (_, subscriber) in &group.subscribers {
                        subscriber.post(&self.executor, version.clone(), &replaced, supply.clone());
                    }
                }
            }
        }
        self.pump(&mut observers, key);
    }

    fn apply_join(&self, key: &str, token: u64, joiners: &[u64], loaded: Loaded) {
        let mut observers = self.observers();
        let Some(group) = observers.groups.get_mut(key).filter(|group| group.token == token) else { return };
        group.facts.reading = false;
        group.facts.read_error = None;
        let (joined, waiting): (Subscribers, Subscribers) = std::mem::take(&mut group.facts.joining).into_iter().partition(|(id, _)| joiners.contains(id));
        group.facts.joining = waiting;
        let version = group.version.clone();
        for (id, subscriber) in joined {
            subscriber.post(&self.executor, version.clone(), &[], loaded.clone());
            group.subscribers.push((id, subscriber));
        }
        self.pump(&mut observers, key);
    }

    fn apply_open(&self, key: &str, token: u64, evaluated: Evaluated) {
        let mut observers = self.observers();
        if !observers.opening.get(key).is_some_and(|opening| opening.token == token) {
            return;
        }
        let Some(opening) = observers.opening.remove(key) else { return };
        let Evaluated { version, loaded } = evaluated;
        let version = Arc::new(version);
        let facts = GroupFacts { again: opening.missed, grows: opening.grows, seen: opening.seen, ..GroupFacts::default() };
        let token = observers.next();
        let group = Group { subject: opening.subject, deps: opening.deps, at: version.revision, epoch: version.epoch, token, version: version.clone(), subscribers: opening.waiting.clone(), facts };
        observers.add_group(key, group);
        for (_, subscriber) in &opening.waiting {
            subscriber.post(&self.executor, version.clone(), &[], loaded.clone());
        }
        self.pump(&mut observers, key);
    }
}

impl Db {
    fn subscribe(&self, subject: Subject, listener: Listener) -> Result<ObserverHandle, DbError> {
        let key = subject.key();
        let deps = self.shared.deps(&subject)?;
        self.shared.validate(&subject)?;
        let subscriber = Subscriber::new(listener, self.shared.counters.clone());
        let known = self.shared.observers().groups.get(&key).map(|group| group.at);
        let behind = known.is_some_and(|at| locked(&self.shared.cache).committed_revision > at);
        let mut observers = self.shared.observers();
        let id = observers.next();
        if let Some(group) = observers.groups.get_mut(&key) {
            group.facts.joining.push((id, subscriber.clone()));
            group.facts.again |= behind;
        } else if let Some(opening) = observers.opening.get_mut(&key) {
            opening.waiting.push((id, subscriber.clone()));
        } else {
            let token = observers.next();
            observers.opening.insert(key.clone(), Opening { subject, deps, token, waiting: vec![(id, subscriber.clone())], reading: false, missed: false, read_error: None, grows: VecDeque::new(), seen: 0 });
        }
        self.shared.pump(&mut observers, &key);
        drop(observers);
        Ok(ObserverHandle { shared: Arc::downgrade(&self.shared), group: key, id, subscriber })
    }

    pub(crate) fn observe_subject(&self, subject: Subject, listener: impl Fn(&Change) + Send + Sync + 'static) -> Result<ObserverHandle, DbError> {
        self.subscribe(subject, Listener::Rows(Box::new(listener)))
    }

    pub fn observe(&self, query: Query, listener: impl Fn(&Change) + Send + Sync + 'static) -> Result<ObserverHandle, DbError> {
        self.subscribe(Subject::Rows(query), Listener::Rows(Box::new(listener)))
    }

    pub fn observe_row(&self, model: &str, id: &RowId, listener: impl Fn(&Change) + Send + Sync + 'static) -> Result<ObserverHandle, DbError> {
        self.subscribe(Subject::Row(RowRef::new(ModelKey::new(model), id.clone())), Listener::Rows(Box::new(listener)))
    }

    pub(crate) fn observe_feed(&self, subject: Subject, listener: impl Fn(Feed) + Send + Sync + 'static) -> Result<ObserverHandle, DbError> {
        self.subscribe(subject, Listener::Feed(Box::new(listener)))
    }

    pub(crate) fn observe_query_feed(&self, query: Query, count: bool, listener: impl Fn(Feed) + Send + Sync + 'static) -> Result<ObserverHandle, DbError> {
        self.observe_feed(if count { Subject::Count(query) } else { Subject::Rows(query) }, listener)
    }

    pub fn observe_count(&self, query: Query, listener: impl Fn(&CountChange) + Send + Sync + 'static) -> Result<ObserverHandle, DbError> {
        self.subscribe(Subject::Count(query), Listener::Count(Box::new(listener)))
    }
}
