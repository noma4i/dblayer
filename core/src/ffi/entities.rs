use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::sync::atomic::{AtomicU64, Ordering};
use crate::db::{locked, EntityChange, EntityObserver};
use crate::identity::RowId;
use crate::sync::RefreshError;
use crate::transport::CancelToken;
use super::entity_machine::{EntityMachine, EntityVerdict};
use super::entity_pool::CachedEntity;
use super::wire::{WireModel, WireRow};
use super::{Database, DbError, EntityRead, EntityStage, LiveChange, LivePhase, LiveSubscription, QueryListener};
pub use crate::sync::EntityFetch;

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum EntityRefreshError { Offline, Failed { message: String } }

#[derive(Clone, Debug, uniffi::Record)]
pub struct EntityRequest { pub id: String, pub lease: u64, pub fetch: EntityFetch }

#[derive(Clone, Debug, uniffi::Record)]
pub struct EntityLease { pub id: String, pub lease: u64 }

#[derive(Clone, Debug, uniffi::Record)]
pub struct WireEntitySnapshot {
    pub id: String,
    pub lease: u64,
    pub value: Option<WireRow>,
    pub value_token: u64,
    pub phase: EntityStage,
    pub read: EntityRead,
    pub revision: i64,
    pub is_stale: bool,
    pub settled: bool,
    pub generation: u64,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct EntityDelivery { pub snapshots: Vec<WireEntitySnapshot> }

#[uniffi::export(rust, foreign)]
pub trait EntityListener: Send + Sync { fn changed(&self, delivery: EntityDelivery); }

struct Entry {
    lease: u64,
    observer_lease: u64,
    machine: EntityMachine,
    verdict: EntityVerdict,
    value: Option<WireRow>,
    previous: Option<WireRow>,
    value_token: u64,
    previous_token: u64,
    cancel: Option<CancelToken>,
    generation: u64,
    epoch: u64,
}

impl Entry {
    fn new(lease: u64, singleton: bool, generation: u64, epoch: u64) -> Self {
        let machine = EntityMachine::new(singleton);
        let verdict = if singleton { machine.begin_feed() } else { machine.reset() };
        Self { lease, observer_lease: 0, machine, verdict, value: None, previous: None, value_token: 0, previous_token: 0, cancel: None, generation, epoch }
    }

    fn snapshot(&self, id: &str) -> WireEntitySnapshot {
        WireEntitySnapshot { id: id.into(), lease: self.lease, value: self.value.clone(), value_token: self.value_token, phase: self.verdict.stage.clone(), read: self.verdict.read.clone(), revision: self.verdict.revision, is_stale: self.verdict.stale, settled: self.verdict.settled, generation: self.generation }
    }

    fn accept(&mut self, revision: i64, row: Option<WireRow>, feed: Option<u64>) {
        self.verdict = self.machine.accept(revision, row.is_some(), feed);
        if self.verdict.applied && self.value != row {
            self.previous = self.value.take();
            self.previous_token = self.value_token;
            self.value = row;
            self.value_token = NEXT_VALUE.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn cancel(&mut self, sync: Option<&crate::sync::Sync>) {
        if let Some(cancel) = self.cancel.take() {
            if let Some(sync) = sync { sync.cancel_entity_refresh(&cancel); } else { cancel.cancel(); }
        }
    }

    fn clear_value(&mut self) {
        self.value = None;
        self.previous = None;
        self.value_token = NEXT_VALUE.fetch_add(1, Ordering::Relaxed);
    }
}

struct Singleton {
    entry: Entry,
    query: String,
    subscription: Option<Arc<LiveSubscription>>,
}

static NEXT_VALUE: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct State { entries: BTreeMap<String, Entry>, singleton: Option<Singleton>, closed: bool }

#[derive(uniffi::Object)]
pub struct LiveEntities {
    observer: EntityObserver,
    database: Database,
    model: String,
    wire: Arc<WireModel>,
    listener: Arc<dyn EntityListener>,
    state: Mutex<State>,
    weak: Weak<Self>,
}

impl LiveEntities {
    fn now(&self) -> u64 { self.database.clock.now().as_millis().try_into().unwrap_or(u64::MAX) }
    fn generation(&self) -> u64 { self.database.db.generation().value() }
    fn epoch(&self) -> u64 { self.database.db.shared.ephemeral.lock().epoch }

    fn emit(&self, snapshots: Vec<WireEntitySnapshot>) {
        if !snapshots.is_empty() { self.listener.changed(EntityDelivery { snapshots }); }
    }

    fn changed(&self, change: &EntityChange) {
        let mut state = locked(&self.state);
        if state.closed || change.epoch != self.epoch() { return; }
        let generation = self.generation();
        let mut snapshots = Vec::new();
        for row in &change.rows {
            if let Some(entry) = state.entries.get_mut(row.id.as_str()).filter(|entry| change.leases.get(&row.id) == Some(&entry.observer_lease)) {
                entry.generation = generation;
                entry.epoch = change.epoch;
                entry.accept(change.revision, Some(self.wire.row(row)), None);
                snapshots.push(entry.snapshot(row.id.as_str()));
            }
        }
        for id in &change.removed {
            if let Some(entry) = state.entries.get_mut(id.as_str()).filter(|entry| change.leases.get(id) == Some(&entry.observer_lease)) {
                entry.generation = generation;
                entry.epoch = change.epoch;
                entry.accept(change.revision, None, None);
                snapshots.push(entry.snapshot(id.as_str()));
            }
        }
        if let Some(message) = &change.failure {
            for (id, entry) in &mut state.entries {
                if !change.leases.iter().any(|(changed, lease)| changed.as_str() == id && *lease == entry.observer_lease) { continue; }
                entry.verdict = entry.machine.fail(message.clone(), None);
                snapshots.push(entry.snapshot(id));
            }
        }
        self.emit(snapshots);
    }

    fn refreshed(&self, id: &str, lease: u64, generation: u64, error: Option<RefreshError>) {
        if generation != self.generation() { return; }
        let landed = match (&error, RowId::new(id)) {
            (None, Ok(row_id)) => Some(self.database.db.entity_row(&self.model, &row_id)),
            _ => None,
        };
        let mut state = locked(&self.state);
        if state.closed || generation != self.generation() { return; }
        let Some(entry) = state.entries.get_mut(id).filter(|entry| entry.lease == lease && entry.generation == generation) else { return };
        let mut error = error.map(|error| match error { RefreshError::Offline => EntityRefreshError::Offline, RefreshError::Failed(message) => EntityRefreshError::Failed { message } });
        match landed {
            Some(Ok((revision, row))) => entry.accept(revision, row.map(|row| self.wire.row(&row)), None),
            Some(Err(failure)) => error = Some(EntityRefreshError::Failed { message: failure.to_string() }),
            None => {}
        }
        entry.verdict = entry.machine.read_ended(error, None);
        self.emit(vec![entry.snapshot(id)]);
    }

    fn reset_core(&self) {
        self.database.entity_cache.clear(&self.model);
        let mut state = locked(&self.state);
        if state.closed { return; }
        let generation = self.generation();
        let epoch = self.epoch();
        let mut snapshots = Vec::new();
        for (id, entry) in &mut state.entries {
            entry.cancel(self.database.sync.as_ref());
            entry.clear_value();
            entry.generation = generation;
            entry.epoch = epoch;
            entry.verdict = entry.machine.reset();
            snapshots.push(entry.snapshot(id));
        }
        let subscription = if let Some(single) = &mut state.singleton {
            single.entry.clear_value();
            single.entry.generation = generation;
            single.entry.epoch = epoch;
            single.entry.verdict = single.entry.machine.reset();
            snapshots.push(single.entry.snapshot(""));
            single.subscription.take()
        } else { None };
        self.emit(snapshots);
        drop(state);
        if let Some(subscription) = subscription { subscription.cancel(); }
    }

    fn single_changed(&self, feed: u64, change: LiveChange) {
        let mut state = locked(&self.state);
        if state.closed { return; }
        let Some(single) = &mut state.singleton else { return };
        if single.entry.verdict.feed != feed || single.entry.generation != self.generation() { return; }
        if change.full || !change.rows.is_empty() || change.size == 0 {
            single.entry.accept(change.revision, change.rows.into_iter().next(), Some(feed));
        }
        if let Some(error) = change.error {
            single.entry.verdict = single.entry.machine.fail(error.to_string(), Some(feed));
        } else if matches!(change.phase, Some(LivePhase::Loaded | LivePhase::Empty)) {
            single.entry.verdict = single.entry.machine.read_ended(None, Some(feed));
        } else if change.phase == Some(LivePhase::Closed) {
            single.entry.clear_value();
            single.entry.verdict = single.entry.machine.database_closed();
        }
        self.emit(vec![single.entry.snapshot("")]);
    }
}

struct SingletonListener { source: Weak<LiveEntities>, feed: u64 }
impl QueryListener for SingletonListener {
    fn changed(&self, change: LiveChange) { if let Some(source) = self.source.upgrade() { source.single_changed(self.feed, change); } }
}

#[uniffi::export]
impl LiveEntities {
    pub fn retain(&self, requests: Vec<EntityRequest>) -> Result<(), DbError> {
        super::entity_calls::entered();
        let generation = self.generation();
        let epoch = self.epoch();
        let mut ids = Vec::new();
        let mut fetches = Vec::new();
        {
            let mut state = locked(&self.state);
            if state.closed { return Err(DbError::Interrupted { detail: "entities closed".into() }); }
            for request in requests {
                let id = RowId::new(&request.id).map_err(|error| DbError::Invalid { detail: error.to_string() })?;
                if state.entries.get(&request.id).is_some_and(|entry| entry.lease == request.lease) { continue; }
                if let Some(mut old) = state.entries.remove(&request.id) { old.cancel(self.database.sync.as_ref()); self.observer.release(std::slice::from_ref(&id)); }
                let mut entry = Entry::new(request.lease, false, generation, epoch);
                if request.fetch != EntityFetch::None { entry.verdict = entry.machine.read_started(); }
                if let Some(cached) = self.database.entity_cache.take(&self.model, &request.id, self.now(), generation, epoch) {
                    entry.verdict = if cached.row.is_some() { entry.machine.seed(cached.revision) } else { entry.machine.accept(cached.revision, false, None) };
                    entry.value = cached.row;
                    entry.value_token = NEXT_VALUE.fetch_add(1, Ordering::Relaxed);
                    self.emit(vec![entry.snapshot(&request.id)]);
                }
                if request.fetch != EntityFetch::None { fetches.push((request.id.clone(), request.lease, request.fetch)); }
                state.entries.insert(request.id, entry);
                ids.push(id);
            }
        }
        {
            let mut state = locked(&self.state);
            self.observer.retain(&ids)?;
            for (id, lease) in self.observer.leases(&ids) {
                if let Some(entry) = state.entries.get_mut(id.as_str()) { entry.observer_lease = lease; }
            }
        }
        let _entered = super::database::executor()?.enter();
        for (id, lease, fetch) in fetches {
            let weak = self.weak.clone();
            let sink: crate::sync::RefreshSink = Arc::new(move |id, error| { if let Some(source) = weak.upgrade() { source.refreshed(id, lease, generation, error); } });
            let refresh = self.database.sync.as_ref().ok_or_else(|| DbError::Invalid { detail: "entity fetch needs a network session".into() })?.refresh_entities(&self.model, std::slice::from_ref(&id), fetch, sink);
            match refresh {
                Ok(cancel) => { if let Some(entry) = locked(&self.state).entries.get_mut(&id).filter(|entry| entry.lease == lease) { entry.cancel = Some(cancel); } else { cancel.cancel(); } }
                Err(error) => self.refreshed(&id, lease, generation, Some(RefreshError::Failed(error.to_string()))),
            }
        }
        Ok(())
    }

    pub fn release(&self, leases: Vec<EntityLease>) -> Result<(), DbError> {
        super::entity_calls::entered();
        let mut state = locked(&self.state);
        let mut ids = Vec::new();
        for lease in leases {
            if !state.entries.get(&lease.id).is_some_and(|entry| entry.lease == lease.lease) { continue; }
            let Some(mut entry) = state.entries.remove(&lease.id) else { continue };
            entry.cancel(self.database.sync.as_ref());
            if !state.closed && (entry.value.is_some() || entry.verdict.stage == EntityStage::Absent) {
                self.database.entity_cache.keep(&self.model, &lease.id, CachedEntity { row: entry.value, revision: entry.verdict.revision, generation: entry.generation, epoch: entry.epoch }, self.now());
            }
            ids.push(RowId::new(&lease.id).map_err(|error| DbError::Invalid { detail: error.to_string() })?);
        }
        self.observer.release(&ids);
        Ok(())
    }

    pub fn singleton(&self, query_json: String, lease: u64) -> Result<(), DbError> {
        super::entity_calls::entered();
        {
            let mut state = locked(&self.state);
            if state.closed { return Err(DbError::Interrupted { detail: "entities closed".into() }); }
            if state.singleton.is_some() { return Ok(()); }
            state.singleton = Some(Singleton { entry: Entry::new(lease, true, self.generation(), self.epoch()), query: query_json, subscription: None });
        }
        self.resume();
        Ok(())
    }

    pub fn resume(&self) {
        super::entity_calls::entered();
        let pending = {
            let mut state = locked(&self.state);
            if state.closed { return; }
            let Some(single) = &mut state.singleton else { return };
            if single.subscription.is_some() { return; }
            if single.entry.verdict.feed == 0 { single.entry.verdict = single.entry.machine.begin_feed(); }
            single.entry.generation = self.generation();
            (single.query.clone(), single.entry.verdict.feed)
        };
        let subscription = self.database.live_query(pending.0, Arc::new(SingletonListener { source: self.weak.clone(), feed: pending.1 }));
        let mut state = locked(&self.state);
        let closed = state.closed;
        if let Some(single) = &mut state.singleton {
            if !closed && single.entry.verdict.feed == pending.1 { single.subscription = Some(subscription); return; }
        }
        drop(state);
        subscription.cancel();
    }

    pub fn reset(&self) { super::entity_calls::entered(); self.reset_core(); }

    pub fn shutdown(&self) {
        super::entity_calls::entered();
        let mut state = locked(&self.state);
        if std::mem::replace(&mut state.closed, true) { return; }
        let mut snapshots = Vec::new();
        for (id, entry) in &mut state.entries {
            entry.cancel(self.database.sync.as_ref());
            entry.clear_value();
            entry.verdict = entry.machine.database_closed();
            snapshots.push(entry.snapshot(id));
        }
        let subscription = if let Some(single) = &mut state.singleton {
            single.entry.clear_value();
            single.entry.verdict = single.entry.machine.database_closed();
            snapshots.push(single.entry.snapshot(""));
            single.subscription.take()
        } else { None };
        self.database.entity_cache.clear(&self.model);
        self.emit(snapshots);
        drop(state);
        if let Some(subscription) = subscription { subscription.cancel(); }
    }

    pub fn decode_failed(&self, lease: EntityLease, revision: i64, message: String) {
        super::entity_calls::entered();
        let mut state = locked(&self.state);
        let entry = if lease.id.is_empty() { state.singleton.as_mut().map(|single| &mut single.entry) } else { state.entries.get_mut(&lease.id) };
        if let Some(entry) = entry.filter(|entry| entry.lease == lease.lease && entry.verdict.revision == revision) {
            entry.value = entry.previous.take();
            entry.value_token = entry.previous_token;
            entry.verdict = entry.machine.fail(message, (entry.verdict.feed != 0).then_some(entry.verdict.feed));
            self.emit(vec![entry.snapshot(&lease.id)]);
        }
    }
}

impl Drop for LiveEntities {
    fn drop(&mut self) {
        super::entity_calls::entered();
        for entry in locked(&self.state).entries.values_mut() { entry.cancel(self.database.sync.as_ref()); }
    }
}

pub(super) fn open(database: Database, model: &str, wire: Arc<WireModel>, listener: Arc<dyn EntityListener>) -> Result<Arc<LiveEntities>, DbError> {
    let db = database.db.clone();
    let target: Arc<OnceLock<Weak<LiveEntities>>> = Arc::default();
    let seen = target.clone();
    let observer = db.observe_entities(model, move |change| { if let Some(source) = seen.get().and_then(Weak::upgrade) { source.changed(change); } })?;
    let source = Arc::new_cyclic(|weak: &Weak<LiveEntities>| {
        let _ = target.set(weak.clone());
        LiveEntities { observer, database, model: model.into(), wire, listener, state: Mutex::new(State::default()), weak: weak.clone() }
    });
    let weak = Arc::downgrade(&source);
    let name = model.to_string();
    db.on_reset(Arc::new(move |models| { if models.is_none_or(|models| models.contains(&name)) { if let Some(source) = weak.upgrade() { source.reset_core(); } } }));
    Ok(source)
}

#[uniffi::export]
pub fn entity_open_failed(id: String, lease: u64, message: String, unopened: bool) -> WireEntitySnapshot {
    super::entity_calls::entered();
    let mut entry = Entry::new(lease, false, 0, 0);
    entry.verdict = if unopened { entry.machine.database_closed() } else { entry.machine.fail(message, None) };
    entry.snapshot(&id)
}

#[cfg(test)]
#[path = "entity_snapshot_tests.rs"]
mod entity_snapshot_tests;
