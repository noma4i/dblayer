use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::sync::atomic::{AtomicU64, Ordering};
use crate::db::dispatch::{Effects, OrderedEffects};
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
    fetch: EntityFetch,
    generation: u64,
    epoch: u64,
}

impl Entry {
    fn new(lease: u64, singleton: bool, generation: u64, epoch: u64) -> Self {
        let machine = EntityMachine::new(singleton);
        let verdict = if singleton { machine.begin_feed() } else { machine.reset() };
        Self { lease, observer_lease: 0, machine, verdict, value: None, previous: None, value_token: 0, previous_token: 0, cancel: None, fetch: EntityFetch::None, generation, epoch }
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
    registration: OnceLock<crate::lifecycle::Registration>,
    observer: EntityObserver,
    database: Database,
    model: String,
    wire: Arc<WireModel>,
    listener: Arc<dyn EntityListener>,
    state: Mutex<State>,
    outbox: OrderedEffects,
    weak: Weak<Self>,
}

/// Declared before the `state` guard, so it drops after it: queued deliveries reach the listener with no library lock held.
struct Delivering<'a>(&'a LiveEntities);

impl Drop for Delivering<'_> {
    fn drop(&mut self) {
        if !std::thread::panicking() { self.0.outbox.run(); }
    }
}

/// Declared between `Delivering` and the `state` guard: cancels, feed cancel and lease releases run after the guard and before the deliveries.
struct Detached<'a> { live: &'a LiveEntities, tokens: Vec<CancelToken>, subscription: Option<Arc<LiveSubscription>>, released: Vec<RowId> }

impl Detached<'_> {
    fn cancel(&mut self, entry: &mut Entry) { self.tokens.extend(entry.cancel.take()); }
}

impl Drop for Detached<'_> {
    fn drop(&mut self) {
        for token in self.tokens.drain(..) { self.live.cancel_refresh(&token); }
        if !self.released.is_empty() { self.live.observer.release(&self.released); }
        if let Some(subscription) = self.subscription.take() { subscription.cancel(); }
    }
}

impl LiveEntities {
    fn now(&self) -> u64 { self.database.clock.now().as_millis().try_into().unwrap_or(u64::MAX) }
    fn generation(&self) -> u64 { self.database.db.generation().value() }
    fn epoch(&self) -> u64 { self.database.db.shared.ephemeral.lock().epoch }

    fn delivering(&self) -> Delivering<'_> { Delivering(self) }

    fn detached(&self) -> Detached<'_> { Detached { live: self, tokens: Vec::new(), subscription: None, released: Vec::new() } }

    fn cancel_refresh(&self, token: &CancelToken) {
        if let Some(sync) = self.database.sync.as_ref() { sync.cancel_entity_refresh(token); } else { token.cancel(); }
    }

    fn emit(&self, snapshots: Vec<WireEntitySnapshot>) {
        if snapshots.is_empty() { return; }
        let listener = self.listener.clone();
        let mut effects = Effects::default();
        effects.push(move || {
            crate::db::lock::assert_no_library_lock("EntityListener::changed");
            listener.changed(EntityDelivery { snapshots });
        });
        self.outbox.push(effects);
    }

    fn changed(&self, change: &EntityChange) {
        let _delivering = self.delivering();
        let mut state = locked(&self.state);
        if state.closed || change.epoch != self.epoch() { return; }
        let generation = self.generation();
        let mut snapshots = Vec::new();
        for row in &change.rows {
            if let Some(entry) = state.entries.get_mut(row.id.as_str()).filter(|entry| entry.generation == generation && change.leases.get(&row.id) == Some(&entry.observer_lease)) {
                let value = self.wire.row(row);
                if entry.verdict.revision == change.revision && !entry.verdict.stale && entry.value.as_ref() == Some(&value) { continue; }
                entry.generation = generation;
                entry.epoch = change.epoch;
                entry.accept(change.revision, Some(value), None);
                snapshots.push(entry.snapshot(row.id.as_str()));
            }
        }
        for id in &change.removed {
            if let Some(entry) = state.entries.get_mut(id.as_str()).filter(|entry| entry.generation == generation && change.leases.get(id) == Some(&entry.observer_lease)) {
                if entry.verdict.revision == change.revision && !entry.verdict.stale && entry.verdict.stage == EntityStage::Absent { continue; }
                entry.generation = generation;
                entry.epoch = change.epoch;
                entry.accept(change.revision, None, None);
                snapshots.push(entry.snapshot(id.as_str()));
            }
        }
        if let Some(message) = &change.failure {
            for (id, entry) in &mut state.entries {
                if entry.generation != generation || !change.leases.iter().any(|(changed, lease)| changed.as_str() == id && *lease == entry.observer_lease) { continue; }
                entry.verdict = entry.machine.fail(message.clone(), None);
                snapshots.push(entry.snapshot(id));
            }
        }
        self.emit(snapshots);
    }

    fn refreshed(&self, id: &str, lease: u64, generation: u64, error: Option<RefreshError>) {
        let _delivering = self.delivering();
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
        let _delivering = self.delivering();
        self.database.entity_cache.clear(&self.model);
        let mut detached = self.detached();
        let mut state = locked(&self.state);
        if state.closed { return; }
        let generation = self.generation();
        let epoch = self.epoch();
        let mut snapshots = Vec::new();
        let mut fetches = Vec::new();
        let mut reads = Vec::new();
        for (id, entry) in &mut state.entries {
            detached.cancel(entry);
            entry.clear_value();
            entry.generation = generation;
            entry.epoch = epoch;
            entry.verdict = entry.machine.reset();
            reads.push((id.clone(), entry.lease));
            if entry.fetch != EntityFetch::None {
                entry.verdict = entry.machine.read_started();
                fetches.push((id.clone(), entry.lease, entry.fetch));
            }
            snapshots.push(entry.snapshot(id));
        }
        if let Some(single) = &mut state.singleton {
            single.entry.clear_value();
            single.entry.generation = generation;
            single.entry.epoch = epoch;
            single.entry.verdict = single.entry.machine.reset();
            if !self.database.streams.snapshot().admits(crate::lifecycle::Origin::Observed) { single.entry.verdict = single.entry.machine.session_ended(); }
            snapshots.push(single.entry.snapshot(""));
            detached.subscription = single.subscription.take();
        }
        self.emit(snapshots);
        let weak = self.weak.clone();
        let mut effects = Effects::default();
        effects.push(move || {
            if let Some(source) = weak.upgrade() {
                source.resume();
                source.read_retained(reads, generation);
                source.refresh_retained(fetches, generation);
            }
        });
        self.outbox.push(effects);
    }

    fn end_reads(&self) {
        let _delivering = self.delivering();
        let mut state = locked(&self.state);
        if state.closed { return; }
        let mut snapshots = Vec::new();
        for (id, entry) in &mut state.entries {
            entry.verdict = entry.machine.session_ended();
            snapshots.push(entry.snapshot(id));
        }
        if let Some(single) = &mut state.singleton {
            single.entry.verdict = single.entry.machine.session_ended();
            snapshots.push(single.entry.snapshot(""));
        }
        self.emit(snapshots);
    }

    fn read_retained(&self, reads: Vec<(String, u64)>, generation: u64) {
        let _delivering = self.delivering();
        for (id, lease) in reads {
            let result = RowId::new(&id).map_err(|error| error.to_string()).and_then(|id| self.database.db.entity_row(&self.model, &id).map_err(|error| error.to_string()));
            let mut state = locked(&self.state);
            if state.closed || generation != self.generation() { return; }
            let Some(entry) = state.entries.get_mut(&id).filter(|entry| entry.lease == lease && entry.generation == generation) else { continue };
            match result {
                Ok((revision, row)) => {
                    let value = row.map(|row| self.wire.row(&row));
                    if entry.verdict.revision == revision && !entry.verdict.stale && entry.value == value && entry.verdict.stage != EntityStage::Opening { continue; }
                    entry.accept(revision, value, None);
                }
                Err(error) => entry.verdict = entry.machine.fail(error, None),
            }
            self.emit(vec![entry.snapshot(&id)]);
        }
    }

    fn refresh_retained(&self, fetches: Vec<(String, u64, EntityFetch)>, generation: u64) {
        if fetches.is_empty() { return; }
        let executor = match super::database::executor() {
            Ok(executor) => executor,
            Err(error) => {
                for (id, lease, _) in fetches { self.refreshed(&id, lease, generation, Some(RefreshError::Failed(error.to_string()))); }
                return;
            }
        };
        let _entered = executor.enter();
        for (id, lease, fetch) in fetches {
            {
                let state = locked(&self.state);
                if state.closed || generation != self.generation() { return; }
                if !state.entries.get(&id).is_some_and(|entry| entry.lease == lease && entry.generation == generation) { continue; }
            }
            let weak = self.weak.clone();
            let sink: crate::sync::RefreshSink = Arc::new(move |id, error| { if let Some(source) = weak.upgrade() { source.refreshed(id, lease, generation, error); } });
            let refresh = self.database.sync.as_ref().ok_or_else(|| DbError::Invalid { detail: "entity fetch needs a network session".into() }).and_then(|sync| sync.refresh_entities(&self.model, std::slice::from_ref(&id), fetch, sink).map_err(DbError::from));
            match refresh {
                Ok(cancel) => {
                    let unclaimed = {
                        let mut state = locked(&self.state);
                        match state.entries.get_mut(&id).filter(|entry| entry.lease == lease && entry.generation == generation) { Some(entry) => entry.cancel.replace(cancel), None => Some(cancel) }
                    };
                    if let Some(cancel) = unclaimed { self.cancel_refresh(&cancel); }
                }
                Err(error) => self.refreshed(&id, lease, generation, Some(RefreshError::Failed(error.to_string()))),
            }
        }
    }

    fn single_changed(&self, feed: u64, change: LiveChange) {
        let _delivering = self.delivering();
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
        let _delivering = self.delivering();
        let generation = self.generation();
        let epoch = self.epoch();
        let now = self.now();
        let mut ids = Vec::new();
        let mut fetches = Vec::new();
        {
            let mut detached = self.detached();
            let mut state = locked(&self.state);
            if state.closed { return Err(DbError::Interrupted { detail: "entities closed".into() }); }
            for request in requests {
                let id = RowId::new(&request.id).map_err(|error| DbError::Invalid { detail: error.to_string() })?;
                if state.entries.get(&request.id).is_some_and(|entry| entry.lease == request.lease) { continue; }
                if let Some(mut old) = state.entries.remove(&request.id) { detached.cancel(&mut old); detached.released.push(id.clone()); }
                let mut entry = Entry::new(request.lease, false, generation, epoch);
                entry.fetch = request.fetch;
                if request.fetch != EntityFetch::None { entry.verdict = entry.machine.read_started(); }
                let unchanged = |cached: &CachedEntity| self.database.db.revision_seq().is_ok_and(|current| cached.revision >= current);
                if let Some(cached) = self.database.entity_cache.take(&self.model, &request.id, now, generation, epoch).filter(unchanged) {
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
        super::database::executor()?;
        if !fetches.is_empty() && self.database.sync.is_none() {
            return Err(DbError::Invalid { detail: "entity fetch needs a network session".into() });
        }
        self.refresh_retained(fetches, generation);
        Ok(())
    }

    pub async fn load_initial(&self, id: String, lease: u64) -> Result<(), DbError> {
        super::entity_calls::entered();
        if self.database.streams.snapshot().admits(crate::lifecycle::Origin::Observed) { return Ok(()); }
        let sync = self.database.sync()?.clone();
        if !sync.holds_session() { return Err(DbError::Interrupted { detail: "the session ended".into() }); }
        let singleton = {
            let state = locked(&self.state);
            if state.closed { return Err(DbError::Interrupted { detail: "entities closed".into() }); }
            if id.is_empty() {
                let Some(single) = state.singleton.as_ref().filter(|single| single.entry.lease == lease) else { return Ok(()) };
                Some(single.query.clone())
            } else if state.entries.get(&id).is_some_and(|entry| entry.lease == lease) {
                None
            } else {
                return Ok(());
            }
        };
        match singleton {
            Some(json) => {
                let (_, remote) = self.database.parsed_query(&json)?;
                if let Some((relation, params)) = remote {
                    sync.load(&self.model, &relation, &params, crate::sync::LoadOptions::default()).await.map_err(DbError::from)?;
                }
            }
            None => sync.load_entity(&self.model, &id).await.map_err(DbError::from)?,
        }
        Ok(())
    }

    pub fn release(&self, leases: Vec<EntityLease>) -> Result<(), DbError> {
        super::entity_calls::entered();
        let now = self.now();
        let mut detached = self.detached();
        let mut state = locked(&self.state);
        for lease in leases {
            if !state.entries.get(&lease.id).is_some_and(|entry| entry.lease == lease.lease) { continue; }
            let Some(mut entry) = state.entries.remove(&lease.id) else { continue };
            detached.cancel(&mut entry);
            if !state.closed && (entry.value.is_some() || entry.verdict.stage == EntityStage::Absent) {
                self.database.entity_cache.keep(&self.model, &lease.id, CachedEntity { row: entry.value, revision: entry.verdict.revision, generation: entry.generation, epoch: entry.epoch }, now);
            }
            detached.released.push(RowId::new(&lease.id).map_err(|error| DbError::Invalid { detail: error.to_string() })?);
        }
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
            if state.closed || !self.database.streams.snapshot().admits(crate::lifecycle::Origin::Observed) { return; }
            let Some(single) = &mut state.singleton else { return };
            if single.subscription.is_some() { return; }
            if single.entry.verdict.feed == 0 { single.entry.verdict = single.entry.machine.begin_feed(); }
            single.entry.generation = self.generation();
            (single.query.clone(), single.entry.verdict.feed)
        };
        let subscription = self.database.live_query(pending.0, Arc::new(SingletonListener { source: self.weak.clone(), feed: pending.1 }));
        let mut detached = self.detached();
        let mut state = locked(&self.state);
        let closed = state.closed;
        if let Some(single) = &mut state.singleton {
            if !closed && single.entry.verdict.feed == pending.1 { single.subscription = Some(subscription); return; }
        }
        detached.subscription = Some(subscription);
    }

    pub fn reset(&self) { super::entity_calls::entered(); self.reset_core(); }

    pub fn shutdown(&self) {
        super::entity_calls::entered();
        if let Some(registration) = self.registration.get() { registration.remove(); }
        let _delivering = self.delivering();
        let mut detached = self.detached();
        let mut state = locked(&self.state);
        if std::mem::replace(&mut state.closed, true) { return; }
        let mut snapshots = Vec::new();
        for (id, entry) in &mut state.entries {
            detached.cancel(entry);
            entry.clear_value();
            entry.verdict = entry.machine.database_closed();
            snapshots.push(entry.snapshot(id));
        }
        if let Some(single) = &mut state.singleton {
            single.entry.clear_value();
            single.entry.verdict = single.entry.machine.database_closed();
            snapshots.push(single.entry.snapshot(""));
            detached.subscription = single.subscription.take();
        }
        self.database.entity_cache.clear(&self.model);
        self.emit(snapshots);
    }

    pub fn decode_failed(&self, lease: EntityLease, revision: i64, message: String) {
        super::entity_calls::entered();
        let _delivering = self.delivering();
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

impl crate::lifecycle::Member for LiveEntities {
    fn close(&self) { self.shutdown(); }
    fn lifecycle(&self, snapshot: &crate::lifecycle::Snapshot) {
        if snapshot.rereads(&self.model) { self.reset_core(); }
        else if matches!(snapshot.change, crate::lifecycle::Change::EndSession) { self.end_reads(); }
    }
    fn registration(&self) -> &OnceLock<crate::lifecycle::Registration> { &self.registration }
    fn busy(&self) -> bool {
        let state = locked(&self.state);
        let reading = state.entries.values().any(|entry| entry.machine.is_reading())
            || state.singleton.as_ref().is_some_and(|single| single.entry.machine.is_reading());
        drop(state);
        reading && self.database.streams.snapshot().admits(crate::lifecycle::Origin::Observed) || !self.outbox.is_idle()
    }
}

impl Drop for LiveEntities {
    fn drop(&mut self) {
        crate::db::lock::assert_no_library_lock("LiveEntities::drop");
        super::entity_calls::entered();
        let state = self.state.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner);
        let tokens: Vec<CancelToken> = state.entries.values_mut().filter_map(|entry| entry.cancel.take()).collect();
        for token in &tokens { self.cancel_refresh(token); }
    }
}

pub(super) fn open(database: Database, model: &str, wire: Arc<WireModel>, listener: Arc<dyn EntityListener>) -> Result<Arc<LiveEntities>, DbError> {
    let target: Arc<OnceLock<Weak<LiveEntities>>> = Arc::default();
    let seen = target.clone();
    let observer = database.db.observe_entities(model, move |change| { if let Some(source) = seen.get().and_then(Weak::upgrade) { source.changed(change); } })?;
    let source = Arc::new_cyclic(|weak: &Weak<LiveEntities>| {
        let _ = target.set(weak.clone());
        LiveEntities { observer, database, model: model.into(), wire, listener, state: Mutex::new(State::default()), outbox: OrderedEffects::default(), weak: weak.clone(), registration: OnceLock::new() }
    });
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

#[cfg(test)]
#[path = "entity_reenter_tests.rs"]
mod entity_reenter_tests;
