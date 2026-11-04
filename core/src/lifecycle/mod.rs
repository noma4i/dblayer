use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};

use statig::prelude::*;
use tokio::sync::watch;

use crate::db::dispatch::{Effects, OrderedEffects};
use crate::identity::Generation;

mod authority;
mod machine;

pub(crate) use authority::{Authority, Epoch};
#[cfg(test)]
pub(crate) use authority::Internal;

use machine::Event;

/// What work needs to start: `Explicit` work runs without a session, `Session` work needs one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Origin { Explicit, Session }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum End { Server, Local }

/// The exclusive transitions. Each one ends every lease of the old epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Boundary {
    SessionStart,
    SessionEnd(End),
    /// The cache of every model clears.
    Content,
    /// The cache of some models clears. Work in flight goes on and its late answers are refused by the generation.
    Models,
}

#[derive(Clone, Debug)]
pub(crate) enum Change {
    SessionStarted(Option<String>),
    SessionEnded,
    Cleared(Option<Vec<String>>),
    Closed,
}

#[derive(Clone, Debug)]
pub(crate) struct Snapshot {
    pub generation: Generation,
    pub session: Option<String>,
    pub ended: bool,
    pub transition: Option<Boundary>,
    pub closed: bool,
    pub revision: u64,
    pub notice: u64,
    pub session_epoch: u64,
    pub change: Change,
}

/// What a call saw when it was admitted. A write that carries an older stamp belongs to a database state that no longer exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Stamp {
    pub generation: Generation,
    pub session_epoch: u64,
}

/// The lease of a call ended because the database closed or a transition began.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Ended;

impl From<Ended> for crate::transport::TransportError {
    fn from(_: Ended) -> Self { crate::transport::TransportError::Interrupted("the database closed, reset or ended its session".into()) }
}

impl From<Ended> for crate::sync::SyncError {
    fn from(ended: Ended) -> Self { crate::sync::SyncError::Transport(ended.into()) }
}

/// Why a transition did not begin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refused {
    Closed,
    /// The epoch moved since the caller was admitted: its answer belongs to a state that no longer exists.
    Stale,
    /// The same session end was already running and has finished: the caller has nothing left to do.
    Joined,
}

impl From<Refused> for crate::transport::TransportError {
    fn from(refused: Refused) -> Self {
        crate::transport::TransportError::Interrupted(match refused {
            Refused::Closed => "the database closed".into(),
            Refused::Stale => "the session ended while its answer was pending".into(),
            Refused::Joined => "the session end was already running".into(),
        })
    }
}

impl From<Refused> for crate::ffi::DbError {
    fn from(refused: Refused) -> Self { Self::Interrupted { detail: crate::transport::TransportError::from(refused).to_string() } }
}

impl Default for Snapshot {
    fn default() -> Self {
        Self { generation: Generation::INITIAL, session: None, ended: false, transition: None, closed: false, revision: 0, notice: 0, session_epoch: 0, change: Change::SessionStarted(None) }
    }
}

impl Snapshot {
    fn apply(&mut self, event: &Event) {
        match event {
            Event::Begin(boundary) => {
                if !matches!(boundary, Boundary::Models) { self.session_epoch += 1; }
                self.transition = Some(*boundary);
            }
            Event::Generation => self.generation = self.generation.next(),
            Event::Ended => { self.session = None; self.ended = true; self.generation = self.generation.next(); self.change = Change::SessionEnded; self.notice += 1; }
            Event::Started(session) => { self.session = session.clone(); self.ended = false; self.change = Change::SessionStarted(session.clone()); self.notice += 1; }
            Event::Cleared(models) => { self.change = Change::Cleared(models.clone()); self.notice += 1; }
            Event::Finish => self.transition = None,
            Event::Close => { self.closed = true; self.transition = None; self.change = Change::Closed; self.notice += 1; }
        }
        self.revision += 1;
    }

    pub(crate) fn signed_in(&self) -> bool { self.session.is_some() && !self.ended }

    pub(crate) fn stamp(&self) -> Stamp { Stamp { generation: self.generation, session_epoch: self.session_epoch } }

    /// Whether work of this kind may start now. A transition admits nothing: the work of its old epoch ends, and the next work waits for the transition to finish.
    pub(crate) fn admits(&self, origin: Origin) -> bool {
        !self.closed && self.transition.is_none() && match origin {
            Origin::Explicit => true,
            Origin::Session => self.signed_in(),
        }
    }

    pub(crate) fn concerns(&self, _model: &str) -> bool { true }

    pub(crate) fn rereads(&self, model: &str) -> bool {
        match &self.change {
            Change::Cleared(models) => models.as_ref().is_none_or(|models| models.iter().any(|name| name == model)),
            Change::SessionStarted(Some(_)) => true,
            _ => false,
        }
    }
}

pub(crate) enum Invalidated {
    Ended,
    Rebound(Snapshot),
}

/// Follows the latest settled change. A receiver that falls behind sees the newest change only, and compares the epoch and generation it holds.
pub(crate) struct Watch {
    state: watch::Receiver<Snapshot>,
    seen: u64,
    generation: Generation,
    session_epoch: u64,
}

impl Watch {
    pub(crate) fn current(&self) -> Snapshot { self.state.borrow().clone() }

    pub(crate) async fn invalidated(&mut self) -> Option<Invalidated> {
        loop {
            let next = self.changed().await?;
            let moved = std::mem::replace(&mut self.generation, next.generation) != next.generation;
            let epoch_moved = std::mem::replace(&mut self.session_epoch, next.session_epoch) != next.session_epoch;
            if moved || !matches!(next.change, Change::SessionStarted(_)) { return Some(Invalidated::Ended); }
            if epoch_moved { return Some(Invalidated::Rebound(next)); }
        }
    }

    pub(crate) async fn changed(&mut self) -> Option<Snapshot> {
        let seen = self.seen;
        let snapshot = self.state.wait_for(|snapshot| snapshot.notice > seen).await.ok()?.clone();
        self.seen = snapshot.notice;
        Some(snapshot)
    }
}

/// An object that lives as long as the database and must hear its closing and its transitions.
pub(crate) trait Holder: Send + Sync {
    fn close(&self);
    fn registration(&self) -> &OnceLock<Registration>;
    fn changed(&self, _snapshot: &Snapshot) {}
}

pub(crate) struct Registration { owner: Weak<Lifecycle>, id: usize }

impl Registration {
    pub(crate) fn remove(&self) {
        if let Some(owner) = self.owner.upgrade() {
            let removed = crate::db::locked(&owner.state).holders.remove(&self.id);
            drop(removed);
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) { self.remove(); }
}

/// Open holders in a vector without gaps, so a walk costs the number of open holders however many were open before.
#[derive(Default)]
struct Holders {
    order: Vec<(usize, Weak<dyn Holder>)>,
    index: HashMap<usize, usize>,
}

impl Holders {
    fn insert(&mut self, id: usize, holder: Weak<dyn Holder>) {
        self.index.insert(id, self.order.len());
        self.order.push((id, holder));
    }

    fn remove(&mut self, id: &usize) -> Option<Weak<dyn Holder>> {
        let at = self.index.remove(id)?;
        let (_, removed) = self.order.swap_remove(at);
        if let Some((moved, _)) = self.order.get(at) { self.index.insert(*moved, at); }
        Some(removed)
    }

    fn open(&self) -> Vec<Weak<dyn Holder>> { self.order.iter().map(|(_, holder)| holder.clone()).collect() }

    #[cfg(test)]
    fn len(&self) -> usize { self.order.len() }

    fn clear(&mut self) {
        self.order = Vec::new();
        self.index = HashMap::new();
    }
}

#[derive(Clone, Debug)]
struct LeaseInfo {
    target: &'static str,
    passive: bool,
}

/// What is running now: the number of leases and whether a transition is waiting for them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) struct Activity {
    pub held: usize,
    pub transition: bool,
    seq: u64,
}

struct State {
    machine: InitializedStateMachine<machine::Phases>,
    holders: Holders,
    leases: HashMap<u64, LeaseInfo>,
    next_lease: u64,
    activity: u64,
    runner: Option<std::thread::ThreadId>,
}

impl State {
    fn snapshot(&self) -> &Snapshot { &self.machine.inner().snapshot }

    fn activity(&mut self) -> Activity {
        self.activity += 1;
        Activity { held: self.leases.len(), transition: self.snapshot().transition.is_some(), seq: self.activity }
    }
}

pub(crate) struct Lifecycle {
    state: Mutex<State>,
    /// Orders a pinned SQL commit against the transitions: a transition changes the epoch while it holds this line, and a pinned write checks the epoch and commits while it holds it.
    commit_line: Mutex<()>,
    opening_issued: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(crate) begin_waits: (Mutex<usize>, Condvar),
    idle: Condvar,
    effects: OrderedEffects,
    changed: watch::Sender<Snapshot>,
    activity: watch::Sender<Activity>,
    #[cfg(test)]
    work: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    pub(crate) pinned_writes: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    strict: std::sync::atomic::AtomicBool,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self {
            state: Mutex::new(State { machine: machine::new(), holders: Holders::default(), leases: HashMap::new(), next_lease: 0, activity: 0, runner: None }),
            commit_line: Mutex::new(()),
            opening_issued: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            begin_waits: (Mutex::new(0), Condvar::new()),
            idle: Condvar::new(),
            effects: OrderedEffects::default(),
            changed: watch::channel(Snapshot::default()).0,
            activity: watch::channel(Activity::default()).0,
            #[cfg(test)]
            work: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            pinned_writes: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            strict: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

/// One piece of work in the database. It is issued before the work is queued and ends with the single outcome of that work. A transition ends every lease of the old epoch.
pub(crate) struct Lease {
    life: Arc<Lifecycle>,
    id: u64,
    epoch: u64,
    /// The data generation the lease was issued in. A lease of work that reads the data (a load, a wait for a row) ends when the data clears. A lease of a send ends only at a session boundary.
    generation: Option<Generation>,
}

impl Lease {
    pub(crate) fn epoch(&self) -> u64 { self.epoch }

    /// Whether the epoch this lease was issued in is still the current one.
    pub(crate) fn is_current(&self) -> bool {
        let state = crate::db::locked(&self.life.state);
        let snapshot = state.snapshot();
        !snapshot.closed && snapshot.session_epoch == self.epoch && self.generation.is_none_or(|issued| issued == snapshot.generation)
    }

    pub(crate) fn pinned<F: std::future::Future>(&self, work: F) -> impl std::future::Future<Output = F::Output> {
        self.epoch_value().scope(work)
    }

    pub(crate) fn epoch_value(&self) -> Epoch { Epoch::new(&self.life, self.epoch) }

    /// Runs the work inside the lease. A transition or the close of the database ends the work with `Ended`, and work that has not started when that happens never starts.
    pub(crate) async fn run<T>(&self, work: impl std::future::Future<Output = T>) -> Result<T, Ended> {
        if !self.is_current() { return Err(Ended); }
        let mut changes = self.life.changed.subscribe();
        let (epoch, generation) = (self.epoch, self.generation);
        tokio::select! {
            biased;
            _ = changes.wait_for(|published| published.closed || published.session_epoch != epoch || generation.is_some_and(|issued| issued != published.generation)) => Err(Ended),
            result = self.epoch_value().scope(work) => Ok(result),
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) { self.life.release(self.id); }
}

/// The right to run one exclusive transition. Dropping it finishes the transition.
pub(crate) struct Permit {
    life: Arc<Lifecycle>,
    boundary: Boundary,
    deferred: Mutex<Vec<Event>>,
    finished: bool,
}

impl Permit {
    pub(crate) fn boundary(&self) -> Boundary { self.boundary }

    pub(crate) fn authority(&self) -> Authority { Authority::Internal(authority::Internal::new()) }

    /// Marks the calling thread as the one that runs this transition. A session end that this thread asks for meanwhile joins it at once, since it cannot wait for itself.
    pub(crate) fn run_on_this_thread(&self) {
        crate::db::locked(&self.life.state).runner = Some(std::thread::current().id());
    }

    /// Waits until every lease of the old epoch has ended. They were all told to stop when the transition began.
    pub(crate) async fn drained(&self) {
        let mut activity = self.life.activity.subscribe();
        let _ = activity.wait_for(|activity| activity.held == 0).await;
    }

    /// Moves the generation of the data. The caller holds the writer lock of the store, so a write that checked the old generation lands before this or is refused.
    pub(crate) fn next_generation(&self) -> Generation {
        let mut state = crate::db::locked(&self.life.state);
        state.machine.handle(&Event::Generation);
        state.snapshot().generation
    }

    /// Ends the session in the machine and moves the generation. Returns the new generation and the ticket of the holders' notification. The caller holds the writer lock of the store.
    pub(crate) fn ended(&self) -> (Generation, u64) {
        self.life.deliver(Event::Ended, true)
    }

    /// Queues a change for the holders. It reaches them when the transition has finished, so a holder that rereads finds the database open.
    pub(crate) fn defer(&self, change: Change) {
        let event = match change {
            Change::SessionStarted(session) => Event::Started(session),
            Change::Cleared(models) => Event::Cleared(models),
            Change::SessionEnded | Change::Closed => return,
        };
        crate::db::locked(&self.deferred).push(event);
    }

    pub(crate) fn finish(mut self) {
        self.complete();
    }

    fn complete(&mut self) {
        if std::mem::replace(&mut self.finished, true) { return; }
        let events = std::mem::take(&mut *crate::db::locked(&self.deferred));
        self.life.finish_transition(events);
    }
}

impl Drop for Permit {
    fn drop(&mut self) { self.complete(); }
}

impl Lifecycle {
    #[cfg(test)]
    pub(crate) fn strict_epoch(&self) -> bool { self.strict.load(std::sync::atomic::Ordering::SeqCst) }

    #[cfg(test)]
    pub(crate) fn require_epoch(&self) { self.strict.store(true, std::sync::atomic::Ordering::SeqCst); }

    pub(crate) fn snapshot(&self) -> Snapshot { crate::db::locked(&self.state).snapshot().clone() }

    /// Whether the epoch is the one now and the database is open. It reads the published snapshot and takes no library lock, so the writer may call it.
    pub(crate) fn admits_epoch(&self, epoch: u64) -> bool {
        let published = self.changed.borrow();
        !published.closed && published.session_epoch == epoch
    }

    /// The line a pinned SQL commit holds from its epoch check to the end of the commit. A transition cannot change the epoch meanwhile.
    pub(crate) fn commit_line(&self) -> crate::db::lock::Guard<'_, ()> { crate::db::locked(&self.commit_line) }

    /// Whether the epoch is the one now and the database is open, read from the state itself. The caller holds the commit line.
    pub(crate) fn epoch_holds(&self, epoch: u64) -> bool {
        let state = crate::db::locked(&self.state);
        let current = state.snapshot();
        !current.closed && current.session_epoch == epoch
    }

    pub(crate) fn watch(&self) -> Watch {
        let state = self.changed.subscribe();
        let (seen, generation, session_epoch) = { let published = state.borrow(); (published.notice, published.generation, published.session_epoch) };
        Watch { state, seen, generation, session_epoch }
    }

    /// Takes a lease for work of this kind, or refuses it when the database is closed, a transition runs, or the work needs a session that is not there.
    pub(crate) fn lease(self: &Arc<Self>, origin: Origin, target: &'static str) -> Result<Lease, Ended> {
        self.issue(origin, target, false, true)
    }

    /// A lease for a send or a session call: only a session boundary or the close ends it. A clear of some models leaves it running, and the generation refuses its late answer.
    pub(crate) fn lease_for_session(self: &Arc<Self>, origin: Origin, target: &'static str) -> Result<Lease, Ended> {
        self.issue(origin, target, false, false)
    }

    /// A lease for a holder that owes a value but does no work: a transition does not wait for it. The transition drops it from the count, and its holder takes a new one when it owes a value in the next epoch.
    pub(crate) fn lease_passive(self: &Arc<Self>, origin: Origin, target: &'static str) -> Result<Lease, Ended> {
        self.issue(origin, target, true, false)
    }

    fn issue(self: &Arc<Self>, origin: Origin, target: &'static str, passive: bool, data_bound: bool) -> Result<Lease, Ended> {
        let (lease, activity) = {
            let mut state = crate::db::locked(&self.state);
            if !state.snapshot().admits(origin) { return Err(Ended); }
            if Epoch::ambient().is_some_and(|held| held.value() != state.snapshot().session_epoch) { return Err(Ended); }
            let (id, epoch) = (state.next_lease, state.snapshot().session_epoch);
            state.next_lease += 1;
            state.leases.insert(id, LeaseInfo { target, passive });
            let generation = data_bound.then(|| state.snapshot().generation);
            (Lease { life: self.clone(), id, epoch, generation }, state.activity())
        };
        self.publish_activity(activity);
        Ok(lease)
    }

    fn release(&self, id: u64) {
        let activity = {
            let mut state = crate::db::locked(&self.state);
            if state.leases.remove(&id).is_none() { return; }
            state.activity()
        };
        self.publish_activity(activity);
    }

    fn publish_activity(&self, activity: Activity) {
        crate::db::lock::effect(|| self.activity.send_if_modified(|current| {
            if activity.seq <= current.seq { return false; }
            *current = activity;
            true
        }));
    }

    fn publish(&self, snapshot: Snapshot) {
        crate::db::lock::effect(|| self.changed.send_if_modified(|current| {
            if snapshot.revision <= current.revision { return false; }
            *current = snapshot;
            true
        }));
    }

    /// True when no lease is held and no transition runs.
    #[cfg(test)]
    pub(crate) fn quiet(&self) -> bool {
        let state = crate::db::locked(&self.state);
        state.leases.is_empty() && state.snapshot().transition.is_none()
    }

    /// Waits until work of this kind may start. False when the database closed first.
    pub(crate) async fn admission(&self, origin: Origin) -> bool {
        let mut changes = self.changed.subscribe();
        changes.wait_for(|published| published.closed || published.admits(origin)).await.is_ok_and(|published| !published.closed)
    }

    /// The work that holds a lease now, for a report that names what a wait is waiting for.
    pub(crate) fn held(&self) -> Vec<String> {
        let state = crate::db::locked(&self.state);
        let mut held: Vec<_> = state.leases.iter().map(|(id, info)| format!("#{id} {}", info.target)).collect();
        held.sort();
        held
    }

    /// Returns when no lease is held and no transition runs. The check is the current quiet, not a promise about later events.
    pub(crate) async fn settled(&self) {
        let mut activity = self.activity.subscribe();
        let _ = activity.wait_for(|activity| activity.held == 0 && !activity.transition).await;
    }

    /// Starts an exclusive transition. The epoch moves at once and every lease of the old epoch is told to stop. A caller that carries the epoch it was admitted in is refused when that epoch has gone.
    pub(crate) fn try_begin(self: &Arc<Self>, boundary: Boundary, expected: Option<u64>) -> Result<Result<Permit, Boundary>, Refused> {
        #[cfg(test)]
        {
            *self.begin_waits.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
            self.begin_waits.1.notify_all();
        }
        let line = crate::db::locked(&self.commit_line);
        let (permit, snapshot, activity) = {
            let mut state = crate::db::locked(&self.state);
            let current = state.snapshot();
            if current.closed { return Err(Refused::Closed); }
            if let Some(running) = current.transition { return Ok(Err(running)); }
            if expected.is_some_and(|epoch| epoch != current.session_epoch) { return Err(Refused::Stale); }
            state.machine.handle(&Event::Begin(boundary));
            state.leases.retain(|_, info| !info.passive);
            let snapshot = state.snapshot().clone();
            let activity = state.activity();
            (Permit { life: self.clone(), boundary, deferred: Mutex::new(Vec::new()), finished: false }, snapshot, activity)
        };
        drop(line);
        self.publish(snapshot);
        self.publish_activity(activity);
        Ok(Ok(permit))
    }

    fn runs_on_this_thread(&self) -> bool {
        crate::db::locked(&self.state).runner == Some(std::thread::current().id())
    }

    /// Waits for a running transition to finish, then begins. A session end that finds another session end running waits for it and has nothing left to do.
    pub(crate) async fn begin(self: &Arc<Self>, boundary: Boundary, expected: Option<u64>) -> Result<Permit, Refused> {
        loop {
            match self.try_begin(boundary, expected)? {
                Ok(permit) => return Ok(permit),
                Err(running) => {
                    if self.runs_on_this_thread() { return Err(Refused::Joined); }
                    let mut changes = self.changed.subscribe();
                    let _ = changes.wait_for(|published| published.closed || published.transition.is_none()).await;
                    if matches!((running, boundary), (Boundary::SessionEnd(_), Boundary::SessionEnd(_))) { return Err(Refused::Joined); }
                }
            }
        }
    }

    /// Like `begin` for a caller that is not async. It blocks its thread, so no lease holder may depend on that thread.
    pub(crate) fn begin_blocking(self: &Arc<Self>, boundary: Boundary, expected: Option<u64>) -> Result<Permit, Refused> {
        loop {
            match self.try_begin(boundary, expected)? {
                Ok(permit) => {
                    permit.run_on_this_thread();
                    return Ok(permit);
                }
                Err(running) => {
                    if self.runs_on_this_thread() { return Err(Refused::Joined); }
                    let mut state = crate::db::locked(&self.state);
                    while state.snapshot().transition.is_some() && !state.snapshot().closed { state = crate::db::lock::wait(&self.idle, state); }
                    drop(state);
                    if matches!((running, boundary), (Boundary::SessionEnd(_), Boundary::SessionEnd(_))) { return Err(Refused::Joined); }
                }
            }
        }
    }

    /// Starts a session when the identity changed. A caller that carries the epoch it was admitted in changes nothing once that epoch has gone. Returns whether the session now holds.
    pub(crate) fn start_session(self: &Arc<Self>, session: Option<String>, expected: u64) -> bool {
        {
            let state = crate::db::locked(&self.state);
            let current = state.snapshot();
            if current.closed || current.session_epoch != expected { return false; }
            if current.session == session && (!current.ended || session.is_none()) { return current.session.is_some(); }
            if session.is_none() && current.ended { return false; }
        }
        match self.try_begin(Boundary::SessionStart, Some(expected)) {
            Ok(Ok(permit)) => {
                permit.defer(Change::SessionStarted(session));
                permit.finish();
                true
            }
            _ => false,
        }
    }

    fn deliver(&self, event: Event, with_holders: bool) -> (Generation, u64) {
        let mut state = crate::db::locked(&self.state);
        state.machine.handle(&event);
        let snapshot = state.snapshot().clone();
        let generation = snapshot.generation;
        let holders = if with_holders { state.holders.open() } else { Vec::new() };
        #[cfg(test)]
        self.work.fetch_add(holders.len(), std::sync::atomic::Ordering::SeqCst);
        if snapshot.closed { state.holders.clear(); }
        drop(state);
        (generation, self.notify(snapshot, holders))
    }

    fn notify(&self, snapshot: Snapshot, holders: Vec<Weak<dyn Holder>>) -> u64 {
        let changed = self.changed.clone();
        let mut effects = Effects::default();
        effects.push(move || {
            for holder in holders.into_iter().filter_map(|holder| holder.upgrade()) {
                if snapshot.closed { holder.close(); } else { holder.changed(&snapshot); }
            }
            crate::db::lock::effect(|| {
                changed.send_if_modified(|current| {
                    if snapshot.revision <= current.revision { return false; }
                    *current = snapshot;
                    true
                });
            });
        });
        self.effects.push_ticket(effects)
    }

    fn finish_transition(&self, deferred: Vec<Event>) {
        let mut tickets = Vec::new();
        let finished;
        let activity = {
            let mut state = crate::db::locked(&self.state);
            state.machine.handle(&Event::Finish);
            finished = state.snapshot().clone();
            state.runner = None;
            for event in deferred {
                if state.snapshot().closed { break; }
                state.machine.handle(&event);
                let snapshot = state.snapshot().clone();
                let holders = state.holders.open();
                #[cfg(test)]
                self.work.fetch_add(holders.len(), std::sync::atomic::Ordering::SeqCst);
                tickets.push((snapshot, holders));
            }
            state.activity()
        };
        self.idle.notify_all();
        self.publish_activity(activity);
        self.publish(finished);
        let last = tickets.into_iter().map(|(snapshot, holders)| self.notify(snapshot, holders)).last();
        if let Some(ticket) = last { self.effects.run_until(ticket); }
    }

    /// Returns when the change with this ticket reached its holders.
    pub(crate) fn delivered(&self, ticket: u64) -> bool {
        self.effects.run_until(ticket)
    }

    /// Registers a holder and returns the state at that moment. Every change after it reaches the holder.
    pub(crate) fn enroll<M: Holder + 'static>(self: &Arc<Self>, holder: &Arc<M>) -> Snapshot {
        assert!(holder.registration().get().is_none());
        let mut state = crate::db::locked(&self.state);
        let snapshot = state.snapshot().clone();
        if snapshot.closed {
            drop(state);
            holder.close();
            return snapshot;
        }
        let id = Arc::as_ptr(holder) as usize;
        assert!(holder.registration().set(Registration { owner: Arc::downgrade(self), id }).is_ok());
        state.holders.insert(id, Arc::downgrade(holder) as Weak<dyn Holder>);
        snapshot
    }

    /// Closes the database: no lease is issued again, every held lease ends, and every holder closes. Returns after every holder closed, also when another thread made the first close. False when a holder's own close called it again: that call cannot wait for itself, and the outer close finishes the work.
    pub(crate) fn close(&self) -> bool {
        let line = crate::db::locked(&self.commit_line);
        let ticket = {
            let mut state = crate::db::locked(&self.state);
            if state.snapshot().closed {
                drop(state);
                drop(line);
                return self.effects.run_until(self.effects.last_ticket());
            }
            state.machine.handle(&Event::Close);
            state.leases.retain(|_, info| !info.passive);
            let snapshot = state.snapshot().clone();
            let holders = state.holders.open();
            #[cfg(test)]
            self.work.fetch_add(holders.len(), std::sync::atomic::Ordering::SeqCst);
            state.holders.clear();
            let activity = state.activity();
            drop(state);
            drop(line);
            self.idle.notify_all();
            self.publish(snapshot.clone());
            self.publish_activity(activity);
            self.notify(snapshot, holders)
        };
        self.effects.run_until(ticket)
    }

    #[cfg(test)]
    pub(crate) fn enrolled(&self) -> usize { crate::db::locked(&self.state).holders.len() }

    #[cfg(test)]
    pub(crate) fn work(&self) -> usize { self.work.load(std::sync::atomic::Ordering::SeqCst) }
}
