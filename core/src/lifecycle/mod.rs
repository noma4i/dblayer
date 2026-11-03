use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use statig::prelude::*;
use tokio::sync::{broadcast, watch};

use crate::db::dispatch::{Effects, OrderedEffects};
use crate::identity::Generation;

mod machine;

const EVENT_BACKLOG: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin { Explicit, Observed, Background }

#[derive(Clone, Debug)]
pub(crate) enum Change {
    BeginReset,
    Reset(Option<Vec<String>>),
    Session(Option<String>, u64),
    EndSession,
    Online(bool),
    Close,
}

#[derive(Clone, Debug)]
pub(crate) struct Snapshot {
    pub generation: Generation,
    pub session: Option<String>,
    pub ended: bool,
    pub online: bool,
    pub resetting: bool,
    pub closed: bool,
    pub revision: u64,
    pub session_epoch: u64,
    pub change: Change,
}

/// What a call saw when it was admitted. A write that carries an older stamp belongs to a database state that no longer exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Stamp {
    pub generation: Generation,
    pub session_epoch: u64,
}

/// The scope of a call ended because the database closed or its session ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Ended;

impl From<Ended> for crate::transport::TransportError {
    fn from(_: Ended) -> Self { crate::transport::TransportError::Interrupted("database session ended or database closed".into()) }
}

impl From<Ended> for crate::sync::SyncError {
    fn from(ended: Ended) -> Self { crate::sync::SyncError::Transport(ended.into()) }
}

impl Default for Snapshot {
    fn default() -> Self {
        Self { generation: Generation::INITIAL, session: None, ended: false, online: true, resetting: false, closed: false, revision: 0, session_epoch: 0, change: Change::Session(None, 0) }
    }
}

impl Snapshot {
    fn apply(&mut self, event: &Change) {
        match event {
            Change::BeginReset => { self.generation = self.generation.next(); self.resetting = true; }
            Change::Reset(_) => self.resetting = false,
            Change::Session(session, _) => { self.session = session.clone(); self.ended = false; }
            Change::EndSession => { self.session = None; self.ended = true; self.session_epoch += 1; self.generation = self.generation.next(); }
            Change::Online(online) => self.online = *online,
            Change::Close => self.closed = true,
        }
        self.revision += 1;
        self.change = event.clone();
    }

    pub(crate) fn signed_in(&self) -> bool { self.session.is_some() && !self.ended }

    pub(crate) fn stamp(&self) -> Stamp { Stamp { generation: self.generation, session_epoch: self.session_epoch } }

    pub(crate) fn admits(&self, origin: Origin) -> bool {
        !self.closed && match origin {
            Origin::Explicit => true,
            Origin::Observed | Origin::Background => !self.resetting && self.signed_in(),
        }
    }

    pub(crate) fn concerns(&self, model: &str) -> bool {
        match &self.change {
            Change::BeginReset | Change::Online(_) => false,
            Change::Reset(_) => self.rereads(model),
            _ => true,
        }
    }

    pub(crate) fn rereads(&self, model: &str) -> bool {
        match &self.change {
            Change::Reset(models) => models.as_ref().is_none_or(|models| models.iter().any(|name| name == model)),
            Change::Session(Some(_), _) => true,
            _ => false,
        }
    }
}

/// Delivers every lifecycle change in commit order and skips the online flag, which holders follow through connectivity. A receiver that falls behind gets one full reset instead of the lost changes.
pub(crate) struct Watch {
    events: broadcast::Receiver<Snapshot>,
    state: watch::Receiver<Snapshot>,
    seen: u64,
}

impl Watch {
    pub(crate) fn current(&self) -> Snapshot { self.state.borrow().clone() }

    pub(crate) async fn changed(&mut self) -> Option<Snapshot> {
        loop {
            match self.events.recv().await {
                Ok(snapshot) if snapshot.revision <= self.seen => {}
                Ok(snapshot) => {
                    self.seen = snapshot.revision;
                    if !matches!(snapshot.change, Change::Online(_)) { return Some(snapshot); }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    let mut snapshot = self.current();
                    self.seen = snapshot.revision;
                    snapshot.change = Change::Reset(None);
                    return Some(snapshot);
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

pub(crate) trait Member: Send + Sync {
    fn close(&self);
    fn registration(&self) -> &OnceLock<Registration>;
    fn busy(&self) -> bool { false }
    fn lifecycle(&self, _snapshot: &Snapshot) {}
}

pub(crate) struct Registration { owner: Weak<Lifecycle>, id: usize }

impl Registration {
    pub(crate) fn remove(&self) {
        if let Some(owner) = self.owner.upgrade() {
            let removed = crate::db::locked(&owner.state).members.remove(&self.id);
            drop(removed);
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) { self.remove(); }
}

/// Open holders in a vector without gaps, so a walk costs the number of open holders however many were open before.
#[derive(Default)]
struct Members {
    order: Vec<(usize, Weak<dyn Member>)>,
    index: HashMap<usize, usize>,
}

impl Members {
    fn insert(&mut self, id: usize, member: Weak<dyn Member>) {
        self.index.insert(id, self.order.len());
        self.order.push((id, member));
    }

    fn remove(&mut self, id: &usize) -> Option<Weak<dyn Member>> {
        let at = self.index.remove(id)?;
        let (_, removed) = self.order.swap_remove(at);
        if let Some((moved, _)) = self.order.get(at) { self.index.insert(*moved, at); }
        Some(removed)
    }

    fn open(&self) -> Vec<Weak<dyn Member>> { self.order.iter().map(|(_, member)| member.clone()).collect() }

    #[cfg(test)]
    fn len(&self) -> usize { self.order.len() }

    fn clear(&mut self) {
        self.order = Vec::new();
        self.index = HashMap::new();
    }
}

struct State {
    machine: InitializedStateMachine<machine::Phases>,
    members: Members,
}

pub(crate) struct Lifecycle {
    state: Mutex<State>,
    effects: OrderedEffects,
    changed: watch::Sender<Snapshot>,
    events: broadcast::Sender<Snapshot>,
    #[cfg(test)]
    work: std::sync::atomic::AtomicUsize,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self { state: Mutex::new(State { machine: machine::new(), members: Members::default() }), effects: OrderedEffects::default(), changed: watch::channel(Snapshot::default()).0, events: broadcast::channel(EVENT_BACKLOG).0, #[cfg(test)] work: std::sync::atomic::AtomicUsize::new(0) }
    }
}

impl Lifecycle {
    pub(crate) fn snapshot(&self) -> Snapshot { crate::db::locked(&self.state).machine.inner().snapshot.clone() }

    pub(crate) fn watch(&self) -> Watch {
        let events = self.events.subscribe();
        let state = self.changed.subscribe();
        let seen = state.borrow().revision;
        Watch { events, state, seen }
    }

    /// Runs the call inside the scope of the database state it was admitted in. The scope ends the call when the database closes or its session ends, whatever the call waits for: a queue, a retry, a request or an answer.
    pub(crate) async fn scoped<T>(&self, work: impl std::future::Future<Output = T>) -> Result<T, Ended> {
        let mut changes = self.changed.subscribe();
        let before = self.snapshot();
        if before.closed { return Err(Ended); }
        tokio::select! {
            biased;
            _ = changes.wait_for(|published| published.closed || published.session_epoch > before.session_epoch) => Err(Ended),
            result = work => Ok(result),
        }
    }

    pub(crate) async fn request<T>(&self, work: impl std::future::Future<Output = Result<T, crate::transport::TransportError>>) -> Result<T, crate::transport::TransportError> {
        self.scoped(work).await?
    }

    /// Returns after every member ran its callback for this change, unless a callback of this lifecycle made the call.
    pub(crate) fn transition(&self, change: Change) {
        let (_, ticket) = self.transition_ticketed(change);
        self.effects.run_until(ticket);
    }

    pub(crate) fn transition_deferred(&self, change: Change) -> Generation {
        self.transition_ticketed(change).0
    }

    /// Returns when the change with this ticket reached its members.
    pub(crate) fn delivered(&self, ticket: u64) {
        self.effects.run_until(ticket);
    }

    pub(crate) fn transition_ticketed(&self, change: Change) -> (Generation, u64) {
        let mut state = crate::db::locked(&self.state);
        let previous = &state.machine.inner().snapshot;
        if previous.closed || matches!(&change, Change::Session(session, epoch) if *epoch != previous.session_epoch || previous.session == *session && !previous.ended) {
            return (previous.generation, 0);
        }
        state.machine.handle(&change);
        let snapshot = state.machine.inner().snapshot.clone();
        let generation = snapshot.generation;
        let members = state.members.open();
        #[cfg(test)]
        self.work.fetch_add(members.len(), std::sync::atomic::Ordering::SeqCst);
        if snapshot.closed { state.members.clear(); }
        let (changed, events) = (self.changed.clone(), self.events.clone());
        let mut effects = Effects::default();
        effects.push(move || {
            for member in members.into_iter().filter_map(|member| member.upgrade()) {
                if snapshot.closed { member.close(); } else { member.lifecycle(&snapshot); }
            }
            crate::db::lock::effect(|| {
                changed.send_replace(snapshot.clone());
                let _ = events.send(snapshot);
            });
        });
        let ticket = self.effects.push_ticket(effects);
        (generation, ticket)
    }

    pub(crate) fn busy(&self) -> bool {
        let open = crate::db::locked(&self.state).members.open();
        !self.effects.is_idle() || open.into_iter().filter_map(|member| member.upgrade()).any(|member| member.busy())
    }

    /// Registers a holder and returns the state at that moment. Every change after it reaches the holder's callback.
    pub(crate) fn enroll<M: Member + 'static>(self: &Arc<Self>, member: &Arc<M>) -> Snapshot {
        assert!(member.registration().get().is_none());
        let mut state = crate::db::locked(&self.state);
        let snapshot = state.machine.inner().snapshot.clone();
        if snapshot.closed {
            drop(state);
            member.close();
            return snapshot;
        }
        let id = Arc::as_ptr(member) as usize;
        assert!(member.registration().set(Registration { owner: Arc::downgrade(self), id }).is_ok());
        state.members.insert(id, Arc::downgrade(member) as Weak<dyn Member>);
        snapshot
    }

    pub(crate) fn close(&self) {
        self.transition(Change::Close);
    }

    #[cfg(test)]
    pub(crate) fn enrolled(&self) -> usize { crate::db::locked(&self.state).members.len() }

    #[cfg(test)]
    pub(crate) fn work(&self) -> usize { self.work.load(std::sync::atomic::Ordering::SeqCst) }
}
