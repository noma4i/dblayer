use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use statig::prelude::*;
use tokio::sync::{broadcast, watch};

use crate::db::dispatch::{Effects, OrderedEffects};
use crate::identity::Generation;

mod machine;

const EVENT_BACKLOG: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin { Explicit, Observed, Background, Cloud }

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
            Change::EndSession => { self.session = None; self.ended = true; self.session_epoch += 1; }
            Change::Online(online) => self.online = *online,
            Change::Close => self.closed = true,
        }
        self.revision += 1;
        self.change = event.clone();
    }

    pub(crate) fn signed_in(&self) -> bool { self.session.is_some() && !self.ended }

    pub(crate) fn admits(&self, origin: Origin) -> bool {
        !self.closed && match origin {
            Origin::Explicit | Origin::Cloud => true,
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

struct State {
    machine: InitializedStateMachine<machine::Phases>,
    members: HashMap<usize, Weak<dyn Member>>,
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
        Self { state: Mutex::new(State { machine: machine::new(), members: HashMap::new() }), effects: OrderedEffects::default(), changed: watch::channel(Snapshot::default()).0, events: broadcast::channel(EVENT_BACKLOG).0, #[cfg(test)] work: std::sync::atomic::AtomicUsize::new(0) }
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

    pub(crate) async fn request<T>(&self, work: impl std::future::Future<Output = Result<T, crate::transport::TransportError>>) -> Result<T, crate::transport::TransportError> {
        let mut changes = self.changed.subscribe();
        let before = self.snapshot();
        let interrupted = || crate::transport::TransportError::Interrupted("database session ended or database closed".into());
        if before.closed { return Err(interrupted()); }
        tokio::select! {
            biased;
            _ = changes.wait_for(|snapshot| snapshot.closed || snapshot.session_epoch != before.session_epoch) => Err(interrupted()),
            result = work => result,
        }
    }

    pub(crate) fn transition(&self, change: Change) {
        self.transition_deferred(change);
        self.effects.run();
    }

    pub(crate) fn transition_deferred(&self, change: Change) -> Generation {
        let mut state = crate::db::locked(&self.state);
        let previous = &state.machine.inner().snapshot;
        if previous.closed || matches!(&change, Change::Session(session, epoch) if *epoch != previous.session_epoch || previous.session == *session && !previous.ended) {
            return previous.generation;
        }
        state.machine.handle(&change);
        let snapshot = state.machine.inner().snapshot.clone();
        let generation = snapshot.generation;
        let members: Vec<_> = state.members.values().cloned().collect();
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
        self.effects.push(effects);
        generation
    }

    pub(crate) fn busy(&self) -> bool {
        let open: Vec<_> = crate::db::locked(&self.state).members.values().cloned().collect();
        !self.effects.is_idle() || open.into_iter().filter_map(|member| member.upgrade()).any(|member| member.busy())
    }

    pub(crate) fn enroll<M: Member + 'static>(self: &Arc<Self>, member: &Arc<M>) {
        assert!(member.registration().get().is_none());
        let mut state = crate::db::locked(&self.state);
        if state.machine.inner().snapshot.closed {
            drop(state);
            member.close();
            return;
        }
        let id = Arc::as_ptr(member) as usize;
        assert!(member.registration().set(Registration { owner: Arc::downgrade(self), id }).is_ok());
        state.members.insert(id, Arc::downgrade(member) as Weak<dyn Member>);
    }

    pub(crate) fn close(&self) {
        #[cfg(test)]
        self.work.fetch_add(crate::db::locked(&self.state).members.len(), std::sync::atomic::Ordering::SeqCst);
        self.transition(Change::Close);
    }

    #[cfg(test)]
    pub(crate) fn enrolled(&self) -> usize { crate::db::locked(&self.state).members.len() }

    #[cfg(test)]
    pub(crate) fn work(&self) -> usize { self.work.load(std::sync::atomic::Ordering::SeqCst) }
}
