//! The leases of the old stack. The owners of `owners/life` decide the epoch, the identity, the generation and the close; this module only
//! follows their effects (`Publish`, `EpochEnded`, `ModelsReset`, `Closing`), hands out the leases of the old modules (`sync/*`, `db/*`, `ffi/stream`, `upload`)
//! and tells their holders what changed. It takes no transition of its own. Sub-direction B and C delete it with the last old module that takes a lease.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use tokio::sync::watch;

use crate::db::dispatch::{Effects, OrderedEffects};
use crate::identity::Generation;
use crate::lifecycle::authority::{Authority, Internal};
use crate::runtime::Cause;

/// What work needs to start: `Explicit` work runs without a session, `Session` work needs one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Origin { Explicit, Session }

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
    /// The epoch ended and the next one is not published yet: no work is admitted.
    pub ending: bool,
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

/// The lease of a call ended because the database closed or its epoch ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Ended;

impl From<Ended> for crate::transport::TransportError {
    fn from(_: Ended) -> Self { crate::transport::TransportError::Interrupted("the database closed, reset or ended its session".into()) }
}

impl Default for Snapshot {
    fn default() -> Self {
        Self { generation: Generation::INITIAL, session: None, ended: false, ending: false, closed: false, revision: 0, notice: 0, session_epoch: 0, change: Change::SessionStarted(None) }
    }
}

impl Snapshot {
    pub(crate) fn signed_in(&self) -> bool { self.session.is_some() && !self.ended }

    pub(crate) fn stamp(&self) -> Stamp { Stamp { generation: self.generation, session_epoch: self.session_epoch } }

    /// Whether work of this kind may start now. An ending epoch admits nothing: the work of the old epoch ended, and the next work waits for the next epoch.
    pub(crate) fn admits(&self, origin: Origin) -> bool {
        !self.closed && !self.ending && match origin {
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
    Rebound,
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
            if epoch_moved { return Some(Invalidated::Rebound); }
        }
    }

    pub(crate) async fn changed(&mut self) -> Option<Snapshot> {
        let seen = self.seen;
        let snapshot = self.state.wait_for(|snapshot| snapshot.notice > seen).await.ok()?.clone();
        self.seen = snapshot.notice;
        Some(snapshot)
    }
}

/// An object that lives as long as the database and must hear its closing and its changes.
pub(crate) trait Holder: Send + Sync {
    fn close(&self);
    fn registration(&self) -> &OnceLock<Registration>;
    fn changed(&self, _snapshot: &Snapshot) {}
    /// Runs before any work of the new epoch is admitted.
    fn session_starting(&self, _session: Option<&str>) {}
}

pub(crate) struct Registration { owner: Weak<Leases>, id: usize }

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) struct Activity {
    pub held: usize,
    pub ending: bool,
    seq: u64,
}

struct State {
    snapshot: Snapshot,
    holders: Holders,
    leases: HashMap<u64, LeaseInfo>,
    next_lease: u64,
    activity: u64,
    /// The epoch ended with a clearing of the account: the next publication tells the holders that the cache is gone.
    cleared_on_publish: bool,
}

impl State {
    fn activity(&mut self) -> Activity {
        self.activity += 1;
        Activity { held: self.leases.values().filter(|lease| !lease.passive).count(), ending: self.snapshot.ending, seq: self.activity }
    }

    fn apply(&mut self, change: Change) {
        self.snapshot.change = change;
        self.snapshot.notice += 1;
        self.snapshot.revision += 1;
    }
}

pub(crate) struct Leases {
    state: Mutex<State>,
    /// Orders a pinned SQL commit against the end of an epoch: the epoch ends while this line is held, and a pinned write checks the epoch and commits while it holds it.
    commit_line: Mutex<()>,
    opening_issued: std::sync::atomic::AtomicBool,
    effects: OrderedEffects,
    changed: watch::Sender<Snapshot>,
    activity: watch::Sender<Activity>,
    #[cfg(test)]
    work: std::sync::atomic::AtomicUsize,
    /// Epoch ends that reached the commit line, for the tests that hold the line.
    #[cfg(test)]
    pub(crate) line_waits: (Mutex<usize>, std::sync::Condvar),
}

impl Default for Leases {
    fn default() -> Self {
        Self {
            state: Mutex::new(State { snapshot: Snapshot::default(), holders: Holders::default(), leases: HashMap::new(), next_lease: 0, activity: 0, cleared_on_publish: false }),
            commit_line: Mutex::new(()),
            opening_issued: std::sync::atomic::AtomicBool::new(false),
            effects: OrderedEffects::default(),
            changed: watch::channel(Snapshot::default()).0,
            activity: watch::channel(Activity::default()).0,
            #[cfg(test)]
            work: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            line_waits: (Mutex::new(0), std::sync::Condvar::new()),
        }
    }
}

/// One piece of work in the database. It is issued before the work is queued and ends with the single outcome of that work. The end of its epoch ends every lease of the old epoch.
#[derive(Clone)]
pub struct Admitted {
    inner: Arc<Admission>,
}

struct Admission {
    life: Arc<Leases>,
    id: u64,
    epoch: u64,
}

impl Admitted {
    pub(crate) fn value(&self) -> u64 { self.inner.epoch }

    pub(crate) fn life(&self) -> &Arc<Leases> { &self.inner.life }

    pub(crate) fn authority(&self) -> Authority { Authority::Admitted(self.clone()) }

    pub(crate) fn scope<F: std::future::Future>(&self, work: F) -> impl std::future::Future<Output = F::Output> {
        crate::task::scope(self.clone(), work)
    }

    pub(crate) fn is_current(&self) -> bool {
        let state = crate::db::locked(&self.inner.life.state);
        let current = &state.snapshot;
        !current.closed && !current.ending && current.session_epoch == self.inner.epoch
    }

    pub(crate) async fn run<T>(&self, work: impl std::future::Future<Output = T>) -> Result<T, Ended> {
        if !self.is_current() { return Err(Ended); }
        let mut changes = self.inner.life.changed.subscribe();
        let epoch = self.inner.epoch;
        tokio::select! {
            biased;
            _ = async { changes.wait_for(|published| published.closed || published.ending || published.session_epoch != epoch).await.map(|_| ()) } => Err(Ended),
            result = self.scope(work) => Ok(result),
        }
    }
}

impl Drop for Admission {
    fn drop(&mut self) { self.life.release(self.id); }
}

impl Leases {
    pub(crate) fn opening(&self) -> Option<Internal> {
        (!self.opening_issued.swap(true, std::sync::atomic::Ordering::SeqCst)).then(Internal::opening)
    }

    pub(crate) fn admit(self: &Arc<Self>, origin: Origin, target: &'static str) -> Result<Admitted, Ended> {
        self.admit_as(origin, target, false, None)
    }

    pub(crate) fn admit_passive(self: &Arc<Self>, origin: Origin, target: &'static str, expected: Option<u64>) -> Result<Admitted, Ended> {
        self.admit_as(origin, target, true, expected)
    }

    pub(crate) fn admit_session_at(self: &Arc<Self>, origin: Origin, target: &'static str, expected: u64) -> Result<Admitted, Ended> {
        self.admit_as(origin, target, false, Some(expected))
    }

    fn admit_as(self: &Arc<Self>, origin: Origin, target: &'static str, passive: bool, expected: Option<u64>) -> Result<Admitted, Ended> {
        let (admitted, activity) = {
            let mut state = crate::db::locked(&self.state);
            if !state.snapshot.admits(origin) { return Err(Ended); }
            if expected.is_some_and(|epoch| epoch != state.snapshot.session_epoch) { return Err(Ended); }
            let (id, epoch) = (state.next_lease, state.snapshot.session_epoch);
            state.next_lease += 1;
            state.leases.insert(id, LeaseInfo { target, passive });
            (Admitted { inner: Arc::new(Admission { life: self.clone(), id, epoch }) }, state.activity())
        };
        self.publish_activity(activity);
        Ok(admitted)
    }

    pub(crate) fn snapshot(&self) -> Snapshot { crate::db::locked(&self.state).snapshot.clone() }

    /// Whether the epoch is the one now and the database is open. It reads the published snapshot and takes no library lock, so the writer may call it.
    pub(crate) fn admits_epoch(&self, epoch: u64) -> bool {
        let published = self.changed.borrow();
        !published.closed && !published.ending && published.session_epoch == epoch
    }

    /// The line a pinned SQL commit holds from its epoch check to the end of the commit. The end of an epoch cannot happen meanwhile.
    pub(crate) fn commit_line(&self) -> crate::db::lock::Guard<'_, ()> { crate::db::locked(&self.commit_line) }

    pub(crate) fn watch(&self) -> Watch {
        let state = self.changed.subscribe();
        let (seen, generation, session_epoch) = { let published = state.borrow(); (published.notice, published.generation, published.session_epoch) };
        Watch { state, seen, generation, session_epoch }
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

    #[cfg(test)]
    pub(crate) fn quiet(&self) -> bool {
        let state = crate::db::locked(&self.state);
        state.leases.values().all(|lease| lease.passive) && !state.snapshot.ending
    }

    /// Waits until work of this kind may start. False when the database closed first.
    pub(crate) async fn admission(&self, origin: Origin) -> bool {
        let mut changes = self.changed.subscribe();
        changes.wait_for(|published| published.closed || published.admits(origin)).await.is_ok_and(|published| !published.closed)
    }

    /// Admits the work a wake starts in the epoch it was woken in. The lease is taken at the call, so a settled wait sees the work. An epoch end that is not a new epoch yet delays the work to the next publication of the same epoch instead of dropping it.
    pub(crate) fn admit_woken(self: &Arc<Self>, origin: Origin, target: &'static str, epoch: u64) -> impl std::future::Future<Output = Option<Admitted>> + Send + 'static {
        let now = self.admit_as(origin, target, false, Some(epoch)).ok();
        let life = self.clone();
        async move {
            match now {
                Some(admitted) => Some(admitted),
                None => life.admit_after_ending(origin, target, epoch).await,
            }
        }
    }

    async fn admit_after_ending(self: Arc<Self>, origin: Origin, target: &'static str, epoch: u64) -> Option<Admitted> {
        let mut changes = self.changed.subscribe();
        loop {
            if let Ok(admitted) = self.admit_as(origin, target, false, Some(epoch)) { return Some(admitted); }
            let waiting = { let published = changes.borrow_and_update(); !published.closed && published.ending && published.session_epoch == epoch };
            if !waiting { return None; }
            changes.changed().await.ok()?;
        }
    }

    /// The work that holds a lease now, for a report that names what a wait is waiting for.
    pub(crate) fn held(&self) -> Vec<String> {
        let state = crate::db::locked(&self.state);
        let mut held: Vec<_> = state.leases.iter().map(|(id, info)| format!("#{id} {}", info.target)).collect();
        held.sort();
        held
    }

    pub(crate) async fn settled(&self) {
        let mut activity = self.activity.subscribe();
        let _ = activity.wait_for(|activity| activity.held == 0 && !activity.ending).await;
    }

    /// Registers a holder and returns the state at that moment. Every change after it reaches the holder.
    pub(crate) fn enroll<M: Holder + 'static>(self: &Arc<Self>, holder: &Arc<M>) -> Snapshot {
        assert!(holder.registration().get().is_none());
        let mut state = crate::db::locked(&self.state);
        let snapshot = state.snapshot.clone();
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

    fn notify(&self, snapshot: Snapshot, holders: Vec<Weak<dyn Holder>>, starting: Option<Option<String>>) -> u64 {
        let changed = self.changed.clone();
        let mut effects = Effects::default();
        effects.push(move || {
            for holder in holders.into_iter().filter_map(|holder| holder.upgrade()) {
                if let Some(session) = &starting { holder.session_starting(session.as_deref()); }
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

    /// `EpochEnded` of Lifecycle: every lease of the epoch ends, nothing is admitted until the next `Publish`. The clearing causes also end the session of the snapshot and move the generation, as `ClearAccount` does in the Store.
    pub(crate) fn epoch_ended(&self, cause: Cause) {
        if cause == Cause::Close { return; }
        #[cfg(test)]
        {
            *self.line_waits.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
            self.line_waits.1.notify_all();
        }
        let line = crate::db::locked(&self.commit_line);
        let (ticket, activity) = {
            let mut state = crate::db::locked(&self.state);
            if state.snapshot.closed { return; }
            state.snapshot.ending = true;
            state.leases.clear();
            let clearing = cause != Cause::SessionStarted;
            if clearing {
                state.snapshot.session = None;
                state.snapshot.ended = true;
                state.snapshot.generation = state.snapshot.generation.next();
                state.cleared_on_publish = true;
                state.apply(Change::SessionEnded);
            } else {
                state.snapshot.revision += 1;
            }
            let snapshot = state.snapshot.clone();
            let holders = if clearing { state.holders.open() } else { Vec::new() };
            #[cfg(test)]
            self.work.fetch_add(holders.len(), std::sync::atomic::Ordering::SeqCst);
            let activity = state.activity();
            drop(line);
            (self.notify(snapshot, holders, None), activity)
        };
        self.publish_activity(activity);
        self.effects.run_until(ticket);
    }

    /// `Publish` of Lifecycle: the epoch, the identity and the generation of the published value. A clearing that ended the previous epoch shows as a cleared cache before the new session starts.
    pub(crate) fn published(&self, published: &crate::owners::contract::Published) {
        if published.closed {
            self.close();
            return;
        }
        self.published_as(published.epoch, published.identity.map(|identity| identity.to_string()), published.generation);
    }

    fn published_as(&self, epoch: u64, session: Option<String>, generation: u64) {
        let mut tickets = Vec::new();
        let activity = {
            let mut state = crate::db::locked(&self.state);
            if state.snapshot.closed { return; }
            let was_ending = std::mem::replace(&mut state.snapshot.ending, false);
            let moved = state.snapshot.session_epoch != epoch || state.snapshot.session != session || was_ending;
            state.snapshot.session_epoch = epoch;
            state.snapshot.generation = Generation::from_value(generation);
            state.snapshot.session = session.clone();
            state.snapshot.ended = false;
            let holders = state.holders.open();
            if std::mem::take(&mut state.cleared_on_publish) {
                state.apply(Change::Cleared(None));
                tickets.push((state.snapshot.clone(), holders.clone(), None));
            }
            if moved {
                state.apply(Change::SessionStarted(session.clone()));
                tickets.push((state.snapshot.clone(), holders, Some(session)));
            } else {
                state.snapshot.revision += 1;
                let snapshot = state.snapshot.clone();
                drop(state);
                self.publish(snapshot);
                return;
            }
            #[cfg(test)]
            self.work.fetch_add(tickets.iter().map(|(_, holders, _)| holders.len()).sum::<usize>(), std::sync::atomic::Ordering::SeqCst);
            state.activity()
        };
        self.publish_activity(activity);
        let last = tickets.into_iter().map(|(snapshot, holders, starting)| self.notify(snapshot, holders, starting)).last();
        if let Some(ticket) = last { self.effects.run_until(ticket); }
    }

    /// `ModelsReset` of Lifecycle: the cache of the models is gone and the generation grew. The epoch stays.
    pub(crate) fn models_reset(&self, generation: u64, models: &[String]) {
        let (ticket, activity) = {
            let mut state = crate::db::locked(&self.state);
            if state.snapshot.closed { return; }
            state.snapshot.generation = Generation::from_value(generation);
            state.apply(Change::Cleared(Some(models.to_vec())));
            let snapshot = state.snapshot.clone();
            let holders = state.holders.open();
            #[cfg(test)]
            self.work.fetch_add(holders.len(), std::sync::atomic::Ordering::SeqCst);
            (self.notify(snapshot, holders, None), state.activity())
        };
        self.publish_activity(activity);
        self.effects.run_until(ticket);
    }

    /// Closes the leases: no lease is issued again, every held lease ends, and every holder closes. Returns after every holder closed, also when another thread made the first close. False when a holder's own close called it again: that call cannot wait for itself, and the outer close finishes the work.
    pub(crate) fn close(&self) -> bool {
        let line = crate::db::locked(&self.commit_line);
        let ticket = {
            let mut state = crate::db::locked(&self.state);
            if state.snapshot.closed {
                drop(state);
                drop(line);
                return self.effects.run_until(self.effects.last_ticket());
            }
            state.snapshot.closed = true;
            state.snapshot.ending = false;
            state.apply(Change::Closed);
            state.leases.retain(|_, info| info.passive);
            let snapshot = state.snapshot.clone();
            let holders = state.holders.open();
            #[cfg(test)]
            self.work.fetch_add(holders.len(), std::sync::atomic::Ordering::SeqCst);
            state.holders.clear();
            let activity = state.activity();
            drop(state);
            drop(line);
            self.publish(snapshot.clone());
            self.publish_activity(activity);
            self.notify(snapshot, holders, None)
        };
        self.effects.run_until(ticket)
    }

    /// What the owners do when the server ends the session, for the tests that run without them.
    #[cfg(test)]
    pub(crate) fn lose_session_in_tests(&self) {
        self.epoch_ended(Cause::ServerEnded);
        let snapshot = self.snapshot();
        self.published_as(snapshot.session_epoch + 1, None, snapshot.generation.value());
    }

    /// What the owners do when a credential commits, for the tests that run without them: the session string is taken as it is. The lease of the caller moves to the new epoch.
    #[cfg(test)]
    pub(crate) fn start_session_in_tests(self: &Arc<Self>, session: Option<String>, admitted: Admitted) -> Result<Admitted, Ended> {
        let snapshot = self.snapshot();
        if snapshot.closed || snapshot.session_epoch != admitted.value() { return Err(Ended); }
        if snapshot.session == session && (!snapshot.ended || session.is_none()) { return Ok(admitted); }
        drop(admitted);
        self.epoch_ended(Cause::SessionStarted);
        self.published_as(snapshot.session_epoch + 1, session, snapshot.generation.value());
        self.admit_as(Origin::Explicit, "test session", false, Some(snapshot.session_epoch + 1))
    }

    #[cfg(test)]
    pub(crate) fn enrolled(&self) -> usize { crate::db::locked(&self.state).holders.len() }

    #[cfg(test)]
    pub(crate) fn work(&self) -> usize { self.work.load(std::sync::atomic::Ordering::SeqCst) }
}

#[cfg(test)]
mod legacy_tests {
    use super::*;

    #[test]
    fn the_opening_token_is_issued_once_per_database() {
        let life = Leases::default();
        assert!(life.opening().is_some());
        assert!(life.opening().is_none());
    }
}
