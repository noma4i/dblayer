use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use tokio::sync::watch;

use crate::db::dispatch::{Effects, OrderedEffects};
use crate::db::locked;
use crate::lifecycle::{Change, Holder, Lease, Lifecycle, Origin, Registration, Snapshot};
use crate::sync::{snapshot_phase, FirstScreenHold, LoadError, LoadPhase, RelationState};
use super::open::Handles;
use super::{Frame, LivePhase, Request, StreamDelivery, StreamEnd, StreamState, StreamTable, StreamValue, ValueSink};
use crate::ffi::DbError;

const MAX_RESYNCS_IN_ROW: u32 = 2;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Readiness {
    Waiting,
    Ready,
    Ended,
}

#[derive(Clone, PartialEq)]
struct Shown {
    state: StreamState,
    size: u32,
    count: Option<i64>,
}

struct State {
    seq: u64,
    revision: Option<i64>,
    ended: bool,
    sink: Option<Arc<dyn ValueSink>>,
    opened: bool,
    rows_at: i64,
    size: u32,
    count: Option<i64>,
    read_error: Option<String>,
    relation: Option<RelationState>,
    applied: Option<RelationState>,
    subscription_error: Option<DbError>,
    load_pending: bool,
    load_error: Option<DbError>,
    reach_request: Option<Arc<dyn Fn(i64) + Send + Sync>>,
    ready_pending: bool,
    shown: Option<Shown>,
    targets_only: bool,
    rejoin: Option<Arc<dyn Fn() + Send + Sync>>,
    awaiting_whole: bool,
    resyncs_in_row: u32,
    unreported: u32,
    load_model: Option<String>,
    entity: bool,
    on_cleared: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// The one owner of a reader's stream in the core: it keeps the state of the read, turns observer reads into values and ends with the epoch it was opened in.
pub(in crate::ffi) struct Stream {
    pub(super) id: u64,
    pub(super) epoch: u64,
    life: Arc<Lifecycle>,
    table: Weak<StreamTable>,
    state: Mutex<State>,
    outbox: OrderedEffects,
    readiness: Arc<watch::Sender<Readiness>>,
    first_screen: Arc<Mutex<Option<FirstScreenHold>>>,
    calls: Arc<crate::db::CallState>,
    revoked: Arc<AtomicBool>,
    handles: Mutex<Option<Handles>>,
    lease: Mutex<Option<Lease>>,
    registration: OnceLock<Registration>,
    pub(super) request: Request,
}

impl Drop for State {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("State::drop"); }
}

impl Drop for Stream {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("Stream::drop"); }
}

fn load_failure(error: &LoadError) -> DbError {
    if let Some(cause) = &error.cause { return DbError::from(cause.clone()); }
    match error.kind {
        "network" => DbError::Network { detail: error.message.clone() },
        "database" => DbError::Database { detail: error.message.clone() },
        _ => DbError::Sync { detail: error.message.clone() },
    }
}

fn live_phase(phase: LoadPhase) -> LivePhase {
    match phase {
        LoadPhase::NotLoaded => LivePhase::NotLoaded,
        LoadPhase::Loading => LivePhase::Loading,
        LoadPhase::Loaded => LivePhase::Loaded,
        LoadPhase::Empty => LivePhase::Empty,
        LoadPhase::Failed => LivePhase::Failed,
        LoadPhase::Closed => LivePhase::Closed,
    }
}

fn interrupted() -> DbError { DbError::Interrupted { detail: "initial load cancelled".into() } }

impl Stream {
    pub(in crate::ffi) fn new(id: u64, epoch: u64, life: Arc<Lifecycle>, table: &Arc<StreamTable>, sink: Arc<dyn ValueSink>, request: Request) -> Arc<Self> {
        let state = State {
            seq: 0, revision: None, ended: false, sink: Some(sink), opened: false, rows_at: -1, size: 0, count: None, read_error: None, relation: None, applied: None, subscription_error: None,
            load_pending: false, load_error: None, reach_request: None, ready_pending: false, shown: None, targets_only: false, rejoin: None, awaiting_whole: false, resyncs_in_row: 0, unreported: 0,
            load_model: None, entity: false, on_cleared: None,
        };
        Arc::new(Self {
            id, epoch, life, table: Arc::downgrade(table), state: Mutex::new(state), outbox: OrderedEffects::default(), readiness: Arc::new(watch::channel(Readiness::Waiting).0),
            first_screen: Arc::default(), calls: Arc::new(crate::db::CallState::new()), revoked: Arc::default(), handles: Mutex::new(None), lease: Mutex::new(None), registration: OnceLock::new(), request,
        })
    }

    pub(super) fn holds(&self, snapshot: &Snapshot) -> bool { !snapshot.closed && snapshot.session_epoch == self.epoch }

    pub(super) fn attach(&self, handles: Handles) {
        *locked(&self.handles) = Some(handles);
        if locked(&self.state).ended { self.release(); }
    }

    pub(super) fn hold_first_screen(&self, hold: FirstScreenHold) {
        let replaced = locked(&self.first_screen).replace(hold);
        drop(replaced);
    }

    /// Runs one change of the state. Deliveries it queues reach the reader after every library lock is released, in the order of the changes.
    fn change<R>(&self, apply: impl FnOnce(&mut State, &mut Effects) -> R) -> R {
        let result = self.outbox.under(&self.state, apply);
        self.keep_lease();
        result
    }

    /// The stream holds a lease while it owes its reader a value: before the first one, while a load runs, and while a whole snapshot is awaited. The lease is made and dropped outside every lock, and the loop repeats until the slot matches the need.
    fn keep_lease(&self) {
        loop {
            let owes = { let state = locked(&self.state); !state.ended && (!state.opened || state.load_pending || state.awaiting_whole) };
            let (gone, make) = {
                let mut slot = locked(&self.lease);
                let mut gone = None;
                if slot.as_ref().is_some_and(|held| !held.is_current()) { gone = slot.take(); }
                match (owes, slot.is_some()) {
                    (true, false) => (gone, true),
                    (false, true) => (slot.take(), false),
                    _ => (gone, false),
                }
            };
            let mut changed = gone.is_some();
            drop(gone);
            if make {
                let made = self.life.lease_passive(Origin::Session, "stream").ok();
                changed |= made.is_some();
                let extra = { let mut slot = locked(&self.lease); if slot.is_none() { *slot = made; None } else { made } };
                drop(extra);
            }
            if !changed { return; }
        }
    }

    fn queue(&self, state: &mut State, effects: &mut Effects, value: StreamValue, last: bool) {
        if state.ended { return; }
        state.seq += 1;
        let Some(sink) = state.sink.clone() else { return };
        let delivery = StreamDelivery { stream: self.id, epoch: self.epoch, seq: state.seq, value };
        if last { state.ended = true; state.sink = None; }
        let (calls, revoked, hold, life, epoch) = (self.calls.clone(), self.revoked.clone(), self.first_screen.clone(), self.life.clone(), self.epoch);
        effects.push(move || {
            if !last && (revoked.load(Ordering::SeqCst) || !life.admits_epoch(epoch)) { return; }
            crate::db::lock::assert_no_library_lock("ValueSink::delivered");
            if !calls.call(|| sink.delivered(delivery)) { return; }
            let held = locked(&hold).take();
            if let Some(held) = held { held.delivered(); }
        });
    }

    fn signal_ready(&self, effects: &mut Effects) {
        let readiness = self.readiness.clone();
        effects.push(move || {
            crate::db::lock::effect(|| readiness.send_if_modified(|current| {
                let waiting = *current == Readiness::Waiting;
                if waiting { *current = Readiness::Ready; }
                waiting
            }));
        });
    }

    fn finish(&self, state: &mut State, effects: &mut Effects, reason: StreamEnd) {
        if state.ended { return; }
        if matches!(reason, StreamEnd::SessionEnded | StreamEnd::Closed) { self.revoked.store(true, Ordering::SeqCst); }
        let table = self.table.clone();
        let id = self.id;
        effects.push(move || { let open = table.upgrade().and_then(|table| table.remove(id)); drop(open); });
        self.queue(state, effects, StreamValue::Ended { reason }, true);
        state.ended = true;
        let readiness = self.readiness.clone();
        effects.push(move || { crate::db::lock::effect(|| readiness.send_replace(Readiness::Ended)); });
    }

    /// Ends the stream with its one outcome and frees what it holds.
    pub(super) fn end(&self, reason: StreamEnd) {
        self.change(|state, effects| self.finish(state, effects, reason));
        self.release();
    }

    /// The reader asked to stop. A delivery admitted before this call may still run to its end, and none is admitted after it. The call does not wait for the reader: a reader may hold a lock that the canceller holds.
    pub(super) fn cancelled(&self) {
        self.calls.cancel();
        self.change(|state, effects| {
            state.ended = true;
            let sink = state.sink.take();
            effects.push(move || drop(sink));
            let readiness = self.readiness.clone();
            effects.push(move || { crate::db::lock::effect(|| readiness.send_replace(Readiness::Ended)); });
        });
        self.release();
    }

    fn release(&self) {
        let open = self.table.upgrade().and_then(|table| table.remove(self.id));
        if let Some(registration) = self.registration.get() { registration.remove(); }
        let handles = locked(&self.handles).take();
        let hold = locked(&self.first_screen).take();
        drop((open, handles, hold));
    }

    /// Fails the stream before it has a value: one failed state, then the end.
    pub(super) fn invalid(&self, error: DbError) {
        self.change(|state, effects| {
            state.opened = true;
            let failed = StreamState { phase: LivePhase::Failed, initial: false, refreshing: false, fetching_more: false, has_more: false, complete: false, total: None, loaded_at: None, error: Some(error), error_kind: Some("invalid".into()) };
            self.queue(state, effects, StreamValue::State { state: failed }, false);
            self.finish(state, effects, StreamEnd::Invalid);
        });
        self.release();
    }

    /// The source of a stream that carries no list is open: a wait for its first load ends.
    pub(super) fn ready(&self) {
        if !self.live() { return; }
        self.change(|state, effects| {
            state.opened = true;
            self.signal_ready(effects);
        });
    }

    /// A value that is not a part of a list: an event, a count of dropped events, the status of an upload.
    pub(super) fn push(&self, value: StreamValue) {
        if !self.live() { return; }
        self.change(|state, effects| {
            state.opened = true;
            self.queue(state, effects, value, false);
            self.signal_ready(effects);
        });
    }

    #[cfg(test)]
    pub(super) fn probe(&self) -> (bool, bool) {
        let handles = locked(&self.handles);
        handles.as_ref().map_or((false, false), |handles| (handles.watches(), handles.loads()))
    }

    /// Whether the epoch of the stream is still the one now. A stream that finds it gone ends with its one outcome and produces nothing.
    fn live(&self) -> bool {
        let snapshot = self.life.snapshot();
        if self.holds(&snapshot) { return true; }
        self.end(if snapshot.closed { StreamEnd::Closed } else { StreamEnd::SessionEnded });
        false
    }

    pub(in crate::ffi) fn rows(&self, part: Frame) {
        if !self.live() { return; }
        let orphan = self.change(|state, effects| {
            if state.awaiting_whole && !part.full { return false; }
            if !part.full && state.revision.is_none() { return true; }
            state.awaiting_whole = false;
            state.opened = true;
            state.read_error = None;
            state.rows_at = state.rows_at.max(part.revision);
            state.size = part.size;
            self.compose(state, effects, Some(part));
            false
        });
        if orphan { self.resync(); }
    }

    pub(super) fn count(&self, part: Frame) {
        if !self.live() { return; }
        self.change(|state, effects| {
            state.opened = true;
            state.read_error = None;
            state.rows_at = state.rows_at.max(part.revision);
            state.count = part.count;
            self.compose(state, effects, Some(part));
        });
    }

    pub(super) fn reached(&self, revision: i64) {
        if !self.live() { return; }
        self.change(|state, effects| {
            state.read_error = None;
            state.rows_at = state.rows_at.max(revision);
            self.compose(state, effects, None);
        });
    }

    pub(super) fn read_failed(&self, detail: String) {
        if !self.live() { return; }
        self.change(|state, effects| {
            state.opened = true;
            state.read_error = Some(detail);
            self.compose(state, effects, None);
        });
    }

    /// The state the key held before this window opened. A failure of an earlier load is not the failure of a load that has not begun, so a window that will load itself does not show it.
    pub(super) fn seed(&self, mut relation: RelationState) {
        if locked(&self.state).load_pending && relation.error.take().is_some() && relation.phase == LoadPhase::Failed {
            relation.phase = LoadPhase::NotLoaded;
        }
        self.relation_changed(relation);
    }

    pub(super) fn relation_changed(&self, relation: RelationState) {
        if !self.live() { return; }
        let ask = self.change(|state, effects| {
            if !state.load_pending && relation.error.is_none() && matches!(relation.phase, LoadPhase::Loaded | LoadPhase::Empty) {
                state.load_error = None;
            }
            let wanted = (relation.revision > state.rows_at).then_some(relation.revision);
            state.relation = Some(relation);
            let request = wanted.zip(state.reach_request.clone());
            self.compose(state, effects, None);
            request
        });
        if let Some((revision, ask)) = ask { ask(revision); }
    }

    /// Asks the observed group to tell this stream through `reached` once it has read through a revision the relation state is ahead of.
    pub(super) fn on_ahead(&self, ask: Arc<dyn Fn(i64) + Send + Sync>) {
        let replaced = locked(&self.state).reach_request.replace(ask);
        drop(replaced);
    }

    /// Sets how to make the observer deliver the whole result again.
    pub(super) fn on_rejoin(&self, rejoin: Arc<dyn Fn() + Send + Sync>) {
        let replaced = locked(&self.state).rejoin.replace(rejoin);
        drop(replaced);
    }

    pub(super) fn show_targets_only(&self) { locked(&self.state).targets_only = true; }

    pub(super) fn fetches_entity(&self) { locked(&self.state).entity = true; }

    /// Runs after a reset of the model cleared the row: the entity asks the server again.
    pub(super) fn on_cleared(&self, again: Arc<dyn Fn() + Send + Sync>) { locked(&self.state).on_cleared = Some(again); }

    /// The lifecycle or the owner of the reads ended the stream: the reason is the one the lifecycle shows.
    pub(super) fn ended_outside(&self) {
        let snapshot = self.life.snapshot();
        self.end(if snapshot.closed { StreamEnd::Closed } else { StreamEnd::SessionEnded });
    }

    pub(super) fn expect_load(&self) { locked(&self.state).load_pending = true; }

    pub(super) fn load_of(&self, model: String) { locked(&self.state).load_model = Some(model); }

    pub(super) fn reset_load(&self) {
        self.change(|state, effects| {
            if state.ended { return; }
            state.load_pending = true;
            state.load_error = None;
            state.ready_pending = false;
            let readiness = self.readiness.clone();
            effects.push(move || {
                crate::db::lock::effect(|| readiness.send_if_modified(|current| {
                    if *current == Readiness::Ended { return false; }
                    *current = Readiness::Waiting;
                    true
                }));
            });
        });
    }

    pub(super) fn load_finished(&self, error: Option<DbError>) {
        if !self.live() { return; }
        self.change(|state, effects| {
            state.load_pending = false;
            state.load_error = error;
            self.compose(state, effects, None);
        });
    }

    pub(super) fn subscription_failed(&self, error: Option<DbError>) {
        if !self.live() { return; }
        self.change(|state, effects| {
            state.subscription_error = error;
            self.compose(state, effects, None);
        });
    }

    /// The reader could not apply the last delta. The observer sends the whole list again, and the stream ends once that did not help twice in a row.
    pub(super) fn resync(&self) {
        let rejoin = self.change(|state, effects| {
            state.unreported = 0;
            if state.ended || state.awaiting_whole { return None; }
            let Some(rejoin) = state.rejoin.clone().filter(|_| state.resyncs_in_row < MAX_RESYNCS_IN_ROW) else {
                let mut failed = state.shown.as_ref().map(|shown| shown.state.clone()).unwrap_or(StreamState { phase: LivePhase::Failed, initial: false, refreshing: false, fetching_more: false, has_more: false, complete: false, total: None, loaded_at: None, error: None, error_kind: None });
                failed.phase = LivePhase::Failed;
                failed.error = Some(DbError::Invalid { detail: "the list is out of sync with the database and a new snapshot did not fix it".into() });
                failed.error_kind = Some("invalid".into());
                self.queue(state, effects, StreamValue::State { state: failed }, false);
                self.finish(state, effects, StreamEnd::OutOfSync);
                return None;
            };
            state.resyncs_in_row += 1;
            state.awaiting_whole = true;
            Some(rejoin)
        });
        match rejoin {
            Some(rejoin) => rejoin(),
            None => if locked(&self.state).ended { self.release(); },
        }
    }

    pub(super) async fn loaded(&self) -> Result<(), DbError> {
        let mut receiver = self.readiness.subscribe();
        let seen = *receiver.wait_for(|readiness| *readiness != Readiness::Waiting).await.map_err(|_| interrupted())?;
        match seen {
            Readiness::Ended => Err(interrupted()),
            _ => locked(&self.state).load_error.clone().map_or(Ok(()), Err),
        }
    }

    fn take_ready(state: &mut State) -> bool { std::mem::take(&mut state.ready_pending) }

    fn compose(&self, state: &mut State, effects: &mut Effects, part: Option<Frame>) {
        if !state.opened || state.ended { return; }
        let rows_at = state.rows_at;
        if let Some(relation) = state.relation.clone().filter(|relation| relation.revision <= rows_at) { state.applied = Some(relation); }
        let gated = state.relation.as_ref().is_some_and(|relation| relation.revision > rows_at);
        let mut effective = state.applied.clone().unwrap_or_else(|| RelationState { complete: false, phase: if state.relation.is_some() { LoadPhase::NotLoaded } else { LoadPhase::Loaded }, initial: false, refreshing: false, fetching_more: false, has_more: false, total: None, error: None, loaded_at: None, revision: 0 });
        if effective.error.is_none() && state.subscription_error.is_some() { effective.error = Some(LoadError::new("subscription", "")); }
        let size = state.size as usize;
        if state.entity {
            if state.load_pending {
                effective.phase = LoadPhase::Loading;
                effective.initial = size == 0;
                effective.refreshing = size > 0;
            } else if state.load_error.is_some() && effective.error.is_none() {
                effective.error = Some(LoadError::new("load", ""));
            }
        }
        let phase = if state.read_error.is_some() && !matches!(effective.phase, LoadPhase::Loading | LoadPhase::Closed) { LoadPhase::Failed } else { snapshot_phase(&effective, size) };
        let error = if let Some(detail) = &state.read_error {
            Some((DbError::Database { detail: detail.clone() }, "database".to_string()))
        } else if let Some(error) = state.applied.as_ref().and_then(|applied| applied.error.as_ref()) {
            Some((load_failure(error), error.kind.to_string()))
        } else if let Some(error) = state.subscription_error.clone() {
            Some((error, "subscription".to_string()))
        } else {
            state.load_error.clone().filter(|error| state.entity || matches!(error, DbError::Network { .. })).map(|error| (error, "load".to_string()))
        };
        let shown = Shown {
            state: StreamState {
                phase: live_phase(phase), initial: effective.initial, refreshing: effective.refreshing, fetching_more: effective.fetching_more, has_more: effective.has_more,
                complete: effective.complete && error.is_none(), total: if state.targets_only { None } else { effective.total }, loaded_at: effective.loaded_at,
                error: error.as_ref().map(|(error, _)| error.clone()), error_kind: error.map(|(_, kind)| kind),
            },
            size: state.size,
            count: state.count,
        };
        let ready = !state.load_pending && !gated;
        state.ready_pending |= ready;
        if part.is_none() && state.shown.as_ref() == Some(&shown) {
            if Self::take_ready(state) { self.signal_ready(effects); }
            return;
        }
        state.shown = Some(shown.clone());
        self.emit(state, effects, part, shown);
        if Self::take_ready(state) { self.signal_ready(effects); }
    }

    fn emit(&self, state: &mut State, effects: &mut Effects, part: Option<Frame>, shown: Shown) {
        let value = match part {
            Some(frame) if frame.count.is_some() => StreamValue::Count { revision: frame.revision, count: frame.count.unwrap_or_default(), state: shown.state },
            Some(frame) if frame.full => {
                state.revision = Some(frame.revision);
                StreamValue::Snapshot { revision: frame.revision, rows: frame.rows, state: shown.state }
            }
            Some(frame) if frame.moves_rows() => {
                let base = state.revision.unwrap_or(frame.revision);
                state.revision = Some(frame.revision);
                state.unreported += 1;
                if state.unreported >= 2 { state.resyncs_in_row = 0; }
                StreamValue::Delta {
                    base, revision: frame.revision, size: frame.size, rows: frame.rows, row_indexes: frame.row_indexes, removed: frame.removed, placed: frame.placed, replaced: frame.replaced, renamed: frame.renamed,
                    state: shown.state,
                }
            }
            _ => StreamValue::State { state: shown.state },
        };
        self.queue(state, effects, value, false);
    }
}

impl Holder for Stream {
    fn close(&self) { self.end(StreamEnd::Closed); }
    fn registration(&self) -> &OnceLock<Registration> { &self.registration }
    fn changed(&self, snapshot: &Snapshot) {
        if !self.holds(snapshot) {
            self.end(if snapshot.closed { StreamEnd::Closed } else { StreamEnd::SessionEnded });
            return;
        }
        let (concerned, again) = {
            let state = locked(&self.state);
            let cleared = !state.ended && matches!(snapshot.change, Change::Cleared(_));
            (cleared && state.load_model.as_deref().is_some_and(|model| snapshot.rereads(model)), state.on_cleared.clone().filter(|_| cleared && snapshot.rereads(&self.request.model)))
        };
        if concerned || again.is_some() { self.reset_load(); }
        if let Some(again) = again { again(); }
    }
}
