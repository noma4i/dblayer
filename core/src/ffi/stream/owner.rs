use std::sync::{Arc, Mutex, OnceLock, Weak};

use statig::blocking::InitializedStateMachine;

use crate::db::dispatch::OrderedEffects;
use crate::db::locked;
use crate::lifecycle::{Admitted, Change, Holder, Lifecycle, Origin, Registration, Snapshot, Stamp};
use crate::sync::{FirstScreenHold, RelationState};
use super::machine::{self, Context, Facts, Fixed, Input};
use super::open::Handles;
use super::{Frame, Request, StreamDelivery, StreamEnd, StreamTable, StreamValue, ValueSink};
use crate::ffi::{DbError, WireModelEvent};

/// The one owner of a reader's stream in the core: its machine keeps the state of the read and the journal the reader reads, and it ends with the epoch it was opened in.
pub(in crate::ffi) struct Stream {
    pub(super) id: u64,
    pub(super) epoch: u64,
    table: Weak<StreamTable>,
    machine: Mutex<InitializedStateMachine<Facts>>,
    outbox: OrderedEffects,
    fixed: Fixed,
    first_screen: Mutex<Option<FirstScreenHold>>,
    handles: Mutex<Option<Handles>>,
    lease: Mutex<Option<Admitted>>,
    registration: OnceLock<Registration>,
    pub(super) request: Request,
}

impl Drop for Stream {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("Stream::drop"); }
}

fn interrupted() -> DbError { DbError::Interrupted { detail: "initial load cancelled".into() } }

impl Stream {
    pub(in crate::ffi) fn new(id: u64, epoch: u64, life: Arc<Lifecycle>, table: &Arc<StreamTable>, sink: Arc<dyn ValueSink>, request: Request) -> Arc<Self> {
        let fixed = Fixed { life, calls: Arc::new(crate::db::CallState::new()) };
        let machine = machine::machine(sink, &fixed);
        Arc::new(Self {
            id, epoch, table: Arc::downgrade(table), machine: Mutex::new(machine), outbox: OrderedEffects::default(), fixed, first_screen: Mutex::new(None), handles: Mutex::new(None), lease: Mutex::new(None),
            registration: OnceLock::new(), request,
        })
    }

    pub(super) fn admitted(&self) -> Result<Admitted, DbError> {
        self.fixed.life.admit_passive(Origin::Explicit, "stream work", Some(self.epoch)).map_err(|_| interrupted())
    }

    pub(super) fn holds(&self, snapshot: &Snapshot) -> bool { !snapshot.closed && snapshot.session_epoch == self.epoch }

    pub(super) fn attach(&self, handles: Handles) {
        *locked(&self.handles) = Some(handles);
        if machine::ended(&locked(&self.machine)) { self.release(); }
    }

    pub(super) fn hold_first_screen(&self, hold: FirstScreenHold) {
        let replaced = locked(&self.first_screen).replace(hold);
        drop(replaced);
    }

    /// Runs one input of the machine. What it queues for outside runs after every library lock is released, in the order of the inputs.
    fn handle(&self, input: Input) {
        self.step(&input);
        drop(input);
    }

    fn step(&self, input: &Input) {
        let ended = self.outbox.under(&self.machine, |machine, effects| {
            let mut context = Context { effects, fixed: &self.fixed };
            machine.handle_with_context(input, &mut context);
            machine::ended(machine)
        });
        self.keep_lease();
        if ended { self.release(); }
    }

    fn configure(&self, setup: impl FnOnce(&mut Facts) + Send + 'static) { self.handle(Input::configure(setup)); }

    /// The stream holds a lease while it owes its reader a value. The lease is made and dropped outside every lock, and the loop repeats until the slot matches the need.
    fn keep_lease(&self) {
        loop {
            let owes = machine::owes(&locked(&self.machine));
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
                let made = self.fixed.life.admit_session_at(Origin::Session, "stream", self.epoch).ok();
                changed |= made.is_some();
                let extra = { let mut slot = locked(&self.lease); if slot.is_none() { *slot = made; None } else { made } };
                drop(extra);
            }
            if !changed { return; }
        }
    }

    /// Ends the stream with its one outcome. The reader still reads what the journal holds, then the end.
    pub(super) fn end(&self, reason: StreamEnd) { self.handle(Input::End(reason)); }

    /// The reader asked to stop. Nothing more reaches it, and the call does not wait for the reader: a reader may hold a lock that the canceller holds.
    pub(super) fn cancelled(&self) {
        self.fixed.calls.cancel();
        self.handle(Input::Cancel);
        self.forget();
    }

    /// The next value of the journal. The end comes once, after every other value, and the stream leaves the table with it.
    pub(super) fn read(&self) -> Option<StreamDelivery> {
        let input = Input::read(self.fixed.life.admits_epoch(self.epoch));
        self.step(&input);
        let Input::Read { taken, done, .. } = input else { return None };
        if done.get() { self.forget(); }
        let (seq, value) = taken.take()?;
        let hold = locked(&self.first_screen).take();
        if let Some(hold) = hold { hold.delivered(); }
        Some(StreamDelivery { stream: self.id, epoch: self.epoch, seq, value })
    }

    fn release(&self) {
        if let Some(registration) = self.registration.get() { registration.remove(); }
        let handles = locked(&self.handles).take();
        let hold = locked(&self.first_screen).take();
        drop((handles, hold));
    }

    fn forget(&self) {
        let open = self.table.upgrade().and_then(|table| table.remove(self.id));
        drop(open);
    }

    /// Fails the stream before it has a value: one failed state, then the end.
    pub(super) fn invalid(&self, error: DbError) { self.handle(Input::Invalid(error)); }

    /// The source of a stream that carries no list is open: the reader hears one settled state.
    pub(super) fn ready(&self) {
        if !self.live() { return; }
        self.handle(Input::Opened);
    }

    /// A value that is not a part of a list: the status of an upload.
    pub(super) fn push(&self, value: StreamValue) {
        if !self.live() { return; }
        self.handle(Input::value(value));
    }

    /// An event of the model, with the state of the database when its source event was received.
    pub(super) fn heard(&self, event: WireModelEvent, stamp: Stamp) { self.handle(Input::heard(event, stamp)); }

    /// An event that could not be read: its position is lost.
    pub(super) fn unread(&self) { self.handle(Input::Unread); }

    #[cfg(test)]
    pub(super) fn probe(&self) -> (bool, bool) {
        let handles = locked(&self.handles);
        handles.as_ref().map_or((false, false), |handles| (handles.watches(), handles.loads()))
    }

    /// Whether the epoch of the stream is still the one now. A stream that finds it gone ends with its one outcome and produces nothing.
    fn live(&self) -> bool {
        let snapshot = self.fixed.life.snapshot();
        if self.holds(&snapshot) { return true; }
        self.end(if snapshot.closed { StreamEnd::Closed } else { StreamEnd::SessionEnded });
        false
    }

    pub(in crate::ffi) fn rows(&self, part: Frame) {
        if !self.live() { return; }
        self.handle(Input::rows(part));
    }

    pub(super) fn count(&self, part: Frame) {
        if !self.live() { return; }
        self.handle(Input::count(part));
    }

    pub(super) fn reached(&self, revision: i64) {
        if !self.live() { return; }
        self.handle(Input::Reached(revision));
    }

    pub(super) fn read_failed(&self, detail: String) {
        if !self.live() { return; }
        self.handle(Input::ReadFailed(detail));
    }

    /// The state the key held before this window opened. A failure of an earlier load is not the failure of a load that has not begun, so a window that will load itself does not show it.
    pub(super) fn seed(&self, relation: RelationState) {
        if !self.live() { return; }
        self.handle(Input::Seed(relation));
    }

    pub(super) fn relation_changed(&self, relation: RelationState) {
        if !self.live() { return; }
        self.handle(Input::Relation(relation));
    }

    /// Asks the observed group to tell this stream through `reached` once it has read through a revision the relation state is ahead of.
    pub(super) fn on_ahead(&self, ask: Arc<dyn Fn(i64) + Send + Sync>) { self.configure(move |facts| facts.reach_request = Some(ask)); }

    /// Sets how to make the observer deliver the whole result again.
    pub(super) fn on_rejoin(&self, rejoin: Arc<dyn Fn() + Send + Sync>) { self.configure(move |facts| facts.rejoin = Some(rejoin)); }

    pub(super) fn show_targets_only(&self) { self.configure(|facts| facts.targets_only = true); }

    pub(super) fn fetches_entity(&self) { self.configure(|facts| facts.entity = true); }

    /// Runs after a reset of the model cleared the row: the entity asks the server again.
    pub(super) fn on_cleared(&self, again: Arc<dyn Fn() + Send + Sync>) { self.configure(move |facts| facts.on_cleared = Some(again)); }

    /// The lifecycle or the owner of the reads ended the stream: the reason is the one the lifecycle shows.
    pub(super) fn ended_outside(&self) {
        let snapshot = self.fixed.life.snapshot();
        self.end(if snapshot.closed { StreamEnd::Closed } else { StreamEnd::SessionEnded });
    }

    pub(super) fn expect_load(&self) { self.configure(|facts| facts.load_pending = true); }

    pub(super) fn load_of(&self, model: String) { self.configure(move |facts| facts.load_model = Some(model)); }

    /// A load of the stream begins: the state the reader holds is not settled until it ends.
    pub(super) fn reset_load(&self) { self.handle(Input::LoadStarted); }

    pub(super) fn load_finished(&self, error: Option<DbError>) {
        if !self.live() { return; }
        self.handle(Input::LoadFinished(error));
    }

    /// The load of an entity landed its row at `revision`: the state settles once the reader of the row has the frame of that revision.
    pub(super) fn entity_loaded(&self, error: Option<DbError>, revision: i64) {
        if !self.live() { return; }
        self.handle(Input::EntityLoaded(error, revision));
    }

    pub(super) fn subscription_failed(&self, error: Option<DbError>) {
        if !self.live() { return; }
        self.handle(Input::SubscriptionFailed(error));
    }

    /// The reader waits for the rows of a revision of the database: the observer reports when it read through it, also when no row of the stream changed.
    pub(super) fn want(&self, revision: i64) { self.handle(Input::Want(revision)); }

    /// The reader could not apply the last delta. The observer sends the whole list again, and the stream ends once that did not help twice in a row.
    pub(super) fn resync(&self) { self.handle(Input::Resync); }
}

impl Holder for Stream {
    fn close(&self) { self.end(StreamEnd::Closed); }
    fn registration(&self) -> &OnceLock<Registration> { &self.registration }
    fn changed(&self, snapshot: &Snapshot) {
        if !self.holds(snapshot) {
            self.end(if snapshot.closed { StreamEnd::Closed } else { StreamEnd::SessionEnded });
            return;
        }
        let cleared = matches!(snapshot.change, Change::Cleared(_));
        if !cleared { return; }
        let (concerned, again) = {
            let machine = locked(&self.machine);
            if machine::ended(&machine) { return; }
            (machine.load_model.as_deref().is_some_and(|model| snapshot.rereads(model)), machine.on_cleared.clone().filter(|_| snapshot.rereads(&self.request.model)))
        };
        if snapshot.rereads(&self.request.model) { self.handle(Input::DataGone); }
        if concerned || again.is_some() { self.reset_load(); }
        if let Some(again) = again { again(); }
    }
}
