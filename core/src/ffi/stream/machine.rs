use std::cell::Cell;
use std::sync::Arc;

use statig::prelude::*;

use crate::db::dispatch::Effects;
use crate::lifecycle::{Lifecycle, Stamp};
use crate::sync::{snapshot_phase, LoadError, LoadPhase, RelationState};
use super::journal::{Appended, Journal};
use super::{Frame, LivePhase, StreamEnd, StreamState, StreamValue, ValueSink};
use crate::ffi::{DbError, WireModelEvent};

const MAX_RESYNCS_IN_ROW: u32 = 2;

/// What happened to a stream. Values that a handler takes out ride in a `Cell`, so the machine reads its input by reference.
pub(super) enum Input {
    Rows(Cell<Option<Frame>>),
    Count(Cell<Option<Frame>>),
    Reached(i64),
    ReadFailed(String),
    Relation(RelationState),
    Seed(RelationState),
    LoadStarted,
    LoadFinished(Option<DbError>),
    EntityLoaded(Option<DbError>, i64),
    SubscriptionFailed(Option<DbError>),
    Opened,
    Value(Cell<Option<StreamValue>>),
    Heard(Cell<Option<WireModelEvent>>, Stamp),
    Unread(u64),
    DataGone,
    Resync,
    /// The reader waits for the rows of this revision of the database.
    Want(i64),
    Invalid(DbError),
    End(StreamEnd),
    Cancel,
    /// The reader takes the next value. `admitted` is false once the epoch of the stream has gone: the values of the old account are dropped.
    Read { admitted: bool, taken: Cell<Option<(u64, StreamValue)>>, done: Cell<bool> },
    /// Sets how the stream reads before its first value.
    Configure(Cell<Option<Setup>>),
}

pub(super) type Setup = Box<dyn FnOnce(&mut Facts) + Send>;

impl Input {
    pub(super) fn rows(frame: Frame) -> Self { Self::Rows(Cell::new(Some(frame))) }
    pub(super) fn count(frame: Frame) -> Self { Self::Count(Cell::new(Some(frame))) }
    pub(super) fn value(value: StreamValue) -> Self { Self::Value(Cell::new(Some(value))) }
    pub(super) fn heard(event: WireModelEvent, stamp: Stamp) -> Self { Self::Heard(Cell::new(Some(event)), stamp) }
    pub(super) fn read(admitted: bool) -> Self { Self::Read { admitted, taken: Cell::new(None), done: Cell::new(false) } }
    pub(super) fn configure(setup: impl FnOnce(&mut Facts) + Send + 'static) -> Self { Self::Configure(Cell::new(Some(Box::new(setup)))) }
}

/// What a handler needs from outside the machine: the effects it queues and the parts of the stream that do not change.
pub(super) struct Context<'a> {
    pub(super) effects: &'a mut Effects,
    pub(super) fixed: &'a Fixed,
}

/// The parts of a stream that every state reads.
pub(super) struct Fixed {
    pub(super) life: Arc<Lifecycle>,
    pub(super) calls: Arc<crate::db::CallState>,
}

#[derive(Clone, PartialEq)]
struct Shown {
    state: StreamState,
    size: u32,
    count: Option<i64>,
}

/// The data of one stream: its journal and what the reader was last shown.
pub(super) struct Facts {
    sink: Option<Arc<dyn ValueSink>>,
    pub(super) journal: Journal,
    revision: Option<i64>,
    rows_at: i64,
    landed_at: i64,
    size: u32,
    count: Option<i64>,
    read_error: Option<String>,
    relation: Option<RelationState>,
    applied: Option<RelationState>,
    subscription_error: Option<DbError>,
    pub(super) load_pending: bool,
    load_error: Option<DbError>,
    pub(super) reach_request: Option<Arc<dyn Fn(i64) + Send + Sync>>,
    shown: Option<Shown>,
    pub(super) targets_only: bool,
    pub(super) rejoin: Option<Arc<dyn Fn() + Send + Sync>>,
    resyncs_in_row: u32,
    unreported: u32,
    pub(super) load_model: Option<String>,
    pub(super) entity: bool,
    pub(super) on_cleared: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl Drop for Facts {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("Facts::drop"); }
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

fn failed_state(error: DbError) -> StreamState {
    StreamState { phase: LivePhase::Failed, settled: true, revision: -1, initial: false, refreshing: false, fetching_more: false, has_more: false, complete: false, total: None, loaded_at: None, error: Some(error), error_kind: Some("invalid".into()) }
}

fn open_state() -> StreamState {
    StreamState { phase: LivePhase::Loaded, settled: true, revision: -1, initial: false, refreshing: false, fetching_more: false, has_more: false, complete: false, total: None, loaded_at: None, error: None, error_kind: None }
}

impl Facts {
    pub(super) fn new(sink: Arc<dyn ValueSink>) -> Self {
        Self {
            sink: Some(sink), journal: Journal::default(), revision: None, rows_at: -1, landed_at: -1, size: 0, count: None, read_error: None, relation: None, applied: None, subscription_error: None,
            load_pending: false, load_error: None, reach_request: None, shown: None, targets_only: false, rejoin: None, resyncs_in_row: 0, unreported: 0, load_model: None, entity: false, on_cleared: None,
        }
    }

    /// Tells the reader that the journal holds values again. The reader reads them with `read_stream`.
    fn notify(&self, context: &mut Context<'_>) {
        let Some(sink) = self.sink.clone() else { return };
        let calls = context.fixed.calls.clone();
        context.effects.push(move || {
            crate::db::lock::assert_no_library_lock("ValueSink::available");
            calls.call(|| sink.available());
        });
    }

    fn noted(&self, context: &mut Context<'_>, appended: Appended) -> bool {
        match appended {
            Appended::First => { self.notify(context); false }
            Appended::Queued => false,
            Appended::Superseded => true,
        }
    }

    /// Keeps a value for the reader. True when the journal dropped the values a whole snapshot replaces.
    fn queue(&mut self, context: &mut Context<'_>, value: StreamValue) -> bool {
        let appended = self.journal.append(value);
        self.noted(context, appended)
    }

    fn ask_reach(&self, context: &mut Context<'_>, revision: i64) {
        if revision <= self.rows_at { return; }
        let Some(ask) = self.reach_request.clone() else { return };
        context.effects.push(move || ask(revision));
    }

    /// The facts every input changes the same way in every live state. True when the input asks the reader's state to be shown again.
    fn note(&mut self, context: &mut Context<'_>, event: &Input) -> bool {
        match event {
            Input::Reached(revision) => {
                self.read_error = None;
                self.rows_at = self.rows_at.max(*revision);
            }
            Input::ReadFailed(detail) => self.read_error = Some(detail.clone()),
            Input::Seed(relation) => {
                let mut relation = relation.clone();
                if self.load_pending && relation.error.take().is_some() && relation.phase == LoadPhase::Failed { relation.phase = LoadPhase::NotLoaded; }
                self.relation_changed(context, relation);
            }
            Input::Relation(relation) => self.relation_changed(context, relation.clone()),
            Input::LoadStarted => {
                self.load_pending = true;
                self.load_error = None;
            }
            Input::LoadFinished(error) => {
                self.load_pending = false;
                self.load_error = error.clone();
            }
            Input::EntityLoaded(error, revision) => {
                self.landed_at = self.landed_at.max(*revision);
                self.load_pending = false;
                self.load_error = error.clone();
                self.ask_reach(context, *revision);
            }
            Input::SubscriptionFailed(error) => self.subscription_error = error.clone(),
            _ => return false,
        }
        true
    }

    fn relation_changed(&mut self, context: &mut Context<'_>, relation: RelationState) {
        if !self.load_pending && relation.error.is_none() && matches!(relation.phase, LoadPhase::Loaded | LoadPhase::Empty) { self.load_error = None; }
        let revision = relation.revision;
        self.relation = Some(relation);
        self.ask_reach(context, revision);
    }

    fn take_frame(cell: &Cell<Option<Frame>>) -> Option<Frame> { cell.take() }

    /// Applies a frame of rows or a count and shows it.
    fn frame(&mut self, context: &mut Context<'_>, frame: Frame) -> Outcome<State> {
        self.read_error = None;
        self.rows_at = self.rows_at.max(frame.revision);
        if frame.count.is_some() { self.count = frame.count; } else { self.size = frame.size; }
        self.show(context, Some(frame))
    }

    /// Builds the state the reader sees and keeps it when it changed. A frame is kept even when the state did not change.
    fn show(&mut self, context: &mut Context<'_>, part: Option<Frame>) -> Outcome<State> {
        let rows_at = self.rows_at;
        if let Some(relation) = self.relation.clone().filter(|relation| relation.revision <= rows_at) { self.applied = Some(relation); }
        let gated = self.relation.as_ref().is_some_and(|relation| relation.revision > rows_at) || self.landed_at > rows_at;
        let mut effective = self.applied.clone().unwrap_or_else(|| RelationState { complete: false, phase: if self.relation.is_some() { LoadPhase::NotLoaded } else { LoadPhase::Loaded }, initial: false, refreshing: false, fetching_more: false, has_more: false, total: None, error: None, loaded_at: None, revision: 0, needs_reload: None });
        if effective.error.is_none() && self.subscription_error.is_some() { effective.error = Some(LoadError::new("subscription", "")); }
        let size = self.size as usize;
        if self.entity {
            if self.load_pending {
                effective.phase = LoadPhase::Loading;
                effective.initial = size == 0;
                effective.refreshing = size > 0;
            } else if self.load_error.is_some() && effective.error.is_none() {
                effective.error = Some(LoadError::new("load", ""));
            }
        }
        let phase = if self.read_error.is_some() && !matches!(effective.phase, LoadPhase::Loading | LoadPhase::Closed) { LoadPhase::Failed } else { snapshot_phase(&effective, size) };
        let error = if let Some(detail) = &self.read_error {
            Some((DbError::Database { detail: detail.clone() }, "database".to_string()))
        } else if let Some(error) = self.applied.as_ref().and_then(|applied| applied.error.as_ref()) {
            Some((load_failure(error), error.kind.to_string()))
        } else if let Some(error) = self.subscription_error.clone() {
            Some((error, "subscription".to_string()))
        } else {
            self.load_error.clone().filter(|error| self.entity || matches!(error, DbError::Network { .. })).map(|error| (error, "load".to_string()))
        };
        let shown = Shown {
            state: StreamState {
                phase: live_phase(phase), settled: !self.load_pending && !gated, revision: rows_at, initial: effective.initial, refreshing: effective.refreshing, fetching_more: effective.fetching_more, has_more: effective.has_more,
                complete: effective.complete && error.is_none(), total: if self.targets_only { None } else { effective.total }, loaded_at: effective.loaded_at,
                error: error.as_ref().map(|(error, _)| error.clone()), error_kind: error.map(|(_, kind)| kind),
            },
            size: self.size,
            count: self.count,
        };
        if part.is_none() && self.shown.as_ref() == Some(&shown) { return Handled; }
        self.shown = Some(shown.clone());
        let value = match part {
            Some(frame) if frame.count.is_some() => StreamValue::Count { revision: frame.revision, count: frame.count.unwrap_or_default(), state: shown.state },
            Some(frame) if frame.full => {
                self.revision = Some(frame.revision);
                StreamValue::Snapshot { revision: frame.revision, rows: frame.rows, state: shown.state }
            }
            Some(frame) if frame.moves_rows() => {
                let base = self.revision.unwrap_or(frame.revision);
                self.revision = Some(frame.revision);
                self.unreported += 1;
                if self.unreported >= 2 { self.resyncs_in_row = 0; }
                StreamValue::Delta {
                    base, revision: frame.revision, size: frame.size, rows: frame.rows, row_indexes: frame.row_indexes, removed: frame.removed, placed: frame.placed, replaced: frame.replaced, renamed: frame.renamed,
                    state: shown.state,
                }
            }
            _ => StreamValue::State { state: shown.state },
        };
        if self.queue(context, value) { self.rejoin(context) } else { Handled }
    }

    /// The reader cannot get the rows it misses from the journal: the observer sends the whole list again.
    fn rejoin(&mut self, context: &mut Context<'_>) -> Outcome<State> {
        let Some(rejoin) = self.rejoin.clone() else { return Handled };
        self.revision = None;
        context.effects.push(move || rejoin());
        Transition(State::rejoining())
    }

    /// The reader could not apply a delta. A new snapshot is asked for, and the stream ends once that did not help twice in a row.
    fn resync(&mut self, context: &mut Context<'_>) -> Outcome<State> {
        self.unreported = 0;
        if self.rejoin.is_some() && self.resyncs_in_row < MAX_RESYNCS_IN_ROW {
            self.resyncs_in_row += 1;
            return self.rejoin(context);
        }
        let mut failed = self.shown.as_ref().map(|shown| shown.state.clone()).unwrap_or_else(|| failed_state(DbError::Invalid { detail: String::new() }));
        failed.phase = LivePhase::Failed;
        failed.error = Some(DbError::Invalid { detail: "the list is out of sync with the database and a new snapshot did not fix it".into() });
        failed.error_kind = Some("invalid".into());
        self.queue(context, StreamValue::State { state: failed });
        self.finish(context, StreamEnd::OutOfSync)
    }

    /// Keeps the one end of the stream. The end of a session also drops the values of the old account that the reader did not read.
    fn finish(&mut self, context: &mut Context<'_>, reason: StreamEnd) -> Outcome<State> {
        if matches!(reason, StreamEnd::SessionEnded | StreamEnd::Closed) { self.journal.clear(); }
        let appended = self.journal.end(reason);
        self.noted(context, appended);
        Transition(State::ended())
    }

    fn heard(&mut self, context: &mut Context<'_>, cell: &Cell<Option<WireModelEvent>>, stamp: Stamp) {
        let now = context.fixed.life.snapshot().stamp();
        if now.session_epoch != stamp.session_epoch { return; }
        let Some(event) = cell.take() else { return };
        let appended = if now.generation == stamp.generation { self.journal.append(StreamValue::Event { event }) } else { self.journal.lose(1) };
        self.noted(context, appended);
    }

    fn read(&mut self, admitted: bool, taken: &Cell<Option<(u64, StreamValue)>>, done: &Cell<bool>) {
        if !admitted { self.journal.clear(); }
        taken.set(self.journal.take());
        done.set(self.journal.end_read());
    }

    /// The source of a stream without a list is open: the reader hears one settled state.
    fn opened(&mut self, context: &mut Context<'_>) { self.queue(context, StreamValue::State { state: open_state() }); }
}

#[state_machine(initial = "State::opening()")]
impl Facts {
    #[superstate]
    fn running(&mut self, context: &mut Context<'_>, event: &Input) -> Outcome<State> {
        match event {
            Input::Value(cell) => {
                if let Some(value) = cell.take() { self.queue(context, value); }
                Handled
            }
            Input::Heard(cell, stamp) => {
                self.heard(context, cell, *stamp);
                Handled
            }
            Input::Unread(count) => {
                let appended = self.journal.lose(*count);
                self.noted(context, appended);
                Handled
            }
            Input::Want(revision) => {
                self.ask_reach(context, *revision);
                Handled
            }
            Input::DataGone => {
                self.journal.lose_events();
                Handled
            }
            Input::Invalid(error) => {
                self.queue(context, StreamValue::State { state: failed_state(error.clone()) });
                self.finish(context, StreamEnd::Invalid)
            }
            Input::End(reason) => self.finish(context, *reason),
            Input::Read { admitted, taken, done } => {
                self.read(*admitted, taken, done);
                Handled
            }
            Input::Configure(setup) => {
                if let Some(setup) = setup.take() { setup(self); }
                Handled
            }
            Input::Cancel => {
                self.journal.clear();
                let sink = self.sink.take();
                context.effects.release(sink);
                Transition(State::ended())
            }
            _ => Handled,
        }
    }

    /// No value reached the reader yet.
    #[state(superstate = "running")]
    fn opening(&mut self, context: &mut Context<'_>, event: &Input) -> Outcome<State> {
        match event {
            Input::Rows(cell) | Input::Count(cell) => {
                let Some(frame) = Self::take_frame(cell) else { return Handled };
                if !frame.full && frame.count.is_none() { return self.rejoin(context); }
                match self.frame(context, frame) {
                    Handled => Transition(State::live()),
                    other => other,
                }
            }
            Input::ReadFailed(_) => {
                self.note(context, event);
                match self.show(context, None) {
                    Handled => Transition(State::live()),
                    other => other,
                }
            }
            Input::Opened => {
                self.opened(context);
                Transition(State::live())
            }
            Input::Value(_) | Input::Heard(..) | Input::Unread(_) => {
                self.opened(context);
                let _ = self.running(context, event);
                Transition(State::live())
            }
            Input::Resync => Handled,
            _ if self.note(context, event) => Handled,
            _ => Super,
        }
    }

    /// The reader holds rows and hears every change.
    #[state(superstate = "running")]
    fn live(&mut self, context: &mut Context<'_>, event: &Input) -> Outcome<State> {
        match event {
            Input::Rows(cell) | Input::Count(cell) => {
                let Some(frame) = Self::take_frame(cell) else { return Handled };
                if !frame.full && frame.count.is_none() && self.revision.is_none() { return self.resync(context); }
                self.frame(context, frame)
            }
            Input::Resync => self.resync(context),
            Input::Opened => Handled,
            _ if self.note(context, event) => self.show(context, None),
            _ => Super,
        }
    }

    /// The reader waits for a whole snapshot. Changes before it are not shown: the snapshot carries them.
    #[state(superstate = "running")]
    fn rejoining(&mut self, context: &mut Context<'_>, event: &Input) -> Outcome<State> {
        match event {
            Input::Rows(cell) | Input::Count(cell) => {
                let Some(frame) = Self::take_frame(cell) else { return Handled };
                if !frame.full && frame.count.is_none() { return Handled; }
                match self.frame(context, frame) {
                    Handled => Transition(State::live()),
                    other => other,
                }
            }
            Input::Resync | Input::Opened => Handled,
            Input::ReadFailed(_) => {
                self.note(context, event);
                self.show(context, None)
            }
            _ if self.note(context, event) => Handled,
            _ => Super,
        }
    }

    /// The stream ended. The journal keeps what the reader did not read yet.
    #[state]
    fn ended(&mut self, event: &Input) -> Outcome<State> {
        if let Input::Read { admitted, taken, done } = event { self.read(*admitted, taken, done); }
        Handled
    }
}

/// Whether the stream still owes its reader a value: before the first one, while a load runs, and while a whole snapshot is awaited.
pub(super) fn owes(machine: &InitializedStateMachine<Facts>) -> bool {
    match machine.state() {
        State::Ended { .. } => false,
        State::Live { .. } => machine.load_pending,
        _ => true,
    }
}

pub(super) fn ended(machine: &InitializedStateMachine<Facts>) -> bool { matches!(machine.state(), State::Ended { .. }) }

pub(super) fn machine(sink: Arc<dyn ValueSink>, fixed: &Fixed) -> InitializedStateMachine<Facts> {
    let mut effects = Effects::default();
    let mut context = Context { effects: &mut effects, fixed };
    Facts::new(sink).uninitialized_state_machine().init_with_context(&mut context)
}
