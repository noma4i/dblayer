use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::sync::{snapshot_phase, LoadError, LoadPhase, RelationState};

use super::live::{LiveChange, LivePhase, LiveWindowEdge, QueryListener};
use super::DbError;

const MAX_RESYNCS_IN_ROW: u32 = 2;

fn gate_open(relation: &RelationState, rows_at: i64) -> bool {
    relation.revision <= rows_at
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Readiness {
    Waiting,
    Ready,
    Ended,
}

#[derive(Clone, PartialEq)]
struct Shown {
    phase: LivePhase,
    kinds: (bool, bool, bool),
    has_more: bool,
    total: Option<i64>,
    loaded_at: Option<i64>,
    error: Option<(String, DbError)>,
    size: u32,
    count: Option<i64>,
}

struct FeedState {
    seq: u64,
    opened: bool,
    rows_at: i64,
    size: u32,
    count: Option<i64>,
    window: Option<LiveWindowEdge>,
    read_error: Option<String>,
    relation: Option<RelationState>,
    applied: Option<RelationState>,
    subscription_error: Option<DbError>,
    load_pending: bool,
    load_error: Option<DbError>,
    reach_request: Option<Arc<dyn Fn(i64) + Send + Sync>>,
    ready_pending: bool,
    shown: Option<Shown>,
    queue: VecDeque<LiveChange>,
    draining: bool,
    ended: bool,
    targets_only: bool,
    rejoin: Option<Arc<dyn Fn() + Send + Sync>>,
    awaiting_whole: bool,
    resyncs_in_row: u32,
    reported: bool,
}

pub(super) struct LiveFeed {
    state: Mutex<FeedState>,
    listener: Arc<dyn QueryListener>,
    pub(super) latest: watch::Sender<Option<LiveChange>>,
    pub(super) readiness: watch::Sender<Readiness>,
}

fn load_failure(error: &LoadError) -> DbError {
    if let Some(cause) = &error.cause {
        return DbError::from(cause.clone());
    }
    match error.kind {
        "network" => DbError::Network { detail: error.message.clone() },
        "database" => DbError::Database { detail: error.message.clone() },
        _ => DbError::Sync { detail: error.message.clone() },
    }
}

fn held<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
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

impl LiveFeed {
    pub(super) fn new(listener: Arc<dyn QueryListener>) -> Arc<Self> {
        let state = FeedState {
            seq: 0,
            opened: false,
            rows_at: -1,
            size: 0,
            count: None,
            window: None,
            read_error: None,
            relation: None,
            applied: None,
            subscription_error: None,
            load_pending: false,
            load_error: None,
            reach_request: None,
            ready_pending: false,
            shown: None,
            queue: VecDeque::new(),
            draining: false,
            ended: false,
            targets_only: false,
            rejoin: None,
            awaiting_whole: false,
            resyncs_in_row: 0,
            reported: false,
        };
        Arc::new(Self { state: Mutex::new(state), listener, latest: watch::channel(None).0, readiness: watch::channel(Readiness::Waiting).0 })
    }

    pub(super) fn rows(&self, part: LiveChange) {
        let mut state = held(&self.state);
        if state.awaiting_whole && !part.full {
            return;
        }
        state.awaiting_whole = false;
        state.opened = true;
        state.read_error = None;
        state.rows_at = state.rows_at.max(part.revision);
        state.size = part.size;
        state.window = part.window.clone();
        self.compose(state, Some(part));
    }

    pub(super) fn count(&self, part: LiveChange) {
        let mut state = held(&self.state);
        state.opened = true;
        state.read_error = None;
        state.rows_at = state.rows_at.max(part.revision);
        state.count = part.count;
        self.compose(state, Some(part));
    }

    pub(super) fn reached(&self, revision: i64) {
        let mut state = held(&self.state);
        state.read_error = None;
        state.rows_at = state.rows_at.max(revision);
        self.compose(state, None);
    }

    pub(super) fn read_failed(&self, detail: String) {
        let mut state = held(&self.state);
        state.opened = true;
        state.read_error = Some(detail);
        self.compose(state, None);
    }

    pub(super) fn state(&self, relation: RelationState) {
        let mut state = held(&self.state);
        let wanted = (relation.revision > state.rows_at).then_some(relation.revision);
        state.relation = Some(relation);
        let request = wanted.zip(state.reach_request.clone());
        self.compose(state, None);
        if let Some((revision, ask)) = request {
            ask(revision);
        }
    }

    /// Asks the observed group to tell this feed through `reached` once it has read through a revision the relation state is ahead of.
    pub(super) fn on_ahead(&self, ask: Arc<dyn Fn(i64) + Send + Sync>) {
        held(&self.state).reach_request = Some(ask);
    }

    /// Sets how to make the observer deliver the whole result again.
    pub(super) fn on_rejoin(&self, rejoin: Arc<dyn Fn() + Send + Sync>) {
        held(&self.state).rejoin = Some(rejoin);
    }

    pub(super) fn show_targets_only(&self) {
        held(&self.state).targets_only = true;
    }

    pub(super) fn expect_load(&self) {
        held(&self.state).load_pending = true;
    }

    pub(super) fn load_error(&self) -> Option<DbError> {
        held(&self.state).load_error.clone()
    }

    pub(super) fn load_finished(&self, error: Option<DbError>) {
        let mut state = held(&self.state);
        state.load_pending = false;
        state.load_error = error;
        self.compose(state, None);
    }

    pub(super) fn subscription_failed(&self, error: Option<DbError>) {
        let mut state = held(&self.state);
        state.subscription_error = error;
        self.compose(state, None);
    }

    pub(super) fn invalid(&self, error: DbError) {
        let mut state = held(&self.state);
        state.opened = true;
        let mut change = LiveChange::empty(0);
        change.error = Some(error.clone());
        change.error_kind = Some("invalid".to_string());
        change.phase = Some(LivePhase::Failed);
        state.seq += 1;
        change.seq = state.seq;
        state.shown = None;
        state.ready_pending = true;
        self.deliver(state, change);
    }

    /// The wrapper could not apply the delivery it was just given. The observer sends the whole list again, and the feed stops with a failure once that did not help twice in a row. Returns true when the feed stopped.
    pub(super) fn resync(&self) -> bool {
        let mut state = held(&self.state);
        state.reported = true;
        if state.ended {
            return true;
        }
        if state.awaiting_whole {
            return false;
        }
        let Some(rejoin) = state.rejoin.clone().filter(|_| state.resyncs_in_row < MAX_RESYNCS_IN_ROW) else {
            let detail = "the list is out of sync with the database and a new snapshot did not fix it".to_string();
            let basis = state.queue.back().cloned().or_else(|| self.latest.borrow().clone()).unwrap_or_else(|| LiveChange::empty(state.rows_at.max(0)));
            let mut change = LiveChange { size: state.size, window: basis.window.clone(), count: basis.count, error: Some(DbError::Invalid { detail }), error_kind: Some("invalid".to_string()), ..LiveChange::empty(basis.revision) };
            change.carry_state(&basis);
            change.phase = Some(LivePhase::Failed);
            change.error_kind = Some("invalid".to_string());
            state.seq += 1;
            change.seq = state.seq;
            state.queue.clear();
            state.shown = None;
            state.ended = true;
            state.ready_pending = true;
            self.readiness.send_replace(Readiness::Ended);
            self.deliver(state, change);
            return true;
        };
        state.resyncs_in_row += 1;
        state.awaiting_whole = true;
        state.queue.clear();
        drop(state);
        rejoin();
        false
    }

    pub(super) fn end(&self) {
        held(&self.state).ended = true;
        self.readiness.send_replace(Readiness::Ended);
    }

    fn compose<'a>(&'a self, mut state: std::sync::MutexGuard<'a, FeedState>, part: Option<LiveChange>) {
        if !state.opened || state.ended {
            return;
        }
        let rows_at = state.rows_at;
        if let Some(relation) = state.relation.clone().filter(|relation| gate_open(relation, rows_at)) {
            state.applied = Some(relation);
        }
        let gated = state.relation.as_ref().is_some_and(|relation| !gate_open(relation, rows_at));
        let mut effective = state.applied.clone().unwrap_or_else(|| RelationState { phase: if state.relation.is_some() { LoadPhase::NotLoaded } else { LoadPhase::Loaded }, initial: false, refreshing: false, fetching_more: false, has_more: false, total: None, error: None, loaded_at: None, revision: 0 });
        if effective.error.is_none() && state.subscription_error.is_some() {
            effective.error = Some(LoadError::new("subscription", ""));
        }
        let size = state.size as usize;
        let phase = if state.read_error.is_some() && !matches!(effective.phase, LoadPhase::Loading | LoadPhase::Closed) { LoadPhase::Failed } else { snapshot_phase(&effective, size) };
        let error = if let Some(detail) = &state.read_error {
            Some((DbError::Database { detail: detail.clone() }, "database".to_string()))
        } else if let Some(error) = state.applied.as_ref().and_then(|applied| applied.error.as_ref()) {
            Some((load_failure(error), error.kind.to_string()))
        } else {
            state.subscription_error.clone().map(|error| (error, "subscription".to_string()))
        };
        let shown = Shown {
            phase: live_phase(phase),
            kinds: (effective.initial, effective.refreshing, effective.fetching_more),
            has_more: effective.has_more,
            total: if state.targets_only { None } else { effective.total },
            loaded_at: effective.loaded_at,
            error: error.clone().map(|(error, kind)| (kind, error)),
            size: state.size,
            count: state.count,
        };
        let ready = !state.load_pending && !gated;
        state.ready_pending |= ready;
        if part.is_none() && state.shown.as_ref() == Some(&shown) {
            self.settle_ready(&mut state);
            return;
        }
        let mut change = part.unwrap_or_else(|| LiveChange::empty(rows_at.max(0)));
        change.phase = Some(shown.phase);
        change.initial = shown.kinds.0;
        change.refreshing = shown.kinds.1;
        change.fetching_more = shown.kinds.2;
        change.has_more = shown.has_more;
        change.total = shown.total;
        change.loaded_at = shown.loaded_at;
        change.size = shown.size;
        change.count = shown.count;
        if change.window.is_none() {
            change.window = state.window.clone();
        }
        change.error = error.as_ref().map(|(error, _)| error.clone());
        change.error_kind = error.map(|(_, kind)| kind);
        state.shown = Some(shown);
        state.seq += 1;
        change.seq = state.seq;
        self.deliver(state, change);
    }

    fn settle_ready(&self, state: &mut FeedState) {
        if state.draining || !state.queue.is_empty() || !std::mem::take(&mut state.ready_pending) {
            return;
        }
        self.readiness.send_if_modified(|current| {
            let waiting = *current == Readiness::Waiting;
            if waiting {
                *current = Readiness::Ready;
            }
            waiting
        });
    }

    fn deliver<'a>(&'a self, mut state: std::sync::MutexGuard<'a, FeedState>, change: LiveChange) {
        state.queue.push_back(change);
        if state.draining {
            return;
        }
        state.draining = true;
        loop {
            let Some(next) = state.queue.pop_front() else {
                state.draining = false;
                self.settle_ready(&mut state);
                return;
            };
            state.reported = false;
            drop(state);
            self.latest.send_replace(Some(next.clone()));
            let delta = !next.full;
            let _ = catch_unwind(AssertUnwindSafe(|| self.listener.changed(next)));
            state = held(&self.state);
            if delta && !state.reported {
                state.resyncs_in_row = 0;
            }
        }
    }
}
