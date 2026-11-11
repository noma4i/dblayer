//! The `delivery` machine as code: one match arm per table row. No library code.
//! A row is `(id, stamp)`: the stamp changes when the content of the row changes. The reference gives an effect list, a value a `Pull` reads is the effect `Answer`.

mod ref_delivery_tail;

use std::collections::VecDeque;

pub(super) const JOURNAL_LIMIT: usize = 256;
pub(super) const DELTA_LIMIT: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Idle,
    Loading,
    Loaded,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Ws {
    pub(super) phase: Phase,
    pub(super) error: bool,
    pub(super) loaded: Option<i64>,
}

impl Ws {
    pub(super) fn idle() -> Self { Self { phase: Phase::Idle, error: false, loaded: None } }

    pub(super) fn loaded_at(revision: i64) -> Self { Self { phase: Phase::Loaded, error: false, loaded: Some(revision) } }

    pub(super) fn loading_after(revision: i64) -> Self { Self { phase: Phase::Loading, error: false, loaded: Some(revision) } }

    fn visible_after(&self) -> i64 { self.loaded.unwrap_or(0) }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Cause {
    SessionEnded,
    Closed,
    Cancelled,
    Invalid,
    Storage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fetch {
    Never,
    Stale,
    Force,
    IfMissing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    List,
    Entity(Fetch),
    Count,
    Events,
    Changes,
    Blob,
}

impl Kind {
    fn reads_rows(self) -> bool { matches!(self, Kind::List | Kind::Entity(_) | Kind::Count) }
}

pub(super) type Row = (String, i64);

pub(super) fn row(id: &str) -> Row { (id.to_string(), 0) }

pub(super) fn rows(ids: &[&str]) -> Vec<Row> { ids.iter().map(|id| row(id)).collect() }

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Input {
    Open { kind: Kind, epoch_current: bool },
    OpenNoSession,
    OpenInvalid,
    Committed { revision: i64, touches: bool },
    Changed { revision: i64, rows: Vec<(String, String)>, reset: bool },
    ReadDone { revision: i64, rows: Vec<Row> },
    ReadRelevant { revision: i64, position: u64, relevant: Option<(i64, u64)>, rows: Vec<Row> },
    ReadOverflow { revision: i64 },
    CountDone { revision: i64, count: u64 },
    CountRelevant { revision: i64, position: u64, relevant: Option<(i64, u64)>, count: u64 },
    ReadStale { revision: i64, rows: Vec<Row> },
    ReadFailed { revision: i64 },
    ReadFailedStale,
    WindowState(Ws),
    Joined { position: u64 },
    ModelReady,
    ModelEvent { position: u64, payload: String },
    ModelLost { first: u64, last: u64 },
    ModelGap,
    Resync,
    Transfer { state: String, revision: i64 },
    Pull,
    PullCancelled { matched: bool },
    Cancel { reply: u64 },
    Release,
    SinkClosed,
    EpochEnded { own: bool },
    EpochClosed,
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Ed {
    Insert { index: u32, id: String },
    Rewrite { index: u32, id: String },
    Remove { index: u32, id: String },
    Move { from: u32, to: u32 },
}

pub(super) fn insert(index: u32, id: &str) -> Ed { Ed::Insert { index, id: id.to_string() } }

pub(super) fn remove(index: u32, id: &str) -> Ed { Ed::Remove { index, id: id.to_string() } }

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Val {
    Snapshot { revision: i64, rows: Vec<String>, state: Ws },
    Delta { base: i64, revision: i64, size: usize, edits: Vec<Ed>, state: Ws },
    Count { revision: i64, count: u64, state: Ws },
    State { revision: i64, state: Ws },
    Ready,
    Changes { revision: i64, rows: Vec<(String, String)> },
    Event { position: u64, payload: String },
    Lost { first: u64, last: u64 },
    Gap,
    Status { revision: i64, state: String },
    Ended(Cause),
}

impl Val {
    fn is_rows(&self) -> bool { matches!(self, Val::Snapshot { .. } | Val::Delta { .. } | Val::Count { .. }) }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Pulled {
    Value(Val),
    Done,
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Effect {
    Read(i64),
    InterestPlus,
    InterestPassive,
    Load { force: bool },
    InterestMinus,
    WantPlus,
    WantMinus,
    StatusInterest(bool),
    Available,
    CloseSink,
    Cancelled(u64),
    Answer(Pulled),
    Drained,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Held {
    Event(u64, String),
    Lost(u64, u64),
    Gap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stage {
    Nothing,
    Opening,
    Live,
    Cancelling,
    Ended,
}

pub(super) struct Delivery {
    pub(super) stage: Stage,
    kind: Kind,
    pub(super) journal: VecDeque<Val>,
    revision: i64,
    value_position: u64,
    acknowledged: Option<i64>,
    resync_pending: bool,
    resync_failed: bool,
    read_error: bool,
    rows: Vec<Row>,
    acked_rows: Vec<Row>,
    count: Option<u64>,
    total: Option<u64>,
    incoming_total: Option<u64>,
    window: Ws,
    held_state: Option<Ws>,
    pending_read: Option<Input>,
    read_revision: i64,
    read_inflight: bool,
    read_seen: i64,
    read_position: u64,
    next_feed: Option<u64>,
    position: u64,
    ready: bool,
    held_events: Vec<Held>,
    waiting: bool,
    ended_given: bool,
    cause: Cause,
    replies: Vec<u64>,
    released: bool,
    sink_closed: bool,
    interested: bool,
    last_status: Option<String>,
    pub(super) gone: bool,
    closing_seen: bool,
    drained: bool,
}

impl Delivery {
    pub(super) fn new() -> Self {
        Self {
            stage: Stage::Nothing,
            kind: Kind::List,
            journal: VecDeque::new(),
            revision: 0,
            value_position: 0,
            acknowledged: None,
            resync_pending: false,
            resync_failed: false,
            read_error: false,
            rows: Vec::new(),
            acked_rows: Vec::new(),
            count: None,
            total: None,
            incoming_total: None,
            window: Ws::idle(),
            held_state: None,
            pending_read: None,
            read_revision: 0,
            read_inflight: false,
            read_seen: 0,
            read_position: 0,
            next_feed: None,
            position: 0,
            ready: false,
            held_events: Vec::new(),
            waiting: false,
            ended_given: false,
            cause: Cause::Cancelled,
            replies: Vec::new(),
            released: false,
            sink_closed: false,
            interested: false,
            last_status: None,
            gone: false,
            closing_seen: false,
            drained: false,
        }
    }

    fn sink_open(&self) -> bool { matches!(self.stage, Stage::Opening | Stage::Live) }

    /// P-DLV-32: the state of a value is one function of the last window value, the read error and the rows of the stream.
    fn delivered(&self, window: Ws) -> Ws {
        let has_rows = if self.kind == Kind::Count { self.count.is_some_and(|count| count > 0) } else { !self.rows.is_empty() };
        let by_rows = if has_rows { Phase::Loaded } else { Phase::Failed };
        if self.read_error { return Ws { phase: by_rows, error: true, loaded: window.loaded }; }
        if window.error && matches!(window.phase, Phase::Loaded | Phase::Failed) { return Ws { phase: by_rows, ..window }; }
        window
    }

    fn evict_feed_prefix(&mut self) {
        let feed = |value: &Val| matches!(value, Val::Event { .. } | Val::Lost { .. } | Val::Gap);
        let Some(start) = self.journal.iter().position(feed) else { return };
        let run = self.journal.iter().skip(start).take_while(|value| feed(value)).count();
        let evicted = run.min(self.journal.len() - (JOURNAL_LIMIT - 1));
        let positions: Vec<(u64, u64)> = self.journal.drain(start..start + evicted).filter_map(|value| match value {
            Val::Event { position, .. } => Some((position, position)),
            Val::Lost { first, last } => Some((first, last)),
            _ => None,
        }).collect();
        let merged = match (positions.iter().map(|range| range.0).min(), positions.iter().map(|range| range.1).max()) {
            (Some(first), Some(last)) => Val::Lost { first, last },
            _ => Val::Gap,
        };
        self.journal.insert(start, merged);
    }

    fn append(&mut self, value: Val, fx: &mut Vec<Effect>) {
        let value = match value {
            Val::Snapshot { revision, rows, state } => Val::Snapshot { revision, rows, state: self.delivered(state) },
            Val::Count { revision, count, state } => Val::Count { revision, count, state: self.delivered(state) },
            Val::State { revision, state } => Val::State { revision, state: self.delivered(state) },
            Val::Delta { base, revision, size, edits, state } => Val::Delta { base, revision, size, edits, state: self.delivered(state) },
            other => other,
        };
        let was_empty = self.journal.is_empty();
        match &value {
            Val::Snapshot { .. } | Val::Count { .. } => self.journal.retain(|kept| !matches!(kept, Val::Snapshot { .. } | Val::Count { .. } | Val::Delta { .. } | Val::State { .. })),
            Val::Delta { .. } if self.journal.iter().any(Val::is_rows) => self.journal.retain(|kept| !matches!(kept, Val::Snapshot { .. } | Val::Count { .. } | Val::Delta { .. } | Val::State { .. })),
            Val::State { .. } => self.journal.retain(|kept| !matches!(kept, Val::State { .. })),
            Val::Status { .. } => self.journal.retain(|kept| !matches!(kept, Val::Status { .. })),
            _ => {}
        }
        if matches!(value, Val::Gap) && matches!(self.journal.back(), Some(Val::Gap)) {
            return;
        }
        self.journal.push_back(value);
        if self.journal.len() > JOURNAL_LIMIT {
            self.evict_feed_prefix();
        }
        if self.waiting {
            self.waiting = false;
            let answer = self.take();
            fx.push(Effect::Answer(answer));
        } else if was_empty && self.sink_open() {
            fx.push(Effect::Available);
        }
    }

    fn take(&mut self) -> Pulled {
        match self.journal.pop_front() {
            Some(value) => {
                match &value {
                    Val::Ended(_) => { self.ended_given = true; self.journal.clear(); }
                    Val::Snapshot { revision, .. } | Val::Count { revision, .. } | Val::Delta { revision, .. } => { self.acknowledged = Some(*revision); self.acked_rows = self.rows.clone(); }
                    _ => {}
                }
                Pulled::Value(value)
            }
            None => Pulled::Done,
        }
    }

    fn state_at(&mut self, revision: i64) -> Ws {
        if let Some(held) = self.held_state.clone() {
            if held.visible_after() <= revision {
                self.window = held;
                self.held_state = None;
            }
        }
        self.window.clone()
    }

    fn leave(&mut self) -> Option<Effect> {
        if !std::mem::take(&mut self.interested) { return None; }
        Some(match self.kind {
            Kind::Events => Effect::WantMinus,
            Kind::Blob => Effect::StatusInterest(false),
            _ => Effect::InterestMinus,
        })
    }

    fn end_now(&mut self, cause: Cause, fx: &mut Vec<Effect>) {
        fx.extend(self.leave());
        self.stage = Stage::Ended;
        self.cause = cause;
        self.journal.clear();
        self.journal.push_back(Val::Ended(cause));
        if self.waiting {
            self.waiting = false;
            let answer = self.take();
            fx.push(Effect::Answer(answer));
        } else {
            fx.push(Effect::Available);
        }
        fx.push(Effect::CloseSink);
    }


    pub(super) fn waiting(&self) -> bool { self.waiting }

    pub(super) fn next_total(&mut self, total: Option<u64>) { self.incoming_total = total; }

    pub(super) fn step(&mut self, input: Input) -> Vec<Effect> {
        let mut fx = Vec::new();
        if input == Input::Closing { self.closing_seen = true; }
        if matches!(&input, Input::ReadDone { .. } | Input::ReadRelevant { .. } | Input::CountDone { .. } | Input::CountRelevant { .. } | Input::ReadFailed { .. } | Input::ReadOverflow { .. }) { self.read_inflight = false; }
        self.apply(input, &mut fx);
        if fx.iter().any(|effect| matches!(effect, Effect::Read(_))) { self.read_inflight = true; }
        if self.stage == Stage::Ended && self.released && self.sink_closed { self.gone = true; }
        let settled = self.gone || self.stage == Stage::Nothing || (self.stage == Stage::Ended && self.sink_closed);
        if self.closing_seen && !self.drained && settled {
            self.drained = true;
            fx.push(Effect::Drained);
        }
        fx
    }

    fn apply(&mut self, input: Input, fx: &mut Vec<Effect>) {
        let input = match input {
            Input::ReadDone { revision, rows } => Input::ReadRelevant { revision, position: 0, relevant: Some((revision, 0)), rows },
            Input::CountDone { revision, count } => Input::CountRelevant { revision, position: 0, relevant: Some((revision, 0)), count },
            other => other,
        };
        match (self.stage, input) {
            (Stage::Opening | Stage::Live, Input::PullCancelled { matched: true }) if self.waiting => {
                self.waiting = false;
                fx.push(Effect::Answer(Pulled::Interrupted));
                fx.extend(self.leave());
                self.stage = Stage::Cancelling;
                self.cause = Cause::Cancelled;
                self.journal.clear();
                fx.push(Effect::CloseSink);
            }
            (Stage::Cancelling, Input::PullCancelled { matched: true }) if self.waiting => {
                self.waiting = false;
                fx.push(Effect::Answer(Pulled::Interrupted));
            }
            (Stage::Nothing, Input::Open { kind, epoch_current: true }) => {
                self.kind = kind;
                match kind {
                    Kind::Events => { self.stage = Stage::Live; self.interested = true; fx.push(Effect::WantPlus); }
                    Kind::Changes => { self.stage = Stage::Live; self.append(Val::Ready, fx); }
                    Kind::Blob => { self.stage = Stage::Opening; self.interested = true; fx.push(Effect::StatusInterest(true)); }
                    _ => {
                        self.stage = Stage::Opening;
                        fx.push(Effect::Read(0));
                        match kind {
                            Kind::List | Kind::Count | Kind::Entity(Fetch::Stale) => { self.interested = true; fx.push(Effect::InterestPlus); }
                            Kind::Entity(Fetch::IfMissing) => { self.interested = true; fx.push(Effect::InterestPassive); }
                            Kind::Entity(Fetch::Force) => { self.interested = true; fx.push(Effect::InterestPlus); fx.push(Effect::Load { force: true }); }
                            _ => {}
                        }
                    }
                }
            }
            (Stage::Nothing, Input::Open { kind, epoch_current: false }) => { self.kind = kind; self.dead_on_open(Cause::SessionEnded, fx); }
            (Stage::Nothing, Input::OpenNoSession) => self.dead_on_open(Cause::SessionEnded, fx),
            (Stage::Nothing, Input::OpenInvalid) => self.dead_on_open(Cause::Invalid, fx),
            (Stage::Nothing, Input::Pull) => fx.push(Effect::Answer(Pulled::Value(Val::Ended(Cause::Cancelled)))),
            (Stage::Nothing, Input::Cancel { reply }) => fx.push(Effect::Cancelled(reply)),
            (Stage::Opening, input @ (Input::ReadRelevant { .. } | Input::CountRelevant { .. })) if self.interested && self.held_state.is_none() && self.kind.reads_rows() => self.pending_read = Some(input),
            (Stage::Opening, Input::ReadRelevant { revision, position, rows, .. }) if self.kind.reads_rows() && self.kind != Kind::Count => {
                self.read_seen = self.read_seen.max(revision);
                self.read_position = position;
                self.value_position = position;
                self.total = self.incoming_total;
                let missing = rows.is_empty() && self.kind == Kind::Entity(Fetch::IfMissing);
                let state = self.state_at(revision);
                self.rows = rows.clone();
                self.revision = revision;
                self.stage = Stage::Live;
                self.append(Val::Snapshot { revision, rows: rows.into_iter().map(|(id, _)| id).collect(), state }, fx);
                if missing { fx.push(Effect::Load { force: false }); }
            }
            (Stage::Opening, Input::CountRelevant { revision, position, count, .. }) if self.kind == Kind::Count => {
                self.read_seen = self.read_seen.max(revision);
                self.read_position = position;
                self.value_position = position;
                let state = self.state_at(revision);
                self.count = Some(count);
                self.revision = revision;
                self.stage = Stage::Live;
                self.append(Val::Count { revision, count, state }, fx);
            }
            (Stage::Opening | Stage::Live, Input::ReadStale { .. }) => {}
            (Stage::Opening, Input::ReadFailed { revision }) if self.kind.reads_rows() => {
                self.read_error = true;
                let state = self.window.clone();
                self.revision = revision;
                self.stage = Stage::Live;
                if self.kind == Kind::Count {
                    self.count = Some(0);
                    self.append(Val::Count { revision, count: 0, state }, fx);
                } else {
                    self.append(Val::Snapshot { revision, rows: Vec::new(), state }, fx);
                }
            }
            (Stage::Opening, Input::Committed { revision, touches }) => {
                if touches && self.kind.reads_rows() { self.read_revision = revision; fx.push(Effect::Read(revision)); }
            }
            (Stage::Opening, Input::WindowState(state)) => {
                self.held_state = Some(state);
                if let Some(read) = self.pending_read.take() { self.apply(read, fx); }
            }
            (Stage::Opening, Input::Pull) => {
                if self.waiting { fx.push(Effect::Answer(Pulled::Value(Val::Ended(Cause::Invalid)))); } else { self.waiting = true; }
            }
            (Stage::Opening, Input::Transfer { state, revision }) if self.kind == Kind::Blob => self.status(state, revision, fx),
            (Stage::Opening | Stage::Live, Input::Cancel { reply }) => {
                fx.extend(self.leave());
                self.stage = Stage::Cancelling;
                self.cause = Cause::Cancelled;
                self.journal.clear();
                self.replies.push(reply);
                fx.push(Effect::CloseSink);
            }
            (Stage::Opening | Stage::Live, Input::Release) => {
                fx.extend(self.leave());
                self.stage = Stage::Cancelling;
                self.cause = Cause::Cancelled;
                self.journal.clear();
                self.released = true;
                fx.push(Effect::CloseSink);
            }
            (Stage::Opening | Stage::Live, Input::EpochEnded { own: true }) => self.end_now(Cause::SessionEnded, fx),
            (Stage::Opening | Stage::Live, Input::EpochClosed) => self.end_now(Cause::Closed, fx),
            (Stage::Cancelling, Input::EpochClosed) => self.cause = Cause::Closed,
            (Stage::Opening | Stage::Live, Input::Closing) => self.end_now(Cause::Closed, fx),
            (Stage::Live, Input::Changed { revision, rows, reset }) if self.kind == Kind::Changes => {
                if !rows.is_empty() || reset { self.append(Val::Changes { revision, rows }, fx); }
            }
            (Stage::Live, Input::Committed { revision, touches }) => {
                if touches && self.kind.reads_rows() && revision > self.read_seen { self.read_revision = revision; fx.push(Effect::Read(revision)); }
            }
            (Stage::Live, Input::ReadOverflow { revision }) if self.kind.reads_rows() => {
                if revision >= self.read_seen { fx.push(Effect::Read(self.read_revision)); }
            }
            (Stage::Live, Input::ReadRelevant { revision, position, relevant, rows }) if self.kind.reads_rows() && self.kind != Kind::Count => {
                if (revision, position) < (self.read_seen, self.read_position) { return; }
                (self.read_seen, self.read_position) = (revision, position);
                let cleared = std::mem::take(&mut self.read_error);
                if cleared && !self.resync_pending && self.rows == rows && self.total == self.incoming_total {
                    let state = self.state_at(revision);
                    self.append(Val::State { revision: self.revision, state }, fx);
                    return;
                }
                if self.resync_pending {
                    self.resync_pending = false;
                    let changed = self.rows != rows || self.total != self.incoming_total;
                    self.rows = rows.clone();
                    let value = if changed { relevant.expect("changed view has a relevant version") } else { (self.revision, self.value_position) };
                    let shown = value.0;
                    self.value_position = value.1;
                    self.revision = shown;
                    let state = self.state_at(revision);
                    self.append(Val::Snapshot { revision: shown, rows: rows.into_iter().map(|(id, _)| id).collect(), state }, fx);
                    return;
                }
                self.live_rows(revision, relevant, rows, fx)
            }
            (Stage::Live, Input::CountRelevant { revision, position, relevant, count }) if self.kind == Kind::Count => {
                if (revision, position) < (self.read_seen, self.read_position) { return; }
                (self.read_seen, self.read_position) = (revision, position);
                let cleared = std::mem::take(&mut self.read_error);
                if self.count == Some(count) {
                    if cleared { let state = self.state_at(revision); self.append(Val::State { revision: self.revision, state }, fx); } else { self.same_rows(revision, fx); }
                    return;
                }
                let state = self.state_at(revision);
                let (shown, position) = relevant.expect("changed count has a relevant version");
                self.value_position = position;
                self.count = Some(count);
                self.revision = shown;
                self.append(Val::Count { revision: shown, count, state }, fx);
            }
            (Stage::Live, Input::CountRelevant { revision, .. }) if self.kind.reads_rows() => self.read_seen = self.read_seen.max(revision),
            (Stage::Live, Input::ReadFailed { .. }) if self.kind.reads_rows() => {
                if self.resync_pending {
                    if self.resync_failed { self.end_now(Cause::Storage, fx); return; }
                    self.resync_failed = true;
                    self.read_error = true;
                    let state = self.window.clone();
                    self.append(Val::State { revision: self.revision, state }, fx);
                    self.read_revision = self.read_revision.max(self.revision);
                    fx.push(Effect::Read(self.read_revision));
                    return;
                }
                self.read_error = true;
                let state = self.window.clone();
                self.append(Val::State { revision: self.revision, state }, fx);
            }
            (Stage::Live, Input::WindowState(state)) if self.kind.reads_rows() => {
                if state.visible_after() > self.read_seen {
                    if !self.read_inflight || self.read_revision < state.visible_after() {
                        self.read_revision = self.read_revision.max(state.visible_after());
                        fx.push(Effect::Read(self.read_revision));
                    }
                    self.held_state = Some(state);
                }
                else if state != self.window {
                    let before = self.delivered(self.window.clone());
                    self.window = state.clone();
                    self.held_state = None;
                    if self.delivered(state.clone()) != before { self.append(Val::State { revision: self.revision, state }, fx); }
                }
            }
            (Stage::Live, Input::Joined { position }) if self.kind == Kind::Events => {
                if self.next_feed.is_none() { self.next_feed = Some(position + 1); self.replay(fx); }
            }
            (Stage::Live, Input::ModelReady) if self.kind == Kind::Events => {
                if !self.ready { self.ready = true; self.append(Val::Ready, fx); self.replay(fx); }
            }
            (Stage::Live, Input::ModelEvent { position, payload }) if self.kind == Kind::Events => self.event(position, payload, fx),
            (Stage::Live, Input::ModelLost { first, last }) if self.kind == Kind::Events => self.lost(first, last, fx),
            (Stage::Live, Input::ModelGap) if self.kind == Kind::Events => {
                if self.next_feed.is_none() || !self.ready { self.held_events.push(Held::Gap); } else { self.append(Val::Gap, fx); }
            }
            (Stage::Live, Input::Transfer { state, revision }) if self.kind == Kind::Blob => self.status(state, revision, fx),
            (Stage::Live, Input::Resync) if matches!(self.kind, Kind::List | Kind::Entity(_)) => {
                self.acknowledged = None;
                self.resync_failed = false;
                self.resync_pending = true;
                self.journal.retain(|v| !matches!(v, Val::Snapshot { .. } | Val::Delta { .. } | Val::State { .. }));
                self.read_revision = self.read_revision.max(self.revision);
                fx.push(Effect::Read(self.read_revision));
            }
            (Stage::Live, Input::Pull) => {
                if self.waiting { fx.push(Effect::Answer(Pulled::Value(Val::Ended(Cause::Invalid)))); }
                else if self.journal.is_empty() { self.waiting = true; }
                else { let answer = self.take(); fx.push(Effect::Answer(answer)); }
            }
            (Stage::Cancelling, Input::Cancel { reply }) => self.replies.push(reply),
            (Stage::Cancelling, Input::Pull) => {
                if self.waiting { fx.push(Effect::Answer(Pulled::Value(Val::Ended(Cause::Invalid)))); } else { self.waiting = true; }
            }
            (Stage::Cancelling, Input::EpochEnded { own: true }) => self.cause = Cause::SessionEnded,
            (Stage::Cancelling, Input::Closing) => self.cause = Cause::Closed,
            (Stage::Cancelling, Input::Release) => self.released = true,
            (Stage::Cancelling, Input::SinkClosed) => {
                for reply in self.replies.drain(..) { fx.push(Effect::Cancelled(reply)); }
                self.stage = Stage::Ended;
                self.sink_closed = true;
                if self.released { return; }
                self.journal.clear();
                self.journal.push_back(Val::Ended(self.cause));
                if self.waiting {
                    self.waiting = false;
                    let answer = self.take();
                    fx.push(Effect::Answer(answer));
                }
            }
            (Stage::Ended, Input::Pull) => {
                if self.journal.is_empty() { fx.push(Effect::Answer(Pulled::Done)); } else { let answer = self.take(); fx.push(Effect::Answer(answer)); }
            }
            (Stage::Ended, Input::Release) => self.released = true,
            (Stage::Ended, Input::SinkClosed) => {
                if self.sink_closed { return; }
                self.sink_closed = true;
                for reply in self.replies.drain(..) { fx.push(Effect::Cancelled(reply)); }
            }
            (Stage::Ended, Input::Cancel { reply }) => {
                if self.sink_closed { fx.push(Effect::Cancelled(reply)); } else { self.replies.push(reply); }
            }
            _ => {}
        }
    }

}
