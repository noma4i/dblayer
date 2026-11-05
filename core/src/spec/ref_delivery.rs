//! The `delivery` machine as code: one match arm per table row. No library code.
//! A row is `(id, stamp)`: the stamp changes when the content of the row changes. The reference gives an effect list, a value a `Pull` reads is the effect `Answer`.

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
    Blob,
    Retry,
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
    ReadDone { revision: i64, rows: Vec<Row> },
    CountDone { revision: i64, count: u64 },
    ReadStale { revision: i64, rows: Vec<Row> },
    ReadFailed { revision: i64 },
    WindowState(Ws),
    Joined { position: u64 },
    ModelReady,
    ModelEvent { position: u64, payload: String },
    ModelLost { first: u64, last: u64 },
    ModelGap,
    Transfer { state: String },
    Retry { attempts: u32 },
    Pull,
    Cancel { reply: u64 },
    Release,
    SinkClosed,
    EpochEnded { own: bool },
    ModelsReset { own_model: bool },
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Val {
    Snapshot { revision: i64, rows: Vec<String>, state: Ws },
    Delta { base: i64, revision: i64, size: usize, upserted: Vec<String>, removed: Vec<String>, state: Ws },
    Count { revision: i64, count: u64, state: Ws },
    State { revision: i64, state: Ws },
    Ready,
    Event { position: u64, payload: String },
    Lost { first: u64, last: u64 },
    Gap,
    Status { revision: i64, state: String },
    Retry { attempts: u32 },
    Ended(Cause),
}

impl Val {
    fn is_rows(&self) -> bool { matches!(self, Val::Snapshot { .. } | Val::Delta { .. } | Val::Count { .. }) }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Pulled {
    Value(Val),
    Done,
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Effect {
    Read(i64),
    InterestPlus,
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
    acknowledged: Option<i64>,
    rows: Vec<Row>,
    acked_rows: Vec<Row>,
    count: Option<u64>,
    window: Ws,
    held_state: Option<Ws>,
    read_revision: i64,
    read_seen: i64,
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
    status_revision: i64,
    last_status: Option<String>,
    last_retry: Option<u32>,
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
            acknowledged: None,
            rows: Vec::new(),
            acked_rows: Vec::new(),
            count: None,
            window: Ws::idle(),
            held_state: None,
            read_revision: 0,
            read_seen: 0,
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
            status_revision: 0,
            last_status: None,
            last_retry: None,
            gone: false,
            closing_seen: false,
            drained: false,
        }
    }

    fn sink_open(&self) -> bool { matches!(self.stage, Stage::Opening | Stage::Live) }

    fn shown(&self, mut state: Ws) -> Ws {
        if state.error && matches!(state.phase, Phase::Loaded | Phase::Failed) {
            let has_rows = if self.kind == Kind::Count { self.count.is_some_and(|count| count > 0) } else { !self.rows.is_empty() };
            state.phase = if has_rows { Phase::Loaded } else { Phase::Failed };
        }
        state
    }

    fn append(&mut self, value: Val, fx: &mut Vec<Effect>) {
        let value = match value {
            Val::Snapshot { revision, rows, state } => Val::Snapshot { revision, rows, state: self.shown(state) },
            Val::Count { revision, count, state } => Val::Count { revision, count, state: self.shown(state) },
            Val::State { revision, state } => Val::State { revision, state: self.shown(state) },
            Val::Delta { base, revision, size, upserted, removed, state } => Val::Delta { base, revision, size, upserted, removed, state: self.shown(state) },
            other => other,
        };
        let was_empty = self.journal.is_empty();
        match &value {
            Val::Snapshot { .. } | Val::Count { .. } => self.journal.retain(|kept| !matches!(kept, Val::Snapshot { .. } | Val::Count { .. } | Val::Delta { .. } | Val::State { .. })),
            Val::Delta { .. } if self.journal.iter().any(Val::is_rows) => self.journal.retain(|kept| !matches!(kept, Val::Snapshot { .. } | Val::Count { .. } | Val::Delta { .. } | Val::State { .. })),
            Val::State { .. } => self.journal.retain(|kept| !matches!(kept, Val::State { .. })),
            Val::Status { .. } => self.journal.retain(|kept| !matches!(kept, Val::Status { .. })),
            Val::Retry { .. } => self.journal.retain(|kept| !matches!(kept, Val::Retry { .. })),
            _ => {}
        }
        self.journal.push_back(value);
        if self.journal.len() > JOURNAL_LIMIT {
            let (mut first, mut last, mut at) = (u64::MAX, 0u64, None);
            while self.journal.len() > JOURNAL_LIMIT - 1 {
                let Some(index) = self.journal.iter().position(|kept| matches!(kept, Val::Event { .. } | Val::Lost { .. })) else { break };
                at.get_or_insert(index);
                match self.journal.remove(index).unwrap() {
                    Val::Event { position, .. } => { first = first.min(position); last = last.max(position); }
                    Val::Lost { first: a, last: b } => { first = first.min(a); last = last.max(b); }
                    _ => unreachable!(),
                }
            }
            if let Some(index) = at { self.journal.insert(index, Val::Lost { first, last }); }
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
            Kind::Blob | Kind::Retry => Effect::StatusInterest(false),
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

    pub(super) fn is_events(&self) -> bool { self.kind == Kind::Events }

    pub(super) fn step(&mut self, input: Input) -> Vec<Effect> {
        let mut fx = Vec::new();
        if input == Input::Closing { self.closing_seen = true; }
        self.apply(input, &mut fx);
        if self.stage == Stage::Ended && self.released && self.sink_closed { self.gone = true; }
        let settled = self.gone || self.stage == Stage::Nothing || (self.stage == Stage::Ended && self.sink_closed);
        if self.closing_seen && !self.drained && settled {
            self.drained = true;
            fx.push(Effect::Drained);
        }
        fx
    }

    fn apply(&mut self, input: Input, fx: &mut Vec<Effect>) {
        match (self.stage, input) {
            (Stage::Nothing, Input::Open { kind, epoch_current: true }) => {
                self.kind = kind;
                match kind {
                    Kind::Events => { self.stage = Stage::Live; self.interested = true; fx.push(Effect::WantPlus); }
                    Kind::Blob | Kind::Retry => { self.stage = Stage::Opening; self.interested = true; fx.push(Effect::StatusInterest(true)); }
                    _ => {
                        self.stage = Stage::Opening;
                        fx.push(Effect::Read(0));
                        match kind {
                            Kind::List | Kind::Count | Kind::Entity(Fetch::Stale | Fetch::IfMissing) => { self.interested = true; fx.push(Effect::InterestPlus); }
                            Kind::Entity(Fetch::Force) => { self.interested = true; fx.push(Effect::InterestPlus); fx.push(Effect::Load { force: true }); }
                            _ => {}
                        }
                    }
                }
            }
            (Stage::Nothing, Input::Open { kind, epoch_current: false }) => { self.kind = kind; self.dead_on_open(Cause::SessionEnded, fx); }
            (Stage::Nothing, Input::OpenNoSession) => self.dead_on_open(Cause::SessionEnded, fx),
            (Stage::Nothing, Input::OpenInvalid) => self.dead_on_open(Cause::Invalid, fx),
            (Stage::Opening, Input::ReadDone { revision, rows }) if self.kind.reads_rows() && self.kind != Kind::Count => {
                self.read_seen = self.read_seen.max(revision);
                let missing = rows.is_empty() && self.kind == Kind::Entity(Fetch::IfMissing);
                let state = self.state_at(revision);
                self.rows = rows.clone();
                self.revision = revision;
                self.stage = Stage::Live;
                self.append(Val::Snapshot { revision, rows: rows.into_iter().map(|(id, _)| id).collect(), state }, fx);
                if missing { fx.push(Effect::Load { force: false }); }
            }
            (Stage::Opening, Input::CountDone { revision, count }) if self.kind == Kind::Count => {
                let state = self.state_at(revision);
                self.count = Some(count);
                self.revision = revision;
                self.stage = Stage::Live;
                self.append(Val::Count { revision, count, state }, fx);
            }
            (Stage::Opening | Stage::Live, Input::ReadStale { .. }) => {}
            (Stage::Opening, Input::ReadFailed { revision }) if self.kind.reads_rows() => {
                let state = Ws { phase: Phase::Failed, error: true, loaded: None };
                self.window = state.clone();
                self.revision = revision;
                self.stage = Stage::Live;
                self.append(Val::Snapshot { revision, rows: Vec::new(), state }, fx);
            }
            (Stage::Opening, Input::Committed { revision, touches }) => {
                if touches && self.kind.reads_rows() { self.read_revision = revision; fx.push(Effect::Read(revision)); }
            }
            (Stage::Opening, Input::WindowState(state)) => self.held_state = Some(state),
            (Stage::Opening, Input::Pull) => {
                if self.waiting { fx.push(Effect::Answer(Pulled::Invalid)); } else { self.waiting = true; }
            }
            (Stage::Opening, Input::ModelsReset { own_model }) => {
                if own_model && self.kind.reads_rows() { fx.push(Effect::Read(self.read_revision)); }
            }
            (Stage::Opening, Input::Transfer { state }) if self.kind == Kind::Blob => self.status(state, fx),
            (Stage::Opening, Input::Retry { attempts }) if self.kind == Kind::Retry => self.retry(attempts, fx),
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
            (Stage::Opening | Stage::Live, Input::Closing) => self.end_now(Cause::Closed, fx),
            (Stage::Live, Input::Committed { revision, touches }) => {
                if touches && self.kind.reads_rows() { fx.push(Effect::Read(revision)); }
            }
            (Stage::Live, Input::ReadDone { revision, rows }) if self.kind.reads_rows() && self.kind != Kind::Count => {
                self.read_seen = self.read_seen.max(revision);
                self.live_rows(revision, rows, fx)
            }
            (Stage::Live, Input::CountDone { revision, count }) if self.kind == Kind::Count => {
                self.read_seen = self.read_seen.max(revision);
                if revision <= self.revision { return; }
                if self.count == Some(count) { self.same_rows(revision, fx); return; }
                let state = self.state_at(revision);
                self.count = Some(count);
                self.revision = revision;
                self.append(Val::Count { revision, count, state }, fx);
            }
            (Stage::Live, Input::ReadFailed { revision }) if self.kind.reads_rows() => {
                if revision <= self.revision { return; }
                let phase = if self.rows.is_empty() { Phase::Failed } else { Phase::Loaded };
                let state = Ws { phase, error: true, loaded: self.window.loaded };
                self.window = state.clone();
                self.append(Val::State { revision: self.revision, state }, fx);
            }
            (Stage::Live, Input::WindowState(state)) if self.kind.reads_rows() => {
                if state.visible_after() > self.read_seen {
                    fx.push(Effect::Read(state.visible_after()));
                    self.held_state = Some(state);
                }
                else if state != self.window {
                    self.window = state.clone();
                    self.held_state = None;
                    self.append(Val::State { revision: self.revision, state }, fx);
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
            (Stage::Live, Input::Transfer { state }) if self.kind == Kind::Blob => self.status(state, fx),
            (Stage::Live, Input::Retry { attempts }) if self.kind == Kind::Retry => self.retry(attempts, fx),
            (Stage::Live, Input::ModelsReset { own_model }) => {
                if own_model && self.kind.reads_rows() { fx.push(Effect::Read(self.revision)); }
            }
            (Stage::Live, Input::Pull) => {
                if self.waiting { fx.push(Effect::Answer(Pulled::Invalid)); }
                else if self.journal.is_empty() { self.waiting = true; }
                else { let answer = self.take(); fx.push(Effect::Answer(answer)); }
            }
            (Stage::Cancelling, Input::Cancel { reply }) => self.replies.push(reply),
            (Stage::Cancelling, Input::Pull) => {
                if self.waiting { fx.push(Effect::Answer(Pulled::Invalid)); } else { self.waiting = true; }
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

    fn dead_on_open(&mut self, cause: Cause, fx: &mut Vec<Effect>) {
        self.stage = Stage::Ended;
        self.cause = cause;
        self.sink_closed = true;
        self.journal.push_back(Val::Ended(cause));
        fx.push(Effect::Available);
    }

    fn same_rows(&mut self, read: i64, fx: &mut Vec<Effect>) {
        if self.held_state.as_ref().is_some_and(|held| held.visible_after() <= read) {
            let state = self.state_at(read);
            self.append(Val::State { revision: self.revision, state }, fx);
        }
    }

    fn live_rows(&mut self, revision: i64, rows: Vec<Row>, fx: &mut Vec<Effect>) {
        if revision <= self.revision { return; }
        if rows == self.rows { self.same_rows(revision, fx); return; }
        let state = self.state_at(revision);
        let old = std::mem::replace(&mut self.rows, rows.clone());
        self.revision = revision;
        let unread_snapshot = self.journal.iter().any(|kept| matches!(kept, Val::Snapshot { .. }));
        let ids: Vec<String> = rows.iter().map(|(id, _)| id.clone()).collect();
        let Some(base) = self.acknowledged.filter(|_| !unread_snapshot) else {
            self.append(Val::Snapshot { revision, rows: ids, state }, fx);
            return;
        };
        let base_rows = if self.acked_rows.is_empty() && old.is_empty() { Vec::new() } else { self.acked_rows.clone() };
        let upserted: Vec<String> = rows.iter().filter(|row| !base_rows.contains(row)).map(|(id, _)| id.clone()).collect();
        let removed: Vec<String> = base_rows.iter().filter(|(id, _)| !rows.iter().any(|(now, _)| now == id)).map(|(id, _)| id.clone()).collect();
        if upserted.len() + removed.len() > DELTA_LIMIT {
            self.append(Val::Snapshot { revision, rows: ids, state }, fx);
        } else {
            self.append(Val::Delta { base, revision, size: rows.len(), upserted, removed, state }, fx);
        }
    }

    fn replay(&mut self, fx: &mut Vec<Effect>) {
        for held in std::mem::take(&mut self.held_events) {
            match held {
                Held::Event(position, payload) => self.event(position, payload, fx),
                Held::Lost(first, last) => self.lost(first, last, fx),
                Held::Gap => if self.next_feed.is_none() || !self.ready { self.held_events.push(Held::Gap); } else { self.append(Val::Gap, fx); },
            }
        }
    }

    fn event(&mut self, position: u64, payload: String, fx: &mut Vec<Effect>) {
        let Some(next) = self.next_feed.filter(|_| self.ready) else { self.held_events.push(Held::Event(position, payload)); return; };
        if position < next { return; }
        if position > next {
            let gap = position - next;
            self.append(Val::Lost { first: self.position + 1, last: self.position + gap }, fx);
            self.position += gap;
        }
        self.position += 1;
        self.append(Val::Event { position: self.position, payload }, fx);
        self.next_feed = Some(position + 1);
    }

    fn lost(&mut self, first: u64, last: u64, fx: &mut Vec<Effect>) {
        let Some(next) = self.next_feed.filter(|_| self.ready) else { self.held_events.push(Held::Lost(first, last)); return; };
        if last < next { return; }
        let count = last - first.max(next) + 1;
        self.append(Val::Lost { first: self.position + 1, last: self.position + count }, fx);
        self.position += count;
        self.next_feed = Some(last + 1);
    }

    fn status(&mut self, state: String, fx: &mut Vec<Effect>) {
        if self.last_status.as_ref() == Some(&state) { return; }
        self.stage = Stage::Live;
        self.status_revision += 1;
        self.last_status = Some(state.clone());
        self.append(Val::Status { revision: self.status_revision, state }, fx);
    }

    fn retry(&mut self, attempts: u32, fx: &mut Vec<Effect>) {
        if self.last_retry == Some(attempts) { return; }
        self.stage = Stage::Live;
        self.last_retry = Some(attempts);
        self.append(Val::Retry { attempts }, fx);
    }
}
