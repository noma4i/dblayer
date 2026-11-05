//! Owner `Deliveries`: a dictionary of `Delivery` machines by stream id (one machine per stream).
//! Only this owner hands values to the platform. A table row applies to the machine of the stream the input names;
//! `Committed`, `EpochStarted`, `EpochEnded`, `ModelsReset`, `Closing` apply to every machine.

use std::collections::{BTreeMap, VecDeque};

use crate::db::{Diff, Entry, Stamp};
use crate::ffi::WireRow;
use crate::identity::RowId;
use crate::owners::contract::{Committed, Drained, EpochEnded, EpochStarted, Models, ModelsReset, ReplyId, WindowPhase, WindowState};
use crate::owners::read::feed::{Feed, FeedInput};
use crate::owners::read::windows::{Goal, WindowKey, Windows, WindowsInput};
use crate::runtime::{Address, Cause, Effect, Effects, Epoch, Outbound, OutcomeId, StateOwner, Time};

pub const JOURNAL_LIMIT: usize = 256;
pub const DELTA_LIMIT: usize = 256;

pub type StreamId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fetch {
    Never,
    Stale,
    Force,
    IfMissing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    List,
    Entity { fetch: Fetch },
    Count,
    Events,
    Blob,
    RetryStatus,
}

impl Kind {
    fn reads_rows(self) -> bool { matches!(self, Kind::List | Kind::Entity { .. } | Kind::Count) }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ReadData {
    Rows { rows: Vec<StreamRow>, replaced: Vec<(String, String)> },
    Count(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct View {
    pub kind: Kind,
    pub model: String,
    pub tables: Vec<String>,
    pub local: bool,
    /// The stream reads a window: its relation is loaded from the server.
    pub remote: bool,
    /// The model takes events of the server (`updates` of a relation or `listens`): an open stream wants its subscription.
    pub live: bool,
    pub window: WindowKey,
    pub subject: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StreamRow {
    pub id: String,
    pub stamp: i64,
    pub row: Option<WireRow>,
}

impl StreamRow {
    pub fn new(id: &str, stamp: i64) -> Self { Self { id: id.to_string(), stamp, row: None } }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndCause {
    SessionEnded,
    Closed,
    Cancelled,
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobStatus {
    pub state: String,
    pub progress: u64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryStatus {
    pub attempts: u32,
    pub phase: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Delta {
    pub base: i64,
    pub revision: i64,
    pub size: u32,
    pub rows: Vec<StreamRow>,
    pub indexes: Vec<u32>,
    pub removed: Vec<String>,
    pub placed: Vec<(String, u32)>,
    pub replaced: Vec<(String, String)>,
    pub renamed: Vec<(String, String)>,
    pub state: WindowState,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Snapshot { revision: i64, rows: Vec<StreamRow>, state: WindowState },
    Delta(Delta),
    Count { revision: i64, count: u64, state: WindowState },
    State { revision: i64, state: WindowState },
    Ready,
    Event { position: u64, payload: String },
    Lost { first: u64, last: u64 },
    Gap,
    Status { revision: i64, blob: BlobStatus },
    Retry(RetryStatus),
    Ended(EndCause),
}

impl Value {
    fn is_rows(&self) -> bool { matches!(self, Value::Snapshot { .. } | Value::Delta(_) | Value::Count { .. }) }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PullAnswer {
    Value(Value),
    Done,
    Invalid,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StreamEffect {
    Read { read_id: u64, revision: i64, view: View },
    Answer { outcome: OutcomeId, answer: PullAnswer },
    Available,
    CloseSink,
    Cancelled { reply: ReplyId },
    StatusInterest { plus: bool },
}

enum Emit {
    Run(StreamEffect),
    Load { force: bool },
    InterestPlus,
    InterestMinus,
    WantPlus,
    WantMinus,
}

#[derive(Clone, Copy, Debug)]
pub struct DeliveryPeers {
    pub windows: Address<Windows>,
    pub feed: Address<Feed>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DeliveryEffect {
    Stream { stream: StreamId, effect: StreamEffect },
    Drained(Drained),
}

#[derive(Clone, Debug)]
pub enum DeliveryInput {
    Open { stream: StreamId, view: View, epoch: Epoch, revision: i64, identity: bool, valid: bool },
    Committed(Committed),
    ReadDone { stream: StreamId, read_id: u64, revision: i64, data: ReadData },
    ReadFailed { stream: StreamId, read_id: u64, revision: i64, error: String },
    WindowState { stream: StreamId, state: WindowState },
    Joined { stream: StreamId, position: u64 },
    ModelReady { stream: StreamId },
    ModelEvent { stream: StreamId, position: u64, payload: String },
    ModelLost { stream: StreamId, first: u64, last: u64 },
    ModelGap { stream: StreamId },
    TransferStatus { stream: StreamId, blob: BlobStatus },
    RetryState { stream: StreamId, retry: RetryStatus },
    Pull { stream: StreamId, outcome: OutcomeId },
    Cancel { stream: StreamId, reply: ReplyId },
    Release { stream: StreamId },
    SinkClosed { stream: StreamId },
    EpochStarted(EpochStarted),
    EpochEnded(EpochEnded),
    ModelsReset(ModelsReset),
    Closing,
    Tick(Time),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryStage {
    Opening,
    Live,
    Cancelling,
    Ended,
}

#[derive(Clone, Debug)]
enum Held {
    Event { position: u64, payload: String },
    Lost { first: u64, last: u64 },
    Gap,
}

#[derive(Debug)]
struct Delivery {
    epoch: Epoch,
    view: View,
    stage: DeliveryStage,
    read_revision: i64,
    read_id: u64,
    /// The revision of the last read that finished: the rows of the stream are current up to it, `revision` is the revision of the last value of rows.
    read_seen: i64,
    revision: i64,
    acknowledged: Option<i64>,
    rows: Vec<StreamRow>,
    acked_rows: Vec<StreamRow>,
    count: Option<u64>,
    window: WindowState,
    held_state: Option<WindowState>,
    next_feed: Option<u64>,
    position: u64,
    ready: bool,
    held_events: Vec<Held>,
    blob: Option<BlobStatus>,
    retry: Option<RetryStatus>,
    status_revision: i64,
    journal: VecDeque<Value>,
    waiting: Option<OutcomeId>,
    ended_given: bool,
    cause: EndCause,
    replies: Vec<ReplyId>,
    closing: bool,
    released: bool,
    sink_closed: bool,
    interested: bool,
    /// A read stream of a live model holds a `Want+` of the Feed.
    wants: bool,
    opened: i64,
}

fn visible_after(state: &WindowState) -> i64 { state.loaded.unwrap_or(0) }

fn entry(row: &StreamRow) -> Result<Entry, String> {
    let id = RowId::new(&row.id).map_err(|error| error.to_string())?;
    Ok(Entry { id, stamp: Stamp { revision: row.stamp, ..Stamp::default() } })
}

impl Delivery {
    fn new(epoch: Epoch, view: View, stage: DeliveryStage, revision: i64) -> Self {
        let window = if view.remote { WindowState::idle() } else { WindowState { phase: WindowPhase::Loaded, error: None, loaded: None } };
        Self {
            epoch,
            view,
            stage,
            read_revision: revision,
            read_id: 0,
            read_seen: 0,
            revision: 0,
            acknowledged: None,
            rows: Vec::new(),
            acked_rows: Vec::new(),
            count: None,
            window,
            held_state: None,
            next_feed: None,
            position: 0,
            ready: false,
            held_events: Vec::new(),
            blob: None,
            retry: None,
            status_revision: 0,
            journal: VecDeque::new(),
            waiting: None,
            ended_given: false,
            cause: EndCause::Cancelled,
            replies: Vec::new(),
            closing: false,
            released: false,
            sink_closed: false,
            interested: false,
            wants: false,
            opened: revision,
        }
    }

    fn open(view: View, epoch: Epoch, revision: i64, fx: &mut Vec<Emit>) -> Self {
        let kind = view.kind;
        let mut machine = Self::new(epoch, view, DeliveryStage::Opening, revision);
        match kind {
            Kind::Events => {
                machine.stage = DeliveryStage::Live;
                fx.push(Emit::WantPlus);
                machine.interested = true;
            }
            Kind::Blob | Kind::RetryStatus => {
                fx.push(Emit::Run(StreamEffect::StatusInterest { plus: true }));
                machine.interested = true;
            }
            Kind::List | Kind::Count | Kind::Entity { .. } => {
                fx.push(machine.new_read(revision));
                if machine.view.live {
                    fx.push(Emit::WantPlus);
                    machine.wants = true;
                }
                let watches = machine.view.remote
                    && matches!(kind, Kind::List | Kind::Count | Kind::Entity { fetch: Fetch::Stale | Fetch::Force | Fetch::IfMissing });
                if watches {
                    fx.push(Emit::InterestPlus);
                    machine.interested = true;
                    if kind == (Kind::Entity { fetch: Fetch::Force }) {
                        fx.push(Emit::Load { force: true });
                    }
                }
            }
        }
        machine
    }

    fn ended(view: View, epoch: Epoch, cause: EndCause, fx: &mut Vec<Emit>) -> Self {
        let mut machine = Self::new(epoch, view, DeliveryStage::Ended, 0);
        machine.journal.push_back(Value::Ended(cause));
        machine.cause = cause;
        machine.sink_closed = true;
        fx.push(Emit::Run(StreamEffect::Available));
        machine
    }

    fn new_read(&mut self, revision: i64) -> Emit {
        self.read_id += 1;
        self.read_revision = revision;
        Emit::Run(StreamEffect::Read { read_id: self.read_id, revision, view: self.view.clone() })
    }

    fn sink_open(&self) -> bool { matches!(self.stage, DeliveryStage::Opening | DeliveryStage::Live) }

    fn take_first(&mut self) -> Option<PullAnswer> {
        match self.journal.pop_front() {
            Some(value) => {
                match &value {
                    Value::Ended(_) => {
                        self.ended_given = true;
                        self.journal.clear();
                    }
                    Value::Snapshot { revision, .. } | Value::Count { revision, .. } => self.acknowledge(*revision),
                    Value::Delta(delta) => self.acknowledge(delta.revision),
                    _ => {}
                }
                Some(PullAnswer::Value(value))
            }
            None if self.ended_given => Some(PullAnswer::Done),
            None => None,
        }
    }

    fn acknowledge(&mut self, revision: i64) {
        self.acknowledged = Some(revision);
        self.acked_rows = self.rows.clone();
    }

    /// `Failed` only without rows, an error with rows is `Loaded` with the error: the rows of the snapshot decide, not the window.
    fn shown(&self, mut state: WindowState) -> WindowState {
        if state.error.is_some() && matches!(state.phase, WindowPhase::Loaded | WindowPhase::Failed) {
            let has_rows = if self.view.kind == Kind::Count { self.count.is_some_and(|count| count > 0) } else { !self.rows.is_empty() };
            state.phase = if has_rows { WindowPhase::Loaded } else { WindowPhase::Failed };
        }
        state
    }

    fn push(&mut self, value: Value, fx: &mut Vec<Emit>) {
        let value = match value {
            Value::Snapshot { revision, rows, state } => Value::Snapshot { revision, rows, state: self.shown(state) },
            Value::Count { revision, count, state } => Value::Count { revision, count, state: self.shown(state) },
            Value::State { revision, state } => Value::State { revision, state: self.shown(state) },
            Value::Delta(mut delta) => {
                delta.state = self.shown(delta.state);
                Value::Delta(delta)
            }
            other => other,
        };
        let was_empty = self.journal.is_empty();
        match &value {
            Value::Snapshot { .. } | Value::Count { .. } => self.journal.retain(|kept| !matches!(kept, Value::Delta(_) | Value::Snapshot { .. } | Value::Count { .. } | Value::State { .. })),
            Value::Delta(_) if self.journal.iter().any(Value::is_rows) => self.journal.retain(|kept| !matches!(kept, Value::Delta(_) | Value::Snapshot { .. } | Value::Count { .. } | Value::State { .. })),
            Value::State { .. } => self.journal.retain(|kept| !matches!(kept, Value::State { .. })),
            Value::Status { .. } => self.journal.retain(|kept| !matches!(kept, Value::Status { .. })),
            Value::Retry(_) => self.journal.retain(|kept| !matches!(kept, Value::Retry(_))),
            _ => {}
        }
        self.journal.push_back(value);
        if self.journal.len() > JOURNAL_LIMIT {
            self.compact();
        }
        if let Some(outcome) = self.waiting.take() {
            let answer = self.take_first().unwrap_or(PullAnswer::Done);
            fx.push(Emit::Run(StreamEffect::Answer { outcome, answer }));
        } else if was_empty && self.sink_open() {
            fx.push(Emit::Run(StreamEffect::Available));
        }
    }

    fn compact(&mut self) {
        let (mut first, mut last, mut at) = (u64::MAX, 0u64, None);
        while self.journal.len() > JOURNAL_LIMIT - 1 {
            let Some(index) = self.journal.iter().position(|kept| matches!(kept, Value::Event { .. } | Value::Lost { .. })) else { break };
            at.get_or_insert(index);
            match self.journal.remove(index) {
                Some(Value::Event { position, .. }) => {
                    first = first.min(position);
                    last = last.max(position);
                }
                Some(Value::Lost { first: from, last: to }) => {
                    first = first.min(from);
                    last = last.max(to);
                }
                _ => {}
            }
        }
        if let Some(index) = at {
            self.journal.insert(index, Value::Lost { first, last });
        }
    }

    fn state_at(&mut self, revision: i64) -> WindowState {
        if self.held_state.as_ref().is_some_and(|held| visible_after(held) <= revision) {
            if let Some(held) = self.held_state.take() {
                self.window = held;
            }
        }
        self.window.clone()
    }

    fn unwant(&mut self) -> Option<Emit> { std::mem::take(&mut self.wants).then_some(Emit::WantMinus) }

    fn leave(&mut self) -> Option<Emit> {
        if !std::mem::take(&mut self.interested) {
            return None;
        }
        Some(match self.view.kind {
            Kind::Events => Emit::WantMinus,
            Kind::Blob | Kind::RetryStatus => Emit::Run(StreamEffect::StatusInterest { plus: false }),
            _ => Emit::InterestMinus,
        })
    }

    fn touches(&self, committed: &Committed) -> bool { committed.models == Models::All || committed.tables.iter().any(|table| self.view.tables.contains(table)) }

    fn committed(&mut self, committed: &Committed, fx: &mut Vec<Emit>) {
        if !self.view.kind.reads_rows() || !self.touches(committed) {
            return;
        }
        if matches!(self.stage, DeliveryStage::Opening | DeliveryStage::Live) {
            fx.push(self.new_read(committed.revision));
        }
    }

    fn read_done(&mut self, read_id: u64, revision: i64, data: ReadData, fx: &mut Vec<Emit>) {
        if read_id != self.read_id || !self.view.kind.reads_rows() {
            return;
        }
        self.read_seen = self.read_seen.max(revision);
        match (self.stage, data) {
            (DeliveryStage::Opening, ReadData::Rows { rows, .. }) if self.view.kind != Kind::Count => {
                let missing = rows.is_empty() && self.view.kind == (Kind::Entity { fetch: Fetch::IfMissing });
                let state = self.state_at(revision);
                self.rows = rows.clone();
                self.revision = revision;
                self.stage = DeliveryStage::Live;
                self.push(Value::Snapshot { revision, rows, state }, fx);
                if missing {
                    fx.push(Emit::Load { force: false });
                }
            }
            (DeliveryStage::Opening, ReadData::Count(now)) if self.view.kind == Kind::Count => {
                let state = self.state_at(revision);
                self.count = Some(now);
                self.revision = revision;
                self.stage = DeliveryStage::Live;
                self.push(Value::Count { revision, count: now, state }, fx);
            }
            (DeliveryStage::Live, ReadData::Rows { rows, replaced }) if self.view.kind != Kind::Count => self.live_rows(revision, rows, replaced, fx),
            (DeliveryStage::Live, ReadData::Count(now)) if self.view.kind == Kind::Count && revision > self.revision => {
                if self.count != Some(now) {
                    let state = self.state_at(revision);
                    self.count = Some(now);
                    self.revision = revision;
                    self.push(Value::Count { revision, count: now, state }, fx);
                } else {
                    self.same_rows(revision, fx);
                }
            }
            _ => {}
        }
    }

    fn same_rows(&mut self, read: i64, fx: &mut Vec<Emit>) {
        if self.held_state.as_ref().is_some_and(|held| visible_after(held) <= read) {
            let state = self.state_at(read);
            self.push(Value::State { revision: self.revision, state }, fx);
        }
    }

    fn live_rows(&mut self, revision: i64, rows: Vec<StreamRow>, replaced: Vec<(String, String)>, fx: &mut Vec<Emit>) {
        if revision <= self.revision {
            return;
        }
        if rows == self.rows {
            self.same_rows(revision, fx);
            return;
        }
        let state = self.state_at(revision);
        self.rows = rows.clone();
        self.revision = revision;
        let unread_snapshot = self.journal.iter().any(|kept| matches!(kept, Value::Snapshot { .. }));
        let Some(base) = self.acknowledged.filter(|_| !unread_snapshot) else {
            self.push(Value::Snapshot { revision, rows, state }, fx);
            return;
        };
        match self.delta(base, revision, &rows, &replaced, state.clone()) {
            Some(delta) => self.push(Value::Delta(delta), fx),
            None => self.push(Value::Snapshot { revision, rows, state }, fx),
        }
    }

    fn delta(&self, base: i64, revision: i64, rows: &[StreamRow], replaced: &[(String, String)], state: WindowState) -> Option<Delta> {
        let old: Vec<Entry> = self.acked_rows.iter().map(entry).collect::<Result<_, _>>().ok()?;
        let new: Vec<Entry> = rows.iter().map(entry).collect::<Result<_, _>>().ok()?;
        let pairs: Vec<(RowId, RowId)> = replaced.iter().map(|(from, to)| Ok((RowId::new(from).map_err(|e| e.to_string())?, RowId::new(to).map_err(|e| e.to_string())?))).collect::<Result<_, String>>().ok()?;
        let diff = Diff::between(&old[..], &new[..], &pairs);
        if diff.at.len() + diff.removed.len() > DELTA_LIMIT {
            return None;
        }
        let mut at: Vec<(String, usize)> = diff.at.iter().map(|(id, index)| (id.to_string(), *index)).collect();
        at.sort_by_key(|(_, index)| *index);
        let by_id: BTreeMap<&str, &StreamRow> = rows.iter().map(|row| (row.id.as_str(), row)).collect();
        let changed: Vec<StreamRow> = at.iter().filter_map(|(id, _)| by_id.get(id.as_str()).map(|row| (*row).clone())).collect();
        let pair = |list: &[(RowId, RowId)]| list.iter().map(|(from, to)| (from.to_string(), to.to_string())).collect::<Vec<_>>();
        Some(Delta {
            base,
            revision,
            size: u32::try_from(rows.len()).unwrap_or(u32::MAX),
            indexes: at.iter().map(|(_, index)| u32::try_from(*index).unwrap_or(u32::MAX)).collect(),
            rows: changed,
            removed: diff.removed.iter().map(ToString::to_string).collect(),
            placed: diff.placed().into_iter().map(|(id, index)| (id.to_string(), u32::try_from(index).unwrap_or(u32::MAX))).collect(),
            replaced: pair(&diff.replaced),
            renamed: pair(&diff.renamed),
            state,
        })
    }

    fn read_failed(&mut self, read_id: u64, revision: i64, error: String, fx: &mut Vec<Emit>) {
        if read_id != self.read_id || !self.view.kind.reads_rows() {
            return;
        }
        match self.stage {
            DeliveryStage::Opening => {
                let state = WindowState { phase: WindowPhase::Failed, error: Some(error), loaded: None };
                self.window = state.clone();
                self.revision = revision;
                self.rows = Vec::new();
                self.stage = DeliveryStage::Live;
                self.push(Value::Snapshot { revision, rows: Vec::new(), state }, fx);
            }
            DeliveryStage::Live if revision > self.revision => {
                let phase = if self.rows.is_empty() { WindowPhase::Failed } else { WindowPhase::Loaded };
                let state = WindowState { phase, error: Some(error), loaded: self.window.loaded };
                self.window = state.clone();
                self.push(Value::State { revision: self.revision, state }, fx);
            }
            _ => {}
        }
    }

    fn window_state(&mut self, state: WindowState, fx: &mut Vec<Emit>) {
        match self.stage {
            DeliveryStage::Opening => self.held_state = Some(state),
            DeliveryStage::Live if self.view.kind.reads_rows() => {
                let visible = visible_after(&state);
                if visible > self.read_seen {
                    self.held_state = Some(state);
                    fx.push(self.new_read(visible));
                } else if state != self.window {
                    self.window = state.clone();
                    self.held_state = None;
                    self.push(Value::State { revision: self.revision, state }, fx);
                }
            }
            _ => {}
        }
    }

    fn events_open(&self) -> bool { self.stage == DeliveryStage::Live && self.view.kind == Kind::Events }

    fn joined(&mut self, position: u64, fx: &mut Vec<Emit>) {
        if self.events_open() && self.next_feed.is_none() {
            self.next_feed = Some(position + 1);
            self.replay(fx);
        }
    }

    fn model_ready(&mut self, fx: &mut Vec<Emit>) {
        if self.events_open() && !self.ready {
            self.ready = true;
            self.push(Value::Ready, fx);
            self.replay(fx);
        }
    }

    fn replay(&mut self, fx: &mut Vec<Emit>) {
        for held in std::mem::take(&mut self.held_events) {
            match held {
                Held::Event { position, payload } => self.model_event(position, payload, fx),
                Held::Lost { first, last } => self.model_lost(first, last, fx),
                Held::Gap => self.model_gap(fx),
            }
        }
    }

    fn holds(&self) -> bool { self.next_feed.is_none() || !self.ready }

    fn model_event(&mut self, position: u64, payload: String, fx: &mut Vec<Emit>) {
        if !self.events_open() {
            return;
        }
        if self.holds() {
            self.held_events.push(Held::Event { position, payload });
            return;
        }
        let next = self.next_feed.unwrap_or(0);
        if position < next {
            return;
        }
        if position > next {
            let gap = position - next;
            self.push(Value::Lost { first: self.position + 1, last: self.position + gap }, fx);
            self.position += gap;
        }
        self.position += 1;
        self.push(Value::Event { position: self.position, payload }, fx);
        self.next_feed = Some(position + 1);
    }

    fn model_lost(&mut self, first: u64, last: u64, fx: &mut Vec<Emit>) {
        if !self.events_open() {
            return;
        }
        if self.holds() {
            self.held_events.push(Held::Lost { first, last });
            return;
        }
        let next = self.next_feed.unwrap_or(0);
        if last < next {
            return;
        }
        let count = last - first.max(next) + 1;
        self.push(Value::Lost { first: self.position + 1, last: self.position + count }, fx);
        self.position += count;
        self.next_feed = Some(last + 1);
    }

    fn model_gap(&mut self, fx: &mut Vec<Emit>) {
        if !self.events_open() {
            return;
        }
        if self.holds() {
            self.held_events.push(Held::Gap);
            return;
        }
        self.push(Value::Gap, fx);
    }

    fn status(&mut self, blob: BlobStatus, fx: &mut Vec<Emit>) {
        if self.view.kind != Kind::Blob || !self.sink_open() || self.blob.as_ref() == Some(&blob) {
            return;
        }
        self.stage = DeliveryStage::Live;
        self.status_revision += 1;
        self.blob = Some(blob.clone());
        self.push(Value::Status { revision: self.status_revision, blob }, fx);
    }

    fn retry_state(&mut self, retry: RetryStatus, fx: &mut Vec<Emit>) {
        if self.view.kind != Kind::RetryStatus || !self.sink_open() || self.retry.as_ref() == Some(&retry) {
            return;
        }
        self.stage = DeliveryStage::Live;
        self.retry = Some(retry.clone());
        self.push(Value::Retry(retry), fx);
    }

    fn models_reset(&mut self, reset: &ModelsReset, fx: &mut Vec<Emit>) {
        if !self.view.kind.reads_rows() || !reset.models.contains(&self.view.model) {
            return;
        }
        match self.stage {
            DeliveryStage::Opening => fx.push(self.new_read(self.read_revision)),
            DeliveryStage::Live => fx.push(self.new_read(self.revision)),
            DeliveryStage::Cancelling | DeliveryStage::Ended => {}
        }
    }

    fn pull(&mut self, outcome: OutcomeId, fx: &mut Vec<Emit>) {
        if self.waiting.is_some() {
            fx.push(Emit::Run(StreamEffect::Answer { outcome, answer: PullAnswer::Invalid }));
            return;
        }
        match self.take_first() {
            Some(answer) => fx.push(Emit::Run(StreamEffect::Answer { outcome, answer })),
            None if self.stage == DeliveryStage::Ended => fx.push(Emit::Run(StreamEffect::Answer { outcome, answer: PullAnswer::Done })),
            None => self.waiting = Some(outcome),
        }
    }

    fn cancel(&mut self, reply: ReplyId, fx: &mut Vec<Emit>) {
        match self.stage {
            DeliveryStage::Opening | DeliveryStage::Live => {
                fx.extend(self.leave());
                fx.extend(self.unwant());
                self.stage = DeliveryStage::Cancelling;
                self.cause = EndCause::Cancelled;
                self.journal.clear();
                self.replies.push(reply);
                fx.push(Emit::Run(StreamEffect::CloseSink));
            }
            DeliveryStage::Cancelling => self.replies.push(reply),
            DeliveryStage::Ended if self.sink_closed => fx.push(Emit::Run(StreamEffect::Cancelled { reply })),
            DeliveryStage::Ended => self.replies.push(reply),
        }
    }

    fn release(&mut self, fx: &mut Vec<Emit>) {
        match self.stage {
            DeliveryStage::Opening | DeliveryStage::Live => {
                fx.extend(self.leave());
                fx.extend(self.unwant());
                self.stage = DeliveryStage::Cancelling;
                self.cause = EndCause::Cancelled;
                self.journal.clear();
                self.released = true;
                fx.push(Emit::Run(StreamEffect::CloseSink));
            }
            DeliveryStage::Cancelling | DeliveryStage::Ended => self.released = true,
        }
    }

    fn finish(&mut self, cause: EndCause, closing: bool, fx: &mut Vec<Emit>) {
        fx.extend(self.leave());
                fx.extend(self.unwant());
        self.stage = DeliveryStage::Ended;
        self.cause = cause;
        self.closing = closing;
        self.journal.clear();
        self.journal.push_back(Value::Ended(cause));
        match self.waiting.take() {
            Some(outcome) => {
                let answer = self.take_first().unwrap_or(PullAnswer::Done);
                fx.push(Emit::Run(StreamEffect::Answer { outcome, answer }));
            }
            None => fx.push(Emit::Run(StreamEffect::Available)),
        }
        fx.push(Emit::Run(StreamEffect::CloseSink));
    }

    fn epoch_ended(&mut self, ended: &EpochEnded, fx: &mut Vec<Emit>) {
        if ended.epoch != self.epoch {
            return;
        }
        let cause = if ended.cause == Cause::Close { EndCause::Closed } else { EndCause::SessionEnded };
        match self.stage {
            DeliveryStage::Opening | DeliveryStage::Live => self.finish(cause, false, fx),
            DeliveryStage::Cancelling => self.cause = cause,
            DeliveryStage::Ended => {}
        }
    }

    fn closing(&mut self, fx: &mut Vec<Emit>) {
        match self.stage {
            DeliveryStage::Opening | DeliveryStage::Live => self.finish(EndCause::Closed, true, fx),
            DeliveryStage::Cancelling => {
                self.cause = EndCause::Closed;
                self.closing = true;
            }
            DeliveryStage::Ended => self.closing = true,
        }
    }

    fn sink_closed(&mut self, fx: &mut Vec<Emit>) {
        match self.stage {
            DeliveryStage::Cancelling => {
                for reply in self.replies.drain(..) {
                    fx.push(Emit::Run(StreamEffect::Cancelled { reply }));
                }
                self.stage = DeliveryStage::Ended;
                self.sink_closed = true;
                if self.released {
                    return;
                }
                self.journal.clear();
                self.journal.push_back(Value::Ended(self.cause));
                if let Some(outcome) = self.waiting.take() {
                    let answer = self.take_first().unwrap_or(PullAnswer::Done);
                    fx.push(Emit::Run(StreamEffect::Answer { outcome, answer }));
                }
            }
            DeliveryStage::Ended if !self.sink_closed => {
                self.sink_closed = true;
                for reply in self.replies.drain(..) {
                    fx.push(Emit::Run(StreamEffect::Cancelled { reply }));
                }
            }
            _ => {}
        }
    }

    fn gone(&self) -> bool { self.stage == DeliveryStage::Ended && self.released && self.sink_closed }

    fn settled(&self) -> bool { self.stage == DeliveryStage::Ended && self.sink_closed }
}

pub struct DeliveriesState {
    epoch: Epoch,
    streams: BTreeMap<StreamId, Delivery>,
    peers: DeliveryPeers,
    closing: bool,
    drained: bool,
}

impl DeliveriesState {
    pub fn new(epoch: Epoch, peers: DeliveryPeers) -> Self { Self { epoch, streams: BTreeMap::new(), peers, closing: false, drained: false } }

    pub fn stage(&self, stream: StreamId) -> Option<DeliveryStage> { self.streams.get(&stream).map(|machine| machine.stage) }

    pub fn journal_len(&self, stream: StreamId) -> usize { self.streams.get(&stream).map_or(0, |machine| machine.journal.len()) }

    pub fn read_id(&self, stream: StreamId) -> u64 { self.streams.get(&stream).map_or(0, |machine| machine.read_id) }

    pub fn streams(&self) -> usize { self.streams.len() }

    pub fn journal(&self, stream: StreamId) -> Vec<Value> { self.streams.get(&stream).map(|machine| machine.journal.iter().cloned().collect()).unwrap_or_default() }

    pub fn acknowledged(&self, stream: StreamId) -> Option<i64> { self.streams.get(&stream).and_then(|machine| machine.acknowledged) }
}

pub struct Deliveries;

fn lower(peers: DeliveryPeers, stream: StreamId, machine: &Delivery, emits: Vec<Emit>, out: &mut Effects<Deliveries>) {
    for emit in emits {
        let effect = match emit {
            Emit::Run(effect) => Effect::Run(DeliveryEffect::Stream { stream, effect }),
            Emit::InterestPlus => Effect::Send(Outbound::new(peers.windows, WindowsInput::InterestPlus { key: machine.view.window.clone(), id: stream })),
            Emit::Load { force } => Effect::Send(Outbound::new(peers.windows, WindowsInput::Load { key: machine.view.window.clone(), id: stream, goal: Goal::First, force, store_revision: machine.opened })),
            Emit::InterestMinus => Effect::Send(Outbound::new(peers.windows, WindowsInput::InterestMinus { key: machine.view.window.clone(), id: stream })),
            Emit::WantPlus => Effect::Send(Outbound::new(peers.feed, FeedInput::WantPlus { model: machine.view.model.clone(), id: stream, stream: machine.view.kind == Kind::Events })),
            Emit::WantMinus => Effect::Send(Outbound::new(peers.feed, FeedInput::WantMinus { model: machine.view.model.clone(), id: stream })),
        };
        out.push(effect);
    }
}

impl StateOwner for Deliveries {
    const EPOCHED: bool = true;

    type State = DeliveriesState;
    type Input = DeliveryInput;
    type Effect = DeliveryEffect;

    fn step(state: &mut DeliveriesState, input: DeliveryInput) -> Effects<Self> {
        let mut out: Effects<Self> = Vec::new();
        match input {
            DeliveryInput::Open { stream, view, epoch, revision, identity, valid } => {
                if state.streams.contains_key(&stream) {
                    return out;
                }
                let mut fx = Vec::new();
                let machine = if !valid {
                    Delivery::ended(view, epoch, EndCause::Invalid, &mut fx)
                } else if epoch != state.epoch || (!identity && !view.local && !matches!(view.kind, Kind::Blob | Kind::RetryStatus)) {
                    Delivery::ended(view, epoch, EndCause::SessionEnded, &mut fx)
                } else {
                    Delivery::open(view, epoch, revision, &mut fx)
                };
                lower(state.peers, stream, &machine, fx, &mut out);
                state.streams.insert(stream, machine);
            }
            DeliveryInput::ReadDone { stream, read_id, revision, data } => on(state, stream, &mut out, |machine, fx| machine.read_done(read_id, revision, data, fx)),
            DeliveryInput::ReadFailed { stream, read_id, revision, error } => on(state, stream, &mut out, |machine, fx| machine.read_failed(read_id, revision, error, fx)),
            DeliveryInput::WindowState { stream, state: window } => on(state, stream, &mut out, |machine, fx| machine.window_state(window, fx)),
            DeliveryInput::Joined { stream, position } => on(state, stream, &mut out, |machine, fx| machine.joined(position, fx)),
            DeliveryInput::ModelReady { stream } => on(state, stream, &mut out, |machine, fx| machine.model_ready(fx)),
            DeliveryInput::ModelEvent { stream, position, payload } => on(state, stream, &mut out, |machine, fx| machine.model_event(position, payload, fx)),
            DeliveryInput::ModelLost { stream, first, last } => on(state, stream, &mut out, |machine, fx| machine.model_lost(first, last, fx)),
            DeliveryInput::ModelGap { stream } => on(state, stream, &mut out, |machine, fx| machine.model_gap(fx)),
            DeliveryInput::TransferStatus { stream, blob } => on(state, stream, &mut out, |machine, fx| machine.status(blob, fx)),
            DeliveryInput::RetryState { stream, retry } => on(state, stream, &mut out, |machine, fx| machine.retry_state(retry, fx)),
            DeliveryInput::Cancel { stream, reply } => {
                if state.streams.contains_key(&stream) {
                    on(state, stream, &mut out, |machine, fx| machine.cancel(reply, fx));
                } else {
                    out.push(Effect::Run(DeliveryEffect::Stream { stream, effect: StreamEffect::Cancelled { reply } }));
                }
            }
            DeliveryInput::Release { stream } => on(state, stream, &mut out, |machine, fx| machine.release(fx)),
            DeliveryInput::SinkClosed { stream } => on(state, stream, &mut out, |machine, fx| machine.sink_closed(fx)),
            DeliveryInput::Pull { stream, outcome } => {
                if state.streams.contains_key(&stream) {
                    on(state, stream, &mut out, |machine, fx| machine.pull(outcome, fx));
                } else {
                    out.push(Effect::Run(DeliveryEffect::Stream { stream, effect: StreamEffect::Answer { outcome, answer: PullAnswer::Done } }));
                }
            }
            DeliveryInput::Committed(committed) => every(state, &mut out, |machine, fx| machine.committed(&committed, fx)),
            DeliveryInput::ModelsReset(reset) => every(state, &mut out, |machine, fx| machine.models_reset(&reset, fx)),
            DeliveryInput::EpochEnded(ended) => every(state, &mut out, |machine, fx| machine.epoch_ended(&ended, fx)),
            DeliveryInput::EpochStarted(started) => state.epoch = started.epoch,
            DeliveryInput::Closing => {
                state.closing = true;
                every(state, &mut out, |machine, fx| machine.closing(fx));
            }
            DeliveryInput::Tick(_) => {}
        }
        state.streams.retain(|_, machine| !machine.gone());
        if state.closing && !state.drained && state.streams.values().all(Delivery::settled) {
            state.drained = true;
            out.push(Effect::Run(DeliveryEffect::Drained(Drained { owner: "deliveries" })));
        }
        out
    }

    fn tick(now: Time) -> DeliveryInput { DeliveryInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> DeliveryInput { DeliveryInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> DeliveryInput { DeliveryInput::Closing }
}

fn on(state: &mut DeliveriesState, stream: StreamId, out: &mut Effects<Deliveries>, apply: impl FnOnce(&mut Delivery, &mut Vec<Emit>)) {
    let peers = state.peers;
    let Some(machine) = state.streams.get_mut(&stream) else { return };
    let mut fx = Vec::new();
    apply(machine, &mut fx);
    lower(peers, stream, machine, fx, out);
}

fn every(state: &mut DeliveriesState, out: &mut Effects<Deliveries>, mut apply: impl FnMut(&mut Delivery, &mut Vec<Emit>)) {
    let peers = state.peers;
    for (stream, machine) in state.streams.iter_mut() {
        let mut fx = Vec::new();
        apply(machine, &mut fx);
        lower(peers, *stream, machine, fx, out);
    }
}
