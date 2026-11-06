//! Owner `Deliveries`: a dictionary of `Delivery` machines by stream id (one machine per stream).
//! Only this owner hands values to the platform. A table row applies to the machine of the stream the input names;
//! `Committed`, `EpochStarted`, `EpochEnded`, `ModelsReset`, `Closing` apply to every machine.

#[path = "deliveries_flow.rs"]
mod deliveries_flow;
#[path = "deliveries_read.rs"]
mod deliveries_read;

use std::collections::{BTreeMap, VecDeque};

use crate::read::{Diff, Entry, Stamp};
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
    /// `total` = the total the server gave for the window, read in the same snapshot as the rows.
    Rows { rows: Vec<StreamRow>, replaced: Vec<(String, String)>, total: Option<u64> },
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
    /// The stream loads its window again each time the network returns.
    pub reload: bool,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndCause {
    SessionEnded,
    Closed,
    Cancelled,
    Invalid,
    Storage { error: String },
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
    Read { read_id: u64, revision: i64, after: i64, view: View },
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
    ReloadOnConnect,
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
    /// The answer of `Census`: the streams that read a window and the streams that ended without a `Release`.
    Census { reply: ReplyId, observed: usize, retained: usize },
}

#[derive(Clone, Debug)]
pub enum DeliveryInput {
    Open { stream: StreamId, view: View, epoch: Epoch, identity: bool, valid: bool },
    Committed(Committed),
    ReadDone { stream: StreamId, read_id: u64, revision: i64, data: ReadData },
    ReadFailed { stream: StreamId, read_id: u64, revision: i64, error: String },
    WindowState { stream: StreamId, state: WindowState },
    Joined { stream: StreamId, position: u64 },
    ModelReady { stream: StreamId },
    ModelEvent { stream: StreamId, position: u64, payload: String },
    ModelLost { stream: StreamId, first: u64, last: u64 },
    ModelGap { stream: StreamId },
    /// The server refused the subscription of the model: the stream of events ends with `Invalid`.
    ModelInvalid { stream: StreamId },
    TransferStatus { stream: StreamId, blob: BlobStatus },
    RetryState { stream: StreamId, retry: RetryStatus },
    Pull { stream: StreamId, outcome: OutcomeId },
    Census { reply: ReplyId },
    Resync { stream: StreamId },
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
    read_position: u64,
    shown_position: u64,
    reading_position: u64,
    revision: i64,
    acknowledged: Option<i64>,
    resync_pending: bool,
    resync_failed: bool,
    read_error: bool,
    rows: Vec<StreamRow>,
    acked_rows: Vec<StreamRow>,
    count: Option<u64>,
    total: Option<u64>,
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
    /// The first read of a watched window waits for the first `WindowState`: the first value holds the phase of the window.
    pending_read: Option<(u64, i64, ReadData)>,
}

fn visible_after(state: &WindowState) -> i64 { state.loaded.unwrap_or(0) }

fn shown_hash(row: &WireRow) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    format!("{:?}", row.cells).hash(&mut hasher);
    hasher.finish()
}

fn entry(row: &StreamRow) -> Result<Entry, String> {
    let id = RowId::new(&row.id).map_err(|error| error.to_string())?;
    Ok(Entry { id, stamp: Stamp { revision: row.stamp, shown: row.row.as_ref().map_or(0, shown_hash), ..Stamp::default() } })
}

impl Delivery {
    fn new(epoch: Epoch, view: View, stage: DeliveryStage, revision: i64) -> Self {
        let window = if view.remote { WindowState::idle() } else { WindowState { phase: WindowPhase::Loaded, error: None, loaded: None, has_more: false, settled: true, total: None } };
        Self {
            epoch,
            view,
            stage,
            read_revision: revision,
            read_id: 0,
            read_seen: 0,
            read_position: 0,
            shown_position: 0,
            reading_position: 0,
            revision: 0,
            acknowledged: None,
            resync_pending: false,
            resync_failed: false,
            read_error: false,
            rows: Vec::new(),
            acked_rows: Vec::new(),
            count: None,
            total: None,
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
            pending_read: None,
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
                    if machine.view.reload { fx.push(Emit::ReloadOnConnect); }
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
        machine.journal.push_back(Value::Ended(cause.clone()));
        machine.cause = cause;
        machine.sink_closed = true;
        fx.push(Emit::Run(StreamEffect::Available));
        machine
    }

}

pub struct DeliveriesState {
    epoch: Epoch,
    streams: BTreeMap<StreamId, Delivery>,
    peers: DeliveryPeers,
    revision: std::sync::Arc<std::sync::atomic::AtomicI64>,
    closing: bool,
    drained: bool,
}

impl DeliveriesState {
    pub fn new(epoch: Epoch, peers: DeliveryPeers, revision: std::sync::Arc<std::sync::atomic::AtomicI64>) -> Self { Self { epoch, streams: BTreeMap::new(), peers, revision, closing: false, drained: false } }

    pub fn stage(&self, stream: StreamId) -> Option<DeliveryStage> { self.streams.get(&stream).map(|machine| machine.stage) }

    pub fn journal_len(&self, stream: StreamId) -> usize { self.streams.get(&stream).map_or(0, |machine| machine.journal.len()) }

    pub fn read_id(&self, stream: StreamId) -> u64 { self.streams.get(&stream).map_or(0, |machine| machine.read_id) }

    pub fn streams(&self) -> usize { self.streams.len() }

    pub fn journal(&self, stream: StreamId) -> Vec<Value> { self.streams.get(&stream).map(|machine| machine.journal.iter().cloned().collect()).unwrap_or_default() }

    pub fn acknowledged(&self, stream: StreamId) -> Option<i64> { self.streams.get(&stream).and_then(|machine| machine.acknowledged) }
}

pub struct Deliveries;

fn lower(peers: DeliveryPeers, revision: &std::sync::atomic::AtomicI64, stream: StreamId, machine: &Delivery, emits: Vec<Emit>, out: &mut Effects<Deliveries>) {
    for emit in emits {
        let effect = match emit {
            Emit::Run(effect) => Effect::Run(DeliveryEffect::Stream { stream, effect }),
            Emit::InterestPlus => Effect::Send(Outbound::new(peers.windows, WindowsInput::InterestPlus { key: machine.view.window.clone(), id: stream })),
            Emit::ReloadOnConnect => Effect::Send(Outbound::new(peers.windows, WindowsInput::ReloadOnConnect { key: machine.view.window.clone(), id: stream })),
            Emit::Load { force } => Effect::Send(Outbound::new(peers.windows, WindowsInput::Load { key: machine.view.window.clone(), id: stream, goal: Goal::First, force, store_revision: revision.load(std::sync::atomic::Ordering::Acquire) })),
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
            DeliveryInput::Open { stream, view, epoch, identity, valid } => {
                if state.streams.contains_key(&stream) {
                    return out;
                }
                let mut fx = Vec::new();
                let machine = if !valid {
                    Delivery::ended(view, epoch, EndCause::Invalid, &mut fx)
                } else if epoch != state.epoch || (!identity && !view.local && !matches!(view.kind, Kind::Blob | Kind::RetryStatus)) {
                    Delivery::ended(view, epoch, EndCause::SessionEnded, &mut fx)
                } else {
                    Delivery::open(view, epoch, 0, &mut fx)
                };
                lower(state.peers, &state.revision, stream, &machine, fx, &mut out);
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
            DeliveryInput::ModelInvalid { stream } => on(state, stream, &mut out, |machine, fx| machine.model_invalid(fx)),
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
            DeliveryInput::Census { reply } => {
                let observed = state.streams.values().filter(|machine| machine.view.remote && machine.stage != DeliveryStage::Ended).count();
                let retained = state.streams.values().filter(|machine| machine.stage == DeliveryStage::Ended && !machine.released).count();
                out.push(Effect::Run(DeliveryEffect::Census { reply, observed, retained }));
            }
            DeliveryInput::Resync { stream } => on(state, stream, &mut out, |machine, fx| machine.resync(fx)),
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
    lower(peers, &state.revision, stream, machine, fx, out);
}

fn every(state: &mut DeliveriesState, out: &mut Effects<Deliveries>, mut apply: impl FnMut(&mut Delivery, &mut Vec<Emit>)) {
    let peers = state.peers;
    for (stream, machine) in state.streams.iter_mut() {
        let mut fx = Vec::new();
        apply(machine, &mut fx);
        lower(peers, &state.revision, *stream, machine, fx, out);
    }
}
