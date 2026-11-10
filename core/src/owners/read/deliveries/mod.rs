//! Owner `Deliveries`: a dictionary of `Delivery` machines by stream id (one machine per stream).
//! Only this owner hands values to the platform. A table row applies to the machine of the stream the input names;
//! `Committed`, `EpochStarted`, `EpochEnded`, `ModelsReset`, `Closing` apply to every machine.

mod deliveries_flow;
mod deliveries_open;
mod deliveries_read;

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::owners::read::diff::{Diff, Entry, Stamp};
use crate::ffi::WireRow;
use crate::identity::RowId;
use crate::owners::contract::{Committed, Drained, EpochEnded, EpochStarted, Models, ModelsReset, ReplyId, WindowPhase, WindowState};
use crate::owners::read::feed::{Feed, ScopedChannel};
use crate::owners::read::windows::{Goal, WindowKey, Windows, WindowsInput};
use crate::runtime::{Address, Cause, Effect, EffectId, Effects, Epoch, Outbound, OutcomeId, StateOwner, Time};

pub const JOURNAL_LIMIT: usize = 256;

const READ_TASK: EffectId = 1 << 62;

pub fn read_task(stream: StreamId, read_id: u64) -> EffectId { READ_TASK | (stream << 32) | (read_id & 0xFFFF_FFFF) }

fn task_read(id: EffectId) -> (StreamId, u64) { ((id & !READ_TASK) >> 32, id & 0xFFFF_FFFF) }
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
    /// The queue of operations: the table `op`, read whole after every commit that touched it.
    Operations,
    Events,
    CableCloses,
    Changes,
    Blob,
    RetryStatus,
}

impl Kind {
    fn reads_rows(self) -> bool { matches!(self, Kind::List | Kind::Entity { .. } | Kind::Count | Kind::Operations) }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ReadData {
    /// `total` = the total the server gave for the window, read in the same snapshot as the rows.
    Rows { rows: Vec<StreamRow>, replaced: Vec<(String, String)>, total: Option<u64> },
    /// The membership in order with the stamp of each member, and the rows that changed after the version the owner holds: the others are the rows the owner holds.
    Changes { keys: Vec<(String, i64)>, rows: Vec<StreamRow>, replaced: Vec<(String, String)>, total: Option<u64> },
    Count(u64),
    Operations(Vec<crate::ffi::OperationInfo>),
    /// The copy URL and the processing that the row of a transfer shows.
    Upload { wait_reason: Option<crate::owners::write::upload::WaitReason>, local_url: Option<String>, processing: Option<crate::ffi::WireProcessing> },
    Overflow,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum Whole {
    Rows { rows: Vec<StreamRow>, replaced: Vec<(String, String)>, total: Option<u64> },
    Count(u64),
    Operations(Vec<crate::ffi::OperationInfo>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadMode {
    Snapshot,
    Changes { from_revision: i64, from_position: u64, limit: usize },
}

type UploadRowRead = Result<(Option<crate::owners::write::upload::WaitReason>, Option<String>, Option<crate::ffi::WireProcessing>), String>;

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
    pub scoped: std::sync::Arc<Vec<ScopedChannel>>,
    /// The stream loads its window again each time the network returns.
    pub reload: bool,
    pub window: WindowKey,
    pub subject: String,
    /// What a list or count reads, already resolved against its relation.
    pub query: Option<std::sync::Arc<crate::store::query::Query>>,
}

/// The scoped channels of the node read of a row id.
#[derive(Clone)]
pub struct ScopedOf(std::sync::Arc<dyn Fn(&str) -> Vec<ScopedChannel> + Send + Sync>);

impl ScopedOf {
    pub fn call(&self, id: &str) -> Vec<ScopedChannel> { (self.0)(id) }
    pub fn new(of: impl Fn(&str) -> Vec<ScopedChannel> + Send + Sync + 'static) -> Self { Self(std::sync::Arc::new(of)) }
}

impl std::fmt::Debug for ScopedOf {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("ScopedOf") }
}

/// The node read of a stream on a temporary id: no window and no request exist until the id is swapped for the server id.
#[derive(Clone, Debug)]
pub struct Deferred {
    pub relation: String,
    pub live: bool,
    pub scoped: ScopedOf,
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

impl EndCause {
    /// `Close` ends every stream and call as `Closed`, every other cause of an epoch end as `SessionEnded` (D-249).
    pub(crate) fn of(cause: crate::runtime::Cause) -> Self { if cause == crate::runtime::Cause::Close { Self::Closed } else { Self::SessionEnded } }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum EndCause {
    SessionEnded,
    Closed,
    Cancelled,
    Discarded,
    Invalid { detail: String },
    Storage { error: String },
    Internal { detail: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobStatus {
    pub state: String,
    pub wait_reason: Option<crate::owners::write::upload::WaitReason>,
    pub progress: u64,
    pub error: Option<String>,
    pub local_url: Option<String>,
    pub processing: Option<crate::ffi::WireProcessing>,
}

impl BlobStatus {
    /// The state and the progress of a transfer; the row of the transfer gives the copy URL and the processing.
    pub fn of(shown: &crate::owners::write::transfer::Shown) -> Self {
        use crate::owners::write::transfer::Shown;
        let (state, progress, error) = match shown {
            Shown::Gone => ("gone", 0, None),
            Shown::Copying => ("copying", 0, None),
            Shown::WaitingSource => ("waiting", 0, None),
            Shown::Queued => ("queued", 0, None),
            Shown::Uploading { progress } => ("uploading", u64::from(*progress), None),
            Shown::Failed { error } => ("failed", 0, Some(error.clone())),
        };
        Self { state: state.into(), wait_reason: shown.wait_reason(), progress, error, local_url: None, processing: None }
    }
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
    /// The steps from the rows the reader holds to the rows of `revision`, in order.
    pub edits: Vec<Edit>,
    /// The owner keeps the temporary ids of the rows the reader took; the platform gets them on the `Rewrite` steps.
    pub temporary: Vec<(String, String)>,
    /// Ids that no longer map to a temporary id.
    pub forgotten: Vec<String>,
    pub state: WindowState,
}

/// One mechanical step on the list of the reader: an index is a place in the list as it stands after the steps before.
#[derive(Clone, Debug, PartialEq)]
pub enum Edit {
    Insert { index: u32, row: StreamRow },
    /// `temporary` is the id the row had when the reader first got it, set when this step settles the swap of a temporary id.
    Rewrite { index: u32, row: StreamRow, temporary: Option<String> },
    Remove { index: u32, id: String },
    Move { from: u32, to: u32 },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// `edits` are the steps from the rows of the last value the reader took to `rows`; `temporary` and `forgotten` follow the rules of `Delta`.
    Snapshot { revision: i64, rows: Vec<StreamRow>, edits: Vec<Edit>, temporary: Vec<(String, String)>, forgotten: Vec<String>, state: WindowState },
    Delta(Delta),
    Count { revision: i64, count: u64, state: WindowState },
    Operations { revision: i64, items: Vec<crate::ffi::OperationInfo> },
    State { revision: i64, state: WindowState },
    Ready,
    Changes { revision: i64, rows: Vec<(String, String)> },
    Event { position: u64, payload: String },
    CableClose { position: u64, event: crate::owners::read::feed::CableClose },
    Lost { first: u64, last: u64 },
    Gap,
    Status { revision: i64, blob: BlobStatus },
    Retry(RetryStatus),
    Ended(EndCause),
}

impl Value {
    fn is_rows(&self) -> bool { matches!(self, Value::Snapshot { .. } | Value::Delta(_) | Value::Count { .. } | Value::Operations { .. }) }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PullAnswer {
    Value(Box<Value>),
    Done,
    Interrupted,
}

impl PullAnswer {
    pub fn ended(cause: EndCause) -> Self { Self::Value(Box::new(Value::Ended(cause))) }
}

#[derive(Clone, Debug, PartialEq)]
pub enum StreamEffect {
    Read { read_id: u64, revision: i64, after: i64, view: View, mode: ReadMode },
    Answer { outcome: OutcomeId, answer: PullAnswer },
    Available,
    CloseSink,
    Cancelled { reply: ReplyId },
    ReaderClaimed { reply: ReplyId, claimed: bool },
    /// `key` is the transfer key of a `Blob` stream, `None` for the retry status.
    StatusInterest { plus: bool, key: Option<String> },
}

enum Emit {
    Run(StreamEffect),
    Load { force: bool },
    InterestPlus,
    InterestPassive,
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
    WantPlus { model: String, id: StreamId, stream: bool, scoped: Vec<ScopedChannel> },
    WantMinus { model: String, id: StreamId },
    Drained(Drained),
    /// A `Cancel` of a stream that is gone: the caller gets its answer, no stream is touched.
    CancelAnswered { reply: ReplyId },
    /// The answer of `Census`: the streams that read a window and the streams that ended without a `Release`.
    Census { reply: ReplyId, observed: usize, retained: usize },
}

#[derive(Clone, Debug)]
pub enum DeliveryInput {
    /// `deferred` is set for an entity stream on a temporary id that has no server id yet: its window opens at the swap.
    Open { stream: StreamId, view: View, epoch: Epoch, identity: bool, valid: bool, now: i64, revision: i64, deferred: Option<Deferred> },
    Committed(Committed),
    ReadDone { stream: StreamId, read_id: u64, revision: i64, data: ReadData },
    ReadFailed { stream: StreamId, read_id: u64, revision: i64, error: String },
    TaskFailed { id: EffectId },
    WindowState { stream: StreamId, state: WindowState },
    Joined { stream: StreamId, position: u64 },
    ModelReady { stream: StreamId },
    ModelEvent { stream: StreamId, position: u64, payload: String },
    CableClosed(crate::owners::read::feed::CableClose),
    ModelLost { stream: StreamId, first: u64, last: u64 },
    ModelGap { stream: StreamId },
    /// The server refused the subscription of the model: the stream of events ends with `Invalid`.
    ModelInvalid { stream: StreamId },
    TransferStatus { stream: StreamId, blob: BlobStatus, revision: i64 },
    RetryState { stream: StreamId, retry: RetryStatus },
    /// `reader` is the number `claim_reader` gave. The first pull of an unclaimed stream takes it, a pull of another reader answers `Invalid`.
    Pull { stream: StreamId, outcome: OutcomeId, reader: u64 },
    PullCancelled { stream: StreamId, outcome: OutcomeId },
    Census { reply: ReplyId },
    Resync { stream: StreamId },
    Cancel { stream: StreamId, reply: ReplyId },
    ClaimReader { stream: StreamId, reader: u64, reply: ReplyId },
    /// `reader` is the reader that left the stream while its handle is held, `None` is the drop of the handle.
    Release { stream: StreamId, reader: Option<u64>, now: Time },
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
    /// The `now` of the open of the stream: the loads this stream asks for carry it.
    opened_at: i64,
    epoch: Epoch,
    view: View,
    stage: DeliveryStage,
    read_revision: i64,
    read_id: u64,
    /// The revision of the last read that finished: the rows of the stream are current up to it, `revision` is the revision of the last value of rows.
    read_seen: i64,
    read_position: u64,
    reading_position: u64,
    revision: i64,
    acknowledged: Option<i64>,
    resync_pending: bool,
    resync_failed: bool,
    read_error: bool,
    rows: Vec<StreamRow>,
    acked_rows: Vec<StreamRow>,
    /// Server id to the temporary id of the rows the reader took.
    temporary: BTreeMap<String, String>,
    count: Option<u64>,
    operations: Option<Vec<crate::ffi::OperationInfo>>,
    total: Option<u64>,
    window: WindowState,
    held_state: Option<WindowState>,
    next_feed: Option<u64>,
    position: u64,
    ready: bool,
    held_events: Vec<Held>,
    blob: Option<BlobStatus>,
    /// The state of the transfer, as the last `TransferStatus` gave it.
    transfer: Option<BlobStatus>,
    /// What the row of the transfer showed at the last read of a `Blob` stream.
    upload_row: Option<UploadRowRead>,
    retry: Option<RetryStatus>,
    status_revision: i64,
    /// A read of the row of a `Blob` stream is in flight: a status that arrives meanwhile waits for it, so a value never mixes revisions.
    upload_inflight: bool,
    journal: VecDeque<Value>,
    waiting: Option<OutcomeId>,
    ended_given: bool,
    cause: EndCause,
    replies: Vec<ReplyId>,
    closing: bool,
    released: bool,
    reader: Option<u64>,
    sink_closed: bool,
    interested: bool,
    /// A read stream of a live model holds a `Want+` of the Feed.
    wants: bool,
    /// The first read of a watched window waits for the first `WindowState`: the first value holds the phase of the window.
    pending_read: Option<(u64, i64, Whole)>,
    /// The node read waits for the swap of the subject.
    deferred: Option<Deferred>,
}

fn entity_row(view: &View) -> Option<(String, &str)> {
    match view.kind {
        Kind::Entity { .. } => Some((crate::store::ddl::table_name(&view.model), view.subject.as_str())),
        Kind::Blob => crate::store::split_transfer_key(&view.subject).map(|(model, id, _)| (crate::store::ddl::table_name(model), id)),
        _ => None,
    }
}

fn visible_after(state: &WindowState) -> i64 { state.loaded.unwrap_or(0) }

fn shown_hash(row: &WireRow) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    format!("{:?}{:?}", row.cells, row.sync).hash(&mut hasher);
    hasher.finish()
}

fn entry(row: &StreamRow) -> Result<Entry, String> {
    let id = RowId::new(&row.id).map_err(|error| error.to_string())?;
    Ok(Entry { id, stamp: Stamp { revision: row.stamp, shown: row.row.as_ref().map_or(0, shown_hash), ..Stamp::default() } })
}

pub struct DeliveriesState {
    epoch: Epoch,
    streams: BTreeMap<StreamId, Delivery>,
    by_table: BTreeMap<String, BTreeSet<StreamId>>,
    by_row: BTreeMap<String, BTreeMap<String, BTreeSet<StreamId>>>,
    watchers: BTreeSet<StreamId>,
    examined: u64,
    peers: DeliveryPeers,
    revision: i64,
    closing: bool,
    drained: bool,
}

impl DeliveriesState {
    pub fn new(epoch: Epoch, peers: DeliveryPeers) -> Self { Self { epoch, streams: BTreeMap::new(), by_table: BTreeMap::new(), by_row: BTreeMap::new(), watchers: BTreeSet::new(), examined: 0, peers, revision: 0, closing: false, drained: false } }

    pub fn stage(&self, stream: StreamId) -> Option<DeliveryStage> { self.streams.get(&stream).map(|machine| machine.stage) }

    #[cfg(test)]
    pub fn journal_len(&self, stream: StreamId) -> usize { self.streams.get(&stream).map_or(0, |machine| machine.journal.len()) }

    pub fn read_id(&self, stream: StreamId) -> u64 { self.streams.get(&stream).map_or(0, |machine| machine.read_id) }

    pub fn streams(&self) -> usize { self.streams.len() }

    #[cfg(test)]
    pub fn streams_examined(&self) -> u64 { self.examined }

    fn index(&mut self, stream: StreamId) {
        if self.streams.get(&stream).is_some_and(|machine| machine.view.kind == Kind::Changes && machine.stage == DeliveryStage::Live) {
            self.watchers.insert(stream);
        }
        let Some(machine) = self.streams.get(&stream).filter(|machine| (machine.view.kind.reads_rows() || machine.view.kind == Kind::Blob) && matches!(machine.stage, DeliveryStage::Opening | DeliveryStage::Live)) else { return };
        let own = entity_row(&machine.view);
        for table in &machine.view.tables {
            match &own {
                Some((own_table, row)) if table == own_table => { self.by_row.entry(table.clone()).or_default().entry(row.to_string()).or_default().insert(stream); }
                _ => { self.by_table.entry(table.clone()).or_default().insert(stream); }
            }
        }
    }

    /// The subject of an entity stream changed from `before`: the stream is indexed by the row it reads now.
    fn rebind(&mut self, stream: StreamId, before: &str) {
        let Some(machine) = self.streams.get(&stream) else { return };
        let table = crate::store::ddl::table_name(&machine.view.model);
        let subject = machine.view.subject.clone();
        if let Some(rows) = self.by_row.get_mut(&table) {
            if let Some(streams) = rows.get_mut(before) {
                streams.remove(&stream);
                if streams.is_empty() { rows.remove(before); }
            }
        }
        self.by_row.entry(table).or_default().entry(subject).or_default().insert(stream);
    }

    fn settle(&mut self, stream: StreamId) {
        let Some(machine) = self.streams.get(&stream) else { return };
        if !matches!(machine.stage, DeliveryStage::Opening | DeliveryStage::Live) {
            self.watchers.remove(&stream);
            let own = entity_row(&machine.view);
            for table in &machine.view.tables {
                if let Some(streams) = self.by_table.get_mut(table) {
                    streams.remove(&stream);
                    if streams.is_empty() {
                        self.by_table.remove(table);
                    }
                }
                if let (Some((_, row)), Some(rows)) = (&own, self.by_row.get_mut(table)) {
                    if let Some(streams) = rows.get_mut(*row) {
                        streams.remove(&stream);
                        if streams.is_empty() {
                            rows.remove(*row);
                        }
                    }
                    if rows.is_empty() {
                        self.by_row.remove(table);
                    }
                }
            }
        }
        if machine.gone() {
            self.streams.remove(&stream);
        }
    }

    fn affected(&self, committed: &Committed) -> BTreeSet<StreamId> {
        match &committed.models {
            Models::All => BTreeSet::new(),
            Models::Set(_) => {
                let tabled = committed.tables.iter().filter_map(|table| self.by_table.get(table)).flatten();
                let rowed: Vec<&BTreeSet<StreamId>> = match &committed.rows {
                    None => committed.tables.iter().filter_map(|table| self.by_row.get(table)).flat_map(BTreeMap::values).collect(),
                    Some(rows) => rows.iter().filter(|(table, _)| committed.tables.contains(table)).filter_map(|(table, row)| self.by_row.get(table)?.get(row)).collect(),
                };
                tabled.chain(rowed.into_iter().flatten()).copied().collect()
            }
        }
    }

    pub fn journal(&self, stream: StreamId) -> Vec<Value> { self.streams.get(&stream).map(|machine| machine.journal.iter().cloned().collect()).unwrap_or_default() }

    pub fn acknowledged(&self, stream: StreamId) -> Option<i64> { self.streams.get(&stream).and_then(|machine| machine.acknowledged) }
}

pub struct Deliveries;

fn lower(peers: DeliveryPeers, revision: i64, stream: StreamId, machine: &Delivery, emits: Vec<Emit>, out: &mut Effects<Deliveries>) {
    for emit in emits {
        let effect = match emit {
            Emit::Run(effect) => Effect::Run(DeliveryEffect::Stream { stream, effect }),
            Emit::InterestPlus => Effect::Send(Outbound::new(peers.windows, WindowsInput::InterestPlus { key: machine.view.window.clone(), id: stream, now: machine.opened_at })),
            Emit::InterestPassive => Effect::Send(Outbound::new(peers.windows, WindowsInput::InterestPassive { key: machine.view.window.clone(), id: stream })),
            Emit::ReloadOnConnect => Effect::Send(Outbound::new(peers.windows, WindowsInput::ReloadOnConnect { key: machine.view.window.clone(), id: stream })),
            Emit::Load { force } => Effect::Send(Outbound::new(peers.windows, WindowsInput::Load { key: machine.view.window.clone(), id: stream, goal: Goal::First, force, store_revision: revision, now: machine.opened_at })),
            Emit::InterestMinus => Effect::Send(Outbound::new(peers.windows, WindowsInput::InterestMinus { key: machine.view.window.clone(), id: stream })),
            Emit::WantPlus => Effect::Run(DeliveryEffect::WantPlus { model: machine.view.model.clone(), id: stream, stream: machine.view.kind == Kind::Events, scoped: machine.view.scoped.to_vec() }),
            Emit::WantMinus => Effect::Run(DeliveryEffect::WantMinus { model: machine.view.model.clone(), id: stream }),
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
            DeliveryInput::Open { stream, view, epoch, identity, valid, now, revision, deferred } => {
                if state.streams.contains_key(&stream) {
                    return out;
                }
                state.revision = state.revision.max(revision);
                let mut fx = Vec::new();
                let machine = if epoch != state.epoch || (!identity && !view.local && !matches!(view.kind, Kind::Blob | Kind::RetryStatus)) {
                    Delivery::ended(view, epoch, EndCause::SessionEnded, &mut fx)
                } else if !valid {
                    Delivery::ended(view, epoch, EndCause::Invalid { detail: "the stream request is invalid".into() }, &mut fx)
                } else {
                    Delivery::open(view, epoch, revision, &mut fx)
                };
                let mut machine = machine;
                machine.opened_at = now;
                machine.deferred = deferred.filter(|_| matches!(machine.view.kind, Kind::Entity { .. }));
                lower(state.peers, state.revision, stream, &machine, fx, &mut out);
                state.streams.insert(stream, machine);
                state.index(stream);
            }
            DeliveryInput::ReadDone { stream, read_id, revision, data } => on(state, stream, &mut out, |machine, fx| machine.read_done(read_id, revision, data, fx)),
            DeliveryInput::TaskFailed { id } => {
                let ((stream, read_id), revision) = (task_read(id), state.revision);
                on(state, stream, &mut out, |machine, fx| machine.read_failed(read_id, revision, "the read task failed".into(), fx));
            }
            DeliveryInput::PullCancelled { stream, outcome } => on(state, stream, &mut out, |machine, fx| machine.pull_cancelled(outcome, fx)),
            DeliveryInput::ReadFailed { stream, read_id, revision, error } => on(state, stream, &mut out, |machine, fx| machine.read_failed(read_id, revision, error, fx)),
            DeliveryInput::WindowState { stream, state: window } => on(state, stream, &mut out, |machine, fx| machine.window_state(window, fx)),
            DeliveryInput::Joined { stream, position } => on(state, stream, &mut out, |machine, fx| machine.joined(position, fx)),
            DeliveryInput::ModelReady { stream } => on(state, stream, &mut out, |machine, fx| machine.model_ready(fx)),
            DeliveryInput::ModelEvent { stream, position, payload } => on(state, stream, &mut out, |machine, fx| machine.model_event(position, payload, fx)),
            DeliveryInput::CableClosed(event) => {
                let streams: Vec<_> = state.streams.iter().filter(|(_, machine)| machine.view.kind == Kind::CableCloses).map(|(id, _)| *id).collect();
                for stream in streams { on(state, stream, &mut out, |machine, fx| machine.cable_closed(event.clone(), fx)); }
            }
            DeliveryInput::ModelLost { stream, first, last } => on(state, stream, &mut out, |machine, fx| machine.model_lost(first, last, fx)),
            DeliveryInput::ModelGap { stream } => on(state, stream, &mut out, |machine, fx| machine.model_gap(fx)),
            DeliveryInput::ModelInvalid { stream } => on(state, stream, &mut out, |machine, fx| machine.model_invalid(fx)),
            DeliveryInput::TransferStatus { stream, blob, revision } => on(state, stream, &mut out, |machine, fx| machine.status(blob, revision, fx)),
            DeliveryInput::RetryState { stream, retry } => on(state, stream, &mut out, |machine, fx| machine.retry_state(retry, fx)),
            DeliveryInput::Cancel { stream, reply } => {
                if state.streams.contains_key(&stream) {
                    on(state, stream, &mut out, |machine, fx| machine.cancel(reply, fx));
                } else {
                    out.push(Effect::Run(DeliveryEffect::CancelAnswered { reply }));
                }
            }
            DeliveryInput::ClaimReader { stream, reader, reply } => {
                let claimed = state.streams.get_mut(&stream).is_none_or(|machine| machine.claim(reader));
                out.push(Effect::Run(DeliveryEffect::Stream { stream, effect: StreamEffect::ReaderClaimed { reply, claimed } }));
            }
            DeliveryInput::Release { stream, reader, .. } => on(state, stream, &mut out, |machine, fx| if machine.reader.is_none() || reader.is_none() || machine.reader == reader { machine.release(fx) }),
            DeliveryInput::SinkClosed { stream } => on(state, stream, &mut out, |machine, fx| machine.sink_closed(fx)),
            DeliveryInput::Pull { stream, outcome, reader } => {
                if state.streams.contains_key(&stream) {
                    on(state, stream, &mut out, |machine, fx| machine.pull(outcome, reader, fx));
                } else {
                    out.push(Effect::Run(DeliveryEffect::Stream { stream, effect: StreamEffect::Answer { outcome, answer: PullAnswer::Value(Box::new(Value::Ended(EndCause::Cancelled))) } }));
                }
            }
            DeliveryInput::Census { reply } => {
                let observed = state.streams.values().filter(|machine| machine.view.remote && machine.stage != DeliveryStage::Ended).count();
                let retained = state.streams.values().filter(|machine| machine.stage == DeliveryStage::Ended && !machine.released && machine.view.kind != Kind::Changes).count();
                out.push(Effect::Run(DeliveryEffect::Census { reply, observed, retained }));
            }
            DeliveryInput::Resync { stream } => on(state, stream, &mut out, |machine, fx| machine.resync(fx)),
            DeliveryInput::Committed(committed) => {
                state.revision = state.revision.max(committed.revision);
                for stream in state.affected(&committed) {
                    state.examined += 1;
                    on(state, stream, &mut out, |machine, fx| machine.committed(&committed, fx));
                }
                let watchers: Vec<StreamId> = state.watchers.iter().copied().collect();
                for stream in watchers {
                    on(state, stream, &mut out, |machine, fx| machine.changed(&committed, fx));
                }
            }
            DeliveryInput::ModelsReset(reset) => every(state, &mut out, |machine, fx| machine.models_reset(&reset, fx)),
            DeliveryInput::EpochEnded(ended) => every(state, &mut out, |machine, fx| machine.epoch_ended(&ended, fx)),
            DeliveryInput::EpochStarted(started) => state.epoch = started.epoch,
            DeliveryInput::Closing => {
                state.closing = true;
                every(state, &mut out, |machine, fx| machine.closing(fx));
            }
            DeliveryInput::Tick(_) => {}
        }
        if state.closing && !state.drained && state.streams.values().all(Delivery::settled) {
            state.drained = true;
            out.push(Effect::Run(DeliveryEffect::Drained(Drained { owner: "deliveries" })));
        }
        out
    }

    fn executor_failed(_executor: &str, pending: Vec<EffectId>) -> Vec<DeliveryInput> { pending.into_iter().map(|id| DeliveryInput::TaskFailed { id }).collect() }

    fn tick(now: Time) -> DeliveryInput { DeliveryInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> DeliveryInput { DeliveryInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> DeliveryInput { DeliveryInput::Closing }
}

fn on(state: &mut DeliveriesState, stream: StreamId, out: &mut Effects<Deliveries>, apply: impl FnOnce(&mut Delivery, &mut Vec<Emit>)) {
    let peers = state.peers;
    let Some(machine) = state.streams.get_mut(&stream) else { return };
    let subject = machine.view.subject.clone();
    let mut fx = Vec::new();
    apply(machine, &mut fx);
    lower(peers, state.revision, stream, machine, fx, out);
    if state.streams.get(&stream).is_some_and(|machine| machine.view.subject != subject) {
        state.rebind(stream, &subject);
    }
    state.settle(stream);
}

fn every(state: &mut DeliveriesState, out: &mut Effects<Deliveries>, mut apply: impl FnMut(&mut Delivery, &mut Vec<Emit>)) {
    let streams: Vec<StreamId> = state.streams.keys().copied().collect();
    for stream in streams {
        on(state, stream, out, |machine, fx| apply(machine, fx));
    }
}
