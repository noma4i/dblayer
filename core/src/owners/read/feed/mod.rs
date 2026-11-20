//! Owner `Feed`: the server subscription of the epoch, the connection protocol, the model events and their positions.
//! Timers are `Effect::Schedule` of the owner (`after` = the debounce or the backoff). Inputs for other owners
//! (`Commit`, `Stale`, `Joined`, `ModelReady`, `ModelEvent`, `ModelLost`, `ModelGap`) are `Effect::Send` to the addresses of the state.
//! The executor opens connections, encrypts and decrypts: it answers with `HandshakeDone`, `FrameOpened` and `ProtocolError`. The inputs
//! `FrameOpened` of an `Established` connection forwards its events as `Event`.
//! A model reads one or more channels (subscriptions of the server) on the one connection: it is ready once every channel of it is confirmed.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::owners::contract::{BatchAuthor, BatchId, Committed, ConnectionId, Drained, EpochEnded, EpochStarted, Online, RejectReason, Rejected, ReplyId};
use crate::owners::read::deliveries::{Deliveries, DeliveryInput};
use crate::owners::read::windows::{StaleCause, StaleKeys, Windows, WindowsInput};
use crate::owners::store::{StoreBatch, StoreInput, StoreWriter};
use crate::runtime::RetryPolicy;
use crate::runtime::{Address, Cause, Effect, Effects, Epoch, Outbound, ScheduleId, StateOwner, Time};

/// The id a `Want+` carries: a stream `Events` of Deliveries or a window with `live`.
pub type SubscriptionId = u64;

/// A connection that finished no handshake by then fails like a protocol error.
pub const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);
/// The pause after close code 4429 when its reason names no `retryAfter`.
pub const CABLE_LIMIT_FLOOR: Duration = Duration::from_secs(5);

/// The schedule ids of the handshake deadlines are above the connection ids that name the resubscribe timers.
const HANDSHAKE_SCHEDULE: u64 = 1 << 40;

/// The schedule ids of the channel resubscribe timers: the connection and the slot of the channel under this bit.
const CHANNEL_SCHEDULE: u64 = 1 << 41;
const KEEPALIVE_SCHEDULE: u64 = 1 << 42;
const PONG_SCHEDULE: u64 = 1 << 43;
const KEEPALIVE: Duration = Duration::from_secs(25);
const PONG_DEADLINE: Duration = Duration::from_secs(10);

/// The drain timer of a closing connection: both id bits, the connection under them.
pub fn drain_schedule(connection: ConnectionId) -> ScheduleId { ScheduleId(HANDSHAKE_SCHEDULE | CHANNEL_SCHEDULE | connection) }

pub fn is_drain_schedule(id: ScheduleId) -> bool { id.0 & (HANDSHAKE_SCHEDULE | CHANNEL_SCHEDULE) == HANDSHAKE_SCHEDULE | CHANNEL_SCHEDULE }

/// The first frame the client seals is number 0 (the hello is no sealed frame).
pub const FIRST_SEND_COUNTER: u64 = 0;
/// The `ready` frame of the server is number 0 and the handshake consumes it: the first frame after it is number 1.
pub const FIRST_RECV_COUNTER: u64 = 1;

/// One event of a frame as the codec reads it: the rows of the event and the keys of the windows that read again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FramedEvent {
    pub model: String,
    pub payload: String,
    pub reread: StaleKeys,
    pub refused: bool,
}

/// An accepted event: its position is fixed when the frame arrives.
#[derive(Clone, Debug, PartialEq, Eq)]
struct FeedEvent {
    model: String,
    payload: String,
    reread: StaleKeys,
    position: u64,
    refused: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Off { gap: bool },
    Opening { gap: bool },
    Live,
    /// A live feed closed its established connection: the writer sends the queue until `Drain` or the end of the connection, then the feed goes on as `then` says.
    Draining { connection: ConnectionId, then: Resume },
    Ended,
}

/// What a draining feed becomes when its drain is over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resume {
    End,
    Off { gap: bool },
    /// The protocol of the connection failed: the feed opens another one if it still wants and is online.
    Reopen,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum CableCloseInitiator { Server, Client, Network }

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct CableClose {
    pub connection_id: u64,
    pub code: Option<u16>,
    pub reason: Option<String>,
    pub failure: Option<crate::transport::RawFailure>,
    pub initiator: CableCloseInitiator,
    pub intended: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolReason {
    Handshake,
    Decrypt,
    /// A frame that passed the schema of the cable but is no valid frame of the subscription.
    Frame,
}

/// The name of a channel: the subscription of the `.dbl` the server runs for a model. A frame carries it as its `id`.
pub type Channel = String;

pub type Variables = serde_json::Map<String, serde_json::Value>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopedChannel {
    pub subscription: String,
    pub variables: Variables,
}

/// The channel name of a scoped channel is the subscription, `-` and a number of the owner: a frame id holds letters, digits, `_` and `-` only.
pub fn subscription_of(channel: &str) -> &str { channel.split_once('-').map_or(channel, |(subscription, _)| subscription) }

/// What a decrypted frame of the server says, as the executor reads it. The owner checks the counter before it reads the meaning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Ping { ts: serde_json::Number },
    Pong { ts: serde_json::Number },
    Events { id: String, events: Vec<FramedEvent> },
    /// The server confirmed the subscription of the channel `id`.
    Confirmed { id: String },
    Complete { id: String },
    /// The server closes the connection.
    Ended,
    /// An `error` frame: `id` names the subscription it is for, none = the whole connection; `retryable` is the flag of the server.
    Error { id: Option<String>, retryable: bool },
    /// `ready`, `pong`: nothing for the owner.
    Quiet,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    Handshake,
    Established { key_id: u64, send_counter: u64, recv_counter: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Connection {
    id: ConnectionId,
    protocol: Protocol,
    pong: Option<u64>,
    last_received: Option<Time>,
    pong_due: Option<Time>,
}

impl Connection {
    /// A connection in its handshake: nothing sent on it yet.
    fn handshake(id: ConnectionId) -> Self { Self { id, protocol: Protocol::Handshake, pong: None, last_received: None, pong_due: None } }

    fn goodbye(&self) -> bool { matches!(self.protocol, Protocol::Established { .. }) }
}

/// The error of the server that ended a channel: a retryable one subscribes the channel again by the backoff, any other removes the models of the channel from `want`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChannelError {
    pub retryable: bool,
}

/// The frames Feed sends over an `Established` connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outgoing {
    Subscribe { channel: Channel, variables: Option<Variables> },
    Unsubscribe { channel: Channel },
    /// The answer to a server `ping`.
    Pong { ts: serde_json::Number },
    Ping { ts: u64 },
}

/// The effects of the runner: IO of the connection and the end of the owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FeedEffect {
    /// Transport `Subscribe(want)`: opens the connection and runs its handshake.
    Connect { connection: ConnectionId },
    /// The executor encrypts `frame` with the key `key_id` and the counter, both given by value.
    Seal { connection: ConnectionId, key_id: u64, counter: u64, frame: Outgoing },
    /// Transport `Unsubscribe(connection)`: `goodbye` is set for an established connection, its queued frames still go out before the line closes. `drain` keeps the connection until `Cancel`.
    Close { connection: ConnectionId, goodbye: bool, drain: bool, intended: bool },
    /// Transport `cancel(connection)`: the drain is over.
    Cancel { connection: ConnectionId },
    Drained(Drained),
    /// `SnapshotRequest{Feed, epoch}` to Network: the first `Online` of the epoch comes back addressed to Feed.
    SnapshotRequest { epoch: Epoch },
    /// The answer of `Census`: the channels the models of `want` read on the active connection.
    Census { reply: ReplyId, subscriptions: usize, refused: u64 },
}

/// What the steps give out: an effect of the runner, a timer, or an input for another owner.
enum Fx {
    Run(FeedEffect),
    Debounce { after_ms: u64 },
    CancelDebounce,
    Resubscribe { connection: ConnectionId, attempt: u32, floor: Option<Duration> },
    CancelResubscribe { connection: ConnectionId },
    Drain { connection: ConnectionId },
    CancelDrain { connection: ConnectionId },
    ResubscribeChannel { connection: ConnectionId, channel: Channel, slot: u64, attempt: u32 },
    HandshakeDeadline { connection: ConnectionId },
    CancelHandshake { connection: ConnectionId },
    Keepalive { connection: ConnectionId, after: Duration },
    PongDeadline { connection: ConnectionId, counter: u64 },
    CancelKeepalive { connection: ConnectionId },
    CancelPong { connection: ConnectionId },
    Commit { batch: BatchId, events: Vec<FeedEvent> },
    Gap(Vec<String>),
    CommitGap { batch: BatchId, models: Vec<String>, kept: Vec<(String, String)> },
    Stale { model: String, keys: StaleKeys, cause: StaleCause },
    Joined { id: SubscriptionId, position: u64 },
    /// `id` = `None`: every stream that wants the model.
    ModelReady { model: String, id: Option<SubscriptionId> },
    ModelEvent { model: String, position: u64, payload: String },
    ModelLost { model: String, first: u64, last: u64 },
    ModelGap { model: String },
    /// The streams `ids` end with `Invalid`.
    ModelInvalid { ids: Vec<SubscriptionId> },
}

#[derive(Clone, Debug)]
pub enum FeedInput {
    CableClosed(CableClose),
    ActivateEpoch(Epoch),
    /// `stream` = the id is a stream `Events` of Deliveries: it gets `Joined` and `ModelReady`.
    WantPlus { model: String, id: SubscriptionId, stream: bool, scoped: Vec<ScopedChannel> },
    WantMinus { model: String, id: SubscriptionId },
    HandshakeDone { connection_id: ConnectionId, key_id: u64 },
    FrameOpened { connection_id: ConnectionId, counter: u64, frame: Frame },
    ProtocolError { connection_id: ConnectionId, reason: ProtocolReason },
    Event { connection_id: ConnectionId, channel: Channel, event: FramedEvent },
    ChannelConfirmed { connection_id: ConnectionId, channel: Channel },
    /// The server closed the subscription of one channel: `failed` = with an error of the request.
    ChannelEnded { connection_id: ConnectionId, channel: Channel, error: Option<ChannelError> },
    ChannelResubscribe { connection_id: ConnectionId, channel: Channel },
    Dropped { connection_id: ConnectionId },
    /// The server closed the cable with code 4429: a `Dropped` whose next socket waits at least `floor`.
    Limited { connection_id: ConnectionId, floor: Duration },
    SubscriptionEnded { connection_id: ConnectionId },
    Resubscribe { connection_id: ConnectionId },
    /// The timer of a `Connect`: a connection still in `Handshake` fails.
    HandshakeDeadline { connection_id: ConnectionId },
    Keepalive { connection_id: ConnectionId },
    PongDeadline { connection_id: ConnectionId, counter: u64 },
    /// The timer of a closing connection: its drain is over.
    Drain { connection_id: ConnectionId },
    Committed(Committed),
    /// The payloads of the events of a batch with the rows read at its landing: they arrive before the `Committed` of the batch.
    Observed { batch: BatchId, payloads: Vec<String> },
    Rejected(Rejected),
    Tick,
    Online { online: Online, online_seq: u64 },
    EpochStarted(EpochStarted),
    EpochEnded(EpochEnded),
    Closing,
    Census { reply: ReplyId },
    /// The Store's verdict on `cable_when`: the connection opens only while it is true.
    Gate(bool),
}

/// The owners `Feed` sends inputs to.
#[derive(Clone, Copy, Debug)]
pub struct FeedPeers {
    pub store: Address<StoreWriter>,
    pub windows: Address<Windows>,
    pub deliveries: Address<Deliveries>,
}

#[derive(Clone, Debug)]
enum Slot {
    Pending,
    Landed(String),
    Lost,
}

/// The positions of one model: only a continuous ready prefix is given out.
#[derive(Debug)]
struct Ledger {
    next: u64,
    slots: BTreeMap<u64, Slot>,
}

impl Ledger {
    fn new() -> Self { Self { next: 1, slots: BTreeMap::new() } }

    fn emit(&mut self, model: &str, fx: &mut Vec<Fx>) {
        loop {
            match self.slots.get(&self.next) {
                Some(Slot::Landed(_)) => {
                    let Some(Slot::Landed(payload)) = self.slots.remove(&self.next) else { return };
                    fx.push(Fx::ModelEvent { model: model.to_string(), position: self.next, payload });
                    self.next += 1;
                }
                Some(Slot::Lost) => {
                    let first = self.next;
                    while matches!(self.slots.get(&self.next), Some(Slot::Lost)) {
                        self.slots.remove(&self.next);
                        self.next += 1;
                    }
                    fx.push(Fx::ModelLost { model: model.to_string(), first, last: self.next - 1 });
                }
                _ => return,
            }
        }
    }
}

pub struct FeedState {
    epoch: Epoch,
    mode: Mode,
    online: Option<bool>,
    online_seq: u64,
    want: BTreeMap<String, BTreeSet<SubscriptionId>>,
    scoped: BTreeMap<SubscriptionId, Vec<Channel>>,
    channel_variables: BTreeMap<Channel, Variables>,
    scope_numbers: BTreeMap<(String, String), u64>,
    streams: BTreeSet<SubscriptionId>,
    /// `updates.debounce` of the models, from the schema at `install`.
    debounce: BTreeMap<String, u64>,
    /// The channels each model reads, from the schema at `install`.
    channels: BTreeMap<String, Vec<Channel>>,
    /// The relations of each model that declare `keep_on_gap`, from the schema at `install`: the windows of a step of the break do not name them (D-421).
    kept: BTreeMap<String, BTreeSet<String>>,
    /// Channels with a `Subscribe` on the active connection.
    subscribed: BTreeSet<Channel>,
    /// Channels the server confirmed on the active connection.
    confirmed: BTreeSet<Channel>,
    /// Models given `ModelReady` on the active connection.
    announced: BTreeSet<String>,
    /// Readers that joined a model whose channels were not all confirmed: each owes one `ModelReady` at the next readiness of its model.
    waiting: BTreeSet<SubscriptionId>,
    /// Models that owe a `ModelGap` at their next readiness: a break came before their channels were confirmed.
    gapped: BTreeMap<String, u64>,
    gap_seq: Option<u64>,
    channel_attempts: BTreeMap<Channel, u32>,
    slots: BTreeMap<Channel, u64>,
    position: BTreeMap<String, u64>,
    ledger: BTreeMap<String, Ledger>,
    held: Vec<FeedEvent>,
    pending: Vec<FeedEvent>,
    landing: Vec<(BatchId, Vec<FeedEvent>)>,
    gap_landing: BTreeMap<BatchId, Vec<String>>,
    gap_retry: Vec<Vec<String>>,
    active: Option<Connection>,
    opening: Option<Connection>,
    next_connection: ConnectionId,
    next_batch: BatchId,
    attempts: u32,
    refused: u64,
    drained: bool,
    /// `Closing` came: the end of the drain answers it with `Drained`.
    closing_asked: bool,
    peers: FeedPeers,
    /// The schema declares `cable_when`: `gate` starts closed and follows the stored row.
    gated: bool,
    gate: bool,
}

mod state;
mod flow;

/// The payload of an event of a model that a stream of events reads: the landing adds the rows before and after.
fn with_observe(payload: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(payload) {
        Ok(serde_json::Value::Object(mut object)) => {
            object.insert("observe".into(), serde_json::Value::Bool(true));
            serde_json::Value::Object(object).to_string()
        }
        _ => payload.to_string(),
    }
}

pub struct Feed;

const DEBOUNCE: ScheduleId = ScheduleId(0);

impl Feed {
    fn to_streams(state: &FeedState, model: &str, out: &mut Effects<Self>, input: impl Fn(SubscriptionId) -> DeliveryInput) {
        let Some(ids) = state.want.get(model) else { return };
        for id in ids.iter().filter(|id| state.streams.contains(id)) {
            out.push(Effect::Send(Outbound::new(state.peers.deliveries, input(*id))));
        }
    }

    fn event_debt(events: &[FeedEvent]) -> Vec<crate::owners::store::StoreOp> {
        use crate::owners::read::windows::WindowKind;
        use crate::owners::store::StoreOp;
        let (mut owed, mut models, mut windows, mut families) = (BTreeSet::new(), BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
        for event in events.iter().filter(|event| !event.refused) {
            match &event.reread {
                StaleKeys::AllBut(_) => { models.insert(event.model.clone()); }
                StaleKeys::Set(keys) => for key in keys {
                    match key.kind() {
                        Ok(WindowKind::Relation { relation, params }) if params.contains_key(crate::schema::NODE_PARAM) => {
                            if let Some(id) = params.get(crate::schema::NODE_PARAM).and_then(serde_json::Value::as_str) { owed.insert((key.model.clone(), id.to_string(), relation)); }
                        }
                        _ => { windows.insert((key.model.clone(), key.name.clone())); }
                    }
                },
                StaleKeys::Relations(relations) => families.extend(relations.iter().map(|relation| (event.model.clone(), relation.clone()))),
            }
        }
        let mut ops: Vec<StoreOp> = owed.into_iter().map(|(model, id, relation)| StoreOp::OweReread { model, id, relation }).collect();
        if !models.is_empty() || !windows.is_empty() { ops.push(StoreOp::MarkStale { models: models.into_iter().collect(), keys: windows.into_iter().collect(), kept: Vec::new() }); }
        if !families.is_empty() { ops.push(StoreOp::StaleFamilies { families: families.into_iter().collect() }); }
        ops
    }

    fn commit_batch(state: &FeedState, batch: BatchId, events: Vec<FeedEvent>) -> StoreBatch {
        let ops = Self::event_debt(&events);
        let rows = events.into_iter().map(|event| {
            let observed = state.want.get(&event.model).is_some_and(|ids| ids.iter().any(|id| state.streams.contains(id)));
            (event.model, if observed { with_observe(&event.payload) } else { event.payload })
        }).collect();
        StoreBatch { author: BatchAuthor::Feed, id: batch, stamp: crate::runtime::EpochStamp::Epoch(state.epoch), rows, intents: 0, credential: None, ops }
    }

    fn lower(state: &mut FeedState, mut fx: Vec<Fx>) -> Effects<Self> {
        let first = fx.iter().position(|item| matches!(item, Fx::Gap(_)));
        let models: BTreeSet<_> = fx.iter().filter_map(|item| match item { Fx::Gap(models) => Some(models), _ => None }).flatten().cloned().collect();
        fx.retain(|item| !matches!(item, Fx::Gap(_)));
        if let Some(first) = first {
            let mut batch = Vec::new();
            state.commit_gap(models.into_iter().collect(), &mut batch);
            fx.splice(first..first, batch);
        }
        let peers = state.peers;
        let mut out: Effects<Self> = Vec::new();
        for item in fx {
            match item {
                Fx::Run(effect) => out.push(Effect::Run(effect)),
                Fx::Debounce { after_ms } => out.push(Effect::Schedule { id: DEBOUNCE, after: Duration::from_millis(after_ms), input: FeedInput::Tick }),
                Fx::CancelDebounce => out.push(Effect::Cancel(DEBOUNCE)),
                Fx::Resubscribe { connection, attempt, floor } => {
                    out.push(Effect::Backoff { id: ScheduleId(connection), policy: RetryPolicy::Cable, attempt, floor: floor.unwrap_or(Duration::ZERO), input: FeedInput::Resubscribe { connection_id: connection } });
                }
                Fx::CancelResubscribe { connection } => out.push(Effect::Cancel(ScheduleId(connection))),
                Fx::ResubscribeChannel { connection, channel, slot, attempt } => {
                    out.push(Effect::Backoff { id: ScheduleId(CHANNEL_SCHEDULE | (connection << 16) | slot), policy: RetryPolicy::Cable, attempt, floor: Duration::ZERO, input: FeedInput::ChannelResubscribe { connection_id: connection, channel } });
                }
                Fx::HandshakeDeadline { connection } => out.push(Effect::Schedule { id: ScheduleId(HANDSHAKE_SCHEDULE | connection), after: HANDSHAKE_DEADLINE, input: FeedInput::HandshakeDeadline { connection_id: connection } }),
                Fx::Drain { connection } => out.push(Effect::Schedule { id: drain_schedule(connection), after: crate::owners::read::CLOSE_DRAIN, input: FeedInput::Drain { connection_id: connection } }),
                Fx::CancelDrain { connection } => out.push(Effect::Cancel(drain_schedule(connection))),
                Fx::CancelHandshake { connection } => out.push(Effect::Cancel(ScheduleId(HANDSHAKE_SCHEDULE | connection))),
                Fx::Keepalive { connection, after } => out.push(Effect::Schedule { id: ScheduleId(KEEPALIVE_SCHEDULE | connection), after, input: FeedInput::Keepalive { connection_id: connection } }),
                Fx::PongDeadline { connection, counter } => out.push(Effect::Schedule { id: ScheduleId(PONG_SCHEDULE | connection), after: PONG_DEADLINE, input: FeedInput::PongDeadline { connection_id: connection, counter } }),
                Fx::CancelKeepalive { connection } => out.push(Effect::Cancel(ScheduleId(KEEPALIVE_SCHEDULE | connection))),
                Fx::CancelPong { connection } => out.push(Effect::Cancel(ScheduleId(PONG_SCHEDULE | connection))),
                Fx::Commit { batch, events } => out.push(Effect::Send(Outbound::new(peers.store, StoreInput::Commit(Self::commit_batch(state, batch, events))))),
                Fx::Gap(_) => {}
                Fx::CommitGap { batch, models, kept } => out.push(Effect::Send(Outbound::new(peers.store, StoreInput::Commit(StoreBatch { author: BatchAuthor::Feed, id: batch, stamp: crate::runtime::EpochStamp::Epoch(state.epoch), rows: Vec::new(), intents: 0, credential: None, ops: vec![crate::owners::store::StoreOp::MarkStale { models, keys: Vec::new(), kept }] })))),
                Fx::Stale { model, keys, cause } => out.push(Effect::Send(Outbound::new(peers.windows, WindowsInput::Stale { model, keys, cause }))),
                Fx::Joined { id, position } => out.push(Effect::Send(Outbound::new(peers.deliveries, DeliveryInput::Joined { stream: id, position }))),
                Fx::ModelReady { id: Some(id), .. } => out.push(Effect::Send(Outbound::new(peers.deliveries, DeliveryInput::ModelReady { stream: id }))),
                Fx::ModelReady { model, id: None } => Self::to_streams(state, &model, &mut out, |stream| DeliveryInput::ModelReady { stream }),
                Fx::ModelEvent { model, position, payload } => Self::to_streams(state, &model, &mut out, |stream| DeliveryInput::ModelEvent { stream, position, payload: payload.clone() }),
                Fx::ModelLost { model, first, last } => Self::to_streams(state, &model, &mut out, |stream| DeliveryInput::ModelLost { stream, first, last }),
                Fx::ModelGap { model } => Self::to_streams(state, &model, &mut out, |stream| DeliveryInput::ModelGap { stream }),
                Fx::ModelInvalid { ids } => out.extend(ids.into_iter().map(|stream| Effect::Send(Outbound::new(peers.deliveries, DeliveryInput::ModelInvalid { stream })))),
            }
        }
        out
    }
}

impl StateOwner for Feed {
    const EPOCHED: bool = true;

    type State = FeedState;
    type Input = FeedInput;
    type Effect = FeedEffect;

    #[cfg(test)]
    fn diagnostic(state: &FeedState) -> Option<String> {
        let line = |id: Option<ConnectionId>| id.map(|id| format!("{id}:{:?}", state.protocol(id))).unwrap_or_else(|| "none".into());
        Some(format!("mode={:?}, online={:?}, attempt={}, active={}, opening={}, wanted={:?}, subscribed(sealed)={:?}, confirmed(Subscribed)={:?}, pending={}, frames_queued={}", state.mode(), state.online(), state.attempts(), line(state.active_connection()), line(state.opening_connection()), state.wanted(), state.subscribed(), state.confirmed(), state.pending(), crate::owners::read::feed_io::FRAMES_QUEUED.load(std::sync::atomic::Ordering::SeqCst)))
    }

    fn step(state: &mut FeedState, input: FeedInput, now: crate::runtime::Time) -> Effects<Self> {
        let mut fx = Vec::new();
        if let FeedInput::CableClosed(event) = input {
            for models in std::mem::take(&mut state.gap_retry) { state.commit_gap(models, &mut fx); }
            let mut effects = Self::lower(state, fx);
            effects.push(Effect::Send(Outbound::new(state.peers.deliveries, DeliveryInput::CableClosed(event))));
            return effects;
        }
        if let FeedInput::ActivateEpoch(epoch) = &input {
            *state = FeedState::new(*epoch, state.peers, std::mem::take(&mut state.debounce), std::mem::take(&mut state.channels)).with_kept(std::mem::take(&mut state.kept)).with_gate(state.gated);
            return Vec::new();
        }
        for models in std::mem::take(&mut state.gap_retry) { state.commit_gap(models, &mut fx); }
        if let FeedInput::Census { reply } = input {
            fx.push(Fx::Run(FeedEffect::Census { reply, subscriptions: state.census(), refused: state.refused }));
            return Self::lower(state, fx);
        }
        let draining = match state.mode {
            Mode::Draining { connection, then } if !matches!(input, FeedInput::Observed { .. } | FeedInput::Committed(_) | FeedInput::Rejected(_)) => {
                state.draining(connection, then, input, &mut fx);
                return Self::lower(state, fx);
            }
            Mode::Draining { connection, .. } => Some(connection),
            _ => None,
        };
        if state.mode == Mode::Ended {
            if matches!(input, FeedInput::Closing) && !std::mem::replace(&mut state.drained, true) {
                fx.push(Fx::Run(FeedEffect::Drained(Drained { owner: "feed" })));
            }
            return Self::lower(state, fx);
        }
        if let FeedInput::HandshakeDone { connection_id, .. } | FeedInput::FrameOpened { connection_id, .. } = &input { state.heard(*connection_id, now); }
        match input {
            FeedInput::CableClosed(_) => {}
            FeedInput::ActivateEpoch(_) => {}
            FeedInput::EpochStarted(started) => {
                state.epoch = started.epoch;
                fx.push(Fx::Run(FeedEffect::SnapshotRequest { epoch: started.epoch }));
            }
            FeedInput::EpochEnded(ended) => {
                if ended.epoch == state.epoch {
                    state.end(false, &mut fx);
                }
            }
            FeedInput::Closing => state.end(true, &mut fx),
            FeedInput::Census { .. } => {}
            FeedInput::Gate(open) => state.gate_changed(open, &mut fx),
            FeedInput::WantPlus { model, id, stream, scoped, .. } => state.want_plus(model, id, stream, scoped, &mut fx),
            FeedInput::WantMinus { model, id, .. } => state.want_minus(&model, id, &mut fx),
            FeedInput::HandshakeDone { connection_id, key_id, .. } => state.handshake_done(connection_id, key_id, &mut fx),
            FeedInput::FrameOpened { connection_id, counter, frame, .. } => state.frame_opened(connection_id, counter, frame, &mut fx),
            FeedInput::ProtocolError { connection_id, .. } => {
                if state.connection_mut(connection_id).is_some() {
                    state.protocol_error(connection_id, &mut fx);
                }
            }
            FeedInput::Event { connection_id, channel, event } => state.event(connection_id, &channel, event, &mut fx),
            FeedInput::ChannelConfirmed { connection_id, channel } => state.channel_confirmed(connection_id, channel, &mut fx),
            FeedInput::ChannelEnded { connection_id, channel, error, .. } => state.channel_ended(connection_id, channel, error, &mut fx),
            FeedInput::ChannelResubscribe { connection_id, channel, .. } => state.channel_resubscribe(connection_id, channel, &mut fx),
            FeedInput::Dropped { connection_id } => state.dropped(connection_id, None, &mut fx),
            FeedInput::Limited { connection_id, floor } => state.dropped(connection_id, Some(floor), &mut fx),
            FeedInput::SubscriptionEnded { connection_id } => state.dropped(connection_id, None, &mut fx),
            FeedInput::Resubscribe { connection_id } => state.resubscribe(connection_id, &mut fx),
            FeedInput::HandshakeDeadline { connection_id } => state.handshake_deadline(connection_id, &mut fx),
            FeedInput::Keepalive { connection_id } => state.keepalive(connection_id, now, &mut fx),
            FeedInput::PongDeadline { connection_id, counter } => state.pong_deadline(connection_id, counter, now, &mut fx),
            FeedInput::Drain { .. } => {}
            FeedInput::Observed { batch, payloads } => state.observed(batch, payloads),
            FeedInput::Committed(committed) => {
                let landed = state.owns_landing(committed.batch);
                state.verdict(committed.batch, None, &committed.refused, Some(committed.revision), &mut fx);
                if landed { fx.extend(committed.stale_parents.into_iter().map(|model| Fx::Stale { model, keys: StaleKeys::all(), cause: StaleCause::Change { revision: Some(committed.revision) } })); }
            }
            FeedInput::Rejected(rejected) => state.verdict(rejected.batch, Some(rejected.reason), &[], None, &mut fx),
            FeedInput::Tick => {
                if state.mode == Mode::Live {
                    state.flush_pending(&mut fx);
                }
            }
            FeedInput::Online { online: Online(online), online_seq, .. } => state.online_changed(online, online_seq, &mut fx),
        }
        if let Some(connection) = draining.filter(|_| state.mode == Mode::Ended) {
            state.drain_cut(connection, &mut fx);
        }
        Self::lower(state, fx)
    }

    fn epoch_started(epoch: Epoch) -> Option<FeedInput> { Some(FeedInput::ActivateEpoch(epoch)) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> FeedInput { FeedInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> FeedInput { FeedInput::Closing }

    fn executor_failed(_executor: &str, pending: Vec<crate::runtime::EffectId>) -> Vec<FeedInput> { pending.into_iter().map(|connection_id| FeedInput::Dropped { connection_id }).collect() }
}
