//! Owner `Feed`: the server subscription of the epoch, the connection protocol, the model events and their positions.
//! Timers are `Effect::Schedule` of the owner (`after` = the debounce or the backoff). Inputs for other owners
//! (`Commit`, `Stale`, `Joined`, `ModelReady`, `ModelEvent`, `ModelLost`, `ModelGap`) are `Effect::Send` to the addresses of the state.
//! The executor opens connections, encrypts and decrypts: it answers with `HandshakeDone`, `FrameOpened` and `ProtocolError`. The inputs
//! `Event` and `ChannelLost` of the data table also come from the transport; `FrameOpened` of an `Established` connection forwards its events as `Event`.
//! A model reads one or more channels (subscriptions of the server) on the one connection: it is ready once every channel of it is confirmed.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::owners::contract::{BatchAuthor, BatchId, Committed, ConnectionId, Drained, EpochEnded, EpochStarted, ModelsReset, Online, RejectReason, Rejected, ReplyId};
use crate::owners::read::deliveries::{Deliveries, DeliveryInput};
use crate::owners::read::windows::{StaleKeys, Windows, WindowsInput};
use crate::owners::store::{StoreBatch, StoreInput, StoreWriter};
use crate::pacing::RetryPolicy;
use crate::runtime::{Address, Cause, Effect, Effects, Epoch, Outbound, ScheduleId, StateOwner, Time};

/// The id a `Want+` carries: a stream `Events` of Deliveries or a window with `live`.
pub type SubscriptionId = u64;

/// A connection that finished no handshake by then fails like a protocol error.
pub const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);

/// The schedule ids of the handshake deadlines are above the connection ids that name the resubscribe timers.
const HANDSHAKE_SCHEDULE: u64 = 1 << 40;

/// The schedule ids of the channel resubscribe timers: the connection and the slot of the channel under this bit.
const CHANNEL_SCHEDULE: u64 = 1 << 41;

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
}

/// An accepted event: its position is fixed when the frame arrives, its basis never changes.
#[derive(Clone, Debug, PartialEq, Eq)]
struct FeedEvent {
    model: String,
    payload: String,
    reread: StaleKeys,
    /// `model_generation` of the model when the frame arrived.
    basis: u64,
    position: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Off { gap: bool },
    Opening { gap: bool },
    Live,
    Ended,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolReason {
    Handshake,
    Decrypt,
    /// A frame that passed the schema of the cable but is no valid frame of the subscription.
    Frame,
    /// A `next` frame whose result carries GraphQL errors, with or without data.
    Errors,
}

/// The name of a channel: the subscription of the `.dbl` the server runs for a model. A frame carries it as its `id`.
pub type Channel = String;

/// What a decrypted frame of the server says, as the executor reads it. The owner checks the counter before it reads the meaning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Ping { ts: serde_json::Number },
    Events { id: String, events: Vec<FramedEvent> },
    /// The server confirmed the subscription of the channel `id`.
    Confirmed { id: String },
    Complete { id: String },
    /// The server closes the connection.
    Ended,
    /// An `error` frame: `id` names the subscription it is for, none = the whole connection.
    Error { id: Option<String> },
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
}

/// The frames Feed sends over an `Established` connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outgoing {
    Subscribe { channel: Channel },
    Unsubscribe { channel: Channel },
    /// The answer to a server `ping`.
    Pong { ts: serde_json::Number },
}

/// The effects of the runner: IO of the connection and the end of the owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FeedEffect {
    /// Transport `Subscribe(want)`: opens the connection and runs its handshake.
    Connect { connection: ConnectionId },
    /// The executor encrypts `frame` with the key `key_id` and the counter, both given by value.
    Seal { connection: ConnectionId, key_id: u64, counter: u64, frame: Outgoing },
    /// Transport `Unsubscribe(connection)`.
    Close { connection: ConnectionId },
    Drained(Drained),
    /// `SnapshotRequest{Feed, epoch}` to Network: the first `Online` of the epoch comes back addressed to Feed.
    SnapshotRequest { epoch: Epoch },
    /// The answer of `Census`: the channels the models of `want` read on the active connection.
    Census { reply: ReplyId, subscriptions: usize },
}

/// What the steps give out: an effect of the runner, a timer, or an input for another owner.
enum Fx {
    Run(FeedEffect),
    Debounce { after_ms: u64 },
    CancelDebounce,
    Resubscribe { connection: ConnectionId, attempt: u32 },
    CancelResubscribe { connection: ConnectionId },
    ResubscribeChannel { connection: ConnectionId, channel: Channel, attempt: u32 },
    HandshakeDeadline { connection: ConnectionId },
    CancelHandshake { connection: ConnectionId },
    Commit { batch: BatchId, events: Vec<FeedEvent> },
    Stale { model: String, keys: StaleKeys },
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
    /// `stream` = the id is a stream `Events` of Deliveries: it gets `Joined` and `ModelReady`.
    WantPlus { model: String, id: SubscriptionId, stream: bool },
    WantMinus { model: String, id: SubscriptionId },
    HandshakeDone { connection_id: ConnectionId, key_id: u64 },
    FrameOpened { connection_id: ConnectionId, counter: u64, frame: Frame },
    ProtocolError { connection_id: ConnectionId, reason: ProtocolReason },
    Event { connection_id: ConnectionId, channel: Channel, event: FramedEvent },
    ChannelConfirmed { connection_id: ConnectionId, channel: Channel },
    /// The server closed the subscription of one channel: `failed` = with an error of the request.
    ChannelEnded { connection_id: ConnectionId, channel: Channel, failed: bool },
    ChannelResubscribe { connection_id: ConnectionId, channel: Channel },
    /// `dropped` = the number of lost events per model, `None` = the number is unknown.
    ChannelLost { connection_id: ConnectionId, dropped: Option<BTreeMap<String, u64>> },
    Dropped { connection_id: ConnectionId },
    SubscriptionEnded { connection_id: ConnectionId },
    Resubscribe { connection_id: ConnectionId },
    /// The timer of a `Connect`: a connection still in `Handshake` fails.
    HandshakeDeadline { connection_id: ConnectionId },
    Committed(Committed),
    /// The payloads of the events of a batch with the rows read at its landing: they arrive before the `Committed` of the batch.
    Observed { batch: BatchId, payloads: Vec<String> },
    Rejected(Rejected),
    Tick(Time),
    Online(Online),
    ModelsReset(ModelsReset),
    EpochStarted(EpochStarted),
    EpochEnded(EpochEnded),
    Closing,
    Census { reply: ReplyId },
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
    want: BTreeMap<String, BTreeSet<SubscriptionId>>,
    streams: BTreeSet<SubscriptionId>,
    /// `updates.debounce` of the models, from the schema at `install`.
    debounce: BTreeMap<String, u64>,
    /// The channels each model reads, from the schema at `install`.
    channels: BTreeMap<String, Vec<Channel>>,
    /// Channels with a `Subscribe` on the active connection.
    subscribed: BTreeSet<Channel>,
    /// Channels the server confirmed on the active connection.
    confirmed: BTreeSet<Channel>,
    /// Models given `ModelReady` on the active connection.
    announced: BTreeSet<String>,
    /// Models that owe a `ModelGap` at their next readiness: a break came before their channels were confirmed.
    gapped: BTreeSet<String>,
    channel_attempts: BTreeMap<Channel, u32>,
    generation: BTreeMap<String, u64>,
    base_generation: u64,
    position: BTreeMap<String, u64>,
    ledger: BTreeMap<String, Ledger>,
    held: Vec<FeedEvent>,
    pending: Vec<FeedEvent>,
    landing: Vec<(BatchId, Vec<FeedEvent>)>,
    active: Option<Connection>,
    opening: Option<Connection>,
    next_connection: ConnectionId,
    next_batch: BatchId,
    attempts: u32,
    drained: bool,
    peers: FeedPeers,
}

impl FeedState {
    pub fn new(epoch: Epoch, peers: FeedPeers, debounce: BTreeMap<String, u64>, channels: BTreeMap<String, Vec<Channel>>) -> Self {
        Self {
            epoch,
            mode: Mode::Off { gap: false },
            online: None,
            want: BTreeMap::new(),
            streams: BTreeSet::new(),
            debounce,
            channels,
            subscribed: BTreeSet::new(),
            confirmed: BTreeSet::new(),
            announced: BTreeSet::new(),
            gapped: BTreeSet::new(),
            channel_attempts: BTreeMap::new(),
            generation: BTreeMap::new(),
            base_generation: 0,
            position: BTreeMap::new(),
            ledger: BTreeMap::new(),
            held: Vec::new(),
            pending: Vec::new(),
            landing: Vec::new(),
            active: None,
            opening: None,
            next_connection: 1,
            next_batch: 1,
            attempts: 0,
            drained: false,
            peers,
        }
    }

    pub fn mode(&self) -> Mode { self.mode }

    /// The channels that the models of `want` read, counted while a connection is active.
    fn census(&self) -> usize {
        if self.mode == Mode::Ended || self.active.is_none() {
            return 0;
        }
        self.want.iter().filter(|(_, ids)| !ids.is_empty()).filter_map(|(model, _)| self.channels.get(model)).flatten().collect::<BTreeSet<_>>().len()
    }

    pub fn online(&self) -> Option<bool> { self.online }

    pub fn position(&self, model: &str) -> u64 { self.position.get(model).copied().unwrap_or(0) }

    pub fn held(&self) -> usize { self.held.len() }

    pub fn pending(&self) -> usize { self.pending.len() }

    pub fn landing(&self) -> usize { self.landing.len() }

    pub fn wants(&self) -> Vec<String> { self.want.keys().cloned().collect() }

    pub fn ids(&self, model: &str) -> Vec<SubscriptionId> { self.want.get(model).map(|ids| ids.iter().copied().collect()).unwrap_or_default() }

    pub fn active_connection(&self) -> Option<ConnectionId> { self.active.as_ref().map(|connection| connection.id) }

    pub fn opening_connection(&self) -> Option<ConnectionId> { self.opening.as_ref().map(|connection| connection.id) }

    pub fn protocol(&self, id: ConnectionId) -> Option<Protocol> {
        [&self.active, &self.opening].into_iter().flatten().find(|connection| connection.id == id).map(|connection| connection.protocol)
    }

    fn models(&self) -> Vec<String> { self.want.keys().cloned().collect() }

    /// The place of a channel among all channels of the schema: a stable number for its timer id.
    fn channel_slot(&self, channel: &str) -> u64 {
        let all: BTreeSet<&Channel> = self.channels.values().flatten().collect();
        all.iter().position(|own| own.as_str() == channel).map_or(0, |at| at as u64)
    }

    fn channels_of(&self, model: &str) -> &[Channel] { self.channels.get(model).map_or(&[], Vec::as_slice) }

    /// The channels the models of `want` read.
    pub fn wanted(&self) -> BTreeSet<Channel> { self.want.keys().flat_map(|model| self.channels_of(model).iter().cloned()).collect() }

    pub fn confirmed(&self) -> Vec<Channel> { self.confirmed.iter().cloned().collect() }

    pub fn subscribed(&self) -> Vec<Channel> { self.subscribed.iter().cloned().collect() }

    fn model_is_ready(&self, model: &str) -> bool { self.channels_of(model).iter().all(|channel| self.confirmed.contains(channel)) }

    fn models_of(&self, channel: &str) -> Vec<String> { self.want.keys().filter(|model| self.channels_of(model).iter().any(|own| own == channel)).cloned().collect() }

    fn debounce_of(&self, model: &str) -> u64 { self.debounce.get(model).copied().unwrap_or(0) }

    fn generation_of(&self, model: &str) -> u64 { self.generation.get(model).copied().unwrap_or(self.base_generation) }

    fn is_online(&self) -> bool { self.online == Some(true) }

    fn fresh_connection(&mut self) -> Connection {
        let id = self.next_connection;
        self.next_connection += 1;
        Connection { id, protocol: Protocol::Handshake }
    }

    /// Transport `Subscribe(want)` on a new connection.
    fn open_connection(&mut self, fx: &mut Vec<Fx>) {
        let connection = self.fresh_connection();
        fx.push(Fx::Run(FeedEffect::Connect { connection: connection.id }));
        fx.push(Fx::HandshakeDeadline { connection: connection.id });
        self.opening = Some(connection);
    }

    /// A new `opening_connection` that waits for `Resubscribe` by the backoff.
    fn wait_resubscribe(&mut self, fx: &mut Vec<Fx>) {
        let connection = self.fresh_connection();
        self.attempts += 1;
        fx.push(Fx::Resubscribe { connection: connection.id, attempt: self.attempts });
        self.opening = Some(connection);
    }

    fn connection_mut(&mut self, id: ConnectionId) -> Option<&mut Connection> {
        [&mut self.active, &mut self.opening].into_iter().flatten().find(|connection| connection.id == id)
    }

    fn seal(connection: &mut Connection, frame: Outgoing, fx: &mut Vec<Fx>) {
        if let Protocol::Established { key_id, send_counter, .. } = &mut connection.protocol {
            fx.push(Fx::Run(FeedEffect::Seal { connection: connection.id, key_id: *key_id, counter: *send_counter, frame }));
            *send_counter += 1;
        }
    }

    /// Transport `Unsubscribe(connection)`: an established connection says goodbye, then closes.
    fn close(connection: Option<Connection>, subscribed: &BTreeSet<Channel>, fx: &mut Vec<Fx>) {
        let Some(mut connection) = connection else { return };
        for channel in subscribed {
            Self::seal(&mut connection, Outgoing::Unsubscribe { channel: channel.clone() }, fx);
        }
        fx.push(Fx::CancelResubscribe { connection: connection.id });
        fx.push(Fx::CancelHandshake { connection: connection.id });
        fx.push(Fx::Run(FeedEffect::Close { connection: connection.id }));
    }

    /// Transport `Unsubscribe` of both connections, the active one first.
    fn close_all(&mut self, fx: &mut Vec<Fx>) {
        Self::close(self.active.take(), &self.subscribed, fx);
        Self::close(self.opening.take(), &BTreeSet::new(), fx);
    }

    fn accept(&mut self, event: FramedEvent) -> FeedEvent {
        let position = self.position(&event.model) + 1;
        self.position.insert(event.model.clone(), position);
        self.ledger.entry(event.model.clone()).or_insert_with(Ledger::new).slots.insert(position, Slot::Pending);
        let basis = self.generation_of(&event.model);
        FeedEvent { model: event.model, payload: event.payload, reread: event.reread, basis, position }
    }

    /// The positions of `count` lost events of `model`.
    fn lose(&mut self, model: &str, count: u64, fx: &mut Vec<Fx>) {
        let at = self.position(model);
        self.position.insert(model.to_string(), at + count);
        let ledger = self.ledger.entry(model.to_string()).or_insert_with(Ledger::new);
        for position in at + 1..=at + count {
            ledger.slots.insert(position, Slot::Lost);
        }
        ledger.emit(model, fx);
    }

    fn mark_lost(&mut self, events: &[FeedEvent], fx: &mut Vec<Fx>) -> Vec<String> {
        let mut models: Vec<String> = Vec::new();
        for event in events {
            if let Some(ledger) = self.ledger.get_mut(&event.model) {
                ledger.slots.insert(event.position, Slot::Lost);
            }
            if !models.contains(&event.model) {
                models.push(event.model.clone());
            }
        }
        for model in &models {
            if let Some(ledger) = self.ledger.get_mut(model) {
                ledger.emit(model, fx);
            }
        }
        models
    }

    fn stale_all(models: &[String], fx: &mut Vec<Fx>) {
        for model in models {
            fx.push(Fx::Stale { model: model.clone(), keys: StaleKeys::All });
        }
    }

    /// `held` -> `Lost` (all, or only the models in `only`): the models whose windows turn `Stale`.
    fn lose_held(&mut self, only: Option<&[String]>, fx: &mut Vec<Fx>) -> Vec<String> {
        let (lost, kept): (Vec<FeedEvent>, Vec<FeedEvent>) = std::mem::take(&mut self.held).into_iter().partition(|event| only.is_none_or(|only| only.contains(&event.model)));
        self.held = kept;
        self.mark_lost(&lost, fx)
    }

    fn commit(&mut self, events: Vec<FeedEvent>, fx: &mut Vec<Fx>) {
        let batch = self.next_batch;
        self.next_batch += 1;
        self.landing.push((batch, events.clone()));
        fx.push(Fx::Commit { batch, events });
    }

    /// One batch per equal basis, in the order the bases first appear.
    fn commit_by_basis(&mut self, events: Vec<FeedEvent>, fx: &mut Vec<Fx>) {
        let mut groups: Vec<(u64, Vec<FeedEvent>)> = Vec::new();
        for event in events {
            match groups.iter_mut().find(|(basis, _)| *basis == event.basis) {
                Some((_, group)) => group.push(event),
                None => groups.push((event.basis, vec![event])),
            }
        }
        for (_, group) in groups {
            self.commit(group, fx);
        }
    }

    fn flush_pending(&mut self, fx: &mut Vec<Fx>) {
        if self.pending.is_empty() {
            return;
        }
        let events = std::mem::take(&mut self.pending);
        fx.push(Fx::CancelDebounce);
        self.commit_by_basis(events, fx);
    }

    fn observed(&mut self, batch: BatchId, payloads: Vec<String>) {
        let Some((_, events)) = self.landing.iter_mut().find(|(id, _)| *id == batch) else { return };
        if payloads.len() == events.len() {
            for (event, payload) in events.iter_mut().zip(payloads) {
                event.payload = payload;
            }
        }
    }

    /// `Committed` and `Rejected` of a batch of `landing`: delivered on `Committed`, lost on `Rejected`. `NoSession` and `Closed` end a `Live` feed.
    fn verdict(&mut self, batch: BatchId, rejected: Option<RejectReason>, fx: &mut Vec<Fx>) {
        let Some(at) = self.landing.iter().position(|(id, _)| *id == batch) else { return };
        let events = self.landing.remove(at).1;
        match rejected {
            None => self.land(events, fx),
            Some(RejectReason::StaleGeneration { models, .. }) => {
                let (lost, rest): (Vec<FeedEvent>, Vec<FeedEvent>) = events.into_iter().partition(|event| models.contains(&event.model));
                let lost_models = self.mark_lost(&lost, fx);
                Self::stale_all(&lost_models, fx);
                if !rest.is_empty() {
                    let rest = rest.into_iter().map(|event| FeedEvent { basis: self.generation_of(&event.model), ..event }).collect();
                    self.commit_by_basis(rest, fx);
                }
            }
            Some(RejectReason::Storage(_) | RejectReason::Invalid) => {
                let lost_models = self.mark_lost(&events, fx);
                Self::stale_all(&lost_models, fx);
            }
            Some(RejectReason::NoSession | RejectReason::Closed) => {
                self.end(false, fx);
            }
        }
    }

    fn land(&mut self, events: Vec<FeedEvent>, fx: &mut Vec<Fx>) {
        let mut models: Vec<String> = Vec::new();
        for event in &events {
            if let Some(ledger) = self.ledger.get_mut(&event.model) {
                ledger.slots.insert(event.position, Slot::Landed(event.payload.clone()));
            }
            if !models.contains(&event.model) {
                models.push(event.model.clone());
            }
        }
        for model in &models {
            if let Some(ledger) = self.ledger.get_mut(model) {
                ledger.emit(model, fx);
            }
        }
        for model in models {
            let keys = events.iter().filter(|event| event.model == model).fold(StaleKeys::none(), |keys, event| keys.union(event.reread.clone()));
            if !keys.is_empty() {
                fx.push(Fx::Stale { model, keys });
            }
        }
    }

    /// The active connection starts: every wanted channel is subscribed, a model without a channel to wait for is ready at once.
    fn begin_connection(&mut self, gap: bool, fx: &mut Vec<Fx>) {
        self.confirmed.clear();
        self.announced.clear();
        self.subscribed.clear();
        self.channel_attempts.clear();
        self.gapped = if gap { self.want.keys().cloned().collect() } else { BTreeSet::new() };
        self.subscribe_missing(fx);
        self.announce_ready(fx);
    }

    /// `Subscribe` of every wanted channel the active connection has not subscribed yet.
    fn subscribe_missing(&mut self, fx: &mut Vec<Fx>) {
        let wanted = self.wanted();
        let Some(connection) = self.active.as_mut() else { return };
        for channel in wanted {
            if self.subscribed.insert(channel.clone()) {
                Self::seal(connection, Outgoing::Subscribe { channel }, fx);
            }
        }
    }

    /// `Unsubscribe` of the channels no model of `want` reads.
    fn narrow(&mut self, fx: &mut Vec<Fx>) {
        let wanted = self.wanted();
        let gone: Vec<Channel> = self.subscribed.iter().filter(|channel| !wanted.contains(*channel)).cloned().collect();
        for channel in gone {
            self.subscribed.remove(&channel);
            self.confirmed.remove(&channel);
            self.channel_attempts.remove(&channel);
            if let Some(connection) = self.active.as_mut() {
                Self::seal(connection, Outgoing::Unsubscribe { channel }, fx);
            }
        }
    }

    /// `ModelGap` and `Stale` of the models that owe a gap, then `ModelReady` of every model whose channels are all confirmed; the frames `held` for them sit.
    fn announce_ready(&mut self, fx: &mut Vec<Fx>) {
        let ready: Vec<String> = self.want.keys().filter(|model| !self.announced.contains(*model) && self.model_is_ready(model)).cloned().collect();
        if ready.is_empty() {
            return;
        }
        let gapped: Vec<String> = ready.iter().filter(|model| self.gapped.remove(*model)).cloned().collect();
        for model in &gapped {
            fx.push(Fx::ModelGap { model: model.clone() });
        }
        Self::stale_all(&gapped, fx);
        for model in &ready {
            fx.push(Fx::ModelReady { model: model.clone(), id: None });
            self.announced.insert(model.clone());
        }
        let (mine, other): (Vec<FeedEvent>, Vec<FeedEvent>) = std::mem::take(&mut self.held).into_iter().partition(|event| ready.contains(&event.model));
        self.held = other;
        self.seat(mine, fx);
    }

    /// Every model of `want` has a break of unknown length: `ModelGap`, `Stale` of its windows.
    fn gap_all(&self, fx: &mut Vec<Fx>) {
        for model in self.models() {
            fx.push(Fx::ModelGap { model });
        }
        Self::stale_all(&self.models(), fx);
    }

    /// `Event` of the current connection in `Live`: the event waits for the debounce of its model or lands at once.
    fn seat(&mut self, events: Vec<FeedEvent>, fx: &mut Vec<Fx>) {
        let mut now: Vec<FeedEvent> = Vec::new();
        let mut scheduled: Option<u64> = None;
        for event in events {
            let debounce = self.debounce_of(&event.model);
            if debounce == 0 {
                now.push(event);
                continue;
            }
            if self.pending.is_empty() && scheduled.is_none() {
                scheduled = Some(debounce);
            }
            self.pending.push(event);
        }
        if let Some(after_ms) = scheduled {
            fx.push(Fx::Debounce { after_ms });
        }
        self.commit_by_basis(now, fx);
    }

    fn ready(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        match self.mode {
            Mode::Opening { gap } if self.opening_connection() == Some(connection) => {
                self.active = self.opening.take();
                self.mode = Mode::Live;
                self.attempts = 0;
                self.begin_connection(gap, fx);
            }
            _ => {}
        }
    }

    fn event(&mut self, connection: ConnectionId, channel: &str, event: FramedEvent, fx: &mut Vec<Fx>) {
        if !self.want.contains_key(&event.model) {
            return;
        }
        match self.mode {
            Mode::Opening { .. } if self.opening_connection() == Some(connection) => {
                let accepted = self.accept(event);
                self.held.push(accepted);
            }
            Mode::Live if self.active_connection() == Some(connection) && self.subscribed.contains(channel) => {
                let ready = self.announced.contains(&event.model);
                let accepted = self.accept(event);
                if ready {
                    self.seat(vec![accepted], fx);
                } else {
                    self.held.push(accepted);
                }
            }
            _ => {}
        }
    }

    fn channel_confirmed(&mut self, connection: ConnectionId, channel: Channel, fx: &mut Vec<Fx>) {
        if self.mode != Mode::Live || self.active_connection() != Some(connection) || !self.subscribed.contains(&channel) || !self.confirmed.insert(channel.clone()) {
            return;
        }
        let regapped: Vec<String> = self.models_of(&channel).into_iter().filter(|model| self.announced.contains(model) && self.gapped.remove(model)).collect();
        for model in &regapped {
            fx.push(Fx::ModelGap { model: model.clone() });
        }
        Self::stale_all(&regapped, fx);
        self.announce_ready(fx);
    }

    /// The server closed one channel: without an error the channel subscribes again by the backoff, with an error its models leave `want`.
    fn channel_ended(&mut self, connection: ConnectionId, channel: Channel, failed: bool, fx: &mut Vec<Fx>) {
        if self.mode != Mode::Live || self.active_connection() != Some(connection) || !self.subscribed.contains(&channel) {
            return;
        }
        let models = self.models_of(&channel);
        self.confirmed.remove(&channel);
        if !failed {
            for model in models.into_iter().filter(|model| self.announced.contains(model)) {
                self.gapped.insert(model);
            }
            let attempt = self.channel_attempts.entry(channel.clone()).or_insert(0);
            *attempt += 1;
            fx.push(Fx::ResubscribeChannel { connection, channel, attempt: *attempt });
            return;
        }
        let mut ids: Vec<SubscriptionId> = Vec::new();
        for model in models {
            self.announced.remove(&model);
            self.gapped.remove(&model);
            ids.extend(self.want.remove(&model).unwrap_or_default().into_iter().filter(|id| self.streams.remove(id)));
        }
        self.subscribed.remove(&channel);
        self.channel_attempts.remove(&channel);
        if !ids.is_empty() {
            fx.push(Fx::ModelInvalid { ids });
        }
        self.narrow(fx);
        if self.want.is_empty() {
            self.mode = Mode::Off { gap: false };
            self.flush_pending(fx);
            self.close_all(fx);
        }
    }

    fn channel_resubscribe(&mut self, connection: ConnectionId, channel: Channel, fx: &mut Vec<Fx>) {
        if self.mode != Mode::Live || self.active_connection() != Some(connection) || !self.subscribed.contains(&channel) || self.confirmed.contains(&channel) {
            return;
        }
        if let Some(found) = self.active.as_mut() {
            Self::seal(found, Outgoing::Subscribe { channel }, fx);
        }
    }

    fn channel_lost(&mut self, connection: ConnectionId, dropped: Option<BTreeMap<String, u64>>, fx: &mut Vec<Fx>) {
        let current = match self.mode {
            Mode::Opening { .. } => self.opening_connection() == Some(connection),
            Mode::Live => self.active_connection() == Some(connection),
            _ => false,
        };
        if !current {
            return;
        }
        if self.mode == Mode::Live {
            self.flush_pending(fx);
        }
        match dropped {
            Some(counts) => {
                let models: Vec<String> = counts.keys().filter(|model| self.want.contains_key(*model)).cloned().collect();
                for model in &models {
                    self.lose(model, counts[model], fx);
                }
                Self::stale_all(&models, fx);
            }
            None => self.gap_all(fx),
        }
    }

    /// `Dropped` and `SubscriptionEnded` of a known connection.
    fn dropped(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        match self.mode {
            Mode::Opening { .. } if self.opening_connection() == Some(connection) => {
                let lost = self.lose_held(None, fx);
                self.wait_resubscribe(fx);
                Self::stale_all(&lost, fx);
            }
            Mode::Live if self.active_connection() == Some(connection) => {
                self.flush_pending(fx);
                let lost = self.lose_held(None, fx);
                self.active = None;
                self.mode = Mode::Opening { gap: true };
                self.attempts = 0;
                self.open_connection(fx);
                Self::stale_all(&lost, fx);
            }
            _ => {}
        }
    }

    fn protocol_failed(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        fx.push(Fx::Run(FeedEffect::Close { connection }));
        self.dropped(connection, fx);
    }

    fn handshake_done(&mut self, connection: ConnectionId, key_id: u64, fx: &mut Vec<Fx>) {
        let Some(found) = self.connection_mut(connection) else { return };
        if found.protocol != Protocol::Handshake {
            return;
        }
        found.protocol = Protocol::Established { key_id, send_counter: FIRST_SEND_COUNTER, recv_counter: FIRST_RECV_COUNTER };
        fx.push(Fx::CancelHandshake { connection });
        self.ready(connection, fx);
    }

    fn handshake_deadline(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        if self.protocol(connection) == Some(Protocol::Handshake) {
            self.protocol_failed(connection, fx);
        }
    }

    fn frame_opened(&mut self, connection: ConnectionId, counter: u64, frame: Frame, fx: &mut Vec<Fx>) {
        let Some(found) = self.connection_mut(connection) else { return };
        let accepted = match &mut found.protocol {
            Protocol::Established { recv_counter, .. } if *recv_counter == counter => {
                *recv_counter += 1;
                true
            }
            _ => false,
        };
        if !accepted {
            self.protocol_failed(connection, fx);
            return;
        }
        match frame {
            Frame::Ping { ts } => {
                if let Some(found) = self.connection_mut(connection) {
                    Self::seal(found, Outgoing::Pong { ts }, fx);
                }
            }
            Frame::Events { id, events } => {
                for event in events {
                    self.event(connection, &id, event, fx);
                }
            }
            Frame::Confirmed { id } => self.channel_confirmed(connection, id, fx),
            Frame::Complete { id } => self.channel_ended(connection, id, false, fx),
            Frame::Error { id: Some(id) } => self.channel_ended(connection, id, true, fx),
            Frame::Ended | Frame::Error { id: None } => self.dropped(connection, fx),
            Frame::Quiet => {}
        }
    }

    fn resubscribe(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        let waiting = matches!(self.mode, Mode::Opening { .. }) && self.is_online() && self.opening_connection() == Some(connection);
        if !waiting {
            return;
        }
        if let Some(found) = self.opening.as_mut() {
            *found = Connection { id: connection, protocol: Protocol::Handshake };
        }
        fx.push(Fx::Run(FeedEffect::Connect { connection }));
        fx.push(Fx::HandshakeDeadline { connection });
    }

    fn want_plus(&mut self, model: String, id: SubscriptionId, stream: bool, fx: &mut Vec<Fx>) {
        let new = !self.want.contains_key(&model);
        let position = self.position(&model);
        self.want.entry(model.clone()).or_default().insert(id);
        if stream {
            self.streams.insert(id);
            fx.push(Fx::Joined { id, position });
        }
        match self.mode {
            Mode::Off { gap } if self.is_online() => {
                self.mode = Mode::Opening { gap };
                self.attempts = 0;
                self.open_connection(fx);
            }
            Mode::Live => {
                self.subscribe_missing(fx);
                if new {
                    self.announce_ready(fx);
                } else if stream && self.announced.contains(&model) {
                    fx.push(Fx::ModelReady { model, id: Some(id) });
                }
            }
            _ => {}
        }
    }

    fn want_minus(&mut self, model: &str, id: SubscriptionId, fx: &mut Vec<Fx>) {
        let Some(ids) = self.want.get_mut(model) else { return };
        if !ids.remove(&id) {
            return;
        }
        self.streams.remove(&id);
        if !ids.is_empty() {
            return;
        }
        self.want.remove(model);
        self.announced.remove(model);
        self.gapped.remove(model);
        if !self.want.is_empty() {
            if self.mode == Mode::Live {
                self.narrow(fx);
            }
            return;
        }
        match self.mode {
            Mode::Opening { gap } => {
                self.mode = Mode::Off { gap };
                self.leave(fx);
            }
            Mode::Live => {
                self.mode = Mode::Off { gap: false };
                self.flush_pending(fx);
                self.close_all(fx);
            }
            _ => {}
        }
    }

    /// `Opening` -> `Off`: `held` is lost, the connection closes.
    fn leave(&mut self, fx: &mut Vec<Fx>) {
        self.close_all(fx);
        let lost = self.lose_held(None, fx);
        Self::stale_all(&lost, fx);
    }

    fn online_changed(&mut self, online: bool, fx: &mut Vec<Fx>) {
        self.online = Some(online);
        match self.mode {
            Mode::Off { gap } if online && !self.want.is_empty() => {
                self.mode = Mode::Opening { gap };
                self.attempts = 0;
                self.open_connection(fx);
            }
            Mode::Opening { gap } if !online => {
                self.mode = Mode::Off { gap };
                self.leave(fx);
            }
            Mode::Live if !online => {
                self.mode = Mode::Off { gap: true };
                self.flush_pending(fx);
                self.close_all(fx);
            }
            _ => {}
        }
    }

    fn models_reset(&mut self, reset: ModelsReset, fx: &mut Vec<Fx>) {
        for model in &reset.models {
            self.generation.insert(model.clone(), reset.generation);
        }
        if matches!(self.mode, Mode::Opening { .. }) {
            let lost = self.lose_held(Some(&reset.models), fx);
            Self::stale_all(&lost, fx);
        }
    }

    fn end(&mut self, drained: bool, fx: &mut Vec<Fx>) {
        self.mode = Mode::Ended;
        self.close_all(fx);
        self.held.clear();
        if !self.pending.is_empty() {
            self.pending.clear();
            fx.push(Fx::CancelDebounce);
        }
        if drained && !std::mem::replace(&mut self.drained, true) {
            fx.push(Fx::Run(FeedEffect::Drained(Drained { owner: "feed" })));
        }
    }
}

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

    fn commit_batch(state: &FeedState, batch: BatchId, events: Vec<FeedEvent>) -> StoreBatch {
        let mut basis = BTreeMap::new();
        for event in &events {
            let entry = basis.entry(event.model.clone()).or_insert(event.basis);
            *entry = (*entry).min(event.basis);
        }
        let rows = events.into_iter().map(|event| {
            let observed = state.want.get(&event.model).is_some_and(|ids| ids.iter().any(|id| state.streams.contains(id)));
            (event.model, if observed { with_observe(&event.payload) } else { event.payload })
        }).collect();
        StoreBatch { author: BatchAuthor::Feed, id: batch, epoch: state.epoch, basis, rows, intents: 0, credential: None, ops: Vec::new() }
    }

    fn lower(state: &FeedState, fx: Vec<Fx>) -> Effects<Self> {
        let peers = state.peers;
        let mut out: Effects<Self> = Vec::new();
        for item in fx {
            match item {
                Fx::Run(effect) => out.push(Effect::Run(effect)),
                Fx::Debounce { after_ms } => out.push(Effect::Schedule { id: DEBOUNCE, after: Duration::from_millis(after_ms), input: FeedInput::Tick(Time(0)) }),
                Fx::CancelDebounce => out.push(Effect::Cancel(DEBOUNCE)),
                Fx::Resubscribe { connection, attempt } => {
                    let after = RetryPolicy::Subscription.backoff().delay(attempt - 1);
                    out.push(Effect::Schedule { id: ScheduleId(connection), after, input: FeedInput::Resubscribe { connection_id: connection } });
                }
                Fx::CancelResubscribe { connection } => out.push(Effect::Cancel(ScheduleId(connection))),
                Fx::ResubscribeChannel { connection, channel, attempt } => {
                    let after = RetryPolicy::Subscription.backoff().delay(attempt - 1);
                    out.push(Effect::Schedule { id: ScheduleId(CHANNEL_SCHEDULE | (connection << 16) | state.channel_slot(&channel)), after, input: FeedInput::ChannelResubscribe { connection_id: connection, channel } });
                }
                Fx::HandshakeDeadline { connection } => out.push(Effect::Schedule { id: ScheduleId(HANDSHAKE_SCHEDULE | connection), after: HANDSHAKE_DEADLINE, input: FeedInput::HandshakeDeadline { connection_id: connection } }),
                Fx::CancelHandshake { connection } => out.push(Effect::Cancel(ScheduleId(HANDSHAKE_SCHEDULE | connection))),
                Fx::Commit { batch, events } => out.push(Effect::Send(Outbound::new(peers.store, StoreInput::Commit(Self::commit_batch(state, batch, events))))),
                Fx::Stale { model, keys } => out.push(Effect::Send(Outbound::new(peers.windows, WindowsInput::Stale { model, keys }))),
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

    fn step(state: &mut FeedState, input: FeedInput) -> Effects<Self> {
        let mut fx = Vec::new();
        if let FeedInput::Census { reply } = input {
            fx.push(Fx::Run(FeedEffect::Census { reply, subscriptions: state.census() }));
            return Self::lower(state, fx);
        }
        if state.mode == Mode::Ended {
            if matches!(input, FeedInput::Closing) && !std::mem::replace(&mut state.drained, true) {
                fx.push(Fx::Run(FeedEffect::Drained(Drained { owner: "feed" })));
            }
            return Self::lower(state, fx);
        }
        match input {
            FeedInput::EpochStarted(started) => {
                state.epoch = started.epoch;
                state.base_generation = started.generation;
                fx.push(Fx::Run(FeedEffect::SnapshotRequest { epoch: started.epoch }));
            }
            FeedInput::EpochEnded(ended) => {
                if ended.epoch == state.epoch {
                    state.end(false, &mut fx);
                }
            }
            FeedInput::Closing => state.end(true, &mut fx),
            FeedInput::Census { .. } => {}
            FeedInput::WantPlus { model, id, stream } => state.want_plus(model, id, stream, &mut fx),
            FeedInput::WantMinus { model, id } => state.want_minus(&model, id, &mut fx),
            FeedInput::HandshakeDone { connection_id, key_id } => state.handshake_done(connection_id, key_id, &mut fx),
            FeedInput::FrameOpened { connection_id, counter, frame } => state.frame_opened(connection_id, counter, frame, &mut fx),
            FeedInput::ProtocolError { connection_id, .. } => {
                if state.connection_mut(connection_id).is_some() {
                    state.dropped(connection_id, &mut fx);
                }
            }
            FeedInput::Event { connection_id, channel, event } => state.event(connection_id, &channel, event, &mut fx),
            FeedInput::ChannelConfirmed { connection_id, channel } => state.channel_confirmed(connection_id, channel, &mut fx),
            FeedInput::ChannelEnded { connection_id, channel, failed } => state.channel_ended(connection_id, channel, failed, &mut fx),
            FeedInput::ChannelResubscribe { connection_id, channel } => state.channel_resubscribe(connection_id, channel, &mut fx),
            FeedInput::ChannelLost { connection_id, dropped } => state.channel_lost(connection_id, dropped, &mut fx),
            FeedInput::Dropped { connection_id } | FeedInput::SubscriptionEnded { connection_id } => state.dropped(connection_id, &mut fx),
            FeedInput::Resubscribe { connection_id } => state.resubscribe(connection_id, &mut fx),
            FeedInput::HandshakeDeadline { connection_id } => state.handshake_deadline(connection_id, &mut fx),
            FeedInput::Observed { batch, payloads } => state.observed(batch, payloads),
            FeedInput::Committed(committed) => state.verdict(committed.batch, None, &mut fx),
            FeedInput::Rejected(rejected) => state.verdict(rejected.batch, Some(rejected.reason), &mut fx),
            FeedInput::Tick(_) => {
                if state.mode == Mode::Live {
                    state.flush_pending(&mut fx);
                }
            }
            FeedInput::Online(Online(online)) => state.online_changed(online, &mut fx),
            FeedInput::ModelsReset(reset) => state.models_reset(reset, &mut fx),
        }
        Self::lower(state, fx)
    }

    fn epoch_started(state: &mut FeedState, epoch: Epoch) { *state = FeedState::new(epoch, state.peers, std::mem::take(&mut state.debounce), std::mem::take(&mut state.channels)); }

    fn tick(now: Time) -> FeedInput { FeedInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> FeedInput { FeedInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> FeedInput { FeedInput::Closing }
}
