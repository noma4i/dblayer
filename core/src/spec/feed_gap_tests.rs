//! Owner-level tests of `specs/machines/feed.md`: the owner `Feed` on `DetScheduler`, every row of the table and every property.
//! A row test feeds the same input to the reference (`ref_feed.rs`) and to the owner through its address, then compares the effects,
//! the observable (`ModelEvent`, `ModelLost`, `ModelGap`) and the state. Names are the `Test` column of the spec.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, PoisonError};

use super::ref_feed::{Counter, Effect, Feed as Ref, Input, Kind, Mode, Obs, Reject, To};
use crate::owners::contract::{BatchId, Committed, ConnectionId, EpochEnded, EpochStarted, ModelsReset, Online, RejectReason, Rejected};
use crate::owners::read::deliveries::{Deliveries, DeliveryInput};
use crate::owners::read::feed::{self as f, Feed, FeedEffect, FeedInput, FeedPeers, Frame, FramedEvent, Outgoing, Protocol, ProtocolReason};
use crate::owners::read::windows::{StaleKeys, WindowKey, Windows};
use crate::owners::store::StoreWriter;
use crate::owners::support::{new_log, number_after, snapshot, take, text_after, Discard, Emitted, Landing, Log, NoEffect, Reset, ResetState, Silent, Sink, Source, SourceInput, SourceState, Spy, SpyState, StoreGeneration, StoreStub, StoreStubState};
use crate::runtime::{explore, Address, Back, Cause as Ended, DetScheduler, Effects, EffectRunner, Epoch, EpochStamp, Limits, StateOwner, Time};

const EPOCH: u64 = 1;
const DEBOUNCE_MS: u64 = 50;
/// Slots of the peers and of the owner: the Store, the Windows, the Deliveries, then the Feed.
const STORE: usize = 0;
const WINDOWS: usize = 1;
const DELIVERIES: usize = 2;
const FEED: usize = 3;

/// The stream `Events` of the model, and a window with `live` on `Chat`.
const CHAT: u64 = 1;
const POST: u64 = 2;
const WINDOW: u64 = 11;

type Spied = Log<Emitted<FeedEffect>>;

fn peers() -> FeedPeers { FeedPeers { store: Address::<StoreWriter>::new(STORE), windows: Address::<Windows>::new(WINDOWS), deliveries: Address::<Deliveries>::new(DELIVERIES) } }

fn debounces(ms: u64) -> BTreeMap<String, u64> { ["Chat", "Post"].into_iter().map(|model| (model.to_string(), ms)).collect() }

fn stream_of(model: &str) -> u64 {
    match model {
        "Chat" => CHAT,
        "Post" => POST,
        other => panic!("no stream for {other}"),
    }
}

fn model_of(stream: u64) -> &'static str { if stream == POST { "Post" } else { "Chat" } }

fn names(models: &[&str]) -> Vec<String> { models.iter().map(|model| model.to_string()).collect() }

fn key() -> BTreeSet<WindowKey> { BTreeSet::from([WindowKey { model: "Chat".into(), name: "all".into() }]) }

fn framed(model: &str, payload: &str, reread: bool) -> FramedEvent { FramedEvent { model: model.into(), payload: payload.into(), reread: if reread { StaleKeys::Set(key()) } else { StaleKeys::none() } } }

fn add_feed(det: &mut DetScheduler, log: &Spied, state: f::FeedState, runner: impl EffectRunner<Spy<Feed>>) -> Address<Spy<Feed>> {
    det.add::<Sink<StoreWriter>>("store", new_log(), Discard);
    det.add::<Sink<Windows>>("windows", new_log(), Discard);
    det.add::<Sink<Deliveries>>("deliveries", new_log(), Discard);
    det.add::<Spy<Feed>>("feed", SpyState::new(state, log), runner)
}

fn mode_of(mode: f::Mode) -> Mode {
    match mode {
        f::Mode::Off { gap } => Mode::Off { gap },
        f::Mode::Opening { gap } => Mode::Opening { gap },
        f::Mode::Live => Mode::Live,
        f::Mode::Ended => Mode::Ended,
    }
}

/// The owner and the reference side by side.
struct Twin {
    det: DetScheduler,
    addr: Address<Spy<Feed>>,
    log: Spied,
    reference: Ref,
    debounce_ms: u64,
    batches: VecDeque<BatchId>,
    generation: u64,
    /// `(connection, counter)` of every `Seal` the owner gave out.
    sealed: Vec<(ConnectionId, u64)>,
    /// The `SnapshotRequest` effects the owner gave out.
    snapshots: usize,
    reread: bool,
    /// Everything the owner gave out, in order, across the steps.
    history: Vec<Emitted<FeedEffect>>,
    /// Everything the streams saw, in order, across the steps.
    shown: Vec<Obs>,
}

impl Twin {
    fn new() -> Self { Self::with(Some(true), true) }

    /// Events land at once: `updates.debounce` is not declared.
    fn immediate() -> Self { Self::with(Some(true), false) }

    fn with(online: Option<bool>, debounce: bool) -> Self {
        let mut det = DetScheduler::new(11);
        let log = new_log();
        let ms = if debounce { DEBOUNCE_MS } else { 0 };
        let addr = add_feed(&mut det, &log, f::FeedState::new(EPOCH, peers(), debounces(ms)), Discard);
        det.start_epoch(EPOCH);
        let mut reference = Ref::new();
        reference.debounce = debounce;
        let mut twin = Self { det, addr, log, reference, debounce_ms: ms, batches: VecDeque::new(), generation: 0, sealed: Vec::new(), snapshots: 0, reread: false, history: Vec::new(), shown: Vec::new() };
        if let Some(on) = online {
            twin.post(FeedInput::Online(Online(on)));
            twin.reference.online = Some(on);
        }
        take(&twin.log);
        twin
    }

    fn state(&self) -> &f::FeedState { &self.det.state(self.addr).expect("the owner state").inner }

    fn run(&mut self) { self.det.run_until_quiet(10_000).expect("the owner is quiet"); }

    fn post(&mut self, input: FeedInput) {
        self.det.post(self.addr, EpochStamp::Epoch(EPOCH), input);
        self.run();
    }

    fn connection(&self, to: To) -> ConnectionId {
        match to {
            To::Active => self.state().active_connection().unwrap_or(8_888),
            To::Opening => self.state().opening_connection().unwrap_or(8_888),
            To::Stray => 9_999,
        }
    }

    fn counter(&self, connection: ConnectionId, counter: Counter) -> u64 {
        match (self.state().protocol(connection), counter) {
            (Some(Protocol::Established { recv_counter, .. }), Counter::Expected) => recv_counter,
            (Some(Protocol::Established { recv_counter, .. }), Counter::Replayed) => recv_counter - 1,
            _ => 0,
        }
    }

    fn batch(&mut self) -> BatchId { self.batches.pop_front().unwrap_or(9_999) }

    fn owner_input(&mut self, input: &Input) -> Vec<FeedInput> {
        let reread = self.reread;
        match input {
            Input::WantPlus { model, id, stream } => vec![FeedInput::WantPlus { model: model.clone(), id: *id, stream: *stream }],
            Input::WantMinus { model, id } => vec![FeedInput::WantMinus { model: model.clone(), id: *id }],
            Input::HandshakeDone => vec![FeedInput::HandshakeDone { connection_id: self.connection(To::Opening), key_id: 7 }],
            Input::FrameOpened { to, counter, events } => {
                let connection = self.connection(*to);
                vec![FeedInput::FrameOpened { connection_id: connection, counter: self.counter(connection, *counter), frame: Frame::Events { id: f::SUBSCRIPTION.into(), events: events.iter().map(|(model, payload)| framed(model, payload, reread)).collect() } }]
            }
            Input::Frame { to, counter, kind } => {
                let connection = self.connection(*to);
                let frame = match kind {
                    Kind::Ping => Frame::Ping { ts: 5.into() },
                    Kind::Complete => Frame::Complete { id: f::SUBSCRIPTION.into() },
                    Kind::OtherId => Frame::Complete { id: "2".into() },
                    Kind::Quiet => Frame::Quiet,
                };
                vec![FeedInput::FrameOpened { connection_id: connection, counter: self.counter(connection, *counter), frame }]
            }
            Input::ProtocolError { to } => vec![FeedInput::ProtocolError { connection_id: self.connection(*to), reason: ProtocolReason::Decrypt }],
            Input::Event { to, model, payload } => vec![FeedInput::Event { connection_id: self.connection(*to), event: framed(model, payload, reread) }],
            Input::ChannelLost { to, dropped } => vec![FeedInput::ChannelLost { connection_id: self.connection(*to), dropped: dropped.as_ref().map(|counts| counts.iter().cloned().collect::<BTreeMap<_, _>>()) }],
            Input::Dropped { to } => vec![FeedInput::Dropped { connection_id: self.connection(*to) }],
            Input::Ended { to } => vec![FeedInput::SubscriptionEnded { connection_id: self.connection(*to) }],
            Input::Resubscribe { to } => vec![FeedInput::Resubscribe { connection_id: self.connection(*to) }],
            Input::HandshakeDeadline { to } => vec![FeedInput::HandshakeDeadline { connection_id: self.connection(*to) }],
            Input::Committed => {
                let batch = self.batch();
                vec![FeedInput::Committed(Committed { batch, revision: 1, tables: Vec::new(), models: Default::default() })]
            }
            Input::Rejected(reason) => {
                let batch = self.batch();
                let reason = match reason {
                    Reject::StaleGeneration(models) => RejectReason::StaleGeneration { models: models.clone(), generation: 0 },
                    Reject::Storage => RejectReason::Storage("disk".into()),
                    Reject::NoSession => RejectReason::NoSession,
                };
                vec![FeedInput::Rejected(Rejected { batch, reason })]
            }
            Input::StrayBatch => vec![FeedInput::Committed(Committed { batch: 9_999, revision: 1, tables: Vec::new(), models: Default::default() })],
            Input::Tick => vec![FeedInput::Tick(Time(0))],
            Input::Online(on) => vec![FeedInput::Online(Online(*on))],
            Input::ModelsReset { models, generation } => vec![FeedInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: *generation, models: models.clone() })],
            Input::EpochEnded | Input::Closing => Vec::new(),
        }
    }

    /// Feeds one input of the table to both sides, checks the effects, the observable and the state, and returns the effects of the reference.
    fn step(&mut self, input: Input) -> Vec<Effect> {
        take(&self.log);
        self.reference.reread = self.reread;
        match input {
            Input::EpochEnded => {
                self.det.end_epoch(EPOCH, Ended::SessionReset);
                self.run();
            }
            Input::Closing => {
                self.det.close();
                self.run();
            }
            ref other => {
                for owner_input in self.owner_input(other) {
                    self.post(owner_input);
                }
            }
        }
        let emitted = take(&self.log);
        self.history.extend(emitted.clone());
        let (got, seen) = self.split(emitted);
        let want = self.reference.step(input);
        let observed = self.reference.take_obs();
        self.shown.extend(observed.clone());
        assert_eq!(got, want, "the owner and the reference give the same effects");
        assert_eq!(seen, observed, "the owner and the reference show the same events");
        assert_eq!(mode_of(self.state().mode()), self.reference.mode, "the same mode");
        assert_eq!((self.state().held(), self.state().pending(), self.state().landing()), (self.reference.held.len(), self.reference.pending.len(), self.reference.landing.len()), "the same held, pending and landing");
        for model in ["Chat", "Post"] {
            assert_eq!(self.state().position(model), self.reference.position.get(model).copied().unwrap_or(0), "the same position of {model}");
        }
        want
    }

    /// Shows the owner effects as the reference writes them: `Stale` per model, `Joined` and `ModelReady` per stream, the events as what a stream sees.
    fn split(&mut self, emitted: Vec<Emitted<FeedEffect>>) -> (Vec<Effect>, Vec<Obs>) {
        let (mut modelled, mut seen) = (Vec::new(), Vec::new());
        for entry in emitted {
            match entry {
                Emitted::Run(FeedEffect::Connect { .. }) => modelled.push(Effect::Connect),
                Emitted::Run(FeedEffect::Seal { connection, counter, frame, .. }) => {
                    self.sealed.push((connection, counter));
                    modelled.push(match frame {
                        Outgoing::Subscribe { .. } => Effect::SealSubscribe,
                        Outgoing::Unsubscribe => Effect::SealUnsubscribe,
                        Outgoing::Pong { .. } => Effect::SealPong,
                    });
                }
                Emitted::Run(FeedEffect::Close { .. }) => modelled.push(Effect::Close),
                Emitted::Run(FeedEffect::SnapshotRequest { .. }) => self.snapshots += 1,
                Emitted::Run(FeedEffect::Drained(_)) => modelled.push(Effect::Drained),
                Emitted::Schedule { after, input, .. } if input.starts_with("Tick") => {
                    assert_eq!(after, std::time::Duration::from_millis(self.debounce_ms), "the debounce is the delay of the Schedule");
                    modelled.push(Effect::ScheduleTick);
                }
                Emitted::Schedule { after, input, .. } if input.starts_with("HandshakeDeadline") => {
                    assert_eq!(after, f::HANDSHAKE_DEADLINE, "the deadline of a handshake");
                    assert_eq!(modelled.last(), Some(&Effect::Connect), "a deadline is set by a Connect: {input}");
                }
                Emitted::Schedule { input, .. } if input.starts_with("Resubscribe") => modelled.push(Effect::ScheduleResubscribe),
                Emitted::Send { to: STORE, input } => {
                    assert!(input.contains("author: Feed"), "a batch of Feed names its author: {input}");
                    self.batches.push_back(number_after(&input, "id: ").expect("the batch id"));
                    modelled.push(Effect::Commit);
                }
                Emitted::Send { to: WINDOWS, input } if input.starts_with("Stale {") => modelled.push(Effect::Stale { model: text_after(&input, "model: ").expect("the model"), all: input.contains("keys: All") }),
                Emitted::Send { to: DELIVERIES, input } => {
                    let stream = number_after(&input, "stream: ").expect("the stream");
                    if input.starts_with("Joined") {
                        modelled.push(Effect::Joined(stream));
                    } else if input.starts_with("ModelReady") {
                        modelled.push(Effect::ModelReady(stream));
                    } else if input.starts_with("ModelEvent") {
                        seen.push(Obs::Event { stream, position: number_after(&input, "position: ").expect("the position"), payload: text_after(&input, "payload: ").expect("the payload") });
                    } else if input.starts_with("ModelLost") {
                        seen.push(Obs::Lost { stream, first: number_after(&input, "first: ").expect("first"), last: number_after(&input, "last: ").expect("last") });
                    } else if input.starts_with("ModelGap") {
                        seen.push(Obs::Gap { stream });
                    }
                }
                Emitted::Send { .. } | Emitted::Schedule { .. } | Emitted::Cancel(_) | Emitted::Broadcast(_) => {}
            }
        }
        (modelled, seen)
    }
}

fn plus(model: &str) -> Input { Input::WantPlus { model: model.into(), id: stream_of(model), stream: true } }

fn window(model: &str) -> Input { Input::WantPlus { model: model.into(), id: WINDOW, stream: false } }

fn minus(model: &str) -> Input { Input::WantMinus { model: model.into(), id: stream_of(model) } }

fn event(to: To, model: &str, payload: &str) -> Input { Input::Event { to, model: model.into(), payload: payload.into() } }

fn chat(payload: &str) -> Input { event(To::Active, "Chat", payload) }

fn opening_with(mut twin: Twin) -> Twin {
    twin.step(plus("Chat"));
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
    twin
}

fn opening() -> Twin { opening_with(Twin::new()) }

fn live_with(mut twin: Twin) -> Twin {
    twin = opening_with(twin);
    twin.step(Input::HandshakeDone);
    assert_eq!(twin.state().mode(), f::Mode::Live);
    twin
}

fn live() -> Twin { live_with(Twin::new()) }

fn live_immediate() -> Twin { live_with(Twin::immediate()) }

// Rows of the table.

#[test]
fn feed_created() {
    let mut twin = Twin::with(None, true);
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
    assert_eq!(twin.state().online(), None, "online is unknown until the first Online");
    twin.post(FeedInput::EpochStarted(EpochStarted { epoch: EPOCH, identity: Some(7), generation: 4 }));
    let requests = take(&twin.log);
    assert!(requests.iter().any(|entry| matches!(entry, Emitted::Run(FeedEffect::SnapshotRequest { epoch: EPOCH }))), "the step of EpochStarted asks Network for the snapshot of Feed: {requests:?}");
    twin.post(FeedInput::Online(Online(true)));
    twin.reference.online = Some(true);
    twin.step(plus("Chat"));
    twin.step(Input::HandshakeDone);
    take(&twin.log);
    twin.step(chat("c1"));
    twin.step(Input::Tick);
    let sent = &twin.history;
    assert!(sent.iter().any(|entry| matches!(entry, Emitted::Send { to: STORE, input } if input.contains("basis: {\"Chat\": 4}"))), "the basis of an event is the generation of the epoch: {sent:?}");
}

#[test]
fn feed_off_want() {
    let mut twin = Twin::new();
    assert_eq!(twin.step(plus("Chat")), [Effect::Joined(CHAT), Effect::Connect]);
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_off_want_offline() {
    for online in [Some(false), None] {
        let mut twin = Twin::with(online, true);
        assert_eq!(twin.step(plus("Chat")), [Effect::Joined(CHAT)]);
        assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
        assert_eq!(twin.state().wants(), names(&["Chat"]));
    }
}

#[test]
fn feed_off_unwant() {
    let mut twin = Twin::with(Some(false), true);
    twin.step(plus("Chat"));
    assert!(twin.step(minus("Chat")).is_empty());
    assert!(twin.step(Input::Online(true)).is_empty(), "no reader is left, so coming online subscribes to nothing");
}

#[test]
fn feed_unwant_unknown() {
    for mut twin in [Twin::with(Some(false), true), opening(), live()] {
        twin.step(Input::WantMinus { model: "Chat".into(), id: 99 });
        twin.step(Input::WantMinus { model: "Post".into(), id: POST });
        assert_eq!(twin.state().wants().len(), usize::from(twin.state().mode() != f::Mode::Off { gap: false }));
    }
}

#[test]
fn feed_off_resume() {
    let mut twin = Twin::with(Some(false), true);
    twin.step(plus("Chat"));
    assert_eq!(twin.step(Input::Online(true)), [Effect::Connect]);
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_off_online_no_want() {
    let mut twin = Twin::with(None, true);
    assert!(twin.step(Input::Online(true)).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_off_offline() {
    let mut twin = Twin::with(Some(false), true);
    twin.step(plus("Chat"));
    assert!(twin.step(Input::Online(false)).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_off_stray() {
    let mut twin = Twin::with(Some(false), true);
    twin.step(plus("Chat"));
    for input in [event(To::Stray, "Chat", "x"), event(To::Active, "Chat", "x"), Input::HandshakeDone, Input::ChannelLost { to: To::Active, dropped: None }, Input::Dropped { to: To::Active }, Input::Ended { to: To::Opening }, Input::Tick, Input::Resubscribe { to: To::Opening }, Input::FrameOpened { to: To::Opening, counter: Counter::Expected, events: Vec::new() }] {
        assert!(twin.step(input).is_empty());
    }
}

#[test]
fn feed_off_models_reset() {
    let mut twin = Twin::with(Some(false), true);
    assert!(twin.step(Input::ModelsReset { models: names(&["Chat"]), generation: 1 }).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_landing_after_drop() {
    let mut twin = live_immediate();
    assert_eq!(twin.step(chat("c1")), [Effect::Commit]);
    twin.step(Input::Dropped { to: To::Active });
    assert!(matches!(twin.state().mode(), f::Mode::Opening { gap: true }));
    twin.step(Input::Committed);
    assert_eq!(twin.reference.take_obs(), Vec::new());
    assert_eq!(twin.state().landing(), 0, "a batch that lands after the break is settled");
}

#[test]
fn feed_landing_rejected_after_drop() {
    let mut opening = live_immediate();
    opening.step(chat("c1"));
    opening.step(Input::Dropped { to: To::Active });
    assert!(matches!(opening.state().mode(), f::Mode::Opening { gap: true }));
    opening.step(Input::Rejected(Reject::NoSession));
    assert_eq!(opening.state().mode(), f::Mode::Ended, "a terminal refusal of the Store ends Feed in Opening");
    assert!(opening.state().opening_connection().is_none());
    let mut off = live_immediate();
    off.step(chat("c1"));
    off.step(Input::Online(false));
    off.step(Input::Rejected(Reject::NoSession));
    assert_eq!(off.state().mode(), f::Mode::Ended, "and in Off");
}

#[test]
fn feed_stray_batch() {
    for mut twin in [live(), Twin::with(Some(false), true)] {
        assert!(twin.step(Input::StrayBatch).is_empty());
    }
    let mut opening = opening();
    assert!(opening.step(Input::StrayBatch).is_empty());
}

#[test]
fn feed_opening_ready() {
    let mut twin = opening();
    assert_eq!(twin.step(Input::HandshakeDone), [Effect::SealSubscribe, Effect::ModelReady(CHAT)]);
    assert_eq!(twin.state().mode(), f::Mode::Live);
    let mut held = opening();
    held.step(event(To::Opening, "Chat", "c1"));
    held.step(event(To::Opening, "Chat", "c2"));
    assert_eq!(held.step(Input::HandshakeDone), [Effect::SealSubscribe, Effect::ModelReady(CHAT), Effect::ScheduleTick], "the held frames sit by the debounce of the model");
    assert_eq!(held.state().position("Chat"), 2, "the positions were given when the frames arrived");
}

#[test]
fn feed_opening_ready_after_gap() {
    let mut twin = live();
    twin.step(Input::Dropped { to: To::Active });
    assert_eq!(twin.step(Input::HandshakeDone), [Effect::SealSubscribe, Effect::Stale { model: "Chat".into(), all: true }, Effect::ModelReady(CHAT)]);
    assert_eq!(twin.state().position("Chat"), 0, "a break of unknown length takes no position");
    assert_eq!(twin.reference.take_obs(), Vec::new());
}

#[test]
fn feed_opening_stray() {
    let mut twin = opening();
    for input in [event(To::Stray, "Chat", "x"), Input::ChannelLost { to: To::Stray, dropped: None }, Input::Dropped { to: To::Stray }, Input::Ended { to: To::Stray }, Input::Resubscribe { to: To::Stray }, Input::FrameOpened { to: To::Stray, counter: Counter::Expected, events: Vec::new() }] {
        assert!(twin.step(input).is_empty());
    }
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_opening_event() {
    let mut twin = opening();
    assert!(twin.step(event(To::Opening, "Chat", "c1")).is_empty());
    assert_eq!(twin.state().held(), 1);
    assert_eq!(twin.state().position("Chat"), 1);
    assert!(twin.step(event(To::Opening, "Post", "p1")).is_empty(), "a model with no reader takes no position");
}

#[test]
fn feed_opening_channel_lost() {
    let mut twin = opening();
    twin.step(event(To::Opening, "Chat", "c1"));
    let counts = Some(vec![("Chat".to_string(), 2)]);
    assert_eq!(twin.step(Input::ChannelLost { to: To::Opening, dropped: counts }), [Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.state().position("Chat"), 3, "the lost positions follow the held one");
    assert_eq!(twin.step(Input::ChannelLost { to: To::Opening, dropped: None }), [Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.state().position("Chat"), 3, "an unknown number takes no position");
}

#[test]
fn feed_opening_dropped() {
    let mut twin = opening();
    twin.step(event(To::Opening, "Chat", "c1"));
    let first = twin.state().opening_connection();
    assert_eq!(twin.step(Input::Dropped { to: To::Opening }), [Effect::ScheduleResubscribe, Effect::Stale { model: "Chat".into(), all: true }]);
    assert_ne!(twin.state().opening_connection(), first, "the retry is a new connection");
    assert_eq!(twin.state().held(), 0);
    let mut ended = opening();
    assert_eq!(ended.step(Input::Ended { to: To::Opening }), [Effect::ScheduleResubscribe]);
}

#[test]
fn feed_opening_resubscribe() {
    let mut twin = opening();
    twin.step(Input::Dropped { to: To::Opening });
    assert_eq!(twin.step(Input::Resubscribe { to: To::Opening }), [Effect::Connect]);
    assert_eq!(twin.state().protocol(twin.connection(To::Opening)), Some(Protocol::Handshake));
}

#[test]
fn feed_opening_want() {
    let mut twin = opening();
    twin.step(event(To::Opening, "Chat", "c1"));
    let first = twin.state().opening_connection();
    assert_eq!(twin.step(plus("Post")), [Effect::Joined(POST), Effect::Close, Effect::Connect, Effect::Stale { model: "Chat".into(), all: true }]);
    assert_ne!(twin.state().opening_connection(), first, "a changed set subscribes on a new connection");
}

#[test]
fn feed_opening_want_same() {
    let mut twin = opening();
    assert_eq!(twin.step(Input::WantPlus { model: "Chat".into(), id: 5, stream: true }), [Effect::Joined(5)], "a model that is wanted already joins without a new subscription");
    assert!(twin.step(window("Chat")).is_empty());
}

#[test]
fn feed_opening_unwant() {
    let mut twin = opening();
    twin.step(plus("Post"));
    assert!(twin.step(minus("Post")).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_opening_last_unwant() {
    let mut twin = opening();
    twin.step(event(To::Opening, "Chat", "c1"));
    assert_eq!(twin.step(minus("Chat")), [Effect::Close, Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_opening_offline() {
    let mut twin = opening();
    twin.step(event(To::Opening, "Chat", "c1"));
    assert_eq!(twin.step(Input::Online(false)), [Effect::Close, Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
    assert_eq!(twin.step(Input::Online(true)), [Effect::Connect], "the want is kept: coming online opens the subscription again");
}

#[test]
fn feed_opening_online() {
    let mut twin = opening();
    assert!(twin.step(Input::Online(true)).is_empty());
    assert!(twin.step(Input::Tick).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_opening_models_reset() {
    let mut twin = opening_with(Twin::new());
    twin.step(plus("Post"));
    twin.step(event(To::Opening, "Chat", "c1"));
    twin.step(event(To::Opening, "Post", "p1"));
    twin.generation += 1;
    let generation = twin.generation;
    assert_eq!(twin.step(Input::ModelsReset { models: names(&["Post"]), generation }), [Effect::Stale { model: "Post".into(), all: true }]);
    assert_eq!(twin.state().held(), 1, "the frames of the other model stay");
}

#[test]
fn feed_live_event() {
    let mut twin = live();
    assert_eq!(twin.step(chat("c1")), [Effect::ScheduleTick]);
    assert!(twin.step(chat("c2")).is_empty(), "the tick is scheduled once");
    assert_eq!(twin.state().pending(), 2);
}

#[test]
fn feed_live_event_now() {
    let mut twin = live_immediate();
    assert_eq!(twin.step(chat("c1")), [Effect::Commit]);
    assert_eq!(twin.state().landing(), 1);
}

#[test]
fn feed_live_event_unwatched() {
    let mut twin = live();
    assert!(twin.step(event(To::Active, "Post", "p1")).is_empty(), "an event of a model with no reader is not applied");
    assert_eq!(twin.state().pending(), 0);
}

#[test]
fn feed_live_event_pending_connection() {
    let mut twin = live();
    twin.step(plus("Post"));
    assert!(twin.step(event(To::Opening, "Chat", "c1")).is_empty(), "a frame before the Ready of the new connection is not applied");
    let mut protocol = live();
    protocol.step(plus("Post"));
    protocol.step(Input::FrameOpened { to: To::Opening, counter: Counter::Expected, events: vec![("Chat".into(), "c1".into())] });
    assert_eq!(protocol.state().position("Chat"), 0, "a frame of a connection in Handshake is a protocol error, not an event");
}

#[test]
fn feed_live_stray() {
    let mut twin = live();
    for input in [event(To::Stray, "Chat", "x"), Input::ChannelLost { to: To::Stray, dropped: None }, Input::Dropped { to: To::Stray }, Input::Ended { to: To::Stray }, Input::Resubscribe { to: To::Stray }] {
        assert!(twin.step(input).is_empty());
    }
    assert_eq!(twin.state().mode(), f::Mode::Live);
}

#[test]
fn feed_live_flush() {
    let mut twin = live();
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::Tick), [Effect::Commit]);
}

#[test]
fn feed_live_tick_empty() {
    let mut twin = live();
    assert!(twin.step(Input::Tick).is_empty());
}

#[test]
fn feed_live_committed() {
    let mut twin = live();
    twin.reread = true;
    for payload in ["c1", "c2", "c3"] {
        twin.step(chat(payload));
    }
    twin.step(Input::Tick);
    assert_eq!(twin.step(Input::Committed), [Effect::Stale { model: "Chat".into(), all: false }]);
    assert_eq!(twin.state().position("Chat"), 3, "positions of a batch follow one another from 1");
    assert_eq!(twin.reference.take_obs(), Vec::new());
}

#[test]
fn feed_live_rejected_generation() {
    let mut twin = live();
    twin.step(plus("Post"));
    twin.step(Input::HandshakeDone);
    twin.step(chat("c1"));
    twin.step(event(To::Active, "Post", "p1"));
    twin.step(Input::Tick);
    let effects = twin.step(Input::Rejected(Reject::StaleGeneration(names(&["Chat"]))));
    assert_eq!(effects, [Effect::Stale { model: "Chat".into(), all: true }, Effect::Commit], "the events of the other models land in a new batch");
    twin.step(Input::Committed);
}

#[test]
fn feed_live_rejected_storage() {
    let mut twin = live_immediate();
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::Rejected(Reject::Storage)), [Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.state().position("Chat"), 1, "the lost event holds its position");
}

#[test]
fn feed_live_rejected_session() {
    let mut twin = live_immediate();
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::Rejected(Reject::NoSession)), [Effect::SealUnsubscribe, Effect::Close]);
    assert_eq!(twin.state().mode(), f::Mode::Ended);
}

#[test]
fn feed_live_stray_batch() {
    let mut twin = live();
    assert!(twin.step(Input::StrayBatch).is_empty());
    assert_eq!(twin.state().position("Chat"), 0);
}

#[test]
fn feed_live_channel_lost() {
    let mut twin = live();
    twin.step(chat("c1"));
    let counts = Some(vec![("Chat".to_string(), 5)]);
    assert_eq!(twin.step(Input::ChannelLost { to: To::Active, dropped: counts }), [Effect::Commit, Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.reference.take_obs(), Vec::new());
    twin.step(Input::Committed);
    assert_eq!(twin.state().position("Chat"), 6, "the pending event and the 5 dropped hold 6 positions");
    assert_eq!(twin.step(Input::ChannelLost { to: To::Active, dropped: None }), [Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.state().position("Chat"), 6, "an unknown number takes no position");
}

#[test]
fn feed_live_dropped() {
    let mut twin = live();
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::Dropped { to: To::Active }), [Effect::Commit, Effect::Connect]);
    assert!(matches!(twin.state().mode(), f::Mode::Opening { gap: true }));
    let mut ended = live();
    assert_eq!(ended.step(Input::Ended { to: To::Active }), [Effect::Connect]);
}

#[test]
fn feed_live_opening_dropped() {
    let mut twin = live();
    twin.step(plus("Post"));
    assert_eq!(twin.step(Input::Dropped { to: To::Opening }), [Effect::ScheduleResubscribe]);
    assert_eq!(twin.state().mode(), f::Mode::Live, "the active connection is alive");
}

#[test]
fn feed_live_offline() {
    let mut twin = live();
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::Online(false)), [Effect::Commit, Effect::SealUnsubscribe, Effect::Close]);
    assert!(matches!(twin.state().mode(), f::Mode::Off { gap: true }));
}

#[test]
fn feed_live_online() {
    let mut twin = live();
    assert!(twin.step(Input::Online(true)).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Live);
}

#[test]
fn feed_live_models_reset() {
    let mut twin = live();
    twin.step(chat("c1"));
    twin.generation += 1;
    let generation = twin.generation;
    assert!(twin.step(Input::ModelsReset { models: names(&["Chat"]), generation }).is_empty(), "the Store decides about the pending events");
    twin.step(Input::Tick);
    assert_eq!(twin.state().landing(), 1);
}

#[test]
fn feed_live_last_unwant() {
    let mut twin = live();
    twin.step(chat("c1"));
    assert_eq!(twin.step(minus("Chat")), [Effect::Commit, Effect::SealUnsubscribe, Effect::Close]);
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_live_unwant() {
    let mut twin = live();
    twin.step(plus("Post"));
    twin.step(Input::HandshakeDone);
    assert!(twin.step(minus("Post")).is_empty(), "the subscription narrows at the next Subscribe");
    assert_eq!(twin.state().mode(), f::Mode::Live);
}

#[test]
fn feed_live_want_more() {
    let mut twin = live();
    assert_eq!(twin.step(plus("Post")), [Effect::Joined(POST), Effect::Connect]);
    assert!(twin.state().opening_connection().is_some());
}

#[test]
fn feed_live_want_more_replaces_opening() {
    let mut twin = live();
    twin.step(plus("Post"));
    twin.step(Input::Dropped { to: To::Opening });
    assert_eq!(twin.step(Input::WantPlus { model: "Third".into(), id: 3, stream: false }), [Effect::Close, Effect::Connect], "a new set replaces the connection that waits");
}

#[test]
fn feed_live_resubscribe() {
    let mut twin = live();
    twin.step(plus("Post"));
    twin.step(Input::Dropped { to: To::Opening });
    assert_eq!(twin.step(Input::Resubscribe { to: To::Opening }), [Effect::Connect]);
    assert_eq!(twin.state().mode(), f::Mode::Live);
}

#[test]
fn feed_live_want_same() {
    let mut twin = live();
    assert_eq!(twin.step(plus("Chat")), [Effect::Joined(CHAT), Effect::ModelReady(CHAT)]);
    assert!(twin.step(window("Chat")).is_empty(), "a window joins without an answer");
}

#[test]
fn feed_live_ready_switch() {
    let mut twin = live();
    twin.step(plus("Post"));
    assert_eq!(
        twin.step(Input::HandshakeDone),
        [Effect::SealUnsubscribe, Effect::Close, Effect::SealSubscribe, Effect::Stale { model: "Chat".into(), all: true }, Effect::Stale { model: "Post".into(), all: true }, Effect::ModelReady(CHAT), Effect::ModelReady(POST)]
    );
    assert_eq!(twin.state().opening_connection(), None);
}

#[test]
fn feed_epoch_ended() {
    let mut twin = live();
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::EpochEnded), [Effect::SealUnsubscribe, Effect::Close]);
    assert_eq!(twin.state().pending(), 0, "the pending events of the ended epoch are dropped");
    for mut off in [Twin::new(), Twin::with(Some(false), true), opening()] {
        off.step(Input::EpochEnded);
        assert_eq!(off.state().mode(), f::Mode::Ended);
    }
}

#[test]
fn feed_closing() {
    for mut twin in [live(), Twin::new(), opening()] {
        let effects = twin.step(Input::Closing);
        assert_eq!(effects.last(), Some(&Effect::Drained));
        assert_eq!(twin.state().mode(), f::Mode::Ended);
    }
}

#[test]
fn feed_ended_closing() {
    let mut twin = live();
    twin.step(Input::EpochEnded);
    assert_eq!(twin.step(Input::Closing), [Effect::Drained]);
    assert!(twin.step(Input::Closing).is_empty(), "Drained goes out once");
}

#[test]
fn feed_ended() {
    let mut twin = live();
    twin.step(Input::EpochEnded);
    for input in [plus("Post"), Input::HandshakeDone, Input::Tick, Input::Online(true), Input::Committed, chat("c9"), Input::Dropped { to: To::Active }] {
        assert!(twin.step(input).is_empty());
    }
}

// The connection protocol.

#[test]
fn feed_proto_handshake_done() {
    let mut twin = opening();
    let connection = twin.connection(To::Opening);
    assert_eq!(twin.state().protocol(connection), Some(Protocol::Handshake));
    twin.step(Input::HandshakeDone);
    assert_eq!(twin.state().protocol(connection), Some(Protocol::Established { key_id: 7, send_counter: 1, recv_counter: f::FIRST_RECV_COUNTER }), "the subscribe frame reserved the first send counter");
    assert!(twin.step(Input::HandshakeDone).is_empty(), "the handshake of an established connection is done once");
}

#[test]
fn feed_proto_frame_before_handshake() {
    let mut twin = opening();
    assert_eq!(twin.step(Input::FrameOpened { to: To::Opening, counter: Counter::Expected, events: vec![("Chat".into(), "c1".into())] }), [Effect::Close, Effect::ScheduleResubscribe]);
    assert_eq!(twin.state().position("Chat"), 0, "the frame does not sit");
}

#[test]
fn feed_proto_ping_pong() {
    let mut twin = live();
    let connection = twin.connection(To::Active);
    assert_eq!(twin.step(Input::Frame { to: To::Active, counter: Counter::Expected, kind: Kind::Ping }), [Effect::SealPong]);
    assert!(matches!(twin.state().protocol(connection), Some(Protocol::Established { send_counter: 2, recv_counter, .. }) if recv_counter == f::FIRST_RECV_COUNTER + 1), "the pong reserved its own send counter");
}

#[test]
fn feed_proto_frame_kinds() {
    let mut other = live();
    assert!(other.step(Input::Frame { to: To::Active, counter: Counter::Expected, kind: Kind::OtherId }).is_empty(), "a frame of another subscription id is not for Feed");
    let mut quiet = live();
    assert!(quiet.step(Input::Frame { to: To::Active, counter: Counter::Expected, kind: Kind::Quiet }).is_empty());
    let mut complete = live();
    assert_eq!(complete.step(Input::Frame { to: To::Active, counter: Counter::Expected, kind: Kind::Complete }), [Effect::Connect]);
    let mut replayed = live();
    assert_eq!(replayed.step(Input::Frame { to: To::Active, counter: Counter::Replayed, kind: Kind::Complete }), [Effect::Close, Effect::Connect], "the counter is checked before the meaning of the frame");
}

#[test]
fn feed_live_dropped_closes_opening() {
    let mut twin = live_immediate();
    twin.step(plus("Post"));
    let waiting = twin.state().opening_connection().expect("a connection waits for the new want");
    assert_eq!(twin.step(Input::Dropped { to: To::Active }), [Effect::Close, Effect::Connect]);
    assert_ne!(twin.state().opening_connection(), Some(waiting));
}

#[test]
fn feed_proto_frame_ok() {
    let mut twin = live();
    let connection = twin.connection(To::Active);
    assert_eq!(twin.step(Input::FrameOpened { to: To::Active, counter: Counter::Expected, events: vec![("Chat".into(), "c1".into()), ("Chat".into(), "c2".into())] }), [Effect::ScheduleTick]);
    assert!(matches!(twin.state().protocol(connection), Some(Protocol::Established { recv_counter, .. }) if recv_counter == f::FIRST_RECV_COUNTER + 1));
    assert_eq!(twin.state().pending(), 2);
}

#[test]
fn feed_proto_frame_counter_mismatch() {
    let mut twin = live();
    assert_eq!(twin.step(Input::FrameOpened { to: To::Active, counter: Counter::Replayed, events: vec![("Chat".into(), "c1".into())] }), [Effect::Close, Effect::Connect]);
    assert_eq!(twin.state().pending(), 0, "a replayed frame does not sit");
    assert!(matches!(twin.state().mode(), f::Mode::Opening { gap: true }));
}

#[test]
fn feed_proto_error() {
    let mut twin = live();
    assert_eq!(twin.step(Input::ProtocolError { to: To::Active }), [Effect::Connect]);
    let mut handshake = opening();
    assert_eq!(handshake.step(Input::ProtocolError { to: To::Opening }), [Effect::ScheduleResubscribe]);
    assert!(handshake.step(Input::ProtocolError { to: To::Stray }).is_empty());
}

#[test]
fn feed_proto_handshake_timeout() {
    let mut twin = opening();
    assert_eq!(twin.step(Input::HandshakeDeadline { to: To::Opening }), [Effect::Close, Effect::ScheduleResubscribe]);
    let mut established = live();
    assert!(established.step(Input::HandshakeDeadline { to: To::Active }).is_empty(), "an established connection has no deadline");
    assert!(established.step(Input::HandshakeDeadline { to: To::Stray }).is_empty());
    let mut switching = live();
    switching.step(plus("Post"));
    assert_eq!(switching.step(Input::HandshakeDeadline { to: To::Opening }), [Effect::Close, Effect::ScheduleResubscribe]);
    assert_eq!(switching.state().mode(), f::Mode::Live, "the active connection is alive");
}

#[test]
fn feed_proto_send_reserves() {
    let mut twin = live();
    twin.step(Input::EpochEnded);
    let counters: Vec<u64> = twin.sealed.iter().map(|(_, counter)| *counter).collect();
    assert_eq!(counters, [0, 1], "the subscribe frame and the unsubscribe frame take the next send counters, each one once");
}

#[test]
fn feed_wait_ready() {
    let mut opening = Twin::new();
    assert_eq!(opening.step(plus("Chat")), [Effect::Joined(CHAT), Effect::Connect]);
    assert_eq!(opening.step(Input::HandshakeDone), [Effect::SealSubscribe, Effect::ModelReady(CHAT)], "Ready comes with the handshake of the connection");
    assert_eq!(opening.step(plus("Chat")), [Effect::Joined(CHAT), Effect::ModelReady(CHAT)], "a live subscription answers at once");
}

// Properties.

/// P-FEED-03: a break after `Live` gives each model of `want` exactly one `ModelGap` at the next `Ready` and `Stale` for its windows, no position.
#[test]
fn feed_prop_gap_visible() {
    let mut twin = live();
    twin.step(plus("Post"));
    twin.step(Input::HandshakeDone);
    let before = (twin.state().position("Chat"), twin.state().position("Post"));
    twin.step(Input::Dropped { to: To::Active });
    assert_eq!(twin.step(Input::HandshakeDone), [Effect::SealSubscribe, Effect::Stale { model: "Chat".into(), all: true }, Effect::Stale { model: "Post".into(), all: true }, Effect::ModelReady(CHAT), Effect::ModelReady(POST)]);
    assert_eq!((twin.state().position("Chat"), twin.state().position("Post")), before, "no position for a break of unknown length");
    let after_break: Vec<&Obs> = twin.shown.iter().filter(|obs| !matches!(obs, Obs::Gap { .. })).collect();
    assert!(after_break.is_empty(), "the break shows no Lost: {after_break:?}");
    let gaps = |stream: u64| twin.shown.iter().filter(|obs| **obs == Obs::Gap { stream }).count();
    assert_eq!((gaps(CHAT), gaps(POST)), (2, 2), "each model sees one gap at the switch of connections and one at the break");
}

/// P-FEED-06: no reader or no frames: no request and no tick.
#[test]
fn feed_prop_idle() {
    let mut twin = live();
    take(&twin.log);
    assert_eq!(twin.det.ready().len(), 0);
    twin.det.tick(Time(100));
    twin.run();
    assert!(take(&twin.log).is_empty(), "a live feed without frames asks nothing");
    let mut off = Twin::new();
    assert!(off.step(Input::Tick).is_empty());
}

/// P-FEED-07: no `Want+` means no `Subscribe`, however often the network comes back.
#[test]
fn feed_prop_subscribe_needs_want() {
    let mut twin = Twin::with(Some(false), true);
    for on in [true, false, true, false, true] {
        assert!(twin.step(Input::Online(on)).is_empty());
    }
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

/// P-FEED-08: every `Want+` of a stream gets one `Joined`, before the first `ModelEvent`, and `ModelReady` comes before it.
#[test]
fn feed_prop_joined_first() {
    let mut twin = Twin::immediate();
    twin.step(plus("Chat"));
    twin.step(Input::HandshakeDone);
    twin.step(chat("c1"));
    twin.step(Input::Committed);
    let index = |prefix: &str| twin.history.iter().position(|entry| matches!(entry, Emitted::Send { to: DELIVERIES, input } if input.starts_with(prefix)));
    let joins = twin.history.iter().filter(|entry| matches!(entry, Emitted::Send { to: DELIVERIES, input } if input.starts_with("Joined"))).count();
    assert_eq!(joins, 1, "one Joined for one Want+");
    assert!(index("Joined") < index("ModelReady") && index("ModelReady") < index("ModelEvent") && index("ModelEvent").is_some(), "Joined, then ModelReady, then the first ModelEvent");
}

/// P-FEED-09: frames apply from one connection at any time, an event never sits twice.
#[test]
fn feed_prop_single_connection() {
    let mut twin = live();
    twin.step(plus("Post"));
    twin.step(chat("c1"));
    twin.step(event(To::Opening, "Chat", "c2"));
    twin.step(Input::HandshakeDone);
    twin.step(chat("c3"));
    twin.step(event(To::Stray, "Chat", "c4"));
    assert_eq!(twin.state().position("Chat"), 2, "c1 and c3 sat once, the frames of the other connections did not");
    assert_eq!(twin.state().pending(), 2);
}

/// P-FEED-10: the `Want-` of a foreign or repeated id changes nothing, a model leaves `want` when its set of ids is empty.
#[test]
fn feed_prop_subscriber_ids() {
    let mut twin = live();
    twin.step(Input::WantPlus { model: "Chat".into(), id: 3, stream: false });
    assert_eq!(twin.state().ids("Chat"), [CHAT, 3]);
    twin.step(Input::WantMinus { model: "Chat".into(), id: 99 });
    twin.step(minus("Chat"));
    twin.step(minus("Chat"));
    assert_eq!(twin.state().ids("Chat"), [3]);
    assert_eq!(twin.state().mode(), f::Mode::Live, "the model stays while an id is left");
    twin.step(Input::WantMinus { model: "Chat".into(), id: 3 });
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

/// P-FEED-11: a frame with a repeated or skipped counter does not sit and gives a protocol error, each frame sent has a unique counter reserved before the encryption.
#[test]
fn feed_prop_counters() {
    let mut twin = live();
    twin.step(Input::FrameOpened { to: To::Active, counter: Counter::Replayed, events: vec![("Chat".into(), "c1".into())] });
    twin.step(Input::HandshakeDone);
    twin.step(Input::EpochEnded);
    let mut seen = BTreeSet::new();
    for (connection, counter) in &twin.sealed {
        assert!(seen.insert((*connection, *counter)), "counter {counter} of connection {connection} was used twice");
    }
    assert_eq!(twin.sealed.len(), 3);
    assert_eq!(twin.state().position("Chat"), 0);
}

/// P-FEED-12: the positions of `held` are given once when the frames arrive, a replaced connection loses each of them only as a continuous prefix of the ledger.
#[test]
fn feed_prop_held_positions() {
    let mut twin = opening();
    twin.step(plus("Post"));
    for payload in ["c1", "c2"] {
        twin.step(event(To::Opening, "Chat", payload));
    }
    let before = twin.state().position("Chat");
    twin.step(Input::HandshakeDone);
    assert_eq!(twin.state().position("Chat"), before, "Ready adds no position for the held frames");
    let mut dropped = opening();
    for payload in ["c1", "c2"] {
        dropped.step(event(To::Opening, "Chat", payload));
    }
    dropped.step(Input::Dropped { to: To::Opening });
    assert_eq!(dropped.state().position("Chat"), 2);
    assert_eq!(dropped.state().held(), 0);
}

// Properties over every order of the sources: an executor stub in the scheduler answers a `Connect` with a handshake, the explorer orders what the sources send.

#[derive(Clone, Debug, PartialEq, Eq)]
enum Saw {
    Event { model: &'static str, position: u64 },
    Lost { model: &'static str, first: u64, last: u64 },
    /// A batch of the Feed reached the Store: `rows` events landed or were refused.
    Verdict { rows: usize },
}

struct Answerer;

impl EffectRunner<Spy<Feed>> for Answerer {
    fn run(&mut self, stamp: EpochStamp, effect: FeedEffect, back: &Back<Spy<Feed>>) {
        if let FeedEffect::Connect { connection, .. } = effect {
            back.reply(stamp, FeedInput::HandshakeDone { connection_id: connection, key_id: 1 });
        }
    }
}

/// The Deliveries of the property tests: it keeps the events and the losses in the order they arrive.
struct Readers;

impl StateOwner for Readers {
    const EPOCHED: bool = false;

    type State = Log<Saw>;
    type Input = DeliveryInput;
    type Effect = NoEffect;

    fn step(state: &mut Log<Saw>, input: DeliveryInput) -> Effects<Self> {
        let mut log = state.lock().unwrap_or_else(PoisonError::into_inner);
        match input {
            DeliveryInput::ModelEvent { stream, position, .. } => log.push(Saw::Event { model: model_of(stream), position }),
            DeliveryInput::ModelLost { stream, first, last } => log.push(Saw::Lost { model: model_of(stream), first, last }),
            _ => {}
        }
        Vec::new()
    }

    fn tick(now: Time) -> DeliveryInput { DeliveryInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Ended) -> DeliveryInput { DeliveryInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> DeliveryInput { DeliveryInput::Closing }
}

thread_local! {
    static SEEN: RefCell<Option<Rc<Log<Saw>>>> = const { RefCell::new(None) };
}

fn seen() -> Log<Saw> { (*SEEN.with(|slot| slot.borrow().clone()).expect("a rig was built")).clone() }

/// The active connection of the rig after its setup: the wants of Chat and Post opened the connections 1 and 2, the second one switched in.
const ACTIVE: ConnectionId = 2;

fn event_input(model: &str, payload: &str) -> FeedInput { FeedInput::Event { connection_id: ACTIVE, event: framed(model, payload, false) } }

fn rig(debounce_ms: u64, frames: Vec<FeedInput>, with_reset: bool) -> DetScheduler {
    let mut det = DetScheduler::new(1);
    let log: Log<Saw> = new_log();
    let post_generation = Arc::new(AtomicU64::new(0));
    let feed_slot = Address::<Feed>::new(FEED);
    let noted = log.clone();
    det.add::<StoreStub<Feed>>("store", StoreStubState {
        to: feed_slot,
        generation: post_generation.clone(),
        revision: 0,
        only: Some("Post"),
        committed: FeedInput::Committed,
        rejected: FeedInput::Rejected,
        note: Box::new(move |landing| {
            let (Landing::Landed { rows, .. } | Landing::Rejected { rows, .. }) = landing;
            noted.lock().unwrap_or_else(PoisonError::into_inner).push(Saw::Verdict { rows });
        }),
    }, Silent);
    det.add::<Sink<Windows>>("windows", new_log(), Discard);
    det.add::<Readers>("deliveries", log.clone(), Silent);
    let feed = det.add::<Spy<Feed>>("feed", SpyState::new(f::FeedState::new(EPOCH, peers(), debounces(debounce_ms)), &new_log()), Answerer);
    let feed = Address::<Feed>::new(feed.index());
    det.start_epoch(EPOCH);
    let setup = [FeedInput::Online(Online(true)), FeedInput::WantPlus { model: "Chat".into(), id: CHAT, stream: true }, FeedInput::WantPlus { model: "Post".into(), id: POST, stream: true }];
    for input in setup {
        det.post(feed, EpochStamp::Epoch(EPOCH), input);
        det.run_until_quiet(1_000).expect("the setup is quiet");
    }
    let (first, second): (Vec<_>, Vec<_>) = frames.into_iter().enumerate().partition(|(index, _)| index % 2 == 0);
    let [first, second] = [first, second].map(|part| part.into_iter().map(|(_, frame)| frame).collect::<Vec<_>>());
    let transport = det.add::<Source<Feed>>("transport", SourceState { to: feed, script: first }, Silent);
    let reader = det.add::<Source<Feed>>("reader", SourceState { to: feed, script: second }, Silent);
    let clock = det.add::<Source<Feed>>("clock", SourceState { to: feed, script: vec![FeedInput::Tick(Time(1)), FeedInput::Tick(Time(2))] }, Silent);
    for source in [transport, reader, clock] {
        det.post(source, EpochStamp::Control, SourceInput::Go);
    }
    if with_reset {
        let reset = det.add::<Reset<Feed>>("lifecycle", ResetState { to: feed, input: FeedInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Post".into()] }), generation: 1 }, StoreGeneration(post_generation.clone()));
        det.post(reset, EpochStamp::Control, SourceInput::Go);
    }
    SEEN.with(|slot| *slot.borrow_mut() = Some(Rc::new(log)));
    det
}

const LIMITS: Limits = Limits { depth: 4, max_traces: 30_000 };

/// Positions of the `ModelEvent` and the `ModelLost` of one model, in the order of the effects.
fn positions(log: &Log<Saw>, model: &str) -> Vec<(u64, u64)> {
    snapshot(log).into_iter().filter_map(|saw| match saw {
        Saw::Event { model: found, position } if found == model => Some((position, position)),
        Saw::Lost { model: found, first, last } if found == model => Some((first, last)),
        _ => None,
    }).collect()
}

fn covers(positions: &[(u64, u64)]) -> Result<u64, String> {
    let mut next = 1;
    for (first, last) in positions {
        if *first != next || last < first {
            return Err(format!("{first}..={last} where {next} was due"));
        }
        next = last + 1;
    }
    Ok(next - 1)
}

fn lost_counts(model: &str, count: u64) -> FeedInput { FeedInput::ChannelLost { connection_id: ACTIVE, dropped: Some(BTreeMap::from([(model.to_string(), count)])) } }

/// P-FEED-01: for every model the delivered events and the lost positions are 1..position without gaps and repeats, whatever the order.
#[test]
fn feed_prop_cover() {
    let frames = vec![event_input("Chat", "c1"), event_input("Post", "p1"), event_input("Chat", "c2"), lost_counts("Chat", 2), FeedInput::Dropped { connection_id: ACTIVE }, event_input("Chat", "c3")];
    let report = explore(|| rig(DEBOUNCE_MS, frames.clone(), false), LIMITS, |_| {
        let log = seen();
        for model in ["Chat", "Post"] {
            covers(&positions(&log, model))?;
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
    assert!(report.traces > 10);
}

/// P-FEED-02: an event of `held`, `pending` or `landing` is delivered or lost: once none is left, the ledger covers every position.
#[test]
fn feed_prop_pending_never_vanishes() {
    let frames = vec![event_input("Chat", "c1"), event_input("Chat", "c2"), FeedInput::Dropped { connection_id: ACTIVE }, event_input("Chat", "c3")];
    let report = explore(|| rig(DEBOUNCE_MS, frames.clone(), false), Limits { depth: 40, max_traces: 4_000 }, |det| {
        if !det.ready().is_empty() {
            return Ok(());
        }
        let state = det.state(Address::<Spy<Feed>>::new(FEED)).map(|spy| &spy.inner).ok_or("no state")?;
        if state.landing() != 0 {
            return Err("a batch is left in landing after the store answered".into());
        }
        if state.held() == 0 && state.pending() == 0 {
            let covered = covers(&positions(&seen(), "Chat"))?;
            if covered != state.position("Chat") {
                return Err(format!("{} positions were given but {covered} are covered", state.position("Chat")));
            }
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-FEED-04: a reset of the model `Post` does not lose the events of `Chat`.
#[test]
fn feed_prop_reset_other_model() {
    let frames = vec![event_input("Chat", "c1"), event_input("Post", "p1"), event_input("Chat", "c2")];
    let report = explore(|| rig(DEBOUNCE_MS, frames.clone(), true), LIMITS, |_| {
        if snapshot(&seen()).into_iter().any(|saw| matches!(saw, Saw::Lost { model: "Chat", .. })) {
            return Err("a Chat event was lost".into());
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-FEED-05: a frame of a connection that is not the current one changes nothing and is not delivered.
#[test]
fn feed_prop_stale_connection() {
    let report = explore(
        || {
            let frames = vec![
                FeedInput::Event { connection_id: 77, event: framed("Chat", "x", false) },
                FeedInput::Dropped { connection_id: 77 },
                FeedInput::ChannelLost { connection_id: 77, dropped: Some(BTreeMap::from([("Chat".to_string(), 3)])) },
            ];
            rig(0, frames, false)
        },
        LIMITS,
        |_| {
            if snapshot(&seen()).iter().any(|saw| matches!(saw, Saw::Event { .. } | Saw::Lost { .. } | Saw::Verdict { .. })) {
                return Err("a frame of another connection had an effect".into());
            }
            Ok(())
        },
    );
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}
