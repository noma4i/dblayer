//! Owner-level tests of the `feed` machine: the owner `Feed` on `DetScheduler`, every row of the table and every property.
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
use crate::spec::support::{new_log, number_after, snapshot, take, text_after, Discard, Emitted, Landing, Log, NoEffect, Reset, ResetState, Silent, Sink, Source, SourceInput, SourceState, Spy, SpyState, StoreGeneration, StoreStub, StoreStubState};
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

/// The one channel every model reads in the row tests: a model is ready once it is confirmed.
const CHANNEL: &str = "feed";

fn channels() -> BTreeMap<String, Vec<String>> { ["Chat", "Post"].into_iter().map(|model| (model.to_string(), vec![CHANNEL.to_string()])).collect() }

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

fn framed(model: &str, payload: &str, reread: bool) -> FramedEvent { FramedEvent { model: model.into(), payload: payload.into(), reread: if reread { StaleKeys::Set(key()) } else { StaleKeys::none() }, refused: false } }

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
        let addr = add_feed(&mut det, &log, f::FeedState::new(EPOCH, peers(), debounces(ms), channels()), Discard);
        det.start_epoch(EPOCH);
        let mut reference = Ref::new();
        reference.debounce = debounce;
        let mut twin = Self { det, addr, log, reference, debounce_ms: ms, batches: VecDeque::new(), generation: 0, sealed: Vec::new(), snapshots: 0, reread: false, history: Vec::new(), shown: Vec::new() };
        if let Some(on) = online {
            twin.post(FeedInput::Online { online: Online(on), online_seq: u64::from(on) });
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
            Input::HandshakeDone => {
                let opening = self.connection(To::Opening);
                self.post(FeedInput::HandshakeDone { connection_id: opening, key_id: 7 });
                vec![FeedInput::ChannelConfirmed { connection_id: opening, channel: CHANNEL.into() }]
            }
            Input::FrameOpened { to, counter, events } => {
                let connection = self.connection(*to);
                vec![FeedInput::FrameOpened { connection_id: connection, counter: self.counter(connection, *counter), frame: Frame::Events { id: CHANNEL.into(), events: events.iter().map(|(model, payload)| framed(model, payload, reread)).collect() } }]
            }
            Input::Frame { to, counter, kind } => {
                let connection = self.connection(*to);
                let frame = match kind {
                    Kind::Ping => Frame::Ping { ts: 5.into() },
                    Kind::Complete => Frame::Complete { id: CHANNEL.into() },
                    Kind::OtherId => Frame::Complete { id: "2".into() },
                    Kind::Quiet => Frame::Quiet,
                };
                vec![FeedInput::FrameOpened { connection_id: connection, counter: self.counter(connection, *counter), frame }]
            }
            Input::ProtocolError { to } => vec![FeedInput::ProtocolError { connection_id: self.connection(*to), reason: ProtocolReason::Decrypt }],
            Input::Event { to, model, payload } => vec![FeedInput::Event { connection_id: self.connection(*to), channel: CHANNEL.into(), event: framed(model, payload, reread) }],
            Input::ChannelLost { to, dropped } => vec![FeedInput::ChannelLost { connection_id: self.connection(*to), dropped: dropped.as_ref().map(|counts| counts.iter().cloned().collect::<BTreeMap<_, _>>()) }],
            Input::Dropped { to } => vec![FeedInput::Dropped { connection_id: self.connection(*to) }],
            Input::Ended { to } => vec![FeedInput::SubscriptionEnded { connection_id: self.connection(*to) }],
            Input::Resubscribe { to } => vec![FeedInput::Resubscribe { connection_id: self.connection(*to) }],
            Input::HandshakeDeadline { to } => vec![FeedInput::HandshakeDeadline { connection_id: self.connection(*to) }],
            Input::Committed => {
                let batch = self.batch();
                vec![FeedInput::Committed(Committed { batch, revision: 1, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0 })]
            }
            Input::CommittedStale(parents) => {
                let batch = self.batch();
                vec![FeedInput::Committed(Committed { batch, revision: 1, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: parents.clone(), position: 0 })]
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
            Input::StrayBatch => vec![FeedInput::Committed(Committed { batch: 9_999, revision: 1, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0 })],
            Input::Tick => vec![FeedInput::Tick(Time(0))],
            Input::Online(on) => vec![FeedInput::Online { online: Online(*on), online_seq: u64::from(*on) }],
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
                        Outgoing::Unsubscribe { .. } => Effect::SealUnsubscribe,
                        Outgoing::Pong { .. } => Effect::SealPong,
                    });
                }
                Emitted::Run(FeedEffect::Close { .. }) => modelled.push(Effect::Close),
                Emitted::Run(FeedEffect::SnapshotRequest { .. }) => self.snapshots += 1,
                Emitted::Run(FeedEffect::Drained(_)) => modelled.push(Effect::Drained),
                Emitted::Run(FeedEffect::Census { .. }) => {}
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

mod feed_cells_tests;
mod feed_props_tests;
