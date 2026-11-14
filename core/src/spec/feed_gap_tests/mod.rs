//! Owner-level tests of the `feed` machine: the owner `Feed` on `DetScheduler`, every row of the table and every property.
//! A row test feeds an input to the owner through its address and asserts the effects it gave out and the observable (`ModelEvent`, `ModelLost`, `ModelGap`).

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;
use std::sync::PoisonError;

use super::feed_words::{Counter, Effect, Input, Kind, Obs, Reject, Reread, To};
use crate::owners::contract::{BatchAuthor, BatchId, Committed, ConnectionId, EpochEnded, EpochStarted, Online, RejectReason, Rejected};
use crate::owners::read::deliveries::{Deliveries, DeliveryInput};
use crate::owners::read::feed::{self as f, Feed, FeedEffect, FeedInput, FeedPeers, Frame, FramedEvent, Outgoing, Protocol, ProtocolReason};
use crate::owners::read::windows::{StaleCause, StaleKeys, WindowKey, Windows, WindowsInput};
use crate::owners::store::{StoreBatch, StoreOp, StoreWriter};
use crate::spec::support::{new_log, sent_batches, Captured, CapturedStore, snapshot, take, Discard, Emitted, Log, NoEffect, Silent, Sink, Source, SourceInput, SourceState, Spy, SpyState, StoreStub, StoreStubState};
use crate::runtime::{explore, Address, Back, Cause as Ended, DetScheduler, Effects, EffectRunner, Epoch, EpochStamp, Limits, StateOwner, Time};
use crate::spec::support::committed_at;

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

type Spied = Log<Emitted<FeedEffect, FeedInput>>;

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

fn framed(model: &str, payload: &str, reread: &Reread, refused: bool) -> FramedEvent {
    let reread = match reread {
        Reread::None => StaleKeys::none(),
        Reread::All => StaleKeys::all(),
        Reread::Set(names) => StaleKeys::Set(names.iter().map(|name| WindowKey { model: model.into(), name: name.clone() }).collect()),
    };
    FramedEvent { model: model.into(), payload: payload.into(), reread, refused }
}

fn add_feed(det: &mut DetScheduler, log: &Spied, stored: &Log<StoreBatch>, staled: &Log<WindowsInput>, state: f::FeedState, runner: impl EffectRunner<Spy<Feed>>) -> Address<Spy<Feed>> {
    det.add::<CapturedStore>("store", stored.clone(), Discard);
    det.add::<Captured<Windows>>("windows", staled.clone(), Discard);
    det.add::<Sink<Deliveries>>("deliveries", new_log(), Discard);
    det.add::<Spy<Feed>>("feed", SpyState::new(state, log), runner)
}

/// The owner on `DetScheduler` with the inputs and the effects of the row tests.
struct Twin {
    det: DetScheduler,
    addr: Address<Spy<Feed>>,
    log: Spied,
    stored: Log<StoreBatch>,
    /// What the Windows received, typed.
    staled: Log<WindowsInput>,
    debounce_ms: u64,
    batches: VecDeque<BatchId>,
    used: Vec<BatchId>,
    /// `(connection, counter)` of every `Seal` the owner gave out.
    sealed: Vec<(ConnectionId, u64)>,
    /// The `SnapshotRequest` effects the owner gave out.
    snapshots: usize,
    reread: Reread,
    /// Everything the owner gave out, in order, across the steps.
    history: Vec<Emitted<FeedEffect, FeedInput>>,
    /// Everything the streams saw, in order, across the steps.
    shown: Vec<Obs>,
    /// What the streams saw in the last step.
    seen: Vec<Obs>,
}

impl Twin {
    fn new() -> Self { Self::with(Some(true), true) }

    /// Events land at once: `updates.debounce` is not declared.
    fn immediate() -> Self { Self::with(Some(true), false) }

    fn with(online: Option<bool>, debounce: bool) -> Self {
        let mut det = DetScheduler::new(11);
        let log = new_log();
        let stored = new_log();
        let staled = new_log();
        let ms = if debounce { DEBOUNCE_MS } else { 0 };
        let addr = add_feed(&mut det, &log, &stored, &staled, f::FeedState::new(EPOCH, peers(), debounces(ms), channels()), Discard);
        det.start_epoch(EPOCH);
        let mut twin = Self { det, addr, log, stored, staled, debounce_ms: ms, batches: VecDeque::new(), used: Vec::new(), sealed: Vec::new(), snapshots: 0, reread: Reread::None, history: Vec::new(), shown: Vec::new(), seen: Vec::new() };
        if let Some(on) = online {
            twin.post(FeedInput::Online { online: Online(on), online_seq: u64::from(on) });
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
            To::Draining => match self.state().mode() {
                f::Mode::Draining { connection, .. } => connection,
                _ => 8_888,
            },
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

    fn batch(&mut self) -> BatchId { self.batch_at(0) }

    fn batch_at(&mut self, nth: usize) -> BatchId {
        let batch = self.batches.remove(nth).unwrap_or(9_999);
        self.used.push(batch);
        batch
    }

    fn reject_reason(reason: &Reject) -> RejectReason {
        match reason {
            Reject::Storage => RejectReason::Storage("disk".into()),
            Reject::Invalid => RejectReason::Invalid(String::new()),
            Reject::NoSession => RejectReason::NoSession,
            Reject::Closed => RejectReason::Closed,
        }
    }

    fn owner_input(&mut self, input: &Input) -> Vec<FeedInput> {
        let reread = self.reread.clone();
        match input {
            Input::WantPlus { model, id, stream } => vec![FeedInput::WantPlus { model: model.clone(), id: *id, stream: *stream, scoped: Vec::new() }],
            Input::WantMinus { model, id } => vec![FeedInput::WantMinus { model: model.clone(), id: *id }],
            Input::HandshakeDone => {
                let opening = self.connection(To::Opening);
                self.post(FeedInput::HandshakeDone { connection_id: opening, key_id: 7 });
                vec![FeedInput::ChannelConfirmed { connection_id: opening, channel: CHANNEL.into() }]
            }
            Input::FrameOpened { to, counter, events } => {
                let connection = self.connection(*to);
                vec![FeedInput::FrameOpened { connection_id: connection, counter: self.counter(connection, *counter), frame: Frame::Events { id: CHANNEL.into(), events: events.iter().map(|(model, payload)| framed(model, payload, &reread, false)).collect() } }]
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
            Input::Event { to, model, payload } => vec![FeedInput::Event { connection_id: self.connection(*to), channel: CHANNEL.into(), event: framed(model, payload, &reread, false) }],
            Input::Dropped { to } => vec![FeedInput::Dropped { connection_id: self.connection(*to) }],
            Input::Ended { to } => vec![FeedInput::SubscriptionEnded { connection_id: self.connection(*to) }],
            Input::Resubscribe { to } => vec![FeedInput::Resubscribe { connection_id: self.connection(*to) }],
            Input::HandshakeDeadline { to } => vec![FeedInput::HandshakeDeadline { connection_id: self.connection(*to) }],
            Input::Committed => {
                let batch = self.batch();
                vec![FeedInput::Committed(committed_at(batch, 1, &[]))]
            }
            Input::CommittedAt(nth) => {
                let batch = self.batch_at(*nth);
                vec![FeedInput::Committed(committed_at(batch, 1, &[]))]
            }
            Input::RejectedAt(nth, reason) => {
                let batch = self.batch_at(*nth);
                vec![FeedInput::Rejected(Rejected { batch, reason: Self::reject_reason(reason) })]
            }
            Input::Refused { model } => {
                let connection = self.connection(To::Active);
                vec![FeedInput::FrameOpened { connection_id: connection, counter: self.counter(connection, Counter::Expected), frame: Frame::Events { id: CHANNEL.into(), events: vec![framed(model, "refused", &Reread::None, true)] } }]
            }
            Input::StrayRepeat => {
                let batch = self.used.last().copied().unwrap_or(9_999);
                vec![FeedInput::Committed(committed_at(batch, 1, &[]))]
            }
            Input::CommittedStale(parents) => {
                let batch = self.batch();
                vec![FeedInput::Committed(Committed { stale_parents: parents.clone(), ..committed_at(batch, 1, &[]) })]
            }
            Input::Rejected(reason) => {
                let batch = self.batch();
                vec![FeedInput::Rejected(Rejected { batch, reason: Self::reject_reason(reason) })]
            }
            Input::CommittedRefused(places) => {
                let batch = self.batch();
                let refused = places.iter().map(|place| (*place, RejectReason::Invalid(String::new()))).collect();
                vec![FeedInput::Committed(Committed { refused, ..committed_at(batch, 1, &[]) })]
            }
            Input::StrayBatch => vec![FeedInput::Committed(committed_at(9_999, 1, &[]))],
            Input::Tick => vec![FeedInput::Tick],
            Input::Online(on) => vec![FeedInput::Online { online: Online(*on), online_seq: u64::from(*on) }],
            Input::Drain { own } => vec![FeedInput::Drain { connection_id: self.connection(if *own { To::Draining } else { To::Stray }) }],
            Input::EpochEnded | Input::Closing => Vec::new(),
        }
    }

    /// Feeds one input to the owner and returns the effects it gave out, `Stale` per model, `Joined` and `ModelReady` per stream.
    fn step(&mut self, input: Input) -> Vec<Effect> {
        take(&self.log);
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
        self.shown.extend(seen.clone());
        self.seen = seen;
        got
    }

    /// Feeds one input and returns the effects with the batches the Store received in that step.
    fn step_stored(&mut self, input: Input) -> (Vec<Effect>, Vec<StoreBatch>) {
        take(&self.stored);
        let effects = self.step(input);
        (effects, take(&self.stored))
    }

    /// Shows the owner effects as the row tests write them: `Stale` per model, `Joined` and `ModelReady` per stream, the events as what a stream sees.
    fn split(&mut self, emitted: Vec<Emitted<FeedEffect, FeedInput>>) -> (Vec<Effect>, Vec<Obs>) {
        let (mut modelled, mut seen) = (Vec::new(), Vec::new());
        let mut batches = sent_batches(&emitted, &self.stored).into_iter();
        let stale_sends = emitted.iter().filter(|entry| matches!(entry, Emitted::Send { to: WINDOWS, payload, .. } if payload.is::<WindowsInput>(|input| matches!(input, WindowsInput::Stale { .. })))).count();
        let stales = snapshot(&self.staled).into_iter().filter(|input| matches!(input, WindowsInput::Stale { .. })).collect::<Vec<_>>();
        let mut stales = stales[stales.len() - stale_sends..].to_vec().into_iter();
        for entry in emitted {
            match entry {
                Emitted::Run(FeedEffect::Connect { .. }) => modelled.push(Effect::Connect),
                Emitted::Run(FeedEffect::Seal { connection, counter, frame, .. }) => {
                    self.sealed.push((connection, counter));
                    modelled.push(match frame {
                        Outgoing::Subscribe { .. } => Effect::SealSubscribe,
                        Outgoing::Unsubscribe { .. } => Effect::SealUnsubscribe,
                        Outgoing::Pong { .. } => Effect::SealPong,
                        Outgoing::Ping { .. } => continue,
                    });
                }
                Emitted::Run(FeedEffect::Close { .. }) => modelled.push(Effect::Close),
                Emitted::Run(FeedEffect::Cancel { .. }) => modelled.push(Effect::Cancel),
                Emitted::Run(FeedEffect::SnapshotRequest { .. }) => self.snapshots += 1,
                Emitted::Run(FeedEffect::Drained(_)) => modelled.push(Effect::Drained),
                Emitted::Run(FeedEffect::Census { .. }) => {}
                Emitted::Schedule { after, input: FeedInput::Tick, .. } => {
                    assert_eq!(after, std::time::Duration::from_millis(self.debounce_ms), "the debounce is the delay of the Schedule");
                    modelled.push(Effect::ScheduleTick);
                }
                Emitted::Schedule { after, input: input @ FeedInput::HandshakeDeadline { .. }, .. } => {
                    assert_eq!(after, f::HANDSHAKE_DEADLINE, "the deadline of a handshake");
                    assert_eq!(modelled.last(), Some(&Effect::Connect), "a deadline is set by a Connect: {input:?}");
                }
                Emitted::Schedule { after, input: FeedInput::Drain { .. }, .. } => {
                    assert_eq!(after, crate::owners::read::CLOSE_DRAIN, "the drain is CLOSE_DRAIN");
                    modelled.push(Effect::ScheduleDrain);
                }
                Emitted::Schedule { input: FeedInput::Resubscribe { .. }, .. } => modelled.push(Effect::ScheduleResubscribe),
                Emitted::Send { to: STORE, .. } => {
                    let batch = batches.next().expect("a batch for every Commit");
                    assert_eq!(batch.author, BatchAuthor::Feed, "a batch of Feed names its author");
                    self.batches.push_back(batch.id);
                    modelled.push(Effect::Commit);
                }
                Emitted::Send { to: WINDOWS, input, payload } if payload.is::<WindowsInput>(|input| matches!(input, WindowsInput::Stale { .. })) => {
                    let Some(WindowsInput::Stale { model, keys, .. }) = stales.next() else { panic!("a Stale for every send: {input}") };
                    modelled.push(match keys {
                        StaleKeys::AllBut(_) => Effect::Stale { model, all: true },
                        StaleKeys::Set(keys) => Effect::StaleSet { model, keys: keys.into_iter().map(|key| key.name).collect() },
                        StaleKeys::Relations(_) => panic!("the Feed stales by relation: {input}"),
                    });
                }
                Emitted::Send { to: DELIVERIES, payload, .. } => match payload.typed::<DeliveryInput>().expect("a Delivery input") {
                    DeliveryInput::Joined { stream, .. } => modelled.push(Effect::Joined(stream)),
                    DeliveryInput::ModelReady { stream } => modelled.push(Effect::ModelReady(stream)),
                    DeliveryInput::ModelEvent { stream, position, payload } => seen.push(Obs::Event { stream, position, payload }),
                    DeliveryInput::ModelLost { stream, first, last } => seen.push(Obs::Lost { stream, first, last }),
                    DeliveryInput::ModelGap { stream } => seen.push(Obs::Gap { stream }),
                    _ => {}
                },
                Emitted::Cancel(id) if f::is_drain_schedule(id) => modelled.push(Effect::CancelDrain),
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
mod feed_time_tests;
