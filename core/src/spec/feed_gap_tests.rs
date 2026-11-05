//! Owner-level tests of `specs/machines/feed.md`: the owner `Feed` on `DetScheduler`, every row of the table and every property.
//! A row test feeds the same input to the reference (`ref_feed.rs`) and to the owner through its address, then compares the effects
//! and the observable (`ModelEvent`, `ModelLost`, `Ended`). Names are the `Test` column, `_owner` marks the owner twin of a test that also runs on the API.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, PoisonError};

use super::ref_feed::{Effect, Feed as Ref, Input, Mode, Obs, Reject};
use crate::owners::contract::{BatchId, Committed, ConnectionId, EpochEnded, EpochStarted, Frame, ModelsReset, Online, RejectReason, Rejected, SubscriptionFrame};
use crate::owners::read::deliveries::{Deliveries, DeliveryInput};
use crate::owners::read::feed::{self as f, Feed, FeedEffect, FeedPeers};
use crate::owners::read::windows::Windows;
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

type Spied = Log<Emitted<FeedEffect>>;

fn peers() -> FeedPeers { FeedPeers { store: Address::<StoreWriter>::new(STORE), windows: Address::<Windows>::new(WINDOWS), deliveries: Address::<Deliveries>::new(DELIVERIES) } }

/// One stream per model: the stream names the model of an input for the Deliveries.
fn stream_of(model: &str) -> u64 {
    match model {
        "Chat" => 1,
        "Post" => 2,
        other => panic!("no stream for {other}"),
    }
}

fn model_of(stream: u64) -> &'static str { if stream == 1 { "Chat" } else { "Post" } }

/// Adds the Store, the Windows and the Deliveries as sinks, then the Feed watched by `log`.
fn add_feed(det: &mut DetScheduler, log: &Spied, state: f::FeedState, runner: impl EffectRunner<Spy<Feed>>) -> Address<Spy<Feed>> {
    det.add::<Sink<StoreWriter>>("store", new_log(), Discard);
    det.add::<Sink<Windows>>("windows", new_log(), Discard);
    det.add::<Sink<Deliveries>>("deliveries", new_log(), Discard);
    det.add::<Spy<Feed>>("feed", SpyState::new(state, log), runner)
}

fn runs(emitted: Vec<Emitted<FeedEffect>>) -> Vec<FeedEffect> { emitted.into_iter().filter_map(|entry| if let Emitted::Run(effect) = entry { Some(effect) } else { None }).collect() }

fn inner(det: &DetScheduler, address: Address<Spy<Feed>>) -> &f::FeedState { &det.state(address).expect("the owner state").inner }

fn names(models: &[&str]) -> Vec<String> { models.iter().map(|model| model.to_string()).collect() }

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
    connection: ConnectionId,
    batches: VecDeque<BatchId>,
    generation: u64,
}

impl Twin {
    fn new() -> Self { Self::with(true, true) }

    /// Events land at once: `updates.debounce` is not declared.
    fn immediate() -> Self { Self::with(true, false) }

    fn with(online: bool, debounce: bool) -> Self {
        let mut det = DetScheduler::new(11);
        let log = new_log();
        let addr = add_feed(&mut det, &log, f::FeedState::new(EPOCH, online, peers()), Discard);
        det.start_epoch(EPOCH);
        let mut reference = Ref::new();
        reference.online = online;
        reference.debounce = debounce;
        Self { det, addr, log, reference, debounce_ms: if debounce { DEBOUNCE_MS } else { 0 }, connection: 0, batches: VecDeque::new(), generation: 0 }
    }

    fn state(&self) -> &f::FeedState { inner(&self.det, self.addr) }

    fn run(&mut self) { self.det.run_until_quiet(10_000).expect("the owner is quiet"); }

    fn post(&mut self, input: f::FeedInput) {
        self.det.post(self.addr, EpochStamp::Epoch(EPOCH), input);
        self.run();
    }

    fn frame(&mut self, connection: ConnectionId, frame: Frame) { self.post(f::FeedInput::Frame(SubscriptionFrame { connection_id: connection, frame })); }

    fn batch(&mut self) -> BatchId { self.batches.pop_front().unwrap_or(9_999) }

    /// Feeds one input of the table to both sides, checks the effects and the observable and returns the effects of the reference.
    fn step(&mut self, input: Input) -> Vec<Effect> {
        take(&self.log);
        match &input {
            Input::WantPlus { model } => self.post(f::FeedInput::WantPlus { model: model.clone(), debounce_ms: self.debounce_ms, delivery: None }),
            Input::WantDelivery { model } => self.post(f::FeedInput::WantPlus { model: model.clone(), debounce_ms: self.debounce_ms, delivery: Some(stream_of(model)) }),
            Input::Ended => self.frame(self.connection, Frame::Ended),
            Input::Stray => {
                for frame in [Frame::Ready, Frame::Dropped, Frame::Ended, Frame::ChannelLost { dropped: 1, models: vec!["Chat".into()] }, Frame::Event { model: "Chat".into(), payload: "x".into() }] {
                    self.frame(9_999, frame);
                }
                self.post(f::FeedInput::ResubscribeDue { connection: 9_999 });
            }
            Input::Resubscribe => {
                let connection = self.connection;
                self.post(f::FeedInput::ResubscribeDue { connection });
            }
            Input::WantMinus { model } => self.post(f::FeedInput::WantMinus { model: model.clone(), delivery: None }),
            Input::Ready => self.frame(self.connection, Frame::Ready),
            Input::Event { current, model, id } => {
                let connection = if *current { self.connection } else { 9_999 };
                self.frame(connection, Frame::Event { model: model.clone(), payload: id.clone() });
            }
            Input::ChannelLost { dropped, models } => self.frame(self.connection, Frame::ChannelLost { dropped: *dropped, models: models.clone() }),
            Input::Dropped => self.frame(self.connection, Frame::Dropped),
            Input::Committed => {
                let batch = self.batch();
                self.post(f::FeedInput::Committed(Committed { batch, revision: 1, tables: Vec::new(), models: Vec::new() }));
            }
            Input::Rejected(reason) => {
                let batch = self.batch();
                let reason = match reason {
                    Reject::StaleGeneration(models) => RejectReason::StaleGeneration { models: models.clone() },
                    Reject::Storage => RejectReason::Storage("disk".into()),
                    Reject::NoSession => RejectReason::NoSession,
                };
                self.post(f::FeedInput::Rejected(Rejected { batch, reason }));
            }
            Input::StrayBatch => self.post(f::FeedInput::Committed(Committed { batch: 9_999, revision: 1, tables: Vec::new(), models: Vec::new() })),
            Input::Tick => self.post(f::FeedInput::Tick(Time(0))),
            Input::Online(on) => self.post(f::FeedInput::Online(Online(*on))),
            Input::ModelsReset { models } => {
                self.generation += 1;
                self.post(f::FeedInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: self.generation, models: models.clone() }));
            }
            Input::EpochStarted => self.post(f::FeedInput::EpochStarted(EpochStarted { epoch: EPOCH, identity: Some(7), generation: 0 })),
            Input::EpochEnded => {
                self.det.end_epoch(EPOCH, Ended::SessionReset);
                self.run();
            }
            Input::Closing => {
                self.det.close();
                self.run();
            }
        }
        let (got, seen) = self.split(take(&self.log));
        let want = self.reference.step(input);
        let reference_obs = self.reference.take_obs();
        let streamed = |model: &str| !inner(&self.det, self.addr).streams(model).is_empty();
        let expected: Vec<Effect> = want.iter().filter(|effect| !matches!(effect, Effect::ModelReady(model) if !streamed(model))).cloned().collect();
        let observed: Vec<Obs> = reference_obs.into_iter().filter(|obs| match obs { Obs::Event { model, .. } | Obs::Lost { model, .. } => streamed(model), Obs::Ended => true }).collect();
        assert_eq!(got, expected, "the owner and the reference give the same effects");
        assert_eq!(seen, observed, "the owner and the reference show the same events");
        assert_eq!(mode_of(self.state().mode()), self.reference.mode, "the same mode");
        want
    }

    /// The reference models `Stale` of the windows as one list, `Joined` and `ModelReady` per model, the events as what a reader sees.
    /// A model with no stream of the Deliveries shows no event and no `ModelReady`.
    fn split(&mut self, emitted: Vec<Emitted<FeedEffect>>) -> (Vec<Effect>, Vec<Obs>) {
        let (mut modelled, mut seen) = (Vec::new(), Vec::new());
        for entry in emitted {
            match entry {
                Emitted::Run(FeedEffect::Subscribe { connection, .. }) => {
                    self.connection = connection;
                    modelled.push(Effect::Subscribe);
                }
                Emitted::Run(FeedEffect::Unsubscribe | FeedEffect::CloseConnection { .. }) => modelled.push(Effect::Unsubscribe),
                Emitted::Run(FeedEffect::Drained(_)) => modelled.push(Effect::Drained),
                Emitted::Run(FeedEffect::Ended) => seen.push(Obs::Ended),
                Emitted::Schedule { after, input, .. } if input.starts_with("Tick") => {
                    assert_eq!(after, std::time::Duration::from_millis(self.debounce_ms), "the debounce is the delay of the Schedule");
                    modelled.push(Effect::ScheduleTick);
                }
                Emitted::Schedule { input, .. } if input.starts_with("ResubscribeDue") => {
                    self.connection = number_after(&input, "connection: ").expect("the connection");
                    modelled.push(Effect::ScheduleResubscribe);
                }
                Emitted::Send { to: STORE, input } => {
                    assert!(input.contains("author: Feed"), "a batch of Feed names its author: {input}");
                    self.batches.push_back(number_after(&input, "id: ").expect("the batch id"));
                    modelled.push(Effect::Commit);
                }
                Emitted::Send { to: WINDOWS, input } if input.starts_with("Stale(") => {
                    let model = text_after(&input, "model: ").expect("the model");
                    match modelled.last_mut() {
                        Some(Effect::StaleWindows(models)) => models.push(model),
                        _ => modelled.push(Effect::StaleWindows(vec![model])),
                    }
                }
                Emitted::Send { to: DELIVERIES, input } => {
                    let model = model_of(number_after(&input, "stream: ").expect("the stream")).to_string();
                    if input.starts_with("Joined") {
                        modelled.push(Effect::Joined(model));
                    } else if input.starts_with("ModelReady") {
                        modelled.push(Effect::ModelReady(model));
                    } else if input.starts_with("ModelEvent") {
                        seen.push(Obs::Event { model, position: number_after(&input, "position: ").expect("the position"), id: text_after(&input, "payload: ").expect("the payload") });
                    } else if input.starts_with("ModelLost") {
                        seen.push(Obs::Lost { model, first: number_after(&input, "first: ").expect("first"), last: number_after(&input, "last: ").expect("last") });
                    }
                }
                Emitted::Send { .. } | Emitted::Schedule { .. } | Emitted::Cancel(_) | Emitted::Broadcast(_) => {}
            }
        }
        (modelled, seen)
    }
}

fn want(twin: &mut Twin, model: &str) -> Vec<Effect> { twin.step(Input::WantPlus { model: model.into() }) }

fn event(twin: &mut Twin, model: &str, id: &str) -> Vec<Effect> { twin.step(Input::Event { current: true, model: model.into(), id: id.into() }) }

fn live_with(mut twin: Twin) -> Twin {
    want(&mut twin, "Chat");
    twin.step(Input::Ready);
    assert_eq!(twin.state().mode(), f::Mode::Live);
    twin
}

fn live() -> Twin { live_with(Twin::new()) }

fn live_immediate() -> Twin { live_with(Twin::immediate()) }

#[test]
fn feed_off_want_owner() {
    let mut twin = Twin::new();
    assert_eq!(want(&mut twin, "Chat"), [Effect::Subscribe]);
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_off_want_offline_owner() {
    let mut twin = Twin::with(false, true);
    assert!(want(&mut twin, "Chat").is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
    assert_eq!(twin.state().wants(), names(&["Chat"]));
}

#[test]
fn feed_off_unwant_owner() {
    let mut twin = Twin::with(false, true);
    want(&mut twin, "Chat");
    assert!(twin.step(Input::WantMinus { model: "Chat".into() }).is_empty());
    assert!(twin.step(Input::Online(true)).is_empty(), "no reader is left, so coming online subscribes to nothing");
}

#[test]
fn feed_off_resume_owner() {
    let mut by_network = Twin::with(false, true);
    want(&mut by_network, "Chat");
    assert_eq!(by_network.step(Input::Online(true)), [Effect::Subscribe]);
}

#[test]
fn feed_off_stray_owner() {
    let mut twin = Twin::with(false, true);
    want(&mut twin, "Chat");
    twin.connection = 77;
    for input in [Input::Event { current: false, model: "Chat".into(), id: "c1".into() }, Input::Ready, Input::ChannelLost { dropped: 1, models: names(&["Chat"]) }, Input::Dropped, Input::Tick] {
        assert!(twin.step(input).is_empty());
    }
}

#[test]
fn feed_landing_after_drop() {
    let mut twin = live_immediate();
    assert_eq!(event(&mut twin, "Chat", "c1"), [Effect::Commit]);
    twin.step(Input::Dropped);
    assert!(matches!(twin.state().mode(), f::Mode::Opening { gap: true }));
    assert!(twin.step(Input::Committed).is_empty());
    assert_eq!(twin.reference.take_obs(), Vec::new());
    assert_eq!(twin.state().position("Chat"), 1, "a batch that lands after the break is still delivered");
}

#[test]
fn feed_stray_batch() {
    let mut twin = live();
    assert!(twin.step(Input::StrayBatch).is_empty());
    assert_eq!(twin.state().position("Chat"), 0);
    let mut off = Twin::with(false, true);
    assert!(off.step(Input::StrayBatch).is_empty());
}

#[test]
fn feed_opening_ready_owner() {
    let mut twin = Twin::new();
    want(&mut twin, "Chat");
    assert_eq!(twin.step(Input::Ready), [Effect::ModelReady("Chat".into())]);
    assert_eq!(twin.state().mode(), f::Mode::Live);
}

#[test]
fn feed_opening_ready_after_gap_owner() {
    let mut twin = live();
    twin.step(Input::Dropped);
    assert_eq!(twin.step(Input::Ready), [Effect::StaleWindows(names(&["Chat"])), Effect::ModelReady("Chat".into())]);
    assert_eq!(twin.state().position("Chat"), 1, "the break is one Lost position");
}

#[test]
fn feed_opening_dropped_owner() {
    let mut twin = Twin::new();
    want(&mut twin, "Chat");
    assert_eq!(twin.step(Input::Dropped), [Effect::ScheduleResubscribe]);
    let connection = twin.connection;
    take(&twin.log);
    twin.post(f::FeedInput::ResubscribeDue { connection });
    let again = runs(take(&twin.log));
    assert!(matches!(again.as_slice(), [FeedEffect::Subscribe { connection: found, .. }] if *found == connection), "the timer resubscribes the connection it was set for");
}

#[test]
fn feed_opening_want() {
    let mut twin = Twin::new();
    want(&mut twin, "Chat");
    let first = twin.connection;
    assert_eq!(want(&mut twin, "Post"), [Effect::Unsubscribe, Effect::Subscribe]);
    assert_ne!(twin.connection, first, "a changed set subscribes on a new connection");
    assert!(twin.step(Input::WantMinus { model: "Post".into() }).is_empty(), "the subscription narrows at the next Subscribe");
    assert!(want(&mut twin, "Chat").is_empty(), "the same set does not subscribe again");
}

#[test]
fn feed_live_event_owner() {
    let mut twin = live();
    assert_eq!(event(&mut twin, "Chat", "c1"), [Effect::ScheduleTick]);
    assert!(event(&mut twin, "Chat", "c2").is_empty(), "the tick is scheduled once");
    assert_eq!(twin.state().pending(), 2);
}

#[test]
fn feed_live_event_now_owner() {
    let mut twin = live_immediate();
    assert_eq!(event(&mut twin, "Chat", "c1"), [Effect::Commit]);
    assert_eq!(twin.state().landing(), 1);
}

#[test]
fn feed_live_event_unwatched() {
    let mut twin = Twin::new();
    want(&mut twin, "Post");
    twin.step(Input::Ready);
    assert!(event(&mut twin, "Chat", "c1").is_empty(), "an event of a model with no reader is not applied");
    assert_eq!(twin.state().pending(), 0);
}

#[test]
fn feed_live_flush_owner() {
    let mut twin = live();
    event(&mut twin, "Chat", "c1");
    assert_eq!(twin.step(Input::Tick), [Effect::Commit]);
    assert!(twin.step(Input::Tick).is_empty(), "a tick with nothing pending does nothing");
}

#[test]
fn feed_live_committed_owner() {
    let mut twin = live();
    for id in ["c1", "c2", "c3"] {
        event(&mut twin, "Chat", id);
    }
    twin.step(Input::Tick);
    assert!(twin.step(Input::Committed).is_empty());
    assert_eq!(twin.state().position("Chat"), 3, "positions of a batch follow one another from 1");
}

#[test]
fn feed_live_rejected_generation() {
    let mut twin = live();
    want(&mut twin, "Post");
    twin.step(Input::Ready);
    event(&mut twin, "Chat", "c1");
    event(&mut twin, "Post", "p1");
    twin.step(Input::Tick);
    let effects = twin.step(Input::Rejected(Reject::StaleGeneration(names(&["Chat"]))));
    assert_eq!(effects.first(), Some(&Effect::StaleWindows(names(&["Chat"]))));
    assert_eq!(effects.last(), Some(&Effect::Commit), "the events of the other models land in a new batch");
    twin.step(Input::Committed);
}

#[test]
fn feed_live_rejected_storage() {
    let mut twin = live_immediate();
    event(&mut twin, "Chat", "c1");
    assert_eq!(twin.step(Input::Rejected(Reject::Storage)), [Effect::StaleWindows(names(&["Chat"]))]);
    assert_eq!(twin.state().position("Chat"), 1, "the lost event holds its position");
}

#[test]
fn feed_live_rejected_session() {
    let mut twin = live_immediate();
    event(&mut twin, "Chat", "c1");
    assert_eq!(twin.step(Input::Rejected(Reject::NoSession)), [Effect::Unsubscribe]);
    assert_eq!(twin.state().mode(), f::Mode::Ended);
}

#[test]
fn feed_live_channel_lost() {
    let mut twin = live();
    event(&mut twin, "Chat", "c1");
    let effects = twin.step(Input::ChannelLost { dropped: 5, models: names(&["Chat"]) });
    assert_eq!(effects, [Effect::Commit, Effect::StaleWindows(names(&["Chat"]))]);
    twin.step(Input::Committed);
    assert_eq!(twin.state().position("Chat"), 6, "the pending event and the 5 dropped hold 6 positions");
}

#[test]
fn feed_live_dropped_owner() {
    let mut twin = live();
    event(&mut twin, "Chat", "c1");
    assert_eq!(twin.step(Input::Dropped), [Effect::Commit, Effect::Subscribe]);
    assert_eq!(twin.step(Input::Ready), [Effect::StaleWindows(names(&["Chat"])), Effect::ModelReady("Chat".into())]);
}

#[test]
fn feed_live_offline_owner() {
    let mut twin = live();
    event(&mut twin, "Chat", "c1");
    assert_eq!(twin.step(Input::Online(false)), [Effect::Commit, Effect::Unsubscribe]);
    assert!(matches!(twin.state().mode(), f::Mode::Off { gap: true }));
}

#[test]
fn feed_live_models_reset() {
    let mut twin = live();
    event(&mut twin, "Chat", "c1");
    assert!(twin.step(Input::ModelsReset { models: names(&["Chat"]) }).is_empty(), "the Store decides about the pending events");
    twin.step(Input::Tick);
    assert_eq!(twin.state().landing(), 1);
}

#[test]
fn feed_live_last_unwant_owner() {
    let mut twin = live();
    event(&mut twin, "Chat", "c1");
    assert_eq!(twin.step(Input::WantMinus { model: "Chat".into() }), [Effect::Commit, Effect::Unsubscribe]);
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_live_want_more() {
    let mut twin = live();
    assert_eq!(want(&mut twin, "Post"), [Effect::Subscribe]);
    let new_connection = twin.connection;
    take(&twin.log);
    twin.frame(new_connection, Frame::Ready);
    let closed = runs(take(&twin.log));
    assert!(closed.iter().any(|effect| matches!(effect, FeedEffect::CloseConnection { connection } if *connection != new_connection)), "the old connection closes after the new one is ready");
}

#[test]
fn feed_epoch_ended() {
    let mut twin = live();
    event(&mut twin, "Chat", "c1");
    assert_eq!(twin.step(Input::EpochEnded), [Effect::Unsubscribe]);
    assert_eq!(twin.state().pending(), 0, "the pending events of the ended epoch are dropped");
    for mode in [Twin::new(), Twin::with(false, true)] {
        let mut off = mode;
        assert_eq!(off.step(Input::EpochEnded), [Effect::Unsubscribe]);
    }
}

#[test]
fn feed_closing_owner() {
    for mut twin in [live(), Twin::new()] {
        assert_eq!(twin.step(Input::Closing), [Effect::Unsubscribe, Effect::Drained]);
        assert_eq!(twin.state().mode(), f::Mode::Ended);
    }
}

#[test]
fn feed_ended_owner() {
    let mut twin = live();
    twin.step(Input::EpochEnded);
    for input in [Input::WantPlus { model: "Post".into() }, Input::Ready, Input::Tick, Input::Online(true), Input::Committed, Input::Event { current: true, model: "Chat".into(), id: "c9".into() }] {
        assert!(twin.step(input).is_empty());
    }
    twin.det.close();
    twin.run();
    assert!(runs(take(&twin.log)).iter().any(|effect| matches!(effect, FeedEffect::Drained(_))), "an ended feed still answers Closing with Drained");
}

// Properties. An executor stub in the scheduler subscribes, commits and resubscribes; the explorer orders what the sources send.

#[derive(Clone, Debug, PartialEq, Eq)]
enum Saw {
    Event { model: &'static str, position: u64 },
    Lost { model: &'static str, first: u64, last: u64 },
    /// A batch of the Feed reached the Store: `rows` events landed or were refused.
    Verdict { rows: usize },
}

/// Answers the subscription: every `Subscribe` becomes `Ready` of its connection.
struct Executor;

impl EffectRunner<Spy<Feed>> for Executor {
    fn run(&mut self, stamp: EpochStamp, effect: FeedEffect, back: &Back<Spy<Feed>>) {
        if let FeedEffect::Subscribe { connection, .. } = effect {
            back.reply(stamp, f::FeedInput::Frame(SubscriptionFrame { connection_id: connection, frame: Frame::Ready }));
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

fn event_frame(model: &str, payload: &str) -> f::FeedInput { f::FeedInput::Frame(SubscriptionFrame { connection_id: 1, frame: Frame::Event { model: model.into(), payload: payload.into() } }) }

fn rig(debounce_ms: u64, frames: Vec<f::FeedInput>, with_reset: bool) -> DetScheduler {
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
        committed: f::FeedInput::Committed,
        rejected: f::FeedInput::Rejected,
        note: Box::new(move |landing| {
            let (Landing::Landed { rows, .. } | Landing::Rejected { rows, .. }) = landing;
            noted.lock().unwrap_or_else(PoisonError::into_inner).push(Saw::Verdict { rows });
        }),
    }, Silent);
    det.add::<Sink<Windows>>("windows", new_log(), Discard);
    det.add::<Readers>("deliveries", log.clone(), Silent);
    let feed = det.add::<Spy<Feed>>("feed", SpyState::new(f::FeedState::new(EPOCH, true, peers()), &new_log()), Executor);
    let feed = Address::<Feed>::new(feed.index());
    det.start_epoch(EPOCH);
    let wants = ["Chat", "Post"].map(|model| f::FeedInput::WantPlus { model: model.into(), debounce_ms, delivery: Some(stream_of(model)) });
    let readers = det.add::<Source<Feed>>("readers", SourceState { to: feed, script: wants.to_vec() }, Silent);
    let transport = det.add::<Source<Feed>>("transport", SourceState { to: feed, script: frames }, Silent);
    let clock = det.add::<Source<Feed>>("clock", SourceState { to: feed, script: vec![f::FeedInput::Tick(Time(1)), f::FeedInput::Tick(Time(2))] }, Silent);
    for source in [readers, transport, clock] {
        det.post(source, EpochStamp::Control, SourceInput::Go);
    }
    if with_reset {
        let reset = det.add::<Reset<Feed>>("lifecycle", ResetState { to: feed, input: f::FeedInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Post".into()] }), generation: 1 }, StoreGeneration(post_generation.clone()));
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

/// P-FEED-01: for every model the delivered events and the lost positions are 1..position without gaps and repeats.
#[test]
fn feed_prop_cover_owner() {
    let frames = vec![
        event_frame("Chat", "c1"),
        event_frame("Chat", "c2"),
        f::FeedInput::Frame(SubscriptionFrame { connection_id: 1, frame: Frame::ChannelLost { dropped: 2, models: vec!["Chat".into()] } }),
        f::FeedInput::Frame(SubscriptionFrame { connection_id: 1, frame: Frame::Dropped }),
    ];
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

/// P-FEED-02: an event of `pending` or `landing` is delivered, lost, or still counted; the sum never drops below what was accepted.
#[test]
fn feed_prop_pending_never_vanishes_owner() {
    let frames = vec![
        event_frame("Chat", "c1"),
        event_frame("Chat", "c2"),
        f::FeedInput::Frame(SubscriptionFrame { connection_id: 1, frame: Frame::Dropped }),
        event_frame("Chat", "c3"),
    ];
    let report = explore(|| rig(DEBOUNCE_MS, frames.clone(), false), Limits { depth: 40, max_traces: 4_000 }, |det| {
        if !det.ready().is_empty() {
            return Ok(());
        }
        let log = seen();
        let effects = snapshot(&log);
        let committed: usize = effects.iter().map(|saw| if let Saw::Verdict { rows } = saw { *rows } else { 0 }).sum();
        let covered = covers(&positions(&log, "Chat"))? as usize;
        let state = det.state(Address::<Spy<Feed>>::new(FEED)).map(|spy| &spy.inner).ok_or("no state")?;
        if covered < committed.saturating_sub(0) && state.landing() == 0 {
            return Err(format!("{committed} events were committed but {covered} positions are covered"));
        }
        if state.landing() != 0 {
            return Err("a batch is left in landing after the store answered".into());
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-FEED-03: a break after `Live` gives each model of `want` exactly one `ModelLost` at the next `Ready`, and `Stale` for its windows.
#[test]
fn feed_prop_gap_visible_owner() {
    let mut twin = live_with(Twin::new());
    want(&mut twin, "Post");
    twin.step(Input::Ready);
    let before = (twin.state().position("Chat"), twin.state().position("Post"));
    twin.step(Input::Dropped);
    let effects = twin.step(Input::Ready);
    assert_eq!(effects, [Effect::StaleWindows(names(&["Chat", "Post"])), Effect::ModelReady("Chat".into()), Effect::ModelReady("Post".into())]);
    assert_eq!((twin.state().position("Chat"), twin.state().position("Post")), (before.0 + 1, before.1 + 1), "one position per model");
}

/// P-FEED-04: a reset of the model `Post` does not lose the events of `Chat`.
#[test]
fn feed_prop_reset_other_model() {
    let frames = vec![event_frame("Chat", "c1"), event_frame("Post", "p1"), event_frame("Chat", "c2")];
    let report = explore(|| rig(DEBOUNCE_MS, frames.clone(), true), LIMITS, |_| {
        let lost = snapshot(&seen()).into_iter().any(|saw| matches!(saw, Saw::Lost { model: "Chat", .. }));
        if lost {
            return Err("a Chat event was lost".into());
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-FEED-05: a frame of a connection that is not the current one changes nothing and is not delivered.
#[test]
fn feed_prop_stale_connection_owner() {
    let report = explore(
        || {
            let frames = vec![
                f::FeedInput::Frame(SubscriptionFrame { connection_id: 77, frame: Frame::Event { model: "Chat".into(), payload: "x".into() } }),
                f::FeedInput::Frame(SubscriptionFrame { connection_id: 77, frame: Frame::Dropped }),
                f::FeedInput::Frame(SubscriptionFrame { connection_id: 77, frame: Frame::ChannelLost { dropped: 3, models: vec!["Chat".into()] } }),
            ];
            rig(0, frames, false)
        },
        LIMITS,
        |_| {
            let effects = snapshot(&seen());
            if effects.iter().any(|saw| matches!(saw, Saw::Event { .. } | Saw::Lost { .. } | Saw::Verdict { .. })) {
                return Err("a frame of another connection had an effect".into());
            }
            Ok(())
        },
    );
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-FEED-06: no reader or no frames: no request and no tick.
#[test]
fn feed_prop_idle_owner() {
    let mut twin = live();
    take(&twin.log);
    assert_eq!(twin.det.ready().len(), 0);
    twin.det.tick(Time(100));
    twin.run();
    assert!(take(&twin.log).is_empty(), "a live feed without frames asks nothing");
    let mut off = Twin::new();
    assert!(off.step(Input::Tick).is_empty());
}

// Cells of the second revision of `feed.md`: `Joined`, `ModelReady`, `SubscriptionEnded`, `Off{gap}`, events and losses while `Opening`.

fn want_delivery(twin: &mut Twin, model: &str) -> Vec<Effect> { twin.step(Input::WantDelivery { model: model.into() }) }

#[test]
fn feed_created_owner() {
    let mut twin = Twin::immediate();
    assert!(twin.step(Input::EpochStarted).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
    twin.post(f::FeedInput::EpochStarted(EpochStarted { epoch: EPOCH, identity: Some(7), generation: 4 }));
    want(&mut twin, "Chat");
    twin.step(Input::Ready);
    take(&twin.log);
    twin.frame(twin.connection, Frame::Event { model: "Chat".into(), payload: "c1".into() });
    let effects = take(&twin.log);
    assert!(effects.iter().any(|entry| matches!(entry, Emitted::Send { to: STORE, input } if input.contains("basis: {\"Chat\": 4}"))), "the basis of an event is the generation of the epoch: {effects:?}");
}

#[test]
fn feed_off_models_reset_owner() {
    let mut twin = Twin::with(false, true);
    assert!(twin.step(Input::ModelsReset { models: names(&["Chat"]) }).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_off_offline_owner() {
    let mut twin = Twin::with(false, true);
    want(&mut twin, "Chat");
    assert!(twin.step(Input::Online(false)).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_off_online_no_want_owner() {
    let mut twin = Twin::with(false, true);
    assert!(twin.step(Input::Online(true)).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_opening_channel_lost_owner() {
    let mut twin = Twin::new();
    want(&mut twin, "Chat");
    assert_eq!(twin.step(Input::ChannelLost { dropped: 2, models: names(&["Chat"]) }), [Effect::StaleWindows(names(&["Chat"]))]);
    assert_eq!(twin.state().position("Chat"), 2);
}

#[test]
fn feed_opening_event_owner() {
    let mut twin = Twin::new();
    want(&mut twin, "Chat");
    assert_eq!(event(&mut twin, "Chat", "c1"), [Effect::ScheduleTick]);
    twin.step(Input::Ready);
    assert_eq!(twin.step(Input::Tick), [Effect::Commit]);
}

#[test]
fn feed_opening_last_unwant_owner() {
    let mut twin = Twin::new();
    want(&mut twin, "Chat");
    assert_eq!(twin.step(Input::WantMinus { model: "Chat".into() }), [Effect::Unsubscribe]);
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_opening_models_reset_owner() {
    let mut twin = Twin::new();
    want(&mut twin, "Chat");
    assert!(twin.step(Input::ModelsReset { models: names(&["Chat"]) }).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_opening_offline_owner() {
    let mut twin = Twin::new();
    want(&mut twin, "Chat");
    assert_eq!(twin.step(Input::Online(false)), [Effect::Unsubscribe]);
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
    assert_eq!(twin.step(Input::Online(true)), [Effect::Subscribe], "the want is kept: coming online opens the subscription again");
}

#[test]
fn feed_opening_online_owner() {
    let mut twin = Twin::new();
    want(&mut twin, "Chat");
    assert!(twin.step(Input::Online(true)).is_empty());
    assert!(twin.step(Input::Tick).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_opening_resubscribe_owner() {
    let mut twin = Twin::new();
    want(&mut twin, "Chat");
    assert_eq!(twin.step(Input::Ended), [Effect::ScheduleResubscribe], "SubscriptionEnded takes the cells of Dropped");
    assert_eq!(twin.step(Input::Resubscribe), [Effect::Subscribe]);
}

#[test]
fn feed_opening_stray_owner() {
    let mut twin = Twin::new();
    want(&mut twin, "Chat");
    assert!(twin.step(Input::Stray).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_opening_unwant_owner() {
    let mut twin = Twin::new();
    want(&mut twin, "Chat");
    want(&mut twin, "Post");
    assert!(twin.step(Input::WantMinus { model: "Post".into() }).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_opening_want_same_owner() {
    let mut twin = Twin::new();
    want(&mut twin, "Chat");
    assert_eq!(want_delivery(&mut twin, "Chat"), [Effect::Joined("Chat".into())], "a model that is wanted already joins without a new subscription");
    assert!(twin.step(Input::WantMinus { model: "Chat".into() }).is_empty(), "the counter keeps the model");
    assert_eq!(twin.state().wants(), names(&["Chat"]));
}

#[test]
fn feed_live_online_owner() {
    let mut twin = live();
    assert!(twin.step(Input::Online(true)).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Live);
}

#[test]
fn feed_live_stray_owner() {
    let mut twin = live();
    assert!(twin.step(Input::Stray).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Live);
    assert_eq!(twin.state().pending(), 0);
}

#[test]
fn feed_live_stray_batch_owner() {
    let mut twin = live();
    assert!(twin.step(Input::StrayBatch).is_empty());
    assert_eq!(twin.state().position("Chat"), 0);
}

#[test]
fn feed_live_tick_empty_owner() {
    let mut twin = live();
    assert!(twin.step(Input::Tick).is_empty());
}

#[test]
fn feed_live_unwant_owner() {
    let mut twin = live();
    want(&mut twin, "Post");
    twin.step(Input::Ready);
    assert!(twin.step(Input::WantMinus { model: "Post".into() }).is_empty(), "the subscription narrows at the next Subscribe");
    assert_eq!(twin.state().mode(), f::Mode::Live);
}

#[test]
fn feed_live_want_same_owner() {
    let mut twin = live();
    assert_eq!(want_delivery(&mut twin, "Chat"), [Effect::Joined("Chat".into()), Effect::ModelReady("Chat".into())]);
}

#[test]
fn feed_live_ready_switch_owner() {
    let mut twin = live();
    want(&mut twin, "Post");
    assert_eq!(twin.step(Input::Ready), [Effect::Unsubscribe, Effect::ModelReady("Chat".into()), Effect::ModelReady("Post".into())]);
}

#[test]
fn feed_ended_closing_owner() {
    let mut twin = live();
    twin.step(Input::EpochEnded);
    assert_eq!(twin.step(Input::Closing), [Effect::Drained]);
}

#[test]
fn feed_wait_ready_owner() {
    let mut opening = Twin::new();
    assert_eq!(want_delivery(&mut opening, "Chat"), [Effect::Joined("Chat".into()), Effect::Subscribe]);
    assert_eq!(opening.step(Input::Ready), [Effect::ModelReady("Chat".into())], "Ready comes with the subscription of the server");
    let mut live = live();
    assert_eq!(want_delivery(&mut live, "Chat"), [Effect::Joined("Chat".into()), Effect::ModelReady("Chat".into())], "a live subscription answers at once");
}

/// P-FEED-07: no `Want+` means no `Subscribe`, however often the network comes back.
#[test]
fn feed_prop_subscribe_needs_want_owner() {
    let mut twin = Twin::with(false, true);
    for on in [true, false, true, false, true] {
        assert!(twin.step(Input::Online(on)).is_empty());
    }
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

/// P-FEED-08: every `Want+` of a stream gets one `Joined`, and it comes before the first event of the model.
#[test]
fn feed_prop_joined_first_owner() {
    let mut twin = Twin::immediate();
    let first = want_delivery(&mut twin, "Chat");
    assert_eq!(first.iter().filter(|effect| matches!(effect, Effect::Joined(_))).count(), 1);
    twin.step(Input::Ready);
    event(&mut twin, "Chat", "c1");
    twin.step(Input::Committed);
    assert_eq!(twin.state().position("Chat"), 1, "the event came after the join");
}
