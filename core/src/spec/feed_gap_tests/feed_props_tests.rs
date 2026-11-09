use super::*;


/// P-FEED-03: a break after `Live` gives each model of `want` exactly one `ModelGap` at the next `Ready` and `Stale` for its windows, no position.
#[test]
fn feed_prop_gap_visible() {
    let mut twin = live();
    twin.step(plus("Post"));
    let before = (twin.state().position("Chat"), twin.state().position("Post"));
    twin.step(Input::Dropped { to: To::Active });
    assert_eq!(twin.step(Input::HandshakeDone), [Effect::SealSubscribe, Effect::Stale { model: "Chat".into(), all: true }, Effect::Stale { model: "Post".into(), all: true }, Effect::ModelReady(CHAT), Effect::ModelReady(POST)]);
    assert_eq!((twin.state().position("Chat"), twin.state().position("Post")), before, "no position for a break of unknown length");
    let after_break: Vec<&Obs> = twin.shown.iter().filter(|obs| !matches!(obs, Obs::Gap { .. })).collect();
    assert!(after_break.is_empty(), "the break shows no Lost: {after_break:?}");
    let gaps = |stream: u64| twin.shown.iter().filter(|obs| **obs == Obs::Gap { stream }).count();
    assert_eq!((gaps(CHAT), gaps(POST)), (1, 1), "each model sees one gap at the break");
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
    twin.step(Input::Drain { own: true });
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

/// P-FEED-11: a frame with a repeated or skipped counter does not sit and gives a protocol error, each frame sent has a unique counter reserved before the encryption.
#[test]
fn feed_prop_counters() {
    let mut twin = live();
    twin.step(Input::FrameOpened { to: To::Active, counter: Counter::Replayed, events: vec![("Chat".into(), "c1".into())] });
    twin.step(Input::Drain { own: true });
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
pub(super) enum Saw {
    Event { model: &'static str, position: u64 },
    Lost { model: &'static str, first: u64, last: u64 },
    /// A batch of the Feed reached the Store: `rows` events landed or were refused.
    Verdict { rows: usize },
}

pub(super) struct Answerer;

impl EffectRunner<Spy<Feed>> for Answerer {
    fn run(&mut self, stamp: EpochStamp, effect: FeedEffect, back: &Back<Spy<Feed>>) {
        if let FeedEffect::Connect { connection, .. } = effect {
            back.reply(stamp, FeedInput::HandshakeDone { connection_id: connection, key_id: 1, now: crate::runtime::Time(0) });
        }
    }
}

/// The Deliveries of the property tests: it keeps the events and the losses in the order they arrive.
pub(super) struct Readers;

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

pub(super) fn seen() -> Log<Saw> { (*SEEN.with(|slot| slot.borrow().clone()).expect("a rig was built")).clone() }

/// The active connection of the rig after its setup: the wants of Chat and Post opened the connections 1 and 2, the second one switched in.
pub(super) const ACTIVE: ConnectionId = 2;

pub(super) fn event_input(model: &str, payload: &str) -> FeedInput { FeedInput::Event { connection_id: ACTIVE, channel: CHANNEL.into(), event: framed(model, payload, &Reread::None, false) } }

pub(super) fn rig(debounce_ms: u64, frames: Vec<FeedInput>, with_reset: bool) -> DetScheduler {
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
    let feed = det.add::<Spy<Feed>>("feed", SpyState::new(f::FeedState::new(EPOCH, peers(), debounces(debounce_ms), channels()), &new_log()), Answerer);
    let feed = Address::<Feed>::new(feed.index());
    det.start_epoch(EPOCH);
    let setup = [FeedInput::Online { online: Online(true), online_seq: 1, now: crate::runtime::Time(0) }, FeedInput::WantPlus { model: "Chat".into(), id: CHAT, stream: true, scoped: Vec::new(), now: crate::runtime::Time(0) }, FeedInput::WantPlus { model: "Post".into(), id: POST, stream: true, scoped: Vec::new(), now: crate::runtime::Time(0) }];
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

pub(super) const LIMITS: Limits = Limits { depth: 4, max_traces: 30_000 };

/// Positions of the `ModelEvent` and the `ModelLost` of one model, in the order of the effects.
pub(super) fn positions(log: &Log<Saw>, model: &str) -> Vec<(u64, u64)> {
    snapshot(log).into_iter().filter_map(|saw| match saw {
        Saw::Event { model: found, position } if found == model => Some((position, position)),
        Saw::Lost { model: found, first, last } if found == model => Some((first, last)),
        _ => None,
    }).collect()
}

pub(super) fn covers(positions: &[(u64, u64)]) -> Result<u64, String> {
    let mut next = 1;
    for (first, last) in positions {
        if *first != next || last < first {
            return Err(format!("{first}..={last} where {next} was due"));
        }
        next = last + 1;
    }
    Ok(next - 1)
}

pub(super) fn lost_counts(model: &str, count: u64) -> FeedInput { FeedInput::ChannelLost { connection_id: ACTIVE, dropped: Some(BTreeMap::from([(model.to_string(), count)])) } }

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
        if state.held() != 0 || state.pending() != 0 {
            return Err(format!("{} held and {} pending events are left at rest", state.held(), state.pending()));
        }
        let covered = covers(&positions(&seen(), "Chat"))?;
        if covered != state.position("Chat") {
            return Err(format!("{} positions were given but {covered} are covered", state.position("Chat")));
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
                FeedInput::Event { connection_id: 77, channel: CHANNEL.into(), event: framed("Chat", "x", &Reread::None, false) },
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

/// P-FEED-26: after any close of an established connection the four ends of a drain come in any order: one timer, one `cancel`, one `Drained` only for `Closing`.
#[test]
fn feed_drain_vs_cancel_orders() {
    let ends = [Input::Dropped { to: To::Draining }, Input::Ended { to: To::Draining }, Input::ChannelLost { to: To::Draining, dropped: None }, Input::Drain { own: true }];
    let closes = [Input::Closing, Input::Online(false), minus("Chat"), Input::FrameOpened { to: To::Active, counter: Counter::Replayed, events: Vec::new() }];
    for (close, order) in closes.iter().flat_map(|close| crate::test_wait::permutations(&ends).into_iter().map(move |order| (close, order))) {
        let mut twin = live();
        twin.step(close.clone());
        for end in &order {
            twin.step(end.clone());
        }
        let cancels = twin.history.iter().filter(|entry| matches!(entry, Emitted::Run(FeedEffect::Cancel { .. }))).count();
        let drained = twin.history.iter().filter(|entry| matches!(entry, Emitted::Run(FeedEffect::Drained(_)))).count();
        let timer_cancels = twin.history.iter().filter(|entry| matches!(entry, Emitted::Cancel(id) if f::is_drain_schedule(*id))).count();
        let schedules = twin.history.iter().filter(|entry| matches!(entry, Emitted::Schedule { input, .. } if input.starts_with("Drain"))).count();
        let first_is_tick = order[0] == Input::Drain { own: true };
        assert_eq!((cancels, drained, schedules, timer_cancels), (1, usize::from(*close == Input::Closing), 1, usize::from(!first_is_tick)), "close {close:?} order {order:?}");
    }
}
