//! Tests of `specs/machines/feed.md`. Names are the `Test` column.

use super::harness::{chat_entry, sequences, Cause, Reading, Val, World};
use super::ref_feed::{Effect, Feed, Input, Mode, Obs};

const CHAT: &str = "Chat";

fn want() -> Input { Input::WantPlus { model: CHAT.into() } }

fn event(index: usize) -> Input { Input::Event { current: true, model: CHAT.into(), id: format!("c{index}") } }

/// What a reader of `Events` saw, in the vocabulary of the reference.
fn obs_of(stream: &Reading) -> Vec<Obs> {
    stream.vals().into_iter().filter_map(|val| match val {
        Val::Event { position, id } => Some(Obs::Event { model: CHAT.into(), position, id }),
        Val::Lost { first, last } => Some(Obs::Lost { model: CHAT.into(), first, last }),
        Val::Ended(Cause::SessionEnded | Cause::Closed) => Some(Obs::Ended),
        _ => None,
    }).collect()
}

/// Lets the debounce and the backoff pass: advances the clock in steps until `done` holds.
async fn pump(world: &World, stream: &Reading, mut done: impl FnMut(&[Obs]) -> bool) -> Vec<Obs> {
    for _ in 0..400 {
        stream.drain();
        let obs = obs_of(stream);
        if done(&obs) { return obs; }
        world.clock.advance(5_000);
        world.breathe().await;
    }
    stream.drain();
    let obs = obs_of(stream);
    assert!(done(&obs), "the condition never held, read: {obs:?}");
    obs
}

async fn live() -> (World, Reading, Feed) {
    let world = World::open().await;
    let stream = world.events_stream();
    world.net.wait_subscribes(1).await;
    world.breathe().await;
    let mut reference = Feed::new();
    assert_eq!(reference.step(want()), [Effect::Subscribe]);
    reference.step(Input::Ready);
    assert_eq!(reference.mode, Mode::Live);
    (world, stream, reference)
}

/// Waits until the core has made `count` subscriptions, moving the clock for a backoff.
async fn wait_resubscribed(world: &World, count: usize) {
    for _ in 0..400 {
        if world.net.subscribes() >= count {
            world.breathe().await;
            return;
        }
        world.clock.advance(5_000);
        world.breathe().await;
    }
    assert!(world.net.subscribes() >= count, "the core did not subscribe again");
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_off_want() {
    let world = World::open().await;
    let mut reference = Feed::new();
    let effects = reference.step(want());
    let _stream = world.events_stream();
    world.net.wait_subscribes(1).await;
    world.breathe().await;
    assert_eq!(world.net.subscribes(), effects.iter().filter(|effect| **effect == Effect::Subscribe).count());
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_off_want_offline() {
    let world = World::open().await;
    world.db.set_online(false).unwrap();
    let mut reference = Feed::new();
    reference.online = false;
    assert!(reference.step(want()).is_empty());
    let _stream = world.events_stream();
    world.breathe().await;
    assert_eq!(world.net.subscribes(), 0, "offline the feed does not subscribe");
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_off_unwant() {
    let world = World::open().await;
    world.db.set_online(false).unwrap();
    let mut reference = Feed::new();
    reference.online = false;
    reference.step(want());
    assert!(reference.step(Input::WantMinus { model: CHAT.into() }).is_empty());
    let stream = world.events_stream();
    stream.cancel();
    world.db.set_online(true).unwrap();
    world.breathe().await;
    let effects = reference.step(Input::Online(true));
    assert_eq!(world.net.subscribes(), effects.len(), "no reader is left, so coming online subscribes to nothing");
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_off_resume() {
    let world = World::open().await;
    world.db.set_online(false).unwrap();
    let mut reference = Feed::new();
    reference.online = false;
    reference.step(want());
    let _stream = world.events_stream();
    world.breathe().await;
    let effects = reference.step(Input::Online(true));
    assert_eq!(effects, [Effect::Subscribe]);
    world.db.set_online(true).unwrap();
    world.net.wait_subscribes(1).await;
    world.breathe().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_off_stray() {
    let (world, stream, mut reference) = live().await;
    reference.step(Input::Dropped);
    world.net.drop_connection();
    wait_resubscribed(&world, 2).await;
    reference.step(Input::Ready);
    reference.take_obs();
    assert!(reference.step(Input::Event { current: false, model: CHAT.into(), id: "c1".into() }).is_empty());
    world.net.emit_on_first(vec![chat_entry(1, "stale")]).unwrap();
    world.breathe().await;
    world.clock.advance(60_000);
    world.settle().await;
    stream.drain();
    assert!(obs_of(&stream).iter().all(|obs| !matches!(obs, Obs::Event { .. })), "a frame of a connection that is not current is not delivered");
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_opening_ready() {
    let (world, stream, mut reference) = live().await;
    world.settle().await;
    stream.drain();
    assert_eq!(obs_of(&stream), reference.take_obs());
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_opening_ready_after_gap() {
    let (world, stream, mut reference) = live().await;
    reference.step(Input::Dropped);
    let effects = reference.step(Input::Ready);
    assert_eq!(effects, [Effect::StaleWindows(vec![CHAT.into()])]);
    world.net.drop_connection();
    wait_resubscribed(&world, 2).await;
    let wanted = reference.take_obs();
    let got = pump(&world, &stream, |obs| obs.iter().any(|obs| matches!(obs, Obs::Lost { .. }))).await;
    assert_eq!(got, wanted, "a break after Live gives each model one Lost at the next Ready");
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_opening_dropped() {
    let world = World::open().await;
    world.net.fail_subscribes(true);
    let mut reference = Feed::new();
    reference.step(want());
    let effects = reference.step(Input::Dropped);
    assert_eq!(effects, [Effect::ScheduleResubscribe]);
    let _stream = world.events_stream();
    world.net.wait_subscribes(1).await;
    world.breathe().await;
    world.breathe().await;
    assert_eq!(world.net.subscribes(), 1, "the new subscription waits for the backoff");
    world.net.fail_subscribes(false);
    wait_resubscribed(&world, 2).await;
    assert_eq!(reference.step(Input::Ready), []);
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_live_event() {
    let (world, stream, mut reference) = live().await;
    assert_eq!(reference.step(event(1)), [Effect::ScheduleTick]);
    reference.step(Input::Tick);
    reference.step(Input::Committed);
    world.net.emit(vec![chat_entry(1, "a")]).unwrap();
    let got = pump(&world, &stream, |obs| obs.iter().any(|obs| matches!(obs, Obs::Event { .. }))).await;
    assert_eq!(got, reference.take_obs());
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_live_flush() {
    let (world, stream, mut reference) = live().await;
    reference.step(event(1));
    assert_eq!(reference.step(Input::Tick), [Effect::Commit]);
    world.net.emit(vec![chat_entry(1, "a")]).unwrap();
    pump(&world, &stream, |obs| obs.iter().any(|obs| matches!(obs, Obs::Event { .. }))).await;
    assert_eq!(world.chat_ids().await, ["c1"], "the event is delivered after its row was committed");
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_live_committed() {
    let (world, stream, mut reference) = live().await;
    for index in 1..=3 { reference.step(event(index)); }
    reference.step(Input::Tick);
    reference.step(Input::Committed);
    world.net.emit((1..=3).map(|index| chat_entry(index, "a")).collect()).unwrap();
    let got = pump(&world, &stream, |obs| obs.len() >= 3).await;
    assert_eq!(got, reference.take_obs(), "positions of a batch follow one another from 1");
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_live_dropped() {
    let (world, stream, mut reference) = live().await;
    reference.step(event(1));
    let effects = reference.step(Input::Dropped);
    assert_eq!(effects, [Effect::Commit, Effect::Subscribe]);
    reference.step(Input::Committed);
    reference.step(Input::Ready);
    world.net.emit(vec![chat_entry(1, "a")]).unwrap();
    world.breathe().await;
    world.net.drop_connection();
    wait_resubscribed(&world, 2).await;
    let wanted = reference.take_obs();
    let got = pump(&world, &stream, |obs| obs.len() >= wanted.len()).await;
    assert_eq!(got, wanted, "an event that waits for the debounce is committed at the break, then the break is one Lost");
    assert_eq!(world.chat_ids().await, ["c1"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_live_offline() {
    let (world, stream, mut reference) = live().await;
    reference.step(event(1));
    let effects = reference.step(Input::Online(false));
    assert_eq!(effects, [Effect::Commit, Effect::Unsubscribe]);
    world.net.emit(vec![chat_entry(1, "a")]).unwrap();
    world.db.set_online(false).unwrap();
    world.breathe().await;
    assert!(world.net.unsubscribes() >= 1, "going offline closes the subscription");
    pump(&world, &stream, |obs| obs.iter().any(|obs| matches!(obs, Obs::Event { .. }))).await;
    assert_eq!(world.chat_ids().await, ["c1"], "the event that waited was committed");
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_live_last_unwant() {
    let (world, stream, mut reference) = live().await;
    let effects = reference.step(Input::WantMinus { model: CHAT.into() });
    assert_eq!(effects, [Effect::Unsubscribe]);
    stream.cancel();
    world.breathe().await;
    assert_eq!(world.net.unsubscribes(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_closing() {
    let (world, stream, mut reference) = live().await;
    let effects = reference.step(Input::Closing);
    assert_eq!(effects, [Effect::Unsubscribe, Effect::Drained]);
    world.close().await;
    let got = pump(&world, &stream, |obs| obs.contains(&Obs::Ended)).await;
    assert_eq!(got, reference.take_obs());
    assert!(world.net.unsubscribes() >= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_ended() {
    let (world, stream, mut reference) = live().await;
    reference.step(Input::Closing);
    world.close().await;
    pump(&world, &stream, |obs| obs.contains(&Obs::Ended)).await;
    reference.take_obs();
    assert!(reference.step(event(1)).is_empty());
    let _ = world.net.emit(vec![chat_entry(1, "late")]);
    world.breathe().await;
    stream.drain();
    assert_eq!(obs_of(&stream), [Obs::Ended], "after Ended nothing else arrives");
}

/// P-FEED-01 over traces of 5 inputs up to depth 4: events, a break, offline, online. Positions are 1..N without a gap or a repeat.
#[tokio::test(flavor = "multi_thread")]
async fn feed_prop_cover() {
    for trace in sequences(5, 4, usize::MAX).into_iter().step_by(8) {
        let (world, stream, _) = live().await;
        let mut next_event = 0usize;
        let mut online = true;
        for symbol in &trace {
            match symbol {
                0 | 1 if online => {
                    let count = if *symbol == 0 { 1 } else { 3 };
                    let entries = (0..count).map(|_| { next_event += 1; chat_entry(next_event, "a") }).collect();
                    let _ = world.net.emit(entries);
                }
                2 if online => world.net.drop_connection(),
                3 => { online = false; world.db.set_online(false).unwrap(); }
                4 if !online => { online = true; world.db.set_online(true).unwrap(); }
                _ => {}
            }
            world.breathe().await;
        }
        if !online { world.db.set_online(true).unwrap(); }
        world.clock.advance(60_000);
        world.settle().await;
        stream.drain();
        let mut covered = 0u64;
        for val in stream.vals() {
            match val {
                Val::Event { position, .. } => { assert_eq!(position, covered + 1, "trace {trace:?}"); covered = position; }
                Val::Lost { first, last } => { assert_eq!(first, covered + 1, "trace {trace:?}"); covered = last; }
                _ => {}
            }
        }
    }
}

/// P-FEED-02: an event that waits for the debounce is in the base or in a Lost range whatever follows.
#[tokio::test(flavor = "multi_thread")]
async fn feed_prop_pending_never_vanishes() {
    for ending in 0..3usize {
        let (world, stream, _) = live().await;
        world.net.emit(vec![chat_entry(1, "a")]).unwrap();
        match ending {
            0 => world.net.drop_connection(),
            1 => world.db.set_online(false).unwrap(),
            _ => stream.cancel(),
        }
        world.breathe().await;
        world.clock.advance(60_000);
        world.settle().await;
        assert_eq!(world.chat_ids().await, ["c1"], "ending {ending}: the pending event reached the base");
    }
}

/// P-FEED-03: each break after Live gives exactly one Lost at the next Ready.
#[tokio::test(flavor = "multi_thread")]
async fn feed_prop_gap_visible() {
    let (world, stream, _) = live().await;
    for round in 1..=2usize {
        world.net.drop_connection();
        wait_resubscribed(&world, round + 1).await;
        let got = pump(&world, &stream, |obs| obs.iter().filter(|obs| matches!(obs, Obs::Lost { .. })).count() >= round).await;
        assert_eq!(got.iter().filter(|obs| matches!(obs, Obs::Lost { .. })).count(), round);
    }
}

/// P-FEED-05: a frame of a connection that is not current changes neither the base nor the reader.
#[tokio::test(flavor = "multi_thread")]
async fn feed_prop_stale_connection() {
    let (world, stream, _) = live().await;
    world.net.drop_connection();
    wait_resubscribed(&world, 2).await;
    world.net.emit_on_first(vec![chat_entry(7, "stale")]).unwrap();
    world.breathe().await;
    world.clock.advance(60_000);
    world.settle().await;
    stream.drain();
    assert!(world.chat_ids().await.is_empty(), "the stale frame changed the base");
    assert!(obs_of(&stream).iter().all(|obs| !matches!(obs, Obs::Event { .. })));
}

/// P-FEED-06: Live without frames makes no request and no schedule.
#[tokio::test(flavor = "multi_thread")]
async fn feed_prop_idle() {
    let (world, stream, _) = live().await;
    world.settle().await;
    stream.drain();
    let before = (world.net.subscribes(), world.net.feed_requests(), stream.vals().len());
    for _ in 0..20 { world.clock.advance(60_000); world.breathe().await; }
    stream.drain();
    assert_eq!((world.net.subscribes(), world.net.feed_requests(), stream.vals().len()), before);
}
