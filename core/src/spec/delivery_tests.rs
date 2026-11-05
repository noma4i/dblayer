//! Tests of `specs/machines/delivery.md`. Names are the `Test` column.

use super::harness::{held_ids, states_of, canon, chat_entry, loaded, revision_of, sequences, Cause, Phase, Reply, Val, WState, World};
use super::ref_delivery::{Delivery, Effect, Input, Kind, Pulled};
use crate::ffi::StreamRequest;

/// The reader reads each value at once, so the reference is drained after every input.
fn read(reference: &mut Delivery, input: Input, into: &mut Vec<Val>) -> Vec<Effect> {
    let effects = reference.step(input);
    into.extend(reference.drain());
    effects
}

fn loading() -> WState { WState { phase: Phase::Loading, error: false, loaded: true } }

fn ids(rows: &[&str]) -> Vec<String> { rows.iter().map(|id| id.to_string()).collect() }

async fn refresh(world: &World) -> Result<(), crate::ffi::DbError> {
    world.db.refresh(world.epoch(), "Moment".into(), "feed".into(), "{}".into()).await
}

/// A world whose feed holds `r1` and is fresh, with a rows stream open and read to its first screen.
async fn live_rows() -> (World, super::harness::Reading, Delivery) {
    let world = World::open().await;
    world.load_ok().await;
    let stream = world.rows_stream();
    let first = stream.first_screen().await;
    let mut reference = Delivery::new();
    reference.step(Input::Open { kind: Kind::Rows, epoch_current: true });
    reference.step(Input::WindowState(loaded()));
    reference.step(Input::ReadDone { revision: 1, rows: ids(&["r1"]) });
    assert_eq!(canon(first), canon(reference.drain()), "the first value of a rows stream");
    (world, stream, reference)
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_open_rows() {
    let world = World::open().await;
    world.load_ok().await;
    let mut reference = Delivery::new();
    let effects = reference.step(Input::Open { kind: Kind::Rows, epoch_current: true });
    assert!(effects.contains(&Effect::InterestPlus));
    let stream = world.rows_stream();
    reference.step(Input::WindowState(loaded()));
    reference.step(Input::ReadDone { revision: 1, rows: ids(&["r1"]) });
    let core = stream.first_screen().await;
    assert_eq!(canon(core.clone()), canon(reference.drain()));
    assert!(matches!(core.first(), Some(Val::Snapshot { .. })), "the first value of a rows stream is a snapshot");
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_open_events() {
    let world = World::open().await;
    let mut reference = Delivery::new();
    let effects = reference.step(Input::Open { kind: Kind::Events, epoch_current: true });
    assert_eq!(effects, [Effect::WantPlus]);
    let stream = world.events_stream();
    world.net.wait_subscribes(1).await;
    world.breathe().await;
    world.settle().await;
    stream.drain();
    assert_eq!(stream.vals(), reference.drain(), "nothing is delivered before the first event");
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_open_stale() {
    let world = World::open().await;
    let mut reference = Delivery::new();
    reference.step(Input::Open { kind: Kind::Rows, epoch_current: false });
    let stream = world.open_stream(world.epoch() + 7, StreamRequest::Query { query_json: super::harness::FEED.into() });
    let core = stream.until(|vals| vals.iter().any(|val| matches!(val, Val::Ended(_)))).await;
    assert_eq!(core, reference.drain());
    assert_eq!(core, [Val::Ended(Cause::SessionEnded)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_opening_read_done() {
    let world = World::open().await;
    world.load_ok().await;
    let mut reference = Delivery::new();
    reference.step(Input::Open { kind: Kind::Rows, epoch_current: true });
    reference.step(Input::WindowState(loaded()));
    reference.step(Input::ReadDone { revision: 1, rows: ids(&["r1"]) });
    let stream = world.rows_stream();
    let core = stream.first_screen().await;
    assert_eq!(canon(core), canon(reference.drain()), "Snapshot with the rows and the window state of the same read");
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_cancel() {
    let (world, stream, mut reference) = live_rows().await;
    let effects = reference.step(Input::Cancel);
    assert_eq!(effects, [Effect::InterestMinus]);
    stream.cancel();
    world.settle().await;
    stream.drain();
    let core: Vec<Val> = stream.vals().into_iter().skip(1).collect();
    assert_eq!(core, reference.drain(), "a cancelled stream delivers Ended(Cancelled)");
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_closing() {
    let (world, stream, mut reference) = live_rows().await;
    let effects = reference.step(Input::Closing);
    assert_eq!(effects, [Effect::Drained]);
    world.close().await;
    let core = stream.until(|vals| vals.iter().any(|val| matches!(val, Val::Ended(_)))).await;
    assert_eq!(core.into_iter().skip(1).collect::<Vec<_>>(), reference.drain());
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_committed() {
    let (world, stream, mut reference) = live_rows().await;
    let mut wanted = Vec::new();
    world.net.script(Reply::page(&["r1", "r2"], false));
    read(&mut reference, Input::WindowState(loading()), &mut wanted);
    let effects = read(&mut reference, Input::Committed { revision: 2, touches: true }, &mut wanted);
    assert_eq!(effects, [Effect::Read(2)]);
    refresh(&world).await.unwrap();
    read(&mut reference, Input::ReadDone { revision: 3, rows: world.feed_ids().await }, &mut wanted);
    read(&mut reference, Input::WindowState(loaded()), &mut wanted);
    world.settle().await;
    stream.drain();
    assert_eq!(canon(stream.vals().into_iter().skip(1).collect()), canon(wanted));
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_unrelated() {
    let (world, stream, mut reference) = live_rows().await;
    let events = world.events_stream();
    world.net.wait_subscribes(1).await;
    world.breathe().await;
    let effects = reference.step(Input::Committed { revision: 2, touches: false });
    assert!(effects.is_empty());
    world.net.emit(vec![chat_entry(1, "a")]).unwrap();
    world.pump_until(&events, |vals| vals.iter().any(|val| matches!(val, Val::Event { .. }))).await;
    world.settle().await;
    stream.drain();
    assert_eq!(stream.vals().len(), 1, "a commit of another model gives the rows stream no value");
    assert!(reference.drain().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_read_changed() {
    let (world, stream, mut reference) = live_rows().await;
    let mut wanted = Vec::new();
    world.net.script(Reply::page(&["r1", "r9"], false));
    refresh(&world).await.unwrap();
    read(&mut reference, Input::WindowState(loading()), &mut wanted);
    read(&mut reference, Input::Committed { revision: 2, touches: true }, &mut wanted);
    read(&mut reference, Input::ReadDone { revision: 3, rows: world.feed_ids().await }, &mut wanted);
    read(&mut reference, Input::WindowState(loaded()), &mut wanted);
    world.settle().await;
    stream.drain();
    assert!(wanted.iter().any(|val| matches!(val, Val::Delta { .. })), "the reference gives a Delta for changed rows");
    assert_eq!(canon(stream.vals().into_iter().skip(1).collect()), canon(wanted));
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_read_same() {
    let (world, stream, mut reference) = live_rows().await;
    let mut wanted = Vec::new();
    world.net.script(Reply::page(&["r1"], false));
    refresh(&world).await.unwrap();
    read(&mut reference, Input::WindowState(loading()), &mut wanted);
    read(&mut reference, Input::Committed { revision: 2, touches: true }, &mut wanted);
    read(&mut reference, Input::ReadDone { revision: 3, rows: ids(&["r1"]) }, &mut wanted);
    read(&mut reference, Input::WindowState(loaded()), &mut wanted);
    world.settle().await;
    stream.drain();
    assert!(!wanted.iter().any(|val| matches!(val, Val::Delta { .. })), "the same rows give no Delta");
    assert_eq!(canon(stream.vals().into_iter().skip(1).collect()), canon(wanted));
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_window() {
    let (world, stream, mut reference) = live_rows().await;
    let mut wanted = Vec::new();
    world.net.script(Reply::refused());
    let _ = refresh(&world).await;
    read(&mut reference, Input::WindowState(loading()), &mut wanted);
    read(&mut reference, Input::WindowState(WState { phase: Phase::Loaded, error: true, loaded: true }), &mut wanted);
    world.settle().await;
    stream.drain();
    assert_eq!(canon(stream.vals().into_iter().skip(1).collect()), canon(wanted), "a new window state arrives as a Snapshot with the same rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_window_same() {
    let (world, stream, mut reference) = live_rows().await;
    let mut wanted = Vec::new();
    world.net.script(Reply::page(&["r1"], false));
    refresh(&world).await.unwrap();
    read(&mut reference, Input::WindowState(loading()), &mut wanted);
    read(&mut reference, Input::WindowState(loaded()), &mut wanted);
    read(&mut reference, Input::WindowState(loaded()), &mut wanted);
    world.settle().await;
    stream.drain();
    assert_eq!(canon(stream.vals().into_iter().skip(1).collect()), canon(wanted));
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_event() {
    let world = World::open().await;
    let stream = world.events_stream();
    world.net.wait_subscribes(1).await;
    world.breathe().await;
    let mut reference = Delivery::new();
    reference.step(Input::Open { kind: Kind::Events, epoch_current: true });
    reference.step(Input::ModelEvent { position: 1, id: "c1".into() });
    world.net.emit(vec![chat_entry(1, "a")]).unwrap();
    let core = world.pump_until(&stream, |vals| vals.iter().any(|val| matches!(val, Val::Event { .. }))).await;
    let core: Vec<Val> = core.into_iter().filter(|val| matches!(val, Val::Event { .. })).collect();
    assert_eq!(core, reference.drain(), "the first event of a stream has position 1");
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_pull() {
    let world = World::open().await;
    let stream = world.events_stream();
    let probe = world.events_stream();
    world.net.wait_subscribes(1).await;
    world.breathe().await;
    world.net.emit(vec![chat_entry(1, "a"), chat_entry(2, "b")]).unwrap();
    world.pump_until(&probe, |vals| vals.iter().filter(|val| matches!(val, Val::Event { .. })).count() >= 2).await;
    let mut reference = Delivery::new();
    reference.step(Input::Open { kind: Kind::Events, epoch_current: true });
    reference.step(Input::ModelEvent { position: 1, id: "c1".into() });
    reference.step(Input::ModelEvent { position: 2, id: "c2".into() });
    let first = world.db.read_stream(stream.id).map(|delivery| super::harness::val_of(&delivery));
    assert_eq!(first.map(Pulled::Value), Some(reference.pull()), "Pull gives the first value of the journal");
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_pull_empty() {
    let world = World::open().await;
    let stream = world.events_stream();
    world.net.wait_subscribes(1).await;
    world.breathe().await;
    world.settle().await;
    stream.drain();
    let mut reference = Delivery::new();
    reference.step(Input::Open { kind: Kind::Events, epoch_current: true });
    assert_eq!(reference.pull(), Pulled::Waits);
    assert!(world.db.read_stream(stream.id).is_none(), "an empty journal gives the reader nothing");
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_ended() {
    let (world, stream, mut reference) = live_rows().await;
    reference.step(Input::Closing);
    world.close().await;
    stream.until(|vals| vals.iter().any(|val| matches!(val, Val::Ended(_)))).await;
    let mut seen = 0;
    while !matches!(reference.pull(), Pulled::Done) { seen += 1; assert!(seen < 10); }
    assert!(world.db.read_stream(stream.id).is_none(), "after Ended a pull gives nothing");
}

/// P-DLV-01 over traces of refreshes: the first value is a Snapshot and every Delta starts at the revision of the value before.
#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_first_and_base() {
    let steps = |symbol: usize| match symbol {
        0 => Some(Reply::page(&["r1", "r2"], false)),
        1 => Some(Reply::page(&["r1"], false)),
        2 => Some(Reply::refused()),
        3 => Some(Reply::page(&["r3"], false)),
        _ => None,
    };
    for trace in sequences(5, 4, usize::MAX).into_iter().step_by(6) {
        let world = World::open().await;
        world.load_ok().await;
        let stream = world.rows_stream();
        stream.first_screen().await;
        for symbol in trace {
            match steps(symbol) {
                Some(reply) => { world.net.script(reply); let _ = refresh(&world).await; }
                None => { stream.drain(); }
            }
            world.settle().await;
        }
        stream.drain();
        let deliveries = stream.all();
        let mut previous: Option<i64> = None;
        for (index, delivery) in deliveries.iter().enumerate() {
            let Some((base, revision)) = revision_of(delivery) else { continue };
            if index == 0 || previous.is_none() { assert!(base.is_none(), "the first value of a rows stream is a Snapshot: {deliveries:?}"); }
            if let Some(base) = base { assert_eq!(Some(base), previous, "a Delta starts at the revision of the value before"); }
            previous = Some(revision);
        }
        world.db.cancel_stream(stream.id);
    }
}

/// P-DLV-02: positions of events and lost ranges cover 1..N.
#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_events_cover() {
    for count in [1usize, 5, 300, 400] {
        let world = World::open().await;
        let stream = world.events_stream();
        world.net.wait_subscribes(1).await;
    world.breathe().await;
        world.net.emit((0..count).map(|index| chat_entry(index, "t")).collect()).unwrap();
        world.pump_until(&stream, |vals| vals.iter().any(|val| matches!(val, Val::Event { .. } | Val::Lost { .. }))).await;
        world.settle().await;
        stream.drain();
        let mut covered = 0u64;
        for val in stream.vals() {
            match val {
                Val::Event { position, .. } => { assert_eq!(position, covered + 1, "positions have no gap and no repeat"); covered = position; }
                Val::Lost { first, last } => { assert_eq!(first, covered + 1); covered = last; }
                _ => {}
            }
        }
        assert_eq!(covered, count as u64, "events and lost ranges cover every event after the open");
    }
}

/// P-DLV-03: after a close one Ended arrives and nothing follows.
#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_ended_once() {
    for steps in 0..3usize {
        let world = World::open().await;
        world.load_ok().await;
        let stream = world.rows_stream();
        stream.first_screen().await;
        for _ in 0..steps { world.net.script(Reply::page(&["r1", "r2"], false)); let _ = refresh(&world).await; }
        world.close().await;
        stream.until(|vals| vals.iter().any(|val| matches!(val, Val::Ended(_)))).await;
        world.settle().await;
        stream.drain();
        let vals = stream.vals();
        assert_eq!(vals.iter().filter(|val| matches!(val, Val::Ended(_))).count(), 1);
        assert!(matches!(vals.last(), Some(Val::Ended(_))), "nothing follows Ended: {vals:?}");
    }
}

/// P-DLV-04: rows in the base are in the last value.
#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_rows_visible() {
    let world = World::open().await;
    world.net.script(Reply::page(&["r1", "r2", "r3"], false));
    world.load_ok().await;
    let stream = world.rows_stream();
    let vals = stream.until(|vals| vals.iter().any(|val| matches!(val, Val::Snapshot { rows, .. } if rows.len() == 3))).await;
    assert!(matches!(vals.last(), Some(Val::Snapshot { rows, .. }) if rows.len() == 3));
}

/// P-DLV-05: a state with the load done goes with the rows of that load.
#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_same_revision() {
    let world = World::open().await;
    world.net.script(Reply::page(&["r1", "r2"], false));
    let stream = world.rows_stream();
    let vals = world.pump_until(&stream, |_| states_of(&stream).iter().any(|state| state.loaded)).await;
    let mut seen: Vec<Val> = Vec::new();
    for val in vals {
        seen.push(val.clone());
        if matches!(&val, Val::Snapshot { state, .. } | Val::StateOnly(state) if state.loaded) {
            assert_eq!(held_ids(&seen), ["r1", "r2"], "a value that says the load is done carries the rows of the load");
        }
    }
}

/// P-DLV-06: Failed only without rows.
#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_failed_without_rows() {
    for rows_first in [false, true] {
        let world = World::open().await;
        if rows_first { world.load_ok().await; }
        world.net.script(Reply::refused());
        let stream = world.rows_stream();
        if rows_first { stream.first_screen().await; let _ = refresh(&world).await; }
        let vals = stream.until(|vals| vals.iter().any(|val| matches!(val, Val::Snapshot { state, .. } | Val::StateOnly(state) if state.error))).await;
        for val in vals {
            if let Val::Snapshot { rows, state } = val {
                if state.phase == Phase::Failed { assert!(rows.is_empty(), "Failed with rows"); }
            }
        }
    }
}

/// P-DLV-07: no commit touching the tables means no read and no value.
#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_idle() {
    let (world, stream, _) = live_rows().await;
    world.settle().await;
    let before = (stream.vals().len(), world.net.feed_requests());
    for _ in 0..10 { world.clock.advance(60_000); world.breathe().await; }
    stream.drain();
    assert_eq!((stream.vals().len(), world.net.feed_requests()), before, "an idle stream gives no value and no request");
}

/// P-DLV-09: a reader that reads rarely gets Lost and the journal stays within 256.
#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_bounded() {
    let world = World::open().await;
    let stream = world.events_stream();
    world.net.wait_subscribes(1).await;
    world.breathe().await;
    world.net.emit((0..400).map(|index| chat_entry(index, "t")).collect()).unwrap();
    world.settle().await;
    let got = stream.drain();
    assert!(got.len() <= 256, "the journal holds at most 256 values, got {}", got.len());
    assert!(got.iter().any(|delivery| matches!(delivery.value, crate::ffi::StreamValue::Lost { .. })), "evicted events come as Lost");
}
