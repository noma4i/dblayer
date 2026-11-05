//! Tests of `delivery.md` that need hooks the public API does not have (`api_gap.md`). Compiled only with the feature `spec_api`.
//! Hooks used on `World`: `hold_reads()`, `release_reads()`, `fail_next_read()`, `end_epoch(cause)`, `reset_models(&[model])`, `inject_model_lost(first, last)`, `inject_model_event(position)`.

use super::harness::{loaded, Cause, Val, World};
use super::ref_delivery::{Delivery, Effect, Input, Kind, Pulled};

fn ids(rows: &[&str]) -> Vec<String> { rows.iter().map(|id| id.to_string()).collect() }

/// A rows stream opened while the read of the first value is held.
async fn opening() -> (World, super::harness::Reading, Delivery) {
    let world = World::open().await;
    world.load_ok().await;
    world.hold_reads();
    let stream = world.rows_stream();
    let mut reference = Delivery::new();
    reference.step(Input::Open { kind: Kind::Rows, epoch_current: true });
    (world, stream, reference)
}

async fn live() -> (World, super::harness::Reading, Delivery) {
    let world = World::open().await;
    world.load_ok().await;
    let stream = world.rows_stream();
    stream.first_screen().await;
    let mut reference = Delivery::new();
    reference.step(Input::Open { kind: Kind::Rows, epoch_current: true });
    reference.step(Input::WindowState(loaded()));
    reference.step(Input::ReadDone { revision: 1, rows: ids(&["r1"]) });
    reference.drain();
    (world, stream, reference)
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_opening_read_failed() {
    let world = World::open().await;
    world.fail_next_read();
    let stream = world.rows_stream();
    let mut reference = Delivery::new();
    reference.step(Input::Open { kind: Kind::Rows, epoch_current: true });
    reference.step(Input::ReadFailed { revision: 1 });
    assert_eq!(stream.until(|vals| !vals.is_empty()).await, reference.drain());
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_opening_committed() {
    let (world, stream, mut reference) = opening().await;
    let effects = reference.step(Input::Committed { revision: 2, touches: true });
    assert_eq!(effects, [Effect::Read(2), Effect::DropOldRead]);
    world.net.script(super::harness::Reply::page(&["r1", "r2"], false));
    world.db.refresh(world.epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    world.release_reads();
    let core = stream.until(|vals| !vals.is_empty()).await;
    reference.step(Input::ReadDone { revision: 3, rows: ids(&["r1", "r2"]) });
    assert_eq!(super::harness::canon(core), super::harness::canon(reference.drain()), "the older read is dropped, the first value shows the newer rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_opening_unrelated() {
    let (world, stream, mut reference) = opening().await;
    assert!(reference.step(Input::Committed { revision: 2, touches: false }).is_empty());
    world.commit_other_model().await;
    stream.drain();
    assert!(stream.vals().is_empty(), "an unrelated commit gives no value while the stream opens");
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_opening_window() {
    let (world, stream, mut reference) = opening().await;
    reference.step(Input::WindowState(loaded()));
    world.release_reads();
    reference.step(Input::ReadDone { revision: 1, rows: ids(&["r1"]) });
    assert_eq!(super::harness::canon(stream.first_screen().await), super::harness::canon(reference.drain()));
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_opening_event() {
    let (world, stream, mut reference) = opening().await;
    reference.step(Input::ModelEvent { position: 1, id: "c1".into() });
    reference.step(Input::ModelLost { first: 2, last: 3 });
    world.inject_model_event(1);
    world.inject_model_lost(2, 3);
    stream.drain();
    assert!(stream.vals().is_empty() && reference.drain().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_opening_pull() {
    let (world, stream, mut reference) = opening().await;
    assert_eq!(reference.pull(), Pulled::Waits);
    assert!(world.db.read_stream(stream.id).is_none(), "the reader waits while the first read is held");
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_epoch_ended() {
    let (world, stream, mut reference) = live().await;
    let effects = reference.step(Input::EpochEnded { own: true });
    assert_eq!(effects, [Effect::InterestMinus]);
    world.end_epoch(Cause::SessionEnded);
    let core = stream.until(|vals| vals.iter().any(|val| matches!(val, Val::Ended(_)))).await;
    assert_eq!(core.into_iter().skip(1).collect::<Vec<_>>(), reference.drain());
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_epoch_other() {
    let (world, stream, mut reference) = live().await;
    assert!(reference.step(Input::EpochEnded { own: false }).is_empty());
    world.end_other_epoch();
    stream.drain();
    assert_eq!(stream.vals().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_read_old() {
    let (world, stream, mut reference) = live().await;
    reference.step(Input::ReadDone { revision: 0, rows: ids(&["rX"]) });
    world.inject_read_done(0, &["rX"]);
    stream.drain();
    assert_eq!(stream.vals().len(), 1, "an older read gives no value");
    assert!(reference.drain().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_read_failed() {
    let (world, stream, mut reference) = live().await;
    reference.step(Input::ReadFailed { revision: 2 });
    world.fail_next_read();
    world.touch_rows().await;
    let core = stream.until(|vals| vals.len() >= 2).await;
    assert_eq!(core.into_iter().skip(1).collect::<Vec<_>>(), reference.drain(), "Loaded with an error at the same rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_event_gap() {
    let world = World::open().await;
    let stream = world.events_stream();
    let mut reference = Delivery::new();
    reference.step(Input::Open { kind: Kind::Events, epoch_current: true });
    reference.step(Input::ModelEvent { position: 3, id: "c3".into() });
    world.inject_model_event(3);
    let core = stream.until(|vals| vals.len() >= 2).await;
    assert_eq!(core, reference.drain(), "Lost{{1,2}} then Event 3");
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_event_old() {
    let world = World::open().await;
    let stream = world.events_stream();
    let mut reference = Delivery::new();
    reference.step(Input::Open { kind: Kind::Events, epoch_current: true });
    reference.step(Input::ModelEvent { position: 2, id: "c2".into() });
    reference.step(Input::ModelEvent { position: 1, id: "c1".into() });
    world.inject_model_event(2);
    world.inject_model_event(1);
    let core = stream.until(|vals| vals.len() >= 2).await;
    world.settle().await;
    stream.drain();
    assert_eq!(stream.vals().len(), core.len());
    assert_eq!(core, reference.drain());
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_lost() {
    let world = World::open().await;
    let stream = world.events_stream();
    let mut reference = Delivery::new();
    reference.step(Input::Open { kind: Kind::Events, epoch_current: true });
    reference.step(Input::ModelLost { first: 1, last: 4 });
    world.inject_model_lost(1, 4);
    assert_eq!(stream.until(|vals| !vals.is_empty()).await, reference.drain());
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_models_reset() {
    let (world, stream, mut reference) = live().await;
    let effects = reference.step(Input::ModelsReset { own_model: true });
    assert_eq!(effects, [Effect::Read(1)]);
    world.reset_models(&["Moment"]).await;
    reference.step(Input::ReadDone { revision: 2, rows: Vec::new() });
    let core = stream.until(|vals| vals.len() >= 2).await;
    assert_eq!(core.into_iter().skip(1).collect::<Vec<_>>(), reference.drain(), "the stream shows the emptied model after a reset");
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_models_other() {
    let (world, stream, mut reference) = live().await;
    assert!(reference.step(Input::ModelsReset { own_model: false }).is_empty());
    world.reset_models(&["Chat"]).await;
    stream.drain();
    assert_eq!(stream.vals().len(), 1);
}

/// P-DLV-08: after the end of the epoch the stream gives only Ended(SessionEnded).
#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_epoch() {
    let (world, stream, _) = live().await;
    world.end_epoch(Cause::SessionEnded);
    stream.until(|vals| vals.iter().any(|val| matches!(val, Val::Ended(_)))).await;
    world.net.script(super::harness::Reply::page(&["r9"], false));
    world.touch_rows().await;
    world.settle().await;
    stream.drain();
    let vals = stream.vals();
    assert_eq!(vals.last(), Some(&Val::Ended(Cause::SessionEnded)));
    assert_eq!(vals.iter().filter(|val| matches!(val, Val::Ended(_))).count(), 1);
}
