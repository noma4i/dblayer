//! Tests of `feed.md` that need hooks the public API does not have (`api_gap.md`). Compiled only with the feature `spec_api`.
//! Hooks used on `World`: `events_stream_of(model)` (a second model with a change feed), `reject_next_commit(reason)`, `inject_channel_lost(models, dropped)`, `reset_models(&[model])`, `end_epoch(cause)`, `hold_commit()`, `release_commit()`, `inject_stray_commit_reply()`.

use super::harness::{chat_entry, Cause, Reading, Val, World};
use super::ref_feed::{Effect, Feed, Input, Mode, Obs, Reject};

fn obs_of(stream: &Reading) -> Vec<Obs> {
    stream.vals().into_iter().filter_map(|val| match val {
        Val::Event { position, id } => Some(Obs::Event { model: "Chat".into(), position, id }),
        Val::Lost { first, last } => Some(Obs::Lost { model: "Chat".into(), first, last }),
        Val::Ended(Cause::SessionEnded | Cause::Closed) => Some(Obs::Ended),
        _ => None,
    }).collect()
}

async fn live() -> (World, Reading, Feed) {
    let world = World::open().await;
    let stream = world.events_stream();
    world.net.wait_subscribes(1).await;
    let mut reference = Feed::new();
    reference.step(Input::WantPlus { model: "Chat".into() });
    reference.step(Input::Ready);
    assert_eq!(reference.mode, Mode::Live);
    (world, stream, reference)
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_opening_want() {
    let world = World::open().await;
    world.net.fail_subscribes(true);
    let mut reference = Feed::new();
    reference.step(Input::WantPlus { model: "Chat".into() });
    let effects = reference.step(Input::WantPlus { model: "Post".into() });
    assert_eq!(effects, [Effect::Subscribe]);
    let _chat = world.events_stream();
    let _post = world.events_stream_of("Post");
    world.breathe().await;
    assert_eq!(world.net.subscribes(), 2, "a changed set subscribes again");
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_live_want_more() {
    let (world, _stream, mut reference) = live().await;
    let effects = reference.step(Input::WantPlus { model: "Post".into() });
    assert_eq!(effects, [Effect::Subscribe]);
    let _post = world.events_stream_of("Post");
    world.net.wait_subscribes(2).await;
    assert_eq!(world.net.unsubscribes(), 1, "the old connection closes after the new one is ready");
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_live_rejected_generation() {
    let (world, stream, mut reference) = live().await;
    reference.step(Input::Event { current: true, model: "Chat".into(), id: "c1".into() });
    reference.step(Input::Tick);
    reference.step(Input::Rejected(Reject::StaleGeneration(vec!["Chat".into()])));
    world.reject_next_commit("stale_generation");
    world.net.emit(vec![chat_entry(1, "a")]).unwrap();
    world.clock.advance(60_000);
    world.settle().await;
    stream.drain();
    assert_eq!(obs_of(&stream), reference.take_obs());
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_live_rejected_storage() {
    let (world, stream, mut reference) = live().await;
    reference.step(Input::Event { current: true, model: "Chat".into(), id: "c1".into() });
    reference.step(Input::Tick);
    reference.step(Input::Rejected(Reject::Storage));
    world.reject_next_commit("storage");
    world.net.emit(vec![chat_entry(1, "a")]).unwrap();
    world.clock.advance(60_000);
    world.settle().await;
    stream.drain();
    assert_eq!(obs_of(&stream), reference.take_obs());
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_live_rejected_session() {
    let (world, stream, mut reference) = live().await;
    reference.step(Input::Event { current: true, model: "Chat".into(), id: "c1".into() });
    reference.step(Input::Tick);
    reference.step(Input::Rejected(Reject::NoSession));
    assert_eq!(reference.mode, Mode::Ended);
    world.reject_next_commit("no_session");
    world.net.emit(vec![chat_entry(1, "a")]).unwrap();
    world.clock.advance(60_000);
    world.settle().await;
    stream.drain();
    assert!(obs_of(&stream).iter().all(|obs| !matches!(obs, Obs::Event { .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_live_channel_lost() {
    let (world, stream, mut reference) = live().await;
    reference.step(Input::Event { current: true, model: "Chat".into(), id: "c1".into() });
    let effects = reference.step(Input::ChannelLost { dropped: 5, models: vec!["Chat".into()] });
    assert_eq!(effects[0], Effect::Commit);
    world.net.emit(vec![chat_entry(1, "a")]).unwrap();
    world.inject_channel_lost(&["Chat"], 5);
    world.clock.advance(60_000);
    world.settle().await;
    stream.drain();
    assert_eq!(obs_of(&stream), {
        reference.step(Input::Committed);
        reference.take_obs()
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_live_models_reset() {
    let (world, stream, mut reference) = live().await;
    reference.step(Input::Event { current: true, model: "Chat".into(), id: "c1".into() });
    assert!(reference.step(Input::ModelsReset { models: vec!["Chat".into()] }).is_empty());
    world.net.emit(vec![chat_entry(1, "a")]).unwrap();
    world.reset_models(&["Chat"]).await;
    world.clock.advance(60_000);
    world.settle().await;
    stream.drain();
    let obs = obs_of(&stream);
    assert!(obs.iter().any(|obs| matches!(obs, Obs::Event { .. } | Obs::Lost { .. })), "the event is delivered or lost after a reset of its model");
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_epoch_ended() {
    let (world, stream, mut reference) = live().await;
    let effects = reference.step(Input::EpochEnded);
    assert_eq!(effects, [Effect::Unsubscribe]);
    world.end_epoch(Cause::SessionEnded);
    world.settle().await;
    stream.drain();
    assert_eq!(obs_of(&stream), reference.take_obs());
    assert_eq!(world.net.unsubscribes(), 1);
}

/// P-FEED-04: a reset of model X does not lose the events of model Y.
#[tokio::test(flavor = "multi_thread")]
async fn feed_prop_reset_other_model() {
    let (world, stream, _) = live().await;
    let _post = world.events_stream_of("Post");
    world.net.emit(vec![chat_entry(1, "a")]).unwrap();
    world.reset_models(&["Post"]).await;
    world.clock.advance(60_000);
    world.settle().await;
    stream.drain();
    assert!(obs_of(&stream).iter().any(|obs| matches!(obs, Obs::Event { id, .. } if id == "c1")), "the Chat event survives a reset of Post");
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_landing_after_drop() {
    let (world, stream, mut reference) = live().await;
    world.hold_commit();
    reference.debounce = false;
    assert_eq!(reference.step(Input::Event { current: true, model: "Chat".into(), id: "c1".into() }), [Effect::Commit]);
    reference.step(Input::Dropped);
    assert!(matches!(reference.mode, Mode::Opening { gap: true }));
    world.net.emit(vec![chat_entry(1, "a")]).unwrap();
    world.breathe().await;
    world.net.drop_connection();
    world.release_commit();
    reference.step(Input::Committed);
    world.clock.advance(60_000);
    world.settle().await;
    stream.drain();
    assert!(obs_of(&stream).contains(&Obs::Event { model: "Chat".into(), position: 1, id: "c1".into() }), "a batch that lands after the break is still delivered");
    assert_eq!(reference.take_obs()[0], Obs::Event { model: "Chat".into(), position: 1, id: "c1".into() });
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_stray_batch() {
    let (world, stream, mut reference) = live().await;
    assert!(reference.step(Input::StrayBatch).is_empty());
    world.inject_stray_commit_reply();
    world.settle().await;
    stream.drain();
    assert!(obs_of(&stream).is_empty(), "a reply for a batch the feed did not send changes nothing");
    assert!(reference.take_obs().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_live_event_unwatched() {
    let world = World::open().await;
    let _post = world.events_stream_of("Post");
    world.net.wait_subscribes(1).await;
    let mut reference = Feed::new();
    reference.step(Input::WantPlus { model: "Post".into() });
    reference.step(Input::Ready);
    assert!(reference.step(Input::Event { current: true, model: "Chat".into(), id: "c1".into() }).is_empty());
    world.net.emit(vec![chat_entry(1, "a")]).unwrap();
    world.clock.advance(60_000);
    world.settle().await;
    assert!(world.chat_ids().await.is_empty(), "an event of a model with no reader and no window is not applied");
}
