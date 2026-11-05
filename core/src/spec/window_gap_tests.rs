//! Tests of `window.md` that need hooks the public API does not have (`api_gap.md`). Compiled only with the feature `spec_api`.
//! Hooks used on `World`: `stale_window(model, relation)`, `reset_models(&[model])`, `reject_next_commit(reason)`, `hold_commit()`, `release_commit()`, `end_epoch(cause)`.

use super::harness::{Gate, Outcome, Reply, World};
use super::ref_window::{Effect, Input, Reject, Result3, State, Window};

fn held(world: &World) -> std::sync::Arc<Gate> {
    let gate = Gate::new();
    world.net.script(Reply::held(&gate, Reply::page(&["r1"], false)));
    gate
}

fn res(outcome: Outcome) -> Result3 {
    match outcome { Outcome::Loaded => Result3::Loaded, Outcome::Failed => Result3::Failed, Outcome::Interrupted => Result3::Interrupted, Outcome::Pending => panic!("pending") }
}

#[tokio::test(flavor = "multi_thread")]
async fn win_idle_stale_watched() {
    let world = World::open().await;
    world.load_ok().await;
    let _stream = world.rows_stream();
    let mut reference = Window::idle_fresh();
    let effects = reference.stale_idle(true);
    let before = world.net.feed_requests();
    world.stale_window("Moment", "feed");
    world.settle().await;
    assert_eq!(world.net.feed_requests(), before + effects.len());
}

#[tokio::test(flavor = "multi_thread")]
async fn win_idle_stale_unwatched() {
    let world = World::open().await;
    world.load_ok().await;
    let mut reference = Window::idle_fresh();
    assert!(reference.stale_idle(false).is_empty());
    let before = world.net.feed_requests();
    world.stale_window("Moment", "feed");
    world.settle().await;
    assert_eq!(world.net.feed_requests(), before, "an unwatched window only notes that it is stale");
    let mut call = world.call();
    assert_eq!(call.outcome().await, Outcome::Loaded);
    assert_eq!(world.net.feed_requests(), before + 1, "the next interest loads because the window is stale");
}

#[tokio::test(flavor = "multi_thread")]
async fn win_idle_models_reset() {
    let world = World::open().await;
    world.load_ok().await;
    let _stream = world.rows_stream();
    let mut reference = Window::idle_fresh();
    reference.step(Input::ModelsReset { own_model: true });
    assert!(matches!(reference.state, State::Idle { fresh: false, .. }));
    let before = world.net.feed_requests();
    world.reset_models(&["Moment"]).await;
    world.settle().await;
    assert_eq!(world.net.feed_requests(), before + 1, "a reset window with interest loads again");
}

#[tokio::test(flavor = "multi_thread")]
async fn win_idle_models_other() {
    let world = World::open().await;
    world.load_ok().await;
    let mut reference = Window::idle_fresh();
    assert!(reference.step(Input::ModelsReset { own_model: false }).is_empty());
    let before = world.net.feed_requests();
    world.reset_models(&["Chat"]).await;
    world.settle().await;
    assert_eq!(world.net.feed_requests(), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_loading_stale() {
    let world = World::open().await;
    let gate = held(&world);
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    reference.step(Input::Stale { newer_than_issue: true });
    let mut call = world.call();
    gate.entered().await;
    world.stale_window("Moment", "feed");
    gate.release();
    assert_eq!(call.outcome().await, Outcome::Loaded);
    world.settle().await;
    assert_eq!(world.net.feed_requests(), 2, "a window made stale during the load loads once more after the answer");
}

#[tokio::test(flavor = "multi_thread")]
async fn win_loading_models_reset() {
    let world = World::open().await;
    let gate = held(&world);
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    let effects = reference.step(Input::ModelsReset { own_model: true });
    assert_eq!(effects.len(), 2);
    let _call = world.call();
    gate.entered().await;
    world.reset_models(&["Moment"]).await;
    world.breathe().await;
    assert_eq!(world.net.cancelled(), 1);
    assert_eq!(world.net.feed_requests(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_loading_models_other() {
    let world = World::open().await;
    let gate = held(&world);
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    assert!(reference.step(Input::ModelsReset { own_model: false }).is_empty());
    let _call = world.call();
    gate.entered().await;
    world.reset_models(&["Chat"]).await;
    world.breathe().await;
    assert_eq!((world.net.cancelled(), world.net.feed_requests()), (0, 1));
}

/// A load whose answer is held at the landing, then rejected for `reason`.
async fn landing(reason: Reject) -> (World, super::harness::Call, Window, Vec<Effect>) {
    let world = World::open().await;
    world.hold_commit();
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    reference.step(Input::Response { request: 1 });
    let effects = reference.step(Input::Rejected { request: 1, reason });
    let call = world.call();
    world.reject_next_commit(match reason { Reject::StaleGeneration => "stale_generation", Reject::NoSession => "no_session", Reject::Storage => "storage" });
    world.release_commit();
    (world, call, reference, effects)
}

#[tokio::test(flavor = "multi_thread")]
async fn win_landing_stale_generation() {
    let (world, mut call, mut reference, effects) = landing(Reject::StaleGeneration).await;
    assert_eq!(call.outcome().await, Outcome::Loaded);
    assert_eq!(world.net.feed_requests(), 1 + effects.len(), "a refused generation loads again");
    assert!(reference.take_outcomes().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn win_landing_no_session() {
    let (_world, mut call, mut reference, _) = landing(Reject::NoSession).await;
    assert_eq!(reference.take_outcomes(), [(1, res(call.outcome().await))]);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_landing_storage() {
    let (_world, mut call, mut reference, _) = landing(Reject::Storage).await;
    assert_eq!(reference.take_outcomes(), [(1, res(call.outcome().await))]);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_landing_join() {
    let world = World::open().await;
    world.hold_commit();
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    reference.step(Input::Response { request: 1 });
    assert!(reference.step(Input::InterestPlus { id: 2, force: false }).is_empty());
    let (mut first, mut second) = (world.call(), world.call());
    world.breathe().await;
    world.release_commit();
    reference.step(Input::Committed { request: 1 });
    assert_eq!(super::ref_window::Result3::Loaded, res(first.outcome().await));
    assert_eq!(world.net.feed_requests(), 1);
    assert_eq!(reference.take_outcomes().len(), 2);
    assert_eq!(second.outcome().await, Outcome::Loaded);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_landing_wait() {
    let world = World::open().await;
    world.hold_commit();
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    reference.step(Input::Response { request: 1 });
    for input in [Input::InterestMinus { id: 9 }, Input::Stale { newer_than_issue: true }, Input::Online(true), Input::ModelsReset { own_model: true }] {
        assert!(reference.step(input).is_empty());
    }
    let mut call = world.call();
    world.breathe().await;
    world.stale_window("Moment", "feed");
    world.db.set_online(true).unwrap();
    world.release_commit();
    assert_eq!(call.outcome().await, Outcome::Loaded);
    assert_eq!(world.net.feed_requests(), 1, "the landing ends by the verdict of the store alone");
}

#[tokio::test(flavor = "multi_thread")]
async fn win_epoch_ended() {
    let world = World::open().await;
    let gate = held(&world);
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    let effects = reference.step(Input::EpochEnded);
    assert_eq!(effects, [Effect::Cancel { request: 1 }]);
    let mut call = world.call();
    gate.entered().await;
    world.end_epoch(super::harness::Cause::SessionEnded);
    assert_eq!(reference.take_outcomes(), [(1, res(call.outcome().await))]);
    assert_eq!(world.net.cancelled(), 1);
}

/// P-WIN-04: an answer issued at generation g does not reach the base after a reset of its model.
#[tokio::test(flavor = "multi_thread")]
async fn win_prop_generation() {
    let world = World::open().await;
    let gate = held(&world);
    let _call = world.call();
    gate.entered().await;
    world.reset_models(&["Moment"]).await;
    gate.release();
    world.settle().await;
    assert!(world.feed_ids().await.iter().all(|id| id != "r1"), "the old answer is not in the base");
}

// Idle{last_error = network} is not reachable from the API: a temporary error is retried, and Online(false) moves Loading to Waiting.
// Hook: `World::fail_window(model, relation, network)` puts the window of the key into Idle with a stored error.

#[tokio::test(flavor = "multi_thread")]
async fn win_idle_online_retry() {
    let world = World::open().await;
    world.fail_window("Moment", "feed", true);
    let stream = world.rows_stream();
    stream.first_screen().await;
    world.settle().await;
    let before = world.net.feed_requests();
    let mut reference = Window::new();
    reference.state = State::Idle { fresh: false, stale: false, last_error: Some(true) };
    reference.online = false;
    let effects = reference.online_idle(true);
    go_offline(&world);
    world.db.set_online(true).unwrap();
    world.breathe().await;
    world.settle().await;
    assert_eq!(world.net.feed_requests(), before + requests_in(&effects), "coming online reloads a window with interest and a network error");
}


/// P-WIN-05: coming online reloads the window with a network error once, with no timer.
#[tokio::test(flavor = "multi_thread")]
async fn win_prop_online_once() {
    let world = World::open().await;
    world.fail_window("Moment", "feed", true);
    let stream = world.rows_stream();
    stream.first_screen().await;
    world.settle().await;
    let before = world.net.feed_requests();
    go_offline(&world);
    world.db.set_online(true).unwrap();
    world.breathe().await;
    world.settle().await;
    assert_eq!(world.net.feed_requests(), before + 1, "one reload for one change to online, without advancing the clock");
}
