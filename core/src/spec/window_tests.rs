//! Tests of `specs/machines/window.md`. Names are the `Test` column.

use super::harness::{states_of, loaded, sequences, Call, Gate, Outcome, Phase, Reply, Val, World};
use super::ref_window::{Effect, Failure, Input, Result3, State, Window};
use crate::ffi::LoadMoreResult;

fn res(outcome: Outcome) -> Result3 {
    match outcome {
        Outcome::Loaded => Result3::Loaded,
        Outcome::Failed => Result3::Failed,
        Outcome::Interrupted => Result3::Interrupted,
        Outcome::Pending => panic!("a call that has not ended has no result"),
    }
}

fn requests_in(effects: &[Effect]) -> usize { effects.iter().filter(|effect| matches!(effect, Effect::Request { .. })).count() }

fn sorted(mut outcomes: Vec<(u32, Result3)>) -> Vec<(u32, Result3)> {
    outcomes.sort_by_key(|(id, _)| *id);
    outcomes
}

/// A held answer for the next request of the feed.
fn held(world: &World, then: Reply) -> std::sync::Arc<Gate> {
    let gate = Gate::new();
    world.net.script(Reply::held(&gate, then));
    gate
}

fn go_offline(world: &World) { world.db.set_online(false).unwrap(); }

/// Ref: the happy end of a load that is in `Loading`.
fn answer_and_land(window: &mut Window, request: u64) {
    let effects = window.step(Input::Response { request });
    assert_eq!(effects, [Effect::Commit { request }]);
    window.step(Input::Committed { request });
}

#[tokio::test(flavor = "multi_thread")]
async fn win_idle_interest_fresh() {
    let world = World::open().await;
    world.load_ok().await;
    let before = world.net.feed_requests();
    let mut reference = Window::idle_fresh();
    let effects = reference.step(Input::InterestPlus { id: 1, force: false });
    let mut call = world.call();
    assert_eq!(call.outcome().await, Outcome::Loaded);
    assert_eq!(sorted(reference.take_outcomes()), [(1, res(Outcome::Loaded))]);
    assert_eq!(world.net.feed_requests(), before + requests_in(&effects), "a fresh window answers at once without a request");
}

#[tokio::test(flavor = "multi_thread")]
async fn win_idle_interest_load() {
    let world = World::open().await;
    let gate = held(&world, Reply::page(&["r1"], false));
    let mut reference = Window::new();
    let effects = reference.step(Input::InterestPlus { id: 1, force: false });
    let mut call = world.call();
    gate.entered().await;
    assert_eq!(world.net.feed_requests(), requests_in(&effects));
    assert_eq!(reference.phase, Phase::Loading);
    assert_eq!(call.outcome_within(150).await, Outcome::Pending);
    gate.release();
    answer_and_land(&mut reference, 1);
    assert_eq!(res(call.outcome().await), reference.take_outcomes()[0].1);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_idle_interest_offline() {
    let world = World::open().await;
    go_offline(&world);
    let mut reference = Window::new();
    reference.online = false;
    reference.step(Input::InterestPlus { id: 1, force: false });
    assert!(matches!(reference.state, State::Waiting { .. }));
    let mut call = world.call();
    assert_eq!(call.outcome_within(300).await, Outcome::Pending, "offline the call waits for the network");
    assert_eq!(world.net.feed_requests(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_idle_interest_minus() {
    let world = World::open().await;
    world.load_ok().await;
    let before = world.net.feed_requests();
    let mut reference = Window::idle_fresh();
    assert!(reference.step(Input::InterestMinus { id: 1 }).is_empty());
    let mut call = world.call();
    call.outcome().await;
    call.cancel();
    world.breathe().await;
    assert_eq!(world.net.feed_requests(), before);
    assert!(reference.take_outcomes().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn win_idle_load_more() {
    let world = World::open().await;
    world.net.script(Reply::page(&["r1"], true));
    world.load_ok().await;
    world.net.script(Reply::page(&["r2"], false));
    let mut reference = Window::idle_fresh();
    reference.cursor = true;
    let effects = reference.step(Input::LoadMore { id: 1 });
    assert_eq!(effects, [Effect::Request { request: 1, page: 2 }]);
    let result = world.load_more().await;
    assert_eq!(result, Ok(LoadMoreResult::Loaded { has_more: false }));
    assert_eq!(world.net.feed_requests(), 2);
    answer_and_land(&mut reference, 1);
    assert_eq!(reference.take_outcomes(), [(1, Result3::Loaded)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_idle_load_more_end() {
    let world = World::open().await;
    world.net.script(Reply::page(&["r1"], false));
    world.load_ok().await;
    let mut reference = Window::idle_fresh();
    let effects = reference.step(Input::LoadMore { id: 1 });
    assert!(effects.is_empty());
    let result = world.load_more().await;
    assert!(matches!(result, Ok(LoadMoreResult::NoMore | LoadMoreResult::Loaded { .. })), "the end of the pages is an answer, not an error: {result:?}");
    assert_eq!(world.net.feed_requests(), 1);
    assert_eq!(reference.take_outcomes(), [(1, Result3::Loaded)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_idle_online() {
    let world = World::open().await;
    world.load_ok().await;
    let stream = world.rows_stream();
    stream.first_screen().await;
    let before = world.net.feed_requests();
    let mut reference = Window::idle_fresh();
    let effects = reference.online_idle(true);
    go_offline(&world);
    world.db.set_online(true).unwrap();
    world.breathe().await;
    world.settle().await;
    assert_eq!(world.net.feed_requests(), before + requests_in(&effects));
}

#[tokio::test(flavor = "multi_thread")]
async fn win_idle_stray() {
    let world = World::open().await;
    let gate = held(&world, Reply::page(&["r1"], false));
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    let call = world.call();
    gate.entered().await;
    reference.step(Input::InterestMinus { id: 1 });
    call.cancel();
    world.breathe().await;
    gate.release();
    world.breathe().await;
    assert!(reference.step(Input::Response { request: 1 }).is_empty());
    assert!(world.feed_ids().await.is_empty(), "the answer of a cancelled request lands nowhere");
}

#[tokio::test(flavor = "multi_thread")]
async fn win_loading_join() {
    let world = World::open().await;
    let gate = held(&world, Reply::page(&["r1"], false));
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    assert!(reference.step(Input::InterestPlus { id: 2, force: false }).is_empty());
    let mut first = world.call();
    gate.entered().await;
    let mut second = world.call();
    world.breathe().await;
    assert_eq!(world.net.feed_requests(), 1, "a second interest shares the request in flight");
    gate.release();
    answer_and_land(&mut reference, 1);
    assert_eq!(sorted(reference.take_outcomes()), [(1, res(first.outcome().await)), (2, res(second.outcome().await))]);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_loading_leave() {
    let world = World::open().await;
    let gate = held(&world, Reply::page(&["r1"], false));
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    reference.step(Input::InterestPlus { id: 2, force: false });
    let first = world.call();
    gate.entered().await;
    let mut second = world.call();
    world.breathe().await;
    assert!(reference.step(Input::InterestMinus { id: 1 }).is_empty());
    first.cancel();
    world.breathe().await;
    assert_eq!(world.net.cancelled(), 0, "the request goes on while another interest waits");
    gate.release();
    answer_and_land(&mut reference, 1);
    assert_eq!(reference.take_outcomes(), [(2, res(second.outcome().await))]);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_loading_last_leave() {
    let world = World::open().await;
    let gate = held(&world, Reply::page(&["r1"], false));
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    let call = world.call();
    gate.entered().await;
    let effects = reference.step(Input::InterestMinus { id: 1 });
    assert_eq!(effects, [Effect::Cancel { request: 1 }]);
    call.cancel();
    world.breathe().await;
    assert_eq!(world.net.cancelled(), effects.len(), "the last interest to leave cancels the request");
}

#[tokio::test(flavor = "multi_thread")]
async fn win_loading_response() {
    let world = World::open().await;
    let gate = held(&world, Reply::page(&["r1"], false));
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    let mut call = world.call();
    gate.entered().await;
    gate.release();
    assert_eq!(res(call.outcome().await), Result3::Loaded);
    answer_and_land(&mut reference, 1);
    assert_eq!(world.feed_ids().await, ["r1"], "the answer was written to the base");
    assert_eq!(reference.take_outcomes(), [(1, Result3::Loaded)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_loading_temp_error() {
    let world = World::open().await;
    world.net.script(Reply::network());
    world.net.script(Reply::page(&["r1"], false));
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    let effects = reference.step(Input::TransportError { request: 1, failure: Failure::Network });
    assert_eq!(effects, [Effect::ScheduleRetry]);
    let mut call = world.call();
    assert_eq!(call.outcome_within(300).await, Outcome::Pending, "a temporary error is retried, the call has no outcome yet");
    for _ in 0..60 {
        if world.net.feed_requests() >= 2 { break; }
        world.clock.advance(30_000);
        world.breathe().await;
    }
    assert_eq!(world.net.feed_requests(), 2, "the retry is one request after the backoff");
    assert_eq!(reference.step(Input::RetryDue), [Effect::Request { request: 1, page: 1 }]);
    answer_and_land(&mut reference, 1);
    assert_eq!(res(call.outcome().await), reference.take_outcomes()[0].1);
}

/// Network failures until the policy limit (3): the reference and the core both stop retrying and report `Failed`.
#[tokio::test(flavor = "multi_thread")]
async fn win_loading_attempts_exhausted() {
    let world = World::open().await;
    for _ in 0..3 { world.net.script(Reply::network()); }
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    for attempt in 0..2 {
        assert_eq!(reference.step(Input::TransportError { request: 1, failure: Failure::Network }), [Effect::ScheduleRetry], "attempt {attempt} is retried");
        reference.step(Input::RetryDue);
    }
    let effects = reference.step(Input::TransportError { request: 1, failure: Failure::Network });
    assert_eq!(effects, [Effect::WindowState]);
    assert_eq!(reference.state, State::Idle { fresh: false, stale: false, last_error: Some(true) });
    let stream = world.rows_stream();
    let mut call = world.call();
    for _ in 0..120 {
        if call.outcome_within(50).await != Outcome::Pending { break; }
        world.clock.advance(30_000);
        world.breathe().await;
    }
    assert_eq!(res(call.outcome().await), Result3::Failed);
    assert_eq!(world.net.feed_requests(), 3, "three attempts, then the error is final");
    let vals = stream.until(|vals| vals.iter().any(|val| matches!(val, Val::Snapshot { state, .. } | Val::StateOnly(state) if state.error))).await;
    let last = vals.iter().rev().find_map(|val| match val { Val::Snapshot { state, .. } | Val::StateOnly(state) => Some(*state), _ => None }).unwrap();
    assert_eq!(last.phase, reference.phase);
    assert_eq!(reference.take_outcomes(), [(1, Result3::Failed)]);
}

/// A window that failed for the network with a reader open: the reference and the core after three failed attempts.
async fn network_failed() -> (World, super::harness::Reading, Window) {
    let world = World::open().await;
    for _ in 0..3 { world.net.script(Reply::network()); }
    let stream = world.rows_stream();
    world.pump_until(&stream, |_| states_of(&stream).iter().any(|state| state.error)).await;
    world.settle().await;
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    for _ in 0..2 {
        reference.step(Input::TransportError { request: 1, failure: Failure::Network });
        reference.step(Input::RetryDue);
    }
    reference.step(Input::TransportError { request: 1, failure: Failure::Network });
    reference.online = false;
    (world, stream, reference)
}

#[tokio::test(flavor = "multi_thread")]
async fn win_idle_online_retry() {
    let (world, _stream, mut reference) = network_failed().await;
    let before = world.net.feed_requests();
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
    let (world, _stream, _) = network_failed().await;
    let before = world.net.feed_requests();
    go_offline(&world);
    world.db.set_online(true).unwrap();
    world.breathe().await;
    world.settle().await;
    assert_eq!(world.net.feed_requests(), before + 1, "one reload for one change to online, without advancing the clock");
}

#[tokio::test(flavor = "multi_thread")]
async fn win_loading_error() {
    let world = World::open().await;
    world.net.script(Reply::refused());
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    let effects = reference.step(Input::TransportError { request: 1, failure: Failure::Permanent });
    assert_eq!(effects, [Effect::WindowState]);
    let stream = world.rows_stream();
    let mut call = world.call();
    assert_eq!(res(call.outcome().await), Result3::Failed);
    let vals = stream.until(|vals| vals.iter().any(|val| matches!(val, Val::Snapshot { state, .. } | Val::StateOnly(state) if state.error))).await;
    let last = vals.iter().rev().find_map(|val| match val { Val::Snapshot { state, .. } | Val::StateOnly(state) => Some(*state), _ => None }).unwrap();
    assert_eq!(last.phase, reference.phase, "without rows the phase is Failed");
    assert_eq!(reference.take_outcomes(), [(1, Result3::Failed)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_loading_offline() {
    let world = World::open().await;
    let gate = held(&world, Reply::page(&["r1"], false));
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    let mut call = world.call();
    gate.entered().await;
    let effects = reference.step(Input::Online(false));
    assert_eq!(effects, [Effect::Cancel { request: 1 }]);
    go_offline(&world);
    world.breathe().await;
    assert_eq!(world.net.cancelled(), 1, "going offline cancels the request");
    assert_eq!(call.outcome_within(300).await, Outcome::Pending, "the call waits for the network, it does not fail");
    assert_eq!(reference.phase, Phase::Loading);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_landing_committed() {
    let world = World::open().await;
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    let stream = world.rows_stream();
    let mut call = world.call();
    assert_eq!(res(call.outcome().await), Result3::Loaded);
    answer_and_land(&mut reference, 1);
    let vals = world.pump_until(&stream, |_| states_of(&stream).iter().any(|state| state.loaded)).await;
    let _ = vals;
    let last = *states_of(&stream).last().unwrap();
    assert_eq!(last, loaded());
    assert_eq!(reference.phase, last.phase);
    assert_eq!(reference.take_outcomes(), [(1, Result3::Loaded)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_waiting_resume() {
    let world = World::open().await;
    go_offline(&world);
    let mut reference = Window::new();
    reference.online = false;
    reference.step(Input::InterestPlus { id: 1, force: false });
    let mut call = world.call();
    assert_eq!(call.outcome_within(200).await, Outcome::Pending);
    let effects = reference.step(Input::Online(true));
    world.db.set_online(true).unwrap();
    assert_eq!(res(call.outcome().await), Result3::Loaded);
    assert_eq!(world.net.feed_requests(), requests_in(&effects));
    answer_and_land(&mut reference, 1);
    assert_eq!(reference.take_outcomes(), [(1, Result3::Loaded)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_waiting_join() {
    let world = World::open().await;
    go_offline(&world);
    let mut reference = Window::new();
    reference.online = false;
    reference.step(Input::InterestPlus { id: 1, force: false });
    assert!(reference.step(Input::InterestPlus { id: 2, force: false }).is_empty());
    let (mut first, mut second) = (world.call(), world.call());
    assert_eq!(first.outcome_within(150).await, Outcome::Pending);
    assert_eq!(second.outcome_within(150).await, Outcome::Pending);
    let effects = reference.step(Input::Online(true));
    world.db.set_online(true).unwrap();
    assert_eq!((res(first.outcome().await), res(second.outcome().await)), (Result3::Loaded, Result3::Loaded));
    assert_eq!(world.net.feed_requests(), requests_in(&effects));
}

#[tokio::test(flavor = "multi_thread")]
async fn win_waiting_leave() {
    let world = World::open().await;
    go_offline(&world);
    let mut reference = Window::new();
    reference.online = false;
    reference.step(Input::InterestPlus { id: 1, force: false });
    assert!(reference.step(Input::InterestMinus { id: 1 }).is_empty());
    let call = world.call();
    world.breathe().await;
    call.cancel();
    world.db.set_online(true).unwrap();
    world.breathe().await;
    assert_eq!(world.net.feed_requests(), 0, "no interest is left, so coming online loads nothing");
    assert!(matches!(reference.state, State::Idle { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn win_closing() {
    let world = World::open().await;
    let gate = held(&world, Reply::page(&["r1"], false));
    let mut reference = Window::new();
    reference.step(Input::InterestPlus { id: 1, force: false });
    let mut call = world.call();
    gate.entered().await;
    let effects = reference.step(Input::Closing);
    assert_eq!(effects, [Effect::Cancel { request: 1 }, Effect::Drained]);
    world.close().await;
    assert_eq!(world.net.cancelled(), 1);
    assert_eq!(reference.take_outcomes(), [(1, res(call.outcome().await))]);
}

#[tokio::test(flavor = "multi_thread")]
async fn win_ended() {
    let world = World::open().await;
    let mut reference = Window::new();
    reference.step(Input::Closing);
    reference.ended_interest(1);
    world.close().await;
    let mut call = world.call();
    assert_eq!(reference.take_outcomes(), [(1, res(call.outcome().await))], "an interest on a closed key is interrupted");
}

/// Starts a held load and returns its call.
fn start(world: &World, gates: &mut Vec<std::sync::Arc<Gate>>) -> Call {
    gates.push(held(world, Reply::page(&["r1"], false)));
    world.call()
}

/// P-WIN-01 and P-WIN-03 over every trace of 5 inputs up to depth 4: starts, a leave, offline, online, release.
#[tokio::test(flavor = "multi_thread")]
async fn win_prop_outcome_once() {
    for trace in sequences(5, 4, usize::MAX).into_iter().step_by(5) {
        let world = World::open().await;
        let (mut gates, mut calls) = (Vec::new(), Vec::new());
        for symbol in &trace {
            match symbol {
                0 => calls.push(start(&world, &mut gates)),
                1 => { if let Some(call) = calls.last() { call.cancel(); } }
                2 => go_offline(&world),
                3 => world.db.set_online(true).unwrap(),
                _ => gates.iter().for_each(|gate| gate.release()),
            }
            world.breathe().await;
        }
        world.db.set_online(true).unwrap();
        gates.iter().for_each(|gate| gate.release());
        for call in &mut calls {
            assert_ne!(call.outcome_within(5_000).await, Outcome::Pending, "trace {trace:?}: a call has no outcome");
        }
        assert!(world.net.max_in_flight() <= 1, "trace {trace:?}: more than one request in flight");
    }
}

/// P-WIN-02: a call that ends Loaded leaves the rows of the answer in the base.
#[tokio::test(flavor = "multi_thread")]
async fn win_prop_loaded_rows() {
    let world = World::open().await;
    world.net.script(Reply::page(&["r1", "r2", "r3"], false));
    let mut call = world.call();
    assert_eq!(call.outcome().await, Outcome::Loaded);
    assert_eq!(world.feed_ids().await, ["r1", "r2", "r3"]);
}

/// P-WIN-03: five interests in one window make one request.
#[tokio::test(flavor = "multi_thread")]
async fn win_prop_single_flight() {
    let world = World::open().await;
    let gate = held(&world, Reply::page(&["r1"], false));
    let mut calls: Vec<Call> = Vec::new();
    for _ in 0..5 { calls.push(world.call()); }
    gate.entered().await;
    world.breathe().await;
    gate.release();
    for call in &mut calls { assert_eq!(call.outcome().await, Outcome::Loaded); }
    assert_eq!(world.net.max_in_flight(), 1);
    assert_eq!(world.net.feed_requests(), 1);
}

/// P-WIN-06: with no input there is no request and no schedule, offline too.
#[tokio::test(flavor = "multi_thread")]
async fn win_prop_idle() {
    let world = World::open().await;
    world.load_ok().await;
    let before = world.net.feed_requests();
    for _ in 0..20 { world.clock.advance(60_000); world.breathe().await; }
    assert_eq!(world.net.feed_requests(), before, "an idle window makes no request");
    world.net.script(Reply::network());
    let refreshing = world.db.refresh(world.epoch(), "Moment".into(), "feed".into(), "{}".into());
    let _ = tokio::time::timeout(std::time::Duration::from_millis(500), refreshing).await;
    go_offline(&world);
    let after_error = world.net.feed_requests();
    for _ in 0..20 { world.clock.advance(60_000); world.breathe().await; }
    assert_eq!(world.net.feed_requests(), after_error, "a retry is scheduled only while online");
}

/// P-WIN-07: the last interest to leave cancels the request and its answer is not written.
#[tokio::test(flavor = "multi_thread")]
async fn win_prop_cancel() {
    let world = World::open().await;
    let gate = held(&world, Reply::page(&["r1"], false));
    let call = world.call();
    gate.entered().await;
    call.cancel();
    world.breathe().await;
    gate.release();
    world.breathe().await;
    assert_eq!(world.net.cancelled(), 1);
    assert!(world.feed_ids().await.is_empty());
}
