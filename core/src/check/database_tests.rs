use super::corpus;
use super::database::DatabaseSut;
use super::model::{replay, Sut};
use super::value::{Input, Observation};
use serde_json::json;

/// Requirement: the shared corpus is green on the core through the public API of `Database`.
/// Observable: the observations of each corpus trace. Assert: equal to the recorded ones after every input; the corpus is not empty. Every red trace is listed, not only the first. The traces run in eight tests so that each stays under its time limit.
async fn shared_corpus(part: usize) {
    let traces = corpus::read_dir(&corpus::shared_dir()).unwrap();
    assert!(!traces.is_empty());
    let mut red = Vec::new();
    for trace in traces.into_iter().enumerate().filter(|(index, _)| index % PARTS == part).map(|(_, trace)| trace) {
        let mut real = DatabaseSut::default();
        if let Err(divergence) = replay(&mut real, &trace).await {
            red.push(format!("{}: {divergence}", trace.name));
        }
        real.finish().await;
    }
    assert!(red.is_empty(), "red traces:\n{}", red.join("\n"));
}

const PARTS: usize = 8;

#[tokio::test(flavor = "multi_thread")]
async fn the_shared_corpus_is_green_on_the_core_part_0() { shared_corpus(0).await }

#[tokio::test(flavor = "multi_thread")]
async fn the_shared_corpus_is_green_on_the_core_part_1() { shared_corpus(1).await }

#[tokio::test(flavor = "multi_thread")]
async fn the_shared_corpus_is_green_on_the_core_part_2() { shared_corpus(2).await }

#[tokio::test(flavor = "multi_thread")]
async fn the_shared_corpus_is_green_on_the_core_part_3() { shared_corpus(3).await }

#[tokio::test(flavor = "multi_thread")]
async fn the_shared_corpus_is_green_on_the_core_part_4() { shared_corpus(4).await }

#[tokio::test(flavor = "multi_thread")]
async fn the_shared_corpus_is_green_on_the_core_part_5() { shared_corpus(5).await }

#[tokio::test(flavor = "multi_thread")]
async fn the_shared_corpus_is_green_on_the_core_part_6() { shared_corpus(6).await }

#[tokio::test(flavor = "multi_thread")]
async fn the_shared_corpus_is_green_on_the_core_part_7() { shared_corpus(7).await }

fn tick(ms: u64) -> Input {
    Input::new("tick").with("ms", ms)
}

async fn fire_after_backoff(early_tick: bool) -> Vec<Observation> {
    let mut sut = DatabaseSut::default();
    assert_eq!(sut.apply(&Input::new("open")).await, vec![Observation::bare("opened")]);
    assert_eq!(sut.apply(&Input::new("set_online").with("online", true)).await, vec![Observation::new("online", json!(true))]);
    if early_tick {
        assert_eq!(sut.apply(&tick(2000)).await, vec![]);
    }
    assert_eq!(sut.apply(&Input::new("run").with("operation", "MomentsFeed").with("variables", json!({"first": 1}))).await, vec![Observation::new("request", json!({"id": "r1", "kind": "query", "purpose": "standard"}))]);
    assert_eq!(sut.apply(&Input::new("answer").with("id", "r1").with("json", json!({"data": {"feed": {"nodes": [], "pageInfo": {"endCursor": null, "hasNextPage": false}}}}))).await, vec![Observation::new("ran", json!({"answer": {"nodes": [], "pageInfo": {"endCursor": null, "hasNextPage": false}}, "operation": "MomentsFeed"}))]);
    assert_eq!(sut.apply(&Input::new("run").with("operation", "CreateMoment").with("variables", json!({"text": "x"}))).await, vec![Observation::new("request", json!({"id": "r2", "kind": "mutation", "purpose": "standard"}))]);
    assert_eq!(sut.apply(&Input::new("error").with("id", "r2").with("kind", "temporary")).await, vec![]);
    let fired = sut.apply(&tick(2000)).await;
    sut.finish().await;
    fired
}

/// Requirement: a `tick` shifts the clock and fires the next due timer; a tick issued before the owner armed its timer still fires exactly that timer once the arm happens, and no tick fires an unarmed clock (corpus.md:43, D-128).
/// Observable: the observations of each tick. Assert: an unarmed tick is empty; the backoff retry request is identical whether a tick preceded the arm or not.
#[tokio::test(flavor = "multi_thread")]
async fn corpus_tick_waits_armed() {
    let mut idle = DatabaseSut::default();
    assert_eq!(idle.apply(&Input::new("open")).await, vec![Observation::bare("opened")]);
    assert_eq!(idle.apply(&tick(60000)).await, vec![]);
    idle.finish().await;

    let arm_then_tick = fire_after_backoff(false).await;
    let tick_then_arm = fire_after_backoff(true).await;
    assert_eq!(arm_then_tick, vec![Observation::new("request", json!({"id": "r3", "kind": "mutation", "purpose": "standard"}))]);
    assert_eq!(tick_then_arm, arm_then_tick);
}
