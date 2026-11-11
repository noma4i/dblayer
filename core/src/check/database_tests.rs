use super::corpus;
use super::database::{error_kind, DatabaseSut};
use crate::ffi::DbError;
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

/// Requirement: a call before any open is `NotOpen`, a different corpus kind from `Closed`.
/// Observable: the corpus error kind of each variant.
#[test]
fn life_call_not_open() {
    assert_eq!(error_kind(&DbError::NotOpen { detail: String::new() }), "not_open");
    assert_eq!(error_kind(&DbError::Closed { detail: String::new() }), "closed");
}

/// Requirement: the corpus error kind is the snake_case name of the `DbError` variant.
/// Observable: the kind of each variant the cells name.
#[test]
fn error_kind_is_the_snake_case_name_of_the_variant() {
    let detail = String::new;
    let table = [
        (DbError::Offline { detail: detail() }, "offline"),
        (DbError::Dropped { detail: detail() }, "dropped"),
        (DbError::Unauthorized { detail: detail() }, "unauthorized"),
        (DbError::InFlight { detail: detail() }, "in_flight"),
        (DbError::Storage { detail: detail() }, "storage"),
        (DbError::Runtime { detail: detail() }, "runtime"),
        (DbError::Internal { detail: detail() }, "internal"),
        (DbError::Copy { detail: detail() }, "copy"),
        (DbError::Http { status: 401, detail: detail() }, "http"),
    ];
    for (error, kind) in table {
        assert_eq!(error_kind(&error), kind, "{error:?}");
    }
}

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

/// Requirement: the symbolic stand shows the value of a stream as library.md section 1 names it: rows, `phase`, `loaded` present, no revision, no `subscription` request.
/// Observable: the `pull` after the open of a feed list, after the page and the second `pull`. Assert: the exact observations.
#[tokio::test(flavor = "multi_thread")]
async fn the_symbolic_stand_shows_stream_values_without_revisions() {
    let head = [Input::new("open").with("encrypted", true), Input::new("set_online").with("online", true), Input::new("login"), Input::new("open_list").with("as", "s1").with("relation", "feed")];
    let pull = Input::new("pull").with("stream", "s1");
    let page = Input::new("answer_page").with("rows", 3u64).with("more", false);
    let table = [
        ("opened and loading", vec![pull.clone()], json!([{ "o": "snapshot", "v": { "phase": "loading", "rows": [], "stream": "s1" } }])),
        ("landed", vec![page.clone(), pull.clone()], json!([{ "o": "snapshot", "v": { "loaded": true, "phase": "loaded", "rows": ["m1", "m2", "m3"], "stream": "s1" } }])),
        ("nothing new", vec![page, pull.clone(), pull.clone()], json!([{ "o": "empty", "v": { "stream": "s1" } }])),
    ];
    for (name, body, expected) in table {
        let mut real = DatabaseSut::symbolic();
        let mut shown = Vec::new();
        for input in head.iter().chain(body.iter()) {
            shown = real.apply(input).await;
            assert!(shown.iter().all(|item| item.kind != "request" || item.value["kind"] != "subscription"), "{name}: a subscription request is shown: {shown:?}");
        }
        assert_eq!(serde_json::to_value(&shown).unwrap(), expected, "{name}");
        real.finish().await;
    }
}
