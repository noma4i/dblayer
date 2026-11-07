use super::corpus;
use super::database::{error_kind, DatabaseSut};
use crate::ffi::DbError;
use super::model::replay;

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
