use super::corpus;
use super::database::DatabaseSut;
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
