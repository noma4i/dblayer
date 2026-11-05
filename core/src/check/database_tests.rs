use super::corpus;
use super::database::DatabaseSut;
use super::model::replay;

/// Requirement: the shared corpus is green on the core through the public API of `Database`.
/// Observable: the observations of each corpus trace. Assert: equal to the recorded ones after every input; the corpus is not empty.
#[tokio::test(flavor = "multi_thread")]
async fn the_shared_corpus_is_green_on_the_core() {
    let traces = corpus::read_dir(&corpus::shared_dir()).unwrap();
    assert!(!traces.is_empty());
    for trace in traces {
        let mut real = DatabaseSut::default();
        if let Err(divergence) = replay(&mut real, &trace).await {
            panic!("{}: {divergence}", trace.name);
        }
    }
}
