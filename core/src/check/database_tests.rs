use super::corpus;
use super::database::DatabaseSut;
use super::model::replay;

/// Requirement: the shared corpus is green on the core through the public API of `Database`.
/// Observable: the observations of each corpus trace. Assert: equal to the recorded ones after every input; the corpus is not empty. Every red trace is listed, not only the first.
#[tokio::test(flavor = "multi_thread")]
async fn the_shared_corpus_is_green_on_the_core() {
    tokio::time::timeout(std::time::Duration::from_secs(300), shared_corpus()).await.expect("the shared corpus did not finish in 300 s");
}

async fn shared_corpus() {
    let traces = corpus::read_dir(&corpus::shared_dir()).unwrap();
    assert!(!traces.is_empty());
    let runs: Vec<_> = traces.into_iter().map(|trace| tokio::spawn(async move {
        let mut real = DatabaseSut::default();
        let result = replay(&mut real, &trace).await.err().map(|divergence| format!("{}: {divergence}", trace.name));
        real.finish().await;
        result
    })).collect();
    let mut red = Vec::new();
    for run in runs {
        red.extend(run.await.expect("a corpus trace did not panic"));
    }
    assert!(red.is_empty(), "red traces:\n{}", red.join("\n"));
}
