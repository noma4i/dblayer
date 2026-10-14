use super::database::{started, Starter};
use super::DbError;

#[test]
fn inv_panic_01_an_executor_that_cannot_start_is_an_error_and_a_later_start_can_succeed() {
    let slot = Starter::new();
    match started(&slot, || Err(std::io::Error::other("no threads left"))) {
        Err(DbError::Database { detail }) => assert!(detail.contains("no threads left"), "{detail}"),
        other => panic!("expected a database error, got {:?}", other.err()),
    }
    let runtime = started(&slot, || tokio::runtime::Builder::new_multi_thread().worker_threads(1).build()).expect("the second start succeeds");
    assert_eq!(runtime.block_on(async { 7 }), 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inv_panic_02_two_first_starts_build_one_runtime() {
    static SLOT: Starter = Starter::new();
    static BUILDS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let together = std::sync::Arc::new(std::sync::Barrier::new(2));
    let mut starts = tokio::task::JoinSet::new();
    for _ in 0..2 {
        let together = together.clone();
        starts.spawn(async move {
            together.wait();
            started(&SLOT, || {
                BUILDS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(100));
                tokio::runtime::Builder::new_multi_thread().worker_threads(1).build()
            })
            .map(|_| ())
        });
    }
    while let Some(done) = starts.join_next().await {
        done.unwrap().unwrap();
    }
    assert_eq!(BUILDS.load(std::sync::atomic::Ordering::SeqCst), 1);
}
