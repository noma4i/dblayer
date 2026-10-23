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

#[test]
fn inv_panic_02_two_first_starts_build_one_runtime() {
    static SLOT: Starter = Starter::new();
    static BUILDS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let (building_tx, building) = std::sync::mpsc::channel();
    let (entering_tx, entering) = std::sync::mpsc::channel();
    let (release_tx, release) = std::sync::mpsc::channel::<()>();
    let first = std::thread::spawn(move || {
        started(&SLOT, || {
            BUILDS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            building_tx.send(()).unwrap();
            release.recv().unwrap();
            tokio::runtime::Builder::new_multi_thread().worker_threads(1).build()
        })
        .map(|_| ())
    });
    building.recv().unwrap();
    let second = std::thread::spawn(move || {
        entering_tx.send(()).unwrap();
        started(&SLOT, || {
            BUILDS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::runtime::Builder::new_multi_thread().worker_threads(1).build()
        })
        .map(|_| ())
    });
    entering.recv().unwrap();
    release_tx.send(()).unwrap();
    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();
    assert_eq!(BUILDS.load(std::sync::atomic::Ordering::SeqCst), 1);
}
