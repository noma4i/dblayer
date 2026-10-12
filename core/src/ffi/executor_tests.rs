use std::sync::OnceLock;

use super::database::started;
use super::DbError;

#[test]
fn inv_panic_01_an_executor_that_cannot_start_is_an_error_and_a_later_start_can_succeed() {
    let slot = OnceLock::new();
    match started(&slot, || Err(std::io::Error::other("no threads left"))) {
        Err(DbError::Database { detail }) => assert!(detail.contains("no threads left"), "{detail}"),
        other => panic!("expected a database error, got {:?}", other.err()),
    }
    assert!(slot.get().is_none());
    let runtime = started(&slot, || tokio::runtime::Builder::new_multi_thread().worker_threads(1).build()).expect("the second start succeeds");
    assert_eq!(runtime.block_on(async { 7 }), 7);
}
