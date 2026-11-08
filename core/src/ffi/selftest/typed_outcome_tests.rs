use crate::ffi::database::{blocking, on_executor};
use crate::ffi::DbError;

/// bridge errors (P-BND-04, D-195): a task of the executor that ends with a panic is a `Runtime` failure of the executor, not a refusal of the caller.
#[tokio::test(flavor = "multi_thread")]
async fn a_task_that_panics_on_the_executor_is_a_runtime_failure() {
    let on = on_executor(async { if std::hint::black_box(true) { panic!("the task failed"); } Ok::<(), DbError>(()) }).await;
    let offloaded = blocking(|| { if std::hint::black_box(true) { panic!("the job failed"); } Ok(()) }).await;
    assert!(matches!(on, Err(DbError::Runtime { .. })), "{on:?}");
    assert!(matches!(offloaded, Err(DbError::Runtime { .. })), "{offloaded:?}");
}
