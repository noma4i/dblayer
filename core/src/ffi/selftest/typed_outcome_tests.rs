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

/// K7.07, K7.11, K7.12 (P-BND-04, D-195): every refusal of the transport keeps its type on the way to the caller; a storage or an internal failure is no network failure and a decode failure is `Invalid`.
#[test]
fn a_transport_refusal_reaches_the_caller_with_its_own_type() {
    use crate::transport::TransportError;
    let cases = [
        (TransportError::Internal("the answer of the session was lost".into()), "Internal"),
        (TransportError::Storage("session storage: disk".into()), "Storage"),
        (TransportError::Decode("not json".into()), "Invalid"),
        (TransportError::Interrupted("ended".into()), "Interrupted"),
        (TransportError::Network("offline".into()), "Network"),
    ];
    for (error, kind) in cases {
        let shown = format!("{:?}", DbError::from(error.clone()));
        assert!(shown.starts_with(kind), "{error:?} reached the caller as {shown}");
    }
}

/// K7.07, K7.09, K7.10: an owner whose answer was lost is `Internal` for a caller whose epoch ended meanwhile; no arm turns it into `Interrupted`.
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_answer_of_an_owner_is_internal_even_after_the_epoch_ended() {
    use super::upload_stand::Stand;
    let stand = Stand::new();
    let database = stand.open(None, &[]).await;
    let outcomes = crate::runtime::Outcomes::<crate::owners::life::LifeReply>::new();
    let waiter = outcomes.register(database.session_epoch());
    outcomes.forget(waiter.id());

    let answer = database.await_life(waiter).await;

    assert!(matches!(answer, Err(DbError::Internal { .. })), "{answer:?}");
    database.close_now();
}

/// K7.13-15 (P-BND-04, D-195): a task of the executor that was aborted ends `Interrupted`, one that panicked ends `Runtime`.
#[tokio::test(flavor = "multi_thread")]
async fn an_aborted_task_is_interrupted_and_a_panicked_task_is_a_runtime_failure() {
    let aborted = tokio::spawn(std::future::pending::<()>());
    aborted.abort();
    let cancelled = super::super::database::joined(aborted.await.unwrap_err());
    let panicked = tokio::spawn(async { if std::hint::black_box(true) { panic!("the task failed"); } });
    let failed = super::super::database::joined(panicked.await.unwrap_err());
    assert!(matches!(cancelled, DbError::Interrupted { .. }), "{cancelled:?}");
    assert!(matches!(failed, DbError::Runtime { .. }), "{failed:?}");
}
