use crate::ffi::{Database, DbError, WireConfirmed, WireOperationReceipt};

/// K7.07, K7.11, K7.12 (P-BND-04, D-195): every refusal of the transport keeps its type on the way to the caller; a storage or an internal failure is no network failure and a decode failure is `Invalid`.
#[test]
fn boundary_failure_orders() {
    use crate::transport::TransportError;
    let cases = [
        (TransportError::Internal("the answer of the session was lost".into()), "Internal"),
        (TransportError::Storage("session storage: disk".into()), "Storage"),
        (TransportError::Decode("not json".into()), "Invalid"),
        (TransportError::Interrupted("ended".into()), "Interrupted"),
        (TransportError::Network("offline".into()), "Network"),
    ];
    for (error, kind) in cases {
        let reached = DbError::from(error.clone());
        assert_eq!(super::cases::error_kind(&reached), kind, "{error:?} reached the caller as {reached:?}");
    }
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

/// The owner of the call holds it, then loses its answer while the epoch of the caller ends: the call ends as the answer the caller lost.
async fn outcome_whose_answer_is_lost(database: &Database, receipt: WireOperationReceipt) -> Result<WireConfirmed, DbError> {
    use crate::runtime::{Cause, OutcomeId};
    let outcomes = database.life.outcomes();
    let epoch = database.session_epoch();
    let probe = outcomes.register(epoch);
    let lost = OutcomeId::from_raw(probe.id().raw() + 1);
    outcomes.forget(probe.id());
    let held = outcomes.open_count();
    let mut call = Box::pin(database.confirmed(receipt, false));
    tokio::select! { biased; _ = &mut call => panic!("the owner answered at once"), () = std::future::ready(()) => {} }
    let life = database.life_handle();
    crate::test_wait::within(tokio::task::spawn_blocking(move || life.barrier())).await.unwrap();
    assert_eq!(outcomes.open_count(), held + 1, "the owner holds the call");
    outcomes.forget(lost);
    outcomes.end_epoch(epoch, Cause::SessionReset);
    super::rig_tests::bound(call).await
}

/// K7.09, K7.10 (P-BND-04, D-195): the outcome of a write or of a transfer whose owner lost the answer is `Internal` when the epoch of the caller ended meanwhile, not `Interrupted`.
#[tokio::test(flavor = "multi_thread")]
async fn the_outcome_of_a_write_or_an_upload_with_a_lost_answer_is_internal() {
    use super::rig_tests::bound;
    use super::upload_stand::{plain, request, Stand, CREATE_MOMENT};
    for upload in [false, true] {
        let stand = Stand::new();
        let database = stand.open(None, &[CREATE_MOMENT]).await;
        database.set_online(false).await.unwrap();
        let epoch = database.session_epoch();
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("intro.mp4");
        std::fs::write(&source, b"video").unwrap();
        let local = bound(database.send(plain("Moment", r#"{"text":"intro"}"#, epoch))).await.unwrap();
        let receipt = if upload {
            bound(database.upload(epoch, "Moment".into(), local.optimistic_value.unwrap().id, "upload_clip".into(), request(&source))).await.unwrap()
        } else {
            local
        };

        let answer = outcome_whose_answer_is_lost(&database, receipt).await;

        assert!(matches!(answer, Err(DbError::Internal { .. })), "upload {upload}: {answer:?}");
        database.close_now();
    }
}
