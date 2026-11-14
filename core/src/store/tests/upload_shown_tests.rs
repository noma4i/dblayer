use crate::owners::contract::BatchAuthor;
use crate::owners::store::{StoreBatch, StoreOp, UploadKind, UploadRow};
use crate::owners::write::transfer::Shown;
use crate::schema::OnFailure;
use crate::store::fixtures::all_specs;
use crate::store::Store;

const KEY: &str = "moments:m1:upload";

fn write(db: &Store, ops: Vec<StoreOp>) {
    let batch = StoreBatch { author: BatchAuthor::Transfer, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), rows: Vec::new(), intents: 0, credential: None, ops };
    db.write_batch(&batch, db.files(), None).unwrap();
}

fn row() -> UploadRow {
    UploadRow {
        key: KEY.into(), seq: 1, source: Some("/source".into()), parts: 3, batch: None, fields: None, upload_id: None, confirmed_parts: 1, progress: 0, weight: None, weighed_prepare: false, batch_progress: 0, attempts: 2, completed: false, failed: None,
        created_at: 1, inline: false, size: 9, sha256: "hash".into(), path: "/mutation".into(), body: "{}".into(), on_failure: OnFailure::Keep, steps_path: None, steps_body: None,
        transfer_id: 2, cycle_id: 1, signed_id: None, aborted: false, staged: false, failed_at: None, blob_created_at: None, held: false, op_id: None, etags: vec![Some("etag".into())], prepare: None, max_bytes: None,
    }
}

fn columns(db: &Store) -> (String, String, Option<String>, i64, i64, i64, Option<String>) {
    db.lock().connection.query_row("SELECT status, stage, wait, sent, total, attempts, error_kind FROM sync_upload WHERE model = 'moments' AND row_id = 'm1' AND action = 'upload'", [], |found| Ok((found.get(0)?, found.get(1)?, found.get(2)?, found.get(3)?, found.get(4)?, found.get(5)?, found.get(6)?))).unwrap()
}

fn shown(shown: Shown, sent: u32, total: u32, attempts: u32) -> StoreOp { StoreOp::UploadShown { key: KEY.into(), batch: 0, error: if let Shown::Failed { error } = &shown { Some(error.clone()) } else { None }, shown, sent, total, attempts, virtual_transfer: None } }

#[test]
fn a_shown_value_writes_the_whole_shown_columns_and_keeps_the_durable_ones() {
    let db = Store::open_models(None, all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    write(&db, vec![StoreOp::Upload { row: row(), kind: UploadKind::SyncUpload }]);
    assert_eq!(columns(&db), ("queued".into(), "prepare".into(), None, 1, 3, 2, None));
    write(&db, vec![shown(Shown::Uploading { progress: 66 }, 2, 3, 3)]);
    assert_eq!(columns(&db), ("uploading".into(), "part".into(), None, 2, 3, 3, None), "the counters of the shown value are written with it");
    write(&db, vec![shown(Shown::WaitingSource, 2, 3, 3)]);
    assert_eq!(columns(&db), ("waiting".into(), "prepare".into(), Some("source_pending".into()), 2, 3, 3, None));
    write(&db, vec![shown(Shown::Failed { error: "missing_file".into() }, 2, 3, 4)]);
    assert_eq!(columns(&db), ("failed".into(), "prepare".into(), None, 2, 3, 4, Some("missing_file".into())), "the error and the attempts of a failed value are written with it");
    write(&db, vec![shown(Shown::Gone, 0, 0, 0)]);
    assert_eq!(columns(&db).0, "failed", "a gone transfer writes nothing");
}

#[test]
fn the_staged_marker_is_written_with_the_row_and_cleared_by_its_own_op() {
    let db = Store::open_models(None, all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let staged = |db: &Store| db.lock().connection.query_row("SELECT COALESCE(staged, 0) FROM sync_upload WHERE model = 'moments' AND row_id = 'm1' AND action = 'upload'", [], |found| found.get::<_, i64>(0)).unwrap();
    write(&db, vec![StoreOp::Upload { row: UploadRow { staged: true, ..row() }, kind: UploadKind::SyncUpload }]);
    assert_eq!(staged(&db), 1);
    write(&db, vec![StoreOp::UploadStaged { key: KEY.into() }]);
    assert_eq!(staged(&db), 0);
}

/// D-427: an upload whose cycle has a written outcome shows that outcome in the batch of the outcome, before the transfer writes its own row: failed and rejected show failed with the error, confirmed and discarded hide the upload.
#[test]
fn the_outcome_of_a_cycle_is_shown_before_the_transfer_writes_its_row() {
    use crate::owners::write::outbox::OutboxOutcome;
    let cases = [
        (OutboxOutcome::Failed { error: "boom".into() }, Some(("failed".to_string(), "boom".to_string()))),
        (OutboxOutcome::Rejected { error: "no".into() }, Some(("failed".to_string(), "no".to_string()))),
        (OutboxOutcome::Confirmed { revision: 0, result: None, row_id: None }, None),
        (OutboxOutcome::Discarded, None),
    ];
    for (outcome, expected) in cases {
        let db = Store::open_models(None, all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
        crate::store::fixtures::put(&db, "moments", serde_json::json!({"id": "m1", "userId": null}));
        write(&db, vec![StoreOp::Upload { row: row(), kind: UploadKind::SyncUpload }, shown(Shown::Uploading { progress: 66 }, 2, 3, 3)]);
        let notices = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = notices.clone();
        db.on_commit(move |notice| sink.lock().unwrap().push(notice)).unwrap();
        let batch = StoreBatch { author: BatchAuthor::Outbox, id: 2, stamp: crate::runtime::EpochStamp::Epoch(1), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::TransferOutcome { transfer_id: 2, cycle_id: 1, outcome: outcome.clone() }] };
        db.write_batch(&batch, db.files(), None).unwrap();
        let sync = crate::store::sync_state::read_sync(&db.lock().connection, "moments", &["m1".to_string()]).unwrap();
        let shown = sync.get("m1").and_then(|info| info.uploads.get("upload")).map(|info| (format!("{:?}", info.status).to_lowercase(), info.error.as_ref().map(|error| error.message.clone()).unwrap_or_default()));
        assert_eq!(shown, expected, "{outcome:?}");
        let notice = notices.lock().unwrap().pop().expect("the batch of the outcome announces a commit");
        let announced = (notice.rows.unwrap_or_default().iter().any(|(_, id)| id == "m1"), notice.tables.iter().any(|table| table == crate::store::operation::QUEUE_TABLE));
        assert_eq!(announced, (true, true), "{outcome:?}: the row and the queue of the upload are announced");
    }
}

/// R12, P-DATA-106 (D-460): a batch whose SQL changes nothing moves no revision and announces nothing: a stale mark of a model without windows, a family mark without windows.
#[test]
fn a_batch_that_changes_nothing_moves_no_revision_and_announces_nothing() {
    let db = Store::open_models(None, all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    crate::store::fixtures::put(&db, "moments", serde_json::json!({"id": "m1", "userId": null}));
    let notices = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = notices.clone();
    db.on_commit(move |notice| sink.lock().unwrap().push(notice)).unwrap();
    let cases = [
        ("a stale mark of a model without windows", vec![StoreOp::MarkStale { models: vec!["moments".into()], keys: Vec::new(), kept: Vec::new() }]),
        ("a family mark without windows", vec![StoreOp::StaleFamilies { families: vec![("moments".into(), "feed".into())] }]),
    ];
    for (name, ops) in cases {
        let before = db.published_revision().load(std::sync::atomic::Ordering::SeqCst);
        let batch = StoreBatch { author: BatchAuthor::Feed, id: 9, stamp: crate::runtime::EpochStamp::Epoch(1), rows: Vec::new(), intents: 0, credential: None, ops };
        let written = db.write_batch(&batch, db.files(), None).unwrap();
        assert_eq!((i64::try_from(written.revision).unwrap(), db.published_revision().load(std::sync::atomic::Ordering::SeqCst)), (before, before), "{name}: no revision moves");
    }
    assert_eq!(notices.lock().unwrap().len(), 0, "a batch that changed nothing wakes nobody");
}

/// D-460, P-DATA-106: a transfer shown without its row (a restored failed transfer) changes what the queue reads, so its batch moves the revision; the same show again changes nothing; `Gone` takes it away.
#[test]
fn a_shown_transfer_without_a_row_is_read_at_its_revision() {
    let db = Store::open_models(None, all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let show = |shown: Shown| StoreBatch { author: BatchAuthor::Transfer, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::UploadShown { key: KEY.into(), batch: 0, error: if let Shown::Failed { error } = &shown { Some(error.clone()) } else { None }, shown, sent: 0, total: 0, attempts: 0, virtual_transfer: Some((2, Some(5))) }] };
    let failed = || Shown::Failed { error: "RestoreFailed".into() };
    let queued = |floor: u64| db.read_queue(i64::try_from(floor).unwrap(), (0, 0), &crate::store::fixtures::operations_view()).unwrap().2.into_iter().map(|transfer| (transfer.transfer_id, transfer.restore_error)).collect::<Vec<_>>();
    let before = db.published_revision().load(std::sync::atomic::Ordering::SeqCst);
    let shown = db.write_batch(&show(failed()), db.files(), None).unwrap();
    assert_eq!((i64::try_from(shown.revision).unwrap(), queued(shown.revision)), (before + 1, vec![(2, Some("RestoreFailed".to_string()))]), "the shown transfer is read at the revision of its batch");
    let again = db.write_batch(&show(failed()), db.files(), None).unwrap();
    assert_eq!(again.revision, shown.revision, "the same show changes nothing");
    let gone = db.write_batch(&show(Shown::Gone), db.files(), None).unwrap();
    assert_eq!((gone.revision, queued(gone.revision)), (shown.revision + 1, Vec::new()), "Gone takes the transfer away at a new revision");
}
