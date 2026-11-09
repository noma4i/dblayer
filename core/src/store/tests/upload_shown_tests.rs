use crate::owners::contract::BatchAuthor;
use crate::owners::store::{StoreBatch, StoreOp, UploadKind, UploadRow};
use crate::owners::write::transfer::Shown;
use crate::schema::OnFailure;
use crate::store::fixtures::all_specs;
use crate::store::Store;

const KEY: &str = "moments:m1:upload";

fn write(db: &Store, ops: Vec<StoreOp>) {
    let batch = StoreBatch { author: BatchAuthor::Transfer, id: 1, epoch: 1, basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
}

fn row() -> UploadRow {
    UploadRow {
        key: KEY.into(), seq: 1, source: Some("/source".into()), parts: 3, batch: None, fields: None, upload_id: None, confirmed_parts: 1, attempts: 2, completed: false, failed: None,
        created_at: 1, inline: false, size: 9, sha256: "hash".into(), path: "/mutation".into(), body: "{}".into(), on_failure: OnFailure::Keep, steps_path: None, steps_body: None,
        transfer_id: 2, cycle_id: 1, signed_id: None, aborted: false, staged: false, failed_at: None, blob_created_at: None, op_id: None, etags: vec![Some("etag".into())],
    }
}

fn columns(db: &Store) -> (String, String, Option<String>, i64, i64, i64, Option<String>) {
    db.lock().connection.query_row("SELECT status, stage, wait, sent, total, attempts, error_kind FROM sync_upload WHERE model = 'moments' AND row_id = 'm1' AND action = 'upload'", [], |found| Ok((found.get(0)?, found.get(1)?, found.get(2)?, found.get(3)?, found.get(4)?, found.get(5)?, found.get(6)?))).unwrap()
}

fn shown(shown: Shown, sent: u32, total: u32, attempts: u32) -> StoreOp { StoreOp::UploadShown { key: KEY.into(), error: if let Shown::Failed { error } = &shown { Some(error.clone()) } else { None }, shown, sent, total, attempts } }

#[test]
fn a_shown_value_writes_the_whole_shown_columns_and_keeps_the_durable_ones() {
    let db = Store::open_models(None, all_specs(), None).unwrap();
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
    let db = Store::open_models(None, all_specs(), None).unwrap();
    let staged = |db: &Store| db.lock().connection.query_row("SELECT COALESCE(staged, 0) FROM sync_upload WHERE model = 'moments' AND row_id = 'm1' AND action = 'upload'", [], |found| found.get::<_, i64>(0)).unwrap();
    write(&db, vec![StoreOp::Upload { row: UploadRow { staged: true, ..row() }, kind: UploadKind::SyncUpload }]);
    assert_eq!(staged(&db), 1);
    write(&db, vec![StoreOp::UploadStaged { key: KEY.into() }]);
    assert_eq!(staged(&db), 0);
}
