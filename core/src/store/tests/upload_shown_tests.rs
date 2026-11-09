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
        transfer_id: 2, cycle_id: 1, signed_id: None, aborted: false, op_id: None, etags: vec![Some("etag".into())],
    }
}

fn columns(db: &Store) -> (String, String, Option<String>, i64, i64) {
    db.lock().connection.query_row("SELECT status, stage, wait, sent, attempts FROM sync_upload WHERE model = 'moments' AND row_id = 'm1' AND action = 'upload'", [], |found| Ok((found.get(0)?, found.get(1)?, found.get(2)?, found.get(3)?, found.get(4)?))).unwrap()
}

#[test]
fn a_shown_value_writes_status_stage_and_wait_and_keeps_the_durable_columns() {
    let db = Store::open_models(None, all_specs(), None).unwrap();
    write(&db, vec![StoreOp::Upload { row: row(), kind: UploadKind::SyncUpload }]);
    assert_eq!(columns(&db), ("queued".into(), "prepare".into(), None, 1, 2));
    write(&db, vec![StoreOp::UploadShown { key: KEY.into(), shown: Shown::Uploading { progress: 33 } }]);
    assert_eq!(columns(&db), ("uploading".into(), "part".into(), None, 1, 2));
    write(&db, vec![StoreOp::UploadShown { key: KEY.into(), shown: Shown::WaitingSource }]);
    assert_eq!(columns(&db), ("waiting".into(), "prepare".into(), Some("source_pending".into()), 1, 2));
    write(&db, vec![StoreOp::UploadShown { key: KEY.into(), shown: Shown::Failed { error: "refused".into() } }]);
    assert_eq!(columns(&db), ("failed".into(), "prepare".into(), None, 1, 2), "the wait goes with the next value and the stage stays");
    write(&db, vec![StoreOp::UploadShown { key: KEY.into(), shown: Shown::Gone }]);
    assert_eq!(columns(&db).0, "failed", "a gone transfer writes nothing");
}
