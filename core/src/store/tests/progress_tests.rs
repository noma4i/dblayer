use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::json;

use crate::owners::contract::BatchAuthor;
use crate::owners::store::{EffectRow, IntentEffects, IntentOrigin, StoreBatch, StoreOp, UploadKind, UploadRow};
use crate::schema::OnFailure;
use crate::store::fixtures::{all_specs, put};
use crate::store::Store;

fn write(db: &Store, author: BatchAuthor, ops: Vec<StoreOp>) {
    let batch = StoreBatch { author, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
}

fn upload(index: u64, confirmed: u32) -> UploadRow {
    UploadRow {
        key: format!("moments:m{index}:upload"), seq: 100 + index, source: Some("/source".into()), parts: 3,
        batch: None, fields: None, upload_id: Some("upload".into()), confirmed_parts: confirmed, attempts: 0,
        completed: false, failed: None, created_at: 1, inline: false,
        size: 9, sha256: "hash".into(), path: "/mutation".into(), body: "{}".into(),
        on_failure: OnFailure::Keep, steps_path: Some("/steps".into()), steps_body: Some("{}".into()),
        transfer_id: index + 1, cycle_id: 1, signed_id: None, aborted: false, staged: false, failed_at: None, blob_created_at: None,
        op_id: Some(100 + index), etags: vec![Some("etag".into()); confirmed as usize],
    }
}

fn steps_of_one_confirmed_part(open: u64) -> usize {
    let db = Arc::new(Store::open_models(None, all_specs(), None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    for index in 0..open {
        let row = format!("m{index}");
        put(&db, "moments", json!({"id": row, "userId": "u1", "text": "old"}));
        write(&db, BatchAuthor::Transfer, vec![StoreOp::Upload { row: upload(index, 0), kind: UploadKind::SyncUpload }]);
        let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: row.clone(), values: json!({"id": row, "userId": "u1", "text": "edit"}).to_string() }], ..Default::default() };
        write(&db, BatchAuthor::Outbox, vec![StoreOp::Intent { attempts: 1, cycle_id: 1, op_id: 100 + index, model: "moments".into(), row_id: row, action: "update".into(), payload: "{}".into(), effects, origin: IntentOrigin::Transfer { transfer_id: index + 1 } }]);
    }
    let steps = Arc::new(AtomicUsize::new(0));
    let counter = steps.clone();
    db.lock().connection.progress_handler(1, Some(move || { counter.fetch_add(1, Ordering::SeqCst); false })).unwrap();
    write(&db, BatchAuthor::Transfer, vec![StoreOp::Upload { row: upload(0, 1), kind: UploadKind::ConfirmedParts }]);
    db.lock().connection.progress_handler(0, None::<fn() -> bool>).unwrap();
    steps.load(Ordering::SeqCst)
}

#[test]
fn inv_sync_05_the_work_of_a_progress_tick_does_not_grow_with_the_number_of_open_operations() {
    let (small, large) = (steps_of_one_confirmed_part(10), steps_of_one_confirmed_part(100));
    assert!(small > 0, "the tick runs SQL");
    assert_eq!(small, large, "the steps of one confirmed part at 10 and at 100 open operations");
}
