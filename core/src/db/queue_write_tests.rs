use serde_json::json;

use super::fixtures::*;
use crate::owners::store::{EffectRow, IntentEffects, StoreOp, UploadKind, UploadRow};
use crate::schema::OnFailure;

fn intent(op_id: u64, values: serde_json::Value) -> StoreOp {
    let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: "m1".into(), values: values.to_string() }], ..Default::default() };
    StoreOp::Intent { op_id, model: "moments".into(), row_id: "m1".into(), action: "update".into(), payload: "{}".into(), effects }
}

fn field(db: &super::Db, name: &str) -> Option<String> { db.find("moments", &rid("m1")).unwrap().unwrap().text(name).map(str::to_string) }

/// out_rollback_committed (D-30): the rollback of the first of two operations of a row gives back only the fields the row still owns.
#[test]
fn a_rollback_gives_back_only_the_fields_the_row_still_owns() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u0", "text": "t0"}));
    db.write_queue_ops(&[intent(1, json!({"id": "m1", "userId": "u1", "text": "a"}))]).unwrap();
    db.write_queue_ops(&[intent(2, json!({"id": "m1", "userId": "u1", "text": "b"}))]).unwrap();
    db.write_queue_ops(&[StoreOp::RollBack { op_id: 1 }, StoreOp::RemoveIntent { op_id: 1 }]).unwrap();
    assert_eq!(field(&db, "text").as_deref(), Some("b"), "the field written later by the second operation stays");
    assert_eq!(field(&db, "userId").as_deref(), Some("u0"), "the field only the first operation wrote goes back");
    db.write_queue_ops(&[StoreOp::RollBack { op_id: 2 }, StoreOp::RemoveIntent { op_id: 2 }]).unwrap();
    assert_eq!(field(&db, "text").as_deref(), Some("t0"), "the second rollback gives back the value from before the first operation");
    assert_eq!(field(&db, "userId").as_deref(), Some("u0"));
}

/// xfer_open_* (D-26): the base keeps the whole value of a transfer and `read_transfers` gives it back.
#[test]
fn the_base_keeps_the_whole_value_of_a_transfer() {
    let db = booted();
    let row = UploadRow {
        key: "moments:m1:upload".into(),
        seq: 7,
        source: "/src/video.mov".into(),
        parts: 4,
        batch: true,
        upload_id: Some("u-1".into()),
        confirmed_parts: 2,
        completed: false,
        failed: None,
        created_at: 1234,
        inline: false,
        size: 4096,
        sha256: "abc".into(),
        path: "/moments".into(),
        body: "{\"a\":1}".into(),
        on_failure: OnFailure::Keep,
        steps_path: "/presign".into(),
        steps_body: "{\"b\":2}".into(),
    };
    db.write_queue_ops(&[StoreOp::Upload { row, kind: UploadKind::SyncUpload }]).unwrap();
    let stored = db.read_transfers().unwrap();
    assert_eq!(stored.len(), 1);
    let transfer = &stored[0];
    assert_eq!((transfer.key.as_str(), transfer.seq, transfer.source.path.as_str(), transfer.source.parts), ("moments:m1:upload", 7, "/src/video.mov", 4));
    assert_eq!((transfer.batch, transfer.upload_id.as_deref(), transfer.confirmed_parts, transfer.completed, transfer.failed.as_deref()), (true, Some("u-1"), 2, false, None));
    assert_eq!((transfer.created_at.0, transfer.size, transfer.sha256.as_str()), (1234, 4096, "abc"));
    assert_eq!((transfer.mutation.path.as_str(), transfer.mutation.body.as_str(), transfer.mutation.on_failure), ("/moments", "{\"a\":1}", OnFailure::Keep));
    assert_eq!((transfer.steps.path.as_str(), transfer.steps.body.as_str()), ("/presign", "{\"b\":2}"));
    assert!(!transfer.file_present);
}
