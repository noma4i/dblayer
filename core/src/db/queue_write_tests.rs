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
        transfer_id: 9,
        cycle_id: 2,
        signed_id: Some("sig".into()),
        aborted: true,
        etags: vec![Some("e1".into()), None],
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
    assert_eq!((transfer.transfer_id, transfer.cycle_id, transfer.signed_id.as_deref(), transfer.aborted, transfer.etags.clone()), (9, 2, Some("sig"), true, vec![Some("e1".to_string()), None]));
    assert!(!transfer.file_present);
}

pub(super) fn paged_moments() -> super::Db {
    let mut moments = moments_spec();
    let mut mine = crate::schema::RelationSpec::new("mine", &["userId"]);
    mine.remote = Some(crate::schema::RemoteSpec {
        shape: crate::schema::RemoteShape::Connection { direction: crate::schema::PageDirection::Forward, coverage: crate::schema::Coverage::Page, max_pages: None, cursor: None },
        operation: "Mine".into(),
        stale_time: "1.minute".into(),
        empty_stale_time: None,
        selects_page_info: true,
        page_size: None,
        variables: Default::default(),
        root: None,
        total_from: None,
    });
    moments.relations.push(mine);
    let specs = all_specs().into_iter().map(|spec| if spec.key.as_str() == "moments" { moments.clone() } else { spec }).collect();
    let db = super::Db::open_in_memory(specs, None).unwrap();
    db.set_clock(std::sync::Arc::new(crate::pacing::ManualClock::new(std::time::Duration::from_millis(1_000_000))));
    db
}

pub(super) fn page(key: &crate::owners::read::windows::WindowKey, number: u32, ids: &[&str], cursor: Option<&str>) -> StoreOp {
    let roots = ids.iter().map(|id| crate::owners::store::RowValue { row_id: (*id).into(), values: json!({"id": id, "userId": "u1", "text": id}).to_string() }).collect();
    StoreOp::Window { key: key.name.clone(), model: key.model.clone(), page: number, replace_all: false, cursor: cursor.map(str::to_string), roots, related: Vec::new(), total: None, base_revision: 0 }
}

/// win_landing_committed (P-WIN-02): a landed page puts its rows into the membership of the scope in order and the window record keeps the cursor and the page count.
#[test]
fn a_landed_page_gives_rows_membership_and_the_window_record() {
    let db = paged_moments();
    let key = crate::owners::read::windows::WindowKey::relation("moments", "mine", &obj(json!({"userId": "u1"})));
    db.write_queue_ops(&[page(&key, 1, &["m1", "m2"], Some("c1"))]).unwrap();
    db.write_queue_ops(&[page(&key, 2, &["m3"], None)]).unwrap();
    let scope = db.spec("moments").unwrap().selection_key("mine", &obj(json!({"userId": "u1"}))).unwrap();
    let members = db.shared.store.read(&["moments"], |reader| Ok(reader.scope_members("moments", scope.as_str())?)).unwrap();
    assert_eq!(members.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(), ["m1", "m2", "m3"]);
    let window = db.shared.store.window(&crate::store::window_key(scope.as_str(), crate::schema::PageDirection::Forward)).unwrap().unwrap();
    assert_eq!((window.cursor.as_deref(), window.has_next, window.pages), (None, false, 2));
}

/// out_land_swap: a landing batch with `Swap` replaces the temporary id by the real one in the commit of the answer rows.
#[test]
fn a_landing_with_a_swap_replaces_the_temporary_row_by_the_real_one() {
    let db = booted();
    let temp = crate::identity::RowId::generate_temp(None);
    let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: temp.to_string(), values: json!({"id": temp.to_string(), "userId": "u0", "text": "a"}).to_string() }], ..Default::default() };
    db.write_queue_ops(&[StoreOp::Intent { op_id: 1, model: "moments".into(), row_id: temp.to_string(), action: "create".into(), payload: "{}".into(), effects }]).unwrap();
    assert!(db.find("moments", &temp).unwrap().is_some());
    let real = crate::owners::store::RowValue { row_id: "m9".into(), values: json!({"id": "m9", "userId": "u0", "text": "a"}).to_string() };
    db.write_queue_ops(&[
        StoreOp::Rows { model: "moments".into(), rows: vec![real] },
        StoreOp::Swap { model: "moments".into(), temp: temp.to_string(), real: "m9".into() },
        StoreOp::RemoveIntent { op_id: 1 },
    ]).unwrap();
    assert!(db.find("moments", &temp).unwrap().is_none(), "the temporary row is gone");
    assert_eq!(db.find("moments", &rid("m9")).unwrap().unwrap().text("text"), Some("a"));
}
