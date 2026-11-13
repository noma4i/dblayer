use std::sync::Arc;

use serde_json::{json, Value};

use crate::owners::contract::BatchAuthor;
use crate::owners::read::windows::WindowKey;
use crate::owners::store::{RowValue, StoreBatch, StoreOp};
use crate::store::fixtures::{all_specs, id, put, row};
use crate::store::Store;

fn write(db: &Arc<Store>, author: BatchAuthor, rows: Vec<(String, String)>, ops: Vec<StoreOp>) -> Result<super::batch_write::BatchWritten, super::StoreError> {
    let batch = StoreBatch { author, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows, intents: 0, credential: None, ops };
    db.write_batch(&batch, db.files(), None)
}

fn present(db: &Store, model: &str, id: &str) -> bool {
    db.read(&[model], |reader| reader.point(model, id)).unwrap().is_some()
}

fn count(db: &Store, sql: &str) -> i64 {
    db.lock().connection.query_row(sql, [], |row| row.get(0)).unwrap()
}

#[test]
fn store_prop_cascade_no_orphans() {
    for via_event in [false, true] {
        let db = Arc::new(Store::open_models(None, all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
        put(&db, "users", json!({"id": "u1", "name": "Ann"}));
        put(&db, "users", json!({"id": "u2", "name": "Bob"}));
        put(&db, "moments", json!({"id": "m1", "userId": "u1"}));
        put(&db, "moments", json!({"id": "m2", "userId": "u1"}));
        put(&db, "moments", json!({"id": "m3", "userId": "u2"}));
        put(&db, "vibes", json!({"id": "v1", "momentId": "m1"}));
        put(&db, "vibes", json!({"id": "v3", "momentId": "m3"}));
        if via_event {
            write(&db, BatchAuthor::Feed, vec![("users".into(), json!({"op": "delete", "id": "u1"}).to_string())], Vec::new()).unwrap();
        } else {
            write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveRows { model: "users".into(), ids: vec!["u1".into()] }]).unwrap();
        }
        let left: Vec<bool> = [("users", "u1"), ("moments", "m1"), ("moments", "m2"), ("vibes", "v1")].iter().map(|(model, id)| present(&db, model, id)).collect();
        assert_eq!(left, [false; 4], "via_event={via_event}");
        let kept: Vec<bool> = [("users", "u2"), ("moments", "m3"), ("vibes", "v3")].iter().map(|(model, id)| present(&db, model, id)).collect();
        assert_eq!(kept, [true; 3], "via_event={via_event}");
    }
}

fn row_with_file(db: &Arc<Store>, bytes: Vec<u8>) {
    let row = RowValue { row_id: "m1".into(), values: json!({"id": "m1", "userId": "u1"}).to_string() };
    write(db, BatchAuthor::LocalWrites, Vec::new(), vec![
        StoreOp::LocalRows { model: "moments".into(), rows: vec![row] },
        StoreOp::AttachFile { model: "moments".into(), row_id: "m1".into(), field: "media".into(), bytes, name: "a.png".into(), mime: "image/png".into() },
    ]).unwrap();
}

#[test]
fn store_erased_file_orphans() {
    let db = Arc::new(Store::open_models(None, all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    row_with_file(&db, vec![1, 2, 3]);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM file"), 1);
    db.erase_account(db.generation().next(), false).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM file"), 0);
}

#[test]
fn store_reset_models_removes_unheld_file() {
    for size in [64, 300_000] {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("data.sqlite");
        let db = Arc::new(Store::open_models(Some(&database), all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
        row_with_file(&db, vec![7; size]);
        let path = db.url("moments", &id("m1"), "media").unwrap().unwrap();
        assert!(path.exists(), "file must exist before reset: {path:?}");
        db.erase_account(db.generation().next(), false).unwrap();
        assert_eq!(count(&db, "SELECT COUNT(*) FROM file"), 0);
        assert!(!path.exists(), "reset left the unheld file at {path:?}, size {size}");
    }
}

#[test]
fn store_url_small_file_cached() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("data.sqlite");
    let db = Arc::new(Store::open_models(Some(&database), all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    row_with_file(&db, vec![7; 64]);
    let descriptor = db.file("moments", &id("m1"), "media").unwrap().unwrap();
    let path = db.url("moments", &id("m1"), "media").unwrap().unwrap();
    assert!(path.ends_with(format!("cache/{}", descriptor.id)), "{path:?}");
    assert!(path.starts_with(directory.path()));
    assert_eq!(std::fs::read(&path).unwrap(), vec![7; 64]);
    write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::DetachFile { model: "moments".into(), row_id: "m1".into(), field: "media".into() }]).unwrap();
    assert!(!path.exists());
}

fn page(db: &Store, relation: &str, rows: &[(&str, Value)], replace_all: bool) -> StoreOp {
    let key = WindowKey::relation("moments", relation, &row(json!({})));
    let roots = rows.iter().map(|(id, edge)| RowValue { row_id: (*id).into(), values: json!({"id": id, "text": "same", "@edge": edge}).to_string() }).collect();
    StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all, cursor: None, roots, related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() }
}

fn edges(db: &Store, ids: &[&str]) -> Value {
    let ids: Vec<String> = ids.iter().map(|id| (*id).to_string()).collect();
    serde_json::to_value(db.edge_values("moments", "feed", &row(json!({})), &ids).unwrap()).unwrap()
}

fn open_feed() -> Arc<Store> {
    Arc::new(Store::open_models(None, crate::schema_fixtures::specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap())
}

#[test]
fn store_prop_edge_values() {
    let db = open_feed();
    let stamp = |uuid: Value, score: Value| json!({"impressionUuid": uuid, "impressionScore": score});
    db.write_queue_ops(&[page(&db, "feed", &[("m1", stamp(json!("a"), json!(1))), ("m2", stamp(json!("b"), Value::Null))], true)]).unwrap();
    assert_eq!(edges(&db, &["m1", "m2"]), json!({"m1": stamp(json!("a"), json!(1)), "m2": stamp(json!("b"), Value::Null)}));
    let revision = db.read(&["moments"], |reader| reader.stored_row("moments", "m1")).unwrap().unwrap().revision;

    let mut second = page(&db, "feed", &[("m1", stamp(Value::Null, Value::Null))], false);
    if let StoreOp::Window { cursor, .. } = &mut second { *cursor = Some("c1".into()); }
    db.write_queue_ops(&[second]).unwrap();
    assert_eq!(edges(&db, &["m1", "m2"]), json!({"m1": stamp(Value::Null, Value::Null), "m2": stamp(json!("b"), Value::Null)}), "a page replaces the values of its rows, null too");
    assert_eq!(db.read(&["moments"], |reader| reader.stored_row("moments", "m1")).unwrap().unwrap().revision, revision, "an edge change does not touch the row");

    db.write_queue_ops(&[page(&db, "feed", &[("m1", stamp(json!("z"), json!(9)))], true)]).unwrap();
    assert_eq!(edges(&db, &["m1", "m2"]), json!({"m1": stamp(json!("z"), json!(9))}), "a row that left the window loses its values");

    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "moments".into(), rows: vec![RowValue { row_id: "m1".into(), values: json!({"id": "m1", "text": "other"}).to_string() }] }]).unwrap();
    assert_eq!(edges(&db, &["m1"]), json!({"m1": stamp(json!("z"), json!(9))}), "an answer without edges leaves the values");
}

#[test]
fn store_prop_edge_read() {
    let db = open_feed();
    let rows: Vec<(String, Value)> = (0..200).map(|id| (format!("m{id}"), json!({}))).collect();
    let borrowed: Vec<(&str, Value)> = rows.iter().map(|(id, value)| (id.as_str(), value.clone())).collect();
    db.write_queue_ops(&[page(&db, "feed", &borrowed, true)]).unwrap();
    let before = db.counters();
    assert_eq!(edges(&db, &["m1", "absent"]), json!({"m1": {"impressionUuid": null, "impressionScore": null}}));
    let after = db.counters();
    assert_eq!(after.edge_read_scans - before.edge_read_scans, 0);
    assert!(after.edge_read_steps - before.edge_read_steps < 100);
    db.erase_account(db.generation().next(), false).unwrap();
    assert_eq!(edges(&db, &["m1"]), json!({}));
}
