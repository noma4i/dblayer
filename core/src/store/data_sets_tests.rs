use std::sync::Arc;

use serde_json::{json, Value};

use crate::owners::contract::BatchAuthor;
use crate::owners::read::windows::WindowKey;
use crate::owners::store::{RowValue, StoreBatch, StoreOp};
use crate::store::spec_fixtures::{all_specs, put};
use crate::store::Store;

fn write(db: &Arc<Store>, author: BatchAuthor, rows: Vec<(String, String)>, ops: Vec<StoreOp>) {
    let batch = StoreBatch { author, id: 1, epoch: 1, basis: Default::default(), rows, intents: 0, credential: None, ops };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
}

fn message(id: &str, at: i64) -> RowValue {
    RowValue { row_id: id.into(), values: json!({"id": id, "chatId": "c1", "body": id, "createdAt": at}).to_string() }
}

fn chat(db: &Store) -> Value {
    let row = db.read(&["chats"], |reader| reader.point("chats", "c1")).unwrap().unwrap();
    let at = row.get("lastMessageAt").and_then(Value::as_str).and_then(|text| text.rsplit(':').next()).and_then(|seconds| seconds.trim_end_matches('Z').parse::<f64>().ok()).map_or(Value::Null, |seconds| json!((seconds * 1000.0).round() as i64));
    json!([row.get("lastMessageId").cloned().unwrap_or(Value::Null), at])
}

fn open() -> Arc<Store> {
    let db = Arc::new(Store::open_models(None, all_specs(), None).unwrap());
    put(&db, "chats", json!({"id": "c1", "title": "t", "lastMessageId": null, "lastMessageAt": null}));
    db
}

#[test]
fn data_sets_every_write() {
    let db = open();
    write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "messages".into(), rows: vec![message("m1", 1)] }]);
    assert_eq!(chat(&db), json!(["m1", 1]), "a local create");
    let event = json!({"op": "upsert", "id": "m2", "row": {"id": "m2", "chatId": "c1", "body": "m2", "createdAt": 2}}).to_string();
    write(&db, BatchAuthor::Feed, vec![("messages".into(), event)], Vec::new());
    assert_eq!(chat(&db), json!(["m2", 2]), "an event");
    let key = WindowKey::nodes("messages", &["m3"]);
    write(&db, BatchAuthor::Window, Vec::new(), vec![StoreOp::Window { key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: vec![message("m3", 3)], related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() }]);
    assert_eq!(chat(&db), json!(["m3", 3]), "a page");
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows: vec![message("m4", 4)] }]);
    assert_eq!(chat(&db), json!(["m4", 4]), "the answer of a mutation");
}

#[test]
fn data_newer_guards_parent() {
    let db = open();
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows: vec![message("m5", 5)] }]);
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows: vec![message("m1", 1)] }]);
    assert_eq!(chat(&db), json!(["m5", 5]), "an older child does not move the pointer back");
    let stale = RowValue { row_id: "c1".into(), values: json!({"id": "c1", "title": "renamed", "lastMessageId": "m1", "lastMessageAt": 1}).to_string() };
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "chats".into(), rows: vec![stale] }]);
    let row = db.read(&["chats"], |reader| reader.point("chats", "c1")).unwrap().unwrap();
    assert_eq!((chat(&db), row.get("title").cloned()), (json!(["m5", 5]), Some(json!("renamed"))), "a parent row of the server keeps the newer pointer and takes the other fields");
}

#[test]
fn data_sets_destroy_moves_back() {
    let db = open();
    let rows = (1..=3).map(|at| message(&format!("m{at}"), at)).collect();
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows }]);
    assert_eq!(chat(&db), json!(["m3", 3]));
    for (gone, expected) in [("m3", json!(["m2", 2])), ("m1", json!(["m2", 2])), ("m2", json!([null, null]))] {
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveRows { model: "messages".into(), ids: vec![gone.into()] }]);
        assert_eq!(chat(&db), expected, "after {gone}");
    }
}

#[test]
fn data_sets_swap() {
    let db = open();
    write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "messages".into(), rows: vec![message("temp-1", 5)] }]);
    assert_eq!(chat(&db), json!(["temp-1", 5]));
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![
        StoreOp::Rows { model: "messages".into(), rows: vec![message("m9", 5)] },
        StoreOp::Swap { model: "messages".into(), temp: "temp-1".into(), real: "m9".into() },
    ]);
    assert_eq!(chat(&db), json!(["m9", 5]));
}

fn intent_creating(db: &Arc<Store>, op_id: u64, row: &RowValue) {
    let effects = crate::owners::store::IntentEffects { rows: vec![crate::owners::store::EffectRow { model: "messages".into(), row_id: row.row_id.clone(), values: row.values.clone() }], ..Default::default() };
    write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { op_id, model: "messages".into(), row_id: row.row_id.clone(), action: "react".into(), payload: row.values.clone(), effects, origin: crate::owners::store::IntentOrigin::Api }]);
}

#[test]
fn data_sets_rollback() {
    let db = open();
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows: vec![message("m1", 1)] }]);
    intent_creating(&db, 7, &message("m2", 2));
    assert_eq!(chat(&db), json!(["m2", 2]), "the optimistic child moves the pointer");
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RollBack { op_id: 7 }]);
    assert_eq!(chat(&db), json!(["m1", 1]), "a rejected or discarded operation gives the parent its values back");
}

#[test]
fn data_sets_missing_parent_store() {
    let db = Arc::new(Store::open_models(None, all_specs(), None).unwrap());
    let event = json!({"op": "upsert", "id": "m1", "row": {"id": "m1", "chatId": "c9", "body": "m1", "createdAt": 1}}).to_string();
    let batch = StoreBatch { author: BatchAuthor::Feed, id: 1, epoch: 1, basis: Default::default(), rows: vec![("messages".into(), event)], intents: 0, credential: None, ops: Vec::new() };
    let written = db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    assert_eq!(written.stale_parents, ["chats".to_string()], "the parent row is not in the base");
    assert!(db.read(&["chats"], |reader| reader.point("chats", "c9")).unwrap().is_none(), "no parent row is made");
    put(&db, "chats", json!({"id": "c9", "title": "t"}));
    let written = db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    assert!(written.stale_parents.is_empty(), "the parent row is there");
}
