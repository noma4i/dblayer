use std::sync::Arc;

use serde_json::{json, Value};

use crate::owners::contract::BatchAuthor;
use crate::owners::read::windows::WindowKey;
use crate::owners::store::{RowValue, StoreBatch, StoreOp};
use crate::store::fixtures::{all_specs, put};
use crate::store::Store;

pub(super) fn write(db: &Arc<Store>, author: BatchAuthor, rows: Vec<(String, String)>, ops: Vec<StoreOp>) {
    let batch = StoreBatch { author, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows, intents: 0, credential: None, ops };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
}

pub(super) fn message(id: &str, at: i64) -> RowValue {
    RowValue { row_id: id.into(), values: json!({"id": id, "chatId": "c1", "body": id, "createdAt": at}).to_string() }
}

pub(super) fn chat(db: &Store) -> Value {
    let row = db.read(&["chats"], |reader| reader.point("chats", "c1")).unwrap().unwrap();
    let at = row.get("lastMessageAt").and_then(Value::as_str).and_then(|text| text.rsplit(':').next()).and_then(|seconds| seconds.trim_end_matches('Z').parse::<f64>().ok()).map_or(Value::Null, |seconds| json!((seconds * 1000.0).round() as i64));
    json!([row.get("lastMessageId").cloned().unwrap_or(Value::Null), at])
}

pub(super) fn open() -> Arc<Store> {
    let db = Arc::new(Store::open_models(None, all_specs(), None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    put(&db, "chats", json!({"id": "c1", "title": "t"}));
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
    write(&db, BatchAuthor::Window, Vec::new(), vec![StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: vec![message("m3", 3)], related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() }]);
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

pub(super) fn intent_creating(db: &Arc<Store>, op_id: u64, row: &RowValue) {
    let effects = crate::owners::store::IntentEffects { rows: vec![crate::owners::store::EffectRow { model: "messages".into(), row_id: row.row_id.clone(), values: row.values.clone() }], ..Default::default() };
    write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { created_at: 0, attempts: 1, cycle_id: 1, op_id, model: "messages".into(), row_id: row.row_id.clone(), action: "react".into(), payload: row.values.clone(), effects, origin: crate::owners::store::IntentOrigin::Api }]);
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
    let db = Arc::new(Store::open_models(None, all_specs(), None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    let event = json!({"op": "upsert", "id": "m1", "row": {"id": "m1", "chatId": "c9", "body": "m1", "createdAt": 1}}).to_string();
    let batch = StoreBatch { author: BatchAuthor::Feed, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: vec![("messages".into(), event)], intents: 0, credential: None, ops: Vec::new() };
    let written = db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    assert_eq!(written.stale_parents, ["chats".to_string()], "the parent row is not in the base");
    assert!(db.read(&["chats"], |reader| reader.point("chats", "c9")).unwrap().is_none(), "no parent row is made");
    put(&db, "chats", json!({"id": "c9", "title": "t"}));
    let written = db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    assert!(written.stale_parents.is_empty(), "the parent row is there");
}

fn land(db: &Arc<Store>, source: &str) {
    match source {
        "parent" => {
            let row = RowValue { row_id: "c1".into(), values: json!({"id": "c1", "title": "t", "lastMessageId": "m5", "lastMessageAt": 7}).to_string() };
            write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "chats".into(), rows: vec![row] }]);
        }
        "answer" => write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows: vec![message("m5", 5)] }]),
        "event" => {
            let event = json!({"op": "upsert", "id": "m5", "row": {"id": "m5", "chatId": "c1", "body": "m5", "createdAt": 5}}).to_string();
            write(db, BatchAuthor::Feed, vec![("messages".into(), event)], Vec::new());
        }
        "page" => {
            let key = WindowKey::nodes("messages", &["m5"]);
            write(db, BatchAuthor::Window, Vec::new(), vec![StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: vec![message("m5", 5)], related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() }]);
        }
        other => panic!("unknown source {other}"),
    }
}

/// P-DATA-103: the row of the parent and the row of its child settle the parent fields in any order and number of landings.
#[test]
fn data_sets_two_sources_settle() {
    let sources = ["parent", "answer", "event", "page"];
    for child in &sources[1..] {
        for first in [[*child, "parent"], ["parent", *child]] {
            for tail in (0..16).map(|n| [sources[n % 4], sources[n / 4]]) {
                let db = open();
                first.iter().for_each(|source| land(&db, source));
                assert_eq!(chat(&db), json!(["m5", 7]), "{first:?}: the parent row gives the fields at its pointer");
                for (step, source) in tail.iter().enumerate() {
                    land(&db, source);
                    assert_eq!(chat(&db), json!(["m5", 7]), "{first:?} then {:?}: a landing of the same rows changes nothing", &tail[..=step]);
                }
            }
        }
    }
}

/// P-DATA-57, P-DATA-60: a prediction does not move the time back, the confirmed child replaces it by the order.
#[test]
fn data_sets_prediction_is_replaced_by_the_confirmed_child() {
    for (device, server, expected) in [(9, 6, ("m9", 6)), (3, 6, ("m9", 6)), (3, 4, ("m5", 5))] {
        let db = open();
        land(&db, "answer");
        write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "messages".into(), rows: vec![message("temp-1", device)] }]);
        assert_eq!(chat(&db), json!(["temp-1", device.max(5)]), "device {device}: a prediction does not move the time back");
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![
            StoreOp::Swap { model: "messages".into(), temp: "temp-1".into(), real: "m9".into() },
            StoreOp::Rows { model: "messages".into(), rows: vec![message("m9", server)] },
        ]);
        assert_eq!(chat(&db), json!([expected.0, expected.1]), "device {device}, server {server}: the first child by the order of newer");
    }
}

/// P-DATA-59: an edit of the child the server value names does not change the fields of the parent.
#[test]
fn data_sets_named_child_edit_keeps_the_server_value() {
    let db = open();
    ["answer", "parent"].iter().for_each(|source| land(&db, source));
    for (edited, expected) in [(6, json!(["m5", 7])), (3, json!(["m5", 7]))] {
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows: vec![message("m5", edited)] }]);
        assert_eq!(chat(&db), expected, "m5 edited to {edited}: the fields come from the row of the parent");
    }
}

/// P-DATA-57: an edit of the predicted child the parent points at does not move the time back.
#[test]
fn data_sets_predicted_edit_keeps_the_time() {
    let db = open();
    land(&db, "answer");
    for (device, expected) in [(3, 5), (4, 5), (9, 9), (8, 8)] {
        write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "messages".into(), rows: vec![message("temp-1", device)] }]);
        assert_eq!(chat(&db), json!(["temp-1", expected]), "the predicted child at {device}");
    }
}

/// P-DATA-59: the child taken after a destroy is the one the order of `newer` ranks first, an equal time is decided by the id.
#[test]
fn data_sets_destroy_takes_the_first_by_the_order() {
    for ids in [["m1", "m2"], ["m2", "m1"]] {
        let db = open();
        let rows = ids.iter().map(|id| message(id, 5)).chain([message("m3", 9)]).collect();
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows }]);
        assert_eq!(chat(&db), json!(["m3", 9]));
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveRows { model: "messages".into(), ids: vec!["m3".into()] }]);
        assert_eq!(chat(&db), json!(["m2", 5]), "written as {ids:?}: the same child a landing of both would point at");
    }
}

/// P-DATA-59: a destroy of the pointed child reads a number of children that does not grow with their count.
#[test]
fn data_sets_destroy_reads_a_bounded_number_of_children() {
    let read = |count: i64| {
        let db = open();
        let rows = (1..=count).map(|at| message(&format!("m{at}"), at)).collect();
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows }]);
        let before = db.counters().pointer_rows_read;
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveRows { model: "messages".into(), ids: vec![format!("m{count}")] }]);
        assert_eq!(chat(&db), json!([format!("m{}", count - 1), count - 1]), "{count} children");
        db.counters().pointer_rows_read - before
    };
    let (many, few) = (read(200), read(20));
    assert!(few > 0 && many == few, "the rows read by the destroy do not depend on the number of children: {few} for 20, {many} for 200");
}

fn parent_row(db: &Arc<Store>, values: Value) {
    write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "chats".into(), rows: vec![RowValue { row_id: "c1".into(), values: values.to_string() }] }]);
}

fn sets(db: &Store) -> super::sets_reference_tests::Sets {
    super::sets_reference_tests::stored_sets(db)
}

fn sets_of(id: &str, at: i64, seq: i64) -> super::sets_reference_tests::Sets {
    super::sets_reference_tests::Sets { id: Some(id.into()), at: Some(at), seq: Some(seq), body: None }
}

fn server_row(id: &str, at: i64, seq: i64) -> Value {
    json!({"id": "c1", "title": "t", "lastMessageId": id, "lastMessageAt": at, "lastSequenceNumber": seq})
}

/// P-DATA-56: the fields are written once for a batch, whatever the number of its writes of children.
#[test]
fn data_sets_derived_once_per_batch() {
    let batches: [(&str, Vec<StoreOp>, i64); 3] = [
        ("three children", (1..=3).map(|at| StoreOp::Rows { model: "messages".into(), rows: vec![message(&format!("m{at}"), at)] }).collect(), 1),
        ("a child, a destroy of it", vec![StoreOp::Rows { model: "messages".into(), rows: vec![message("m4", 4)] }, StoreOp::RemoveRows { model: "messages".into(), ids: vec!["m4".into()] }], 0),
        ("two children and the row of the parent", vec![
            StoreOp::Rows { model: "messages".into(), rows: vec![message("m5", 5)] },
            StoreOp::Rows { model: "chats".into(), rows: vec![RowValue { row_id: "c1".into(), values: server_row("m6", 9, 1).to_string() }] },
            StoreOp::Rows { model: "messages".into(), rows: vec![message("m6", 6)] },
        ], 1),
    ];
    let db = open();
    crate::store::fixtures::exec(&db, "CREATE TABLE sets_writes (n INTEGER); CREATE TRIGGER sets_written AFTER UPDATE OF lastMessageId, lastMessageAt, lastSequenceNumber, lastMessageBody ON row_chats BEGIN INSERT INTO sets_writes VALUES (1); END");
    for (name, ops, expected) in batches {
        write(&db, BatchAuthor::Outbox, Vec::new(), ops);
        let writes = crate::store::fixtures::count(&db, "SELECT COUNT(*) FROM sets_writes");
        crate::store::fixtures::exec(&db, "DELETE FROM sets_writes");
        assert_eq!(writes, expected, "{name}: writes of the fields of the parent");
    }
}

/// P-DATA-57: any order of landing of children with equal and absent values gives the first child by the order.
#[test]
fn data_newer_order_is_total() {
    use super::sets_reference_tests::{derive, newest, Child};
    fn permutations(items: &[usize]) -> Vec<Vec<usize>> {
        if items.len() <= 1 {
            return vec![items.to_vec()];
        }
        (0..items.len()).flat_map(|at| {
            let mut rest = items.to_vec();
            let head = rest.remove(at);
            permutations(&rest).into_iter().map(move |mut tail| { tail.insert(0, head); tail })
        }).collect()
    }
    let sets: [Vec<(&str, i64, Option<i64>)>; 3] = [
        vec![("9", 2, None), ("10", 2, None), ("8", 2, None), ("m0", 1, None)],
        vec![("m1", 2, Some(1)), ("m2", 2, Some(1)), ("m3", 5, None), ("m4", 5, Some(0))],
        vec![("m1", 3, None), ("m2", 3, None), ("m3", 3, None), ("m4", 3, None)],
    ];
    for children in sets {
        let kids: Vec<Child> = children.iter().map(|(id, at, seq)| Child { id: (*id).into(), at: *at, seq: *seq, body: (*id).into() }).collect();
        let first = newest(&kids).unwrap().id.clone();
        for order in permutations(&(0..kids.len()).collect::<Vec<_>>()) {
            let db = open();
            for index in &order {
                let child = &kids[*index];
                let seq = child.seq.map_or(Value::Null, |seq| json!(seq));
                let row = RowValue { row_id: child.id.clone(), values: json!({"id": child.id, "chatId": "c1", "body": child.body, "createdAt": child.at, "sequenceNumber": seq}).to_string() };
                write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows: vec![row] }]);
            }
            assert_eq!((sets(&db).id, sets(&db)), (Some(first.clone()), derive(None, &kids)), "landed as {order:?} of {children:?}");
        }
    }
}

/// P-DATA-58: the value the server gave is kept: a destroy of children, a row of the parent without the fields and an older child do not lose it.
#[test]
fn data_sets_server_value_is_kept() {
    let db = open();
    parent_row(&db, server_row("m5", 7, 3));
    assert_eq!(sets(&db), sets_of("m5", 7, 3), "no children");
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows: vec![message("m1", 1)] }]);
    assert_eq!((sets(&db).id, sets(&db).at, sets(&db).seq), (Some("m5".into()), Some(7), Some(3)), "an older child");
    parent_row(&db, json!({"id": "c1", "title": "renamed"}));
    assert_eq!((sets(&db).id, sets(&db).at, sets(&db).seq), (Some("m5".into()), Some(7), Some(3)), "a row of the parent without the fields");
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveRows { model: "messages".into(), ids: vec!["m1".into()] }]);
    assert_eq!(sets(&db), sets_of("m5", 7, 3), "the child is destroyed");
}

/// P-DATA-60: the server value that named the real id while the operation was open wins after the swap.
#[test]
fn data_sets_server_value_during_pending_wins_after_swap() {
    let db = open();
    intent_creating(&db, 1, &message("temp-1", 9));
    parent_row(&db, server_row("r1", 4, 2));
    assert_eq!(sets(&db).id.as_deref(), Some("temp-1"), "the open operation is newest");
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![
        StoreOp::Swap { model: "messages".into(), temp: "temp-1".into(), real: "r1".into() },
        StoreOp::Rows { model: "messages".into(), rows: vec![message("r1", 3)] },
        StoreOp::RemoveIntent { op_id: 1 },
    ]);
    assert_eq!(sets(&db), super::sets_reference_tests::Sets { body: Some("r1".into()), ..sets_of("r1", 4, 2) }, "the value of the server for r1");
}

/// P-DATA-61: a rollback gives the fields of the server value back, the parent keeps no value from before the operation.
#[test]
fn data_sets_rollback_keeps_the_server_value() {
    let db = open();
    parent_row(&db, server_row("m5", 7, 3));
    intent_creating(&db, 1, &message("temp-1", 9));
    assert_eq!(sets(&db).id.as_deref(), Some("temp-1"), "the open operation is newest");
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RollBack { op_id: 1 }, StoreOp::RemoveIntent { op_id: 1 }]);
    assert_eq!(sets(&db), sets_of("m5", 7, 3), "the fields of the server value");
}
