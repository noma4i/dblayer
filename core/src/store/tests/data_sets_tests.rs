use std::sync::Arc;

use serde_json::{json, Value};

use crate::owners::contract::BatchAuthor;
use crate::owners::read::windows::WindowKey;
use crate::owners::store::{RowValue, StoreBatch, StoreOp};
use crate::store::fixtures::{all_specs, put};
use crate::store::Store;

use super::sets_reference_tests::{by_id, Sets};

pub(super) fn write_as(db: &Arc<Store>, id: u64, author: BatchAuthor, rows: Vec<(String, String)>, ops: Vec<StoreOp>) -> crate::store::batch_write::BatchWritten {
    let batch = StoreBatch { author, id, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows, intents: 0, credential: None, ops };
    db.write_batch(&batch, db.files(), None).unwrap()
}

pub(super) fn write(db: &Arc<Store>, author: BatchAuthor, rows: Vec<(String, String)>, ops: Vec<StoreOp>) {
    write_as(db, 1, author, rows, ops);
}

pub(super) fn deliver(db: &Arc<Store>, batch: u64, path: &str, model: &str, row: RowValue, related: Vec<(String, RowValue)>) -> crate::store::batch_write::BatchWritten {
    match path {
        "event" => {
            let event = json!({"op": "upsert", "id": row.row_id, "row": serde_json::from_str::<Value>(&row.values).unwrap()}).to_string();
            write_as(db, batch, BatchAuthor::Feed, vec![(model.into(), event)], Vec::new())
        }
        "response" => write_as(db, batch, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: model.into(), rows: vec![row] }]),
        "page" | "node" => {
            let source = if path == "node" { crate::schema::RowSource::NodeReread } else { crate::schema::RowSource::Page };
            let key = WindowKey::nodes(model, &[row.row_id.as_str()]);
            let op = StoreOp::Window { source, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: vec![row], related, total: None, base_revision: db.revision_seq().unwrap() };
            write_as(db, batch, BatchAuthor::Window, Vec::new(), vec![op])
        }
        other => panic!("no path {other}"),
    }
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
    deliver(&db, 1, "event", "messages", message("m2", 2), Vec::new());
    assert_eq!(chat(&db), json!(["m2", 2]), "an event");
    deliver(&db, 1, "page", "messages", message("m3", 3), Vec::new());
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

pub(super) fn intent_hiding(db: &Arc<Store>, op_id: u64, model: &str, row_id: &str) {
    let effects = crate::owners::store::IntentEffects { tombstones: vec![crate::owners::store::EffectTombstone { model: model.into(), row_id: row_id.into() }], ..Default::default() };
    write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { created_at: 0, attempts: 1, cycle_id: 1, op_id, model: model.into(), row_id: row_id.into(), action: "destroy".into(), payload: "{}".into(), effects, origin: crate::owners::store::IntentOrigin::Api }]);
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
    let written = db.write_batch(&batch, db.files(), None).unwrap();
    assert_eq!(written.stale_parents, ["chats".to_string()], "the parent row is not in the base");
    assert!(db.read(&["chats"], |reader| reader.point("chats", "c9")).unwrap().is_none(), "no parent row is made");
    put(&db, "chats", json!({"id": "c9", "title": "t"}));
    let written = db.write_batch(&batch, db.files(), None).unwrap();
    assert!(written.stale_parents.is_empty(), "the parent row is there");
}

fn land(db: &Arc<Store>, source: &str) {
    match source {
        "parent" => {
            let row = RowValue { row_id: "c1".into(), values: json!({"id": "c1", "title": "t", "lastMessageId": "m5", "lastMessageAt": 7}).to_string() };
            write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "chats".into(), rows: vec![row] }]);
        }
        "answer" | "event" | "page" => {
            deliver(db, 1, if source == "answer" { "response" } else { source }, "messages", message("m5", 5), Vec::new());
        }
        other => panic!("unknown source {other}"),
    }
}

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

#[test]
fn data_sets_named_child_edit_keeps_the_server_value() {
    let db = open();
    ["answer", "parent"].iter().for_each(|source| land(&db, source));
    for (edited, expected) in [(6, json!(["m5", 7])), (3, json!(["m5", 7]))] {
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows: vec![message("m5", edited)] }]);
        assert_eq!(chat(&db), expected, "m5 edited to {edited}: the fields come from the row of the parent");
    }
}

#[test]
fn data_sets_predicted_edit_keeps_the_time() {
    let db = open();
    land(&db, "answer");
    for (device, expected) in [(3, 5), (4, 5), (9, 9), (8, 8)] {
        write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "messages".into(), rows: vec![message("temp-1", device)] }]);
        assert_eq!(chat(&db), json!(["temp-1", expected]), "the predicted child at {device}");
    }
}

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
    crate::store::fixtures::exec(&db, "CREATE TABLE sets_writes (n INTEGER); CREATE TRIGGER sets_written AFTER UPDATE OF last_message_id, last_message_at, last_sequence_number, last_message_body ON row_chats BEGIN INSERT INTO sets_writes VALUES (1); END; CREATE TRIGGER sets_inserted AFTER INSERT ON row_chats BEGIN INSERT INTO sets_writes VALUES (1); END");
    for (name, ops, expected) in batches {
        write(&db, BatchAuthor::Outbox, Vec::new(), ops);
        let writes = crate::store::fixtures::count(&db, "SELECT COUNT(*) FROM sets_writes");
        crate::store::fixtures::exec(&db, "DELETE FROM sets_writes");
        assert_eq!(writes, expected, "{name}: writes of the fields of the parent");
    }
}

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
    let groups: [Vec<(&str, i64, Option<i64>)>; 3] = [
        vec![("9", 2, None), ("10", 2, None), ("8", 2, None), ("m0", 1, None)],
        vec![("m1", 2, Some(1)), ("m2", 2, Some(1)), ("m3", 5, None), ("m4", 5, Some(0))],
        vec![("m1", 3, None), ("m2", 3, None), ("m3", 3, None), ("m4", 3, None)],
    ];
    for children in groups {
        let kids: Vec<Child> = children.iter().map(|(id, at, seq)| Child { id: (*id).into(), at: Some(*at), seq: *seq, body: (*id).into() }).collect();
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

#[test]
fn data_sets_rollback_keeps_the_server_value() {
    let db = open();
    parent_row(&db, server_row("m5", 7, 3));
    intent_creating(&db, 1, &message("temp-1", 9));
    assert_eq!(sets(&db).id.as_deref(), Some("temp-1"), "the open operation is newest");
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RollBack { op_id: 1 }, StoreOp::RemoveIntent { op_id: 1 }]);
    assert_eq!(sets(&db), sets_of("m5", 7, 3), "the fields of the server value");
}

fn child(id: &str, chat: &str, at: i64, seq: Option<i64>) -> RowValue {
    RowValue { row_id: id.into(), values: json!({"id": id, "chatId": chat, "body": id, "createdAt": at, "sequenceNumber": seq}).to_string() }
}

fn land_children(db: &Arc<Store>, rows: Vec<RowValue>) {
    write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows }]);
}

fn shown(id: &str, at: i64, seq: Option<i64>, body: &str) -> Sets {
    Sets { id: Some(id.into()), at: Some(at), seq, body: Some(body.into()) }
}

#[test]
fn data_id_order_is_total() {
    let ids = ["0", "7", "9", "10", "007", "+5", "-3", "m1", "m2", "1a", "Z", "18446744073709551615", "18446744073709551616", "99999999999999999999"];
    let winner = |first: &str, second: &str| {
        let db = open();
        land_children(&db, vec![child(first, "c1", 5, None)]);
        land_children(&db, vec![child(second, "c1", 5, None)]);
        sets(&db).id.unwrap()
    };
    let mut beats = std::collections::BTreeSet::new();
    for (index, a) in ids.iter().enumerate() {
        for b in &ids[index + 1..] {
            let (forward, backward) = (winner(a, b), winner(b, a));
            let expected = if by_id(a, b) == std::cmp::Ordering::Greater { a } else { b };
            assert_eq!((forward.as_str(), backward.as_str()), (*expected, *expected), "{a} against {b}");
            beats.insert((if expected == a { *b } else { *a }, *expected));
        }
    }
    for (a, b) in &beats {
        for (c, d) in &beats {
            if b == c {
                assert!(beats.contains(&(*a, *d)), "{a} < {b} < {d} but not {a} < {d}");
            }
        }
    }
    for group in [["9", "10", "m1", "007"], ["18446744073709551615", "18446744073709551616", "99999999999999999999", "7"], ["+5", "-3", "1a", "Z"]] {
        let first = group.iter().max_by(|a, b| by_id(a, b)).unwrap();
        for order in permutations(&[0, 1, 2, 3]) {
            let db = open();
            order.iter().for_each(|index| land_children(&db, vec![child(group[*index], "c1", 5, None)]));
            assert_eq!(sets(&db).id.as_deref(), Some(*first), "landed as {order:?} of {group:?}");
        }
    }
}

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

#[test]
fn data_sets_destroy_of_the_named_child_marks_the_parent_stale() {
    for remaining in [false, true] {
        let db = open();
        parent_row(&db, server_row("m1", 3, 1));
        land_children(&db, if remaining { vec![child("m1", "c1", 3, None), child("m0", "c1", 1, None)] } else { vec![child("m1", "c1", 3, None)] });
        assert_eq!(sets(&db), shown("m1", 3, Some(1), "m1"), "remaining {remaining}: the value of the server names the newest child");
        let written = write_as(&db, 1, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveRows { model: "messages".into(), ids: vec!["m1".into()] }]);
        assert_eq!(written.stale_parents, ["chats".to_string()], "remaining {remaining}: the windows of the parent are stale");
        let expected = if remaining { shown("m0", 1, None, "m0") } else { Sets::default() };
        assert_eq!(sets(&db), expected, "remaining {remaining}: the value naming the destroyed child is left out");
        parent_row(&db, server_row("m6", 9, 2));
        let expected = if remaining { Sets { body: Some("m0".into()), ..sets_of("m6", 9, 2) } } else { sets_of("m6", 9, 2) };
        assert_eq!(sets(&db), expected, "remaining {remaining}: the next row of the parent gives the value again");
    }
}

#[test]
fn data_sets_server_winner_takes_the_empty_field_of_the_second() {
    let server = |values: Value| {
        let mut row = json!({"id": "c1", "title": "t"});
        row.as_object_mut().unwrap().extend(values.as_object().unwrap().clone());
        row
    };
    let cases = [
        ("the server names the newest child, no sequence", json!({"lastMessageId": "m5", "lastMessageAt": 7}), child("m5", "c1", 5, Some(2)), shown("m5", 7, Some(2), "m5")),
        ("the server value is not older, no sequence", json!({"lastMessageId": "m5", "lastMessageAt": 7}), child("m6", "c1", 5, Some(4)), shown("m5", 7, Some(4), "m6")),
        ("the server names the newest child, no time", json!({"lastMessageId": "m5", "lastSequenceNumber": 3}), child("m5", "c1", 5, Some(3)), shown("m5", 5, Some(3), "m5")),
    ];
    for (name, values, newest, expected) in cases {
        let db = open();
        land_children(&db, vec![newest]);
        parent_row(&db, server(values));
        assert_eq!(sets(&db), expected, "{name}");
    }
}

#[test]
fn data_sets_child_moves_to_another_parent() {
    let db = open();
    put(&db, "chats", json!({"id": "c2", "title": "t"}));
    let sets_of_chat = |chat: &str| super::sets_reference_tests::stored_sets_of(&db, chat);
    land_children(&db, vec![child("m1", "c1", 1, None), child("m2", "c1", 2, None)]);
    land_children(&db, vec![child("m2", "c2", 2, None)]);
    assert_eq!((sets_of_chat("c1"), sets_of_chat("c2")), (shown("m1", 1, None, "m1"), shown("m2", 2, None, "m2")), "the old parent takes its other child, the new parent takes the moved one");
    land_children(&db, vec![child("m1", "c2", 1, None)]);
    assert_eq!((sets_of_chat("c1"), sets_of_chat("c2")), (Sets::default(), shown("m2", 2, None, "m2")), "the old parent has no child left");
}

#[test]
fn data_sets_hidden_child_does_not_count() {
    let db = open();
    land_children(&db, vec![child("m1", "c1", 1, None), child("m2", "c1", 2, None)]);
    intent_hiding(&db, 1, "messages", "m2");
    assert_eq!(sets(&db), shown("m1", 1, None, "m1"), "the hidden child is left out");
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RollBack { op_id: 1 }, StoreOp::RemoveIntent { op_id: 1 }]);
    assert_eq!(sets(&db), shown("m2", 2, None, "m2"), "the child is back after the rollback");
}

#[test]
fn data_sets_rollback_of_an_edit_of_a_stored_child() {
    let db = open();
    land_children(&db, vec![child("m1", "c1", 1, None), child("m2", "c1", 5, None)]);
    intent_creating(&db, 7, &child("m1", "c1", 9, None));
    assert_eq!(sets(&db), shown("m1", 9, None, "m1"), "the edit makes m1 the newest");
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RollBack { op_id: 7 }, StoreOp::RemoveIntent { op_id: 7 }]);
    assert_eq!(sets(&db), shown("m2", 5, None, "m2"), "the fields of the parent follow the stored rows");
}

#[test]
fn data_sets_server_value_is_accepted_like_a_server_field() {
    let mut specs = crate::store::fixtures::all_specs();
    specs[3] = crate::store::fixtures::revisioned_chats_spec();
    let db = Arc::new(Store::open_models(None, specs, None, Arc::new(crate::runtime::SystemClock)).unwrap());
    put(&db, "chats", json!({"id": "c1", "title": "t", "rev": 1}));
    let parent = |rev: i64, id: &str, at: i64, seq: i64| RowValue { row_id: "c1".into(), values: json!({"id": "c1", "title": "t", "rev": rev, "lastMessageId": id, "lastMessageAt": at, "lastSequenceNumber": seq}).to_string() };
    let answer = |row: RowValue| write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "chats".into(), rows: vec![row] }]);
    let page = |row: RowValue, base_revision: i64| {
        let key = WindowKey::nodes("chats", &["c1"]);
        let op = StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: vec![row], related: Vec::new(), total: None, base_revision };
        write(&db, BatchAuthor::Window, Vec::new(), vec![op]);
    };
    answer(parent(5, "m5", 7, 3));
    assert_eq!(sets(&db), sets_of("m5", 7, 3));
    answer(parent(3, "m9", 50, 9));
    assert_eq!(sets(&db), sets_of("m5", 7, 3), "a row with a lower server revision is dropped");
    let early = db.revision_seq().unwrap();
    answer(parent(6, "m6", 8, 4));
    assert_eq!(sets(&db), sets_of("m6", 8, 4));
    page(parent(7, "m7", 9, 5), early);
    assert_eq!(sets(&db), sets_of("m6", 8, 4), "a page asked before the later row does not change the fields of the later row");
    page(parent(7, "m7", 9, 5), db.revision_seq().unwrap());
    assert_eq!(sets(&db), sets_of("m7", 9, 5), "a page asked after the later row lands");
}

fn parent_value(fields: Value) -> RowValue {
    let mut values = json!({"id": "c1", "title": "t"});
    values.as_object_mut().unwrap().extend(fields.as_object().unwrap().clone());
    RowValue { row_id: "c1".into(), values: values.to_string() }
}

fn confirmed_child(id: &str, at: i64) -> super::sets_reference_tests::Child {
    super::sets_reference_tests::Child { id: id.into(), at: Some(at), seq: None, body: id.into() }
}

fn server_pointer(id: &str, at: i64) -> super::sets_reference_tests::Server {
    super::sets_reference_tests::Server { id: Some(id.into()), at: Some(at), seq: None }
}

fn reopened_at(path: &std::path::Path) -> Arc<Store> {
    Arc::new(Store::open_models(Some(path), all_specs(), None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap())
}

fn temp_rows(db: &Store) -> i64 {
    crate::store::fixtures::count(db, "SELECT COUNT(*) FROM row_messages WHERE id LIKE 'temp-%'")
}

fn with_confirmed_child(path: &std::path::Path) -> Arc<Store> {
    let db = reopened_at(path);
    put(&db, "chats", json!({"id": "c1", "title": "t"}));
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "chats".into(), rows: vec![parent_value(json!({"lastMessageId": "m1", "lastMessageAt": 1}))] }]);
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows: vec![message("m1", 1)] }]);
    db
}

/// data_sets_recovered_at_open (P-DATA-56, D-338), (a): the open removes the row of a lost operation and the parent loses the pointer to it.
#[test]
fn data_sets_recovered_at_open() {
    use super::sets_reference_tests::{derive, stored_sets};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("recovered.sqlite");
    {
        let db = with_confirmed_child(&path);
        write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "messages".into(), rows: vec![message("temp-1", 5)] }]);
        assert_eq!(stored_sets(&db).id.as_deref(), Some("temp-1"), "the prediction moved the pointer before the process was killed");
    }
    let db = reopened_at(&path);
    assert_eq!(temp_rows(&db), 0, "the open removed the row of the lost operation");
    assert_eq!(stored_sets(&db), derive(Some(&server_pointer("m1", 1)), &[confirmed_child("m1", 1)]), "the parent has no pointer to the removed row");
}

/// data_sets_recovered_at_open_returned (P-DATA-56, D-338), (b): the open returns the row of an open operation and the fields of the parent count it.
#[test]
fn data_sets_recovered_at_open_returned() {
    use super::sets_reference_tests::{derive, stored_sets, Child};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("returned.sqlite");
    {
        let db = with_confirmed_child(&path);
        intent_creating(&db, 7, &message("temp-2", 6));
        assert_eq!(stored_sets(&db).id.as_deref(), Some("temp-2"), "the operation predicts the child");
        crate::store::fixtures::exec(&db, "DELETE FROM row_messages WHERE id = 'temp-2'");
        crate::store::fixtures::exec(&db, "UPDATE op SET temp_id = 'temp-2' WHERE seq = 7");
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "chats".into(), rows: vec![parent_value(json!({"lastMessageId": "m1", "lastMessageAt": 1}))] }]);
        assert_eq!(stored_sets(&db).id.as_deref(), Some("m1"), "the parent counts the children that are stored");
    }
    let db = reopened_at(&path);
    assert_eq!(temp_rows(&db), 1, "the open returned the row of the open operation");
    let temp = Child { id: "temp-2".into(), at: Some(6), seq: None, body: "temp-2".into() };
    assert_eq!(stored_sets(&db), derive(Some(&server_pointer("m1", 1)), &[confirmed_child("m1", 1), temp]), "the fields of the parent count the returned child");
}

/// data_sets_hidden_value_is_not_read (P-DATA-58, D-338): the stored server value never reaches a reader or a subscriber.
#[test]
fn data_sets_hidden_value_is_not_read() {
    use std::sync::Mutex;
    let db = Arc::new(Store::open_in_memory(Arc::new(crate::runtime::SystemClock)).unwrap());
    db.boot(&all_specs(), None).unwrap();
    db.mark_ready();
    let changed: Arc<Mutex<Vec<(String, Vec<String>)>>> = Arc::default();
    let log = changed.clone();
    db.set_hook(Box::new(move |event| {
        if let crate::store::StoreEvent::Committed(result) = event {
            log.lock().unwrap().extend(result.rows.iter().map(|change| (change.row.id.to_string(), change.fields.iter().cloned().collect())));
        }
    })).unwrap();
    put(&db, "chats", json!({"id": "c1", "title": "t"}));
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "chats".into(), rows: vec![parent_value(json!({"lastMessageId": "m5", "lastMessageAt": 7, "lastMessageBody": "hi"}))] }]);
    deliver(&db, 2, "event", "messages", message("m6", 8), Vec::new());
    let point = db.read(&["chats"], |reader| reader.point("chats", "c1")).unwrap().unwrap();
    let window = db.read(&["chats"], |reader| reader.stored_rows("chats", &["c1".to_string()])).unwrap();
    let hidden = |keys: Vec<String>| keys.into_iter().filter(|key| key.starts_with("server__")).collect::<Vec<_>>();
    assert!(point.contains_key("lastMessageId"), "the read is not empty");
    assert_eq!(hidden(point.keys().cloned().collect()), Vec::<String>::new(), "the point read");
    assert_eq!(window.len(), 1);
    assert_eq!(hidden(window["c1"].row.keys().cloned().collect()), Vec::<String>::new(), "the window read");
    let changes = changed.lock().unwrap();
    assert!(changes.iter().any(|(id, fields)| id == "c1" && fields.iter().any(|field| field == "lastMessageId")), "the subscriber saw the parent change");
    assert_eq!(changes.iter().flat_map(|(_, fields)| hidden(fields.clone())).collect::<Vec<_>>(), Vec::<String>::new(), "the changed fields");
}

/// data_sets_hidden_value_is_not_read_declared (P-DATA-58): a field `server__x` of a `.dbl` model is refused when the schema is built.
#[test]
fn data_sets_hidden_value_is_not_read_declared() {
    let mut declared = crate::store::fixtures::chats_spec();
    declared.fields.push(crate::schema::FieldSpec::scalar("server__x", crate::value::ScalarKind::Str).optional());
    assert_eq!(declared.validate(&[]), Err(crate::schema::DeclarationError::ReservedFieldName("server__x".into())), "the storage name is taken");
}
