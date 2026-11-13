use std::sync::Arc;

use serde_json::{json, Value};

use crate::owners::contract::BatchAuthor;
use crate::owners::read::windows::WindowKey;
use crate::owners::store::{RowValue, StoreBatch, StoreOp};
use crate::store::fixtures::{all_specs, put};
use crate::store::Store;

use super::sets_reference_tests::{by_id, Sets};

pub(super) fn write_as(db: &Arc<Store>, id: u64, author: BatchAuthor, rows: Vec<(String, String)>, ops: Vec<StoreOp>) -> crate::store::batch_write::BatchWritten {
    let batch = StoreBatch { author, id, stamp: crate::runtime::EpochStamp::Epoch(1), rows, intents: 0, credential: None, ops };
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
    let db = Arc::new(Store::open_models(None, all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    put(&db, "chats", json!({"id": "c1", "title": "t"}));
    db
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
fn data_sets_missing_parent_store() {
    let db = Arc::new(Store::open_models(None, all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    let event = json!({"op": "upsert", "id": "m1", "row": {"id": "m1", "chatId": "c9", "body": "m1", "createdAt": 1}}).to_string();
    let batch = StoreBatch { author: BatchAuthor::Feed, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), rows: vec![("messages".into(), event)], intents: 0, credential: None, ops: Vec::new() };
    let written = db.write_batch(&batch, db.files(), None).unwrap();
    assert_eq!(written.stale_parents, ["chats".to_string()], "the parent row is not in the base");
    assert!(db.read(&["chats"], |reader| reader.point("chats", "c9")).unwrap().is_none(), "no parent row is made");
    put(&db, "chats", json!({"id": "c9", "title": "t"}));
    let written = db.write_batch(&batch, db.files(), None).unwrap();
    assert!(written.stale_parents.is_empty(), "the parent row is there");
}

fn sets(db: &Store) -> super::sets_reference_tests::Sets {
    super::sets_reference_tests::stored_sets(db)
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
    let groups: [(Vec<(&str, i64, Option<i64>)>, &str); 4] = [
        (vec![("9", 2, None), ("10", 2, None), ("8", 2, None), ("m0", 1, None)], "10"),
        (vec![("m1", 2, Some(1)), ("m2", 2, Some(1)), ("m3", 5, None), ("m4", 5, Some(0))], "m2"),
        (vec![("m1", 3, None), ("m2", 3, None), ("m3", 3, None), ("m4", 3, None)], "m4"),
        (vec![("7", 2, Some(9_007_199_254_740_992)), ("6", 2, Some(9_007_199_254_740_993))], "6"),
    ];
    for (children, first) in groups {
        for order in permutations(&(0..children.len()).collect::<Vec<_>>()) {
            let db = open();
            for index in &order {
                let (id, at, seq) = children[*index];
                land_children(&db, vec![child(id, "c1", at, seq)]);
            }
            assert_eq!(sets(&db).id.as_deref(), Some(first), "landed as {order:?} of {children:?}");
        }
    }
}

fn child(id: &str, chat: &str, at: i64, seq: Option<i64>) -> RowValue {
    RowValue { row_id: id.into(), values: json!({"id": id, "chatId": chat, "body": id, "createdAt": at, "sequenceNumber": seq}).to_string() }
}

pub(super) fn land_children(db: &Arc<Store>, rows: Vec<RowValue>) {
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

/// P-DATA-59 (d), P-DATA-122: the children that are left give the fields of the parent and its windows turn stale when the server removes the named child and when the confirm of an own destroy ends its hiding.
#[test]
fn data_sets_destroy_of_the_named_child_marks_the_parent_stale() {
    for (path, remaining) in [("the server removes it", false), ("the server removes it", true), ("the confirm of an own destroy", false), ("the confirm of an own destroy", true)] {
        let db = open();
        land_children(&db, if remaining { vec![child("m1", "c1", 3, None), child("m0", "c1", 1, None)] } else { vec![child("m1", "c1", 3, None)] });
        assert_eq!(sets(&db), shown("m1", 3, None, "m1"), "{path}, remaining {remaining}: the newest child");
        let written = if path == "the server removes it" {
            write_as(&db, 1, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveRows { model: "messages".into(), ids: vec!["m1".into()] }])
        } else {
            intent_hiding(&db, 1, "messages", "m1");
            write_as(&db, 2, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveIntent { op_id: 1 }])
        };
        assert_eq!(written.stale_parents, ["chats".to_string()], "{path}, remaining {remaining}: the windows of the parent are stale");
        let expected = if remaining { shown("m0", 1, None, "m0") } else { Sets::default() };
        assert_eq!(sets(&db), expected, "{path}, remaining {remaining}: the fields come from the children that are left");
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
fn data_sets_rollback_of_an_edit_of_a_stored_child() {
    let db = open();
    land_children(&db, vec![child("m1", "c1", 1, None), child("m2", "c1", 5, None)]);
    intent_creating(&db, 7, &child("m1", "c1", 9, None));
    assert_eq!(sets(&db), shown("m1", 9, None, "m1"), "the edit makes m1 the newest");
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RollBack { op_id: 7 }, StoreOp::RemoveIntent { op_id: 7 }]);
    assert_eq!(sets(&db), shown("m2", 5, None, "m2"), "the fields of the parent follow the stored rows");
}

fn parent_value(fields: Value) -> RowValue {
    let mut values = json!({"id": "c1", "title": "t"});
    values.as_object_mut().unwrap().extend(fields.as_object().unwrap().clone());
    RowValue { row_id: "c1".into(), values: values.to_string() }
}

fn reopened_at(path: &std::path::Path) -> Arc<Store> {
    Arc::new(Store::open_models(Some(path), all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap())
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
    use super::sets_reference_tests::stored_sets;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("recovered.sqlite");
    {
        let db = with_confirmed_child(&path);
        write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "messages".into(), rows: vec![message("temp-00000000-0000-0000-0000-310000000000", 5)] }]);
        assert_eq!(stored_sets(&db).id.as_deref(), Some("temp-00000000-0000-0000-0000-310000000000"), "the prediction moved the pointer before the process was killed");
    }
    let db = reopened_at(&path);
    assert_eq!(temp_rows(&db), 0, "the open removed the row of the lost operation");
    assert_eq!(stored_sets(&db), shown("m1", 1, None, "m1"), "the parent has no pointer to the removed row");
}

/// data_sets_recovered_at_open_returned (P-DATA-56, D-338), (b): the open returns the row of an open operation and the fields of the parent count it.
#[test]
fn data_sets_recovered_at_open_returned() {
    use super::sets_reference_tests::stored_sets;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("returned.sqlite");
    {
        let db = with_confirmed_child(&path);
        intent_creating(&db, 7, &message("temp-00000000-0000-0000-0000-320000000000", 6));
        assert_eq!(stored_sets(&db).id.as_deref(), Some("temp-00000000-0000-0000-0000-320000000000"), "the operation predicts the child");
        crate::store::fixtures::exec(&db, "DELETE FROM row_messages WHERE id = 'temp-00000000-0000-0000-0000-320000000000'");
        crate::store::fixtures::exec(&db, "UPDATE op SET temp_id = 'temp-00000000-0000-0000-0000-320000000000' WHERE seq = 7");
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "chats".into(), rows: vec![parent_value(json!({"lastMessageId": "m1", "lastMessageAt": 1}))] }]);
        assert_eq!(stored_sets(&db).id.as_deref(), Some("m1"), "the parent counts the children that are stored");
    }
    let db = reopened_at(&path);
    assert_eq!(temp_rows(&db), 1, "the open returned the row of the open operation");
    assert_eq!(stored_sets(&db), shown("temp-00000000-0000-0000-0000-320000000000", 6, None, "temp-00000000-0000-0000-0000-320000000000"), "the fields of the parent count the returned child");
}

pub(super) fn parent_operation(db: &Arc<Store>, op_id: u64, action: &str, row: &RowValue) {
    let effects = crate::owners::store::IntentEffects { rows: vec![crate::owners::store::EffectRow { model: "chats".into(), row_id: row.row_id.clone(), values: row.values.clone() }], ..Default::default() };
    write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { created_at: 0, attempts: 1, cycle_id: 1, op_id, model: "chats".into(), row_id: row.row_id.clone(), action: action.into(), payload: row.values.clone(), effects, origin: crate::owners::store::IntentOrigin::Api }]);
}

pub(super) fn local_parent(db: &Arc<Store>, path: &str, values: Value) {
    let row = RowValue { row_id: values["id"].as_str().unwrap().into(), values: values.to_string() };
    match path {
        "rows" => write(db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "chats".into(), rows: vec![row] }]),
        "operation" => parent_operation(db, 7, "create", &row),
        other => panic!("no path {other}"),
    }
}

/// P-DATA-57, P-DATA-59 (d), D-400: the child that was the newest is the first of the one SQL order of the children, with a NULL in `newer:` and with a temporary id too.
#[test]
fn data_sets_destroy_marks_the_parent_stale_only_for_the_sql_winner() {
    let nulls: Vec<(&str, i64, Option<i64>)> = vec![("m1", 2, Some(1)), ("m2", 2, Some(1)), ("m3", 5, None), ("m4", 5, Some(0))];
    let large: &[(&str, i64, Option<i64>)] = &[("7", 2, Some(9_007_199_254_740_992)), ("6", 2, Some(9_007_199_254_740_993))];
    let cases: [(&str, &[(&str, i64, Option<i64>)], &str, bool, &str); 7] = [
        ("the winner with NULLs", &nulls, "m2", true, "m1"),
        ("a child with the newer time and no number", &nulls, "m3", false, "m2"),
        ("a child with the lower number", &nulls, "m4", false, "m2"),
        ("a confirmed child with the larger id behind a temporary one", &[("zz", 5, None), ("temp-00000000-0000-0000-0000-310000000000", 5, None)], "zz", false, "temp-00000000-0000-0000-0000-310000000000"),
        ("the temporary winner", &[("zz", 5, None), ("temp-00000000-0000-0000-0000-310000000000", 5, None)], "temp-00000000-0000-0000-0000-310000000000", true, "zz"),
        ("the larger number beyond 2^53 before the id", large, "6", true, "7"),
        ("the smaller number beyond 2^53 behind the larger id", large, "7", false, "6"),
    ];
    for (name, children, destroyed, stale, winner_after) in cases {
        for order in permutations(&(0..children.len()).collect::<Vec<_>>()) {
            let db = open();
            for index in &order {
                let (id, at, seq) = children[*index];
                if id.starts_with("temp-") {
                    write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "messages".into(), rows: vec![message(id, at)] }]);
                } else {
                    land_children(&db, vec![child(id, "c1", at, seq)]);
                }
            }
            intent_hiding(&db, 1, "messages", destroyed);
            let written = write_as(&db, 2, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveIntent { op_id: 1 }]);
            assert_eq!(!written.stale_parents.is_empty(), stale, "{name}, landed as {order:?}: the windows of the parent are stale only when the destroyed child was the newest");
            assert_eq!(sets(&db).id.as_deref(), Some(winner_after), "{name}, landed as {order:?}: the newest child after the destroy");
        }
    }
}
