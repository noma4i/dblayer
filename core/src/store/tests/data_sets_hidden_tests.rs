use std::sync::Arc;

use serde_json::{json, Value};

use crate::owners::contract::BatchAuthor;
use crate::owners::store::{RowValue, StoreOp};
use crate::store::fixtures::{all_specs, put};
use crate::store::Store;

use super::data_sets_tests::*;
use super::sets_reference_tests::{stored_sets, Sets};

fn land_children(db: &Arc<Store>, rows: Vec<RowValue>) {
    write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows }]);
}

fn server_parent(db: &Arc<Store>, id: &str, at: i64, seq: i64) {
    let values = json!({"id": "c1", "title": "t", "lastMessageId": id, "lastMessageAt": at, "lastSequenceNumber": seq});
    write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "chats".into(), rows: vec![RowValue { row_id: "c1".into(), values: values.to_string() }] }]);
}

fn shown(id: &str, at: i64, seq: Option<i64>) -> Sets {
    Sets { id: Some(id.into()), at: Some(at), seq, body: Some(id.into()) }
}

/// A parent whose server value names `m1` (time 3, sequence 1), with the child `m1` and, when `other`, the older child `m0`.
fn parent_naming_a_child(other: bool) -> Arc<Store> {
    let db = open();
    server_parent(&db, "m1", 3, 1);
    land_children(&db, if other { vec![message("m1", 3), message("m0", 1)] } else { vec![message("m1", 3)] });
    assert_eq!(stored_sets(&db), shown("m1", 3, Some(1)), "the value of the server names the newest child");
    db
}

#[test]
fn data_sets_hidden_named_child_does_not_show() {
    for (other, expected) in [(true, shown("m0", 1, None)), (false, Sets::default())] {
        let db = parent_naming_a_child(other);
        intent_hiding(&db, 1, "messages", "m1");
        assert_eq!(stored_sets(&db), expected, "other child {other}: the child hidden by an open destroy is not shown by the parent");
    }
}

#[test]
fn data_sets_destroy_confirm_resets_named_server_value() {
    for other in [true, false] {
        let db = parent_naming_a_child(other);
        intent_hiding(&db, 1, "messages", "m1");
        let written = write_as(&db, 2, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveIntent { op_id: 1 }]);
        assert_eq!(written.stale_parents, ["chats".to_string()], "other child {other}: the confirm batch marks the windows of the parent");
        let expected = if other { shown("m0", 1, None) } else { Sets::default() };
        assert_eq!(stored_sets(&db), expected, "other child {other}: the confirm batch recomputes the parent without a later write");
        land_children(&db, vec![message("m5", 2)]);
        assert_eq!(stored_sets(&db), shown("m5", 2, None), "other child {other}: the value of the server is reset as a whole, the next older child wins");
    }
}

#[test]
fn data_sets_destroy_rollback_restores_named_child() {
    let db = parent_naming_a_child(true);
    intent_hiding(&db, 1, "messages", "m1");
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RollBack { op_id: 1 }, StoreOp::RemoveIntent { op_id: 1 }]);
    assert_eq!(stored_sets(&db), shown("m1", 3, Some(1)), "the rollback gives the parent its values as before the destroy");
}

fn store_without_newer() -> Arc<Store> {
    let specs = all_specs().into_iter().map(|mut spec| {
        if spec.key.as_str() == "messages" {
            spec.associations.iter_mut().for_each(|association| association.newer.clear());
        }
        spec
    }).collect();
    let db = Arc::new(Store::open_models(None, specs, None, Arc::new(crate::runtime::SystemClock)).unwrap());
    put(&db, "chats", json!({"id": "c1", "title": "t"}));
    db
}

#[test]
fn data_sets_newest_read_is_bounded() {
    let read = |db: Arc<Store>, count: i64| {
        let rows = (1..=count).map(|id| message(&id.to_string(), 5)).collect();
        land_children(&db, rows);
        let before = db.counters().pointer_rows_read;
        let edited = RowValue { row_id: "1".into(), values: json!({"id": "1", "chatId": "c1", "body": "edited", "createdAt": 5}).to_string() };
        land_children(&db, vec![edited]);
        let winner = chat(&db)[0].clone();
        assert_eq!(winner, Value::String(count.to_string()), "{count} children: the newest by the order of ids is the largest number");
        db.counters().pointer_rows_read - before
    };
    for (name, make) in [("newer with equal values", open as fn() -> Arc<Store>), ("no newer", store_without_newer)] {
        let (few, many) = (read(make(), 20), read(make(), 200));
        assert!(few <= 2 && many == few, "{name}: the rows read for one change do not depend on the number of children: {few} for 20, {many} for 200");
    }
}
