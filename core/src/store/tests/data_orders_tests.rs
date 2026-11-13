//! Orders of independent inputs for `data.md`: server rows, local edits, ephemeral values, ties, order keys, sync state. Requirement -> trace -> observed value.

use serde_json::{json, Value};

use super::data_inv_tests::{attach, ephemeral_specs, field, intent, open, server_row, set_typing, typing, write};
use crate::owners::contract::BatchAuthor;
use crate::owners::store::{EffectRow, IntentEffects, IntentOrigin, RowValue, StoreBatch, StoreOp};
use crate::store::fixtures::put;
use crate::store::query::{Order, Query, Where};
use crate::store::Store;
use crate::test_wait::permutations;
use crate::value::SortDirection;
use std::sync::Arc;

#[test]
fn data_orders_server_rows_of_disjoint_fields_by_path() {
    let rows = [json!({"id": "m1", "text": "new"}), json!({"id": "m1", "userId": null})];
    let mut traces = 0;
    for first_path in ["answer", "event", "page"] {
        for second_path in ["answer", "event", "page"] {
            for reversed in [false, true] {
                let db = open();
                put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
                let steps = [(first_path, &rows[0]), (second_path, &rows[1])];
                for (path, row) in if reversed { [steps[1], steps[0]] } else { steps } { server_row(path, &db, row.clone()); }
                assert_eq!((field(&db, "text"), field(&db, "userId")), (json!("new"), Value::Null), "{first_path} {second_path} reversed {reversed}");
                traces += 1;
            }
        }
    }
    assert_eq!(traces, 18);
}

#[test]
fn data_orders_server_row_and_unconfirmed_local_edit() {
    #[derive(Clone, Copy, PartialEq, Debug)]
    enum Event { Edit, Server, Confirm }
    use Event::*;
    let orders: Vec<_> = permutations(&[Edit, Server, Confirm]).into_iter().filter(|order| order.iter().position(|e| *e == Edit) < order.iter().position(|e| *e == Confirm)).collect();
    assert_eq!(orders.len(), 3);
    for path in ["answer", "event", "page"] {
        for order in &orders {
            let db = open();
            put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
            for event in order {
                match event {
                    Edit => write(&db, BatchAuthor::Outbox, Vec::new(), vec![intent(5)]),
                    Server => server_row(path, &db, json!({"id": "m1", "text": "server value"})),
                    Confirm => write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveIntent { op_id: 5 }]),
                }
            }
            let server_after_confirm = order.iter().position(|e| *e == Server) > order.iter().position(|e| *e == Confirm);
            let expected = if server_after_confirm { "server value" } else { "edit" };
            assert_eq!(field(&db, "text"), json!(expected), "{path} {order:?}");
        }
    }
}

#[test]
fn data_orders_real_row_around_the_create_answer_and_the_file() {
    #[derive(Clone, Copy, PartialEq, Debug)]
    enum Event { Create, Attach, Real, Answer }
    use Event::*;
    let before = |order: &[Event], a: Event, b: Event| order.iter().position(|e| *e == a) < order.iter().position(|e| *e == b);
    let orders: Vec<_> = permutations(&[Create, Attach, Real, Answer]).into_iter().filter(|order| before(order, Create, Attach) && before(order, Attach, Answer) && before(order, Create, Answer)).collect();
    assert_eq!(orders.len(), 4);
    for order in orders {
        let db = open();
        let temp = crate::identity::RowId::generate_temp(None).to_string();
        for event in &order {
            match event {
                Create => {
                    let values = json!({"id": temp, "userId": "u1", "text": "a"}).to_string();
                    let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: temp.clone(), values }], ..Default::default() };
                    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { created_at: 0, attempts: 1, cycle_id: 1, op_id: 1, model: "moments".into(), row_id: temp.clone(), action: "create".into(), payload: "{}".into(), effects, origin: IntentOrigin::Api }]);
                }
                Attach => write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![attach(&temp, vec![7; 40])]),
                Real => put(&db, "moments", json!({"id": "m9", "userId": "u1", "text": "a"})),
                Answer => {
                    let answer = RowValue { row_id: "m9".into(), values: json!({"id": "m9", "userId": "u1", "text": "a"}).to_string() };
                    write(&db, BatchAuthor::Outbox, Vec::new(), vec![
                        StoreOp::Rows { model: "moments".into(), rows: vec![answer] },
                        StoreOp::Swap { model: "moments".into(), temp: temp.clone(), real: "m9".into() },
                        StoreOp::RemoveIntent { op_id: 1 },
                    ]);
                }
            }
        }
        let id = |text: &str| crate::identity::RowId::new(text).unwrap();
        assert!(db.read(&["moments"], |reader| reader.point("moments", &temp)).unwrap().is_none(), "{order:?}: the temp row is gone");
        assert_eq!(db.count(&Query::new("moments")).unwrap(), 1, "{order:?}: one row stays");
        assert_eq!(db.url("moments", &id("m9"), "media").unwrap().map(|path| std::fs::read(path).unwrap()), Some(vec![7; 40]), "{order:?}: the real row owns the bytes");
        assert!(db.read(&["moments"], |reader| Ok(crate::store::files::find(reader.connection(), "moments", &temp, "media")?)).unwrap().is_none(), "{order:?}: the temporary row owns none");
    }
}

fn reset_batch(reset: &crate::owners::store::EphemeralReset) -> StoreBatch {
    StoreBatch { author: BatchAuthor::Outbox, id: 2, stamp: crate::runtime::EpochStamp::Epoch(1), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::ResetEphemeral { model: reset.model.clone(), row_id: reset.row_id.clone(), field: reset.field.clone(), position: reset.position }] }
}

#[test]
fn data_orders_ephemeral_write_row_and_clock() {
    #[derive(Clone, Copy, PartialEq, Debug)]
    enum Event { Row, Write, Clock }
    use Event::*;
    let orders: Vec<_> = permutations(&[Row, Write, Clock]).into_iter().filter(|order| order.iter().position(|e| *e == Write) < order.iter().position(|e| *e == Clock)).collect();
    assert_eq!(orders.len(), 3);
    for order in orders {
        let db = Arc::new(Store::open_models(None, ephemeral_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
        let (mut row, mut written, mut clocked) = (false, None, false);
        let mut positions = Vec::new();
        for event in &order {
            match event {
                Row => { put(&db, "moments", json!({"id": "m9", "userId": "u1"})); row = true; }
                Write => {
                    let before = db.revision_seq().unwrap();
                    let batch = set_typing(&db, "m9", true);
                    assert_eq!(db.revision_seq().unwrap(), before, "{order:?}: a write is no database revision");
                    positions.push((batch.revision, batch.notice.position));
                    written = Some(batch);
                }
                Clock => {
                    let reset = written.as_ref().unwrap().resets[0].clone();
                    let before = db.revision_seq().unwrap();
                    let batch = db.write_batch(&reset_batch(&reset), db.files(), None).unwrap();
                    assert_eq!(db.revision_seq().unwrap(), before, "{order:?}: a reset is no database revision");
                    positions.push((batch.revision, batch.notice.position));
                    clocked = true;
                }
            }
            let expected = if !row { None } else if written.is_some() && !clocked { Some(json!(true)) } else { Some(json!(false)) };
            assert_eq!(typing(&db, "m9"), expected, "{order:?} after {event:?}");
        }
        assert!(positions.windows(2).all(|pair| pair[0] < pair[1]), "{order:?}: (revision, position) grows with every write and reset: {positions:?}");
    }
}

#[test]
fn data_orders_ephemeral_write_against_destroy_and_erase() {
    #[derive(Clone, Copy, PartialEq, Debug)]
    enum Event { Write, Remove, Return }
    use Event::*;
    let orders: Vec<_> = permutations(&[Write, Remove, Return]).into_iter().filter(|order| order.iter().position(|e| *e == Remove) < order.iter().position(|e| *e == Return)).collect();
    assert_eq!(orders.len(), 3);
    for erase in [false, true] {
        for order in &orders {
            let db = Arc::new(Store::open_models(None, ephemeral_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
            put(&db, "moments", json!({"id": "m1", "userId": "u1"}));
            for event in order {
                match event {
                    Write => { set_typing(&db, "m1", true); }
                    Remove if erase => { db.erase_account(db.generation().next(), false).unwrap(); }
                    Remove => {
                        let delete = json!({"op": "delete", "id": "m1"}).to_string();
                        let batch = StoreBatch { author: BatchAuthor::Feed, id: 2, stamp: crate::runtime::EpochStamp::Epoch(1), rows: vec![("moments".into(), delete)], intents: 0, credential: None, ops: Vec::new() };
                        db.write_batch(&batch, db.files(), None).unwrap();
                    }
                    Return => put(&db, "moments", json!({"id": "m1", "userId": "u1"})),
                }
            }
            let written_first = order.iter().position(|e| *e == Write) < order.iter().position(|e| *e == Remove);
            let expected = if written_first { json!(false) } else { json!(true) };
            assert_eq!(typing(&db, "m1"), Some(expected), "erase {erase} {order:?}: a removal drops the values written before it, a later value waits for the row");
        }
    }
}

#[test]
fn data_orders_equal_keys_in_every_insertion_order() {
    let ids = ["b", "a", "temp-9", "temp-1", "c"];
    let orders = permutations(&ids);
    assert_eq!(orders.len(), 120);
    for order in orders {
        let db = open();
        for id in &order { put(&db, "moments", json!({"id": id, "userId": "u1", "text": "same"})); }
        let ordered = Query { order: vec![Order::new("text", SortDirection::Asc)], ..Query::new("moments") };
        let found: Vec<String> = db.query(&ordered).unwrap().iter().map(|row| row.get("id").and_then(Value::as_str).unwrap().to_string()).collect();
        assert_eq!(found, ["temp-1", "temp-9", "a", "b", "c"], "insertion order {order:?}");
    }
}

fn intent_on(op_id: u64, row_id: &str) -> StoreOp {
    let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: row_id.into(), values: json!({"id": row_id, "userId": "u1", "text": format!("edit{op_id}")}).to_string() }], ..Default::default() };
    StoreOp::Intent { created_at: 0, attempts: 1, cycle_id: 1, op_id, model: "moments".into(), row_id: row_id.into(), action: "update".into(), payload: "{}".into(), effects, origin: IntentOrigin::Api }
}

#[test]
fn data_orders_row_sync_follows_the_open_operations() {
    #[derive(Clone, Copy, PartialEq, Debug)]
    enum Event { Open(u64), Close(u64) }
    use Event::*;
    let rows = [(5, "m1"), (6, "m1"), (7, "m2")];
    let events: Vec<Event> = rows.iter().flat_map(|(op, _)| [Open(*op), Close(*op)]).collect();
    let orders: Vec<_> = permutations(&events).into_iter().filter(|order| rows.iter().all(|(op, _)| order.iter().position(|e| *e == Open(*op)) < order.iter().position(|e| *e == Close(*op)))).collect();
    assert_eq!(orders.len(), 90);
    let pending = Query::new("moments").filtered(Where::equal("sync", json!("pending")));
    for order in orders {
        let db = open();
        for id in ["m1", "m2"] { put(&db, "moments", json!({"id": id, "userId": "u1", "text": "old"})); }
        let mut open_ops: Vec<u64> = Vec::new();
        for event in &order {
            match *event {
                Open(op) => { let row = rows.iter().find(|(id, _)| *id == op).unwrap().1; write(&db, BatchAuthor::Outbox, Vec::new(), vec![intent_on(op, row)]); open_ops.push(op); }
                Close(op) => { write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveIntent { op_id: op }]); open_ops.retain(|open| *open != op); }
            }
            let mut expected: Vec<&str> = rows.iter().filter(|(op, _)| open_ops.contains(op)).map(|(_, row)| *row).collect();
            expected.dedup();
            let mut found: Vec<String> = db.query(&pending).unwrap().iter().map(|row| row.get("id").and_then(Value::as_str).unwrap().to_string()).collect();
            found.sort();
            assert_eq!((found, db.count(&pending).unwrap()), (expected.iter().map(|id| id.to_string()).collect(), expected.len() as i64), "{order:?} after {event:?}");
        }
    }
}

#[test]
fn data_orders_removed_member_and_a_reused_order_key() {
    use crate::identity::{ListKey, ModelKey};
    use crate::store::{BatchPlan, ScopeOp, ScopeRef};
    #[derive(Clone, Copy, PartialEq, Debug)]
    enum Event { Remove, Reuse, Restore }
    use Event::*;
    let orders: Vec<_> = permutations(&[Remove, Reuse, Restore]).into_iter().filter(|order| order.iter().position(|e| *e == Remove) < order.iter().position(|e| *e == Restore)).collect();
    assert_eq!(orders.len(), 3);
    for order in orders {
        let db = open();
        let scope = ScopeRef::new(ModelKey::new("moments"), ListKey::from_raw("feed"));
        for id in ["a", "b", "x"] { put(&db, "moments", json!({"id": id, "userId": "u1", "text": id})); }
        let first = crate::value::fractional_between(None, None).unwrap();
        let second = crate::value::fractional_between(Some(&first), None).unwrap();
        let append = |id: &str, key: &str| {
            let mut envelope = BatchPlan::from_source(crate::schema::RowSource::Event);
            envelope.scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: crate::store::fixtures::id(id), order_key: key.into() });
            db.commit(envelope).unwrap();
        };
        append("a", &first);
        append("b", &second);
        for event in &order {
            match event {
                Remove => {
                    let leave = crate::owners::store::EffectMember { model: "moments".into(), list: "feed".into(), row_id: "b".into(), order_key: String::new(), leave: true };
                    let effects = IntentEffects { tombstones: vec![crate::owners::store::EffectTombstone { model: "moments".into(), row_id: "b".into() }], members: vec![leave], ..Default::default() };
                    write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::Intent { created_at: 0, attempts: 1, cycle_id: 1, op_id: 3, model: "moments".into(), row_id: "b".into(), action: "destroy".into(), payload: "{}".into(), effects, origin: IntentOrigin::Api }]);
                }
                Reuse => append("x", &second),
                Restore => write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::RollBack { op_id: 3 }, StoreOp::RemoveIntent { op_id: 3 }]),
            }
        }
        let members = db.scope_members("moments", "feed").unwrap();
        let mut ids: Vec<&str> = members.iter().map(|(id, _)| id.as_str()).collect();
        ids.sort();
        let mut keys: Vec<&str> = members.iter().map(|(_, key)| key.as_str()).collect();
        keys.sort();
        keys.dedup();
        assert_eq!((ids, keys.len()), (vec!["a", "b", "x"], 3), "{order:?}: every member keeps its own key: {members:?}");
    }
}

#[test]
fn data_orders_children_of_a_pointer_in_every_path() {
    use super::data_sets_tests::{chat, message, open as open_chat, write as write_chat};
    use crate::owners::read::windows::WindowKey;
    let children = [("m1", 1), ("m2", 2), ("m3", 3)];
    let mut traces = 0;
    for path in ["local", "event", "page", "answer"] {
        for order in permutations(&children) {
            let db = open_chat();
            for (id, at) in &order {
                match path {
                    "local" => write_chat(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "messages".into(), rows: vec![message(id, *at)] }]),
                    "event" => write_chat(&db, BatchAuthor::Feed, vec![("messages".into(), json!({"op": "upsert", "id": id, "row": {"id": id, "chatId": "c1", "body": id, "createdAt": at}}).to_string())], Vec::new()),
                    "page" => {
                        let key = WindowKey::nodes("messages", &[id]);
                        write_chat(&db, BatchAuthor::Window, Vec::new(), vec![StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: vec![message(id, *at)], related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() }]);
                    }
                    _ => write_chat(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "messages".into(), rows: vec![message(id, *at)] }]),
                }
            }
            assert_eq!(chat(&db), json!(["m3", 3]), "{path} {order:?}: the pointer ends on the newest child whatever the arrival order");
            traces += 1;
        }
    }
    assert_eq!(traces, 24);
}
