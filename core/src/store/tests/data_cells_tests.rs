use std::sync::Arc;

use serde_json::{json, Value};

use crate::owners::contract::BatchAuthor;
use crate::owners::read::windows::WindowKey;
use crate::owners::store::{RowValue, StoreBatch, StoreOp};
use crate::schema::{FieldSpec, ModelSpec};
use crate::store::Store;
use crate::value::ScalarKind;

fn write(db: &Arc<Store>, author: BatchAuthor, rows: Vec<(String, String)>, ops: Vec<StoreOp>) {
    let batch = StoreBatch { author, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows, intents: 0, credential: None, ops };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
}

fn doc(revision: i64, text: &str) -> RowValue {
    RowValue { row_id: "d1".into(), values: json!({"id": "d1", "rev": revision, "text": text}).to_string() }
}

fn text(db: &Store) -> Value {
    db.read(&["docs"], |reader| reader.point("docs", "d1")).unwrap().and_then(|row| row.get("text").cloned()).unwrap_or(Value::Null)
}

#[test]
fn data_server_revision_drops_older() {
    let spec = ModelSpec { server_revision: Some("rev".into()), ..ModelSpec::new("docs", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("rev", ScalarKind::Int).optional(), FieldSpec::scalar("text", ScalarKind::Str).optional()]) };
    let db = Arc::new(Store::open_models(None, vec![spec], None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    let rows = |row: RowValue| vec![StoreOp::Rows { model: "docs".into(), rows: vec![row] }];
    write(&db, BatchAuthor::Outbox, Vec::new(), rows(doc(5, "first")));
    assert_eq!(text(&db), json!("first"), "no stored revision: the first version lands");
    write(&db, BatchAuthor::Outbox, Vec::new(), rows(doc(3, "older answer")));
    assert_eq!(text(&db), json!("first"), "an answer below the stored revision is dropped");
    let event = |row: &RowValue| (String::from("docs"), json!({"op": "upsert", "id": "d1", "row": serde_json::from_str::<Value>(&row.values).unwrap()}).to_string());
    write(&db, BatchAuthor::Feed, vec![event(&doc(4, "older event"))], Vec::new());
    assert_eq!(text(&db), json!("first"), "an event below it is dropped");
    let key = WindowKey::nodes("docs", &["d1"]);
    let page = |row: RowValue, base: i64| vec![StoreOp::Window { landed_at: 0, key: key.name.clone(), model: key.model.clone(), page: 1, replace_all: false, cursor: None, roots: vec![row], related: Vec::new(), total: None, base_revision: base }];
    write(&db, BatchAuthor::Window, Vec::new(), page(doc(2, "older page"), db.revision_seq().unwrap()));
    assert_eq!(text(&db), json!("first"), "a page below it is dropped");
    write(&db, BatchAuthor::Outbox, Vec::new(), rows(doc(5, "equal")));
    assert_eq!(text(&db), json!("equal"), "an equal revision lands");
    write(&db, BatchAuthor::Outbox, Vec::new(), rows(doc(6, "newer")));
    assert_eq!(text(&db), json!("newer"), "a greater revision lands");
    write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "docs".into(), rows: vec![doc(1, "optimistic")] }]);
    assert_eq!(text(&db), json!("optimistic"), "an optimistic write does not check the rule");
}

fn open_moments() -> Arc<Store> {
    let db = Arc::new(Store::open_models(None, crate::store::fixtures::all_specs(), None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    crate::store::fixtures::put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
    crate::store::fixtures::put(&db, "vibes", json!({"id": "m1", "momentId": "m1", "kind": "like"}));
    db
}

fn moment_page(db: &Store, text: &str) -> StoreOp {
    let key = WindowKey::nodes("moments", &["m1"]);
    let root = RowValue { row_id: "m1".into(), values: json!({"id": "m1", "userId": "u1", "text": text}).to_string() };
    StoreOp::Window { landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: vec![root], related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() }
}

fn moment_text(db: &Store) -> Value {
    db.read(&["moments"], |reader| reader.point("moments", "m1")).unwrap().and_then(|row| row.get("text").cloned()).unwrap_or(Value::Null)
}

#[test]
fn out_prop_other_model_same_id() {
    let db = open_moments();
    let effects = crate::owners::store::IntentEffects { rows: vec![crate::owners::store::EffectRow { model: "vibes".into(), row_id: "m1".into(), values: json!({"id": "m1", "momentId": "m1", "kind": "wow"}).to_string() }], ..Default::default() };
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { attempts: 1, cycle_id: 1, op_id: 3, model: "vibes".into(), row_id: "m1".into(), action: "react".into(), payload: "{}".into(), effects, origin: crate::owners::store::IntentOrigin::Api }]);
    write(&db, BatchAuthor::Window, Vec::new(), vec![moment_page(&db, "new")]);
    assert_eq!(moment_text(&db), json!("new"), "an open operation of vibes does not protect the moment with the same id");
}

#[test]
fn out_prop_destroy_hides_row() {
    let effects = || crate::owners::store::IntentEffects { tombstones: vec![crate::owners::store::EffectTombstone { model: "moments".into(), row_id: "m1".into() }], ..Default::default() };
    for via_event in [false, true] {
        let db = open_moments();
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { attempts: 1, cycle_id: 1, op_id: 4, model: "moments".into(), row_id: "m1".into(), action: "destroy".into(), payload: "{}".into(), effects: effects(), origin: crate::owners::store::IntentOrigin::Api }]);
        assert_eq!(moment_text(&db), Value::Null, "the destroy hides the row at once");
        if via_event {
            let event = json!({"op": "upsert", "id": "m1", "row": {"id": "m1", "userId": "u1", "text": "again"}}).to_string();
            write(&db, BatchAuthor::Feed, vec![("moments".into(), event)], Vec::new());
        } else {
            write(&db, BatchAuthor::Window, Vec::new(), vec![moment_page(&db, "again")]);
        }
        assert_eq!(moment_text(&db), Value::Null, "via_event={via_event}: a landing does not show the row before the outcome of the destroy");
    }
}

#[test]
fn out_missing_destroy_server_removal() {
    for optimistic in [false, true] {
        let db = open_moments();
        let effects = crate::owners::store::IntentEffects { tombstones: if optimistic { vec![crate::owners::store::EffectTombstone { model: "moments".into(), row_id: "m1".into() }] } else { Vec::new() }, ..Default::default() };
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { attempts: 1, cycle_id: 1, op_id: 7, model: "moments".into(), row_id: "m1".into(), action: "destroy".into(), payload: "{}".into(), effects, origin: crate::owners::store::IntentOrigin::Api }]);
        let batch = StoreBatch { author: BatchAuthor::Outbox, id: 2, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::RemoveServerRows { model: "moments".into(), ids: vec!["m1".into()] }, StoreOp::RemoveIntent { op_id: 7 }] };
        let result = db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
        assert!(result.removed_rows.contains(&("moments".into(), "m1".into())), "optimistic={optimistic}: {:?}", result.removed_rows);
        assert_eq!(moment_text(&db), Value::Null);
    }
}

#[test]
fn out_server_removed_row() {
    let db = open_moments();
    let effects = crate::owners::store::IntentEffects { rows: vec![crate::owners::store::EffectRow { model: "moments".into(), row_id: "m1".into(), values: json!({"id": "m1", "userId": "u1", "text": "edit"}).to_string() }], ..Default::default() };
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { attempts: 1, cycle_id: 1, op_id: 7, model: "moments".into(), row_id: "m1".into(), action: "update".into(), payload: "{}".into(), effects, origin: crate::owners::store::IntentOrigin::Api }]);
    let open = |db: &Store| db.lock().connection.query_row("SELECT COUNT(*) FROM op", [], |row| row.get::<_, i64>(0)).unwrap();
    assert_eq!(open(&db), 1);
    let event = json!({"op": "delete", "id": "m1"}).to_string();
    let batch = StoreBatch { author: BatchAuthor::Feed, id: 2, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: vec![("moments".into(), event)], intents: 0, credential: None, ops: Vec::new() };
    let written = db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    assert_eq!(written.removed_operations, vec![7], "the operation of the removed row leaves with the row");
    let mut removed = written.removed_rows.clone();
    removed.sort();
    assert_eq!(removed, vec![("moments".to_string(), "m1".to_string()), ("vibes".to_string(), "m1".to_string())], "the Outbox learns the removed row and its cascade child, whatever the stage of its operations");
    assert_eq!(open(&db), 0);
    assert_eq!(moment_text(&db), Value::Null);
}

#[test]
fn out_local_destroy_is_not_a_server_removal() {
    let db = open_moments();
    let effects = crate::owners::store::IntentEffects { tombstones: vec![crate::owners::store::EffectTombstone { model: "moments".into(), row_id: "m1".into() }], ..Default::default() };
    let batch = StoreBatch { author: BatchAuthor::Outbox, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::Intent { attempts: 1, cycle_id: 1, op_id: 4, model: "moments".into(), row_id: "m1".into(), action: "destroy".into(), payload: "{}".into(), effects, origin: crate::owners::store::IntentOrigin::Api }] };
    let written = db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    assert!(written.removed_rows.is_empty(), "a local destroy of the app is not a server removal: its operation must not end Discarded");
}

fn feed(db: &Store, events: Vec<Value>) -> crate::store::batch_write::BatchWritten {
    let rows = events.into_iter().map(|event| (event["model"].as_str().unwrap().to_string(), event.to_string())).collect();
    let batch = StoreBatch { author: BatchAuthor::Feed, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows, intents: 0, credential: None, ops: Vec::new() };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap()
}

#[test]
fn feed_event_delete_cascade() {
    let db = open_moments();
    crate::store::fixtures::put(&db, "vibes", json!({"id": "v2", "momentId": "m1", "kind": "wow"}));
    let count = |model: &str| db.read(&[model], |reader| Ok(reader.point(model, "m1")?.is_some() as usize + reader.point(model, "v2")?.is_some() as usize)).unwrap();
    assert_eq!((count("moments"), count("vibes")), (1, 2));
    feed(&db, vec![json!({"op": "delete", "model": "moments", "id": "m1"})]);
    assert_eq!((count("moments"), count("vibes")), (0, 0), "the event removes the row and its dependent children like a local destroy");
}

#[test]
fn feed_event_touch_parent_once() {
    let mut specs = crate::store::fixtures::all_specs();
    for association in specs.iter_mut().filter(|spec| spec.key.as_str() == "messages").flat_map(|spec| spec.associations.iter_mut()) {
        association.sets.clear();
        association.newer.clear();
    }
    let db = Arc::new(Store::open_models(None, specs, None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    crate::store::fixtures::put(&db, "chats", json!({"id": "c1"}));
    let revision_of = |id: &str| db.read(&["chats"], |reader| Ok(reader.stored_row("chats", id)?.map(|row| row.revision))).unwrap();
    let before = revision_of("c1");
    let message = |id: &str, chat: &str| json!({"op": "upsert", "model": "messages", "id": id, "row": {"id": id, "chatId": chat}});
    let written = feed(&db, vec![message("a", "c1"), message("b", "c1"), message("c", "c2")]);
    assert_ne!(before, revision_of("c1"));
    assert_eq!(revision_of("c1"), i64::try_from(written.revision).ok(), "the resident parent is touched at the revision of the batch");
    assert_eq!(revision_of("c2"), None, "an absent parent is skipped, not created");
}

unsafe extern "C" fn count_batch_reads(kind: u32, context: *mut std::ffi::c_void, statement: *mut std::ffi::c_void, _: *mut std::ffi::c_void) -> i32 {
    if kind == rusqlite::ffi::SQLITE_TRACE_STMT {
        let sql = unsafe { rusqlite::ffi::sqlite3_sql(statement.cast()) };
        if !sql.is_null() && unsafe { std::ffi::CStr::from_ptr(sql) }.to_bytes().starts_with(b"SELECT") {
            unsafe { *context.cast::<usize>() += 1; }
        }
    }
    0
}

#[test]
fn feed_batch_select_work() {
    let measure = |size: usize| {
        let spec = ModelSpec::new("docs", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
        let db = Arc::new(Store::open_models(None, vec![spec], None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
        let events = (0..size).map(|i| ("docs".into(), json!({"op":"upsert", "id":format!("d{i}"), "row":{"id":format!("d{i}"), "text":"value"}, "observe":true}).to_string())).collect();
        let batch = StoreBatch { author: BatchAuthor::Feed, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: events, intents: 0, credential: None, ops: Vec::new() };
        let mut count = 0usize;
        let connection = unsafe { db.lock().connection.handle() };
        unsafe { rusqlite::ffi::sqlite3_trace_v2(connection, rusqlite::ffi::SQLITE_TRACE_STMT, Some(count_batch_reads), (&mut count as *mut usize).cast()); }
        let result = db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
        unsafe { rusqlite::ffi::sqlite3_trace_v2(connection, 0, None, std::ptr::null_mut()); }
        assert_eq!(result.observed.len(), size);
        assert!(result.observed.iter().all(|event| serde_json::from_str::<Value>(event).unwrap()["after"]["text"] == "value"));
        count
    };
    assert_eq!(measure(2), measure(12), "feed SELECT count must not grow with event count");
}

#[test]
fn store_read_intents_distinguishes_local_and_server_tombstones() {
    use crate::owners::store::{EffectRow, EffectTombstone, IntentEffects, IntentOrigin};
    use crate::schema::{ActionDeclaration, RootShape};
    let mut specs = crate::store::fixtures::all_specs();
    for spec in &mut specs {
        if spec.key.as_str() == "moments" || spec.key.as_str() == "vibes" {
            for name in ["update", "destroy"] {
                let mut action = ActionDeclaration::new(name, RootShape::Upsert);
                action.mutation = Some(format!("{name}Mutation"));
                spec.actions.push(action);
            }
        }
    }
    let db = Arc::new(Store::open_models(None, specs, None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    for (model, value) in [
        ("moments", json!({"id":"m1","userId":"u1","text":"parent"})),
        ("vibes", json!({"id":"v1","momentId":"m1","kind":"like"})),
        ("moments", json!({"id":"m2","userId":"u1","text":"server"})),
    ] { crate::store::fixtures::put(&db, model, value); }
    let intent = |op_id, model: &str, row_id: &str, action: &str, effects| StoreOp::Intent { attempts: 1, cycle_id: 1, op_id, model: model.into(), row_id: row_id.into(), action: action.into(), payload: "{}".into(), effects, origin: IntentOrigin::Api };
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![intent(1, "vibes", "v1", "update", IntentEffects { rows: vec![EffectRow { model: "vibes".into(), row_id: "v1".into(), values: json!({"kind":"love"}).to_string() }], ..Default::default() })]);
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![intent(2, "moments", "m2", "update", IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: "m2".into(), values: json!({"text":"edited"}).to_string() }], ..Default::default() })]);
    let local = [EffectTombstone { model: "moments".into(), row_id: "m1".into() }, EffectTombstone { model: "vibes".into(), row_id: "v1".into() }];
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![intent(3, "moments", "m1", "destroy", IntentEffects { tombstones: local.to_vec(), ..Default::default() })]);
    db.mark_tombstone("moments", "m2", 123, 1_000_000).unwrap();
    db.mark_tombstone("moments", "m1", 124, 1_000_000).unwrap();
    {
        let inner = db.lock();
        inner.connection.execute("INSERT INTO sync_upload (model, row_id, action, status, stage, sent, total, attempts, seq, transfer_id, cycle_id, completed, created_at, inline, size, sha256, mutation_path, mutation_body, on_failure) VALUES ('vibes', 'v1', 'upload', 'queued', 'prepare', 0, 1, 0, 1, 44, 1, 0, 0, 0, 1, 'sha', '/mutation', '{}', 'rollback')", []).unwrap();
        inner.connection.execute("INSERT INTO op_transfer (op_id, position, transfer_id) VALUES (1, 0, 44)", []).unwrap();
    }
    {
        let inner = db.lock();
        let mut query = inner.connection.prepare("SELECT model, row_id FROM op_effect WHERE seq = 3 AND kind = 'destroy' ORDER BY model, row_id").unwrap();
        let actual: Vec<(String, String)> = query.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().collect::<Result<_,_>>().unwrap();
        assert_eq!(actual, vec![("moments".into(), "m1".into()), ("vibes".into(), "v1".into())]);
    }
    for status in ["pending", "sending", "failed"] {
        db.lock().connection.execute("UPDATE op SET status = ?1 WHERE seq = 3", [status]).unwrap();
        let restored = db.read_restore().unwrap().ops.unwrap();
        let removed = |id| restored.iter().find(|op| op.op_id == id).unwrap().removed_rows.clone();
        assert_eq!(removed(1), Vec::new(), "{status}: cascade child");
        assert_eq!(removed(3), Vec::new(), "{status}: local destroy");
        assert_eq!(removed(2), vec![("moments".into(), "m2".into(), 123)], "{status}: server removal");
    }
}

fn docs_with_revision() -> Arc<Store> {
    let spec = ModelSpec { server_revision: Some("rev".into()), ..ModelSpec::new("docs", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("rev", ScalarKind::Int).optional(), FieldSpec::scalar("text", ScalarKind::Str).optional()]) };
    Arc::new(Store::open_models(None, vec![spec], None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap())
}

#[test]
fn data_event_without_revision_applies() {
    let db = docs_with_revision();
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "docs".into(), rows: vec![doc(9, "stored")] }]);
    let event = json!({"op": "upsert", "id": "d1", "row": {"id": "d1", "text": "event"}}).to_string();
    write(&db, BatchAuthor::Feed, vec![("docs".into(), event)], Vec::new());
    assert_eq!(text(&db), json!("event"), "an event without a revision applies over a stored revision");
}

