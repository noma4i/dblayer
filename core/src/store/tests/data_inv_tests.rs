//! Cells of `data.md` on the Store: queries, sync state, merge of server rows, order keys. Requirement -> observed value -> assert.

use std::sync::Arc;

use serde_json::{json, Value};

use crate::owners::contract::BatchAuthor;
use crate::owners::store::{EffectRow, IntentEffects, StoreBatch, StoreOp};
use crate::store::query::{Order, Query, Where};
use crate::owners::read::windows::WindowKey;
use crate::store::fixtures::{all_specs, moments_spec, put, row};
use crate::store::Store;
use crate::value::SortDirection;

pub(super) fn open() -> Arc<Store> { Arc::new(Store::open_models(None, all_specs(), None).unwrap()) }

pub(super) fn write(db: &Store, author: BatchAuthor, rows: Vec<(String, String)>, ops: Vec<StoreOp>) {
    let batch = StoreBatch { author, id: 1, epoch: 1, basis: Default::default(), rows, intents: 0, credential: None, ops };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
}

fn texts(db: &Store, query: &Query) -> Vec<Value> { db.query(query).unwrap().iter().map(|row| row.get("text").cloned().unwrap_or(Value::Null)).collect() }

#[test]
fn inv_row_06_count_exists_and_first_follow_the_query() {
    let db = open();
    for (id, text) in [("m1", "a"), ("m2", "b"), ("m3", "a")] { put(&db, "moments", json!({"id": id, "userId": "u1", "text": text})); }
    let wanted = Query::new("moments").filtered(Where::equal("text", json!("a")));
    assert_eq!(db.count(&wanted).unwrap(), 2);
    assert_eq!(db.count(&Query::new("moments").filtered(Where::equal("text", json!("zzz")))).unwrap(), 0, "exists is count > 0");
    let first = Query { limit: Some(1), order: vec![Order::new("text", SortDirection::Desc)], ..Query::new("moments") };
    assert_eq!(texts(&db, &first), [json!("b")], "first follows the order of the query");
}

pub(super) fn intent(op_id: u64) -> StoreOp {
    let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: "m1".into(), values: json!({"id": "m1", "userId": "u1", "text": "edit"}).to_string() }], ..Default::default() };
    StoreOp::Intent { op_id, model: "moments".into(), row_id: "m1".into(), action: "update".into(), payload: "{}".into(), effects, origin: crate::owners::store::IntentOrigin::Api }
}

#[test]
fn transfer_effects_become_intent_without_reapplying_or_losing_prior() {
    let db = open();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
    let StoreOp::Intent { op_id, model, row_id, action, payload, effects, .. } = intent(5) else { unreachable!() };
    write(&db, BatchAuthor::Transfer, Vec::new(), vec![StoreOp::Effects { op_id, model: model.clone(), row_id: row_id.clone(), action: action.clone(), payload: payload.clone(), effects }]);
    let state: String = db.read_locked(|connection| connection.query_row("SELECT intent_state FROM op WHERE seq = 5", [], |row| row.get(0))).unwrap();
    assert_eq!(state, "effects_only");
    assert_eq!(texts(&db, &Query::new("moments")), vec![json!("edit")]);
    write(&db, BatchAuthor::Transfer, Vec::new(), vec![StoreOp::Intent { op_id, model, row_id, action, payload, effects: Default::default(), origin: crate::owners::store::IntentOrigin::Transfer { transfer_id: 1 } }]);
    let state: String = db.read_locked(|connection| connection.query_row("SELECT intent_state FROM op WHERE seq = 5", [], |row| row.get(0))).unwrap();
    assert_eq!(state, "ready");
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RollBack { op_id }]);
    assert_eq!(texts(&db, &Query::new("moments")), vec![json!("old")]);
}

#[test]
fn transfer_outcome_is_written_with_intent_removal() {
    use crate::owners::write::outbox::OutboxOutcome;
    let db = open();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![intent(5)]);
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![
        StoreOp::TransferOutcome { transfer_id: 3, cycle_id: 2, outcome: OutboxOutcome::Confirmed { revision: 0, result: None, row_id: None } },
        StoreOp::RemoveIntent { op_id: 5 },
    ]);
    let (count, revision): (i64, i64) = db.read_locked(|connection| {
        let count = connection.query_row("SELECT COUNT(*) FROM op WHERE seq = 5", [], |row| row.get(0))?;
        let revision = connection.query_row("SELECT revision FROM transfer_outcome WHERE transfer_id = 3 AND cycle_id = 2 AND kind = 'confirmed'", [], |row| row.get(0))?;
        Ok((count, revision))
    }).unwrap();
    assert_eq!(count, 0);
    assert_eq!(revision, db.revision_seq().unwrap());
}

#[test]
fn store_retry_preserves_transfer_outcome() {
    use crate::owners::store::{IntentOrigin, UploadKind, UploadRow};
    use crate::owners::write::outbox::OutboxOutcome;
    use crate::schema::OnFailure;
    let db = open();
    put(&db, "moments", json!({"id":"m1", "userId":"u1", "text":"old"}));
    let row = UploadRow {
        key: "moments:m1:upload".into(), seq: 3, source: "/source".into(), parts: 1,
        batch: None, fields: None, upload_id: None, confirmed_parts: 1, attempts: 1,
        completed: true, failed: Some("refused".into()), created_at: 1, inline: false,
        size: 3, sha256: "hash".into(), path: "/mutation".into(), body: "{}".into(),
        on_failure: OnFailure::Keep, steps_path: "/steps".into(), steps_body: "{}".into(),
        transfer_id: 3, cycle_id: 1, signed_id: Some("signed".into()), aborted: false,
        op_id: Some(5), etags: vec![None],
    };
    write(&db, BatchAuthor::Transfer, Vec::new(), vec![StoreOp::Upload { row, kind: UploadKind::Failed }]);
    let StoreOp::Intent { op_id, model, row_id, action, payload, effects, .. } = intent(5) else { unreachable!() };
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { op_id, model, row_id, action, payload, effects, origin: IntentOrigin::Transfer { transfer_id: 3 } }]);
    let first = OutboxOutcome::Failed { error: "refused".into() };
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::TransferOutcome { transfer_id: 3, cycle_id: 1, outcome: first.clone() }]);
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::MarkError { op_id: 5, error: None }, StoreOp::Attempt { op_id: 5, attempts: 1, cycle_id: 2 }]);
    let second = OutboxOutcome::Discarded;
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::TransferOutcome { transfer_id: 3, cycle_id: 2, outcome: second.clone() }]);
    assert_eq!(db.read_restore().unwrap().transfer_outcomes.get(&3), Some(&[(1, first), (2, second)].into()));
    write(&db, BatchAuthor::Transfer, Vec::new(), vec![StoreOp::RemoveUpload { key: "moments:m1:upload".into() }]);
    assert!(!db.read_restore().unwrap().transfer_outcomes.contains_key(&3));
}

#[test]
fn inv_sync_02_queries_filter_by_sync_state_and_count_matches_rows() {
    let db = open();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
    put(&db, "moments", json!({"id": "m2", "userId": "u1", "text": "other"}));
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![intent(5)]);
    let pending = Query::new("moments").filtered(Where::equal("sync", json!("pending")));
    let synced = Query::new("moments").filtered(Where::equal("sync", json!("synced")));
    assert_eq!((texts(&db, &pending), db.count(&pending).unwrap()), ([json!("edit")].to_vec(), 1));
    assert_eq!((texts(&db, &synced), db.count(&synced).unwrap()), ([json!("other")].to_vec(), 1), "a row without a sync entry is synced");
}

#[test]
fn inv_sync_02_state_change_reaches_a_list_observer_and_a_scope_free_query() {
    let db = open();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
    let pending = Query::new("moments").filtered(Where::equal("sync", json!("pending")));
    let before = db.revision_seq().unwrap();
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![intent(6)]);
    assert!(db.revision_seq().unwrap() > before, "the state change is a commit that observers see");
    assert_eq!(db.count(&pending).unwrap(), 1);
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RollBack { op_id: 6 }, StoreOp::RemoveIntent { op_id: 6 }]);
    assert_eq!(db.count(&pending).unwrap(), 0, "the end of the operation moves the row out of the pending query");
}

pub(super) fn ephemeral_specs() -> Vec<crate::schema::ModelSpec> {
    let mut specs = all_specs();
    let field = crate::schema::FieldSpec::scalar("typing", crate::value::ScalarKind::Bool).with_default(json!(false));
    specs.iter_mut().find(|spec| spec.key.as_str() == "moments").unwrap().ephemeral.push(crate::schema::EphemeralSpec::new(field).resetting_after("5.seconds"));
    specs.push(crate::schema::ModelSpec { storage: crate::schema::Storage::Memory, ..crate::schema::ModelSpec::new("presence", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id)]) });
    specs
}

pub(super) fn set_typing(db: &Store, id: &str, on: bool) -> crate::store::batch_write::BatchWritten {
    let event = json!({"op": "ephemeral", "id": id, "values": {"typing": on}}).to_string();
    let batch = StoreBatch { author: BatchAuthor::Feed, id: 1, epoch: 1, basis: Default::default(), rows: vec![("moments".into(), event)], intents: 0, credential: None, ops: Vec::new() };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap()
}

pub(super) fn typing(db: &Store, id: &str) -> Option<Value> { db.read(&["moments"], |reader| Ok(reader.point("moments", id)?.and_then(|row| row.get("typing").cloned()))).unwrap() }

#[test]
fn inv_ephemeral_01_values_and_memory_rows_do_not_survive_a_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data.sqlite");
    {
        let db = Store::open_models(Some(&path), ephemeral_specs(), None).map(Arc::new).unwrap();
        put(&db, "moments", json!({"id": "m1", "userId": "u1"}));
        put(&db, "presence", json!({"id": "p1"}));
        set_typing(&db, "m1", true);
        assert_eq!(typing(&db, "m1"), Some(json!(true)));
    }
    let db = Store::open_models(Some(&path), ephemeral_specs(), None).map(Arc::new).unwrap();
    assert_eq!(typing(&db, "m1"), Some(json!(false)), "the value is back to the default");
    assert!(db.read(&["presence"], |reader| reader.point("presence", "p1")).unwrap().is_none(), "the memory row is gone");
}

#[test]
fn inv_ephemeral_04_a_write_reaches_observers_as_an_updated_row_without_a_database_revision() {
    let db = Arc::new(Store::open_models(None, ephemeral_specs(), None).unwrap());
    put(&db, "moments", json!({"id": "m1", "userId": "u1"}));
    let before = db.revision_seq().unwrap();
    let written = set_typing(&db, "m1", true);
    assert_eq!((db.revision_seq().unwrap(), written.revision), (before, u64::try_from(before).unwrap()), "no database revision");
    assert!(written.notice.position > 0, "the overlay version moves, observers read the row again at it");
}

#[test]
fn inv_ephemeral_04_the_position_grows_with_every_write_and_reset_while_the_revision_stays() {
    let db = Arc::new(Store::open_models(None, ephemeral_specs(), None).unwrap());
    put(&db, "moments", json!({"id": "m1", "userId": "u1"}));
    let first = set_typing(&db, "m1", true);
    let second = set_typing(&db, "m1", false);
    let reset = first.resets[0].clone();
    let batch = StoreBatch { author: BatchAuthor::Outbox, id: 2, epoch: 1, basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::ResetEphemeral { model: reset.model, row_id: reset.row_id, field: reset.field, position: reset.position }] };
    let third = db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    assert!(first.notice.position < second.notice.position && second.notice.position < third.notice.position, "{} {} {}", first.notice.position, second.notice.position, third.notice.position);
    assert_eq!((first.revision, second.revision, third.revision), (second.revision, second.revision, second.revision));
}

#[test]
fn inv_ephemeral_06_a_value_for_a_missing_row_waits_for_the_row_and_still_resets_by_the_clock() {
    let db = Arc::new(Store::open_models(None, ephemeral_specs(), None).unwrap());
    let written = set_typing(&db, "m9", true);
    assert_eq!(typing(&db, "m9"), None, "no row, nothing to show");
    put(&db, "moments", json!({"id": "m9", "userId": "u1"}));
    assert_eq!(typing(&db, "m9"), Some(json!(true)), "the value waited for the row");
    let reset = &written.resets[0];
    let batch = StoreBatch { author: BatchAuthor::Outbox, id: 2, epoch: 1, basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::ResetEphemeral { model: reset.model.clone(), row_id: reset.row_id.clone(), field: reset.field.clone(), position: reset.position }] };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    assert_eq!(typing(&db, "m9"), Some(json!(false)), "the clock reset applies");
}

#[test]
fn inv_ephemeral_06_destroying_a_row_removes_its_values() {
    let db = Arc::new(Store::open_models(None, ephemeral_specs(), None).unwrap());
    put(&db, "moments", json!({"id": "m1", "userId": "u1"}));
    set_typing(&db, "m1", true);
    let delete = json!({"op": "delete", "id": "m1"}).to_string();
    let batch = StoreBatch { author: BatchAuthor::Feed, id: 2, epoch: 1, basis: Default::default(), rows: vec![("moments".into(), delete)], intents: 0, credential: None, ops: Vec::new() };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    put(&db, "moments", json!({"id": "m1", "userId": "u1"}));
    assert_eq!(typing(&db, "m1"), Some(json!(false)), "a new row of the same id starts from the default");
}

#[test]
fn inv_ephemeral_06_reset_forgets_every_value() {
    let db = Arc::new(Store::open_models(None, ephemeral_specs(), None).unwrap());
    put(&db, "moments", json!({"id": "m1", "userId": "u1"}));
    set_typing(&db, "m1", true);
    db.erase_models(db.generation().next(), &["moments".to_string()]).unwrap();
    put(&db, "moments", json!({"id": "m1", "userId": "u1"}));
    assert_eq!(typing(&db, "m1"), Some(json!(false)));
}

#[test]
fn inv_ephemeral_07_ephemeral_fields_cannot_be_queried() {
    let specs = ephemeral_specs();
    let spec = specs.iter().find(|spec| spec.key.as_str() == "moments").unwrap();
    let filtered = Query::new("moments").filtered(Where::equal("typing", json!(true)));
    assert_eq!(crate::store::query::compile_keys(&filtered, spec).unwrap_err(), crate::store::query::QueryError::EphemeralField("typing".into()));
    let ordered = Query::new("moments").ordered(vec![Order::new("typing", SortDirection::Asc)]);
    assert_eq!(crate::store::query::compile_keys(&ordered, spec).unwrap_err(), crate::store::query::QueryError::EphemeralField("typing".into()));
}

#[test]
fn inv_ephemeral_08_values_for_rows_that_never_arrive_leave_no_memory_behind() {
    let db = Arc::new(Store::open_models(None, ephemeral_specs(), None).unwrap());
    for index in 0..5000 { set_typing(&db, &format!("ghost{index}"), true); }
    let held = db.overlays.get(db.revision_seq().unwrap()).unwrap().values.len();
    assert!(held <= 1024, "{held} values for rows that never arrived stay in memory");
}

#[test]
fn inv_order_02_equal_keys_sort_temp_rows_first_then_by_id_in_every_permutation() {
    let ids = ["b", "a", "temp-9", "temp-1", "c"];
    for shift in 0..ids.len() {
        let db = open();
        for index in 0..ids.len() { put(&db, "moments", json!({"id": ids[(index + shift) % ids.len()], "userId": "u1", "text": "same"})); }
        let ordered = Query { order: vec![Order::new("text", SortDirection::Asc)], ..Query::new("moments") };
        let found: Vec<String> = db.query(&ordered).unwrap().iter().map(|row| row.get("id").and_then(Value::as_str).unwrap().to_string()).collect();
        assert_eq!(found, ["temp-1", "temp-9", "a", "b", "c"], "insertion order shift {shift}");
    }
}

pub(super) fn server_row(path: &str, db: &Store, row: Value) {
    match path {
        "answer" => write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "moments".into(), rows: vec![crate::owners::store::RowValue { row_id: "m1".into(), values: row.to_string() }] }]),
        "event" => write(db, BatchAuthor::Feed, vec![("moments".into(), json!({"op": "upsert", "id": "m1", "row": row}).to_string())], Vec::new()),
        _ => {
            let key = crate::owners::read::windows::WindowKey::nodes("moments", &["m1"]);
            let page = StoreOp::Window { key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: vec![crate::owners::store::RowValue { row_id: "m1".into(), values: row.to_string() }], related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() };
            write(db, BatchAuthor::Window, Vec::new(), vec![page]);
        }
    }
}

pub(super) fn field(db: &Store, name: &str) -> Value { db.read(&["moments"], |reader| Ok(reader.point("moments", "m1")?.and_then(|row| row.get(name).cloned()).unwrap_or(Value::Null))).unwrap() }

#[test]
fn inv_merge_01_inv_merge_02_every_path_applies_the_sent_fields_and_leaves_the_rest() {
    for path in ["answer", "event", "page"] {
        let db = open();
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
        server_row(path, &db, json!({"id": "m1", "text": "new"}));
        assert_eq!((field(&db, "text"), field(&db, "userId")), (json!("new"), json!("u1")), "{path}: fields outside the row stay");
        server_row(path, &db, json!({"id": "m1", "userId": null}));
        assert_eq!((field(&db, "text"), field(&db, "userId")), (json!("new"), Value::Null), "{path}: null erases the field it names");
    }
}

#[test]
fn inv_merge_03_a_server_row_does_not_undo_an_unconfirmed_local_edit() {
    for path in ["answer", "event", "page"] {
        let db = open();
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![intent(5)]);
        assert_eq!(field(&db, "text"), json!("edit"));
        server_row(path, &db, json!({"id": "m1", "text": "stale server value"}));
        assert_eq!(field(&db, "text"), json!("edit"), "{path}: the open operation owns the field");
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveIntent { op_id: 5 }]);
        server_row(path, &db, json!({"id": "m1", "text": "confirmed"}));
        assert_eq!(field(&db, "text"), json!("confirmed"), "{path}: after the end of the operation the server value lands");
    }
}

pub(super) fn attach(model_row: &str, bytes: Vec<u8>) -> StoreOp {
    StoreOp::AttachFile { model: "moments".into(), row_id: model_row.into(), field: "media".into(), bytes, name: "photo".into(), mime: "image/jpeg".into() }
}

#[test]
fn inv_file_04_temp_row_files_follow_the_row_to_its_real_id() {
    let db = open();
    put(&db, "moments", json!({"id": "temp-1", "userId": "u1"}));
    write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![attach("temp-1", vec![7; 40])]);
    write(&db, BatchAuthor::Outbox, vec![("moments".into(), json!({"id": "m5", "userId": "u1"}).to_string())], vec![StoreOp::Swap { model: "moments".into(), temp: "temp-1".into(), real: "m5".into() }]);
    let id = |text: &str| crate::identity::RowId::new(text).unwrap();
    assert_eq!(db.read_file("moments", &id("m5"), "media").unwrap(), Some(vec![7; 40]), "the real row owns the bytes");
    assert_eq!(db.read_file("moments", &id("temp-1"), "media").unwrap(), None, "the temporary row owns none");
}

#[test]
fn inv_file_07_the_bytes_read_back_the_same_on_both_sides_of_the_storage_threshold() {
    let directory = tempfile::tempdir().unwrap();
    let db = Store::open_models(Some(&directory.path().join("data.sqlite")), all_specs(), None).map(Arc::new).unwrap();
    put(&db, "moments", json!({"id": "m1", "userId": "u1"}));
    for size in [1usize, 256 * 1024, 256 * 1024 + 1, 600_000] {
        let bytes: Vec<u8> = (0..size).map(|index| (index % 251) as u8).collect();
        write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![attach("m1", bytes.clone())]);
        assert_eq!(db.read_file("moments", &crate::identity::RowId::new("m1").unwrap(), "media").unwrap(), Some(bytes), "size {size}");
    }
}

fn paged_moments() -> Arc<Store> {
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
    Arc::new(Store::open_models(None, specs, None).unwrap())
}

fn mine(user: &str) -> WindowKey { WindowKey::relation("moments", "mine", &row(json!({"userId": user}))) }

fn mine_page(db: &Store, user: &str, ids: &[&str], cursor: Option<&str>, replace_all: bool) -> StoreOp {
    let key = mine(user);
    let roots = ids.iter().map(|id| crate::owners::store::RowValue { row_id: (*id).into(), values: json!({"id": id, "userId": user, "text": id}).to_string() }).collect();
    StoreOp::Window { key: key.name, model: key.model, page: 1, replace_all, cursor: cursor.map(str::to_string), roots, related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() }
}

fn members(db: &Store, user: &str) -> Vec<String> {
    let scope = db.spec("moments").unwrap().selection_key("mine", &row(json!({"userId": user}))).unwrap();
    db.read(&["moments"], |reader| reader.scope_members("moments", scope.as_str())).unwrap().into_iter().map(|(id, _)| id).collect()
}

#[test]
fn inv_merge_07_a_page_keeps_the_tail_after_a_found_anchor_and_drops_it_without_one() {
    let db = paged_moments();
    write(&db, BatchAuthor::Window, Vec::new(), vec![mine_page(&db, "u1", &["m1", "m2", "m3"], None, false)]);
    write(&db, BatchAuthor::Window, Vec::new(), vec![mine_page(&db, "u1", &["m1", "m2"], Some("c1"), false)]);
    assert_eq!(members(&db, "u1"), ["m1", "m2", "m3"], "the anchor m2 is found: the tail stays");
    write(&db, BatchAuthor::Window, Vec::new(), vec![mine_page(&db, "u1", &["n1", "n2"], None, true)]);
    assert_eq!(members(&db, "u1"), ["n1", "n2"], "the anchor n2 is not in the list: the tail goes");
}

#[test]
fn inv_merge_08_a_landing_changes_only_the_list_of_its_own_selection() {
    let db = paged_moments();
    write(&db, BatchAuthor::Window, Vec::new(), vec![mine_page(&db, "u1", &["m1"], None, false)]);
    write(&db, BatchAuthor::Window, Vec::new(), vec![mine_page(&db, "u2", &["m2"], None, false)]);
    write(&db, BatchAuthor::Window, Vec::new(), vec![mine_page(&db, "u1", &["m3"], None, true)]);
    assert_eq!(members(&db, "u1"), ["m3"]);
    assert_eq!(members(&db, "u2"), ["m2"]);
}

#[test]
fn inv_id_06_a_real_row_that_arrived_before_the_create_answer_leaves_one_row_after_it() {
    let db = open();
    let temp = crate::identity::RowId::generate_temp(None).to_string();
    let values = json!({"id": temp, "userId": "u1", "text": "a"}).to_string();
    let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: temp.clone(), values }], ..Default::default() };
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { op_id: 1, model: "moments".into(), row_id: temp.clone(), action: "create".into(), payload: "{}".into(), effects, origin: crate::owners::store::IntentOrigin::Api }]);
    put(&db, "moments", json!({"id": "m9", "userId": "u1", "text": "a"}));
    let answer = crate::owners::store::RowValue { row_id: "m9".into(), values: json!({"id": "m9", "userId": "u1", "text": "a"}).to_string() };
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![
        StoreOp::Rows { model: "moments".into(), rows: vec![answer] },
        StoreOp::Swap { model: "moments".into(), temp: temp.clone(), real: "m9".into() },
        StoreOp::RemoveIntent { op_id: 1 },
    ]);
    assert!(db.read(&["moments"], |reader| reader.point("moments", &temp)).unwrap().is_none(), "the temp row is gone");
    assert_eq!(db.count(&Query::new("moments")).unwrap(), 1, "one row stays");
}

pub(super) fn sent_create(db: &Store, op_id: u64, text: &str) -> String {
    let temp = crate::identity::RowId::generate_temp(None).to_string();
    let values = json!({"id": temp, "userId": "u1", "text": text}).to_string();
    let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: temp.clone(), values }], ..Default::default() };
    write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { op_id, model: "moments".into(), row_id: temp.clone(), action: "create".into(), payload: "{}".into(), effects, origin: crate::owners::store::IntentOrigin::Api }]);
    temp
}

pub(super) fn event_row(db: &Store, id: &str, text: &str) {
    write(db, BatchAuthor::Feed, vec![("moments".into(), json!({"op": "upsert", "id": id, "row": {"id": id, "userId": "u1", "text": text}}).to_string())], Vec::new());
}

#[test]
fn inv_id_08_one_matching_candidate_is_replaced_by_the_row_of_the_event_and_several_are_not() {
    use crate::schema::{ActionDeclaration, RootShape};
    let mut specs = all_specs();
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.correlate = vec!["userId".into(), "text".into()];
    specs.iter_mut().find(|spec| spec.key.as_str() == "moments").unwrap().actions.push(create);
    let db = Arc::new(Store::open_models(None, specs, None).unwrap());
    let gone = |temp: &str| db.read(&["moments"], |reader| reader.point("moments", temp)).unwrap().is_none();
    let single = sent_create(&db, 1, "one");
    event_row(&db, "m1", "one");
    assert!(gone(&single), "the temp row of the single sent record is replaced");
    assert_eq!(db.count(&Query::new("moments")).unwrap(), 1, "one row stays: the row of the event");
    let (first, second) = (sent_create(&db, 2, "two"), sent_create(&db, 3, "two"));
    event_row(&db, "m2", "two");
    assert!(!gone(&first) && !gone(&second), "two candidates match: both temp rows stay");
    assert_eq!(db.count(&Query::new("moments")).unwrap(), 4, "the row of the event is stored as a new row");
    let blank = sent_create(&db, 4, "");
    event_row(&db, "m3", "");
    assert!(!gone(&blank), "an empty named field never matches");
    assert_eq!(db.count(&Query::new("moments")).unwrap(), 6);
}

#[test]
fn inv_order_04_restored_member_takes_a_free_key_when_its_old_key_was_reused() {
    use crate::store::{BatchPlan, ScopeOp, ScopeRef};
    use crate::identity::{ListKey, ModelKey};
    let db = open();
    let scope = ScopeRef::new(ModelKey::new("moments"), ListKey::from_raw("feed"));
    for id in ["a", "b"] { put(&db, "moments", json!({"id": id, "userId": "u1", "text": id})); }
    let first = crate::value::fractional_between(None, None).unwrap();
    let second = crate::value::fractional_between(Some(&first), None).unwrap();
    let append = |db: &Store, id: &str, key: &str| {
        let mut envelope = BatchPlan::from_server(db.generation());
        envelope.scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: crate::store::fixtures::id(id), order_key: key.into() });
        db.commit(envelope).unwrap();
    };
    append(&db, "a", &first);
    append(&db, "b", &second);
    let leave = crate::owners::store::EffectMember { model: "moments".into(), list: "feed".into(), row_id: "b".into(), order_key: String::new(), leave: true };
    let effects = IntentEffects { tombstones: vec![crate::owners::store::EffectTombstone { model: "moments".into(), row_id: "b".into() }], members: vec![leave], ..Default::default() };
    write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::Intent { op_id: 3, model: "moments".into(), row_id: "b".into(), action: "destroy".into(), payload: "{}".into(), effects, origin: crate::owners::store::IntentOrigin::Api }]);
    put(&db, "moments", json!({"id": "x", "userId": "u1", "text": "x"}));
    append(&db, "x", &second);
    write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::RollBack { op_id: 3 }, StoreOp::RemoveIntent { op_id: 3 }]);
    let members: Vec<String> = db.read(&["moments"], |reader| reader.scope_members("moments", "feed")).unwrap().into_iter().map(|(id, _)| id).collect();
    assert_eq!(members.len(), 3, "{members:?}");
    assert!(members.contains(&"b".to_string()), "{members:?}");
}

#[test]
fn inv_row_07_fsck_quarantines_temp_rows_without_an_operation_and_keeps_the_others() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let kept = crate::identity::RowId::generate_temp(None).to_string();
    {
        let db = Arc::new(Store::open_models(Some(&path), all_specs(), None).unwrap());
        put(&db, "moments", json!({"id": "temp-orphan", "userId": "u1", "text": "orphan"}));
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "server row"}));
        let values = json!({"id": kept, "userId": "u1", "text": "queued"}).to_string();
        let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: kept.clone(), values }], ..Default::default() };
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { op_id: 1, model: "moments".into(), row_id: kept.clone(), action: "create".into(), payload: "{}".into(), effects, origin: crate::owners::store::IntentOrigin::Api }]);
    }
    let db = Arc::new(Store::open_models(Some(&path), all_specs(), None).unwrap());
    let present = |id: &str| db.read(&["moments"], |reader| reader.point("moments", id)).unwrap().is_some();
    assert!(!present("temp-orphan"), "a temp row without an operation goes");
    assert!(present("m1") && present(&kept), "the server row and the row of a queued create stay");
    let quarantined: i64 = db.lock().connection.query_row("SELECT COUNT(*) FROM quarantine WHERE reason = 'orphan-temp-row'", [], |row| row.get(0)).unwrap();
    assert_eq!(quarantined, 1);
}

fn candidates_of_one_event_row(unrelated: u64) -> usize {
    use crate::schema::{ActionDeclaration, RootShape};
    let mut specs = all_specs();
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.correlate = vec!["userId".into(), "text".into()];
    specs.iter_mut().find(|spec| spec.key.as_str() == "moments").unwrap().actions.push(create);
    let db = Arc::new(Store::open_models(None, specs, None).unwrap());
    for op_id in 0..unrelated {
        let intent = StoreOp::Intent { op_id: 100 + op_id, model: "users".into(), row_id: format!("u{op_id}"), action: "edit".into(), payload: "{}".into(), effects: IntentEffects::default(), origin: crate::owners::store::IntentOrigin::Api };
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![intent]);
    }
    sent_create(&db, 1, "one");
    let before = db.counters().outbox_candidates;
    event_row(&db, "m1", "one");
    db.counters().outbox_candidates - before
}

#[test]
fn an_event_row_that_arrives_new_looks_at_the_candidates_of_its_model_and_action_only() {
    assert_eq!(candidates_of_one_event_row(2), 1, "the one sent create of the model is the only candidate");
    assert_eq!(candidates_of_one_event_row(8), 1, "operations of other models are not looked at");
}
