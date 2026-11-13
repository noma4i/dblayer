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

pub(super) fn open() -> Arc<Store> { Arc::new(Store::open_models(None, all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap()) }

pub(super) fn write(db: &Store, author: BatchAuthor, rows: Vec<(String, String)>, ops: Vec<StoreOp>) {
    let batch = StoreBatch { author, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows, intents: 0, credential: None, ops };
    db.write_batch(&batch, db.files(), None).unwrap();
}

#[test]
fn dlv_deferred_alias_after_log_evicted() {
    let db = open();
    let mut initial = Vec::new();
    let mut landed = Vec::new();
    let mut swaps = Vec::new();
    for i in 0..4097 {
        let temp = format!("temp-{i}");
        let real = format!("real-{i}");
        initial.push(("moments".into(), json!({"id":temp, "userId":"u1"}).to_string()));
        landed.push(("moments".into(), json!({"id":real, "userId":"u1"}).to_string()));
        swaps.push(StoreOp::Swap { model: "moments".into(), temp, real });
    }
    write(&db, BatchAuthor::Outbox, initial, vec![]);
    let baseline_query = Query::new("moments");
    let baseline = db.read_view(&baseline_query, false, 0, 0, None, None, &crate::store::fixtures::list_view(&baseline_query), (0, 0)).unwrap().0;
    write(&db, BatchAuthor::Outbox, landed, swaps);
    for query in [Query::new("moments").filtered(Where::equal("id", json!("real-0"))), Query::new("moments")] {
        let (_, _, _, view, replaced) = db.read_view(&query, false, baseline, baseline, None, None, &crate::store::fixtures::list_view(&query), (baseline, 0)).unwrap();
        assert!(matches!(view, crate::store::lookup::ViewRead::Rows(ref rows, _) if rows.iter().any(|(id, _, _)| id == "real-0")), "the view must contain the real row");
        assert!(replaced.iter().any(|(temp, real)| temp.as_str() == "temp-0" && real.as_str() == "real-0"), "evicted swap must remain visible: {replaced:?}");
    }
}

#[test]
fn dlv_swap_same_id_other_model() {
    let db = open();
    write(&db, BatchAuthor::Outbox, vec![("moments".into(), json!({"id":"temp-1", "userId":"u1"}).to_string()), ("users".into(), json!({"id":"temp-1"}).to_string())], vec![]);
    let query = Query::new("users");
    let baseline = db.read_view(&query, false, 0, 0, None, None, &crate::store::fixtures::list_view(&query), (0, 0)).unwrap().0;
    write(&db, BatchAuthor::Outbox, vec![("moments".into(), json!({"id":"real-1", "userId":"u1"}).to_string())], vec![StoreOp::Swap { model: "moments".into(), temp: "temp-1".into(), real: "real-1".into() }]);
    let (_, _, _, view, replaced) = db.read_view(&query, false, baseline, baseline, None, None, &crate::store::fixtures::list_view(&query), (baseline, 0)).unwrap();
    assert!(matches!(view, crate::store::lookup::ViewRead::Rows(ref rows, _) if rows.iter().any(|(id, _, _)| id == "temp-1")), "the other model keeps its row");
    assert!(replaced.is_empty(), "another model's alias must not be returned: {replaced:?}");
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
    StoreOp::Intent { created_at: 0, attempts: 1, cycle_id: 1, op_id, model: "moments".into(), row_id: "m1".into(), action: "update".into(), payload: "{}".into(), effects, origin: crate::owners::store::IntentOrigin::Api }
}

#[test]
fn transfer_effects_become_intent_without_reapplying_or_losing_prior() {
    let db = open();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
    let StoreOp::Intent { op_id, model, row_id, action, payload, effects, .. } = intent(5) else { unreachable!() };
    write(&db, BatchAuthor::Transfer, Vec::new(), vec![StoreOp::Effects { created_at: 0, op_id, model: model.clone(), row_id: row_id.clone(), action: action.clone(), payload: payload.clone(), effects }]);
    let state: String = db.read_locked(|connection| connection.query_row("SELECT intent_state FROM op WHERE seq = 5", [], |row| row.get(0))).unwrap();
    assert_eq!(state, "effects_only");
    assert_eq!(texts(&db, &Query::new("moments")), vec![json!("edit")]);
    write(&db, BatchAuthor::Transfer, Vec::new(), vec![StoreOp::Intent { created_at: 0, attempts: 1, cycle_id: 1, op_id, model, row_id, action, payload, effects: Default::default(), origin: crate::owners::store::IntentOrigin::Transfer { transfer_id: 1 } }]);
    let state: String = db.read_locked(|connection| connection.query_row("SELECT intent_state FROM op WHERE seq = 5", [], |row| row.get(0))).unwrap();
    assert_eq!(state, "ready");
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RollBack { op_id }]);
    assert_eq!(texts(&db, &Query::new("moments")), vec![json!("old")]);
}

#[test]
fn transfer_appended_effects_rollback_to_the_original_row() {
    let db = open();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
    let StoreOp::Intent { op_id, model, row_id, action, payload, effects, .. } = intent(5) else { unreachable!() };
    let append = |effects| StoreOp::Effects { created_at: 0, op_id, model: model.clone(), row_id: row_id.clone(), action: action.clone(), payload: payload.clone(), effects };
    write(&db, BatchAuthor::Transfer, Vec::new(), vec![append(effects)]);
    let added = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: "m1".into(), values: json!({"id": "m1", "userId": "u2", "text": "prepared"}).to_string() }], ..Default::default() };
    write(&db, BatchAuthor::Transfer, Vec::new(), vec![append(added)]);
    assert_eq!((field(&db, "text"), field(&db, "userId")), (json!("prepared"), json!("u2")));
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RollBack { op_id }]);
    assert_eq!((field(&db, "text"), field(&db, "userId")), (json!("old"), json!("u1")));
}

#[test]
fn transfer_appended_effects_rollback_removes_the_created_row() {
    let db = open();
    let StoreOp::Intent { op_id, model, row_id, action, payload, effects, .. } = intent(5) else { unreachable!() };
    let append = |effects| StoreOp::Effects { created_at: 0, op_id, model: model.clone(), row_id: row_id.clone(), action: action.clone(), payload: payload.clone(), effects };
    write(&db, BatchAuthor::Transfer, Vec::new(), vec![append(effects)]);
    let added = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: "m1".into(), values: json!({"id": "m1", "text": "prepared"}).to_string() }], ..Default::default() };
    write(&db, BatchAuthor::Transfer, Vec::new(), vec![append(added)]);
    assert_eq!(field(&db, "text"), json!("prepared"));
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RollBack { op_id }]);
    assert!(db.read(&["moments"], |reader| reader.point("moments", "m1")).unwrap().is_none());
}

#[test]
fn transfer_effects_cannot_append_to_a_ready_intent() {
    let db = open();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![intent(5)]);
    let StoreOp::Intent { op_id, model, row_id, action, payload, effects, .. } = intent(5) else { unreachable!() };
    let batch = StoreBatch { author: BatchAuthor::Transfer, id: 2, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::Effects { created_at: 0, op_id, model, row_id, action, payload, effects }] };
    assert!(db.write_batch(&batch, db.files(), None).is_err());
    assert_eq!(field(&db, "text"), json!("edit"));
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
        key: "moments:m1:upload".into(), seq: 3, source: Some("/source".into()), parts: 1,
        batch: None, fields: None, upload_id: None, confirmed_parts: 1, progress: 0, weight: None, weighed_prepare: false, batch_progress: 0, attempts: 1,
        completed: true, failed: Some("refused".into()), created_at: 1, inline: false,
        size: 3, sha256: "hash".into(), path: "/mutation".into(), body: "{}".into(),
        on_failure: OnFailure::Keep, steps_path: Some("/steps".into()), steps_body: Some("{}".into()),
        transfer_id: 3, cycle_id: 1, signed_id: Some("signed".into()), aborted: false, staged: false, failed_at: None, blob_created_at: None, held: false,
        op_id: Some(5), etags: vec![None], prepare: None, max_bytes: None,
    };
    write(&db, BatchAuthor::Transfer, Vec::new(), vec![StoreOp::Upload { row, kind: UploadKind::Failed }]);
    let StoreOp::Intent { op_id, model, row_id, action, payload, effects, .. } = intent(5) else { unreachable!() };
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { created_at: 0, attempts: 1, cycle_id: 1, op_id, model, row_id, action, payload, effects, origin: IntentOrigin::Transfer { transfer_id: 3 } }]);
    let first = OutboxOutcome::Failed { error: "refused".into() };
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::TransferOutcome { transfer_id: 3, cycle_id: 1, outcome: first.clone() }]);
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::MarkError { op_id: 5, error: None }, StoreOp::Attempt { op_id: 5, attempts: 1, cycle_id: 2, cause: None }]);
    let second = OutboxOutcome::Discarded;
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::TransferOutcome { transfer_id: 3, cycle_id: 2, outcome: second.clone() }]);
    assert_eq!(db.read_restore().unwrap().transfer_outcomes.get(&3), Some(&[(1, first), (2, second)].into()));
    write(&db, BatchAuthor::Transfer, Vec::new(), vec![StoreOp::RemoveUpload { key: "moments:m1:upload".into() }]);
    assert_eq!(db.read_restore().unwrap().transfer_outcomes.get(&3), Some(&[(1, OutboxOutcome::Failed { error: "refused".into() }), (2, OutboxOutcome::Discarded)].into()), "RemoveUpload keeps the outcomes of every cycle (D-237)");
}

#[test]
fn store_transfer_outcome_keeps_the_first_result() {
    use crate::owners::write::outbox::OutboxOutcome;
    let db = open();
    let first = OutboxOutcome::Failed { error: "refused".into() };
    write(&db, BatchAuthor::Transfer, Vec::new(), vec![StoreOp::TransferOutcome { transfer_id: 3, cycle_id: 1, outcome: first.clone() }]);
    let before: (String, i64) = db.lock().connection.query_row("SELECT kind, terminal_at FROM transfer_outcome WHERE transfer_id = 3 AND cycle_id = 1", [], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
    write(&db, BatchAuthor::Transfer, Vec::new(), vec![StoreOp::TransferOutcome { transfer_id: 3, cycle_id: 1, outcome: OutboxOutcome::Discarded }]);
    let after: (String, i64) = db.lock().connection.query_row("SELECT kind, terminal_at FROM transfer_outcome WHERE transfer_id = 3 AND cycle_id = 1", [], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
    assert_eq!(after, before);
    assert_eq!(db.read_restore().unwrap().transfer_outcomes.get(&3), Some(&[(1, first)].into()));
}

#[test]
fn inv_sync_02_queries_filter_by_sync_state_and_count_matches_rows() {
    let db = open();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
    put(&db, "moments", json!({"id": "m2", "userId": "u1", "text": "other"}));
    let pending = Query::new("moments").filtered(Where::equal("sync", json!("pending")));
    let synced = Query::new("moments").filtered(Where::equal("sync", json!("synced")));
    let before = db.revision_seq().unwrap();
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![intent(6)]);
    assert!(db.revision_seq().unwrap() > before, "the state change is a commit that observers see");
    assert_eq!((texts(&db, &pending), db.count(&pending).unwrap()), ([json!("edit")].to_vec(), 1));
    assert_eq!((texts(&db, &synced), db.count(&synced).unwrap()), ([json!("other")].to_vec(), 1), "a row without a sync entry is synced");
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
    let batch = StoreBatch { author: BatchAuthor::Feed, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: vec![("moments".into(), event)], intents: 0, credential: None, ops: Vec::new() };
    db.write_batch(&batch, db.files(), None).unwrap()
}

pub(super) fn typing(db: &Store, id: &str) -> Option<Value> { db.read(&["moments"], |reader| Ok(reader.point("moments", id)?.and_then(|row| row.get("typing").cloned()))).unwrap() }

#[test]
fn store_relevant_view_keeps_overlay_position_and_handles_eviction() {
    let db = Store::open_models(None, ephemeral_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    put(&db, "moments", json!({"id":"m1", "userId":"u1"}));
    let query = Query::new("moments");
    let view = crate::store::fixtures::list_view(&query);
    let baseline = db.revision_seq().unwrap();
    set_typing(&db, "m1", true);
    let (seen, position, relevant, read, _) = db.read_view(&query, false, baseline, baseline, None, None, &view, (baseline, 0)).unwrap();
    assert_eq!((seen, position, relevant), (baseline, 1, Some((baseline, 1))));
    assert!(matches!(read, crate::store::lookup::ViewRead::Rows(ref rows, _) if rows.iter().any(|(_, _, row)| row.get("typing") == Some(&json!(true)))));
    set_typing(&db, "m1", false);
    let held = crate::store::HeldVersion { from: (baseline, 1), limit: 256 };
    let (seen, position, relevant, _, _) = db.read_view(&query, false, baseline, baseline, None, Some(held), &view, (baseline, 1)).unwrap();
    assert_eq!((seen, position, relevant), (baseline, 2, Some((baseline, 2))));

    let users = Query::new("users");
    let users_view = crate::store::fixtures::list_view(&users);
    let batch = (0..8193).map(|index| ("moments".into(), json!({"id":format!("m{index}"), "userId":"u1"}).to_string())).collect();
    write(&db, BatchAuthor::Outbox, batch, vec![]);
    let (seen, position, relevant, _, _) = db.read_view(&users, false, 0, baseline, None, None, &users_view, (baseline, 0)).unwrap();
    assert_eq!(relevant, Some((seen, position)), "an evicted interval without a retained match uses the snapshot version");
}

#[test]
fn inv_ephemeral_01_values_and_memory_rows_do_not_survive_a_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data.sqlite");
    {
        let db = Store::open_models(Some(&path), ephemeral_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).map(Arc::new).unwrap();
        put(&db, "moments", json!({"id": "m1", "userId": "u1"}));
        put(&db, "presence", json!({"id": "p1"}));
        set_typing(&db, "m1", true);
        assert_eq!(typing(&db, "m1"), Some(json!(true)));
    }
    let db = Store::open_models(Some(&path), ephemeral_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).map(Arc::new).unwrap();
    assert_eq!(typing(&db, "m1"), Some(json!(false)), "the value is back to the default");
    assert!(db.read(&["presence"], |reader| reader.point("presence", "p1")).unwrap().is_none(), "the memory row is gone");
}

#[test]
fn inv_ephemeral_08_values_for_rows_that_never_arrive_leave_no_memory_behind() {
    let db = Arc::new(Store::open_models(None, ephemeral_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    for index in 0..1500 { set_typing(&db, &format!("ghost{index}"), true); }
    let held = db.overlays.get(db.revision_seq().unwrap()).unwrap().values.len();
    assert!(held <= 1024, "{held} values for rows that never arrived stay in memory");
}

pub(super) fn server_row(path: &str, db: &Store, row: Value) {
    match path {
        "answer" => write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "moments".into(), rows: vec![crate::owners::store::RowValue { row_id: "m1".into(), values: row.to_string() }] }]),
        "event" => write(db, BatchAuthor::Feed, vec![("moments".into(), json!({"op": "upsert", "id": "m1", "row": row}).to_string())], Vec::new()),
        _ => {
            let key = crate::owners::read::windows::WindowKey::nodes("moments", &["m1"]);
            let page = StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: vec![crate::owners::store::RowValue { row_id: "m1".into(), values: row.to_string() }], related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() };
            write(db, BatchAuthor::Window, Vec::new(), vec![page]);
        }
    }
}

pub(super) fn field(db: &Store, name: &str) -> Value { db.read(&["moments"], |reader| Ok(reader.point("moments", "m1")?.and_then(|row| row.get(name).cloned()).unwrap_or(Value::Null))).unwrap() }

pub(super) fn attach(model_row: &str, bytes: Vec<u8>) -> StoreOp {
    StoreOp::AttachFile { model: "moments".into(), row_id: model_row.into(), field: "media".into(), bytes, name: "photo".into(), mime: "image/jpeg".into() }
}

#[test]
fn inv_file_07_the_bytes_read_back_the_same_on_both_sides_of_the_storage_threshold() {
    let directory = tempfile::tempdir().unwrap();
    let db = Store::open_models(Some(&directory.path().join("data.sqlite")), all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).map(Arc::new).unwrap();
    put(&db, "moments", json!({"id": "m1", "userId": "u1"}));
    for size in [1usize, 256 * 1024, 256 * 1024 + 1, 600_000] {
        let bytes: Vec<u8> = (0..size).map(|index| (index % 251) as u8).collect();
        write(&db, BatchAuthor::LocalWrites, Vec::new(), vec![attach("m1", bytes.clone())]);
        assert_eq!(db.url("moments", &crate::identity::RowId::new("m1").unwrap(), "media").unwrap().map(|path| std::fs::read(path).unwrap()), Some(bytes), "size {size}");
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
    Arc::new(Store::open_models(None, specs, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap())
}

fn mine(user: &str) -> WindowKey { WindowKey::relation("moments", "mine", &row(json!({"userId": user}))) }

fn mine_page(db: &Store, user: &str, ids: &[&str], cursor: Option<&str>, replace_all: bool) -> StoreOp {
    let key = mine(user);
    let roots = ids.iter().map(|id| crate::owners::store::RowValue { row_id: (*id).into(), values: json!({"id": id, "userId": user, "text": id}).to_string() }).collect();
    StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all, cursor: cursor.map(str::to_string), roots, related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() }
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

pub(super) fn sent_create(db: &Store, op_id: u64, text: &str) -> String {
    let temp = crate::identity::RowId::generate_temp(None).to_string();
    let values = json!({"id": temp, "userId": "u1", "text": text}).to_string();
    let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: temp.clone(), values }], ..Default::default() };
    write(db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { created_at: 0, attempts: 1, cycle_id: 1, op_id, model: "moments".into(), row_id: temp.clone(), action: "create".into(), payload: "{}".into(), effects, origin: crate::owners::store::IntentOrigin::Api }]);
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
    let db = Arc::new(Store::open_models(None, specs, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
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
fn a_temp_row_replaced_by_the_row_of_an_event_is_found_by_its_temporary_id() {
    use crate::schema::{ActionDeclaration, RootShape};
    let mut specs = all_specs();
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.correlate = vec!["userId".into(), "text".into()];
    specs.iter_mut().find(|spec| spec.key.as_str() == "moments").unwrap().actions.push(create);
    let db = Arc::new(Store::open_models(None, specs, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    let temp = sent_create(&db, 1, "one");
    event_row(&db, "m1", "one");
    let found = db.find("moments", &crate::identity::RowId::new(&temp).unwrap()).unwrap().map(|row| row.id.to_string());
    assert_eq!(found, Some("m1".to_string()));
}

#[test]
fn inv_row_07_fsck_quarantines_temp_rows_without_an_operation_and_keeps_the_others() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let kept = crate::identity::RowId::generate_temp(None).to_string();
    {
        let db = Arc::new(Store::open_models(Some(&path), all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
        put(&db, "moments", json!({"id": "temp-orphan", "userId": "u1", "text": "orphan"}));
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "server row"}));
        let values = json!({"id": kept, "userId": "u1", "text": "queued"}).to_string();
        let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: kept.clone(), values }], ..Default::default() };
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Intent { created_at: 0, attempts: 1, cycle_id: 1, op_id: 1, model: "moments".into(), row_id: kept.clone(), action: "create".into(), payload: "{}".into(), effects, origin: crate::owners::store::IntentOrigin::Api }]);
    }
    let db = Arc::new(Store::open_models(Some(&path), all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
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
    let db = Arc::new(Store::open_models(None, specs, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    for op_id in 0..unrelated {
        let intent = StoreOp::Intent { created_at: 0, attempts: 1, cycle_id: 1, op_id: 100 + op_id, model: "users".into(), row_id: format!("u{op_id}"), action: "edit".into(), payload: "{}".into(), effects: IntentEffects::default(), origin: crate::owners::store::IntentOrigin::Api };
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
