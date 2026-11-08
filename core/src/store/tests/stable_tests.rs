use rusqlite::StatementStatus;
use serde_json::json;

use crate::identity::{ListKey, ModelKey, RowId};
use crate::schema_fixtures::{messages_spec, CHAT};
use crate::store::{BatchPlan, EntityOp, ScopeOp, ScopeRef, Store};

use super::stable::LOWER_NEIGHBOUR_SQL;

fn open(path: Option<&std::path::Path>, spec: crate::schema::ModelSpec) -> std::sync::Arc<Store> {
    Store::open_models(path, vec![spec], None).map(std::sync::Arc::new).unwrap()
}

fn scope_of(db: &Store) -> (ScopeRef, ListKey) {
    let query = db.remote_relation_query("messages", "thread", json!({"chatId": CHAT}).as_object().unwrap()).unwrap().unwrap();
    let key = query.scope.clone().unwrap();
    (ScopeRef::new(ModelKey::new("messages"), key.clone()), key)
}

fn land(db: &Store, scope: &ScopeRef, rows: &[(&str, serde_json::Value)], append: &[&str]) {
    let mut envelope = BatchPlan::from_server(db.generation());
    for (id, sequence) in rows {
        let row = json!({"id": id, "chatId": CHAT, "sequenceNumber": sequence, "createdAt": "2026-01-01T00:00:00.000Z"}).as_object().unwrap().clone();
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("messages"), id: RowId::new(id).unwrap(), row });
    }
    for id in append {
        envelope.scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: RowId::new(id).unwrap(), order_key: "ignored".into() });
    }
    db.commit(envelope).unwrap();
}

fn members(db: &Store, key: &ListKey) -> Vec<String> {
    db.read(&["messages"], |reader| reader.scope_members("messages", key.as_str())).unwrap().into_iter().map(|(id, _)| id).collect()
}

fn steps_to_find_a_place(size: usize) -> i32 {
    let db = open(None, messages_spec(true));
    let (scope, _) = scope_of(&db);
    let ids: Vec<String> = (0..size).map(|index| format!("m{index:05}")).collect();
    let rows: Vec<(&str, serde_json::Value)> = ids.iter().enumerate().map(|(index, id)| (id.as_str(), json!(index * 10))).collect();
    land(&db, &scope, &rows, &ids.iter().map(String::as_str).collect::<Vec<_>>());
    db.lock().connection.prepare_cached(LOWER_NEIGHBOUR_SQL).unwrap().reset_status(StatementStatus::VmStep);
    land(&db, &scope, &[("fresh", json!(size * 5))], &["fresh"]);
    let steps = db.lock().connection.prepare_cached(LOWER_NEIGHBOUR_SQL).unwrap().get_status(StatementStatus::VmStep);
    steps
}

#[test]
fn data_stable_order_places() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("stable.sqlite");
    let db = open(Some(&path), messages_spec(true));
    let (scope, key) = scope_of(&db);
    land(&db, &scope, &[("a", json!(10)), ("b", json!(20)), ("c", json!(30))], &["a", "b", "c"]);
    assert_eq!(members(&db, &key), ["a", "b", "c"], "a row takes its place when it enters the window");

    land(&db, &scope, &[("b", json!(5))], &[]);
    land(&db, &scope, &[("a", json!(40))], &["a"]);
    assert_eq!(members(&db, &key), ["a", "b", "c"], "an edit of the order fields, also a row seen again, keeps the place");

    land(&db, &scope, &[("old", json!(1)), ("mid", json!(7))], &["old", "mid"]);
    assert_eq!(members(&db, &key), ["old", "a", "b", "mid", "c"].map(String::from), "a server row outside the window stands by its order values among the shown rows");

    let temp = RowId::generate_temp(None);
    let mut envelope = BatchPlan::new(db.generation());
    let row = json!({"id": temp.as_str(), "chatId": CHAT, "sequenceNumber": 0, "createdAt": "2026-01-01T00:00:00.000Z"}).as_object().unwrap().clone();
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("messages"), id: temp.clone(), row });
    envelope.scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: temp.clone(), order_key: "ignored".into() });
    db.commit(envelope).unwrap();
    assert_eq!(members(&db, &key).last().map(String::as_str), Some(temp.as_str()), "a row of the device stands behind the last one");
    let before: Vec<String> = members(&db, &key).into_iter().filter(|id| id != temp.as_str()).collect();
    drop(db);

    let reopened = open(Some(&path), messages_spec(true));
    assert_eq!(members(&reopened, &key), before, "the places live in the cache and survive a restart, a temp row without an operation does not");
    reopened.erase_account(reopened.generation(), false).unwrap();
    assert!(members(&reopened, &key).is_empty(), "a reset of the session erases the places");
    land(&reopened, &scope, &[("a", json!(10))], &["a"]);
    drop(reopened);

    let mut changed = messages_spec(true);
    changed.fields.push(crate::schema::FieldSpec::scalar("extra", crate::value::ScalarKind::Str).optional());
    let refingerprinted = open(Some(&path), changed);
    assert!(members(&refingerprinted, &key).is_empty(), "a new fingerprint erases the places");

    let (small, large) = (steps_to_find_a_place(200), steps_to_find_a_place(2000));
    assert!(large <= small + small / 10, "{small} steps at 200 members, {large} at 2000");
}
