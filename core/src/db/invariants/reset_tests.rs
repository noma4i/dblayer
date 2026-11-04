use serde_json::json;

use crate::db::fixtures::*;

#[test]
fn inv_fresh_04_resets_close_only_the_selected_models_operations() {
    for all in [false, true] {
        let db = booted();
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
        put(&db, "users", json!({"id": "u1", "name": "Ann"}));
        db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
        db.update(&db.admit().unwrap(), "users", &rid("u1"), obj(json!({"name": "Bea"}))).unwrap();
        db.create(&db.admit().unwrap(), "moments", obj(json!({"userId": "u1", "text": "new"}))).unwrap();
        if all {
            db.reset_all().unwrap();
        } else {
            db.reset(&["moments"]).unwrap();
        }
        let left: Vec<String> = db.outbox().unwrap().into_iter().map(|entry| entry.operation.action_key).collect();
        assert_eq!(left, if all { Vec::new() } else { vec!["users:update".to_string()] }, "all={all}");
        assert!(db.find("moments", &rid("m1")).unwrap().is_none());
        assert_eq!(super::fixtures::state_of(&db, "moments", "m1"), crate::syncstate::SyncState::Synced);
        if !all {
            assert_eq!(db.find("users", &rid("u1")).unwrap().unwrap().text("name"), Some("Bea"));
            assert_eq!(super::fixtures::state_of(&db, "users", "u1"), crate::syncstate::SyncState::Pending);
        }
    }
}

#[test]
fn inv_dsl_07_a_schema_change_quarantines_the_operations_of_every_server_model() {
    use crate::db::Db;
    use crate::operation::{quarantine_counts, QuarantineReason};
    use crate::schema::FieldSpec;
    use crate::value::ScalarKind;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("1.sqlite");
    let first = Db::open(&path, all_specs(), None).unwrap();
    put(&first, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    put(&first, "users", json!({"id": "u1", "name": "Ann"}));
    first.update(&first.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    first.update(&first.admit().unwrap(), "users", &rid("u1"), obj(json!({"name": "Bea"}))).unwrap();
    first.close().unwrap();
    drop(first);
    let mut changed = all_specs();
    changed.iter_mut().find(|spec| spec.key.as_str() == "moments").unwrap().fields.push(FieldSpec::scalar("extra", ScalarKind::Str).optional());
    let reopened = Db::open(&path, changed, None).unwrap();
    let left: Vec<String> = reopened.outbox().unwrap().into_iter().map(|entry| entry.operation.action_key).collect();
    assert!(left.is_empty());
    let quarantined = quarantine_counts(&reopened.shared.store.lock().connection).unwrap();
    assert_eq!(quarantined.get(QuarantineReason::SchemaChanged.as_str()), Some(&2));
}

#[test]
fn inv_fresh_04_a_full_reset_leaves_no_row_in_any_data_table() {
    use crate::commit::{CommitEnvelope, ScopeOp, ScopeRef, WriteOrigin};
    use crate::identity::{ModelKey, ListKey};
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    db.attach(&db.admit().unwrap(), "moments", &rid("m1"), "media", b"bytes", "a.txt", "text/plain").unwrap();
    let scope = ScopeRef::new(ModelKey::new("moments"), ListKey::compose(&["moments", "feed"]));
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: rid("m1"), order_key: "a".into() });
    db.land(&db.admit().unwrap(), envelope).unwrap();
    db.shared.store.mark_tombstone("moments", "m9", 1, 1).unwrap();
    db.shared
        .store
        .save_window(&crate::store::WindowRecord { key: format!("{}:forward", scope.key.as_str()), cursor: None, has_next: false, total: None, pages: 1, issued: 0, applied: 0, generation: 0 })
        .unwrap();
    db.reset_all().unwrap();
    let kept = ["manifest", "quarantine", "quarantine_value", "encrypted_session", "encrypted_pending"];
    let inner = db.shared.store.lock();
    let tables: Vec<String> = {
        let mut statement = inner.connection.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND sql NOT LIKE 'CREATE VIRTUAL%'").unwrap();
        statement.query_map([], |row| row.get(0)).unwrap().map(Result::unwrap).collect()
    };
    let filled: Vec<String> = tables
        .into_iter()
        .filter(|table| !kept.contains(&table.as_str()))
        .filter(|table| inner.connection.query_row(&format!("SELECT COUNT(*) FROM \"{table}\""), [], |row| row.get::<_, i64>(0)).is_ok_and(|count| count > 0))
        .collect();
    assert_eq!(filled, Vec::<String>::new(), "tables that still hold rows after a full reset");
}
