use serde_json::json;

use crate::db::fixtures::*;

#[test]
fn inv_fresh_04_resets_close_only_the_selected_models_operations() {
    for all in [false, true] {
        let db = booted();
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
        put(&db, "users", json!({"id": "u1", "name": "Ann"}));
        db.update("moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
        db.update("users", &rid("u1"), obj(json!({"name": "Bea"}))).unwrap();
        db.create("moments", obj(json!({"userId": "u1", "text": "new"}))).unwrap();
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
fn inv_dsl_07_a_schema_change_quarantines_the_operations_of_the_changed_model_and_keeps_the_others() {
    use crate::db::Db;
    use crate::operation::{quarantine_counts, QuarantineReason};
    use crate::schema::FieldSpec;
    use crate::value::ScalarKind;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("1.sqlite");
    let first = Db::open(&path, all_specs(), None).unwrap();
    put(&first, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    put(&first, "users", json!({"id": "u1", "name": "Ann"}));
    first.update("moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    first.update("users", &rid("u1"), obj(json!({"name": "Bea"}))).unwrap();
    drop(first);
    let mut changed = all_specs();
    changed.iter_mut().find(|spec| spec.key.as_str() == "moments").unwrap().fields.push(FieldSpec::scalar("extra", ScalarKind::Str).optional());
    let reopened = Db::open(&path, changed, None).unwrap();
    let left: Vec<String> = reopened.outbox().unwrap().into_iter().map(|entry| entry.operation.action_key).collect();
    assert_eq!(left, ["users:update"]);
    let quarantined = quarantine_counts(&reopened.shared.store.lock().connection).unwrap();
    assert_eq!(quarantined.get(QuarantineReason::SchemaChanged.as_str()), Some(&1));
}
