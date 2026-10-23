use super::fixtures::*;
use super::*;
use crate::schema::ModelSpec;
use crate::syncstate::SyncState;
use serde_json::{Value, json};

#[test]
fn storage_rules_and_writes_follow_the_shared_cases() {
    let cases: Vec<Value> = serde_json::from_str(include_str!("../../../fixtures/storage.json")).unwrap();
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let models: Vec<ModelSpec> = serde_json::from_value(case["models"].clone()).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db.sqlite");
        let opened = Db::open(&path, models.clone(), None);
        if case["error"] == true {
            let DbError::Invalid(faults) = opened.err().expect(name) else { panic!("{name}") };
            let error = faults[0].error.to_string();
            assert!(error.contains("local model has no server"), "{name}: {error}");
            continue;
        }
        let local = models[0].is_local();
        let state = if local { SyncState::Synced } else { SyncState::Pending };
        let db = opened.unwrap();
        let (log, callback) = recorder();
        let observer = db.observe(crate::query::Query::new("StorageRow"), callback).unwrap();
        db.await_revision(db.revision_seq().unwrap());
        let row = db.create("StorageRow", obj(json!({"record": "one"}))).unwrap();
        db.await_revision(db.revision_seq().unwrap());
        assert_eq!(changes(&log).last().unwrap().rows[0].text("record"), Some("one"), "{name}");
        assert_eq!(row.sync.state, state, "{name}");
        assert_eq!(db.outbox().unwrap().len(), usize::from(!local), "{name}: create");
        db.update("StorageRow", &row.id, obj(json!({"record": "two"}))).unwrap();
        db.await_revision(db.revision_seq().unwrap());
        assert_eq!(changes(&log).last().unwrap().rows[0].text("record"), Some("two"), "{name}");
        assert_eq!(db.outbox().unwrap().len(), 2 * usize::from(!local), "{name}: update");
        drop(observer);
        drop(db);
        let db = Db::open(&path, models, None).unwrap();
        let reopened = db.find("StorageRow", &row.id).unwrap().unwrap_or_else(|| panic!("{name}: row {} lost on reopen, report {:?}", row.id, db.open_report()));
        assert_eq!(reopened.text("record"), Some("two"), "{name}: reopen");
        assert_eq!(reopened.sync.state, state, "{name}: reopen");
        db.destroy("StorageRow", &row.id).unwrap();
        assert!(db.find("StorageRow", &row.id).unwrap().is_none(), "{name}: destroy unsent");
        assert!(db.outbox().unwrap().is_empty(), "{name}: unsent create is cancelled");
        put(&db, "StorageRow", json!({"id": "confirmed", "record": "from disk"}));
        db.destroy("StorageRow", &rid("confirmed")).unwrap();
        assert_eq!(db.outbox().unwrap().len(), usize::from(!local), "{name}: destroy confirmed");
    }
}
