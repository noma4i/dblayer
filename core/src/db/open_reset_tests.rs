use serde_json::json;

use super::fixtures::*;
use super::*;
use crate::identity::ListKey;
use crate::schema::{FieldSpec, MergeRule, ModelSpec, PageDirection, Storage};
use crate::store::{window_key, WindowRecord};
use crate::value::ScalarKind;

pub(super) fn specs(changed_user: bool) -> Vec<ModelSpec> {
    let mut users = users_spec();
    if changed_user {
        users.fields.push(FieldSpec::scalar("avatar", ScalarKind::Str).optional());
    }
    let mut nickname = FieldSpec::scalar("nickname", ScalarKind::Str).optional();
    nickname.merge = Some(MergeRule::Local);
    users.fields.push(nickname);
    let mut moments = moments_spec();
    let mut pinned = FieldSpec::scalar("pinned", ScalarKind::Str).optional();
    pinned.merge = Some(MergeRule::Local);
    moments.fields.push(pinned);
    let mut notes = ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("body", ScalarKind::Str).optional()]);
    notes.storage = Storage::Local;
    vec![users, moments, vibes_spec(), notes]
}

fn count(db: &Db, sql: &str) -> i64 {
    db.shared.store.lock().connection.query_row(sql, [], |row| row.get(0)).unwrap()
}

#[test]
fn inv_open_05_a_fingerprint_change_of_one_server_model_clears_the_whole_server_cache() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let note = {
        let db = Db::open(&path, specs(false), None).unwrap();
        put(&db, "users", json!({"id": "u1", "name": "Ann"}));
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
        db.as_app().set_local("moments", &rid("m1"), "pinned", json!("yes")).unwrap();
        db.as_app().set_local("moments", &rid("m2"), "pinned", json!("early")).unwrap();
        db.shared
            .store
            .save_window(&WindowRecord { key: window_key(ListKey::compose(&["moments", "feed"]).as_str(), PageDirection::Forward), cursor: None, has_next: false, total: None, pages: 1, issued: 1, applied: 1, generation: 0 })
            .unwrap();
        let created = db.as_app().create("notes", obj(json!({"body": "mine"}))).unwrap();
        created
    };

    let db = Db::open(&path, specs(true), None).unwrap();
    assert!(db.find("moments", &rid("m1")).unwrap().is_none());
    assert_eq!(count(&db, "SELECT COUNT(*) FROM query_window"), 0);
    assert_eq!(db.find("notes", &note.id).unwrap().unwrap().text("body"), Some("mine"));
    assert_eq!(db.open_report().changed_models, vec!["users".to_string()]);
    assert_eq!(db.open_report().reset_models, vec!["moments".to_string(), "users".to_string(), "vibes".to_string()]);

    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    put(&db, "moments", json!({"id": "m2", "userId": "u1", "text": "two"}));
    assert_eq!(db.find("moments", &rid("m1")).unwrap().unwrap().text("pinned"), Some("yes"));
    assert_eq!(db.find("moments", &rid("m2")).unwrap().unwrap().text("pinned"), Some("early"));
}

#[test]
fn inv_open_05_a_fingerprint_change_of_a_local_model_clears_only_that_model() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    {
        let db = Db::open(&path, specs(false), None).unwrap();
        put(&db, "users", json!({"id": "u1", "name": "Ann"}));
        db.as_app().create("notes", obj(json!({"body": "mine"}))).unwrap();
    }
    let mut changed = specs(false);
    changed[3].fields.push(FieldSpec::scalar("extra", ScalarKind::Str).optional());
    let db = Db::open(&path, changed, None).unwrap();
    assert_eq!(db.open_report().changed_models, vec!["notes".to_string()]);
    assert_eq!(db.open_report().reset_models, vec!["notes".to_string()]);
    assert!(db.find("users", &rid("u1")).unwrap().is_some());
}

#[test]
fn inv_open_05_an_open_that_stopped_after_the_clearing_keeps_local_values() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    {
        let db = Db::open(&path, specs(false), None).unwrap();
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
        db.as_app().set_local("moments", &rid("m1"), "pinned", json!("yes")).unwrap();
    }
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER stop_open BEFORE INSERT ON manifest BEGIN SELECT RAISE(ABORT, 'stopped'); END;").unwrap();
    drop(connection);
    assert!(Db::open(&path, specs(true), None).is_err());
    rusqlite::Connection::open(&path).unwrap().execute_batch("DROP TRIGGER stop_open;").unwrap();

    let db = Db::open(&path, specs(true), None).unwrap();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    assert_eq!(db.find("moments", &rid("m1")).unwrap().unwrap().text("pinned"), Some("yes"));
}

#[test]
fn inv_open_05_a_changed_model_keeps_local_values_of_columns_that_survive() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    {
        let db = Db::open(&path, specs(false), None).unwrap();
        put(&db, "users", json!({"id": "u1", "name": "Ann"}));
        db.as_app().set_local("users", &rid("u1"), "nickname", json!("annie")).unwrap();
    }
    let db = Db::open(&path, specs(true), None).unwrap();
    assert_eq!(db.open_report().changed_models, vec!["users".to_string()]);
    assert!(db.find("users", &rid("u1")).unwrap().is_none());
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    assert_eq!(db.find("users", &rid("u1")).unwrap().unwrap().text("nickname"), Some("annie"));
}
