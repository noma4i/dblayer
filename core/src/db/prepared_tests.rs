use serde_json::json;

use super::fixtures::*;
use super::open_reset_tests::specs;
use super::*;

fn disk() -> (Db, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let db = Db::open(&directory.path().join("db.sqlite"), specs(false), None).unwrap();
    (db, directory)
}

fn plans(db: &Db) -> (usize, usize) {
    let counters = db.shared.store.counters();
    (counters.plans_off_writer, counters.plans_under_writer)
}

#[test]
fn local_writes_plan_off_the_writer_on_a_disk_database() {
    let (db, _directory) = disk();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let before = plans(&db);
    let row = db.create(&db.admit().unwrap(), "moments", obj(json!({"userId": "u1", "text": "hi"}))).unwrap();
    db.update(&db.admit().unwrap(), "moments", &row.id, obj(json!({"text": "edited"}))).unwrap();
    db.set_local(&db.admit().unwrap(), "moments", &row.id, "pinned", json!("yes")).unwrap();
    db.set_local(&db.admit().unwrap(), "moments", &rid("absent"), "pinned", json!("early")).unwrap();
    db.destroy(&db.admit().unwrap(), "moments", &row.id).unwrap();
    let after = plans(&db);
    assert_eq!((after.0 - before.0, after.1 - before.1), (5, 0), "five writes, each planned once and off the writer");
}

#[test]
fn a_commit_landing_before_a_local_create_is_written_replans_without_duplicating_its_effects() {
    let (db, _directory) = disk();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let rival = db.clone();
    *db.shared.store.before_write.lock().unwrap() = Some(Box::new(move || put(&rival, "users", json!({"id": "u2", "name": "Bob"}))));
    let before = plans(&db);
    let row = db.create(&db.admit().unwrap(), "moments", obj(json!({"userId": "u1", "text": "hi"}))).unwrap();
    let after = plans(&db);
    assert_eq!((after.0 - before.0, after.1 - before.1), (3, 0), "the rival commit and two plans of the create, none under the writer");
    assert_eq!(db.outbox().unwrap().len(), 1, "one queued operation");
    assert_eq!(db.find("moments", &row.id).unwrap().unwrap().text("text"), Some("hi"));
    assert!(db.find("users", &rid("u2")).unwrap().is_some());
}

#[test]
fn a_local_value_for_a_missing_row_is_kept_pending_once_even_when_the_plan_runs_twice() {
    let (db, _directory) = disk();
    let rival = db.clone();
    *db.shared.store.before_write.lock().unwrap() = Some(Box::new(move || put(&rival, "users", json!({"id": "u2", "name": "Bob"}))));
    db.set_local(&db.admit().unwrap(), "moments", &rid("m9"), "pinned", json!("early")).unwrap();
    put(&db, "moments", json!({"id": "m9", "userId": "u2", "text": "arrived"}));
    assert_eq!(db.find("moments", &rid("m9")).unwrap().unwrap().text("pinned"), Some("early"));
}
