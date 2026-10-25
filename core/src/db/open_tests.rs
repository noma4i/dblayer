use super::fixtures::*;
use super::*;
use crate::operation::OperationStatus;
use crate::schema::{AssociationKind, AssociationSpec, DeclarationError};
use crate::ddl::RecreateReason;
use serde_json::json;
use std::sync::Arc;

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn db_and_subscription_are_send_and_sync() {
    assert_send_sync::<Db>();
    assert_send_sync::<ObserverHandle>();
}

#[test]
fn open_rejects_invalid_models() {
    let mut specs = all_specs();
    specs[1].associations.push(AssociationSpec::new("ghost", AssociationKind::BelongsTo, "nowhere", "userId"));
    match Db::open_in_memory(specs, None) {
        Err(DbError::Invalid(faults)) => assert!(faults.iter().any(|fault| fault.error == DeclarationError::UnknownTarget("ghost".into()))),
        other => panic!("expected invalid models, got {:?}", other.err()),
    }
}

#[test]
fn inv_open_04_a_reopen_with_the_same_schema_keeps_rows_and_operations() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let created = {
        let db = Db::open(&path, all_specs(), Some("v1")).unwrap();
        db.create("moments", obj(json!({"userId": "u1", "text": "kept"}))).unwrap()
    };
    let db = Db::open(&path, all_specs(), Some("v1")).unwrap();
    assert_eq!(db.find("moments", &created.id).unwrap().unwrap().text("text"), Some("kept"));
    let outbox = db.outbox().unwrap();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].operation.status, OperationStatus::Pending);
}

#[test]
fn fsck_keeps_unsent_operations_and_marks_started_ones_unknown_on_open() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    {
        let db = Db::open(&path, all_specs(), None).unwrap();
        db.execute_for_test(
            "INSERT INTO op (action_key, status, transport_started, created_at, since) VALUES \
             ('users:rename', 'pending', 0, 1, 1), \
             ('users:rename', 'pending', 1, 2, 2), \
             ('users:create', 'pending', 0, 3, 3)",
        );
    }
    let db = Db::open(&path, all_specs(), None).unwrap();
    let statuses: Vec<OperationStatus> = db.outbox().unwrap().iter().map(|entry| entry.operation.status).collect();
    assert_eq!(statuses, vec![OperationStatus::Pending, OperationStatus::DeliveryUnknown, OperationStatus::Pending]);
}

#[test]
fn concurrent_creates_all_land() {
    let db = Arc::new(booted());
    let workers: Vec<_> = (0..4)
        .map(|worker| {
            let db = db.clone();
            std::thread::spawn(move || {
                for index in 0..25 {
                    db.create("moments", obj(json!({"userId": "u1", "text": format!("{worker}-{index}")}))).unwrap();
                }
            })
        })
        .collect();
    workers.into_iter().for_each(|worker| worker.join().unwrap());
    assert_eq!(db.count(&crate::query::Query::new("moments")).unwrap(), 100);
    assert_eq!(db.outbox().unwrap().len(), 100);
}

fn reopened_after(setup: impl FnOnce(&Db)) -> (tempfile::TempDir, Db) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    {
        let db = Db::open(&path, all_specs(), None).unwrap();
        setup(&db);
    }
    let db = Db::open(&path, all_specs(), None).unwrap();
    (directory, db)
}

fn quarantined(db: &Db, reason: &str) -> i64 {
    db.shared.store.lock().connection.query_row("SELECT COUNT(*) FROM quarantine WHERE reason = ?1", [reason], |row| row.get(0)).unwrap()
}

#[test]
fn inv_identity_02_recovery_distinguishes_models_with_the_same_temp_id() {
    for mode in ["rebuild", "one_orphan", "two_orphans"] {
        let (_directory, db) = reopened_after(|db| {
            db.create("users", obj(json!({"id": "temp-shared", "name": "user"}))).unwrap();
            db.create("moments", obj(json!({"id": "temp-shared", "text": "moment"}))).unwrap();
            match mode {
                "rebuild" => db.execute_for_test("DELETE FROM row_moments"),
                "one_orphan" => db.execute_for_test("DELETE FROM op WHERE action_key = 'moments:create'"),
                _ => db.execute_for_test("DELETE FROM op"),
            }
        });
        assert_eq!(db.find("moments", &rid("temp-shared")).unwrap().is_some(), mode == "rebuild", "{mode}");
        assert_eq!(db.find("users", &rid("temp-shared")).unwrap().is_some(), mode != "two_orphans", "{mode}");
        assert_eq!(quarantined(&db, "orphan-temp-row"), match mode { "rebuild" => 0, "one_orphan" => 1, _ => 2 });
    }
}

#[test]
fn fsck_rebuilds_a_missing_temp_row_from_its_create_operation_on_open() {
    let mut temp_id = None;
    let (_directory, db) = reopened_after(|db| {
        let created = db.create("moments", obj(json!({"userId": "u1", "text": "lost"}))).unwrap();
        db.execute_for_test("DELETE FROM row_moments");
        temp_id = Some(created.id.clone());
    });
    let rebuilt = db.find("moments", &temp_id.unwrap()).unwrap().unwrap();
    assert_eq!(rebuilt.text("text"), Some("lost"));
    assert_eq!(db.outbox().unwrap().len(), 1);
}

#[test]
fn inv_row_07_fsck_quarantines_temp_rows_without_an_operation_and_keeps_the_others() {
    let mut kept_id = None;
    let (_directory, db) = reopened_after(|db| {
        put(db, "moments", json!({"id": "temp-orphan", "text": "orphan"}));
        put(db, "moments", json!({"id": "m1", "text": "server row"}));
        kept_id = Some(db.create("moments", obj(json!({"userId": "u1", "text": "queued"}))).unwrap().id.clone());
    });
    assert!(db.find("moments", &rid("temp-orphan")).unwrap().is_none());
    assert!(db.find("moments", &rid("m1")).unwrap().is_some());
    assert!(db.find("moments", &kept_id.unwrap()).unwrap().is_some());
    assert_eq!(quarantined(&db, "orphan-temp-row"), 1);
}

#[test]
fn inv_panic_01_open_reports_a_thread_that_cannot_start_and_leaves_no_open_handle() {
    for refused_name in ["dblayer-dispatch", "dblayer-read", "dblayer-ephemeral"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db.sqlite");
        super::threads::refuse_spawn_here(refused_name);
        let refused = Db::open(&path, all_specs(), None);
        super::threads::refuse_spawn_here("");
        match refused {
            Err(DbError::ThreadSpawn { name, .. }) => assert_eq!(name, refused_name),
            other => panic!("expected a thread spawn error for {refused_name}, got {:?}", other.err()),
        }
        Db::open(&path, all_specs(), None).expect("the file opens again after the refused open");
    }
}

#[test]
fn inv_open_03_the_open_report_names_what_the_open_discarded() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    std::fs::write(&path, "this is not a database ".repeat(400)).unwrap();
    let report = Db::open(&path, all_specs(), None).unwrap().open_report().clone();
    assert_eq!(report, OpenReport { recreated: Some(RecreateReason::UnreadableFile), changed_models: vec![], reset_models: vec![] });

    assert_eq!(Db::open(&path, all_specs(), None).unwrap().open_report(), &OpenReport { recreated: None, changed_models: vec![], reset_models: vec![] });

    let mut changed = all_specs();
    changed[1].fields.push(crate::schema::FieldSpec::scalar("extra", crate::value::ScalarKind::Str).optional());
    let report = Db::open(&path, changed, None).unwrap().open_report().clone();
    assert_eq!(report, OpenReport { recreated: None, changed_models: vec!["moments".to_string()], reset_models: ["chats", "messages", "moments", "users", "vibes"].map(String::from).to_vec() });
}
