use super::support::*;
use super::*;
use crate::commit::OperationStatus;
use crate::schema::{AssociationKind, AssociationSpec, DeclarationError};
use serde_json::json;
use std::sync::Arc;

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn db_and_subscription_are_send_and_sync() {
    assert_send_sync::<Db>();
    assert_send_sync::<Subscription>();
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
fn open_marks_the_store_ready() {
    let db = booted();
    assert!(db.is_ready());
}

#[test]
fn rows_and_operations_survive_a_reopen() {
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
fn fsck_recovers_interrupted_request_operations_on_open() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    {
        let db = Db::open(&path, all_specs(), None).unwrap();
        db.execute_for_test(
            "INSERT INTO op (action_key, action_mode, status, row_ids, input, transport_started, created_at) VALUES \
             ('users:rename', 'request', 'pending', x'5b5d', x'7b7d', 0, 1), \
             ('users:rename', 'request', 'pending', x'5b5d', x'7b7d', 1, 2), \
             ('users:create', 'durable', 'pending', x'5b5d', x'7b7d', 0, 3)",
        );
    }
    let db = Db::open(&path, all_specs(), None).unwrap();
    let statuses: Vec<OperationStatus> = db.outbox().unwrap().iter().map(|entry| entry.operation.status).collect();
    assert_eq!(statuses, vec![OperationStatus::Failed, OperationStatus::DeliveryUnknown, OperationStatus::Pending]);
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
