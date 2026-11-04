use serde_json::json;

use super::fixtures::*;
use crate::operation::OperationStatus;
use crate::db::fixtures::*;
use crate::db::{Db, DbError};

fn row_in_state(state: &str) -> Db {
    let db = booted();
    if state == "synced" || state == "hidden" {
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    } else {
        db.create(&db.admit().unwrap(), "moments", obj(json!({"id": "m1", "userId": "u1", "text": "one"}))).unwrap();
    }
    match state {
        "sending" => start(&db, only_seq(&db)),
        "failed" => fail(&db, only_seq(&db), OperationStatus::Failed),
        "delivery_unknown" => {
            let seq = only_seq(&db);
            start(&db, seq);
            fail(&db, seq, OperationStatus::DeliveryUnknown);
        }
        "hidden" => {
            db.destroy(&db.admit().unwrap(), "moments", &rid("m1")).unwrap();
        }
        _ => {}
    }
    db
}

#[test]
fn inv_op_19_create_with_an_existing_id_is_an_error_and_writes_nothing() {
    for state in ["synced", "pending", "sending", "failed", "delivery_unknown", "hidden"] {
        let db = row_in_state(state);
        let before = snapshot(&db);
        let outcome = db.create(&db.admit().unwrap(), "moments", obj(json!({"id": "m1", "userId": "u1", "text": "other"})));
        assert!(matches!(outcome, Err(DbError::AlreadyExists { .. })), "state {state}: {outcome:?}");
        assert_eq!(snapshot(&db), before, "state {state}");
    }
}

#[test]
fn inv_op_20_unsent_updates_of_one_record_stay_separate_operations_in_order() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    for text in ["a", "b", "c"] {
        db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": text}))).unwrap();
    }
    let outbox = db.outbox().unwrap();
    assert_eq!(outbox.len(), 3);
    assert!(outbox.windows(2).all(|pair| pair[0].operation.id.0 < pair[1].operation.id.0));
    let sent: Vec<&str> = outbox.iter().map(|entry| entry.input["text"].as_str().unwrap()).collect();
    assert_eq!(sent, ["a", "b", "c"]);
    assert!(outbox.iter().all(|entry| entry.rollback.is_some()));
}
