use std::sync::Arc;

use serde_json::json;

use crate::owners::contract::BatchAuthor;
use crate::owners::store::{OutcomeOwner, StoreOp};
use crate::owners::write::outbox::OutboxOutcome;
use crate::store::fixtures::{all_specs, put};
use crate::store::Store;

use super::data_inv_tests::{intent, write};

/// The record that the terminal batch of an operation wrote answers a receipt after the intent is gone and after the process restarted, and the end of the session removes it.
#[test]
fn an_operation_outcome_written_with_the_intent_removal_survives_a_reopen_and_goes_with_the_session() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("a.sqlite");
    let confirmed = OutboxOutcome::Confirmed { revision: 2, result: Some("{\"ok\":true}".into()), row_id: Some("m1".into()) };
    {
        let db = Store::open_models(Some(file.as_path()), all_specs(), Arc::new(crate::runtime::SystemClock)).unwrap();
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![intent(5)]);
        write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Outcome { owner: OutcomeOwner::Operation, id: 5, cycle_id: 1, model: "moments".into(), outcome: confirmed }, StoreOp::RemoveIntent { op_id: 5 }]);
        assert!(db.read_outcome(OutcomeOwner::Operation, 5, 1).unwrap().is_some(), "the record answers while the intent is gone");
    }
    let db = Store::open_models(Some(file.as_path()), all_specs(), Arc::new(crate::runtime::SystemClock)).unwrap();
    assert!(matches!(db.read_outcome(OutcomeOwner::Operation, 5, 1).unwrap(), Some(OutboxOutcome::Confirmed { result: Some(_), row_id: Some(_), .. })), "the record outlives the process with its result and row");
    assert_eq!(db.read_outcome(OutcomeOwner::Transfer, 5, 1).unwrap(), None, "the owner is part of the key");
    db.erase_account(db.generation().next(), false, false).unwrap();
    assert_eq!(db.read_outcome(OutcomeOwner::Operation, 5, 1).unwrap(), None, "the end of the session removes the records of the account");
}

/// A record of an operation lives 7 days: the restore names its id, so the owner never gives a new operation an id with a stale record.
#[test]
fn the_restore_names_the_highest_id_of_an_operation_record_as_the_floor_of_new_ids() {
    let db = Store::open_models(None, all_specs(), Arc::new(crate::runtime::SystemClock)).unwrap();
    assert_eq!(db.read_restore().unwrap().reserved, None);
    write(&db, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Outcome { owner: OutcomeOwner::Operation, id: 7, cycle_id: 1, model: "moments".into(), outcome: OutboxOutcome::Discarded }]);
    assert_eq!(db.read_restore().unwrap().reserved, Some(7));
}
