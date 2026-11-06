//! Cells of `data.md` through the public API of `Database` on the rig with a mock server.

use super::rig::{bounded, within, Rig};
use crate::ffi::{DbError, WireOperationOutcome};
use crate::schema::OnFailure;
use serde_json::json;

#[test]
fn inv_op_19_create_with_an_existing_id_is_an_error_and_writes_nothing() { bounded("inv_op_19", inv_op_19_run) }

async fn inv_op_19_run() {
    let rig = Rig::open(OnFailure::Rollback).await;
    let created = rig.create("hello").await;
    assert!(matches!(rig.outcome(&created).await, WireOperationOutcome::Confirmed { .. }));
    let (revision, mutations) = (rig.row("srv1").await.unwrap().revision, rig.server.mutations());

    let again = within(rig.db.send(crate::ffi::CommandRequest { epoch: rig.epoch(), model: "Moment".into(), values_json: json!({"id": "srv1", "text": "other"}).to_string(), args_json: "{}".into(), upload: None })).await;

    assert!(matches!(&again, Err(DbError::AlreadyExists { model, id }) if model == "Moment" && id == "srv1"), "{again:?}");
    rig.quiet().await;
    assert_eq!((rig.row("srv1").await.unwrap().revision, rig.server.mutations()), (revision, mutations), "nothing is written and nothing is sent");
    assert_eq!(rig.row("srv1").await.unwrap().id, "srv1");
}

#[test]
fn inv_row_05_update_with_unchanged_values_writes_nothing() { bounded("inv_row_05", inv_row_05_run) }

async fn inv_row_05_run() {
    let rig = Rig::open(OnFailure::Rollback).await;
    let created = rig.create("hello").await;
    assert!(matches!(rig.outcome(&created).await, WireOperationOutcome::Confirmed { .. }));
    let (revision, mutations) = (rig.row("srv1").await.unwrap().revision, rig.server.mutations());

    let receipt = rig.update("srv1", "hello").await;

    rig.quiet().await;
    assert!(receipt.is_ok(), "{receipt:?}");
    assert_eq!((rig.row("srv1").await.unwrap().revision, rig.server.mutations()), (revision, mutations), "no commit, no operation, no request");
    let changed = rig.update("srv1", "changed").await.unwrap();
    assert!(matches!(rig.outcome(&changed).await, WireOperationOutcome::Confirmed { .. }));
    assert!(rig.row("srv1").await.unwrap().revision > revision, "a real change commits");
}
