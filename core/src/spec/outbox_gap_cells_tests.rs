use super::outbox_cells_tests::{answers, kinds, refuse, tick};
use super::outbox_harness::*;
use super::outbox_harness::{Policy, St};
use super::rig::{bounded, within, Kind, Rig, Server};
use crate::owners::contract::RejectReason;
use crate::owners::write::outbox::{CommitKind, Fault, NewOp, OutboxAnswer, OutboxInput};
use crate::schema::{FieldSpec, OnFailure};
use crate::value::ScalarKind;

#[test]
fn out_models_reset_keeps_operations() { bounded("out_models_reset_keeps_operations", reset_run) }

async fn reset_run() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("reset.sqlite").to_str().unwrap().to_string();
    let server = Server::new();
    let first = Rig::open_at(OnFailure::Rollback, Some(path.clone()), server.clone()).await;
    first.seed_row("seed").await;
    assert!(first.row("seed").await.is_some());
    server.hold(Kind::Mutate, true);
    first.create("queued").await;
    server.wait_held(Kind::Mutate, 1).await;
    within(first.db.close_database()).await.unwrap();
    drop(first);

    let mut models: Vec<crate::schema::ModelSpec> = serde_json::from_str(&super::rig::moment_models(OnFailure::Rollback)).unwrap();
    models[0].fields.push(FieldSpec::scalar("note", ScalarKind::Str).optional());
    let second = Rig::open_models(serde_json::to_string(&models).unwrap(), Some(path), server.clone()).await;

    server.wait_seen("MakeMoment", 2).await;
    assert!(second.row("seed").await.is_none(), "the cached rows of the changed model are reset");
}

#[test]
fn out_intent_rejected_closed() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    let effects = refuse(&mut twin, id, RejectReason::Closed);
    assert_eq!(answers(&effects, SUBMIT_REPLY), vec![OutboxAnswer::Err(Fault::Rejected(RejectReason::Closed))]);
    assert_eq!((twin.stage(id), kinds(&effects)), (Some(St::Done), Vec::new()), "the operation is done and no batch follows");

    let mut reopened = Twin::new();
    reopened.post(OutboxInput::Restored { ops: Vec::new(), pending_lost: false });
    assert!(reopened.state().ops.is_empty(), "no operation after the open");
}

#[test]
fn out_destroy_failed_keeps_row_hidden() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::Submit { epoch: EPOCH, op: NewOp { action: "destroy".into(), ..model_op("r", Policy::Rollback, false) }, reply: 22 });
    let id = *twin.state().ops.keys().max().unwrap();
    twin.committed(id);
    for _ in 0..39 {
        twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
        tick(&mut twin, id);
        twin.committed(id);
    }
    let last = twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
    if twin.stage(id) != Some(St::Failed) { twin.committed(id); }
    assert_eq!(twin.stage(id), Some(St::Failed), "{last:?}");
    assert!(!kinds(&last).contains(&CommitKind::Rollback), "a destroy without an answer does not bring the row back");

    let discarded = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("destroy".into()), reply: 23 });
    assert!(kinds(&discarded).contains(&CommitKind::Rollback), "the discard brings the row back");
}
