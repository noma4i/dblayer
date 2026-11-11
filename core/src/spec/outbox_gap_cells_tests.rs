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
    let queued = first.create("queued").await;
    let temporary = queued.optimistic_value.clone().expect("the create shows its row").id;
    server.wait_held(Kind::Mutate, 1).await;
    within(first.db.close_database()).await.unwrap();
    drop(first);

    let mut models: Vec<crate::schema::ModelSpec> = serde_json::from_str(&super::rig::moment_models(OnFailure::Rollback)).unwrap();
    models[0].fields.push(FieldSpec::scalar("note", ScalarKind::Str).optional());
    let second = Rig::open_models(serde_json::to_string(&models).unwrap(), Some(path), server.clone()).await;

    server.wait_seen("MakeMoment", 2).await;
    assert!(second.row("seed").await.is_none(), "the cached rows of the changed model are reset");
    assert!(second.row(&temporary).await.is_some(), "the optimistic row of the held create is visible");
}

#[test]
fn out_intent_rejected_closed() {
    let mut twin = Twin::new();
    let submitted = twin.post(OutboxInput::Submit { epoch: EPOCH, op: model_op("r", Policy::Rollback, false), reply: SUBMIT_REPLY });
    let id = *twin.state().ops.keys().max().unwrap();
    assert_eq!(kinds(&submitted), vec![CommitKind::Intent], "the intent batch is the only write of the submit");
    let effects = refuse(&mut twin, id, RejectReason::Closed);
    assert_eq!(answers(&effects, SUBMIT_REPLY), vec![OutboxAnswer::Err(Fault::Rejected(RejectReason::Closed))]);
    assert_eq!((twin.stage(id), kinds(&effects)), (Some(St::Done), Vec::new()), "the operation is done and no batch follows");
    assert!(twin.state().ops[&id].outcome.is_some(), "the operation ended with its cause");
}

#[test]
fn out_destroy_failed_keeps_row_hidden() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::Submit { epoch: EPOCH, op: NewOp { action: "destroy".into(), ..model_op("r", Policy::Rollback, false) }, reply: 22 });
    let id = *twin.state().ops.keys().max().unwrap();
    twin.committed(id);
    for step in 1..=39 {
        let failed = twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
        assert_ne!(twin.stage(id), Some(St::Failed), "step {step} is before the limit");
        assert!(!kinds(&failed).contains(&CommitKind::Rollback), "step {step}: the row stays hidden");
        tick(&mut twin, id);
        let sent = twin.committed(id);
        assert!(!kinds(&sent).contains(&CommitKind::Rollback), "step {step}: the row stays hidden after the attempt");
    }
    let last = twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
    let marked = twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Failed), "{last:?}");
    assert!(!kinds(&last).contains(&CommitKind::Rollback) && !kinds(&marked).contains(&CommitKind::Rollback), "a destroy without an answer does not bring the row back");

    let discarded = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("destroy".into()), reply: 23 });
    assert!(kinds(&discarded).contains(&CommitKind::Rollback), "the discard brings the row back");
}
