//! Owner-level tests of the `outbox` machine: the properties of the offline queue, the receipt, the restored answer, the status column and the stray inputs. Names are the `Test` column.

use super::outbox_cells_tests::{answers, kinds, sending, sends, submit_op, tick};
use super::outbox_harness::*;
use super::outbox_harness::{Policy, RWhy, St};
use crate::owners::contract::{Committed, Online};
use crate::owners::session::StoredAnswer;
use crate::owners::write::outbox::{CommitKind, NewOp, OutboxAnswer, OutboxInput, Receipt, ReserveTarget, StoredOp};

fn stored(op_id: u64, row: &str) -> StoredOp {
    StoredOp { answered: None, op_id, op: model_op(row, Policy::Rollback, false), created_at: 0, error_mark: false, pending: None, attempts: 1, cycle_id: 1, cause: None, restore_failed: None, restore_element: None, removed_rows: Vec::new() }
}

fn optimistic_row() -> crate::store::Row {
    std::sync::Arc::new(crate::store::Record { id: crate::identity::RowId::new("r").expect("the row id"), revision: 11, data: serde_json::Map::new(), sync: Default::default() })
}

fn commit_shown(twin: &mut Twin, id: crate::owners::write::outbox::OpId, shown: Vec<Option<crate::store::Row>>) -> Vec<OutboxEffect> {
    let batch = twin.batches[&id];
    twin.revision += 1;
    twin.post(OutboxInput::Committed(Committed { batch, revision: twin.revision, tables: vec!["moments".into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown, files_removed: 0 }))
}

fn receipt_of(effects: &[OutboxEffect], reply: u64) -> Receipt {
    effects.iter().find_map(|effect| match effect { OutboxEffect::Reply { reply: id, answer: OutboxAnswer::Receipt(receipt) } if *id == reply => Some(receipt.clone()), _ => None }).expect("a receipt")
}

/// P-OUT-32: any write without the network becomes Queued with its intent and goes out on Online, no `Err(Offline)`.
#[test]
fn out_prop_offline_queue() {
    for action in ["update", "create", "destroy"] {
        let mut twin = Twin::new();
        twin.post(OutboxInput::Online(Online(false)));
        let op = NewOp { action: action.into(), ..model_op("r", Policy::Rollback, false) };
        let id = submit_op(&mut twin, op, 30);
        let committed = twin.committed(id);
        assert_eq!((twin.stage(id), sends(&committed)), (Some(St::Queued), 0), "{action}: offline the operation is Queued with its intent");
        assert!(matches!(answers(&committed, 30).as_slice(), [OutboxAnswer::Receipt(_)]), "{action}: a receipt, not an offline error: {committed:?}");
        let online = twin.post(OutboxInput::Online(Online(true)));
        assert_eq!((twin.stage(id), sends(&online)), (Some(St::Sending), 1), "{action}: it goes out on Online with no platform call");
    }
}

/// P-OUT-36: the receipt carries the optimistic row of its commit, fixed; a fast confirm does not rewrite it; `None` only without optimistic effects.
#[test]
fn out_prop_receipt_fast_confirm() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    let row = optimistic_row();
    let committed = commit_shown(&mut twin, id, vec![Some(row.clone())]);
    let receipt = receipt_of(&committed, SUBMIT_REPLY);
    assert_eq!(receipt.optimistic, Some(row.clone()), "the receipt carries the optimistic row of the commit");
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    twin.committed(id);
    assert_eq!(receipt.optimistic, Some(row), "the receipt is fixed, the fast confirm does not rewrite it");

    let mut plain = Twin::new();
    let op = NewOp { effects: Default::default(), ..model_op("r", Policy::Rollback, false) };
    let id = submit_op(&mut plain, op, 30);
    let committed = commit_shown(&mut plain, id, vec![None]);
    assert_eq!(receipt_of(&committed, 30).optimistic, None, "no optimistic effects means no optimistic row");
}

/// P-OUT-20: a saved `Ok` answer lands the decoded body rows with no request.
#[test]
fn out_prop_restored_answer_rows() {
    let mut twin = Twin::bare();
    twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into())] });
    let answered = StoredOp { answered: Some((41, StoredAnswer::Ok("{}".into()))), ..stored(7, "r") };
    let restored = twin.post(OutboxInput::Restored { ops: vec![answered], pending_lost: false });
    assert_eq!((twin.stage(7), sends(&restored)), (Some(St::Landing), 0), "the saved answer lands with no request");
    twin.post(OutboxInput::RestoreReady { epoch: EPOCH });
    let landed = twin.post(OutboxInput::Response { op_id: 7, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert!(twin.store_text.contains("server") && kinds(&landed).contains(&CommitKind::Land), "the decoded body rows land in the snapshot: {}", twin.store_text);
}

/// P-OUT-33: the batch written on each transition matches the status of the stage.
#[test]
fn out_prop_status_column() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    assert_eq!((twin.stage(id), kinds(&twin.last)), (Some(St::Submitting), vec![CommitKind::Intent]), "Submit writes the intent");
    twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Sending), "the committed intent sends");
    twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
    let attempt = tick(&mut twin, id);
    assert_eq!((twin.stage(id), kinds(&attempt)), (Some(St::PersistingAttempt), vec![CommitKind::Attempt]), "the next attempt writes the sending status");
    twin.post(OutboxInput::Online(Online(false)));
    let requeued = twin.committed(id);
    assert_eq!((twin.stage(id), kinds(&requeued)), (Some(St::Queued), vec![CommitKind::Requeue]), "offline requeues with the pending status");
}

/// P-OUT-33: the transition `Sending -> Queued` writes the pending status with a `Requeue` batch.
#[test]
fn out_prop_status_requeue() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::Online(Online(false)));
    let requeued = twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
    assert_eq!((twin.stage(id), kinds(&requeued)), (Some(St::Queued), vec![CommitKind::Requeue]), "a temporary failure offline requeues");

    let mut persist = Twin::new();
    let id = sending(&mut persist, Policy::Rollback, false);
    persist.post(OutboxInput::TransportError { op_id: id, temporary: true });
    tick(&mut persist, id);
    persist.post(OutboxInput::Online(Online(false)));
    let committed = persist.committed(id);
    assert_eq!((persist.stage(id), kinds(&committed)), (Some(St::Queued), vec![CommitKind::Requeue]), "a persisted attempt committed offline returns to Queued pending");
}

/// P-OUT-38: the stray pairs (RestoreReady, Restored, Submit of a busy row, ReserveOp) leave the addressed operation unchanged.
#[test]
fn out_prop_stray_input_ignored() {
    let states = [St::Submitting, St::Queued, St::PersistingAttempt, St::Sending, St::Landing, St::RollingBack(RWhy::Rejected), St::MarkingFailed, St::MarkingRetry, St::Failed, St::WaitingDeclaration];
    for state in states {
        let ctx = if state == St::Queued { offline() } else { ready() };
        for input in [OutboxInput::RestoreReady { epoch: EPOCH }, OutboxInput::Restored { ops: Vec::new(), pending_lost: false }] {
            let (mut twin, id) = reach(state, &ctx);
            let effects = twin.post(input.clone());
            assert_eq!((twin.stage(id), effects.is_empty()), (Some(state), true), "{state:?} x {input:?}: the operation and its effects are unchanged");
        }
        let (mut twin, id) = reach(state, &ctx);
        twin.post(OutboxInput::Submit { epoch: EPOCH, op: model_op("r", Policy::Rollback, false), reply: 950 });
        assert_eq!(twin.stage(id), Some(state), "{state:?} x Submit: the earlier operation is unchanged");
        let (mut twin, id) = reach(state, &ctx);
        twin.post(OutboxInput::ReserveOp { epoch: EPOCH, target: ReserveTarget::Transfer(1) });
        assert_eq!(twin.stage(id), Some(state), "{state:?} x ReserveOp: the earlier operation is unchanged");
    }
}

/// Waits table: a Submit while the slot is taken gets a receipt and waits, one request at a time (DRAIN_LIMIT = 1).
#[test]
fn out_queued_drain_limit_api() {
    let mut twin = Twin::new();
    let first = twin.submit("ra", Policy::Rollback, false);
    twin.committed(first);
    assert_eq!(twin.stage(first), Some(St::Sending), "the first operation holds the one slot");
    let second = submit_op(&mut twin, model_op("rb", Policy::Rollback, false), 30);
    let committed = twin.committed(second);
    assert_eq!((twin.stage(second), sends(&committed)), (Some(St::Queued), 0), "the second operation waits for the slot, no request");
    assert!(matches!(answers(&committed, 30).as_slice(), [OutboxAnswer::Receipt(_)]), "the caller still gets a receipt: {committed:?}");
    let landed = twin.post(OutboxInput::Response { op_id: first, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert_eq!((twin.stage(first), twin.stage(second), sends(&landed)), (Some(St::Landing), Some(St::Sending), 1), "the freed slot sends the second, one request at a time");
    twin.committed(first);
    assert_eq!(twin.stage(first), Some(St::Done), "the first operation lands");
}
