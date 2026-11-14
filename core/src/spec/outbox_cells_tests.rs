//! Owner-level tests of the `outbox` machine: the cells of the attempt counter,
//! the swap of a temporary id and the unsent `create` cancelled by a `destroy`. Names are the `Test` column.

use super::outbox_harness::*;
use super::outbox_harness::{Policy, RReason, St, RWhy};
use crate::owners::contract::{Online, RejectReason, Rejected};
use crate::owners::store::StoreOp;
use crate::owners::write::outbox::{CommitKind, Fault, NewOp, OpId, OutboxAnswer, OutboxInput, OutboxOutcome};

pub(super) const RETRY_REPLY: u64 = 802;

pub(super) fn timers(effects: &[OutboxEffect]) -> usize { effects.iter().filter(|effect| matches!(effect, OutboxEffect::Schedule)).count() }

pub(super) fn transfer_outcomes(effects: &[OutboxEffect]) -> Vec<OutboxOutcome> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::TransferOutcome { outcome, .. } => Some(outcome.clone()), _ => None }).collect()
}

pub(super) fn refuse(twin: &mut Twin, id: OpId, reason: RejectReason) -> Vec<OutboxEffect> {
    let batch = twin.batches[&id];
    twin.post(OutboxInput::Rejected(Rejected { batch, reason }))
}

pub(super) fn tick(twin: &mut Twin, id: OpId) -> Vec<OutboxEffect> {
    let cycle_id = twin.state().ops[&id].cycle_id;
    twin.post(OutboxInput::RetryDue { op_id: id, cycle_id })
}

pub(super) fn retry(twin: &mut Twin) -> Vec<OutboxEffect> { twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply: RETRY_REPLY }) }

pub(super) fn submit_op(twin: &mut Twin, op: NewOp, reply: u64) -> OpId {
    twin.post(OutboxInput::Submit { epoch: EPOCH, op, reply });
    *twin.state().ops.keys().max().expect("the operation exists")
}

pub(super) fn sending(twin: &mut Twin, policy: Policy, transfer: bool) -> OpId {
    let id = twin.submit("r", policy, transfer);
    twin.committed(id);
    id
}

/// `Sending` after a temporary failure and its timer: the next attempt is being written.
pub(super) fn persisting(twin: &mut Twin, transfer: bool) -> OpId {
    let id = sending(twin, Policy::Rollback, transfer);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
    tick(twin, id);
    assert_eq!(twin.stage(id), Some(St::PersistingAttempt));
    id
}

/// `Sending` with the 40th attempt written.
pub(super) fn exhausted(twin: &mut Twin, policy: Policy) -> OpId {
    let id = sending(twin, policy, false);
    for _ in 0..39 {
        twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
        tick(twin, id);
        twin.committed(id);
    }
    assert_eq!((twin.stage(id), twin.state().ops[&id].attempts), (Some(St::Sending), 40));
    id
}

pub(super) fn marking_failed(twin: &mut Twin, transfer: bool) -> OpId {
    let id = sending(twin, Policy::Keep, transfer);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
    assert_eq!(twin.stage(id), Some(St::MarkingFailed));
    id
}

pub(super) fn failed(twin: &mut Twin) -> OpId {
    let id = marking_failed(twin, false);
    twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Failed));
    id
}

pub(super) fn marking_retry(twin: &mut Twin) -> OpId {
    let id = failed(twin);
    retry(twin);
    assert_eq!(twin.stage(id), Some(St::MarkingRetry));
    id
}

/// An unsent `Queued` operation of the row `r`: a blocker holds the row.
pub(super) fn queued_behind_blocker(twin: &mut Twin, op: NewOp) -> OpId {
    sending(twin, Policy::Rollback, false);
    let id = submit_op(twin, op, 20);
    twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Queued));
    id
}

pub(super) fn unchanged_by(twin: &mut Twin, inputs: Vec<OutboxInput>) {
    for input in inputs {
        let effects = twin.post(input.clone());
        assert!(effects.is_empty(), "{input:?}: {effects:?}");
    }
}

#[test]
fn out_submit_stale() {
    let mut twin = Twin::new();
    let effects = twin.post(OutboxInput::Submit { epoch: EPOCH + 1, op: model_op("r", Policy::Rollback, false), reply: 901 });
    assert_eq!(answers(&effects, 901), vec![OutboxAnswer::Err(Fault::NoSession)]);
    assert!(twin.state().ops.is_empty());
    assert_eq!(effects.len(), 1);
}

#[test]
fn out_submit() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    assert_eq!((twin.stage(id), kinds(&twin.last), twin.last.len()), (Some(St::Submitting), vec![CommitKind::Intent], 1));
    assert!(has_op!(twin, StoreOp::Intent { op_id, model, row_id, attempts: 1, cycle_id: 1, .. } if *op_id == id && model == "Moment" && row_id == "r"), "{:?}", twin.store_ops);
}

#[test]
fn out_sending_temp() {
    for dropped in [false, true] {
        let mut twin = Twin::new();
        let id = sending(&mut twin, Policy::Rollback, false);
        let input = if dropped { OutboxInput::Dropped { op_id: id } } else { OutboxInput::TransportError { op_id: id, temporary: true } };
        let effects = twin.post(input);
        assert_eq!((twin.stage(id), twin.state().ops[&id].waiting_retry, timers(&effects), effects.len()), (Some(St::Sending), true, 1, 1));
    }
}

#[test]
fn out_sending_tick() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
    let effects = tick(&mut twin, id);
    assert_eq!((twin.stage(id), kinds(&effects), effects.len()), (Some(St::PersistingAttempt), vec![CommitKind::Attempt], 1));
    assert!(has_op!(twin, StoreOp::Attempt { op_id, attempts: 2, cycle_id: 1, .. } if *op_id == id), "{:?}", twin.store_ops);
}

#[test]
fn out_sending_reject_rollback() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let effects = twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
    assert_eq!((twin.stage(id), kinds(&effects), effects.len()), (Some(St::RollingBack(RWhy::Rejected)), vec![CommitKind::Rollback], 1));
    let request_id = twin.state().ops[&id].request_id;
    assert!(twin.store_ops.contains(&StoreOp::RollBack { op_id: id }), "{:?}", twin.store_ops);
    assert!(twin.store_ops.contains(&StoreOp::RemoveIntent { op_id: id }), "{:?}", twin.store_ops);
    assert!(twin.store_ops.contains(&StoreOp::RemoveAnswer { request_id }), "{:?}", twin.store_ops);
}

#[test]
fn out_sending_reject_keep() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Keep, false);
    let effects = twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
    assert_eq!((twin.stage(id), kinds(&effects), effects.len()), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark], 1));
    let request_id = twin.state().ops[&id].request_id;
    assert!(has_op!(twin, StoreOp::MarkError { op_id, error: Some(_) } if *op_id == id), "{:?}", twin.store_ops);
    assert!(twin.store_ops.contains(&StoreOp::RemoveAnswer { request_id }), "{:?}", twin.store_ops);
}

#[test]
fn out_sending_response() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let request_id = twin.state().ops[&id].request_id;
    let effects = twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert_eq!((twin.stage(id), kinds(&effects), effects.len()), (Some(St::Landing), vec![CommitKind::Land], 1));
    assert!(twin.store_ops.contains(&StoreOp::RemoveIntent { op_id: id }) && twin.store_ops.contains(&StoreOp::RemoveAnswer { request_id }), "{:?}", twin.store_ops);
    assert!(has_op!(twin, StoreOp::Rows { model, rows } if model == "Moment" && rows.iter().any(|row| row.values.contains("state"))), "{:?}", twin.store_ops);
}

#[test]
fn out_landing_retry() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let effects = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: RETRY_REPLY });
    assert_eq!(twin.stage(id), Some(St::Landing));
    assert!(matches!(answers(&effects, RETRY_REPLY).as_slice(), [OutboxAnswer::Receipt(receipt)] if receipt.op_id == id));
    assert_eq!(effects.len(), 1);
    for invalid_first in [false, true] {
        let mut twin = Twin::new();
        let id = sending(&mut twin, Policy::Rollback, false);
        twin.waiter(id);
        twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
        if invalid_first { refuse(&mut twin, id, RejectReason::Invalid("bad row".into())); }
        let retried = retry(&mut twin);
        assert_eq!((twin.stage(id), sends(&retried)), (Some(St::Landing), 0), "invalid_first={invalid_first}: {retried:?}");
        for _ in 0..3 {
            let refused = refuse(&mut twin, id, RejectReason::Storage("disk".into()));
            let again = tick(&mut twin, id);
            assert_eq!((sends(&refused), sends(&again), twin.stage(id)), (0, 0, Some(St::Landing)));
            assert!(retry(&mut twin).iter().all(|effect| !matches!(effect, OutboxEffect::Send { .. })));
        }
        let done = twin.committed(id);
        assert!(answers(&done, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Confirmed { .. }))), "{done:?}");
    }
}

#[test]
fn out_outcome_stale() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let effects = twin.post(OutboxInput::Outcome { receipt: twin.receipt(id, EPOCH + 1), reply: WAITER , discard_on_failure: false});
    assert_eq!((twin.stage(id), answers(&effects, WAITER), effects.len()), (Some(St::Sending), vec![OutboxAnswer::Err(Fault::NoSession)], 1));
}

#[test]
fn out_outcome_done() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    twin.committed(id);
    let effects = twin.post(OutboxInput::Outcome { receipt: twin.receipt(id, EPOCH), reply: WAITER , discard_on_failure: false});
    assert_eq!(twin.stage(id), Some(St::Done));
    assert!(matches!(answers(&effects, WAITER).as_slice(), [OutboxAnswer::Outcome(OutboxOutcome::Confirmed { .. })]));
    assert_eq!(effects.len(), 1);
}

#[test]
fn out_done_refuses() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    twin.committed(id);
    for input in [
        OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: RETRY_REPLY },
        OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: RETRY_REPLY },
    ] {
        let effects = twin.post(input);
        assert_eq!((twin.stage(id), answers(&effects, RETRY_REPLY), effects.len()), (Some(St::Done), vec![OutboxAnswer::Err(Fault::NotFound)], 1));
    }
}

#[test]
fn out_failed_retry() {
    let mut twin = Twin::new();
    let id = failed(&mut twin);
    let effects = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: RETRY_REPLY });
    assert_eq!((twin.stage(id), kinds(&effects), effects.len()), (Some(St::MarkingRetry), vec![CommitKind::ClearMark], 1));
    assert!(twin.store_ops.contains(&StoreOp::MarkError { op_id: id, error: None }), "{:?}", twin.store_ops);
    assert!(has_op!(twin, StoreOp::Attempt { op_id, attempts: 1, cycle_id: 2, .. } if *op_id == id), "{:?}", twin.store_ops);
}

#[test]
fn out_failed_discard() {
    let mut twin = Twin::new();
    let id = failed(&mut twin);
    let effects = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: DISCARD_REPLY });
    assert_eq!((twin.stage(id), kinds(&effects), effects.len()), (Some(St::RollingBack(RWhy::Discarded)), vec![CommitKind::Rollback], 1));
    let request_id = twin.state().ops[&id].request_id;
    assert!(twin.store_ops.contains(&StoreOp::RollBack { op_id: id }), "{:?}", twin.store_ops);
    assert!(twin.store_ops.contains(&StoreOp::RemoveIntent { op_id: id }), "{:?}", twin.store_ops);
    assert!(twin.store_ops.contains(&StoreOp::RemoveAnswer { request_id }), "{:?}", twin.store_ops);
}

#[test]
fn out_submitting_retry() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    let effects = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: RETRY_REPLY });
    assert_eq!((twin.stage(id), effects.len()), (Some(St::Submitting), 0));
    let effects = twin.committed(id);
    assert!(matches!(answers(&effects, RETRY_REPLY).as_slice(), [OutboxAnswer::Receipt(receipt)] if receipt.op_id == id));
}

#[test]
fn out_unknown_operation() {
    let mut twin = Twin::new();
    for input in [
        OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: WAITER },
        OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: WAITER },
        OutboxInput::Outcome { receipt: twin.receipt(99, EPOCH), reply: WAITER , discard_on_failure: false},
    ] {
        let effects = twin.post(input);
        assert_eq!((answers(&effects, WAITER), effects.len(), twin.state().ops.len()), (vec![OutboxAnswer::Err(Fault::NotFound)], 1, 0));
    }
}

#[test]
fn out_queued_send() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Queued));
    let effects = twin.post(OutboxInput::Online(Online(true)));
    assert_eq!((twin.stage(id), sends(&effects), effects.len()), (Some(St::Sending), 1, 1));
    assert_eq!(twin.state().ops[&id].attempts, 1);
}

#[test]
fn out_rollback_discard() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    let effects = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: DISCARD_REPLY });
    assert_eq!((twin.stage(id), kinds(&effects), effects.len()), (Some(St::RollingBack(RWhy::Discarded)), vec![CommitKind::Rollback], 1));
    assert!(has_op!(twin, StoreOp::RollBack { .. }) && has_op!(twin, StoreOp::RemoveIntent { .. }), "{:?}", twin.store_ops);
    let effects = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: 803 });
    assert_eq!((twin.stage(id), effects.len()), (Some(St::RollingBack(RWhy::Discarded)), 0));
    let effects = twin.committed(id);
    assert_eq!(answers(&effects, DISCARD_REPLY), vec![OutboxAnswer::Ok]);
    assert_eq!(answers(&effects, 803), vec![OutboxAnswer::Ok]);
}

#[test]
fn out_submitting_committed_queued() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    let id = twin.submit("r", Policy::Rollback, false);
    let effects = twin.committed(id);
    assert_eq!((twin.stage(id), sends(&effects), effects.len()), (Some(St::Queued), 0, 1));
    assert!(matches!(answers(&effects, SUBMIT_REPLY).as_slice(), [OutboxAnswer::Receipt(receipt)] if receipt.op_id == id));
}

#[test]
fn out_submitting_committed() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    let effects = twin.committed(id);
    assert_eq!((twin.stage(id), sends(&effects), twin.state().ops[&id].attempts), (Some(St::Sending), 1, 1));
    assert!(matches!(answers(&effects, SUBMIT_REPLY).as_slice(), [OutboxAnswer::Receipt(receipt)] if receipt.op_id == id && receipt.cycle_id == 1 && receipt.revision == twin.revision));
}

#[test]
fn out_submit_after_closing() {
    let mut twin = Twin::new();
    twin.det.close();
    twin.run();
    let effects = twin.post(OutboxInput::Submit { epoch: EPOCH, op: model_op("r", Policy::Rollback, false), reply: SUBMIT_REPLY });
    assert_eq!((answers(&effects, SUBMIT_REPLY), effects.len(), twin.state().ops.len()), (vec![OutboxAnswer::Err(Fault::Ended(crate::ffi::EndCause::Closed))], 1, 0));
}

#[test]
fn out_queued_persist_attempt() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::Online(Online(false)));
    twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
    assert_eq!(twin.stage(id), Some(St::Queued));
    let effects = twin.post(OutboxInput::Online(Online(true)));
    assert_eq!((twin.stage(id), kinds(&effects), sends(&effects)), (Some(St::PersistingAttempt), vec![CommitKind::Attempt], 0));
    assert!(has_op!(twin, StoreOp::Attempt { op_id, attempts: 2, cycle_id: 1, .. } if *op_id == id), "{:?}", twin.store_ops);
}

#[test]
fn out_queued_exhausted() {
    for cause in [None, Some(64)] {
    let mut queued = Twin::new();
    let pending = sending(&mut queued, Policy::Rollback, false);
    for _ in 0..38 {
        queued.post(OutboxInput::TransportError { op_id: pending, temporary: true });
        tick(&mut queued, pending);
        queued.committed(pending);
    }
    if let Some(limit) = cause {
        let req_id = queued.state().ops[&pending].request_id;
        queued.post(OutboxInput::Answered { req_id, outcome: crate::owners::session::AuthorOutcome::Error { class: crate::owners::contract::ErrorClass::Network, error: crate::owners::session::SessionError::ResponseTooLarge { limit }, pending_retained: true } });
    } else { queued.post(OutboxInput::TransportError { op_id: pending, temporary: true }); }
    tick(&mut queued, pending);
    queued.post(OutboxInput::Online(Online(false)));
    queued.committed(pending);
    assert_eq!((queued.stage(pending), queued.state().ops[&pending].attempts), (Some(St::Queued), 40));
    let resumed = queued.post(OutboxInput::Online(Online(true)));
    assert_eq!((queued.stage(pending), kinds(&resumed), sends(&resumed)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark], 0));
    assert_exhausted_mark(&queued, pending, cause.is_some());
    }
    let mut twin = Twin::new();
    let id = exhausted(&mut twin, Policy::Rollback);
    let effects = twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
    assert_eq!((twin.stage(id), kinds(&effects), sends(&effects)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark], 0));
}

#[test]
fn out_sending_response_temporary() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let effects = twin.post(OutboxInput::Response { op_id: id, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: Some(true) });
    assert_eq!((twin.stage(id), timers(&effects), sends(&effects)), (Some(St::Sending), 1, 0), "as a temporary TransportError: the backoff timer");
}

#[test]
fn out_sending_response_errors() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let effects = twin.post(OutboxInput::Response { op_id: id, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: Some(false) });
    assert_eq!((twin.stage(id), kinds(&effects)), (Some(St::RollingBack(RWhy::Rejected)), vec![CommitKind::Rollback]), "as a permanent TransportError by on_failure");
    let mut keeping = Twin::new();
    let kept = sending(&mut keeping, Policy::Keep, false);
    let effects = keeping.post(OutboxInput::Response { op_id: kept, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: Some(false) });
    assert_eq!((keeping.stage(kept), kinds(&effects)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark]));
}

#[test]
fn out_persisting_attempt_committed() {
    let mut twin = Twin::new();
    let id = persisting(&mut twin, false);
    let first = twin.first_pair[&id];
    let effects = twin.committed(id);
    let pairs: Vec<_> = effects.iter().filter_map(|effect| match effect { OutboxEffect::Send { request_id, sequence, .. } => Some((*request_id, *sequence)), _ => None }).collect();
    assert_eq!((twin.stage(id), pairs, twin.state().ops[&id].attempts), (Some(St::Sending), vec![first], 2));
}

#[test]
fn out_persisting_attempt_committed_offline() {
    let mut twin = Twin::new();
    let id = persisting(&mut twin, false);
    twin.post(OutboxInput::Online(Online(false)));
    let effects = twin.committed(id);
    assert_eq!((twin.stage(id), sends(&effects)), (Some(St::Queued), 0));
    assert_eq!(kinds(&effects), vec![CommitKind::Requeue]);
    assert!(twin.store_ops.contains(&StoreOp::Requeue { op_id: id }), "{:?}", twin.store_ops);
}

#[test]
fn out_server_removed_row_all_stages() {
    for stage in [St::WaitingDeclaration, St::Submitting, St::Queued, St::Sending, St::PersistingAttempt, St::Landing, St::RollingBack(RWhy::Rejected), St::MarkingFailed, St::MarkingRetry, St::Failed] {
        let ctx = if stage == St::Queued { offline() } else { ready() };
        let (mut twin, id) = reach(stage, &ctx);
        if stage == St::MarkingRetry {
            let mut receipt = twin.receipt(id, EPOCH);
            receipt.cycle_id = twin.state().ops[&id].cycle_id;
            twin.post(OutboxInput::Outcome { receipt, reply: WAITER , discard_on_failure: false});
        }
        let input = OutboxInput::RowRemoved { model: "Moment".into(), row_id: "r".into(), revision: 20 };
        let before = twin.post(input.clone());
        assert!(!answers(&before, WAITER).contains(&OutboxAnswer::Outcome(OutboxOutcome::Discarded)), "{stage:?}");
        if matches!(stage, St::Submitting | St::PersistingAttempt | St::Landing | St::RollingBack(_) | St::MarkingFailed | St::MarkingRetry) {
            twin.committed(id);
        }
        assert!(has_op!(twin, StoreOp::RemoveIntent { .. }) && has_op!(twin, StoreOp::ReleasePendingOwner { .. }) && !has_op!(twin, StoreOp::RollBack { .. }), "{stage:?}: {:?}", twin.store_ops);
        let request_id = twin.state().ops[&id].request_id;
        assert!(twin.store_ops.contains(&StoreOp::RemoveIntent { op_id: id }) && twin.store_ops.contains(&StoreOp::ReleasePendingOwner { request_id }) && twin.store_ops.contains(&StoreOp::RemoveAnswer { request_id }), "{stage:?}: {:?}", twin.store_ops);
        twin.post(input);
        let effects = twin.committed(id);
        let answers = if stage == St::Failed {
            let receipt = twin.receipt(id, EPOCH);
            answers(&twin.post(OutboxInput::Outcome { receipt, reply: WAITER , discard_on_failure: false}), WAITER)
        } else { answers(&effects, WAITER) };
        assert_eq!(answers, [OutboxAnswer::Outcome(OutboxOutcome::Discarded)], "{stage:?}");
        assert_eq!(twin.stage(id), Some(St::Done));
    }
}

/// P-OUT-60: the Store refuses the removal batch of an operation whose row the server removed as invalid data: the batch is not written again, the operation ends `Discarded`.
#[test]
fn out_removed_row_invalid_is_not_retried() {
    let mut wrong = Vec::new();
    for stage in [St::Queued, St::Sending, St::Failed] {
        let ctx = if stage == St::Queued { offline() } else { ready() };
        let (mut twin, id) = reach(stage, &ctx);
        twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "r".into(), revision: 20 });
        let effects = refuse(&mut twin, id, RejectReason::Invalid(String::new()));
        let answered = if stage == St::Failed {
            let receipt = twin.receipt(id, EPOCH);
            answers(&twin.post(OutboxInput::Outcome { receipt, reply: WAITER, discard_on_failure: false }), WAITER)
        } else { answers(&effects, WAITER) };
        let got = (kinds(&effects), timers(&effects), answered, twin.stage(id));
        let want = (Vec::new(), 0, vec![OutboxAnswer::Outcome(OutboxOutcome::Discarded)], Some(St::Done));
        if got != want {
            wrong.push(format!("{stage:?}: (batches, timers, answers, stage) = {got:?}, the spec asks {want:?}"));
        }
    }
    assert!(wrong.is_empty(), "{} wrong cases:\n{}", wrong.len(), wrong.join("\n"));
}

/// P-DATA-101: a submit the Store refuses as invalid answers its caller and the later wait of its outcome with the text of the Store.
#[test]
fn out_submit_invalid_carries_the_store_text() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    let receipt = twin.receipt(id, EPOCH);
    let effects = refuse(&mut twin, id, RejectReason::Invalid("bad row".into()));
    assert_eq!(answers(&effects, SUBMIT_REPLY), vec![OutboxAnswer::Err(Fault::Rejected(RejectReason::Invalid("bad row".into())))]);
    let waited = twin.post(OutboxInput::Outcome { receipt, reply: WAITER, discard_on_failure: false });
    assert_eq!(answers(&waited, WAITER), vec![OutboxAnswer::Outcome(OutboxOutcome::Ended(crate::ffi::EndCause::Invalid { detail: "bad row".into() }))]);
}

#[test]
fn out_persisting_attempt_invalid() {
    let mut twin = Twin::new();
    let id = persisting(&mut twin, false);
    let effects = refuse(&mut twin, id, RejectReason::Invalid("bad row".into()));
    assert_eq!((twin.stage(id), kinds(&effects)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark]));
    assert_eq!(twin.state().ops[&id].error.as_deref(), Some("Internal(Invalid): bad row"));
    assert!(twin.store_ops.contains(&StoreOp::MarkError { op_id: id, error: Some("Internal(Invalid): bad row".into()) }), "{:?}", twin.store_ops);
}

#[test]
fn out_landing_committed_swap() {
    let mut twin = Twin::new();
    let created = sending(&mut twin, Policy::Rollback, false);
    let follower = submit_op(&mut twin, NewOp { body: "{\"row\":\"r\"}".into(), ..model_op("r", Policy::Rollback, false) }, 23);
    twin.committed(follower);
    twin.waiter(created);
    twin.post(OutboxInput::Response { op_id: created, rows: server_rows(), swaps: vec![("r".into(), "R".into())], stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert!(has_op!(twin, StoreOp::Swap { temp, real, .. } if temp == "r" && real == "R"), "the landing batch swaps the row: {:?}", twin.store_ops);
    let effects = twin.committed(created);
    assert!(answers(&effects, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Confirmed { .. }))), "{effects:?}");
    let next = &twin.state().ops[&follower];
    assert_eq!((next.op.row_id.as_str(), next.op.body.contains("\"R\"")), ("R", true), "the open operations of the row address the real row");
}

#[test]
fn out_landing_committed_result() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.waiter(id);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: Some(r#"{"copies":3}"#.into()), missing: Vec::new(), errors: None });
    let effects = twin.committed(id);
    assert!(answers(&effects, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Confirmed { result: Some(text), .. }) if text == r#"{"copies":3}"#)), "{effects:?}");
    assert!(matches!(&twin.state().ops[&id].outcome, Some(OutboxOutcome::Confirmed { result: Some(text), .. }) if text == r#"{"copies":3}"#), "a second read of the outcome gives the same result");
}

#[test]
fn out_landing_committed_stale() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.waiter(id);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: vec![("Moment".into(), "history".into())], result: None, missing: Vec::new(), errors: None });
    assert!(has_op!(twin, StoreOp::StaleFamilies { families } if families.contains(&("Moment".to_string(), "history".to_string()))), "the landing batch marks the windows stale: {:?}", twin.store_ops);
    let effects = twin.committed(id);
    assert!(answers(&effects, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Confirmed { .. }))), "{effects:?}");
}

#[test]
fn out_rollback_invalid() {
    let mut twin = Twin::new();
    let id = failed(&mut twin);
    let started = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: DISCARD_REPLY });
    assert_eq!((twin.stage(id), kinds(&started)), (Some(St::RollingBack(RWhy::Discarded)), vec![CommitKind::Rollback]));
    let effects = refuse(&mut twin, id, RejectReason::Invalid("bad row".into()));
    assert_eq!((twin.stage(id), kinds(&effects)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark]));
    assert_eq!(answers(&effects, DISCARD_REPLY), vec![OutboxAnswer::Err(Fault::Rejected(RejectReason::Invalid("bad row".into())))], "the waiting Discard gets Err(Invalid) with the text of the error");
    assert!(twin.store_ops.contains(&StoreOp::MarkError { op_id: id, error: Some("Internal(Invalid): bad row".into()) }), "{:?}", twin.store_ops);
}

fn discarded(effects: &[OutboxEffect]) -> usize { answers(effects, WAITER).iter().filter(|answer| **answer == OutboxAnswer::Outcome(OutboxOutcome::Discarded)).count() }

/// The `RollBack` and `RemoveIntent` ops of one Store batch, sorted: `("RollBack", op_id)`.
fn take_back_ops(batch: &str) -> Vec<(&'static str, u64)> {
    let mut ops: Vec<(&'static str, u64)> = ["RollBack", "RemoveIntent"].into_iter().flat_map(|name| {
        let marker = format!("{name} {{ op_id: ");
        batch.match_indices(&marker).filter_map(|(at, _)| crate::spec::support::number_after(&batch[at..], "op_id: ")).map(move |id| (name, id)).collect::<Vec<_>>()
    }).collect();
    ops.sort();
    ops
}

fn chain(ids: &[OpId]) -> Vec<(&'static str, u64)> {
    let mut ops: Vec<(&'static str, u64)> = ids.iter().flat_map(|id| [("RollBack", *id), ("RemoveIntent", *id)]).collect();
    ops.sort();
    ops
}

#[test]
fn out_destroy_discards_chain() {
    let mut twin = Twin::new();
    let created = queued_behind_blocker(&mut twin, NewOp { action: "create".into(), ..model_op("r", Policy::Rollback, false) });
    let later = submit_op(&mut twin, model_op("r", Policy::Rollback, false), 21);
    twin.committed(later);
    let after = submit_op(&mut twin, model_op("r", Policy::Rollback, false), 23);
    twin.committed(after);
    twin.post(OutboxInput::ReserveOp { epoch: EPOCH, target: crate::owners::write::outbox::ReserveTarget::Transfer(1) });
    let moved = after + 1;
    twin.post(OutboxInput::SubmitReserved { epoch: EPOCH, op_id: moved, op: model_op("r", Policy::Rollback, true), created_at: 0 });
    assert_eq!(twin.stage(moved), Some(St::Queued), "the intent of Transfer waits behind the create");
    twin.waiter(created);
    twin.waiter(later);
    twin.waiter(after);
    let effects = twin.post(OutboxInput::Submit { epoch: EPOCH, op: NewOp { action: "destroy".into(), ..model_op("r", Policy::Rollback, false) }, reply: 22 });
    assert_eq!(kinds(&effects), vec![CommitKind::Rollback], "one batch takes back the create and every later operation of the row");
    let [(rollback, batch)] = twin.stores.clone().try_into().expect("one Store batch");
    assert_eq!(take_back_ops(&batch), chain(&[created, later, after, moved]), "{batch}");
    let destroy = *twin.state().ops.keys().max().expect("the destroy");
    let done = twin.commit_batch(rollback);
    assert!(matches!(answers(&done, 22).as_slice(), [OutboxAnswer::Receipt(_)]), "{done:?}");
    assert_eq!((discarded(&done), transfer_outcomes(&done), twin.stage(later), twin.stage(after), twin.stage(moved), sends(&done)), (3, vec![OutboxOutcome::Discarded], Some(St::Done), Some(St::Done), Some(St::Done), 0), "out_destroy_unsent_create: every operation of the chain is Done");
    assert_eq!(twin.state().ops[&destroy].outcome, Some(OutboxOutcome::Confirmed { revision: twin.revision, result: None, row_id: None }));
    let online = twin.post(OutboxInput::Online(Online(true)));
    assert_eq!((discarded(&online), transfer_outcomes(&online)), (0, Vec::new()), "each operation is discarded once");
}

#[test]
fn out_destroy_discards_chain_with_a_batch_in_flight() {
    for committed in [true, false] {
        let mut twin = Twin::new();
        let created = queued_behind_blocker(&mut twin, NewOp { action: "create".into(), ..model_op("r", Policy::Rollback, false) });
        let later = submit_op(&mut twin, model_op("r", Policy::Rollback, false), 21);
        let [(later_batch, _)] = twin.stores.clone().try_into().expect("the Submit batch of the later operation");
        twin.waiter(created);
        let rollback = twin.post(OutboxInput::Submit { epoch: EPOCH, op: NewOp { action: "destroy".into(), ..model_op("r", Policy::Rollback, false) }, reply: 22 });
        assert_eq!(kinds(&rollback), vec![CommitKind::Rollback], "committed={committed}");
        let [(rollback_batch, batch)] = twin.stores.clone().try_into().expect("one Store batch");
        assert_eq!(take_back_ops(&batch), chain(&[created, later]), "committed={committed}: {batch}");
        let reply = if committed { twin.commit_batch(later_batch) } else { twin.reject_batch(later_batch, RReason::Storage) };
        let submit = answers(&reply, 21);
        assert!(if committed { matches!(submit.as_slice(), [OutboxAnswer::Receipt(_)]) } else { matches!(submit.as_slice(), [OutboxAnswer::Err(Fault::Rejected(RejectReason::Storage(detail)))] if detail == "disk") }, "committed={committed}: {reply:?}");
        if committed { twin.waiter(later); }
        let done = twin.commit_batch(rollback_batch);
        assert_eq!((discarded(&done), twin.stage(later), sends(&done)), (if committed { 2 } else { 1 }, if committed { Some(St::Done) } else { None }, 0), "committed={committed}: a rejected submit leaves no operation: {done:?}");
        let rollbacks = done.iter().chain(reply.iter()).filter(|effect| kinds(std::slice::from_ref(effect)) == vec![CommitKind::Rollback]).count();
        assert_eq!(rollbacks, 0, "committed={committed}: the chain is taken back once");
    }

    let mut twin = Twin::new();
    let created = queued_behind_blocker(&mut twin, NewOp { action: "create".into(), ..model_op("r", Policy::Rollback, false) });
    twin.waiter(created);
    let rollback = twin.post(OutboxInput::Submit { epoch: EPOCH, op: NewOp { action: "destroy".into(), ..model_op("r", Policy::Rollback, false) }, reply: 22 });
    assert_eq!(kinds(&rollback), vec![CommitKind::Rollback], "the destroy takes the create back");
    let [(rollback_batch, _)] = twin.stores.clone().try_into().expect("one Store batch");
    let refused = twin.post(OutboxInput::Rejected(Rejected { batch: rollback_batch, reason: RejectReason::Invalid("bad row".into()) }));
    assert_eq!(answers(&refused, 22), vec![OutboxAnswer::Err(Fault::Rejected(RejectReason::Invalid("bad row".into())))], "Rejected(Invalid) of the in-flight chain batch answers the waiting destroy the refusal reason with its text: {refused:?}");
}

#[test]
fn out_closing_after_the_applied_answer_changes_nothing() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, true);
    twin.waiter(id);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let landed = twin.committed(id);
    assert!(answers(&landed, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Confirmed { .. }))), "the answer that arrived before Closing lands Confirmed first: {landed:?}");
    let after = input_of(&mut twin, id, &ready(), In::Closing);
    assert_eq!((twin.stage(id), answers(&after, WAITER), transfer_outcomes(&after)), (Some(St::Done), Vec::new(), Vec::new()), "Closing after the applied answer changes nothing: {after:?}");
}

fn response_of(op_id: OpId) -> OutboxInput { OutboxInput::Response { op_id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None } }

/// An operation of the row `r` waiting for the answer of its own Store batch, and the kind of that batch.
fn in_batch_stage(stage: St, transfer: bool) -> (Twin, OpId, CommitKind) {
    let mut twin = Twin::new();
    match stage {
        St::Submitting => { let id = twin.submit("r", Policy::Rollback, transfer); (twin, id, CommitKind::Intent) }
        St::PersistingAttempt => { let id = persisting(&mut twin, transfer); (twin, id, CommitKind::Attempt) }
        St::Landing => { let id = sending(&mut twin, Policy::Rollback, transfer); twin.post(response_of(id)); (twin, id, CommitKind::Land) }
        St::RollingBack(_) => { let id = sending(&mut twin, Policy::Rollback, transfer); twin.post(OutboxInput::TransportError { op_id: id, temporary: false }); (twin, id, CommitKind::Rollback) }
        St::MarkingFailed => { let id = marking_failed(&mut twin, transfer); (twin, id, CommitKind::ErrorMark) }
        St::MarkingRetry => { let id = marking_retry(&mut twin); (twin, id, CommitKind::ClearMark) }
        other => panic!("no batch in {other:?}"),
    }
}

#[test]
fn out_storage_refusal_sets_a_timer_and_the_timer_writes_the_same_batch_again() {
    for stage in [St::PersistingAttempt, St::MarkingFailed] {
        let (mut twin, id, kind) = in_batch_stage(stage, false);
        let original = twin.store_ops.clone();
        let refused = refuse(&mut twin, id, RejectReason::Storage("disk".into()));
        assert_eq!((timers(&refused), refused.len()), (1, 1), "{stage:?}");
        let again = tick(&mut twin, id);
        assert_eq!((kinds(&again), twin.store_ops.clone()), (vec![kind], original), "{stage:?}");
    }
}

#[test]
fn out_batch_refused_without_a_session_ends_the_operation_for_the_waiter_and_transfer() {
    for stage in [St::PersistingAttempt, St::Landing, St::RollingBack(RWhy::Rejected), St::MarkingFailed] {
        for (reason, cause) in [(RejectReason::NoSession, crate::ffi::EndCause::SessionEnded), (RejectReason::Closed, crate::ffi::EndCause::Closed)] {
            let (mut twin, id, _) = in_batch_stage(stage, true);
            let effects = refuse(&mut twin, id, reason.clone());
            assert_eq!(transfer_outcomes(&effects), vec![OutboxOutcome::Ended(cause)], "{stage:?} {reason:?}");
        }
    }
}

#[test]
fn out_discard_of_an_operation_in_flight_is_refused() {
    let discard = || OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: DISCARD_REPLY };
    for stage in [St::Landing, St::Sending] {
        let mut twin = if stage == St::Sending { let mut twin = Twin::new(); sending(&mut twin, Policy::Rollback, false); twin } else { in_batch_stage(stage, false).0 };
        let effects = twin.post(discard());
        assert_eq!((answers(&effects, DISCARD_REPLY), effects.len()), (vec![OutboxAnswer::Err(Fault::InFlight)], 1), "{stage:?}");
    }
    for stage in [St::RollingBack(RWhy::Rejected), St::MarkingFailed, St::MarkingRetry] {
        let mut twin = in_batch_stage(stage, false).0;
        let effects = retry(&mut twin);
        assert_eq!((answers(&effects, RETRY_REPLY), effects.len()), (vec![OutboxAnswer::Err(Fault::InFlight)], 1), "{stage:?}");
    }
}

#[test]
fn out_retry_of_an_open_operation_answers_its_receipt_and_changes_nothing() {
    let mut sent = Twin::new();
    let sent_id = sending(&mut sent, Policy::Rollback, false);
    let mut queued = Twin::new();
    queued.post(OutboxInput::Online(Online(false)));
    let queued_id = queued.submit("r", Policy::Rollback, false);
    queued.committed(queued_id);
    let (persisting_twin, persisting_id, _) = in_batch_stage(St::PersistingAttempt, false);
    for (stage, mut twin, id) in [(St::Sending, sent, sent_id), (St::Queued, queued, queued_id), (St::PersistingAttempt, persisting_twin, persisting_id)] {
        let effects = retry(&mut twin);
        assert!(matches!(answers(&effects, RETRY_REPLY).as_slice(), [OutboxAnswer::Receipt(receipt)] if receipt.op_id == id) && effects.len() == 1, "{stage:?}: {effects:?}");
    }
}

#[test]
fn out_inputs_that_do_not_belong_to_the_stage_change_nothing() {
    let foreign = |id: OpId| vec![response_of(id), OutboxInput::TransportError { op_id: id, temporary: true }, OutboxInput::Dropped { op_id: id }];
    for stage in [St::Submitting, St::Landing, St::RollingBack(RWhy::Rejected), St::MarkingFailed, St::MarkingRetry] {
        let (mut twin, id, _) = in_batch_stage(stage, false);
        unchanged_by(&mut twin, [vec![OutboxInput::Online(Online(true))], foreign(id)].concat());
    }
    for (stage, cycle_id) in [(St::PersistingAttempt, 99), (St::MarkingRetry, 2)] {
        let (mut twin, id, _) = in_batch_stage(stage, false);
        unchanged_by(&mut twin, [vec![OutboxInput::Online(Online(true)), OutboxInput::RetryDue { op_id: id, cycle_id }], foreign(id)].concat());
    }

    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let strays = foreign(id + 100);
    unchanged_by(&mut twin, [vec![OutboxInput::Online(Online(false)), OutboxInput::Online(Online(true)), OutboxInput::RetryDue { op_id: id, cycle_id: 1 }], strays].concat());

    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    let batch = twin.batches[&id];
    unchanged_by(&mut twin, [foreign(id), vec![OutboxInput::RetryDue { op_id: id, cycle_id: 1 }, OutboxInput::Rejected(Rejected { batch, reason: RejectReason::Invalid(String::new()) })]].concat());

    let mut twin = Twin::new();
    queued_behind_blocker(&mut twin, model_op("r", Policy::Rollback, false));
    unchanged_by(&mut twin, vec![OutboxInput::Online(Online(true)), OutboxInput::Online(Online(false))]);

    let mut twin = Twin::new();
    failed(&mut twin);
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    unchanged_by(&mut twin, vec![OutboxInput::Online(Online(true))]);
}
