//! Owner-level tests of the `outbox` machine: the cells of the marks `MarkingFailed` and `MarkingRetry` and the receipts of the cycles.

use super::outbox_cells_tests::*;
use super::outbox_harness::*;
use super::outbox_harness::{Policy, St};
use crate::owners::contract::{Online, RejectReason};
use crate::owners::store::StoreOp;
use crate::owners::write::outbox::{CommitKind, Fault, NewOp, OutboxAnswer, OutboxInput, OutboxOutcome, Receipt};

#[test]
fn out_marking_failed_invalid() {
    let mut twin = Twin::new();
    let id = marking_failed(&mut twin, true);
    let effects = refuse(&mut twin, id, RejectReason::Invalid(String::new()));
    assert!(matches!(transfer_outcomes(&effects).as_slice(), [OutboxOutcome::Failed { .. }]), "{effects:?}");
}

#[test]
fn out_retry_waits_for_the_failure_mark_and_joins_the_same_new_cycle() {
    let mut twin = Twin::new();
    let id = marking_failed(&mut twin, false);
    let effects = retry(&mut twin);
    assert!(answers(&effects, RETRY_REPLY).is_empty());
    let effects = twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::MarkingRetry));
    assert!(answers(&effects, RETRY_REPLY).is_empty());
    let joined = RETRY_REPLY + 1;
    let effects = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply: joined });
    assert!(answers(&effects, joined).is_empty());
    let effects = twin.committed(id);
    for reply in [RETRY_REPLY, joined] {
        assert!(matches!(answers(&effects, reply).as_slice(), [OutboxAnswer::Receipt(Receipt { cycle_id: 2, .. })]));
    }
    assert_eq!(sends(&effects), 1);
}

#[test]
fn out_failed_retry_restore_failed() {
    let mut twin = Twin::bare();
    declare(&mut twin);
    let stored = crate::owners::write::outbox::StoredOp { answered: None, op_id: 7, op: model_op("r", Policy::Rollback, false), created_at: 0, error_mark: false, pending: None, attempts: 1, cycle_id: 1, cause: None, restore_failed: Some("layout".into()), restore_element: None, removed_rows: Vec::new() };
    twin.post(OutboxInput::Restored { ops: vec![stored], pending_lost: false });
    twin.post(OutboxInput::RestoreReady { epoch: EPOCH });
    let effects = retry(&mut twin);
    assert_eq!((twin.stage(7), answers(&effects, RETRY_REPLY)), (Some(St::Failed), vec![OutboxAnswer::Err(Fault::RestoreFailed)]));
    assert!(kinds(&effects).is_empty(), "nothing is written: {effects:?}");
}

#[test]
fn out_marking_retry_committed() {
    let mut twin = Twin::new();
    let id = marking_retry(&mut twin);
    assert_eq!(twin.state().ops[&id].cycle_id, 2);
    let effects = twin.committed(id);
    let receipt = answers(&effects, RETRY_REPLY);
    assert!(matches!(receipt.as_slice(), [OutboxAnswer::Receipt(Receipt { cycle_id: 2, .. })]), "{effects:?}");
    assert_eq!(sends(&effects), 1);
}

#[test]
fn out_marking_retry_committed_queued() {
    let mut twin = Twin::new();
    let id = marking_retry(&mut twin);
    twin.post(OutboxInput::Online(Online(false)));
    let effects = twin.committed(id);
    assert_eq!((sends(&effects), answers(&effects, RETRY_REPLY).len()), (0, 1));
    assert_eq!(twin.batch_ops(), vec![StoreOp::Requeue { op_id: id }]);
}

#[test]
fn out_marking_retry_refused_batch_answers_the_retry_by_reason() {
    for reason in [RejectReason::Storage("disk".into()), RejectReason::NoSession, RejectReason::Closed, RejectReason::Invalid("bad row".into())] {
        let mut twin = Twin::new();
        let id = marking_retry(&mut twin);
        let effects = refuse(&mut twin, id, reason.clone());
        assert_eq!(answers(&effects, RETRY_REPLY), vec![OutboxAnswer::Err(crate::spec::support::refusal_of(reason.clone()))], "{reason:?}");
    }
}

#[test]
fn out_outcome_old_cycle() {
    let mut twin = Twin::new();
    let id = failed(&mut twin);
    let old = Receipt { epoch: EPOCH, op_id: id, cycle_id: 1, revision: 0, optimistic: None };
    retry(&mut twin);
    twin.committed(id);
    let effects = twin.post(OutboxInput::Outcome { receipt: old, reply: 804 , discard_on_failure: false});
    assert!(matches!(answers(&effects, 804).as_slice(), [OutboxAnswer::Outcome(OutboxOutcome::Failed { .. })]), "the outcome of the earlier cycle is immutable: {effects:?}");
}

#[test]
fn out_sending_response_ignore() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.waiter(id);
    twin.post(response_with(id, Vec::new(), Vec::new()));
    let request_id = twin.state().ops[&id].request_id;
    assert_eq!(twin.batch_ops(), vec![StoreOp::RemoveIntent { op_id: id }, StoreOp::RemoveAnswer { request_id }], "the intent goes, the optimistic fields stay, no row of the answer");
    let effects = twin.committed(id);
    assert!(answers(&effects, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Confirmed { .. }))), "{effects:?}");
}

#[test]
fn out_sending_refused_without_effects() {
    let mut twin = Twin::new();
    let op = NewOp { effects: Default::default(), ..model_op("r", Policy::Rollback, false) };
    let id = submit_op(&mut twin, op, 31);
    twin.committed(id);
    twin.waiter(id);
    twin.post(response_errors(id, false));
    let request_id = twin.state().ops[&id].request_id;
    assert_eq!(twin.stores.len(), 1);
    assert_eq!(twin.batch_ops(), vec![StoreOp::RollBack { op_id: id }, StoreOp::RemoveIntent { op_id: id }, StoreOp::RemoveAnswer { request_id }]);
    let effects = twin.committed(id);
    assert!(answers(&effects, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Rejected { .. }))), "the rollback of nothing still gives the outcome: {effects:?}");
    let effects = retry(&mut twin);
    assert!(answers(&effects, RETRY_REPLY).iter().any(|answer| matches!(answer, OutboxAnswer::Err(Fault::NotFound))), "retry after Rejected: {effects:?}");
}

#[test]
fn out_landing_committed_root_row() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.waiter(id);
    let other = vec![("Other".to_string(), vec![crate::owners::store::RowValue { row_id: "z".into(), values: "{\"id\":\"z\"}".into() }])];
    twin.post(response_with(id, other, Vec::new()));
    let effects = twin.committed(id);
    assert!(answers(&effects, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Confirmed { .. }))) && twin.stage(id).is_none_or(|stage| stage == St::Done), "{effects:?}");
    assert!(!has_op!(twin, StoreOp::Swap { .. }), "no swap for a root of another model");
}

/// A request above the limit of the transport ends the operation with a typed failure: the kind and both numbers reach the outcome, not a general transport text.
#[test]
fn out_sending_answered_too_large() {
    use crate::owners::contract::ErrorClass;
    use crate::owners::session::{AuthorOutcome, SessionError};
    use crate::value::sync::FailureKind;
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Keep, false);
    twin.waiter(id);
    let request_id = twin.state().ops[&id].request_id;
    twin.post(OutboxInput::Answered { req_id: request_id, outcome: AuthorOutcome::Error { class: ErrorClass::Permanent, error: SessionError::TooLarge { size: 300_000, limit: 262_144 }, pending_retained: false } });
    let effects = twin.committed(id);
    let failed = answers(&effects, WAITER).into_iter().find_map(|answer| match answer { OutboxAnswer::Outcome(OutboxOutcome::Failed { error }) => Some(error), _ => None }).expect("a failed outcome");
    let failure = crate::owners::write::failure::parse(&failed);
    assert_eq!(failure.kind, FailureKind::RequestTooLarge { size: 300_000, limit: 262_144 });
}

#[test]
fn out_mark_failed_rejected_no_outcome() {
    let mut twin = Twin::new();
    let id = marking_failed(&mut twin, true);
    twin.waiter(id);
    let rejected = refuse(&mut twin, id, RejectReason::Storage("disk".into()));
    assert_eq!((twin.stage(id), answers(&rejected, WAITER), transfer_outcomes(&rejected), timers(&rejected)), (Some(St::MarkingFailed), Vec::new(), Vec::new(), 1), "Rejected MarkFailed delivers no Failed, the op stays MarkingFailed, a retry timer is set");
    let reticked = tick(&mut twin, id);
    assert_eq!((twin.stage(id), kinds(&reticked)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark]), "the next owner input re-writes MarkFailed");
    let done = twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Failed));
    assert!(answers(&done, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Failed { .. }))), "{done:?}");
    assert!(matches!(transfer_outcomes(&done).as_slice(), [OutboxOutcome::Failed { .. }]), "Failed reaches the waiter and Transfer only after Committed: {done:?}");
}
