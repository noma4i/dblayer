//! Owner-level tests of the `outbox` machine: the cells of the marks `MarkingFailed` and `MarkingRetry` and the receipts of the cycles.

use super::outbox_cells_tests::*;
use super::outbox_harness::*;
use super::ref_outbox::{Policy, Reason as RReason, St};
use crate::owners::contract::{ModelsReset, Online, RejectReason};
use crate::owners::write::outbox::{CommitKind, Fault, NewOp, OutboxAnswer, OutboxInput, OutboxOutcome, Receipt};

#[test]
fn out_marking_failed_committed() {
    let mut twin = Twin::new();
    let id = marking_failed(&mut twin, true);
    twin.waiter(id);
    let effects = twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Failed));
    assert!(answers(&effects, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Failed { .. }))), "{effects:?}");
    assert!(matches!(transfer_outcomes(&effects).as_slice(), [OutboxOutcome::Failed { .. }]), "{effects:?}");
}

#[test]
fn out_marking_failed_storage() {
    let mut twin = Twin::new();
    let id = marking_failed(&mut twin, false);
    let effects = refuse(&mut twin, id, RejectReason::Storage("disk".into()));
    assert_eq!((twin.stage(id), timers(&effects)), (Some(St::MarkingFailed), 1));
}

#[test]
fn out_marking_failed_no_session() {
    let mut twin = Twin::new();
    let id = marking_failed(&mut twin, true);
    let effects = refuse(&mut twin, id, RejectReason::NoSession);
    assert_eq!((twin.stage(id), transfer_outcomes(&effects)), (Some(St::Done), vec![OutboxOutcome::Interrupted]));
}

#[test]
fn out_marking_failed_stale_generation() {
    let mut twin = Twin::new();
    let id = marking_failed(&mut twin, false);
    let effects = twin.rejected(id, RReason::StaleGeneration);
    assert!(matches!(effects.as_slice(), [OutboxEffect::Store { kind: CommitKind::ErrorMark, generation, .. }] if *generation == NEW_GENERATION), "{effects:?}");
}

#[test]
fn out_marking_failed_invalid() {
    let mut twin = Twin::new();
    let id = marking_failed(&mut twin, true);
    let effects = refuse(&mut twin, id, RejectReason::Invalid);
    assert_eq!(twin.stage(id), Some(St::Failed), "the mark is only in memory");
    assert!(matches!(transfer_outcomes(&effects).as_slice(), [OutboxOutcome::Failed { .. }]), "{effects:?}");
}

#[test]
fn out_marking_failed_tick() {
    let mut twin = Twin::new();
    let id = marking_failed(&mut twin, false);
    let effects = tick(&mut twin, id);
    assert_eq!((twin.stage(id), kinds(&effects)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark]));
}

#[test]
fn out_marking_failed_refuses() {
    let mut twin = Twin::new();
    let id = marking_failed(&mut twin, false);
    let effects = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), reply: DISCARD_REPLY });
    assert_eq!(answers(&effects, DISCARD_REPLY), vec![OutboxAnswer::Err(Fault::InFlight)]);
    let effects = retry(&mut twin);
    assert_eq!((twin.stage(id), answers(&effects, RETRY_REPLY)), (Some(St::MarkingFailed), vec![OutboxAnswer::Err(Fault::InFlight)]));
}

#[test]
fn out_marking_failed_other() {
    let mut twin = Twin::new();
    let id = marking_failed(&mut twin, false);
    let reset = OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 2, models: vec!["Chat".into()] });
    unchanged_by(&mut twin, id, St::MarkingFailed, vec![
        OutboxInput::Online(Online(true)),
        reset,
        OutboxInput::Response { op_id: id, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None },
        OutboxInput::TransportError { op_id: id, temporary: true },
        OutboxInput::Dropped { op_id: id },
    ]);
}

#[test]
fn out_failed_retry_restore_failed() {
    let mut twin = Twin::bare();
    twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into())] });
    let stored = crate::owners::write::outbox::StoredOp { answered: None, op_id: 7, op: model_op("r", Policy::Rollback, false), error_mark: false, pending: None, attempts: 1, cycle_id: 1, restore_failed: Some("layout".into()), restore_element: None };
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
    assert_eq!((twin.stage(id), sends(&effects)), (Some(St::Sending), 1));
}

#[test]
fn out_marking_retry_committed_queued() {
    let mut twin = Twin::new();
    let id = marking_retry(&mut twin);
    twin.post(OutboxInput::Online(Online(false)));
    let effects = twin.committed(id);
    assert_eq!((twin.stage(id), sends(&effects), answers(&effects, RETRY_REPLY).len()), (Some(St::Queued), 0, 1));
}

#[test]
fn out_marking_retry_storage() {
    let mut twin = Twin::new();
    let id = marking_retry(&mut twin);
    let effects = refuse(&mut twin, id, RejectReason::Storage("disk".into()));
    assert_eq!((twin.stage(id), answers(&effects, RETRY_REPLY)), (Some(St::Failed), vec![OutboxAnswer::Err(Fault::Rejected(RejectReason::Storage("disk".into())))]));
}

#[test]
fn out_marking_retry_no_session() {
    let mut twin = Twin::new();
    let id = marking_retry(&mut twin);
    let effects = refuse(&mut twin, id, RejectReason::NoSession);
    assert_eq!((twin.stage(id), answers(&effects, RETRY_REPLY)), (Some(St::Done), vec![OutboxAnswer::Err(Fault::Interrupted)]));
}

#[test]
fn out_marking_retry_stale_generation() {
    let mut twin = Twin::new();
    let id = marking_retry(&mut twin);
    let effects = twin.rejected(id, RReason::StaleGeneration);
    assert!(matches!(effects.as_slice(), [OutboxEffect::Store { kind: CommitKind::ClearMark, generation, .. }] if *generation == NEW_GENERATION), "{effects:?}");
    assert_eq!(twin.stage(id), Some(St::MarkingRetry));
}

#[test]
fn out_marking_retry_invalid() {
    let mut twin = Twin::new();
    let id = marking_retry(&mut twin);
    let effects = refuse(&mut twin, id, RejectReason::Invalid);
    assert_eq!((twin.stage(id), answers(&effects, RETRY_REPLY)), (Some(St::Failed), vec![OutboxAnswer::Err(Fault::Internal)]));
}

#[test]
fn out_marking_retry_refuses() {
    let mut twin = Twin::new();
    let id = marking_retry(&mut twin);
    let effects = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), reply: DISCARD_REPLY });
    assert_eq!(answers(&effects, DISCARD_REPLY), vec![OutboxAnswer::Err(Fault::InFlight)]);
    let effects = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), reply: 803 });
    assert_eq!((twin.stage(id), answers(&effects, 803)), (Some(St::MarkingRetry), vec![OutboxAnswer::Err(Fault::InFlight)]));
}

#[test]
fn out_marking_retry_other() {
    let mut twin = Twin::new();
    let id = marking_retry(&mut twin);
    let reset = OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 2, models: vec!["Chat".into()] });
    unchanged_by(&mut twin, id, St::MarkingRetry, vec![
        OutboxInput::Online(Online(true)),
        reset,
        OutboxInput::Response { op_id: id, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None },
        OutboxInput::TransportError { op_id: id, temporary: true },
        OutboxInput::Dropped { op_id: id },
        OutboxInput::RetryDue { op_id: id, cycle_id: 2 },
    ]);
}

#[test]
fn out_outcome_old_cycle() {
    let mut twin = Twin::new();
    let id = failed(&mut twin);
    let old = Receipt { epoch: EPOCH, op_id: id, cycle_id: 1, revision: 0, optimistic: None };
    retry(&mut twin);
    twin.committed(id);
    let effects = twin.post(OutboxInput::Outcome { receipt: old, reply: 804 });
    assert!(matches!(answers(&effects, 804).as_slice(), [OutboxAnswer::Outcome(OutboxOutcome::Failed { .. })]), "the outcome of the earlier cycle is immutable: {effects:?}");
}

#[test]
fn out_outcome_failed() {
    let mut twin = Twin::new();
    let id = failed(&mut twin);
    let current = Receipt { epoch: EPOCH, op_id: id, cycle_id: 1, revision: 0, optimistic: None };
    let effects = twin.post(OutboxInput::Outcome { receipt: current, reply: 805 });
    assert!(matches!(answers(&effects, 805).as_slice(), [OutboxAnswer::Outcome(OutboxOutcome::Failed { .. })]), "{effects:?}");
    assert_eq!(twin.stage(id), Some(St::Failed));
}

#[test]
fn out_submitting_committed_revision() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    twin.revision = 40;
    let effects = twin.committed(id);
    let receipt = effects.iter().find_map(|effect| match effect { OutboxEffect::Reply { answer: OutboxAnswer::Receipt(receipt), .. } => Some(receipt.clone()), _ => None }).expect("the receipt");
    assert_eq!(receipt.revision, 41, "the revision of the batch of the Submit, not of a later row write");
}

#[test]
fn out_sending_response_ignore() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.waiter(id);
    twin.post(OutboxInput::Response { op_id: id, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert_eq!(twin.stage(id), Some(St::Landing));
    assert!(twin.store_text.contains("RemoveIntent") && !twin.store_text.contains("Rows"), "the intent goes, the optimistic fields stay, no row of the answer: {}", twin.store_text);
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
    twin.post(OutboxInput::Response { op_id: id, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: Some(false) });
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
    twin.post(OutboxInput::Response { op_id: id, rows: other, swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let effects = twin.committed(id);
    assert!(answers(&effects, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Confirmed { .. }))) && twin.stage(id).is_none_or(|stage| stage == St::Done), "{effects:?}");
    assert!(!twin.store_text.contains("Swap"), "no swap for a root of another model");
}

#[test]
fn out_landing_committed_result_no_root() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.waiter(id);
    twin.post(OutboxInput::Response { op_id: id, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: Some(r#"{"copies":3}"#.into()), missing: Vec::new(), errors: None });
    let effects = twin.committed(id);
    assert!(answers(&effects, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Confirmed { result: Some(text), .. }) if text == r#"{"copies":3}"#)), "{effects:?}");
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
