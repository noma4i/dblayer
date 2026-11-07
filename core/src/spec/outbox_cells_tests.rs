//! Owner-level tests of the `outbox` machine: the cells of the attempt counter,
//! the swap of a temporary id and the unsent `create` cancelled by a `destroy`. Names are the `Test` column.

use super::outbox_harness::*;
use super::ref_outbox::{Policy, Reason as RReason, St, Why as RWhy};
use crate::owners::contract::{ModelsReset, Online, RejectReason, Rejected};
use crate::owners::write::outbox::{CommitKind, Fault, NewOp, OpId, OutboxAnswer, OutboxInput, OutboxOutcome};

pub(super) const RETRY_REPLY: u64 = 802;

pub(super) fn kinds(effects: &[OutboxEffect]) -> Vec<CommitKind> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::Store { kind, .. } => Some(*kind), _ => None }).collect()
}

pub(super) fn sends(effects: &[OutboxEffect]) -> usize { effects.iter().filter(|effect| matches!(effect, OutboxEffect::Send { .. })).count() }

pub(super) fn timers(effects: &[OutboxEffect]) -> usize { effects.iter().filter(|effect| matches!(effect, OutboxEffect::Schedule)).count() }

pub(super) fn answers(effects: &[OutboxEffect], reply: u64) -> Vec<OutboxAnswer> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::Reply { reply: found, answer } if *found == reply => Some(answer.clone()), _ => None }).collect()
}

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

pub(super) fn retry(twin: &mut Twin) -> Vec<OutboxEffect> { twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), reply: RETRY_REPLY }) }

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

/// An unsent `Queued` operation of the row `r`: a durable blocker holds the row.
pub(super) fn queued_behind_blocker(twin: &mut Twin, op: NewOp) -> OpId {
    sending(twin, Policy::Rollback, false);
    let id = submit_op(twin, op, 20);
    twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Queued));
    id
}

pub(super) fn unchanged_by(twin: &mut Twin, id: OpId, stage: St, inputs: Vec<OutboxInput>) {
    for input in inputs {
        let effects = twin.post(input.clone());
        assert_eq!(twin.stage(id), Some(stage), "{input:?}");
        assert!(effects.is_empty(), "{input:?}: {effects:?}");
    }
}

#[test]
fn out_submit_offline_not_durable() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    let effects = twin.post(OutboxInput::Submit { epoch: EPOCH, op: NewOp { durable: false, ..model_op("r", Policy::Rollback, false) }, reply: 21 });
    assert_eq!(answers(&effects, 21), vec![OutboxAnswer::Err(Fault::Offline)]);
    assert!(kinds(&effects).is_empty() && twin.state().ops.is_empty(), "no optimism, no intent: {effects:?}");
}

#[test]
fn out_queued_plain_offline() {
    let mut twin = Twin::new();
    let id = queued_behind_blocker(&mut twin, NewOp { durable: false, ..model_op("r", Policy::Rollback, false) });
    twin.waiter(id);
    let effects = twin.post(OutboxInput::Online(Online(false)));
    assert_eq!(kinds(&effects), vec![CommitKind::Rollback]);
    let done = twin.committed(id);
    assert!(answers(&done, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Rejected { error }) if error == "network")), "{done:?}");
    assert_eq!(sends(&twin.post(OutboxInput::Online(Online(true)))), 0, "it is not sent after the network returns");
}

#[test]
fn out_destroy_unsent_create() {
    let mut twin = Twin::new();
    let created = queued_behind_blocker(&mut twin, NewOp { action: "create".into(), ..model_op("r", Policy::Rollback, false) });
    twin.waiter(created);
    let effects = twin.post(OutboxInput::Submit { epoch: EPOCH, op: NewOp { action: "destroy".into(), ..model_op("r", Policy::Rollback, false) }, reply: 22 });
    assert_eq!(kinds(&effects), vec![CommitKind::Rollback], "the unsent create is taken back, the destroy writes no intent");
    let destroy = *twin.state().ops.keys().max().expect("the destroy");
    let done = twin.committed(created);
    assert!(answers(&done, WAITER).contains(&OutboxAnswer::Outcome(OutboxOutcome::Discarded)), "{done:?}");
    assert!(matches!(answers(&done, 22).as_slice(), [OutboxAnswer::Receipt(_)]), "{done:?}");
    assert_eq!(twin.state().ops[&destroy].outcome, Some(OutboxOutcome::Confirmed { revision: twin.revision, result: None }));
    assert_eq!(sends(&done), 0, "the network is not called for either");
}

#[test]
fn out_queued_persist_attempt() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::Online(Online(false)));
    tick(&mut twin, id);
    assert_eq!(twin.stage(id), Some(St::Queued));
    let effects = twin.post(OutboxInput::Online(Online(true)));
    assert_eq!((twin.stage(id), kinds(&effects), sends(&effects)), (Some(St::PersistingAttempt), vec![CommitKind::Attempt], 0));
}

#[test]
fn out_queued_exhausted() {
    let mut twin = Twin::new();
    let id = exhausted(&mut twin, Policy::Rollback);
    twin.post(OutboxInput::Online(Online(false)));
    tick(&mut twin, id);
    assert_eq!(twin.stage(id), Some(St::Queued));
    let effects = twin.post(OutboxInput::Online(Online(true)));
    assert_eq!((twin.stage(id), kinds(&effects), sends(&effects)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark], 0));
}

#[test]
fn out_queued_blocked() {
    let mut twin = Twin::new();
    failed(&mut twin);
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Queued));
    unchanged_by(&mut twin, id, St::Queued, vec![OutboxInput::Online(Online(true))]);
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
fn out_sending_tick_exhausted() {
    let mut twin = Twin::new();
    let id = exhausted(&mut twin, Policy::Rollback);
    let effects = tick(&mut twin, id);
    assert_eq!((twin.stage(id), kinds(&effects), sends(&effects)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark], 0), "the 41st attempt never goes out");
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
}

#[test]
fn out_persisting_attempt_storage() {
    let mut twin = Twin::new();
    let id = persisting(&mut twin, false);
    let effects = refuse(&mut twin, id, RejectReason::Storage("disk".into()));
    assert_eq!((twin.stage(id), timers(&effects), sends(&effects), twin.state().ops[&id].attempts), (Some(St::PersistingAttempt), 1, 0, 1));
}

#[test]
fn out_persisting_attempt_tick() {
    let mut twin = Twin::new();
    let id = persisting(&mut twin, false);
    let effects = tick(&mut twin, id);
    assert_eq!((twin.stage(id), kinds(&effects)), (Some(St::PersistingAttempt), vec![CommitKind::Attempt]));
}

#[test]
fn out_persisting_attempt_stale_generation() {
    let mut twin = Twin::new();
    let id = persisting(&mut twin, false);
    let effects = twin.rejected(id, RReason::StaleGeneration);
    assert!(matches!(effects.as_slice(), [OutboxEffect::Store { kind: CommitKind::Attempt, generation, .. }] if *generation == NEW_GENERATION), "{effects:?}");
    assert_eq!(twin.stage(id), Some(St::PersistingAttempt));
}

#[test]
fn out_persisting_attempt_no_session() {
    for reason in [RejectReason::NoSession, RejectReason::Closed] {
        let mut twin = Twin::new();
        let id = persisting(&mut twin, true);
        let effects = refuse(&mut twin, id, reason);
        assert_eq!((twin.stage(id), transfer_outcomes(&effects)), (Some(St::Done), vec![OutboxOutcome::Interrupted]));
        assert_eq!(twin.state().ops[&id].outcome, Some(OutboxOutcome::Interrupted));
    }
}

#[test]
fn out_persisting_attempt_invalid() {
    let mut twin = Twin::new();
    let id = persisting(&mut twin, false);
    let effects = refuse(&mut twin, id, RejectReason::Invalid);
    assert_eq!((twin.stage(id), kinds(&effects)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark]));
    assert_eq!(twin.state().ops[&id].error.as_deref(), Some("Internal(Invalid)"));
}

#[test]
fn out_persisting_attempt_discard() {
    let mut twin = Twin::new();
    let id = persisting(&mut twin, false);
    let effects = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), reply: DISCARD_REPLY });
    assert_eq!((twin.stage(id), answers(&effects, DISCARD_REPLY)), (Some(St::PersistingAttempt), vec![OutboxAnswer::Err(Fault::InFlight)]));
}

#[test]
fn out_persisting_attempt_retry() {
    let mut twin = Twin::new();
    let id = persisting(&mut twin, false);
    let effects = retry(&mut twin);
    assert_eq!(twin.stage(id), Some(St::PersistingAttempt));
    assert!(matches!(answers(&effects, RETRY_REPLY).as_slice(), [OutboxAnswer::Receipt(_)]), "{effects:?}");
}

#[test]
fn out_persisting_attempt_other() {
    let mut twin = Twin::new();
    let id = persisting(&mut twin, false);
    let foreign_cycle = OutboxInput::RetryDue { op_id: id, cycle_id: 99 };
    let reset = OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 2, models: vec!["Chat".into()] });
    unchanged_by(&mut twin, id, St::PersistingAttempt, vec![
        OutboxInput::Online(Online(true)),
        reset,
        OutboxInput::Response { op_id: id, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None },
        OutboxInput::TransportError { op_id: id, temporary: true },
        OutboxInput::Dropped { op_id: id },
        foreign_cycle,
    ]);
}

#[test]
fn out_landing_committed_swap() {
    let mut twin = Twin::new();
    let created = sending(&mut twin, Policy::Rollback, false);
    let follower = submit_op(&mut twin, NewOp { body: "{\"row\":\"r\"}".into(), ..model_op("r", Policy::Rollback, false) }, 23);
    twin.committed(follower);
    twin.waiter(created);
    twin.post(OutboxInput::Response { op_id: created, rows: server_rows(), swaps: vec![("r".into(), "R".into())], stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert!(twin.store_text.contains("Swap") && twin.store_text.contains("\"R\""), "the landing batch swaps the row: {}", twin.store_text);
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
    assert!(twin.store_text.contains("StaleFamilies") && twin.store_text.contains("history"), "the landing batch marks the windows stale: {}", twin.store_text);
    let effects = twin.committed(id);
    assert!(answers(&effects, WAITER).iter().any(|answer| matches!(answer, OutboxAnswer::Outcome(OutboxOutcome::Confirmed { .. }))), "{effects:?}");
}

#[test]
fn out_landing_invalid() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let effects = refuse(&mut twin, id, RejectReason::Invalid);
    assert_eq!((twin.stage(id), kinds(&effects)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark]));
}

#[test]
fn out_rollback_invalid() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
    let effects = refuse(&mut twin, id, RejectReason::Invalid);
    assert_eq!((twin.stage(id), kinds(&effects)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark]));
}
