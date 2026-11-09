//! Owner-level tests of the `outbox` machine: the cells of the attempt counter,
//! the swap of a temporary id and the unsent `create` cancelled by a `destroy`. Names are the `Test` column.

use super::outbox_harness::*;
use super::outbox_harness::{Policy, RReason, St, RWhy};
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

pub(super) fn unchanged_by(twin: &mut Twin, id: OpId, stage: St, inputs: Vec<OutboxInput>) {
    for input in inputs {
        let effects = twin.post(input.clone());
        assert_eq!(twin.stage(id), Some(stage), "{input:?}");
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
    assert!(twin.store_text.contains("attempts: 1") && twin.store_text.contains("cycle_id: 1"), "{}", twin.store_text);
}

#[test]
fn out_submitting_stale_generation() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    let effects = twin.rejected(id, RReason::StaleGeneration);
    assert!(matches!(effects.as_slice(), [OutboxEffect::Store { kind: CommitKind::Intent, generation, .. }] if *generation == NEW_GENERATION));
    assert_eq!(twin.stage(id), Some(St::Submitting));
}

#[test]
fn out_submitting_discard() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    let effects = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: DISCARD_REPLY });
    assert_eq!((twin.stage(id), answers(&effects, DISCARD_REPLY), effects.len()), (Some(St::Submitting), vec![OutboxAnswer::Err(Fault::InFlight)], 1));
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
}

#[test]
fn out_sending_offline() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    unchanged_by(&mut twin, id, St::Sending, vec![OutboxInput::Online(Online(false))]);
}

#[test]
fn out_sending_online() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    unchanged_by(&mut twin, id, St::Sending, vec![OutboxInput::Online(Online(true))]);
}

#[test]
fn out_sending_discard() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let effects = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: DISCARD_REPLY });
    assert_eq!((twin.stage(id), answers(&effects, DISCARD_REPLY), effects.len()), (Some(St::Sending), vec![OutboxAnswer::Err(Fault::InFlight)], 1));
}

#[test]
fn out_sending_reject_rollback() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let effects = twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
    assert_eq!((twin.stage(id), kinds(&effects), effects.len()), (Some(St::RollingBack(RWhy::Rejected)), vec![CommitKind::Rollback], 1));
    assert!(twin.store_text.contains("RemoveAnswer"));
}

#[test]
fn out_sending_reject_keep() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Keep, false);
    let effects = twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
    assert_eq!((twin.stage(id), kinds(&effects), effects.len()), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark], 1));
}

#[test]
fn out_sending_retry() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let effects = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: RETRY_REPLY });
    assert_eq!(twin.stage(id), Some(St::Sending));
    assert!(matches!(answers(&effects, RETRY_REPLY).as_slice(), [OutboxAnswer::Receipt(_)]));
    assert_eq!(effects.len(), 1);
}

#[test]
fn out_sending_stray() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    unchanged_by(&mut twin, id, St::Sending, vec![
        OutboxInput::Response { op_id: id + 100, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None },
        OutboxInput::TransportError { op_id: id + 100, temporary: true },
        OutboxInput::Dropped { op_id: id + 100 },
    ]);
}

#[test]
fn out_sending_response() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let request_id = twin.state().ops[&id].request_id;
    let effects = twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert_eq!((twin.stage(id), kinds(&effects), effects.len()), (Some(St::Landing), vec![CommitKind::Land], 1));
    assert!(twin.store_text.contains("RemoveIntent") && twin.store_text.contains("RemoveAnswer") && twin.store_text.contains(&request_id.to_string()));
}

#[test]
fn out_landing_stale_generation() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let effects = twin.rejected(id, RReason::StaleGeneration);
    assert_eq!(twin.stage(id), Some(St::Landing));
    assert!(matches!(effects.as_slice(), [OutboxEffect::Store { kind: CommitKind::Land, generation, .. }] if *generation == NEW_GENERATION));
}

#[test]
fn out_landing_storage() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let effects = refuse(&mut twin, id, RejectReason::Storage("disk".into()));
    assert_eq!((twin.stage(id), timers(&effects), effects.len()), (Some(St::Landing), 1, 1));
}

#[test]
fn out_landing_tick() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let effects = tick(&mut twin, id);
    assert_eq!((twin.stage(id), kinds(&effects), effects.len()), (Some(St::Landing), vec![CommitKind::Land], 1));
}

#[test]
fn out_landing_discard() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let effects = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: DISCARD_REPLY });
    assert_eq!((twin.stage(id), answers(&effects, DISCARD_REPLY), effects.len()), (Some(St::Landing), vec![OutboxAnswer::Err(Fault::InFlight)], 1));
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
}

#[test]
fn out_rollback_storage() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
    let effects = refuse(&mut twin, id, RejectReason::Storage("disk".into()));
    assert_eq!((twin.stage(id), timers(&effects), effects.len()), (Some(St::RollingBack(RWhy::Rejected)), 1, 1));
}

#[test]
fn out_rollback_tick() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
    let effects = tick(&mut twin, id);
    assert_eq!((twin.stage(id), kinds(&effects), effects.len()), (Some(St::RollingBack(RWhy::Rejected)), vec![CommitKind::Rollback], 1));
}

#[test]
fn out_rollback_stale_generation() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
    let effects = twin.rejected(id, RReason::StaleGeneration);
    assert_eq!(twin.stage(id), Some(St::RollingBack(RWhy::Rejected)));
    assert!(matches!(effects.as_slice(), [OutboxEffect::Store { kind: CommitKind::Rollback, generation, .. }] if *generation == NEW_GENERATION));
}

#[test]
fn out_rollback_retry() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
    let effects = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: RETRY_REPLY });
    assert_eq!((twin.stage(id), answers(&effects, RETRY_REPLY), effects.len()), (Some(St::RollingBack(RWhy::Rejected)), vec![OutboxAnswer::Err(Fault::InFlight)], 1));
}

#[test]
fn out_outcome_wait() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let effects = twin.post(OutboxInput::Outcome { receipt: twin.receipt(id, EPOCH), reply: WAITER });
    assert_eq!((twin.stage(id), effects.len()), (Some(St::Sending), 0));
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let effects = twin.committed(id);
    assert!(matches!(answers(&effects, WAITER).as_slice(), [OutboxAnswer::Outcome(OutboxOutcome::Confirmed { .. })]));
}

#[test]
fn out_outcome_stale() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let effects = twin.post(OutboxInput::Outcome { receipt: twin.receipt(id, EPOCH + 1), reply: WAITER });
    assert_eq!((twin.stage(id), answers(&effects, WAITER), effects.len()), (Some(St::Sending), vec![OutboxAnswer::Err(Fault::NoSession)], 1));
}

#[test]
fn out_outcome_done() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    twin.committed(id);
    let effects = twin.post(OutboxInput::Outcome { receipt: twin.receipt(id, EPOCH), reply: WAITER });
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
    assert!(twin.store_text.contains("attempts: 1") && twin.store_text.contains("cycle_id: 2"), "{}", twin.store_text);
}

#[test]
fn out_failed_discard() {
    let mut twin = Twin::new();
    let id = failed(&mut twin);
    let effects = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: DISCARD_REPLY });
    assert_eq!((twin.stage(id), kinds(&effects), effects.len()), (Some(St::RollingBack(RWhy::Discarded)), vec![CommitKind::Rollback], 1));
    assert!(twin.store_text.contains("RemoveAnswer"));
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
fn out_submitting_other() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    unchanged_by(&mut twin, id, St::Submitting, vec![
        OutboxInput::Online(Online(true)),
        OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 2, models: vec!["Moment".into()] }),
        OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None },
        OutboxInput::TransportError { op_id: id, temporary: true },
        OutboxInput::Dropped { op_id: id },
        OutboxInput::Tick(crate::runtime::Time(100)),
    ]);
}

#[test]
fn out_landing_other() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    unchanged_by(&mut twin, id, St::Landing, vec![
        OutboxInput::Online(Online(true)),
        OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 2, models: vec!["Moment".into()] }),
        OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None },
        OutboxInput::TransportError { op_id: id, temporary: true },
        OutboxInput::Dropped { op_id: id },
    ]);
}

#[test]
fn out_unknown_operation() {
    let mut twin = Twin::new();
    for input in [
        OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: WAITER },
        OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: WAITER },
        OutboxInput::Outcome { receipt: twin.receipt(99, EPOCH), reply: WAITER },
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
fn out_queued_discard() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    let effects = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: DISCARD_REPLY });
    assert_eq!((twin.stage(id), kinds(&effects), effects.len()), (Some(St::RollingBack(RWhy::Discarded)), vec![CommitKind::Rollback], 1));
    assert!(twin.store_text.contains("RollBack") && twin.store_text.contains("RemoveIntent"));
}

#[test]
fn out_queued_wait() {
    let mut twin = Twin::new();
    let id = queued_behind_blocker(&mut twin, model_op("r", Policy::Rollback, false));
    unchanged_by(&mut twin, id, St::Queued, vec![OutboxInput::Online(Online(true))]);
}

#[test]
fn out_queued_models_reset() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    let effects = twin.post(OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 2, models: vec!["Moment".into()] }));
    assert_eq!((twin.stage(id), effects.len(), twin.state().ops.contains_key(&id)), (Some(St::Queued), 0, true));
}

#[test]
fn out_queued_stray() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    let batch = twin.batches[&id];
    unchanged_by(&mut twin, id, St::Queued, vec![
        OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None },
        OutboxInput::TransportError { op_id: id, temporary: true },
        OutboxInput::Dropped { op_id: id },
        OutboxInput::RetryDue { op_id: id, cycle_id: 1 },
        OutboxInput::Rejected(Rejected { batch, reason: RejectReason::Invalid }),
    ]);
}

#[test]
fn out_landing_no_session() {
    for reason in [RejectReason::NoSession, RejectReason::Closed] {
        let mut twin = Twin::new();
        let id = sending(&mut twin, Policy::Rollback, true);
        twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
        let cause = if reason == RejectReason::Closed { "Closed" } else { "SessionEnded" };
        let effects = refuse(&mut twin, id, reason);
        assert_eq!(twin.stage(id), Some(St::Done));
        assert!(format!("{:?}", twin.state().ops[&id].outcome).contains(cause));
        assert!(format!("{:?}", transfer_outcomes(&effects)).contains(cause));
    }
}

#[test]
fn out_rollback_no_session() {
    for reason in [RejectReason::NoSession, RejectReason::Closed] {
        let mut twin = Twin::new();
        let id = sending(&mut twin, Policy::Rollback, true);
        twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
        let cause = if reason == RejectReason::Closed { "Closed" } else { "SessionEnded" };
        let effects = refuse(&mut twin, id, reason);
        assert_eq!(twin.stage(id), Some(St::Done));
        assert!(format!("{:?}", twin.state().ops[&id].outcome).contains(cause));
        assert!(format!("{:?}", transfer_outcomes(&effects)).contains(cause));
    }
}

#[test]
fn out_rollback_discard() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: DISCARD_REPLY });
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
fn out_rollback_other() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
    unchanged_by(&mut twin, id, St::RollingBack(RWhy::Rejected), vec![
        OutboxInput::Online(Online(true)),
        OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 2, models: vec!["Moment".into()] }),
        OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None },
        OutboxInput::TransportError { op_id: id, temporary: true },
        OutboxInput::Dropped { op_id: id },
    ]);
}

#[test]
fn out_submit_after_closing() {
    let mut twin = Twin::new();
    twin.det.close();
    twin.run();
    let effects = twin.post(OutboxInput::Submit { epoch: EPOCH, op: model_op("r", Policy::Rollback, false), reply: SUBMIT_REPLY });
    assert_eq!((answers(&effects, SUBMIT_REPLY), effects.len(), twin.state().ops.len()), (vec![OutboxAnswer::Err(Fault::Closed)], 1, 0));
}

#[test]
fn out_done_closing() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    twin.committed(id);
    take_fx(&twin.log, None);
    twin.det.close();
    twin.run();
    let effects = take_fx(&twin.log, state_of(&twin.det));
    assert_eq!((twin.stage(id), effects.iter().filter(|effect| matches!(effect, OutboxEffect::Drained)).count(), effects.len()), (Some(St::Done), 1, 1));
}

#[test]
fn out_sending_tick_not_waiting() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let effects = tick(&mut twin, id);
    assert_eq!((twin.stage(id), effects.len()), (Some(St::Sending), 0));
}

#[test]
fn out_queued_offline() {
    let mut twin = Twin::new();
    let id = queued_behind_blocker(&mut twin, model_op("r", Policy::Rollback, false));
    unchanged_by(&mut twin, id, St::Queued, vec![OutboxInput::Online(Online(false))]);
}

#[test]
fn out_queued_models_other() {
    let mut twin = Twin::new();
    let id = queued_behind_blocker(&mut twin, model_op("r", Policy::Rollback, false));
    unchanged_by(&mut twin, id, St::Queued, vec![OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 2, models: vec!["Chat".into()] })]);
}

#[test]
fn out_queued_retry() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    let effects = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: RETRY_REPLY });
    assert_eq!(twin.stage(id), Some(St::Queued));
    assert!(matches!(answers(&effects, RETRY_REPLY).as_slice(), [OutboxAnswer::Receipt(receipt)] if receipt.op_id == id && receipt.cycle_id == twin.state().ops[&id].cycle_id));
    assert_eq!(effects.len(), 1);
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
    assert_eq!(twin.state().ops[&destroy].outcome, Some(OutboxOutcome::Confirmed { revision: twin.revision, result: None, row_id: None }));
    assert_eq!(sends(&done), 0, "the network is not called for either");
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
}

#[test]
fn out_queued_exhausted() {
    let mut twin = Twin::new();
    let id = exhausted(&mut twin, Policy::Rollback);
    let effects = twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
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
        let cause = if reason == RejectReason::Closed { crate::ffi::EndCause::Closed } else { crate::ffi::EndCause::SessionEnded };
        let effects = refuse(&mut twin, id, reason);
        assert_eq!((twin.stage(id), transfer_outcomes(&effects)), (Some(St::Done), vec![OutboxOutcome::Interrupted(cause.clone())]));
        assert_eq!(twin.state().ops[&id].outcome, Some(OutboxOutcome::Interrupted(cause)));
    }
}

#[test]
fn out_server_removed_row_all_stages() {
    for stage in [St::WaitingDeclaration, St::Submitting, St::Queued, St::Sending, St::PersistingAttempt, St::Landing, St::RollingBack(RWhy::Rejected), St::MarkingFailed, St::MarkingRetry, St::Failed] {
        let ctx = if stage == St::Queued { offline() } else { ready() };
        let (mut twin, id) = reach(stage, &ctx);
        if stage == St::MarkingRetry {
            let mut receipt = twin.receipt(id, EPOCH);
            receipt.cycle_id = twin.state().ops[&id].cycle_id;
            twin.post(OutboxInput::Outcome { receipt, reply: WAITER });
        }
        let input = OutboxInput::RowRemoved { model: "Moment".into(), row_id: "r".into(), revision: 20 };
        let before = twin.post(input.clone());
        assert!(!answers(&before, WAITER).contains(&OutboxAnswer::Outcome(OutboxOutcome::Discarded)), "{stage:?}");
        if matches!(stage, St::Submitting | St::PersistingAttempt | St::Landing | St::RollingBack(_) | St::MarkingFailed | St::MarkingRetry) {
            twin.committed(id);
        }
        assert!(twin.store_text.contains("RemoveIntent") && twin.store_text.contains("ReleasePendingOwner") && !twin.store_text.contains("RollBack"), "{stage:?}: {}", twin.store_text);
        twin.post(input);
        let effects = twin.committed(id);
        let answers = if stage == St::Failed {
            let receipt = twin.receipt(id, EPOCH);
            answers(&twin.post(OutboxInput::Outcome { receipt, reply: WAITER }), WAITER)
        } else { answers(&effects, WAITER) };
        assert_eq!(answers, [OutboxAnswer::Outcome(OutboxOutcome::Discarded)], "{stage:?}");
        assert_eq!(twin.stage(id), Some(St::Done));
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
    let effects = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: DISCARD_REPLY });
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
