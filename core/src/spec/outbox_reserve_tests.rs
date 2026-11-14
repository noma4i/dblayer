//! Owner-level tests of the owner `Outbox`: `ReserveOp` for a transfer, `Submit` of a reserved operation and the check of the declaration.

use super::outbox_harness::*;
use super::outbox_harness::Policy;
use crate::spec::support::snapshot;
use crate::owners::write::transfer::TransferInput;
use crate::owners::session::SessionInput;
use crate::owners::write::outbox::{Fault, OutboxAnswer, OutboxInput, ReserveTarget};
use crate::owners::store::{IntentOrigin, StoreOp};
use crate::owners::write::outbox_types::{OpId, OutboxOutcome, Stage};
use crate::owners::contract::Online;
use crate::runtime::Cause;
use crate::spec::support::committed_at;

/// The typed inputs of every send the step gave, and the replies.
fn raw(twin: &mut Twin, input: OutboxInput) -> (Vec<Sent>, Vec<OutboxAnswer>) {
    take_fx(&twin.log, None);
    twin.det.post(crate::runtime::Address::<crate::owners::write::outbox::Outbox>::new(OUTBOX_AT), crate::runtime::EpochStamp::Epoch(EPOCH), input);
    twin.run();
    settle(twin)
}

fn settle(twin: &mut Twin) -> (Vec<Sent>, Vec<OutboxAnswer>) {
    let seen = snapshot(&twin.log);
    take_fx(&twin.log, None);
    let sends = seen.iter().filter_map(|found| if let Seen::Sent(sent) = found { Some(sent.clone()) } else { None }).collect();
    let replies = seen.into_iter().filter_map(|found| if let Seen::Run(crate::owners::write::outbox::OutboxEffect::Reply { answer, .. }) = found { Some(answer) } else { None }).collect();
    (sends, replies)
}

fn reserved(sends: &[Sent]) -> Vec<(ReserveTarget, Result<OpId, Fault>)> { sends.iter().filter_map(|sent| match sent { Sent::Transfer(TransferInput::OpReserved { target, result }) => Some((*target, result.clone())), _ => None }).collect() }

fn batches(sends: &[Sent]) -> Vec<Vec<StoreOp>> { sends.iter().filter_map(|sent| match sent { Sent::Store(batch) => Some(batch.ops.clone()), _ => None }).collect() }

fn to_transfer(sends: &[Sent]) -> Vec<&TransferInput> { sends.iter().filter_map(|sent| match sent { Sent::Transfer(input) => Some(input), _ => None }).collect() }

fn retry_outcomes(sends: &[Sent]) -> Vec<(Vec<(u64, u64)>, Result<i64, Fault>)> { sends.iter().filter_map(|sent| match sent { Sent::Transfer(TransferInput::RetryOutcome { cycles, result }) => Some((cycles.clone(), result.clone())), _ => None }).collect() }

fn mutation_outcomes(sends: &[Sent]) -> Vec<(u64, Vec<(u64, u64, OutboxOutcome)>, bool)> { sends.iter().filter_map(|sent| match sent { Sent::Transfer(TransferInput::MutationOutcomes { batch_id, members, intent_removed }) => Some((*batch_id, members.clone(), *intent_removed)), _ => None }).collect() }

fn transfer_outcome_op(ops: &[StoreOp], id: u64, cycle: u64) -> bool { ops.iter().any(|op| matches!(op, StoreOp::TransferOutcome { transfer_id, cycle_id, .. } if (*transfer_id, *cycle_id) == (id, cycle))) }

fn any_transfer_outcome_op(ops: &[StoreOp]) -> bool { ops.iter().any(|op| matches!(op, StoreOp::TransferOutcome { .. } | StoreOp::RetireTransferOutcome { .. })) }

fn session_has(twin: &Twin, matching: impl Fn(&SessionInput) -> bool) -> bool { twin.session_sends.iter().any(matching) }

fn is_cancel(input: &SessionInput) -> bool { matches!(input, SessionInput::Cancel { .. }) }

fn is_release(input: &SessionInput) -> bool { matches!(input, SessionInput::Release { .. }) }

fn undeclared_op() -> crate::owners::write::outbox::NewOp { crate::owners::write::outbox::NewOp { action: "send".into(), ..model_op("r", Policy::Rollback, false) } }

fn upload(transfer_id: u64, cycle_id: u64) -> crate::owners::store::UploadRow {
    crate::owners::store::UploadRow {
        key: format!("Moment:{transfer_id}:upload"), seq: transfer_id, source: Some(String::new()), parts: 1,
        batch: Some(9), fields: None, upload_id: None, confirmed_parts: 1, progress: 0, weight: None, weighed_prepare: false, batch_progress: 0, attempts: 1, completed: true,
        failed: None, created_at: 0, inline: true, size: 1, sha256: String::new(), path: String::new(),
        body: String::new(), on_failure: crate::schema::OnFailure::Keep, steps_path: Some(String::new()),
        steps_body: Some(String::new()), transfer_id, cycle_id, signed_id: None, aborted: false, staged: false, failed_at: None, blob_created_at: None, held: false, op_id: Some(4), etags: Vec::new(), prepare: None, max_bytes: None,
    }
}

#[test]
fn out_reserve_op() {
    let mut twin = Twin::new();
    let (sends, _) = raw(&mut twin, OutboxInput::ReserveOp { epoch: EPOCH, target: ReserveTarget::Transfer(5) });
    let found = reserved(&sends);
    assert!(matches!(found.as_slice(), [(ReserveTarget::Transfer(5), Ok(1))]), "{sends:?}");
    assert!(batches(&sends).is_empty(), "nothing is written");
    let (sends, _) = raw(&mut twin, OutboxInput::ReserveOp { epoch: EPOCH, target: ReserveTarget::Group(6) });
    assert!(matches!(reserved(&sends).first(), Some((_, Ok(2)))), "the next op_id of the sequence: {sends:?}");
    let id: OpId = twin.submit("r", Policy::Rollback, false);
    assert_eq!(id, 3, "a reserved op_id is not given again");
}

#[test]
fn out_reserve_op_while_restoring() {
    let mut twin = Twin::bare();
    let (sends, _) = raw(&mut twin, OutboxInput::ReserveOp { epoch: EPOCH, target: ReserveTarget::Transfer(5) });
    assert!(reserved(&sends).is_empty(), "the call waits for Restored");
    raw(&mut twin, OutboxInput::ReservedFloor { op_id: 9 });
    let (sends, _) = raw(&mut twin, OutboxInput::Restored { ops: Vec::new(), pending_lost: false });
    let found = reserved(&sends);
    assert!(matches!(found.as_slice(), [(_, Ok(10))]), "after Restored, above the floor of the restored transfers: {sends:?}");

    let mut ended = Twin::bare();
    raw(&mut ended, OutboxInput::ReserveOp { epoch: EPOCH, target: ReserveTarget::Transfer(5) });
    ended.det.end_epoch(EPOCH, Cause::SessionReset);
    ended.run();
    let (sends, _) = settle(&mut ended);
    let found = reserved(&sends);
    assert!(matches!(found.as_slice(), [(_, Err(Fault::Ended(crate::ffi::EndCause::SessionEnded)))]), "{sends:?}");
}

#[test]
fn out_reserve_op_queue_failed() {
    let mut twin = Twin::bare();
    raw(&mut twin, OutboxInput::RestoreFailed { error: "queue".into() });
    let (sends, _) = raw(&mut twin, OutboxInput::ReserveOp { epoch: EPOCH, target: ReserveTarget::Transfer(5) });
    let found = reserved(&sends);
    assert!(matches!(found.as_slice(), [(_, Err(Fault::Rejected(crate::owners::contract::RejectReason::Storage(_))))]), "{sends:?}");
}

#[test]
fn out_submit_intent_written() {
    let mut twin = Twin::new();
    let op = crate::owners::write::outbox::NewOp { effects: Default::default(), ..model_op("r", Policy::Rollback, true) };
    let (sends, replies) = raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op: op.clone(), created_at: 0 });
    assert_eq!(twin.state().stage_of(4), Some(Stage::Sending), "online and the row free: the request leaves");
    assert!(batches(&sends).is_empty() && sends.iter().any(|sent| matches!(sent, Sent::Session(SessionInput::Mutate { .. }))), "no write of the intent, one Mutate: {sends:?}");
    assert!(replies.is_empty(), "the outcome goes to Transfer, nobody waits for a receipt");

    let mut offline = Twin::new();
    raw(&mut offline, OutboxInput::Online(Online(false)));
    let (sends, _) = raw(&mut offline, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
    assert_eq!(offline.state().stage_of(4), Some(Stage::Queued));
    assert!(sends.is_empty(), "{sends:?}");
}

/// P-OUT-59: a reserved operation carries the `created_at` of its transfer, and the step that picks it for sending judges its expiry by the time of that step.
#[test]
fn out_reserved_expiry_reads_the_step_time() {
    const EXPIRES_MS: u64 = 10_000;
    let created_at = 50_000;
    for (name, wait, sent) in [("1 ms over the term", EXPIRES_MS + 1, false), ("the term is exact", EXPIRES_MS, true)] {
        let mut twin = Twin::new();
        twin.det.tick(crate::runtime::Time(created_at + wait));
        let op = crate::owners::write::outbox::NewOp { effects: Default::default(), expires_after: Some(std::time::Duration::from_millis(EXPIRES_MS)), ..model_op("r", Policy::Rollback, true) };
        raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at });
        let stage = twin.state().stage_of(4);
        assert_eq!(stage == Some(Stage::Sending), sent, "{name}: created at {created_at}, picked at {}: the stage is {stage:?}", created_at + wait);
    }
}

#[test]
fn out_submit_undeclared() {
    let mut twin = Twin::new();
    let (sends, replies) = raw(&mut twin, OutboxInput::Submit { epoch: EPOCH, op: undeclared_op(), reply: 31 });
    assert_eq!(replies, vec![OutboxAnswer::Err(Fault::Invalid)]);
    assert!(sends.is_empty() && twin.state().ops.is_empty(), "no optimism, no intent: {sends:?}");
}

#[test]
fn out_sending_response_batch_items_batch() {
    use crate::owners::store::RowValue;
    use crate::owners::write::outbox::{OpSource, TransferMember};
    let mut twin = Twin::new();
    let members = vec![TransferMember { transfer_id: 11, cycle_id: 1, row_id: "a".into(), op_id: None }, TransferMember { transfer_id: 12, cycle_id: 1, row_id: "b".into(), op_id: None }];
    let op = crate::owners::write::outbox::NewOp { origin: OpSource::TransferGroup { batch_id: 9, members }, ..model_op("a", Policy::Rollback, false) };
    raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
    let rows = vec![("Moment".into(), vec![RowValue { row_id: "A".into(), values: "{}".into() }, RowValue { row_id: "B".into(), values: "{}".into() }])];
    let (sends, _) = raw(&mut twin, OutboxInput::Response { op_id: 4, rows, swaps: vec![("a".into(), "A".into()), ("b".into(), "B".into())], stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let written = batches(&sends);
    assert_eq!(written.len(), 1);
    let batch = &written[0];
    assert!(batch.contains(&StoreOp::Swap { model: "Moment".into(), temp: "a".into(), real: "A".into() }) && batch.contains(&StoreOp::Swap { model: "Moment".into(), temp: "b".into(), real: "B".into() }), "{batch:?}");
    assert!(batch.iter().any(|op| matches!(op, StoreOp::Rows { model, .. } if model == "Moment")) && batch.contains(&StoreOp::RemoveIntent { op_id: 4 }) && batch.iter().any(|op| matches!(op, StoreOp::RemoveAnswer { .. })), "{batch:?}");
}

#[test]
fn out_transfer_group_outcomes_commit_with_the_intent() {
    use crate::owners::write::outbox::{OpSource, TransferMember};
    let mut twin = Twin::new();
    let members = vec![TransferMember { transfer_id: 11, cycle_id: 2, row_id: "a".into(), op_id: None }, TransferMember { transfer_id: 12, cycle_id: 4, row_id: "b".into(), op_id: None }];
    let op = crate::owners::write::outbox::NewOp { origin: OpSource::TransferGroup { batch_id: 9, members }, ..model_op("a", Policy::Rollback, false) };
    raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
    let (sends, _) = raw(&mut twin, OutboxInput::Response { op_id: 4, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: vec!["b".into()], errors: None });
    let store = batches(&sends);
    assert_eq!(store.len(), 1);
    let batch = &store[0];
    assert!(batch.contains(&StoreOp::RemoveIntent { op_id: 4 }) && batch.iter().any(|op| matches!(op, StoreOp::TransferOutcome { transfer_id: 11, cycle_id: 2, outcome: OutboxOutcome::Confirmed { .. } })) && batch.iter().any(|op| matches!(op, StoreOp::TransferOutcome { transfer_id: 12, cycle_id: 4, outcome: OutboxOutcome::Failed { .. } })), "{batch:?}");
    assert!(to_transfer(&sends).is_empty());
    let (sends, _) = raw(&mut twin, OutboxInput::Committed(committed_at(1, 37, &[])));
    let events = mutation_outcomes(&sends);
    assert_eq!(events.len(), 1);
    let (batch_id, members, intent_removed) = &events[0];
    assert!(*batch_id == 9 && *intent_removed && members.iter().any(|member| matches!(member, (11, 2, OutboxOutcome::Confirmed { revision: 37, .. }))) && members.iter().any(|member| matches!(member, (12, 4, OutboxOutcome::Failed { .. }))), "{events:?}");
}

#[test]
fn out_group_failed_outcome_and_retry_preserves_cycles() {
    use crate::owners::contract::{Rejected, RejectReason};
    use crate::owners::write::outbox::{OpSource, TransferMember};
    let mut twin = Twin::new();
    let members = vec![TransferMember { transfer_id: 11, cycle_id: 1, row_id: "a".into(), op_id: None }, TransferMember { transfer_id: 12, cycle_id: 1, row_id: "b".into(), op_id: None }];
    let op = crate::owners::write::outbox::NewOp { origin: OpSource::TransferGroup { batch_id: 9, members }, ..model_op("a", Policy::Keep, false) };
    raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
    let (sends, _) = raw(&mut twin, OutboxInput::Refused { op_id: 4, temporary: false, detail: "refused".into() });
    let store = batches(&sends).swap_remove(0);
    assert!(store.iter().any(|op| matches!(op, StoreOp::MarkError { .. })) && transfer_outcome_op(&store, 11, 1) && transfer_outcome_op(&store, 12, 1), "{store:?}");
    let (sends, _) = raw(&mut twin, OutboxInput::Committed(committed_at(1, 20, &[])));
    assert!(mutation_outcomes(&sends).iter().any(|(_, _, intent_removed)| !*intent_removed), "{sends:?}");
    let (sends, _) = raw(&mut twin, OutboxInput::RetryTransfer { model: "Moment".into(), row_id: "a".into(), reply: 1, uploads: vec![upload(11, 2), upload(12, 2)] });
    let store = batches(&sends).swap_remove(0);
    assert!(store.iter().any(|op| matches!(op, StoreOp::MarkError { .. })) && store.iter().any(|op| matches!(op, StoreOp::Attempt { .. })) && store.iter().any(|op| matches!(op, StoreOp::Upload { kind: crate::owners::store::UploadKind::Retrying, .. })) && !any_transfer_outcome_op(&store), "{store:?}");
    assert!(to_transfer(&sends).is_empty());
    let (sends, _) = raw(&mut twin, OutboxInput::Rejected(Rejected { batch: 2, reason: RejectReason::Storage("disk".into()) }));
    assert!(retry_outcomes(&sends).iter().any(|(cycles, result)| cycles.contains(&(11, 2)) && matches!(result, Err(Fault::Rejected(RejectReason::Storage(_))))), "{sends:?}");
    raw(&mut twin, OutboxInput::RetryTransfer { model: "Moment".into(), row_id: "a".into(), reply: 2, uploads: vec![upload(11, 2), upload(12, 2)] });
    let (sends, _) = raw(&mut twin, OutboxInput::Committed(committed_at(3, 21, &[])));
    assert!(retry_outcomes(&sends).iter().any(|(cycles, result)| cycles.contains(&(11, 2)) && cycles.contains(&(12, 2)) && matches!(result, Ok(21))), "{sends:?}");
    let (sends, _) = raw(&mut twin, OutboxInput::Response { op_id: 4, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let store = batches(&sends).swap_remove(0);
    assert!(transfer_outcome_op(&store, 11, 2) && transfer_outcome_op(&store, 12, 2), "{store:?}");
}

#[test]
fn out_group_rollback_outcome() {
    use crate::owners::write::outbox::{OpSource, TransferMember};
    let mut twin = Twin::new();
    let members = vec![TransferMember { transfer_id: 11, cycle_id: 2, row_id: "a".into(), op_id: None }, TransferMember { transfer_id: 12, cycle_id: 4, row_id: "b".into(), op_id: None }];
    let op = crate::owners::write::outbox::NewOp { origin: OpSource::TransferGroup { batch_id: 9, members }, ..model_op("a", Policy::Rollback, false) };
    raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
    let (sends, _) = raw(&mut twin, OutboxInput::Refused { op_id: 4, temporary: false, detail: "refused".into() });
    let store = batches(&sends).swap_remove(0);
    assert!(store.iter().any(|op| matches!(op, StoreOp::RollBack { .. })) && store.iter().any(|op| matches!(op, StoreOp::RemoveIntent { .. })) && store.iter().any(|op| matches!(op, StoreOp::TransferOutcome { transfer_id: 11, cycle_id: 2, outcome: OutboxOutcome::Rejected { .. } })) && store.iter().any(|op| matches!(op, StoreOp::TransferOutcome { transfer_id: 12, cycle_id: 4, outcome: OutboxOutcome::Rejected { .. } })), "{store:?}");
    let (sends, _) = raw(&mut twin, OutboxInput::Committed(committed_at(1, 20, &[])));
    let events = mutation_outcomes(&sends);
    assert!(matches!(events.as_slice(), [(_, members, _)] if members.iter().any(|member| matches!(member, (11, 2, OutboxOutcome::Rejected { .. }))) && members.iter().any(|member| matches!(member, (12, 4, OutboxOutcome::Rejected { .. })))) && to_transfer(&sends).len() == 1, "{events:?}");
}

/// A TransferGroup on the root row `a` and the member `member`: its intent is written by Transfer under the reserved `op_id`.
fn group_on_rows(twin: &mut Twin, member: &str, policy: Policy) -> OpId {
    use crate::owners::write::outbox::{OpSource, TransferMember};
    let (sends, _) = raw(twin, OutboxInput::ReserveOp { epoch: EPOCH, target: ReserveTarget::Group(9) });
    let op_id = reserved(&sends).first().and_then(|(_, result)| result.clone().ok()).expect("the reserved op_id");
    let members = vec![TransferMember { transfer_id: 11, cycle_id: 1, row_id: "a".into(), op_id: None }, TransferMember { transfer_id: 12, cycle_id: 4, row_id: member.into(), op_id: None }];
    let op = crate::owners::write::outbox::NewOp { origin: OpSource::TransferGroup { batch_id: 9, members }, ..model_op("a", policy, false) };
    twin.post(OutboxInput::SubmitReserved { epoch: EPOCH, op_id, op, created_at: 0 });
    op_id
}

/// Brings the group to `Sending`: the write of its attempt (out_submit_intent_written) is committed when the owner asked for one.
fn group_sending(twin: &mut Twin, group: OpId) {
    if twin.stage(group) == Some(super::outbox_harness::St::PersistingAttempt) { twin.committed(group); }
    assert_eq!(twin.stage(group), Some(super::outbox_harness::St::Sending), "the group reached Sending");
}

fn receipts_of(effects: &[OutboxEffect], reply: u64) -> Vec<OpId> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::Reply { reply: found, answer: OutboxAnswer::Receipt(receipt) } if *found == reply => Some(receipt.op_id), _ => None }).collect()
}

#[test]
fn out_selector_group_member() {
    use super::outbox_harness::{St, RWhy};
    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    let group = group_on_rows(&mut twin, "b", Policy::Rollback);
    assert_eq!(twin.stage(group), Some(St::Queued));
    let retried = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "b".into(), action: Some("update".into()), reply: 61 });
    assert_eq!(receipts_of(&retried, 61), vec![group], "a Retry of the member row b answers the receipt of the group: {retried:?}");
    let effects = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "b".into(), action: Some("update".into()), reply: 62 });
    assert_eq!((twin.stage(group), twin.store_ops.contains(&StoreOp::RollBack { op_id: group }), twin.store_ops.contains(&StoreOp::RemoveIntent { op_id: group })), (Some(St::RollingBack(RWhy::Discarded)), true, true), "a Discard of the member row b reaches the group: {effects:?}");
}

#[test]
fn out_selector_group_member_orders_the_row() {
    use super::outbox_harness::St;
    let mut twin = Twin::new();
    let group = group_on_rows(&mut twin, "b", Policy::Keep);
    group_sending(&mut twin, group);
    twin.post(OutboxInput::Refused { op_id: group, temporary: false, detail: "refused".into() });
    twin.committed(group);
    assert_eq!(twin.stage(group), Some(St::Failed));
    let member = twin.submit("b", Policy::Rollback, false);
    let effects = twin.committed(member);
    assert_eq!((twin.stage(member), sends(&effects)), (Some(St::Queued), 0), "an operation of the member row b waits behind the failed group: {effects:?}");
}

#[test]
fn out_selector_group_member_swapped() {
    use super::outbox_harness::{St, RWhy};
    let mut twin = Twin::new();
    let created = twin.submit("tb", Policy::Rollback, false);
    twin.committed(created);
    twin.post(OutboxInput::Response { op_id: created, rows: Vec::new(), swaps: vec![("tb".to_string(), "rb".to_string())], stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    twin.committed(created);
    assert_eq!(twin.stage(created), Some(St::Done), "the create of tb landed as rb");
    twin.post(OutboxInput::Online(Online(false)));
    let group = group_on_rows(&mut twin, "tb", Policy::Rollback);
    let retried = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "rb".into(), action: Some("update".into()), reply: 63 });
    assert_eq!(receipts_of(&retried, 63), vec![group], "the real id of the member row reaches the group: {retried:?}");
    twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "rb".into(), action: Some("update".into()), reply: 64 });
    assert_eq!(twin.stage(group), Some(St::RollingBack(RWhy::Discarded)));
}

#[test]
fn out_group_member_removed_keeps_group() {
    use super::outbox_harness::St;
    let mut twin = Twin::new();
    let group = group_on_rows(&mut twin, "b", Policy::Rollback);
    group_sending(&mut twin, group);
    twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "b".into(), revision: 30 });
    assert!(!session_has(&twin, is_cancel), "the request of the group goes on: {:?}", twin.session_sends);
    let membership = StoreOp::IntentOrigin { op_id: group, origin: IntentOrigin::TransferGroup { batch_id: 9, transfer_ids: vec![11] } };
    assert_eq!(
        (twin.stores.len(), twin.store_ops.contains(&membership), twin.store_ops.contains(&StoreOp::RemoveUploadByTransferId { transfer_id: 12 }), has_op!(twin, StoreOp::TransferOutcome { transfer_id: 12, cycle_id: 4, outcome: OutboxOutcome::Discarded }), has_op!(twin, StoreOp::TransferOutcome { transfer_id: 11, .. }), session_has(&twin, is_cancel), session_has(&twin, is_release), twin.stage(group)),
        (1, true, true, true, false, false, false, Some(St::Sending)),
        "one batch rewrites membership to [11], drops member 12 and its upload, the shared pair is untouched: {:?}", twin.store_ops);
    let batch = twin.stores[0].0;
    twin.commit_batch(batch);
    assert_eq!(twin.stage(group), Some(St::Sending), "after Committed the group still waits for the pair");
    let rows = vec![("Moment".to_string(), ["a", "b"].into_iter().map(|row| crate::owners::store::RowValue { row_id: row.into(), values: format!("{{\"m\":\"{row}\"}}") }).collect())];
    twin.post(OutboxInput::Response { op_id: group, rows, swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert_eq!(
        (has_op!(twin, StoreOp::TransferOutcome { transfer_id: 11, cycle_id: 1, outcome: OutboxOutcome::Confirmed { .. } }), has_op!(twin, StoreOp::TransferOutcome { transfer_id: 12, .. })),
        (true, false),
        "the pair answer then lands with Confirmed for 11 and nothing for 12: {:?}", twin.store_ops);
}

#[test]
fn out_group_last_member_removed() {
    use super::outbox_harness::{St, RWhy};
    let mut twin = Twin::new();
    let group = group_on_rows(&mut twin, "b", Policy::Rollback);
    group_sending(&mut twin, group);
    twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "b".into(), revision: 30 });
    let first = twin.stores[0].0;
    twin.commit_batch(first);
    let removed = twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "a".into(), revision: 31 });
    assert_eq!(
        (twin.stores.len(), twin.store_ops.contains(&StoreOp::RollBack { op_id: group }), twin.store_ops.contains(&StoreOp::RemoveIntent { op_id: group }), twin.store_ops.contains(&StoreOp::RemoveUploadByTransferId { transfer_id: 11 }), has_op!(twin, StoreOp::TransferOutcome { transfer_id: 11, cycle_id: 1, outcome: OutboxOutcome::Discarded }), twin.stage(group)),
        (1, true, true, true, true, Some(St::RollingBack(RWhy::Discarded))),
        "removing the last member discards the group in one batch: {:?} {removed:?}", twin.store_ops);
    let batch = twin.stores[0].0;
    let done = twin.commit_batch(batch);
    assert_eq!(
        (session_has(&twin, is_release), done.iter().any(|effect| matches!(effect, OutboxEffect::TransferOutcomes)), twin.stage(group), twin.stores.is_empty()),
        (true, true, Some(St::Done), true),
        "the started pair is released, the shared mutation ends Discarded, no empty group remains: {:?} {done:?}", twin.session_sends);
}

/// P-OUT-60: the Store refuses the removal batch of a group member as invalid data: the batch is not written again, the member ends `Discarded`, the answer of the pair lands for the members left.
#[test]
fn out_group_member_removal_invalid_is_not_retried() {
    use super::outbox_cells_tests::{timers, transfer_outcomes};
    use super::outbox_harness::St;
    use crate::owners::contract::{Rejected, RejectReason};
    let mut twin = Twin::new();
    let group = group_on_rows(&mut twin, "b", Policy::Rollback);
    group_sending(&mut twin, group);
    twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "b".into(), revision: 30 });
    let first = twin.stores[0].0;
    let refused = twin.post(OutboxInput::Rejected(Rejected { batch: first, reason: RejectReason::Invalid(String::new()) }));
    assert_eq!(
        (twin.stores.len(), timers(&refused), transfer_outcomes(&refused), twin.stage(group)),
        (0, 0, vec![crate::owners::write::outbox_types::OutboxOutcome::Discarded], Some(St::Sending)),
        "the refused removal is not written again, the member 12 ends Discarded, the group waits for the pair: {refused:?}");
    let rows = vec![("Moment".to_string(), ["a", "b"].into_iter().map(|row| crate::owners::store::RowValue { row_id: row.into(), values: format!("{{\"m\":\"{row}\"}}") }).collect())];
    twin.post(OutboxInput::Response { op_id: group, rows, swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert_eq!(
        (twin.stores.len(), has_op!(twin, StoreOp::IntentOrigin { .. }), has_op!(twin, StoreOp::TransferOutcome { transfer_id: 11, cycle_id: 1, outcome: OutboxOutcome::Confirmed { .. } }), has_op!(twin, StoreOp::TransferOutcome { transfer_id: 12, .. })),
        (1, false, true, false),
        "the answer of the pair lands once with Confirmed for 11, the removal batch is not issued again: {:?}", twin.store_ops);
}

#[test]
fn out_group_member_removal_rejected() {
    use super::outbox_harness::St;
    use crate::owners::contract::{Rejected, RejectReason};

    let mut twin = Twin::new();
    let group = group_on_rows(&mut twin, "b", Policy::Rollback);
    group_sending(&mut twin, group);
    twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "b".into(), revision: 30 });
    let first = twin.stores[0].0;
    twin.reject_batch(first, RReason::Storage);
    assert_eq!(
        (twin.stores.is_empty(), has_op!(twin, StoreOp::TransferOutcome { outcome: OutboxOutcome::Discarded, .. }), twin.stage(group)),
        (true, false, Some(St::Sending)),
        "Storage keeps the member in the group, nothing is written and no Discarded is delivered: {:?}", twin.store_ops);
    let membership = StoreOp::IntentOrigin { op_id: group, origin: IntentOrigin::TransferGroup { batch_id: 9, transfer_ids: vec![11] } };
    twin.waiter(group);
    assert!(twin.store_ops.contains(&membership) && twin.store_ops.contains(&StoreOp::RemoveUploadByTransferId { transfer_id: 12 }), "the next owner input for the group resends the membership batch: {:?}", twin.store_ops);

    let mut twin = Twin::new();
    let group = group_on_rows(&mut twin, "b", Policy::Rollback);
    group_sending(&mut twin, group);
    twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "b".into(), revision: 30 });
    let first = twin.stores[0].0;
    let (sends, _) = raw(&mut twin, OutboxInput::Rejected(Rejected { batch: first, reason: RejectReason::NoSession }));
    let transfer = mutation_outcomes(&sends);
    assert!(transfer.iter().any(|(_, members, _)| members.contains(&(11, 1, OutboxOutcome::Ended(crate::ffi::EndCause::SessionEnded))) && members.contains(&(12, 4, OutboxOutcome::Ended(crate::ffi::EndCause::SessionEnded)))) && twin.stage(group) == Some(St::Done), "NoSession ends the group SessionEnded to every member: {transfer:?}");

    let mut twin = Twin::new();
    let group = group_on_rows(&mut twin, "b", Policy::Rollback);
    group_sending(&mut twin, group);
    twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "b".into(), revision: 30 });
    let first = twin.stores[0].0;
    let (sends, _) = raw(&mut twin, OutboxInput::Rejected(Rejected { batch: first, reason: RejectReason::Closed }));
    let transfer = mutation_outcomes(&sends);
    assert!(transfer.iter().any(|(_, members, _)| members.contains(&(11, 1, OutboxOutcome::Ended(crate::ffi::EndCause::Closed))) && members.contains(&(12, 4, OutboxOutcome::Ended(crate::ffi::EndCause::Closed)))) && twin.stage(group) == Some(St::Done), "Closed ends the group Closed to every member: {transfer:?}");
}

#[test]
fn out_group_answer_waits_membership() {
    use super::outbox_harness::St;
    let mut twin = Twin::new();
    let group = group_on_rows(&mut twin, "b", Policy::Rollback);
    group_sending(&mut twin, group);
    twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "b".into(), revision: 30 });
    let member_batch = twin.stores[0].0;
    assert_eq!(twin.stores.len(), 1, "the membership batch is in flight: {:?}", twin.store_ops);
    let waited = twin.respond(group);
    assert_eq!((twin.stores.is_empty(), twin.stage(group)), (true, Some(St::Sending)), "the pair answer waits for the membership batch, no landing yet: {:?} {waited:?}", twin.store_ops);
    twin.commit_batch(member_batch);
    assert_eq!(
        (has_op!(twin, StoreOp::TransferOutcome { transfer_id: 11, cycle_id: 1, outcome: OutboxOutcome::Confirmed { .. } }), has_op!(twin, StoreOp::TransferOutcome { transfer_id: 12, .. })),
        (true, false),
        "Committed of the membership batch lets the answer land on [11]: {:?}", twin.store_ops);

    let mut twin = Twin::new();
    let group = group_on_rows(&mut twin, "b", Policy::Rollback);
    group_sending(&mut twin, group);
    twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "b".into(), revision: 30 });
    let first = twin.stores[0].0;
    twin.respond(group);
    assert!(twin.stores.is_empty(), "the pair answer waits: {:?}", twin.store_ops);
    let membership = StoreOp::IntentOrigin { op_id: group, origin: IntentOrigin::TransferGroup { batch_id: 9, transfer_ids: vec![11] } };
    let resent = twin.reject_batch(first, RReason::Storage);
    assert_eq!(
        (twin.store_ops.contains(&membership), has_op!(twin, StoreOp::TransferOutcome { transfer_id: 11, cycle_id: 1, outcome: OutboxOutcome::Confirmed { .. } })),
        (true, false),
        "Storage resends the membership batch first, nothing lands yet (D-258c): {:?} {resent:?}", twin.store_ops);
    let second = twin.stores[0].0;
    twin.commit_batch(second);
    assert!(has_op!(twin, StoreOp::TransferOutcome { transfer_id: 11, cycle_id: 1, outcome: OutboxOutcome::Confirmed { .. } }) && !has_op!(twin, StoreOp::TransferOutcome { transfer_id: 12, .. }), "then the answer lands on [11]: {:?}", twin.store_ops);
}

#[test]
fn out_group_outcome_each_member() {
    use crate::owners::write::outbox::{OpSource, TransferMember};
    let group = |twin: &mut Twin, policy: Policy| {
        let members = vec![TransferMember { transfer_id: 11, cycle_id: 2, row_id: "a".into(), op_id: None }, TransferMember { transfer_id: 12, cycle_id: 4, row_id: "b".into(), op_id: None }];
        let op = crate::owners::write::outbox::NewOp { origin: OpSource::TransferGroup { batch_id: 9, members }, ..model_op("a", policy, false) };
        twin.post(OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
        assert_eq!(twin.stage(4), Some(St::Sending), "the group is Sending");
    };

    let mut twin = Twin::new();
    group(&mut twin, Policy::Keep);
    twin.post(OutboxInput::Refused { op_id: 4, temporary: false, detail: "refused".into() });
    assert!(has_op!(twin, StoreOp::TransferOutcome { transfer_id: 11, cycle_id: 2, outcome: OutboxOutcome::Failed { .. } }) && has_op!(twin, StoreOp::TransferOutcome { transfer_id: 12, cycle_id: 4, outcome: OutboxOutcome::Failed { .. } }), "Failed reaches each member with its cycle_id: {:?}", twin.store_ops);

    let mut twin = Twin::new();
    group(&mut twin, Policy::Rollback);
    take_fx(&twin.log, None);
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    let (sends, _) = settle(&mut twin);
    assert!(mutation_outcomes(&sends).iter().any(|(_, members, _)| members.contains(&(11, 2, OutboxOutcome::Ended(crate::ffi::EndCause::SessionEnded))) && members.contains(&(12, 4, OutboxOutcome::Ended(crate::ffi::EndCause::SessionEnded)))), "EpochEnded reaches every member: {sends:?}");

    let mut twin = Twin::new();
    group(&mut twin, Policy::Rollback);
    take_fx(&twin.log, None);
    twin.det.close();
    twin.run();
    let (sends, _) = settle(&mut twin);
    assert!(mutation_outcomes(&sends).iter().any(|(_, members, _)| members.contains(&(11, 2, OutboxOutcome::Ended(crate::ffi::EndCause::Closed))) && members.contains(&(12, 4, OutboxOutcome::Ended(crate::ffi::EndCause::Closed)))), "Closing reaches every member: {sends:?}");
}

#[test]
fn out_retry_outcome_target() {
    use crate::owners::contract::{Rejected, RejectReason};
    use crate::owners::write::outbox::OpSource;
    let committed = |revision: i64| committed_at(2, revision, &[]);
    let failed = |twin: &mut Twin| {
        let op = crate::owners::write::outbox::NewOp { origin: OpSource::Transfer { transfer_id: 1, cycle_id: 1 }, ..model_op("r", Policy::Keep, false) };
        raw(twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
        raw(twin, OutboxInput::Refused { op_id: 4, temporary: false, detail: "refused".into() });
        raw(twin, OutboxInput::Committed(committed_at(1, 20, &[])));
        raw(twin, OutboxInput::RetryTransfer { model: "Moment".into(), row_id: "r".into(), reply: 1, uploads: vec![upload(1, 2)] });
    };

    let mut twin = Twin::new();
    failed(&mut twin);
    let (sends, _) = raw(&mut twin, OutboxInput::Committed(committed(21)));
    let found = retry_outcomes(&sends);
    assert!(found.iter().any(|(cycles, result)| cycles.contains(&(1, 2)) && matches!(result, Ok(21))), "Committed is addressed to the target (1, 2): {sends:?}");

    let mut twin = Twin::new();
    failed(&mut twin);
    let (sends, _) = raw(&mut twin, OutboxInput::Rejected(Rejected { batch: 2, reason: RejectReason::Storage("disk".into()) }));
    let found = retry_outcomes(&sends);
    assert!(found.iter().any(|(cycles, result)| cycles.contains(&(1, 2)) && matches!(result, Err(Fault::Rejected(RejectReason::Storage(_))))), "Rejected(Storage) is addressed to the target (1, 2): {sends:?}");

    let mut twin = Twin::new();
    failed(&mut twin);
    let (sends, _) = raw(&mut twin, OutboxInput::Rejected(Rejected { batch: 2, reason: RejectReason::NoSession }));
    let found = retry_outcomes(&sends);
    assert!(found.iter().any(|(cycles, result)| cycles.contains(&(1, 2)) && matches!(result, Err(Fault::Ended(crate::ffi::EndCause::SessionEnded)))), "Rejected(NoSession) is addressed to the target (1, 2): {sends:?}");
}

#[test]
fn out_retry_transfer_not_failed() {
    use crate::owners::write::outbox::OpSource;
    let mut twin = Twin::new();
    let op = crate::owners::write::outbox::NewOp { origin: OpSource::Transfer { transfer_id: 1, cycle_id: 1 }, ..model_op("r", Policy::Rollback, false) };
    raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
    assert_eq!(twin.state().stage_of(4), Some(Stage::Sending), "the operation is not Failed");
    let (sends, _) = raw(&mut twin, OutboxInput::RetryTransfer { model: "Moment".into(), row_id: "r".into(), reply: 5, uploads: vec![upload(1, 2)] });
    assert!(retry_outcomes(&sends).iter().any(|(cycles, result)| cycles.contains(&(1, 2)) && matches!(result, Err(Fault::InFlight))), "RetryTransfer outside Failed answers Err(InFlight) to the target: {sends:?}");
    assert_eq!((twin.state().stage_of(4), !batches(&sends).is_empty()), (Some(Stage::Sending), false), "the operation is unchanged and nothing is written");
}

#[test]
fn out_landing_swaps_all() {
    let mut twin = Twin::new();
    let id = twin.submit("a", Policy::Rollback, false);
    twin.committed(id);
    twin.post(OutboxInput::Response { op_id: id, rows: Vec::new(), swaps: vec![("a".into(), "real_a".into()), ("b".into(), "real_b".into())], stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert_eq!(twin.stores.len(), 1, "all swaps must land in one batch");
    let batch = &twin.stores[0].1;
    assert!(batch.contains(&StoreOp::Swap { model: "Moment".into(), temp: "a".into(), real: "real_a".into() }) && batch.contains(&StoreOp::Swap { model: "Moment".into(), temp: "b".into(), real: "real_b".into() }), "{batch:?}");
}
