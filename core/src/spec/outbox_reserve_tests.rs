//! Owner-level tests of the owner `Outbox`: `ReserveOp` for a transfer, `Submit` of a reserved operation and the check of the declaration.

use super::outbox_harness::*;
use super::outbox_harness::Policy;
use crate::spec::support::{snapshot, Emitted};
use crate::owners::write::outbox::{Fault, OutboxAnswer, OutboxInput, ReserveTarget};
use crate::owners::write::outbox_types::{OpId, Stage};
use crate::owners::contract::Online;
use crate::runtime::Cause;

/// The typed text of every send the step gave, by receiver, and the replies.
fn raw(twin: &mut Twin, input: OutboxInput) -> (Vec<(usize, String)>, Vec<OutboxAnswer>) {
    take_fx(&twin.log, None);
    twin.det.post(crate::runtime::Address::<crate::owners::write::outbox::Outbox>::new(OUTBOX_AT), crate::runtime::EpochStamp::Epoch(EPOCH), input);
    twin.run();
    settle(twin)
}

fn settle(twin: &mut Twin) -> (Vec<(usize, String)>, Vec<OutboxAnswer>) {
    let emitted = snapshot(&twin.log);
    take_fx(&twin.log, None);
    let sends = emitted.iter().filter_map(|found| if let Emitted::Send { to, input } = found { Some((*to, input.clone())) } else { None }).collect();
    let replies = emitted.into_iter().filter_map(|found| if let Emitted::Run(crate::owners::write::outbox::OutboxEffect::Reply { answer, .. }) = found { Some(answer) } else { None }).collect();
    (sends, replies)
}

fn reserved(sends: &[(usize, String)]) -> Vec<String> { sends.iter().filter(|(to, text)| *to == TRANSFER_AT && text.contains("OpReserved")).map(|(_, text)| text.clone()).collect() }

fn undeclared_op() -> crate::owners::write::outbox::NewOp { crate::owners::write::outbox::NewOp { action: "send".into(), ..model_op("r", Policy::Rollback, false) } }

fn upload(transfer_id: u64, cycle_id: u64) -> crate::owners::store::UploadRow {
    crate::owners::store::UploadRow {
        key: format!("Moment:{transfer_id}:upload"), seq: transfer_id, source: Some(String::new()), parts: 1,
        batch: Some(9), fields: None, upload_id: None, confirmed_parts: 1, attempts: 1, completed: true,
        failed: None, created_at: 0, inline: true, size: 1, sha256: String::new(), path: String::new(),
        body: String::new(), on_failure: crate::schema::OnFailure::Keep, steps_path: Some(String::new()),
        steps_body: Some(String::new()), transfer_id, cycle_id, signed_id: None, aborted: false, staged: false, failed_at: None, blob_created_at: None, op_id: Some(4), etags: Vec::new(), prepare: None,
    }
}

#[test]
fn out_reserve_op() {
    let mut twin = Twin::new();
    let (sends, _) = raw(&mut twin, OutboxInput::ReserveOp { epoch: EPOCH, target: ReserveTarget::Transfer(5) });
    let found = reserved(&sends);
    assert!(found.len() == 1 && found[0].contains("Transfer(5)") && found[0].contains("Ok(1)"), "{sends:?}");
    assert!(sends.iter().all(|(to, _)| *to != STORE_AT), "nothing is written");
    let (sends, _) = raw(&mut twin, OutboxInput::ReserveOp { epoch: EPOCH, target: ReserveTarget::Group(6) });
    assert!(reserved(&sends)[0].contains("Ok(2)"), "the next op_id of the sequence");
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
    assert!(found.len() == 1 && found[0].contains("Ok(10)"), "after Restored, above the floor of the restored transfers: {sends:?}");

    let mut ended = Twin::bare();
    raw(&mut ended, OutboxInput::ReserveOp { epoch: EPOCH, target: ReserveTarget::Transfer(5) });
    ended.det.end_epoch(EPOCH, Cause::SessionReset);
    ended.run();
    let (sends, _) = settle(&mut ended);
    let found = reserved(&sends);
    assert!(found.len() == 1 && found[0].contains("Err(Ended(SessionEnded))"), "{sends:?}");
}

#[test]
fn out_reserve_op_queue_failed() {
    let mut twin = Twin::bare();
    raw(&mut twin, OutboxInput::RestoreFailed { error: "queue".into() });
    let (sends, _) = raw(&mut twin, OutboxInput::ReserveOp { epoch: EPOCH, target: ReserveTarget::Transfer(5) });
    let found = reserved(&sends);
    assert!(found.len() == 1 && found[0].contains("Err(Rejected(Storage("), "{sends:?}");
}

#[test]
fn out_submit_intent_written() {
    let mut twin = Twin::new();
    let op = crate::owners::write::outbox::NewOp { effects: Default::default(), ..model_op("r", Policy::Rollback, true) };
    let (sends, replies) = raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op: op.clone(), created_at: 0 });
    assert_eq!(twin.state().stage_of(4), Some(Stage::Sending), "online and the row free: the request leaves");
    assert!(sends.iter().all(|(to, _)| *to != STORE_AT) && sends.iter().any(|(to, _)| *to == SESSION_AT), "no write of the intent, one Mutate: {sends:?}");
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
    let members = vec![TransferMember { transfer_id: 11, cycle_id: 1, row_id: "a".into() }, TransferMember { transfer_id: 12, cycle_id: 1, row_id: "b".into() }];
    let op = crate::owners::write::outbox::NewOp { origin: OpSource::TransferGroup { batch_id: 9, members }, ..model_op("a", Policy::Rollback, false) };
    raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
    let rows = vec![("Moment".into(), vec![RowValue { row_id: "A".into(), values: "{}".into() }, RowValue { row_id: "B".into(), values: "{}".into() }])];
    let (sends, _) = raw(&mut twin, OutboxInput::Response { op_id: 4, rows, swaps: vec![("a".into(), "A".into()), ("b".into(), "B".into())], stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let batches: Vec<_> = sends.iter().filter(|(to, _)| *to == STORE_AT).collect();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0].1;
    assert!(batch.contains("Swap { model: \"Moment\", temp: \"a\", real: \"A\" }") && batch.contains("Swap { model: \"Moment\", temp: \"b\", real: \"B\" }"), "{batch}");
    assert!(batch.contains("Rows { model: \"Moment\"") && batch.contains("RemoveIntent { op_id: 4 }") && batch.contains("RemoveAnswer { request_id:"), "{batch}");
}

#[test]
fn out_transfer_group_outcomes_commit_with_the_intent() {
    use crate::owners::contract::Committed;
    use crate::owners::write::outbox::{OpSource, TransferMember};
    let mut twin = Twin::new();
    let members = vec![TransferMember { transfer_id: 11, cycle_id: 2, row_id: "a".into() }, TransferMember { transfer_id: 12, cycle_id: 4, row_id: "b".into() }];
    let op = crate::owners::write::outbox::NewOp { origin: OpSource::TransferGroup { batch_id: 9, members }, ..model_op("a", Policy::Rollback, false) };
    raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
    let (sends, _) = raw(&mut twin, OutboxInput::Response { op_id: 4, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: vec!["b".into()], errors: None });
    let store: Vec<_> = sends.iter().filter(|(to, _)| *to == STORE_AT).collect();
    assert_eq!(store.len(), 1);
    let batch = &store[0].1;
    assert!(batch.contains("RemoveIntent { op_id: 4 }") && batch.contains("TransferOutcome { transfer_id: 11, cycle_id: 2, outcome: Confirmed") && batch.contains("TransferOutcome { transfer_id: 12, cycle_id: 4, outcome: Failed"), "{batch}");
    assert!(sends.iter().all(|(to, _)| *to != TRANSFER_AT));
    let (sends, _) = raw(&mut twin, OutboxInput::Committed(Committed { batch: 1, revision: 37, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }));
    let events: Vec<_> = sends.iter().filter(|(to, _)| *to == TRANSFER_AT).collect();
    assert_eq!(events.len(), 1);
    assert!(events[0].1.contains("MutationOutcomes { batch_id: 9") && events[0].1.contains("(11, 2, Confirmed { revision: 37") && events[0].1.contains("(12, 4, Failed") && events[0].1.contains("intent_removed: true"), "{events:?}");
}

#[test]
fn out_group_failed_outcome_and_retry_preserves_cycles() {
    use crate::owners::contract::{Committed, Rejected, RejectReason};
    use crate::owners::write::outbox::{OpSource, TransferMember};
    let mut twin = Twin::new();
    let members = vec![TransferMember { transfer_id: 11, cycle_id: 1, row_id: "a".into() }, TransferMember { transfer_id: 12, cycle_id: 1, row_id: "b".into() }];
    let op = crate::owners::write::outbox::NewOp { origin: OpSource::TransferGroup { batch_id: 9, members }, ..model_op("a", Policy::Keep, false) };
    raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
    let (sends, _) = raw(&mut twin, OutboxInput::Refused { op_id: 4, temporary: false, detail: "refused".into() });
    let store = &sends.iter().find(|(to, _)| *to == STORE_AT).unwrap().1;
    assert!(store.contains("MarkError") && store.contains("transfer_id: 11, cycle_id: 1") && store.contains("transfer_id: 12, cycle_id: 1"), "{store}");
    let (sends, _) = raw(&mut twin, OutboxInput::Committed(Committed { batch: 1, revision: 20, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }));
    assert!(sends.iter().any(|(to, input)| *to == TRANSFER_AT && input.contains("MutationOutcomes") && input.contains("intent_removed: false")), "{sends:?}");
    let (sends, _) = raw(&mut twin, OutboxInput::RetryTransfer { model: "Moment".into(), row_id: "a".into(), reply: 1, uploads: vec![upload(11, 2), upload(12, 2)] });
    let store = &sends.iter().find(|(to, _)| *to == STORE_AT).unwrap().1;
    assert!(store.contains("MarkError") && store.contains("Attempt") && store.contains("Retrying") && !store.contains("TransferOutcome"), "{store}");
    assert!(sends.iter().all(|(to, _)| *to != TRANSFER_AT));
    let (sends, _) = raw(&mut twin, OutboxInput::Rejected(Rejected { batch: 2, reason: RejectReason::Storage("disk".into()) }));
    assert!(sends.iter().any(|(to, input)| *to == TRANSFER_AT && input.contains("RetryOutcome") && input.contains("(11, 2)") && input.contains("Err(Rejected(Storage")), "{sends:?}");
    raw(&mut twin, OutboxInput::RetryTransfer { model: "Moment".into(), row_id: "a".into(), reply: 2, uploads: vec![upload(11, 2), upload(12, 2)] });
    let (sends, _) = raw(&mut twin, OutboxInput::Committed(Committed { batch: 3, revision: 21, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }));
    assert!(sends.iter().any(|(to, input)| *to == TRANSFER_AT && input.contains("RetryOutcome") && input.contains("(11, 2)") && input.contains("(12, 2)") && input.contains("Ok(21)")), "{sends:?}");
    let (sends, _) = raw(&mut twin, OutboxInput::Response { op_id: 4, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    let store = &sends.iter().find(|(to, _)| *to == STORE_AT).unwrap().1;
    assert!(store.contains("transfer_id: 11, cycle_id: 2") && store.contains("transfer_id: 12, cycle_id: 2"), "{store}");
}

#[test]
fn out_group_rollback_outcome() {
    use crate::owners::contract::Committed;
    use crate::owners::write::outbox::{OpSource, TransferMember};
    let mut twin = Twin::new();
    let members = vec![TransferMember { transfer_id: 11, cycle_id: 1, row_id: "a".into() }, TransferMember { transfer_id: 12, cycle_id: 1, row_id: "b".into() }];
    let op = crate::owners::write::outbox::NewOp { origin: OpSource::TransferGroup { batch_id: 9, members }, ..model_op("a", Policy::Rollback, false) };
    raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
    let (sends, _) = raw(&mut twin, OutboxInput::Refused { op_id: 4, temporary: false, detail: "refused".into() });
    let store = &sends.iter().find(|(to, _)| *to == STORE_AT).unwrap().1;
    assert!(store.contains("RollBack") && store.contains("RemoveIntent") && store.contains("transfer_id: 11, cycle_id: 1, outcome: Rejected") && store.contains("transfer_id: 12, cycle_id: 1, outcome: Rejected"), "{store}");
    let (sends, _) = raw(&mut twin, OutboxInput::Committed(Committed { batch: 1, revision: 20, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }));
    let events: Vec<_> = sends.iter().filter(|(to, _)| *to == TRANSFER_AT).collect();
    assert!(matches!(events.as_slice(), [(_, input)] if input.contains("(11, 1, Rejected") && input.contains("(12, 1, Rejected")), "{events:?}");
}

/// A TransferGroup on the root row `a` and the member `member`: its intent is written by Transfer under the reserved `op_id`.
fn group_on_rows(twin: &mut Twin, member: &str, policy: Policy) -> OpId {
    use crate::owners::write::outbox::{OpSource, TransferMember};
    let (sends, _) = raw(twin, OutboxInput::ReserveOp { epoch: EPOCH, target: ReserveTarget::Group(9) });
    let op_id = reserved(&sends).first().and_then(|text| crate::spec::support::number_after(text, "Ok(")).expect("the reserved op_id");
    let members = vec![TransferMember { transfer_id: 11, cycle_id: 1, row_id: "a".into() }, TransferMember { transfer_id: 12, cycle_id: 1, row_id: member.into() }];
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
    assert_eq!((twin.stage(group), twin.store_text.contains(&format!("RollBack {{ op_id: {group} }}")), twin.store_text.contains(&format!("RemoveIntent {{ op_id: {group} }}"))), (Some(St::RollingBack(RWhy::Discarded)), true, true), "a Discard of the member row b reaches the group: {effects:?}");
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
    assert_eq!((twin.stage(member), sends_of(&effects)), (Some(St::Queued), 0), "an operation of the member row b waits behind the failed group: {effects:?}");
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
    assert!(!twin.session_text.contains("Cancel"), "the request of the group goes on: {}", twin.session_text);
    let membership = format!("IntentOrigin {{ op_id: {group}, origin: TransferGroup {{ batch_id: 9, transfer_ids: [11] }} }}");
    assert_eq!(
        (twin.stores.len(), twin.store_text.contains(&membership), twin.store_text.contains("RemoveUploadByTransferId { transfer_id: 12 }"), twin.store_text.contains("TransferOutcome { transfer_id: 12, cycle_id: 1, outcome: Discarded"), twin.store_text.contains("TransferOutcome { transfer_id: 11"), twin.session_text.contains("Cancel"), twin.session_text.contains("Release"), twin.stage(group)),
        (1, true, true, true, false, false, false, Some(St::Sending)),
        "one batch rewrites membership to [11], drops member 12 and its upload, the shared pair is untouched: {}", twin.store_text);
    let batch = twin.stores[0].0;
    twin.commit_batch(batch);
    assert_eq!(twin.stage(group), Some(St::Sending), "after Committed the group still waits for the pair");
    let rows = vec![("Moment".to_string(), ["a", "b"].into_iter().map(|row| crate::owners::store::RowValue { row_id: row.into(), values: format!("{{\"m\":\"{row}\"}}") }).collect())];
    twin.post(OutboxInput::Response { op_id: group, rows, swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert_eq!(
        (twin.store_text.contains("TransferOutcome { transfer_id: 11, cycle_id: 1, outcome: Confirmed"), twin.store_text.contains("TransferOutcome { transfer_id: 12")),
        (true, false),
        "the pair answer then lands with Confirmed for 11 and nothing for 12: {}", twin.store_text);
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
        (twin.stores.len(), twin.store_text.contains(&format!("RollBack {{ op_id: {group} }}")), twin.store_text.contains(&format!("RemoveIntent {{ op_id: {group} }}")), twin.store_text.contains("RemoveUploadByTransferId { transfer_id: 11 }"), twin.store_text.contains("TransferOutcome { transfer_id: 11, cycle_id: 1, outcome: Discarded"), twin.stage(group)),
        (1, true, true, true, true, Some(St::RollingBack(RWhy::Discarded))),
        "removing the last member discards the group in one batch: {} {removed:?}", twin.store_text);
    let batch = twin.stores[0].0;
    let done = twin.commit_batch(batch);
    assert_eq!(
        (twin.session_text.contains("Release"), done.iter().any(|effect| matches!(effect, OutboxEffect::TransferOutcomes)), twin.stage(group), twin.stores.is_empty()),
        (true, true, Some(St::Done), true),
        "the started pair is released, the shared mutation ends Discarded, no empty group remains: {} {done:?}", twin.session_text);
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
        (twin.stores.len(), twin.store_text.contains("IntentOrigin"), twin.store_text.contains("TransferOutcome { transfer_id: 11, cycle_id: 1, outcome: Confirmed"), twin.store_text.contains("TransferOutcome { transfer_id: 12")),
        (1, false, true, false),
        "the answer of the pair lands once with Confirmed for 11, the removal batch is not issued again: {}", twin.store_text);
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
    let membership = format!("IntentOrigin {{ op_id: {group}, origin: TransferGroup {{ batch_id: 9, transfer_ids: [11] }} }}");
    let resent = twin.reject_batch(first, RReason::StaleGeneration);
    assert_eq!(
        (twin.stores.len(), twin.stores[0].0 != first, twin.store_text.contains(&membership), twin.store_text.contains("RemoveUploadByTransferId { transfer_id: 12 }")),
        (1, true, true, true),
        "StaleGeneration resends the same membership batch on a new generation: {} {resent:?}", twin.store_text);

    let mut twin = Twin::new();
    let group = group_on_rows(&mut twin, "b", Policy::Rollback);
    group_sending(&mut twin, group);
    twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "b".into(), revision: 30 });
    let first = twin.stores[0].0;
    twin.reject_batch(first, RReason::Storage);
    assert_eq!(
        (twin.stores.is_empty(), twin.store_text.contains("Discarded"), twin.stage(group)),
        (true, false, Some(St::Sending)),
        "Storage keeps the member in the group, nothing is written and no Discarded is delivered: {}", twin.store_text);
    let membership = format!("IntentOrigin {{ op_id: {group}, origin: TransferGroup {{ batch_id: 9, transfer_ids: [11] }} }}");
    twin.waiter(group);
    assert!(twin.store_text.contains(&membership) && twin.store_text.contains("RemoveUploadByTransferId { transfer_id: 12 }"), "the next owner input for the group resends the membership batch: {}", twin.store_text);

    let mut twin = Twin::new();
    let group = group_on_rows(&mut twin, "b", Policy::Rollback);
    group_sending(&mut twin, group);
    twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "b".into(), revision: 30 });
    let first = twin.stores[0].0;
    let (sends, _) = raw(&mut twin, OutboxInput::Rejected(Rejected { batch: first, reason: RejectReason::NoSession }));
    let transfer: Vec<_> = sends.iter().filter(|(to, _)| *to == TRANSFER_AT).map(|(_, text)| text.clone()).collect();
    assert!(transfer.iter().any(|text| text.contains("(11, 1, Ended(SessionEnded))") && text.contains("(12, 1, Ended(SessionEnded))")) && twin.stage(group) == Some(St::Done), "NoSession ends the group SessionEnded to every member: {transfer:?}");

    let mut twin = Twin::new();
    let group = group_on_rows(&mut twin, "b", Policy::Rollback);
    group_sending(&mut twin, group);
    twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "b".into(), revision: 30 });
    let first = twin.stores[0].0;
    let (sends, _) = raw(&mut twin, OutboxInput::Rejected(Rejected { batch: first, reason: RejectReason::Closed }));
    let transfer: Vec<_> = sends.iter().filter(|(to, _)| *to == TRANSFER_AT).map(|(_, text)| text.clone()).collect();
    assert!(transfer.iter().any(|text| text.contains("(11, 1, Ended(Closed))") && text.contains("(12, 1, Ended(Closed))")) && twin.stage(group) == Some(St::Done), "Closed ends the group Closed to every member: {transfer:?}");
}

#[test]
fn out_group_answer_waits_membership() {
    use super::outbox_harness::St;
    let mut twin = Twin::new();
    let group = group_on_rows(&mut twin, "b", Policy::Rollback);
    group_sending(&mut twin, group);
    twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "b".into(), revision: 30 });
    let member_batch = twin.stores[0].0;
    assert_eq!(twin.stores.len(), 1, "the membership batch is in flight: {}", twin.store_text);
    let waited = twin.post(OutboxInput::Response { op_id: group, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert_eq!((twin.stores.is_empty(), twin.stage(group)), (true, Some(St::Sending)), "the pair answer waits for the membership batch, no landing yet: {} {waited:?}", twin.store_text);
    twin.commit_batch(member_batch);
    assert_eq!(
        (twin.store_text.contains("TransferOutcome { transfer_id: 11, cycle_id: 1, outcome: Confirmed"), twin.store_text.contains("TransferOutcome { transfer_id: 12")),
        (true, false),
        "Committed of the membership batch lets the answer land on [11]: {}", twin.store_text);

    let mut twin = Twin::new();
    let group = group_on_rows(&mut twin, "b", Policy::Rollback);
    group_sending(&mut twin, group);
    twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "b".into(), revision: 30 });
    let first = twin.stores[0].0;
    twin.post(OutboxInput::Response { op_id: group, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert!(twin.stores.is_empty(), "the pair answer waits: {}", twin.store_text);
    let membership = format!("IntentOrigin {{ op_id: {group}, origin: TransferGroup {{ batch_id: 9, transfer_ids: [11] }} }}");
    let resent = twin.reject_batch(first, RReason::Storage);
    assert_eq!(
        (twin.store_text.contains(&membership), twin.store_text.contains("TransferOutcome { transfer_id: 11, cycle_id: 1, outcome: Confirmed")),
        (true, false),
        "Storage resends the membership batch first, nothing lands yet (D-258c): {} {resent:?}", twin.store_text);
    let second = twin.stores[0].0;
    twin.commit_batch(second);
    assert!(twin.store_text.contains("TransferOutcome { transfer_id: 11, cycle_id: 1, outcome: Confirmed") && !twin.store_text.contains("TransferOutcome { transfer_id: 12"), "then the answer lands on [11]: {}", twin.store_text);
}

#[test]
fn out_group_outcome_each_member() {
    use crate::owners::write::outbox::{OpSource, TransferMember};
    let group = |twin: &mut Twin, policy: Policy| {
        let members = vec![TransferMember { transfer_id: 11, cycle_id: 2, row_id: "a".into() }, TransferMember { transfer_id: 12, cycle_id: 4, row_id: "b".into() }];
        let op = crate::owners::write::outbox::NewOp { origin: OpSource::TransferGroup { batch_id: 9, members }, ..model_op("a", policy, false) };
        twin.post(OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
        assert_eq!(twin.stage(4), Some(St::Sending), "the group is Sending");
    };

    let mut twin = Twin::new();
    group(&mut twin, Policy::Rollback);
    twin.post(OutboxInput::Refused { op_id: 4, temporary: false, detail: "refused".into() });
    assert!(twin.store_text.contains("TransferOutcome { transfer_id: 11, cycle_id: 2, outcome: Rejected") && twin.store_text.contains("TransferOutcome { transfer_id: 12, cycle_id: 4, outcome: Rejected"), "Rejected reaches each member with its cycle_id: {}", twin.store_text);

    let mut twin = Twin::new();
    group(&mut twin, Policy::Keep);
    twin.post(OutboxInput::Refused { op_id: 4, temporary: false, detail: "refused".into() });
    assert!(twin.store_text.contains("TransferOutcome { transfer_id: 11, cycle_id: 2, outcome: Failed") && twin.store_text.contains("TransferOutcome { transfer_id: 12, cycle_id: 4, outcome: Failed"), "Failed reaches each member with its cycle_id: {}", twin.store_text);

    let mut twin = Twin::new();
    group(&mut twin, Policy::Rollback);
    twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "b".into(), revision: 30 });
    assert!(twin.store_text.contains("TransferOutcome { transfer_id: 12, cycle_id: 4, outcome: Discarded"), "RowRemoved discards the member with its cycle_id: {}", twin.store_text);

    let mut twin = Twin::new();
    group(&mut twin, Policy::Rollback);
    take_fx(&twin.log, None);
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    let (sends, _) = settle(&mut twin);
    assert!(sends.iter().any(|(to, text)| *to == TRANSFER_AT && text.contains("MutationOutcomes") && text.contains("(11, 2, Ended(SessionEnded))") && text.contains("(12, 4, Ended(SessionEnded))")), "EpochEnded reaches every member: {sends:?}");

    let mut twin = Twin::new();
    group(&mut twin, Policy::Rollback);
    take_fx(&twin.log, None);
    twin.det.close();
    twin.run();
    let (sends, _) = settle(&mut twin);
    assert!(sends.iter().any(|(to, text)| *to == TRANSFER_AT && text.contains("MutationOutcomes") && text.contains("(11, 2, Ended(Closed))") && text.contains("(12, 4, Ended(Closed))")), "Closing reaches every member: {sends:?}");
}

#[test]
fn out_retry_outcome_target() {
    use crate::owners::contract::{Committed, Rejected, RejectReason};
    use crate::owners::write::outbox::OpSource;
    let committed = |revision: i64| Committed { batch: 2, revision, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() };
    let failed = |twin: &mut Twin| {
        let op = crate::owners::write::outbox::NewOp { origin: OpSource::Transfer { transfer_id: 1, cycle_id: 1 }, ..model_op("r", Policy::Keep, false) };
        raw(twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
        raw(twin, OutboxInput::Refused { op_id: 4, temporary: false, detail: "refused".into() });
        raw(twin, OutboxInput::Committed(Committed { batch: 1, revision: 20, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }));
        raw(twin, OutboxInput::RetryTransfer { model: "Moment".into(), row_id: "r".into(), reply: 1, uploads: vec![upload(1, 2)] });
    };
    let retry_outcome = |sends: &[(usize, String)]| -> Vec<String> { sends.iter().filter(|(to, text)| *to == TRANSFER_AT && text.contains("RetryOutcome")).map(|(_, text)| text.clone()).collect() };

    let mut twin = Twin::new();
    failed(&mut twin);
    let (sends, _) = raw(&mut twin, OutboxInput::Committed(committed(21)));
    let found = retry_outcome(&sends);
    assert!(found.iter().any(|text| text.contains("(1, 2)") && text.contains("Ok(21)")), "Committed is addressed to the target (1, 2): {sends:?}");

    let mut twin = Twin::new();
    failed(&mut twin);
    let (sends, _) = raw(&mut twin, OutboxInput::Rejected(Rejected { batch: 2, reason: RejectReason::Storage("disk".into()) }));
    let found = retry_outcome(&sends);
    assert!(found.iter().any(|text| text.contains("(1, 2)") && text.contains("Err(Rejected(Storage")), "Rejected(Storage) is addressed to the target (1, 2): {sends:?}");

    let mut twin = Twin::new();
    failed(&mut twin);
    let (sends, _) = raw(&mut twin, OutboxInput::Rejected(Rejected { batch: 2, reason: RejectReason::NoSession }));
    let found = retry_outcome(&sends);
    assert!(found.iter().any(|text| text.contains("(1, 2)") && text.contains("Err(Ended(SessionEnded")), "Rejected(NoSession) is addressed to the target (1, 2): {sends:?}");
}

#[test]
fn out_retry_transfer_not_failed() {
    use crate::owners::write::outbox::OpSource;
    let mut twin = Twin::new();
    let op = crate::owners::write::outbox::NewOp { origin: OpSource::Transfer { transfer_id: 1, cycle_id: 1 }, ..model_op("r", Policy::Rollback, false) };
    raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op, created_at: 0 });
    assert_eq!(twin.state().stage_of(4), Some(Stage::Sending), "the operation is not Failed");
    let (sends, _) = raw(&mut twin, OutboxInput::RetryTransfer { model: "Moment".into(), row_id: "r".into(), reply: 5, uploads: vec![upload(1, 2)] });
    assert!(sends.iter().any(|(to, text)| *to == TRANSFER_AT && text.contains("RetryOutcome") && text.contains("(1, 2)") && text.contains("Err(InFlight)")), "RetryTransfer outside Failed answers Err(InFlight) to the target: {sends:?}");
    assert_eq!((twin.state().stage_of(4), sends.iter().any(|(to, _)| *to == STORE_AT)), (Some(Stage::Sending), false), "the operation is unchanged and nothing is written");
}

#[test]
fn out_landing_swaps_all() {
    for group in [false, true] {
        let mut twin = Twin::new();
        let id = if group {
            let id = group_on_rows(&mut twin, "b", Policy::Rollback);
            group_sending(&mut twin, id);
            id
        } else {
            let id = twin.submit("a", Policy::Rollback, false);
            twin.committed(id);
            id
        };
        twin.post(OutboxInput::Response { op_id: id, rows: Vec::new(), swaps: vec![("a".into(), "real_a".into()), ("b".into(), "real_b".into())], stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
        assert_eq!(twin.stores.len(), 1, "all swaps must land in one batch");
        let batch = &twin.stores[0].1;
        assert!(batch.contains("temp: \"a\", real: \"real_a\"") && batch.contains("temp: \"b\", real: \"real_b\""), "group={group}: {batch}");
    }
}
