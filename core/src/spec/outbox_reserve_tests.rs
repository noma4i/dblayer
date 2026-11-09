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
        steps_body: Some(String::new()), transfer_id, cycle_id, signed_id: None, aborted: false, staged: false, failed_at: None, op_id: Some(4), etags: Vec::new(),
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
    assert!(found.len() == 1 && found[0].contains("Err(SessionEnded)"), "{sends:?}");
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
    let (sends, replies) = raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op: op.clone() });
    assert_eq!(twin.state().stage_of(4), Some(Stage::Sending), "online and the row free: the request leaves");
    assert!(sends.iter().all(|(to, _)| *to != STORE_AT) && sends.iter().any(|(to, _)| *to == SESSION_AT), "no write of the intent, one Mutate: {sends:?}");
    assert!(replies.is_empty(), "the outcome goes to Transfer, nobody waits for a receipt");

    let mut offline = Twin::new();
    raw(&mut offline, OutboxInput::Online(Online(false)));
    let (sends, _) = raw(&mut offline, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op });
    assert_eq!(offline.state().stage_of(4), Some(Stage::Queued));
    assert!(sends.is_empty(), "{sends:?}");
}

#[test]
fn out_submit_undeclared() {
    let mut twin = Twin::new();
    let (sends, replies) = raw(&mut twin, OutboxInput::Submit { epoch: EPOCH, op: undeclared_op(), reply: 31 });
    assert_eq!(replies, vec![OutboxAnswer::Err(Fault::Invalid)]);
    assert!(sends.is_empty() && twin.state().ops.is_empty(), "no optimism, no intent: {sends:?}");
}

#[test]
fn out_transfer_group_outcomes_commit_with_the_intent() {
    use crate::owners::contract::Committed;
    use crate::owners::write::outbox::{OpSource, TransferMember};
    let mut twin = Twin::new();
    let members = vec![TransferMember { transfer_id: 11, cycle_id: 2, row_id: "a".into() }, TransferMember { transfer_id: 12, cycle_id: 4, row_id: "b".into() }];
    let op = crate::owners::write::outbox::NewOp { origin: OpSource::TransferGroup { batch_id: 9, members }, ..model_op("a", Policy::Rollback, false) };
    raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op });
    let (sends, _) = raw(&mut twin, OutboxInput::Response { op_id: 4, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: vec!["b".into()], errors: None });
    let store: Vec<_> = sends.iter().filter(|(to, _)| *to == STORE_AT).collect();
    assert_eq!(store.len(), 1);
    let batch = &store[0].1;
    assert!(batch.contains("RemoveIntent { op_id: 4 }") && batch.contains("TransferOutcome { transfer_id: 11, cycle_id: 2, outcome: Confirmed") && batch.contains("TransferOutcome { transfer_id: 12, cycle_id: 4, outcome: Failed"), "{batch}");
    assert!(sends.iter().all(|(to, _)| *to != TRANSFER_AT));
    let (sends, _) = raw(&mut twin, OutboxInput::Committed(Committed { batch: 1, revision: 37, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
    let events: Vec<_> = sends.iter().filter(|(to, _)| *to == TRANSFER_AT).collect();
    assert_eq!(events.len(), 1);
    assert!(events[0].1.contains("MutationOutcomes { batch_id: 9") && events[0].1.contains("11: Confirmed { revision: 37") && events[0].1.contains("12: Failed") && events[0].1.contains("intent_removed: true"), "{events:?}");
}

#[test]
fn out_group_failed_outcome_and_retry_preserves_cycles() {
    use crate::owners::contract::{Committed, Rejected, RejectReason};
    use crate::owners::write::outbox::{OpSource, TransferMember};
    let mut twin = Twin::new();
    let members = vec![TransferMember { transfer_id: 11, cycle_id: 1, row_id: "a".into() }, TransferMember { transfer_id: 12, cycle_id: 1, row_id: "b".into() }];
    let op = crate::owners::write::outbox::NewOp { origin: OpSource::TransferGroup { batch_id: 9, members }, ..model_op("a", Policy::Keep, false) };
    raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op });
    let (sends, _) = raw(&mut twin, OutboxInput::Refused { op_id: 4, temporary: false, detail: "refused".into() });
    let store = &sends.iter().find(|(to, _)| *to == STORE_AT).unwrap().1;
    assert!(store.contains("MarkError") && store.contains("transfer_id: 11, cycle_id: 1") && store.contains("transfer_id: 12, cycle_id: 1"), "{store}");
    let (sends, _) = raw(&mut twin, OutboxInput::Committed(Committed { batch: 1, revision: 20, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
    assert!(sends.iter().any(|(to, input)| *to == TRANSFER_AT && input.contains("MutationOutcomes") && input.contains("intent_removed: false")), "{sends:?}");
    let (sends, _) = raw(&mut twin, OutboxInput::RetryTransfer { model: "Moment".into(), row_id: "a".into(), reply: 1, uploads: vec![upload(11, 2), upload(12, 2)] });
    let store = &sends.iter().find(|(to, _)| *to == STORE_AT).unwrap().1;
    assert!(store.contains("MarkError") && store.contains("Attempt") && store.contains("Retrying") && !store.contains("TransferOutcome"), "{store}");
    assert!(sends.iter().all(|(to, _)| *to != TRANSFER_AT));
    let (sends, _) = raw(&mut twin, OutboxInput::Rejected(Rejected { batch: 2, reason: RejectReason::Storage("disk".into()) }));
    assert!(sends.iter().any(|(to, input)| *to == TRANSFER_AT && input.contains("RetryOutcome") && input.contains("(11, 2)") && input.contains("Err(Rejected(Storage")), "{sends:?}");
    raw(&mut twin, OutboxInput::RetryTransfer { model: "Moment".into(), row_id: "a".into(), reply: 2, uploads: vec![upload(11, 2), upload(12, 2)] });
    let (sends, _) = raw(&mut twin, OutboxInput::Committed(Committed { batch: 3, revision: 21, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
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
    raw(&mut twin, OutboxInput::SubmitReserved { epoch: EPOCH, op_id: 4, op });
    let (sends, _) = raw(&mut twin, OutboxInput::Refused { op_id: 4, temporary: false, detail: "refused".into() });
    let store = &sends.iter().find(|(to, _)| *to == STORE_AT).unwrap().1;
    assert!(store.contains("RollBack") && store.contains("RemoveIntent") && store.contains("transfer_id: 11, cycle_id: 1, outcome: Rejected") && store.contains("transfer_id: 12, cycle_id: 1, outcome: Rejected"), "{store}");
    let (sends, _) = raw(&mut twin, OutboxInput::Committed(Committed { batch: 1, revision: 20, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
    let events: Vec<_> = sends.iter().filter(|(to, _)| *to == TRANSFER_AT).collect();
    assert!(matches!(events.as_slice(), [(_, input)] if input.contains("11: Rejected") && input.contains("12: Rejected")), "{events:?}");
}
