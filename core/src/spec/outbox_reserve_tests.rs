//! Owner-level tests of the owner `Outbox`: `ReserveOp` for a transfer, `Submit` of a reserved operation and the check of the declaration.

use super::outbox_harness::*;
use super::ref_outbox::Policy;
use crate::owners::support::{snapshot, Emitted};
use crate::owners::write::outbox::{Fault, OutboxAnswer, OutboxInput};
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

#[test]
fn out_reserve_op() {
    let mut twin = Twin::new();
    let (sends, _) = raw(&mut twin, OutboxInput::ReserveOp { epoch: EPOCH, transfer_id: 5 });
    let found = reserved(&sends);
    assert!(found.len() == 1 && found[0].contains("transfer_id: 5") && found[0].contains("Ok(1)"), "{sends:?}");
    assert!(sends.iter().all(|(to, _)| *to != STORE_AT), "nothing is written");
    let (sends, _) = raw(&mut twin, OutboxInput::ReserveOp { epoch: EPOCH, transfer_id: 6 });
    assert!(reserved(&sends)[0].contains("Ok(2)"), "the next op_id of the sequence");
    let id: OpId = twin.submit("r", Policy::Rollback, false);
    assert_eq!(id, 3, "a reserved op_id is not given again");
}

#[test]
fn out_reserve_op_while_restoring() {
    let mut twin = Twin::bare();
    let (sends, _) = raw(&mut twin, OutboxInput::ReserveOp { epoch: EPOCH, transfer_id: 5 });
    assert!(reserved(&sends).is_empty(), "the call waits for Restored");
    raw(&mut twin, OutboxInput::ReservedFloor { op_id: 9 });
    let (sends, _) = raw(&mut twin, OutboxInput::Restored { ops: Vec::new(), pending_lost: false });
    let found = reserved(&sends);
    assert!(found.len() == 1 && found[0].contains("Ok(10)"), "after Restored, above the floor of the restored transfers: {sends:?}");

    let mut ended = Twin::bare();
    raw(&mut ended, OutboxInput::ReserveOp { epoch: EPOCH, transfer_id: 5 });
    ended.det.end_epoch(EPOCH, Cause::SessionReset);
    ended.run();
    let (sends, _) = settle(&mut ended);
    let found = reserved(&sends);
    assert!(found.len() == 1 && found[0].contains("Err(Interrupted)"), "{sends:?}");
}

#[test]
fn out_reserve_op_queue_failed() {
    let mut twin = Twin::bare();
    raw(&mut twin, OutboxInput::RestoreFailed { error: "queue".into() });
    let (sends, _) = raw(&mut twin, OutboxInput::ReserveOp { epoch: EPOCH, transfer_id: 5 });
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
