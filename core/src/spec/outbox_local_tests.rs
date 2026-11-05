//! Owner-level tests of the `outbox` machine: the local writes (the owner `LocalWrites`) and the properties of the marks and the discards. Names are the `Test` column.

use super::outbox_harness::*;
use super::ref_outbox::{Policy, St};
use crate::owners::contract::{Committed, ModelsReset, Online, RejectReason, Rejected};
use crate::owners::write::outbox::{CommitKind, Fault, LocalWriteKind, OutboxAnswer, OutboxInput, OutboxOutcome};
use crate::runtime::{Cause, Time};

const WRITE: u64 = 60;

fn write(kind: LocalWriteKind) -> OutboxInput { OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: kind, row: "w".into(), values: "{}".into(), reply: WRITE, now: Time(100) } }

fn answers(effects: &[OutboxEffect], reply: u64) -> Vec<OutboxAnswer> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::Reply { reply: found, answer } if *found == reply => Some(answer.clone()), _ => None }).collect()
}

fn drained(effects: &[OutboxEffect]) -> usize { effects.iter().filter(|effect| matches!(effect, OutboxEffect::Drained)).count() }

fn kinds(effects: &[OutboxEffect]) -> Vec<CommitKind> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::Store { kind, .. } => Some(*kind), _ => None }).collect()
}

fn commit_first(twin: &mut Twin) -> Vec<OutboxEffect> {
    twin.post(OutboxInput::Committed(Committed { batch: 1, revision: 11, tables: vec!["moments".into()], models: Default::default() }))
}

fn close(twin: &mut Twin) -> Vec<OutboxEffect> {
    take_fx(&twin.log, None);
    twin.det.close();
    twin.run();
    take_fx(&twin.log, state_of(&twin.det))
}

#[test]
fn out_local_write_closing() {
    let mut twin = Twin::new();
    close(&mut twin);
    let effects = twin.post(write(LocalWriteKind::SetLocal));
    assert_eq!(answers(&effects, WRITE), vec![OutboxAnswer::Err(Fault::Closed)]);
}

#[test]
fn out_local_attach_copy() {
    let mut twin = Twin::new();
    let effects = twin.post(write(LocalWriteKind::Attach { source: Some("/tmp/a.bin".into()) }));
    assert!(effects.iter().any(|effect| matches!(effect, OutboxEffect::Copy { source, .. } if source == "/tmp/a.bin")), "{effects:?}");
    assert!(answers(&effects, WRITE).is_empty() && kinds(&effects).is_empty(), "nothing is written before the copy ends");
}

#[test]
fn out_local_attach_copied() {
    let mut twin = Twin::new();
    let copy = twin.post(write(LocalWriteKind::Attach { source: Some("/tmp/a.bin".into()) }));
    let id = copy.iter().find_map(|effect| match effect { OutboxEffect::Copy { id, .. } => Some(*id), _ => None }).expect("the copy");
    let copied = twin.post(OutboxInput::CopyDone { id });
    assert_eq!(kinds(&copied), vec![CommitKind::LocalWrite]);
    assert_eq!(answers(&commit_first(&mut twin), WRITE), vec![OutboxAnswer::Ok]);
}

#[test]
fn out_local_attach_copy_failed() {
    let mut twin = Twin::new();
    let copy = twin.post(write(LocalWriteKind::Attach { source: Some("/tmp/a.bin".into()) }));
    let id = copy.iter().find_map(|effect| match effect { OutboxEffect::Copy { id, .. } => Some(*id), _ => None }).expect("the copy");
    take_fx(&twin.log, None);
    twin.det.close();
    twin.run();
    let failed = twin.post(OutboxInput::CopyFailed { id });
    assert_eq!(answers(&failed, WRITE), vec![OutboxAnswer::Err(Fault::Rejected(RejectReason::Storage("file".into())))]);
    assert_eq!(drained(&failed), 1, "Drained when closing and nothing is active");
}

#[test]
fn out_local_attach_copy_waits() {
    let mut twin = Twin::new();
    let copy = twin.post(write(LocalWriteKind::Attach { source: Some("/tmp/a.bin".into()) }));
    let id = copy.iter().find_map(|effect| match effect { OutboxEffect::Copy { id, .. } => Some(*id), _ => None }).expect("the copy");
    let closing = close(&mut twin);
    assert!(answers(&closing, WRITE).is_empty() && drained(&closing) == 0, "{closing:?}");
    let ended = twin.post(OutboxInput::CopyDone { id });
    assert_eq!(kinds(&ended), vec![CommitKind::LocalWrite], "the copy ends first, then the write goes to the Store");
}

#[test]
fn out_local_set_ephemeral() {
    let mut twin = Twin::new();
    twin.post(write(LocalWriteKind::SetEphemeral { expire_after: Some(10) }));
    assert!(twin.store_text.contains("EphemeralRows"), "{}", twin.store_text);
    let committed = commit_first(&mut twin);
    assert_eq!(answers(&committed, WRITE), vec![OutboxAnswer::Ok]);
    assert!(committed.iter().any(|effect| matches!(effect, OutboxEffect::Schedule { .. })), "the reset timer: {committed:?}");
}

#[test]
fn out_local_set_local_pending() {
    let mut twin = Twin::new();
    twin.post(write(LocalWriteKind::SetLocalPending));
    assert!(twin.store_text.contains("LocalPending"), "{}", twin.store_text);
    assert_eq!(answers(&commit_first(&mut twin), WRITE), vec![OutboxAnswer::Ok]);
}

#[test]
fn out_local_write_epoch_ended() {
    let mut twin = Twin::new();
    twin.post(write(LocalWriteKind::SetLocal));
    take_fx(&twin.log, None);
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    assert!(answers(&take_fx(&twin.log, state_of(&twin.det)), WRITE).is_empty(), "the Store decides");
    let refused = twin.post(OutboxInput::Rejected(Rejected { batch: 1, reason: RejectReason::NoSession }));
    assert_eq!(answers(&refused, WRITE), vec![OutboxAnswer::Err(Fault::Rejected(RejectReason::NoSession))]);
}

#[test]
fn out_local_write_closing_waits() {
    let mut twin = Twin::new();
    twin.post(write(LocalWriteKind::SetLocal));
    let closing = close(&mut twin);
    assert!(answers(&closing, WRITE).is_empty() && drained(&closing) == 0, "{closing:?}");
    let committed = commit_first(&mut twin);
    assert_eq!((answers(&committed, WRITE), drained(&committed)), (vec![OutboxAnswer::Ok], 1));
}

#[test]
fn out_local_write_other() {
    let mut twin = Twin::new();
    twin.post(write(LocalWriteKind::SetLocal));
    for input in [OutboxInput::Online(Online(false)), OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 3, models: vec!["Chat".into()] }), OutboxInput::Tick(Time(5))] {
        let effects = twin.post(input.clone());
        assert!(effects.is_empty(), "{input:?}: {effects:?}");
    }
}

#[test]
fn out_local_closing() {
    let mut twin = Twin::new();
    assert_eq!(drained(&close(&mut twin)), 1);
}

/// P-OUT-11: a stale expiry leaves the row of a later write.
#[test]
fn out_prop_expire_last_write() {
    let mut twin = Twin::new();
    twin.post(write(LocalWriteKind::SetEphemeral { expire_after: Some(10) }));
    commit_first(&mut twin);
    let stale = twin.post(OutboxInput::Expire { model: "Moment".into(), row: "w".into(), due: Time(50) });
    assert!(kinds(&stale).is_empty() && stale.is_empty(), "{stale:?}");
    let due = twin.post(OutboxInput::Expire { model: "Moment".into(), row: "w".into(), due: Time(110) });
    assert_eq!(kinds(&due), vec![CommitKind::RemoveRow]);
}

/// P-OUT-12 and P-OUT-15: a `Mutate` after the first goes out only after the batch of its attempt is committed.
#[test]
fn out_prop_marks_durable() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
    let attempt = twin.post(OutboxInput::RetryDue { op_id: id, cycle_id: 1 });
    assert_eq!(super::outbox_harness::sends_of(&attempt), 0, "no Mutate before the attempt is written");
    let refused = twin.rejected(id, super::ref_outbox::Reason::Storage);
    assert_eq!(super::outbox_harness::sends_of(&refused), 0, "a refused write of the attempt starts no request");
    let again = twin.post(OutboxInput::RetryDue { op_id: id, cycle_id: 1 });
    assert_eq!(super::outbox_harness::sends_of(&again), 0);
    let sent = twin.committed(id);
    assert_eq!((super::outbox_harness::sends_of(&sent), twin.state().ops[&id].attempts), (1, 2));
}

/// P-OUT-15: the counter written is never lower than the number of `Mutate` sent.
#[test]
fn out_prop_attempt_persisted() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    twin.committed(id);
    let mut sent = 1u32;
    for _ in 0..5 {
        twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
        twin.post(OutboxInput::RetryDue { op_id: id, cycle_id: 1 });
        sent += u32::try_from(super::outbox_harness::sends_of(&twin.committed(id))).unwrap_or(0);
        assert!(twin.state().ops[&id].attempts >= sent, "attempts {} < sent {sent}", twin.state().ops[&id].attempts);
    }
}

/// P-OUT-16: a discard gives one answer and, for a transfer, one outcome after the rollback.
#[test]
fn out_prop_discard_outcome_once() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    let id = twin.submit("r", Policy::Rollback, true);
    twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Queued));
    let first = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), reply: 801 });
    let second = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), reply: 802 });
    assert!(answers(&first, 801).is_empty() && answers(&second, 802).is_empty(), "the answers wait for the rollback");
    let done = twin.committed(id);
    let outcomes = done.iter().filter(|effect| matches!(effect, OutboxEffect::TransferOutcome { outcome: OutboxOutcome::Discarded, .. })).count();
    assert_eq!((answers(&done, 801), answers(&done, 802), outcomes), (vec![OutboxAnswer::Ok], vec![OutboxAnswer::Ok], 1));
}
