//! Owner-level tests of the `outbox` machine: the local writes (the owner `LocalWrites`) and the properties of the marks and the discards. Names are the `Test` column.

use super::outbox_harness::*;
use super::outbox_harness::{Policy, St};
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
    twin.post(OutboxInput::Committed(Committed { batch: 1, revision: 11, tables: vec!["moments".into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }))
}

fn close(twin: &mut Twin) -> Vec<OutboxEffect> {
    take_fx(&twin.log, None);
    twin.det.close();
    twin.run();
    take_fx(&twin.log, state_of(&twin.det))
}

#[test]
fn out_local_write() {
    for kind in [LocalWriteKind::SetLocal, LocalWriteKind::SetEphemeral { expire_after: None }, LocalWriteKind::Attach { source: None }, LocalWriteKind::Detach] {
        let mut twin = Twin::new();
        let effects = twin.post(write(kind));
        assert_eq!((kinds(&effects), answers(&effects, WRITE)), (vec![CommitKind::LocalWrite], Vec::<OutboxAnswer>::new()));
    }
}

#[test]
fn out_local_write_stale() {
    let mut twin = Twin::new();
    let input = OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH + 1, write: LocalWriteKind::SetLocal, row: "w".into(), values: "{}".into(), reply: WRITE, now: Time(100) };
    let effects = twin.post(input);
    assert_eq!((answers(&effects, WRITE), kinds(&effects), effects.len()), (vec![OutboxAnswer::Err(Fault::NoSession)], Vec::<CommitKind>::new(), 1));
}

#[test]
fn out_local_write_stale_generation() {
    let mut twin = Twin::new();
    twin.post(write(LocalWriteKind::SetLocal));
    let effects = twin.post(OutboxInput::Rejected(Rejected { batch: 1, reason: RejectReason::StaleGeneration { models: vec!["Moment".into()], generation: 2 } }));
    assert!(answers(&effects, WRITE).is_empty());
    assert!(matches!(effects.as_slice(), [OutboxEffect::Store { kind: CommitKind::LocalWrite, generation: 2, .. }]));
    let [OutboxEffect::Store { batch, .. }] = effects.as_slice() else { unreachable!() };
    let committed = twin.post(OutboxInput::Committed(Committed { batch: *batch, revision: 11, tables: vec!["moments".into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
    assert_eq!(answers(&committed, WRITE), vec![OutboxAnswer::Written { revision: 11, shown: None, files_removed: 0 }]);
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
    assert_eq!(answers(&commit_first(&mut twin), WRITE), vec![OutboxAnswer::Written { revision: 11, shown: None, files_removed: 0 }]);
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
fn out_local_attach_copy_other() {
    let mut twin = Twin::new();
    let copy = twin.post(write(LocalWriteKind::Attach { source: Some("/tmp/a.bin".into()) }));
    let id = copy.iter().find_map(|effect| match effect { OutboxEffect::Copy { id, .. } => Some(*id), _ => None }).expect("the copy");
    for input in [OutboxInput::Online(Online(false)), OutboxInput::Online(Online(true)), OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, models: vec!["Moment".into()], generation: 9 }), OutboxInput::Tick(Time(5))] {
        let label = format!("{input:?}");
        let effects = twin.post(input);
        assert!(effects.is_empty(), "{label}: {effects:?}");
    }
    assert_eq!(kinds(&twin.post(OutboxInput::CopyDone { id })), vec![CommitKind::LocalWrite], "the copy still ends the write");
}

#[test]
fn out_local_set_ephemeral() {
    let mut twin = Twin::new();
    twin.post(write(LocalWriteKind::SetEphemeral { expire_after: Some(10) }));
    assert!(twin.store_text.contains("EphemeralRows"), "{}", twin.store_text);
    let committed = commit_first(&mut twin);
    assert_eq!(answers(&committed, WRITE), vec![OutboxAnswer::Written { revision: 11, shown: None, files_removed: 0 }]);
    assert!(committed.iter().any(|effect| matches!(effect, OutboxEffect::Schedule)), "the reset timer: {committed:?}");
}

#[test]
fn out_local_set_local_pending() {
    let mut twin = Twin::new();
    twin.post(write(LocalWriteKind::SetLocalPending));
    assert!(twin.store_text.contains("LocalPending"), "{}", twin.store_text);
    assert_eq!(answers(&commit_first(&mut twin), WRITE), vec![OutboxAnswer::Written { revision: 11, shown: None, files_removed: 0 }]);
}

#[test]
fn out_local_write_epoch_ended() {
    let mut twin = Twin::new();
    twin.post(write(LocalWriteKind::SetLocal));
    take_fx(&twin.log, None);
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    assert!(answers(&take_fx(&twin.log, state_of(&twin.det)), WRITE).is_empty(), "the Store decides");
    take_fx(&twin.log, None);
    twin.det.post(crate::runtime::Address::<crate::owners::write::outbox::Outbox>::new(3), crate::runtime::EpochStamp::Control, OutboxInput::Rejected(Rejected { batch: 1, reason: RejectReason::NoSession }));
    twin.run();
    let refused = take_fx(&twin.log, state_of(&twin.det));
    assert_eq!(answers(&refused, WRITE), vec![OutboxAnswer::Err(Fault::Rejected(RejectReason::NoSession))]);
}

#[test]
fn out_local_write_closing_waits() {
    let mut twin = Twin::new();
    twin.post(write(LocalWriteKind::SetLocal));
    let closing = close(&mut twin);
    assert!(answers(&closing, WRITE).is_empty() && drained(&closing) == 0, "{closing:?}");
    let committed = commit_first(&mut twin);
    assert_eq!((answers(&committed, WRITE), drained(&committed)), (vec![OutboxAnswer::Written { revision: 11, shown: None, files_removed: 0 }], 1));
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
    let refused = twin.rejected(id, super::outbox_harness::RReason::Storage);
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
    let first = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: 801 });
    let second = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: 802 });
    assert!(answers(&first, 801).is_empty() && answers(&second, 802).is_empty(), "the answers wait for the rollback");
    let done = twin.committed(id);
    let outcomes = done.iter().filter(|effect| matches!(effect, OutboxEffect::TransferOutcome { outcome: OutboxOutcome::Discarded, .. })).count();
    assert_eq!((answers(&done, 801), answers(&done, 802), outcomes), (vec![OutboxAnswer::Ok], vec![OutboxAnswer::Ok], 1));
}

/// A write of the values a memory row already holds still commits and renews the expiry: the earlier timer finds a newer write and leaves the row.
#[test]
fn out_local_write_memory_same() {
    let mut twin = Twin::new();
    let first = twin.post(write(LocalWriteKind::SetEphemeral { expire_after: Some(10) }));
    assert_eq!(kinds(&first), vec![CommitKind::LocalWrite]);
    commit_first(&mut twin);
    let again = twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::SetEphemeral { expire_after: Some(10) }, row: "w".into(), values: "{}".into(), reply: WRITE + 1, now: Time(200) });
    assert_eq!(kinds(&again), vec![CommitKind::LocalWrite], "the same values commit again: the write renews the expiry");
    let committed = twin.post(OutboxInput::Committed(Committed { batch: 2, revision: 12, tables: vec!["moments".into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
    assert_eq!(answers(&committed, WRITE + 1), vec![OutboxAnswer::Written { revision: 12, shown: None, files_removed: 0 }]);
    assert!(committed.iter().any(|effect| matches!(effect, OutboxEffect::Schedule)), "the new timer: {committed:?}");
    let stale = twin.post(OutboxInput::Expire { model: "Moment".into(), row: "w".into(), due: Time(110) });
    assert!(stale.is_empty(), "the timer of the first write does not remove the row: {stale:?}");
    let due = twin.post(OutboxInput::Expire { model: "Moment".into(), row: "w".into(), due: Time(210) });
    assert_eq!(kinds(&due), vec![CommitKind::RemoveRow]);
}

/// P-OUT-30: the answer of a `create` or a `destroy` without `refetch` marks no window stale, with `refetch` it marks exactly the declared pairs.
#[test]
fn out_prop_landing_no_stale() {
    for (stale, marks) in [(Vec::new(), false), (vec![("Moment".to_string(), "feed".to_string())], true)] {
        let mut twin = Twin::new();
        let id = twin.submit("r", Policy::Rollback, false);
        twin.committed(id);
        twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale, result: None, missing: Vec::new(), errors: None });
        assert_eq!(twin.store_text.contains("StaleFamilies"), marks, "{}", twin.store_text);
        twin.committed(id);
        let announced = twin.det.trace().iter().any(|entry| entry.effects.contains("Stale"));
        assert_eq!(announced, marks, "the windows turn stale only for a declared refetch");
    }
}

fn store_outcome(twin: &mut Twin, batch: u64, outcome: Option<RejectReason>) -> Vec<OutboxEffect> {
    take_fx(&twin.log, None);
    let input = match outcome {
        None => OutboxInput::Committed(Committed { batch, revision: 11, tables: vec!["moments".into()], rows: None, shown: Vec::new(), files_removed: 0, models: Default::default(), stale_parents: Vec::new(), position: 0 }),
        Some(reason) => OutboxInput::Rejected(Rejected { batch, reason }),
    };
    twin.det.post(crate::runtime::Address::<crate::owners::write::outbox::Outbox>::new(OUTBOX_AT), crate::runtime::EpochStamp::Control, input);
    twin.run();
    take_fx(&twin.log, state_of(&twin.det))
}

fn end_epoch(twin: &mut Twin) -> Vec<OutboxEffect> {
    take_fx(&twin.log, None);
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    take_fx(&twin.log, state_of(&twin.det))
}

/// P-OUT-35: a local write against the Store outcome of its batch, `Closing`, `EpochEnded` and `ModelsReset` in every order gets one answer and one batch.
#[test]
fn out_prop_local_write_orders() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Event { Write, Outcome, Closing, EpochEnded, ModelsReset }
    use Event::*;
    let stale = RejectReason::StaleGeneration { models: vec!["Moment".into()], generation: NEW_GENERATION };
    let outcomes: [Vec<(u64, Option<RejectReason>)>; 3] = [vec![(1, None)], vec![(1, Some(RejectReason::NoSession))], vec![(1, Some(stale)), (2, None)]];
    let orders: Vec<_> = crate::test_wait::permutations(&[Write, Outcome, Closing, EpochEnded, ModelsReset]).into_iter().filter(|order| order.iter().position(|e| *e == Write) < order.iter().position(|e| *e == Outcome)).collect();
    assert_eq!(orders.len(), 60);
    for outcome in outcomes {
        for order in &orders {
            let mut twin = Twin::new();
            let mut effects = Vec::new();
            for event in order {
                effects.extend(match event {
                    Write => twin.post(write(LocalWriteKind::SetLocal)),
                    Outcome => outcome.iter().flat_map(|(batch, result)| store_outcome(&mut twin, *batch, result.clone())).collect(),
                    Closing => close(&mut twin),
                    EpochEnded => end_epoch(&mut twin),
                    ModelsReset => twin.post(OutboxInput::ModelsReset(crate::owners::contract::ModelsReset { epoch: EPOCH, generation: 3, models: vec!["Moment".into()] })),
                });
            }
            let label = format!("{order:?} x {outcome:?}");
            let closed_first = order.iter().position(|e| *e == Closing) < order.iter().position(|e| *e == Write);
            let ended_first = order.iter().position(|e| *e == EpochEnded) < order.iter().position(|e| *e == Write);
            let want: Vec<OutboxAnswer> = if ended_first { Vec::new() } else if closed_first { vec![OutboxAnswer::Err(Fault::Closed)] } else { vec![outcome.last().and_then(|(_, result)| result.clone()).map_or(OutboxAnswer::Written { revision: 11, shown: None, files_removed: 0 }, |reason| OutboxAnswer::Err(Fault::Rejected(reason)))] };
            assert_eq!(answers(&effects, WRITE), want, "{label}: exactly one answer, none from the owner for a write of an ended epoch (the runtime answers the caller NoSession)");
            assert_eq!(kinds(&effects).len(), if closed_first || ended_first { 0 } else { 1 + outcome.iter().filter(|(_, result)| matches!(result, Some(RejectReason::StaleGeneration { .. }))).count() }, "{label}: one batch per attempt of the Store, none after Closing");
            assert_eq!(drained(&effects), 1, "{label}: Drained once");
        }
    }
}
