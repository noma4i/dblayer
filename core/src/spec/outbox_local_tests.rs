//! Owner-level tests of the `outbox` machine: the local writes (the owner `LocalWrites`) and the properties of the marks and the discards. Names are the `Test` column.

use super::outbox_harness::*;
use super::outbox_harness::Policy;
use crate::owners::contract::{Online, RejectReason, Rejected};
use crate::owners::store::{RowValue, StoreOp};
use crate::owners::write::outbox::{CommitKind, Fault, LocalWriteKind, OutboxAnswer, OutboxInput, OutboxOutcome};
use crate::runtime::{Cause, Time};
use crate::spec::support::committed_at;

const WRITE: u64 = 60;

fn write(kind: LocalWriteKind) -> OutboxInput { OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: kind, row: "w".into(), values: "{}".into(), reply: WRITE } }

fn commit_first(twin: &mut Twin) -> Vec<OutboxEffect> {
    twin.post(OutboxInput::Committed(committed_at(1, 11, &["moments"])))
}

fn close(twin: &mut Twin) -> Vec<OutboxEffect> {
    take_fx(&twin.log, None);
    twin.det.close();
    twin.run();
    take_fx(&twin.log, state_of(&twin.det))
}

#[test]
fn out_local_write() {
    let rows = || vec![RowValue { row_id: "w".into(), values: "{}".into() }];
    let cases = [
        (LocalWriteKind::SetLocal, StoreOp::LocalRows { model: "Moment".into(), rows: rows() }),
        (LocalWriteKind::SetEphemeral { expire_after: None }, StoreOp::EphemeralRows { model: "Moment".into(), rows: rows() }),
        (LocalWriteKind::Detach, StoreOp::DetachFile { model: "Moment".into(), row_id: "w".into(), field: "{}".into() }),
    ];
    for (kind, expected) in cases {
        let mut twin = Twin::new();
        twin.det.tick(Time(100));
        let effects = twin.post(write(kind));
        assert_eq!((kinds(&effects), answers(&effects, WRITE)), (vec![CommitKind::LocalWrite], Vec::<OutboxAnswer>::new()));
        assert_eq!(twin.stores.len(), 1);
        assert_eq!(twin.store_ops, vec![expected], "the batch carries the write of the row");
    }
}

#[test]
fn out_local_write_stale() {
    let mut twin = Twin::new();
    twin.det.tick(Time(100));
    let input = OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH + 1, write: LocalWriteKind::SetLocal, row: "w".into(), values: "{}".into(), reply: WRITE };
    let effects = twin.post(input);
    assert_eq!((answers(&effects, WRITE), kinds(&effects), effects.len()), (vec![OutboxAnswer::Err(Fault::NoSession)], Vec::<CommitKind>::new(), 1));
}

/// P-OUT-11: a stale expiry leaves the row of a later write.
#[test]
fn out_prop_expire_last_write() {
    let mut twin = Twin::new();
    twin.det.tick(Time(100));
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
    assert_eq!(sends(&attempt), 0, "no Mutate before the attempt is written");
    let refused = twin.rejected(id, super::outbox_harness::RReason::Storage);
    assert_eq!(sends(&refused), 0, "a refused write of the attempt starts no request");
    let again = twin.post(OutboxInput::RetryDue { op_id: id, cycle_id: 1 });
    assert_eq!(sends(&again), 0);
    let sent = twin.committed(id);
    assert_eq!(sends(&sent), 1);
}

/// P-OUT-16: a discard gives one answer and, for a transfer, one outcome after the rollback.
#[test]
fn out_prop_discard_outcome_once() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    let id = twin.submit("r", Policy::Rollback, true);
    twin.committed(id);
    let first = twin.post(discard(None, 801));
    let second = twin.post(discard(None, 802));
    assert!(answers(&first, 801).is_empty() && answers(&second, 802).is_empty(), "the answers wait for the rollback");
    let done = twin.committed(id);
    let outcomes = done.iter().filter(|effect| matches!(effect, OutboxEffect::TransferOutcome { outcome: OutboxOutcome::Discarded, .. })).count();
    assert_eq!((answers(&done, 801), answers(&done, 802), outcomes), (vec![OutboxAnswer::Ok], vec![OutboxAnswer::Ok], 1));
}

/// A write of the values a memory row already holds still commits and renews the expiry: the earlier timer finds a newer write and leaves the row.
#[test]
fn out_local_write_memory_same() {
    let mut twin = Twin::new();
    twin.det.tick(Time(100));
    let first = twin.post(write(LocalWriteKind::SetEphemeral { expire_after: Some(10) }));
    assert_eq!(kinds(&first), vec![CommitKind::LocalWrite]);
    commit_first(&mut twin);
    twin.det.tick(Time(105));
    let again = twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::SetEphemeral { expire_after: Some(10) }, row: "w".into(), values: "{}".into(), reply: WRITE + 1 });
    assert_eq!(kinds(&again), vec![CommitKind::LocalWrite], "the same values commit again: the write renews the expiry");
    assert_eq!(twin.stores.len(), 1);
    assert_eq!(twin.store_ops, vec![StoreOp::EphemeralRows { model: "Moment".into(), rows: vec![RowValue { row_id: "w".into(), values: "{}".into() }] }]);
    let committed = twin.post(OutboxInput::Committed(committed_at(2, 12, &["moments"])));
    assert_eq!(answers(&committed, WRITE + 1), vec![OutboxAnswer::Written { revision: 12, shown: None, files_removed: 0 }]);
    assert!(committed.iter().any(|effect| matches!(effect, OutboxEffect::Schedule)), "the new timer: {committed:?}");
    twin.det.tick(Time(110));
    twin.run();
    let stale = take_fx(&twin.log, None);
    assert!(stale.is_empty(), "the timer of the first write does not remove the row: {stale:?}");
    twin.det.tick(Time(115));
    twin.run();
    let due = take_fx(&twin.log, None);
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
        assert_eq!(has_op!(twin, StoreOp::StaleFamilies { .. }), marks, "{:?}", twin.store_ops);
        twin.committed(id);
        let announced = stale_marks(&twin.history()) > 0;
        assert_eq!(announced, marks, "the windows turn stale only for a declared refetch");
    }
}

fn store_outcome(twin: &mut Twin, batch: u64, outcome: Option<RejectReason>) -> Vec<OutboxEffect> {
    take_fx(&twin.log, None);
    let input = match outcome {
        None => OutboxInput::Committed(committed_at(batch, 11, &["moments"])),
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

/// P-OUT-35: a local write against the Store outcome of its batch, `Closing`, `EpochEnded` in every order gets one answer and one batch.
#[test]
fn out_prop_local_write_orders() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Event { Write, Outcome, Closing, EpochEnded }
    use Event::*;
    let outcomes: [Vec<(u64, Option<RejectReason>)>; 2] = [vec![(1, None)], vec![(1, Some(RejectReason::NoSession))]];
    let orders: Vec<_> = crate::test_wait::permutations(&[Write, Outcome, Closing, EpochEnded]).into_iter().filter(|order| order.iter().position(|e| *e == Write) < order.iter().position(|e| *e == Outcome)).collect();
    assert_eq!(orders.len(), 12);
    for outcome in outcomes {
        for order in &orders {
            let mut twin = Twin::new();
            let mut effects = Vec::new();
            for event in order {
                effects.extend(match event {
                    Write => { twin.det.tick(Time(100)); twin.post(write(LocalWriteKind::SetLocal)) }
                    Outcome => outcome.iter().flat_map(|(batch, result)| store_outcome(&mut twin, *batch, result.clone())).collect(),
                    Closing => close(&mut twin),
                    EpochEnded => end_epoch(&mut twin),
                });
            }
            let label = format!("{order:?} x {outcome:?}");
            let closed_first = order.iter().position(|e| *e == Closing) < order.iter().position(|e| *e == Write);
            let ended_first = order.iter().position(|e| *e == EpochEnded) < order.iter().position(|e| *e == Write);
            let want: Vec<OutboxAnswer> = if ended_first { Vec::new() } else if closed_first { vec![OutboxAnswer::Err(Fault::Ended(crate::ffi::EndCause::Closed))] } else { vec![outcome.last().and_then(|(_, result)| result.clone()).map_or(OutboxAnswer::Written { revision: 11, shown: None, files_removed: 0 }, |reason| OutboxAnswer::Err(crate::spec::support::refusal_of(reason)))] };
            assert_eq!(answers(&effects, WRITE), want, "{label}: exactly one answer, none from the owner for a write of an ended epoch (the runtime answers the caller NoSession)");
            assert_eq!(kinds(&effects).len(), if closed_first || ended_first { 0 } else { 1 }, "{label}: one batch per attempt of the Store, none after Closing");
            assert_eq!(drained(&effects), 1, "{label}: Drained once");
        }
    }
}

#[test]
fn out_local_write_committed() {
    let mut twin = Twin::new();
    twin.det.tick(Time(100));
    twin.post(write(LocalWriteKind::SetLocal));
    let committed = commit_first(&mut twin);
    assert_eq!(answers(&committed, WRITE), vec![OutboxAnswer::Written { revision: 11, shown: None, files_removed: 0 }]);
    assert!(!committed.iter().any(|effect| matches!(effect, OutboxEffect::Schedule)), "a plain local write schedules no expiry: {committed:?}");
    let mut ephemeral = Twin::new();
    ephemeral.det.tick(Time(100));
    ephemeral.post(write(LocalWriteKind::SetEphemeral { expire_after: Some(10) }));
    let committed = commit_first(&mut ephemeral);
    assert_eq!(answers(&committed, WRITE), vec![OutboxAnswer::Written { revision: 11, shown: None, files_removed: 0 }]);
    assert!(committed.iter().any(|effect| matches!(effect, OutboxEffect::Schedule)), "an ephemeral write schedules the expiry: {committed:?}");
}

#[test]
fn out_local_write_rejected() {
    for reason in [RejectReason::NoSession, RejectReason::Closed, RejectReason::Storage("disk".into()), RejectReason::Invalid(String::new())] {
        let mut twin = Twin::new();
        twin.det.tick(Time(100));
        twin.post(write(LocalWriteKind::SetLocal));
        let effects = twin.post(OutboxInput::Rejected(Rejected { batch: 1, reason: reason.clone() }));
        assert_eq!(answers(&effects, WRITE), vec![OutboxAnswer::Err(crate::spec::support::refusal_of(reason))], "the local write is refused with the reason");
    }
}

#[test]
fn out_ephemeral_reset_by_position_batch() {
    let mut twin = Twin::new();
    let reset = crate::owners::store::EphemeralReset { model: "Moment".into(), row_id: "w".into(), field: "typing".into(), position: 42, deadline: 100 };
    twin.post(OutboxInput::ArmReset { reset: reset.clone() });
    twin.post(OutboxInput::ResetField { reset });
    assert_eq!(twin.stores.len(), 1);
    assert_eq!(twin.batch_ops(), vec![StoreOp::ResetEphemeral { model: "Moment".into(), row_id: "w".into(), field: "typing".into(), position: 42 }]);
}

#[test]
fn out_ephemeral_expire_batch() {
    let mut twin = Twin::new();
    twin.post(OutboxInput::ArmExpiry { model: "Moment".into(), row: "w".into(), revision: 42, deadline: 100 });
    twin.post(OutboxInput::ExpireRow { model: "Moment".into(), row: "w".into(), revision: 42 });
    assert_eq!(twin.stores.len(), 1);
    assert_eq!(twin.batch_ops(), vec![StoreOp::ExpireRow { model: "Moment".into(), row_id: "w".into(), write_revision: 42 }]);
}
