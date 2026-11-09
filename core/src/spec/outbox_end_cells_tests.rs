//! Owner-level tests of the `outbox` machine: the ends of the operations (`Closing`, `EpochEnded`), the terminal `Done` and `Failed`, and the ignored live `EpochStarted`. Names are the `Test` column.

use super::outbox_cells_tests::{answers, sending, transfer_outcomes, unchanged_by};
use super::outbox_harness::*;
use super::outbox_harness::{In, Policy, RWhy, St};
use crate::ffi::EndCause;
use crate::owners::contract::{Committed, EpochStarted, ModelsReset, Online, Rejected, RejectReason};
use crate::owners::write::outbox::{OutboxAnswer, OutboxInput, OutboxOutcome};
use crate::runtime::Time;

fn drained(effects: &[OutboxEffect]) -> usize { effects.iter().filter(|effect| matches!(effect, OutboxEffect::Drained)).count() }

fn cancels(effects: &[OutboxEffect]) -> usize { effects.iter().filter(|effect| matches!(effect, OutboxEffect::CancelRequest)).count() }

fn transfer() -> Ctx { Ctx { transfer: true, ..ready() } }

#[test]
fn out_closing() {
    let cause = EndCause::Closed;
    for state in [St::Submitting, St::PersistingAttempt, St::Sending, St::Landing, St::RollingBack(RWhy::Rejected), St::MarkingFailed, St::MarkingRetry] {
        let (mut twin, id) = reach(state, &transfer());
        let effects = input_of(&mut twin, id, &transfer(), In::Closing);
        assert_eq!((twin.stage(id), answers(&effects, WAITER), transfer_outcomes(&effects), drained(&effects)), (Some(St::Done), vec![OutboxAnswer::Outcome(OutboxOutcome::Ended(cause.clone()))], vec![OutboxOutcome::Ended(cause.clone())], 1), "{state:?}: Closing ends the operation Closed to the waiter and Transfer, Drained once");
        assert_eq!(cancels(&effects), usize::from(state == St::Sending), "{state:?}: the request is cancelled only when it is in flight");
    }
}

#[test]
fn out_closing_queued() {
    let cause = EndCause::Closed;
    for state in [St::WaitingDeclaration, St::Queued, St::Failed] {
        let ctx = if state == St::Queued { Ctx { online: false, ..transfer() } } else { transfer() };
        let (mut twin, id) = reach(state, &ctx);
        let effects = input_of(&mut twin, id, &ctx, In::Closing);
        assert_eq!((twin.stage(id), answers(&effects, WAITER), transfer_outcomes(&effects), drained(&effects), cancels(&effects)), (Some(St::Done), vec![OutboxAnswer::Outcome(OutboxOutcome::Ended(cause.clone()))], vec![OutboxOutcome::Ended(cause.clone())], 1, 0), "{state:?}: Closing ends the waiting operation Closed, Drained once, no request to cancel");
    }
}

#[test]
fn out_epoch_ended() {
    let cause = EndCause::SessionEnded;
    for state in [St::WaitingDeclaration, St::Submitting, St::Queued, St::PersistingAttempt, St::Sending, St::Landing, St::RollingBack(RWhy::Rejected), St::MarkingFailed, St::MarkingRetry, St::Failed] {
        let ctx = if state == St::Queued { Ctx { online: false, ..transfer() } } else { transfer() };
        let (mut twin, id) = reach(state, &ctx);
        let effects = input_of(&mut twin, id, &ctx, In::EpochEnded);
        assert_eq!((twin.stage(id), answers(&effects, WAITER), transfer_outcomes(&effects)), (Some(St::Done), vec![OutboxAnswer::Outcome(OutboxOutcome::Ended(cause.clone()))], vec![OutboxOutcome::Ended(cause.clone())]), "{state:?}: EpochEnded ends the operation SessionEnded to the waiter and Transfer");
        assert_eq!(cancels(&effects), usize::from(state == St::Sending), "{state:?}: the request is cancelled only when it is in flight");
    }
}

#[test]
fn out_epoch_started_live_ignored() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    let effects = twin.post(OutboxInput::EpochStarted(EpochStarted { epoch: EPOCH, identity: Some(9), generation: 3 }));
    assert_eq!((twin.stage(id), effects.is_empty()), (Some(St::Sending), true), "a second EpochStarted of the live epoch tells nobody and leaves the operation unchanged");
}

#[test]
fn out_done() {
    let (mut twin, id) = reach(St::Done, &ready());
    unchanged_by(&mut twin, id, St::Done, vec![
        OutboxInput::Online(Online(true)),
        OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 2, models: vec!["Moment".into()] }),
        OutboxInput::Response { op_id: id + 100, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None },
        OutboxInput::TransportError { op_id: id + 100, temporary: true },
        OutboxInput::Dropped { op_id: id + 100 },
        OutboxInput::RetryDue { op_id: id + 100, cycle_id: 1 },
        OutboxInput::Tick(Time(50)),
    ]);
}

#[test]
fn out_failed_other() {
    let (mut twin, id) = reach(St::Failed, &keep());
    unchanged_by(&mut twin, id, St::Failed, vec![
        OutboxInput::Online(Online(true)),
        OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 2, models: vec!["Chat".into()] }),
        OutboxInput::Response { op_id: id, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None },
        OutboxInput::TransportError { op_id: id, temporary: true },
        OutboxInput::Dropped { op_id: id },
        OutboxInput::RetryDue { op_id: id, cycle_id: 1 },
        OutboxInput::Committed(Committed { batch: 999, revision: 50, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }),
        OutboxInput::Rejected(Rejected { batch: 999, reason: RejectReason::Storage("disk".into()) }),
    ]);
}

#[test]
fn out_server_removed_row_other() {
    let (mut twin, id) = reach(St::Done, &ready());
    let effects = twin.post(OutboxInput::RowRemoved { model: "Moment".into(), row_id: "r".into(), revision: 20 });
    assert_eq!((twin.stage(id), effects.is_empty()), (Some(St::Done), true), "RowRemoved of a done operation changes nothing");
}
