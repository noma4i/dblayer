//! Owner-level tests of the `outbox` machine: the ends of the operations (`Closing`, `EpochEnded`), the terminal `Done` and `Failed`, and the ignored live `EpochStarted`. Names are the `Test` column.

use super::outbox_cells_tests::{sending, transfer_outcomes, unchanged_by};
use super::outbox_harness::*;
use super::outbox_harness::{In, Policy, RWhy, St};
use crate::ffi::EndCause;
use crate::owners::contract::{Committed, EpochStarted, Online, Rejected, RejectReason};
use crate::owners::write::outbox::{OutboxInput, OutboxOutcome};

fn drained(effects: &[OutboxEffect]) -> usize { effects.iter().filter(|effect| matches!(effect, OutboxEffect::Drained)).count() }

fn cancels(effects: &[OutboxEffect]) -> usize { effects.iter().filter(|effect| matches!(effect, OutboxEffect::CancelRequest)).count() }

fn transfer() -> Ctx { Ctx { transfer: true, ..ready() } }

#[test]
fn out_closing() {
    let cause = EndCause::Closed;
    for state in [St::Submitting, St::PersistingAttempt, St::Sending, St::Landing, St::RollingBack(RWhy::Rejected), St::MarkingFailed, St::MarkingRetry] {
        let origin_transfer = state != St::MarkingRetry;
        let ctx = Ctx { transfer: origin_transfer, ..ready() };
        let (mut twin, id) = reach(state, &ctx);
        let effects = input_of(&mut twin, id, &ctx, In::Closing);
        assert_eq!((twin.stage(id), twin.state().ops[&id].outcome.clone(), drained(&effects), cancels(&effects)), (Some(St::Done), Some(OutboxOutcome::Ended(cause.clone())), 1, usize::from(state == St::Sending)), "{state:?}: Closing ends the operation Closed, Drained once, the request is cancelled only when in flight");
        let want = if origin_transfer { vec![OutboxOutcome::Ended(cause.clone())] } else { Vec::new() };
        assert_eq!(transfer_outcomes(&effects), want, "{state:?}: a transfer operation gets the Ended outcome");
    }
}

#[test]
fn out_closing_queued() {
    let cause = EndCause::Closed;
    for state in [St::WaitingDeclaration, St::Queued, St::Failed] {
        let ctx = if state == St::Queued { Ctx { online: false, ..transfer() } } else { transfer() };
        let (mut twin, id) = reach(state, &ctx);
        let effects = input_of(&mut twin, id, &ctx, In::Closing);
        assert_eq!((twin.stage(id), twin.state().ops[&id].outcome.clone(), transfer_outcomes(&effects), drained(&effects), cancels(&effects)), (Some(St::Done), Some(OutboxOutcome::Ended(cause.clone())), vec![OutboxOutcome::Ended(cause.clone())], 1, 0), "{state:?}: Closing ends the waiting operation Closed, Drained once, no request to cancel");
    }
}

#[test]
fn out_epoch_ended() {
    let cause = EndCause::SessionEnded;
    for state in [St::WaitingDeclaration, St::Submitting, St::Queued, St::PersistingAttempt, St::Sending, St::Landing, St::RollingBack(RWhy::Rejected), St::MarkingFailed, St::MarkingRetry, St::Failed] {
        let origin_transfer = state != St::MarkingRetry;
        let ctx = Ctx { online: state != St::Queued, transfer: origin_transfer, ..ready() };
        let (mut twin, id) = reach(state, &ctx);
        let effects = input_of(&mut twin, id, &ctx, In::EpochEnded(End::SessionEnded));
        assert_eq!((twin.stage(id), twin.state().ops[&id].outcome.clone(), cancels(&effects)), (Some(St::Done), Some(OutboxOutcome::Ended(cause.clone())), usize::from(state == St::Sending)), "{state:?}: EpochEnded ends the operation SessionEnded, the request is cancelled only when in flight");
        let want = if origin_transfer { vec![OutboxOutcome::Ended(cause.clone())] } else { Vec::new() };
        assert_eq!(transfer_outcomes(&effects), want, "{state:?}: a transfer operation gets the Ended outcome");
    }
}

#[test]
fn out_epoch_ended_close() {
    for state in [St::Submitting, St::Queued, St::Sending, St::Landing, St::MarkingRetry] {
        let ctx = Ctx { online: state != St::Queued, transfer: state != St::MarkingRetry, ..ready() };
        let (mut twin, id) = reach(state, &ctx);
        let effects = input_of(&mut twin, id, &ctx, In::EpochEnded(End::Closed));
        assert_eq!((twin.stage(id), twin.state().ops[&id].outcome.clone()), (Some(St::Done), Some(OutboxOutcome::Ended(EndCause::Closed))), "{state:?}: an epoch ended by close ends the operation Closed, not SessionEnded");
        let want = if state != St::MarkingRetry { vec![OutboxOutcome::Ended(EndCause::Closed)] } else { Vec::new() };
        assert_eq!(transfer_outcomes(&effects), want, "{state:?}: the transfer outcome carries Closed");
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
        OutboxInput::Response { op_id: id + 100, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None },
        OutboxInput::TransportError { op_id: id + 100, temporary: true },
        OutboxInput::Dropped { op_id: id + 100 },
        OutboxInput::RetryDue { op_id: id + 100, cycle_id: 1 },
    ]);
}

#[test]
fn out_failed_other() {
    let (mut twin, id) = reach(St::Failed, &keep());
    unchanged_by(&mut twin, id, St::Failed, vec![
        OutboxInput::Online(Online(true)),
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
