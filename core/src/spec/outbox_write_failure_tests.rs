//! Owner-level tests of the `outbox` machine: a refused write after the answer of the server (R14, R17), the saved answer at the opening and the retry wait cut by the network (R18).

use super::outbox_cells_tests::*;
use super::outbox_harness::*;
use super::outbox_harness::{Policy, RWhy, St};
use super::outbox_restore_tests::{ready, restore};
use crate::owners::contract::{Online, RejectReason};
use crate::owners::session::StoredAnswer;
use crate::owners::store::StoreOp;
use crate::owners::write::outbox::{CommitKind, OpId, OutboxAnswer, OutboxInput, OutboxOutcome, StoredOp};

fn landing(twin: &mut Twin) -> OpId {
    let id = sending(twin, Policy::Rollback, false);
    twin.waiter(id);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    id
}

fn rolling_back(twin: &mut Twin) -> OpId {
    let id = sending(twin, Policy::Rollback, false);
    twin.waiter(id);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
    id
}

fn outcomes(effects: &[OutboxEffect]) -> Vec<OutboxOutcome> {
    answers(effects, WAITER).into_iter().filter_map(|answer| match answer { OutboxAnswer::Outcome(outcome) => Some(outcome), _ => None }).collect()
}

#[test]
fn out_invalid_refusal_of_the_server_outcome_batch_writes_it_again_without_rows() {
    for landing_stage in [true, false] {
        let mut twin = Twin::new();
        let id = if landing_stage { landing(&mut twin) } else { rolling_back(&mut twin) };
        let at = twin.det.trace().len();
        let refused = refuse(&mut twin, id, RejectReason::Invalid("bad row".into()));
        assert_eq!(sends(&refused), 0, "landing={landing_stage}");
        assert!(!twin.det.trace()[at..].iter().any(|entry| entry.effects.contains("Stale")), "landing={landing_stage}: no Stale inside the refused batch");
        let rows_or_rollback = if landing_stage { has_op!(twin, StoreOp::Rows { model, .. } if model == "Moment") } else { has_op!(twin, StoreOp::RollBack { .. }) };
        assert!(has_op!(twin, StoreOp::RemoveIntent { .. }) && has_op!(twin, StoreOp::RemoveAnswer { .. }) && !rows_or_rollback, "landing={landing_stage}: {:?}", twin.store_ops);
        assert!(outcomes(&refused).is_empty(), "no outcome before Committed: {refused:?}");
        let at = twin.det.trace().len();
        let done = twin.committed(id);
        let want = if landing_stage { matches!(outcomes(&done).as_slice(), [OutboxOutcome::Confirmed { .. }]) } else { matches!(outcomes(&done).as_slice(), [OutboxOutcome::Rejected { .. }]) };
        assert!(want, "landing={landing_stage}: {done:?}");
        assert_eq!(twin.det.trace()[at..].iter().filter(|entry| entry.effects.contains("Stale")).count(), 1, "landing={landing_stage}: the windows of the answer models are marked stale after Committed");
    }
}

/// D-341 (2): a `create` answer with a swap whose batch is refused as Invalid: the batch has no rows and no swap of the row, the alias temp -> real is written, the open operations and the next operation of the row address the real id.
#[test]
fn out_landing_invalid_create_alias_written() {
    let mut twin = Twin::new();
    let created = sending(&mut twin, Policy::Rollback, false);
    let follower = twin.submit("r", Policy::Rollback, false);
    twin.committed(follower);
    twin.waiter(created);
    twin.post(OutboxInput::Response { op_id: created, rows: server_rows(), swaps: vec![("r".into(), "R".into())], stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    assert!(has_op!(twin, StoreOp::Swap { .. }), "the first landing batch swaps: {:?}", twin.store_ops);
    let refused = refuse(&mut twin, created, RejectReason::Invalid("bad row".into()));
    assert!(!has_op!(twin, StoreOp::Swap { .. }) && !has_op!(twin, StoreOp::Rows { model, .. } if model == "Moment") && has_op!(twin, StoreOp::RemoveIntent { .. }), "the batch without rows has no swap: {:?}", twin.store_ops);
    assert_eq!((twin.stage(created), refused.iter().filter(|effect| matches!(effect, OutboxEffect::Store { .. })).count()), (Some(St::Landing), 1));
    twin.committed(created);
    assert!(!has_op!(twin, StoreOp::Rows { rows, .. } if rows.iter().any(|row| row.row_id == "R")), "no row with the real id is written: {:?}", twin.store_ops);
    assert_eq!(twin.state().ops[&follower].op.row_id, "R", "the alias is written: the open operation of the row goes with the real id");
    let next = twin.submit("r", Policy::Rollback, false);
    assert_eq!(twin.state().ops[&next].op.row_id, "R", "the next operation of the row goes with the real id");
}

/// D-341 (1): a repeated Invalid of the batch without rows ends the operation as Failed, the waiters get the answer.
#[test]
fn out_invalid_without_rows_repeated_fails() {
    for landing_stage in [true, false] {
        let mut twin = Twin::new();
        let id = if landing_stage { landing(&mut twin) } else { rolling_back(&mut twin) };
        refuse(&mut twin, id, RejectReason::Invalid("bad row".into()));
        let again = refuse(&mut twin, id, RejectReason::Invalid("bad row".into()));
        assert_eq!((twin.stage(id), kinds(&again), sends(&again)), (Some(St::MarkingFailed), vec![CommitKind::ErrorMark], 0), "landing={landing_stage}: {again:?}");
        assert!(twin.state().ops[&id].error.as_deref().is_some_and(|error| error.contains("Internal(Invalid): bad row")), "landing={landing_stage}: {:?}", twin.state().ops[&id].error);
        let done = twin.committed(id);
        assert!(matches!(outcomes(&done).as_slice(), [OutboxOutcome::Failed { error }] if error.contains("Internal(Invalid): bad row")), "landing={landing_stage}: the waiter gets Failed: {done:?}");
        assert_eq!(twin.stage(id), Some(St::Failed), "landing={landing_stage}");
    }
}

#[test]
fn out_prop_write_storage_keeps_server_outcome() {
    for landing_stage in [true, false] {
        for refusals in 1..=5 {
            let mut twin = Twin::new();
            let (id, stage) = if landing_stage { (landing(&mut twin), St::Landing) } else { (rolling_back(&mut twin), St::RollingBack(RWhy::Rejected)) };
            let mut pauses = Vec::new();
            for _ in 0..refusals {
                let refused = refuse(&mut twin, id, RejectReason::Storage("disk".into()));
                assert_eq!((twin.stage(id), timers(&refused), sends(&refused)), (Some(stage), 1, 0), "landing={landing_stage} refusals={refusals}");
                pauses.extend(twin.pauses.clone());
                let again = tick(&mut twin, id);
                assert_eq!((twin.stage(id), sends(&again), again.len()), (Some(stage), 0, 1), "the same write goes again: {again:?}");
            }
            assert_eq!(pauses.len(), refusals, "one pause per refusal");
            assert!(pauses.windows(2).all(|pair| pair[0] <= pair[1]), "the pause grows with the count of refusals (P-DATA-102): {pauses:?}");
            if refusals > 2 { assert!(pauses[refusals - 1] > pauses[0], "the pause grows: {pauses:?}"); }
            let done = twin.committed(id);
            let outcome = outcomes(&done);
            if landing_stage { assert!(matches!(outcome.as_slice(), [OutboxOutcome::Confirmed { .. }]), "landing refusals={refusals}: {done:?}"); }
            else { assert!(matches!(outcome.as_slice(), [OutboxOutcome::Rejected { .. }]), "rollback refusals={refusals}: {done:?}"); }
        }
    }
}

/// P-DATA-102, D-336 (7): any outcome of the write except `Storage` resets the count of refusals: the next `Storage` pause starts from the first again.
#[test]
fn out_prop_write_storage_count_reset_by_other_outcome() {
    for landing_stage in [true, false] {
        let mut twin = Twin::new();
        let (id, stage) = if landing_stage { (landing(&mut twin), St::Landing) } else { (rolling_back(&mut twin), St::RollingBack(RWhy::Rejected)) };
        let mut first = Vec::new();
        for round in 0..3 {
            refuse(&mut twin, id, RejectReason::Storage("disk".into()));
            if round == 0 { first = twin.pauses.clone(); }
            tick(&mut twin, id);
        }
        refuse(&mut twin, id, RejectReason::Storage("disk".into()));
        assert!(twin.pauses[0] > first[0], "landing={landing_stage}: the pause grew with the count: {:?} against {first:?}", twin.pauses);
        tick(&mut twin, id);
        refuse(&mut twin, id, RejectReason::Invalid("bad row".into()));
        assert_eq!(twin.stage(id), Some(stage), "the server outcome stays: the batch without rows is written");
        refuse(&mut twin, id, RejectReason::Storage("disk".into()));
        assert_eq!(twin.pauses, first, "landing={landing_stage}: an outcome other than Storage reset the count");
    }
}

#[test]
fn out_prop_landing_ended_answer_lands_at_open() {
    for reason in [RejectReason::NoSession, RejectReason::Closed] {
        for storage_first in 0..=3 {
            let mut twin = Twin::new();
            let id = landing(&mut twin);
            let request_id = twin.state().ops[&id].request_id;
            for _ in 0..storage_first {
                refuse(&mut twin, id, RejectReason::Storage("disk".into()));
                tick(&mut twin, id);
            }
            let ended = refuse(&mut twin, id, reason.clone());
            assert_eq!(twin.stage(id), Some(St::Done), "{reason:?}");
            let cause = if reason == RejectReason::Closed { "Closed" } else { "SessionEnded" };
            assert!(sends(&ended) == 0 && format!("{:?}", outcomes(&ended)) == format!("[Ended({cause})]"), "the unit of work ends with Ended, not Failed or Confirmed: {ended:?}");
            assert!(kinds(&ended).is_empty() && !has_op!(twin, StoreOp::RemoveAnswer { .. } | StoreOp::RemoveIntent { .. } | StoreOp::MarkError { .. }), "Ended writes nothing to the Store: {ended:?} {:?}", twin.store_ops);
            let saved = twin.state().ops[&id].clone();
            let mut reopened = Twin::bare();
            declare(&mut reopened);
            let item = StoredOp { answered: Some((request_id, StoredAnswer::Ok("{}".into()))), op: saved.op.clone(), attempts: saved.attempts, cycle_id: saved.cycle_id, ..stored(id, "r") };
            let opened = restore(&mut reopened, vec![item]);
            let readied = ready(&mut reopened);
            assert_eq!(reopened.stage(id), Some(St::Landing), "{reason:?}");
            assert_eq!(sends(&opened) + sends(&readied), 0, "no second request for the saved answer");
            reopened.waiter(id);
            let landed = reopened.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
            assert_eq!((kinds(&landed), sends(&landed)), (vec![CommitKind::Land], 0), "the rows of the saved answer land in one batch: {landed:?}");
            let done = reopened.committed(id);
            assert!(matches!(outcomes(&done).as_slice(), [OutboxOutcome::Confirmed { .. }]), "exactly one Confirmed after the reopening: {done:?}");
            assert_eq!(sends(&done), 0, "no Send after Committed");
        }
    }
}

#[test]
fn out_sending_retry_offline_requeues() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
    let before = twin.det.trace().len();
    let effects = twin.post(OutboxInput::Online(Online(false)));
    assert_eq!((twin.stage(id), kinds(&effects), sends(&effects), twin.state().ops[&id].attempts), (Some(St::Queued), vec![CommitKind::Requeue], 0, 1));
    let cancelled = twin.det.trace()[before..].iter().filter(|entry| entry.effects.contains("Cancel")).count();
    assert_eq!(cancelled, 1, "the timer of the wait is removed");
    let stale = tick(&mut twin, id);
    assert_eq!((twin.stage(id), sends(&stale), kinds(&stale)), (Some(St::Queued), 0, Vec::new()), "a late Tick changes nothing");
}

#[test]
fn out_prop_retry_wait_survives_offline() {
    for drops in 1..=4 {
        let mut twin = Twin::new();
        let id = sending(&mut twin, Policy::Rollback, false);
        twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
        for _ in 0..drops { twin.post(OutboxInput::Online(Online(false))); }
        assert_eq!((twin.stage(id), twin.state().ops[&id].attempts), (Some(St::Queued), 1), "drops={drops}");
        let back = twin.post(OutboxInput::Online(Online(true)));
        assert_eq!((twin.stage(id), kinds(&back), sends(&back), timers(&back)), (Some(St::PersistingAttempt), vec![CommitKind::Attempt], 0, 0), "drops={drops}: no pause after the network is back");
        let written = twin.committed(id);
        assert_eq!((twin.stage(id), sends(&written), twin.state().ops[&id].attempts), (Some(St::Sending), 1, 2), "drops={drops}");
        assert_eq!(sends(&twin.post(OutboxInput::Online(Online(true)))), 0, "one request per attempt");
    }
}
