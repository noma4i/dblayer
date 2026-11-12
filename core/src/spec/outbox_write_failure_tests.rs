//! Owner-level tests of the `outbox` machine: a refused write after the answer of the server (R14, R17), the saved answer at the opening and the retry wait cut by the network (R18).

use super::outbox_cells_tests::*;
use super::outbox_harness::*;
use super::outbox_harness::{Policy, RWhy, St};
use super::outbox_restore_tests::{declare, ready, restore, stored};
use crate::owners::contract::{Online, RejectReason};
use crate::owners::session::StoredAnswer;
use crate::owners::write::outbox::{CommitKind, OpId, OutboxAnswer, OutboxInput, OutboxOutcome, StoredOp};
use crate::spec::support::{snapshot, Emitted};

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
fn out_landing_invalid() {
    let mut twin = Twin::new();
    let id = landing(&mut twin);
    let refused = refuse(&mut twin, id, RejectReason::Invalid("bad row".into()));
    assert_eq!((twin.stage(id), sends(&refused)), (Some(St::Landing), 0));
    assert!(twin.store_text.contains("RemoveIntent") && twin.store_text.contains("RemoveAnswer") && !twin.store_text.contains("Rows { model: \"Moment\""), "{}", twin.store_text);
    assert!(outcomes(&refused).is_empty(), "no outcome before Committed: {refused:?}");
    let done = twin.committed(id);
    assert!(matches!(outcomes(&done).as_slice(), [OutboxOutcome::Confirmed { .. }]), "{done:?}");
    assert_eq!(twin.stage(id), Some(St::Done));
}

#[test]
fn out_rollback_invalid_server_outcome() {
    let mut twin = Twin::new();
    let id = rolling_back(&mut twin);
    assert!(twin.store_text.contains("RollBack {"), "{}", twin.store_text);
    let refused = refuse(&mut twin, id, RejectReason::Invalid("bad row".into()));
    assert_eq!((twin.stage(id), sends(&refused)), (Some(St::RollingBack(RWhy::Rejected)), 0));
    assert!(twin.store_text.contains("RemoveIntent") && twin.store_text.contains("RemoveAnswer") && !twin.store_text.contains("RollBack {"), "{}", twin.store_text);
    assert!(outcomes(&refused).is_empty(), "{refused:?}");
    let done = twin.committed(id);
    assert!(matches!(outcomes(&done).as_slice(), [OutboxOutcome::Rejected { .. }]), "{done:?}");
}

#[test]
fn out_prop_write_storage_keeps_server_outcome() {
    for landing_stage in [true, false] {
        for refusals in 1..=5 {
            let mut twin = Twin::new();
            let (id, stage) = if landing_stage { (landing(&mut twin), St::Landing) } else { (rolling_back(&mut twin), St::RollingBack(RWhy::Rejected)) };
            for _ in 0..refusals {
                let refused = refuse(&mut twin, id, RejectReason::Storage("disk".into()));
                assert_eq!((twin.stage(id), timers(&refused), sends(&refused)), (Some(stage), 1, 0), "landing={landing_stage} refusals={refusals}");
                let again = tick(&mut twin, id);
                assert_eq!((twin.stage(id), sends(&again), again.len()), (Some(stage), 0, 1), "the same write goes again: {again:?}");
            }
            let done = twin.committed(id);
            let outcome = outcomes(&done);
            assert!(matches!(outcome.as_slice(), [OutboxOutcome::Confirmed { .. }] | [OutboxOutcome::Rejected { .. }]), "landing={landing_stage} refusals={refusals}: {done:?}");
            assert_eq!(outcome[0] == OutboxOutcome::Discarded, false);
        }
    }
}

#[test]
fn out_retry_keeps_saved_answer() {
    for invalid_first in [false, true] {
        let mut twin = Twin::new();
        let id = landing(&mut twin);
        if invalid_first { refuse(&mut twin, id, RejectReason::Invalid("bad row".into())); }
        let retried = retry(&mut twin);
        assert_eq!((twin.stage(id), sends(&retried)), (Some(St::Landing), 0), "invalid_first={invalid_first}: {retried:?}");
        for _ in 0..3 {
            let refused = refuse(&mut twin, id, RejectReason::Storage("disk".into()));
            let again = tick(&mut twin, id);
            assert_eq!((sends(&refused), sends(&again), twin.stage(id)), (0, 0, Some(St::Landing)));
            assert!(retry(&mut twin).iter().all(|effect| !matches!(effect, OutboxEffect::Send { .. })));
        }
        let done = twin.committed(id);
        assert!(matches!(outcomes(&done).as_slice(), [OutboxOutcome::Confirmed { .. }]), "{done:?}");
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
            let mut reopened = Twin::bare();
            declare(&mut reopened);
            let item = StoredOp { answered: Some((request_id, StoredAnswer::Ok("{}".into()))), ..stored(id, "r") };
            let opened = restore(&mut reopened, vec![item]);
            let readied = ready(&mut reopened);
            assert_eq!(reopened.stage(id), Some(St::Landing), "{reason:?}");
            assert_eq!(sends(&opened) + sends(&readied), 0, "no second request for the saved answer");
        }
    }
}

#[test]
fn out_sending_retry_offline_requeues() {
    let mut twin = Twin::new();
    let id = sending(&mut twin, Policy::Rollback, false);
    twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
    let effects = twin.post(OutboxInput::Online(Online(false)));
    assert_eq!((twin.stage(id), kinds(&effects), sends(&effects), twin.state().ops[&id].attempts), (Some(St::Queued), vec![CommitKind::Requeue], 0, 1));
    let cancelled = snapshot(&twin.log).iter().filter(|emitted| matches!(emitted, Emitted::Cancel(_))).count();
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
