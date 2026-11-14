//! Owner-level tests of the `outbox` machine: the waits of the calls and the properties P-OUT-*.


use std::collections::BTreeMap;
use super::outbox_harness::{In, Out, Policy, St};
use crate::owners::contract::{Online};


use crate::spec::support::Log;
use crate::owners::write::outbox::{self as o, CommitKind, Fault, LocalWriteKind, OpId, Outbox, OutboxAnswer, OutboxInput, Stage};

use crate::runtime::{Address, Cause, DetScheduler, EpochStamp, Time};
use super::outbox_harness::*;
use crate::spec::support::committed_at;

/// The `Waits` table: every call gets one answer on every terminal row.
#[test]
fn out_wait_submit() {
    let (mut twin, _) = reach(St::Submitting, &ready());
    let ended = { take_fx(&twin.log, state_of(&twin.det)); twin.det.end_epoch(EPOCH, Cause::SessionReset); twin.run(); take_fx(&twin.log, state_of(&twin.det)) };
    assert_eq!(answers(&ended, SUBMIT_REPLY), [OutboxAnswer::Err(Fault::Ended(crate::ffi::EndCause::SessionEnded))]);
    let (mut twin, _) = reach(St::Submitting, &ready());
    take_fx(&twin.log, state_of(&twin.det));
    twin.det.close();
    twin.run();
    assert_eq!(answers(&take_fx(&twin.log, state_of(&twin.det)), SUBMIT_REPLY), [OutboxAnswer::Err(Fault::Ended(crate::ffi::EndCause::Closed))]);
}

#[test]
fn out_wait_outcome() {
    for (state, input, want) in [(St::Sending, In::EpochEnded(End::SessionEnded), Out::Ended(End::SessionEnded)), (St::Queued, In::Closing, Out::Ended(End::Closed))] {
        let ctx = if state == St::Queued { offline() } else { ready() };
        let (mut twin, id) = reach(state, &ctx);
        let effects = input_of(&mut twin, id, &ctx, input);
        let answers = answers(&effects, WAITER);
        assert_eq!(answers.len(), 1, "{state:?}: one answer");
        assert!(matches!(&answers[0], OutboxAnswer::Outcome(outcome) if out_of(outcome) == want), "{answers:?}");
    }
}

#[test]
fn out_wait_retry() {
    let (mut twin, _) = reach(St::Submitting, &ready());
    let pending = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply: 64 });
    assert!(answers(&pending, 64).is_empty());
    take_fx(&twin.log, state_of(&twin.det));
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    assert_eq!(answers(&take_fx(&twin.log, state_of(&twin.det)), 64), [OutboxAnswer::Err(Fault::Ended(crate::ffi::EndCause::SessionEnded))]);
}

#[test]
fn out_wait_discard() {
    let (mut twin, _) = reach(St::Queued, &offline());
    twin.post(discard(None, 74));
    take_fx(&twin.log, state_of(&twin.det));
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    assert_eq!(answers(&take_fx(&twin.log, state_of(&twin.det)), 74), [OutboxAnswer::Err(Fault::Ended(crate::ffi::EndCause::SessionEnded))]);
}

#[test]
fn out_wait_local_write() {
    let mut twin = Twin::new();
    let effects = twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::SetLocal, row: "w".into(), values: "{}".into(), reply: 81 });
    assert!(answers(&effects, 81).is_empty());
    let committed = twin.post(OutboxInput::Committed(committed_at(1, 11, &["moments"])));
    assert_eq!(answers(&committed, 81), [OutboxAnswer::Written { revision: 11, shown: None, files_removed: 0 }]);
    let mut twin = Twin::new();
    twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::SetLocal, row: "w".into(), values: "{}".into(), reply: 82 });
    take_fx(&twin.log, state_of(&twin.det));
    twin.det.close();
    twin.run();
    let closing = take_fx(&twin.log, state_of(&twin.det));
    assert!(answers(&closing, 82).is_empty() && !closing.iter().any(|effect| matches!(effect, OutboxEffect::Drained)), "the write waits for the Store: {closing:?}");
    let committed = twin.post(OutboxInput::Committed(committed_at(1, 11, &["moments"])));
    assert_eq!(answers(&committed, 82), [OutboxAnswer::Written { revision: 11, shown: None, files_removed: 0 }]);
    assert_eq!(committed.iter().filter(|effect| matches!(effect, OutboxEffect::Drained)).count(), 1, "Drained after the last answer");
}

#[test]
fn out_done_closing_without_operations() {
    let mut twin = Twin::new();
    take_fx(&twin.log, state_of(&twin.det));
    twin.det.close();
    twin.run();
    assert_eq!(take_fx(&twin.log, state_of(&twin.det)).iter().filter(|effect| matches!(effect, OutboxEffect::Drained)).count(), 1);
}

// Properties: the explorer walks the orders of the inputs of three sources.

use crate::spec::support::{Source, SourceInput, SourceState};
use crate::runtime::{explore, Limits};
use std::cell::RefCell;

thread_local! { static LOG: RefCell<Option<Log<Seen>>> = const { RefCell::new(None) }; }

fn scripted(sources: Vec<Vec<OutboxInput>>) -> DetScheduler {
    let (mut det, log, _) = world(3);
    LOG.with(|cell| *cell.borrow_mut() = Some(log.clone()));
    let addr = Address::<Outbox>::new(OUTBOX_AT);
    let mut feeders = Vec::new();
    for script in sources {
        feeders.push(det.add::<Source<Outbox>>("source", SourceState { to: addr, script }, crate::spec::support::Silent));
    }
    det.start_epoch(EPOCH);
    for feeder in feeders {
        det.post(feeder, EpochStamp::Control, SourceInput::Go);
    }
    det
}

fn api_script(rows: &[&str]) -> Vec<OutboxInput> {
    rows.iter().enumerate().map(|(index, row)| OutboxInput::Submit { epoch: EPOCH, op: model_op(row, Policy::Rollback, false), reply: 100 + index as u64 }).collect()
}

fn net_script() -> Vec<OutboxInput> {
    vec![
        response_of(1),
        OutboxInput::TransportError { op_id: 2, temporary: false },
        OutboxInput::Dropped { op_id: 2 },
        OutboxInput::Online(Online(false)),
        OutboxInput::Online(Online(true)),
    ]
}

fn store_script() -> Vec<OutboxInput> {
    (1..=4).map(|batch| OutboxInput::Committed(committed_at(batch, 10 + batch as i64, &["moments"]))).collect()
}

fn effects_now() -> Vec<OutboxEffect> { LOG.with(|cell| cell.borrow().as_ref().map(|log| crate::spec::support::snapshot(log).into_iter().filter_map(|emitted| lower(emitted, None)).collect()).unwrap_or_default()) }

fn walk(sources: impl Fn() -> Vec<Vec<OutboxInput>>, check: impl FnMut(&DetScheduler) -> Result<(), String>) {
    let report = explore(|| scripted(sources()), Limits { depth: 6, max_traces: 4_000 }, check);
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
    assert!(report.traces > 0);
}

#[test]
fn out_prop_outcome_once() {
    walk(|| vec![api_script(&["r", "r"]), net_script(), store_script()], |_| {
        let mut seen = BTreeMap::new();
        for effect in effects_now() {
            if let OutboxEffect::Reply { reply, answer: OutboxAnswer::Outcome(_) } = effect { *seen.entry(reply).or_insert(0) += 1; }
        }
        match seen.values().find(|count| **count > 1) { Some(count) => Err(format!("an outcome came {count} times")), None => Ok(()) }
    });
}

#[test]
fn out_prop_no_send_after_end() {
    let mut det = scripted(vec![api_script(&["r", "q"]), store_script()]);
    det.run_until_quiet(10_000).unwrap();
    crate::spec::support::take(&LOG.with(|cell| cell.borrow().clone().unwrap()));
    det.end_epoch(EPOCH, Cause::SessionReset);
    det.run_until_quiet(10_000).unwrap();
    det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), OutboxInput::Online(Online(true)));
    det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), OutboxInput::RetryDue { op_id: 1, cycle_id: 1 });
    det.run_until_quiet(10_000).unwrap();
    let after = effects_now();
    assert!(after.iter().all(|effect| !matches!(effect, OutboxEffect::Send { .. } | OutboxEffect::Store { .. })), "{after:?}");
}

#[test]
fn out_prop_row_order() {
    walk(|| vec![api_script(&["r", "r", "r"]), net_script(), store_script()], |det| {
        let state = state_of(det).ok_or("no state")?;
        let sending: Vec<_> = state.ops.values().filter(|op| op.op.row_id == "r" && matches!(op.stage, Stage::Sending)).collect();
        if sending.len() > 1 { return Err("two operations of one row in the network".into()); }
        let busy: Vec<OpId> = state.ops.values().filter(|op| matches!(op.stage, Stage::Sending | Stage::Landing | Stage::RollingBack(_) | Stage::Failed | Stage::Submitting)).map(|op| op.op_id).collect();
        match (busy.first(), state.ops.values().filter(|op| op.stage == Stage::Sending).map(|op| op.op_id).min()) {
            (Some(first), Some(sending)) if sending > *first => Err("a later operation went out before an earlier one".into()),
            _ => Ok(()),
        }
    });
}

#[test]
fn out_prop_intent_kept() {
    walk(|| vec![api_script(&["r", "q"]), net_script(), store_script()], |det| {
        let state = state_of(det).ok_or("no state")?;
        let done_without_outcome = state.ops.values().find(|op| op.stage == Stage::Done && op.outcome.is_none());
        match done_without_outcome { Some(op) => Err(format!("operation {} is Done without an outcome", op.op_id)), None => Ok(()) }
    });
}

#[test]
fn out_prop_idle() {
    let mut det = scripted(vec![api_script(&["r"]), store_script()]);
    det.run_until_quiet(10_000).unwrap();
    det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), response_of(1));
    det.run_until_quiet(10_000).unwrap();
    det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), OutboxInput::Committed(committed_at(2, 20, &[])));
    det.run_until_quiet(10_000).unwrap();
    let before = det.trace().len();
    det.tick(Time(u64::MAX / 2));
    assert!(det.run_until_quiet(100).is_ok());
    assert_eq!(det.trace().len(), before, "no operation waits: a tick wakes nothing");
}

#[test]
fn out_prop_confirmed_lands() {
    walk(|| vec![api_script(&["r"]), net_script(), store_script(), vec![OutboxInput::Online(Online(true))]], |_| {
        let effects = effects_now();
        let lands = effects.iter().filter(|effect| matches!(effect, OutboxEffect::Store { kind: CommitKind::Land, .. })).count();
        let confirmed = effects.iter().filter(|effect| matches!(effect, OutboxEffect::Reply { answer: OutboxAnswer::Outcome(o::OutboxOutcome::Confirmed { .. }), .. })).count();
        if confirmed > lands { Err("Confirmed without a landed batch".into()) } else { Ok(()) }
    });
}

#[test]
fn out_prop_rollback_clean() {
    walk(|| vec![api_script(&["r"]), net_script(), store_script(), vec![OutboxInput::SelectedOutbox { model: "Moment".into(), row_id: "r".into(), action: "update".into(), kind: o::SelectKind::Discard, reply: 200, version: 0 }]], |_| {
        let effects = effects_now();
        let rollbacks = effects.iter().filter(|effect| matches!(effect, OutboxEffect::Store { kind: CommitKind::Rollback, .. })).count();
        let ended = effects.iter().filter(|effect| matches!(effect, OutboxEffect::Reply { answer: OutboxAnswer::Outcome(o::OutboxOutcome::Rejected { .. } | o::OutboxOutcome::Discarded), .. })).count();
        if ended > rollbacks { Err("Rejected or Discarded without a rollback batch".into()) } else { Ok(()) }
    });
}

#[test]
fn out_prop_dropped_resent() {
    let (mut twin, id) = reach(St::Sending, &ready());
    let first = twin.first_pair[&id];
    let mut sends = 1;
    for _ in 0..60 {
        twin.post(OutboxInput::Dropped { op_id: id });
        for effect in twin.post(OutboxInput::RetryDue { op_id: id, cycle_id: 1 }) {
            if let OutboxEffect::Send { request_id, sequence, .. } = effect {
                assert_eq!((request_id, sequence), first, "the same pair on every Send");
                sends += 1;
            }
        }
    }
    assert!(sends <= 40, "{sends} Send");
    assert_ne!(twin.stage(id), Some(St::Sending), "the attempts run out");
    assert!(twin.state().ops[&id].outcome.as_ref().is_none_or(|outcome| !matches!(outcome, o::OutboxOutcome::Ended(_))));
}

/// boundary_selector_orders (TRACE-02): `Retry` and `Discard` with an action address only the operation of that action.
#[test]
fn boundary_selector_orders() {
    let (mut twin, _) = reach(St::Sending, &ready());
    let other = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: Some("destroy".into()), reply: 91 });
    assert_eq!(answers(&other, 91), [OutboxAnswer::Err(Fault::NotFound)]);
    let other = twin.post(discard(Some("destroy"), 92));
    assert_eq!(answers(&other, 92), [OutboxAnswer::Err(Fault::NotFound)]);
    let own = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: 93 });
    assert!(matches!(answers(&own, 93).as_slice(), [OutboxAnswer::Receipt(_)]));
}
