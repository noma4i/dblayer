//! Owner-level tests of the `outbox` machine: the waits of the calls and the properties P-OUT-*.


use std::collections::BTreeMap;
use super::ref_outbox::{In, Out, Policy, Reason as RReason, St};
use crate::owners::contract::{Committed, ModelsReset, Online, RejectReason, ReplyId};


use crate::spec::support::{Emitted, Log};
use crate::owners::write::outbox::{self as o, CommitKind, Fault, LocalWriteKind, OpId, Outbox, OutboxAnswer, OutboxEffect as Run, OutboxInput, Stage};

use crate::runtime::{Address, Cause, DetScheduler, EpochStamp, Time};
use super::outbox_harness::*;

fn replies_of(effects: &[OutboxEffect], reply: ReplyId) -> Vec<OutboxAnswer> {
    effects.iter().filter_map(|effect| match effect {
        OutboxEffect::Reply { reply: id, answer } if *id == reply => Some(answer.clone()),
        _ => None,
    }).collect()
}

/// The `Waits` table: every call gets one answer on every terminal row.
#[test]
fn out_wait_submit() {
    let (mut twin, id) = reach(St::Submitting, &ready());
    let ended = { take_fx(&twin.log, state_of(&twin.det)); twin.det.end_epoch(EPOCH, Cause::SessionReset); twin.run(); take_fx(&twin.log, state_of(&twin.det)) };
    assert_eq!(replies_of(&ended, SUBMIT_REPLY), [OutboxAnswer::Err(Fault::Interrupted)]);
    let (mut twin, id2) = reach(St::Submitting, &ready());
    let committed = twin.committed(id2);
    assert!(matches!(replies_of(&committed, SUBMIT_REPLY).as_slice(), [OutboxAnswer::Receipt(_)]));
    let (mut twin, id3) = reach(St::Submitting, &ready());
    let rejected = twin.rejected(id3, RReason::NoSession);
    assert_eq!(replies_of(&rejected, SUBMIT_REPLY), [OutboxAnswer::Err(Fault::Rejected(RejectReason::NoSession))]);
    let (mut twin, _) = reach(St::Submitting, &ready());
    take_fx(&twin.log, state_of(&twin.det));
    twin.det.close();
    twin.run();
    assert_eq!(replies_of(&take_fx(&twin.log, state_of(&twin.det)), SUBMIT_REPLY), [OutboxAnswer::Err(Fault::Interrupted)]);
    let _ = id;
}

#[test]
fn out_wait_outcome() {
    for (state, input, want) in [(St::Landing, In::Committed, Out::Confirmed), (St::Sending, In::EpochEnded, Out::Interrupted), (St::Queued, In::Closing, Out::Interrupted)] {
        let ctx = if state == St::Queued { offline() } else { ready() };
        let (mut twin, id) = reach(state, &ctx);
        let effects = input_of(&mut twin, id, &ctx, input);
        let answers = replies_of(&effects, WAITER);
        assert_eq!(answers.len(), 1, "{state:?}: one answer");
        assert!(matches!(&answers[0], OutboxAnswer::Outcome(outcome) if out_of(outcome) == want), "{answers:?}");
    }
    let (mut twin, id) = reach(St::Sending, &ready());
    let effects = twin.post(OutboxInput::Outcome { receipt: twin.receipt(id, EPOCH + 1), reply: 77 });
    assert_eq!(replies_of(&effects, 77), [OutboxAnswer::Err(Fault::NoSession)]);
}

#[test]
fn out_wait_retry() {
    let (mut twin, id) = reach(St::Failed, &keep());
    let before = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply: 61 });
    assert!(replies_of(&before, 61).is_empty(), "the receipt waits for the batch of the new cycle");
    let ok = twin.committed(id);
    assert!(matches!(replies_of(&ok, 61).as_slice(), [OutboxAnswer::Receipt(_)]));
    let (mut twin, _) = reach(St::Done, &ready());
    let none = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply: 62 });
    assert_eq!(replies_of(&none, 62), [OutboxAnswer::Err(Fault::NotFound)]);
    let (mut twin, _) = reach(St::Sending, &ready());
    let busy = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply: 63 });
    assert!(matches!(replies_of(&busy, 63).as_slice(), [OutboxAnswer::Receipt(_)]));
    let (mut twin, _) = reach(St::Submitting, &ready());
    let pending = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply: 64 });
    assert!(replies_of(&pending, 64).is_empty());
    take_fx(&twin.log, state_of(&twin.det));
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    assert_eq!(replies_of(&take_fx(&twin.log, state_of(&twin.det)), 64), [OutboxAnswer::Err(Fault::Interrupted)]);
}

#[test]
fn out_wait_discard() {
    let (mut twin, id) = reach(St::Queued, &offline());
    let queued = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: 71 });
    assert!(replies_of(&queued, 71).is_empty(), "no answer before Committed");
    let committed = twin.committed(id);
    assert_eq!(replies_of(&committed, 71), [OutboxAnswer::Ok]);
    let (mut twin, _) = reach(St::Sending, &ready());
    let in_flight = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: 72 });
    assert_eq!(replies_of(&in_flight, 72), [OutboxAnswer::Err(Fault::InFlight)]);
    let (mut twin, _) = reach(St::Done, &ready());
    let none = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: 73 });
    assert_eq!(replies_of(&none, 73), [OutboxAnswer::Err(Fault::NotFound)]);
    let (mut twin, _) = reach(St::Queued, &offline());
    twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: 74 });
    take_fx(&twin.log, state_of(&twin.det));
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    assert_eq!(replies_of(&take_fx(&twin.log, state_of(&twin.det)), 74), [OutboxAnswer::Err(Fault::Interrupted)]);
}

#[test]
fn out_wait_local_write() {
    let mut twin = Twin::new();
    let effects = twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::SetLocal, row: "w".into(), values: "{}".into(), reply: 81, now: Time(0) });
    assert!(replies_of(&effects, 81).is_empty());
    let committed = twin.post(OutboxInput::Committed(Committed { batch: 1, revision: 11, tables: vec!["moments".into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
    assert_eq!(replies_of(&committed, 81), [OutboxAnswer::Written { revision: 11, shown: None, files_removed: 0 }]);
    let mut twin = Twin::new();
    twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::Attach { source: None }, row: "w".into(), values: "{}".into(), reply: 82, now: Time(0) });
    take_fx(&twin.log, state_of(&twin.det));
    twin.det.close();
    twin.run();
    let closing = take_fx(&twin.log, state_of(&twin.det));
    assert!(replies_of(&closing, 82).is_empty() && !closing.iter().any(|effect| matches!(effect, OutboxEffect::Drained)), "the write waits for the Store: {closing:?}");
    let committed = twin.post(OutboxInput::Committed(Committed { batch: 1, revision: 11, tables: vec!["moments".into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
    assert_eq!(replies_of(&committed, 82), [OutboxAnswer::Written { revision: 11, shown: None, files_removed: 0 }]);
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

thread_local! { static LOG: RefCell<Option<Log<Emitted<Run>>>> = const { RefCell::new(None) }; }

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
        OutboxInput::Response { op_id: 1, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None },
        OutboxInput::TransportError { op_id: 2, temporary: false },
        OutboxInput::Dropped { op_id: 2 },
        OutboxInput::Online(Online(false)),
        OutboxInput::Online(Online(true)),
    ]
}

fn store_script() -> Vec<OutboxInput> {
    (1..=4).map(|batch| OutboxInput::Committed(Committed { batch, revision: 10 + batch as i64, tables: vec!["moments".into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 })).collect()
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
    det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), OutboxInput::Response { op_id: 1, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
    det.run_until_quiet(10_000).unwrap();
    det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), OutboxInput::Committed(Committed { batch: 2, revision: 20, tables: vec![], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
    det.run_until_quiet(10_000).unwrap();
    let before = det.trace().len();
    det.tick(Time(u64::MAX / 2));
    assert!(det.run_until_quiet(100).is_ok());
    assert_eq!(det.trace().len(), before, "no operation waits: a tick wakes nothing");
}

#[test]
fn out_prop_confirmed_lands() {
    walk(|| vec![api_script(&["r"]), net_script(), store_script(), vec![OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Moment".into()] })]], |_| {
        let effects = effects_now();
        let lands = effects.iter().filter(|effect| matches!(effect, OutboxEffect::Store { kind: CommitKind::Land, .. })).count();
        let confirmed = effects.iter().filter(|effect| matches!(effect, OutboxEffect::Reply { answer: OutboxAnswer::Outcome(o::OutboxOutcome::Confirmed { .. }), .. })).count();
        if confirmed > lands { Err("Confirmed without a landed batch".into()) } else { Ok(()) }
    });
}

#[test]
fn out_prop_rollback_clean() {
    walk(|| vec![api_script(&["r"]), net_script(), store_script(), vec![OutboxInput::SelectedOutbox { model: "Moment".into(), row_id: "r".into(), action: "update".into(), kind: o::SelectKind::Discard, reply: 200 }]], |_| {
        let effects = effects_now();
        let rollbacks = effects.iter().filter(|effect| matches!(effect, OutboxEffect::Store { kind: CommitKind::Rollback, .. })).count();
        let ended = effects.iter().filter(|effect| matches!(effect, OutboxEffect::Reply { answer: OutboxAnswer::Outcome(o::OutboxOutcome::Rejected { .. } | o::OutboxOutcome::Discarded), .. })).count();
        if ended > rollbacks { Err("Rejected or Discarded without a rollback batch".into()) } else { Ok(()) }
    });
}

#[test]
fn out_prop_membership_revision() {
    let (mut twin, id) = reach(St::Landing, &ready());
    let before = twin.state().ops[&id].op.clone();
    let effects = twin.post(OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Other".into()] }));
    assert!(effects.iter().all(|effect| !matches!(effect, OutboxEffect::Store { .. })), "a reset of another model touches no batch");
    assert_eq!(twin.state().ops[&id].op, before);
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
    assert!(twin.state().ops[&id].outcome.as_ref().is_none_or(|outcome| !matches!(outcome, o::OutboxOutcome::Interrupted)));
}

#[test]
fn out_prop_local_write_kept() {
    walk(|| vec![vec![OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::SetLocal, row: "w".into(), values: "{}".into(), reply: 300, now: Time(0) }], store_script(), vec![OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Moment".into()] })]], |_| {
        let answers = effects_now().iter().filter(|effect| matches!(effect, OutboxEffect::Reply { reply: 300, .. })).count();
        if answers > 1 { Err("a local write got two answers".into()) } else { Ok(()) }
    });
}

/// boundary_selector_orders (TRACE-02): `Retry` and `Discard` with an action address only the operation of that action.
#[test]
fn boundary_selector_orders() {
    let (mut twin, _) = reach(St::Sending, &ready());
    let other = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: Some("destroy".into()), reply: 91 });
    assert_eq!(replies_of(&other, 91), [OutboxAnswer::Err(Fault::NotFound)]);
    let other = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("destroy".into()), reply: 92 });
    assert_eq!(replies_of(&other, 92), [OutboxAnswer::Err(Fault::NotFound)]);
    let own = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: 93 });
    assert!(matches!(replies_of(&own, 93).as_slice(), [OutboxAnswer::Receipt(_)]));
}
