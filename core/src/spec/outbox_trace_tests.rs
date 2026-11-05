//! Reference-level tests of the `outbox` machine: the traces against the model and the properties through the real database.

use super::ref_outbox::{step_of, Ctx, In, Out, Policy, St, Step};
use super::rig::{bounded, step, until, within, Kind, Reply, Rig, Server};
use crate::ffi::{WireOperationOutcome, WireOperationReceipt};
use crate::schema::OnFailure;
use super::outbox_tests::*;

struct Op { text: String, state: St, requesting: bool, resolved: Option<Out>, receipt: WireOperationReceipt, handle: tokio::task::JoinHandle<WireOperationOutcome> }

struct Model { ops: Vec<Op>, online: bool }

#[derive(Clone, Copy, Debug)]
enum Act { Create, Off, On, Ok, Refuse, Discard, Close }

const ALPHABET: [Act; 7] = [Act::Create, Act::Off, Act::On, Act::Ok, Act::Refuse, Act::Discard, Act::Close];

fn ctx_of(model: &Model) -> Ctx { Ctx { online: model.online, ..Ctx::ready() } }

fn model_apply(model: &mut Model, act: Act) {
    let ctx = ctx_of(model);
    match act {
        Act::On | Act::Off => {
            model.online = matches!(act, Act::On);
            let ctx = ctx_of(model);
            if !model.online {
                if let Some(in_flight) = model.ops.iter_mut().find(|op| op.state == St::Sending && op.requesting) {
                    in_flight.state = St::Queued;
                    in_flight.requesting = false;
                }
            }
            for op in model.ops.iter_mut().filter(|op| matches!(op.state, St::Queued | St::Sending)) {
                let step = want(op.state, &ctx, In::Online(model.online));
                op.requesting = match (op.state, step.next) {
                    (St::Sending, St::Sending) => op.requesting,
                    (_, St::Sending) => true,
                    _ => false,
                };
                op.state = step.next;
            }
        }
        Act::Ok | Act::Refuse => {
            let Some(op) = model.ops.iter_mut().find(|op| op.requesting) else { return };
            op.requesting = false;
            if matches!(act, Act::Ok) {
                op.state = want(want(op.state, &ctx, In::Response).next, &ctx, In::Committed).next;
                op.resolved = Some(Out::Confirmed);
            } else {
                let rolling = want(op.state, &Ctx { policy: Policy::Rollback, ..ctx }, In::TransportError { temporary: false });
                op.state = want(rolling.next, &ctx, In::Committed).next;
                op.resolved = Some(Out::Rejected);
            }
        }
        Act::Discard => {
            let Some(op) = model.ops.first_mut() else { return };
            let step = want_or(op.state, &ctx, In::Discard);
            if step.next != op.state {
                op.state = want(step.next, &ctx, In::Committed).next;
                op.resolved = Some(Out::Discarded);
            }
        }
        Act::Close => {
            for op in model.ops.iter_mut().filter(|op| op.resolved.is_none()) {
                let step = want(op.state, &ctx, In::Closing);
                op.state = step.next;
                op.requesting = false;
                op.resolved = Some(Out::Interrupted);
            }
        }
        Act::Create => unreachable!(),
    }
}

fn want_or(state: St, ctx: &Ctx, input: In) -> Step { step_of(state, ctx, input).unwrap_or(Step { next: state, obs: vec![], eff: vec![] }) }

#[derive(Clone, Copy, PartialEq)]
enum Check { Outcomes, RollbackClean, Idle }

async fn run_trace(trace: &[Act], check: Check) {
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let mut model = Model { ops: vec![], online: true };
    let label = format!("{trace:?}");
    for (index, act) in trace.iter().enumerate() {
        step(&format!("{label} step {index} {act:?}"));
        match act {
            Act::Create => {
                let ctx = ctx_of(&model);
                let submitted = want(St::None, &ctx, In::Submit);
                let committed = want(submitted.next, &ctx, In::Committed);
                let receipt = rig.create(&format!("t{index}")).await;
                let db = rig.db.clone();
                let asked = receipt.clone();
                let handle = tokio::spawn(async move { db.outcome(asked).await.unwrap() });
                model.ops.push(Op { text: format!("t{index}"), state: committed.next, requesting: sends(&committed), resolved: None, receipt, handle });
            }
            Act::On => { rig.db.set_online(true).unwrap(); model_apply(&mut model, *act); }
            Act::Off => { rig.db.set_online(false).unwrap(); model_apply(&mut model, *act); }
            Act::Ok | Act::Refuse if !model.online => {}
            Act::Ok | Act::Refuse => {
                if let Some(op) = model.ops.iter().find(|op| op.requesting) {
                    until(&label, || rig.server.holds_text(&op.text)).await;
                    rig.server.answer_text(&op.text, if matches!(act, Act::Ok) { Reply::Ok } else { Reply::Refuse });
                }
                model_apply(&mut model, *act);
            }
            Act::Discard => {
                if let Some(first) = model.ops.first() {
                    let id = first.receipt.optimistic_value.clone().unwrap().id;
                    let _ = within(rig.db.discard(rig.epoch(), "Moment".into(), id, None)).await;
                }
                model_apply(&mut model, *act);
            }
            Act::Close => { within(rig.db.close_database()).await.unwrap(); model_apply(&mut model, *act); }
        }
        let requesting = if model.online { model.ops.iter().filter(|op| op.requesting).count().min(1) } else { 0 };
        until(&format!("{label} step {index}: requests in flight {requesting}"), || rig.server.held(Kind::Mutate) == requesting).await;
        let expected: Vec<_> = model.ops.iter().map(|op| op.resolved.is_some()).collect();
        until(&format!("{label} step {index}: resolved {expected:?}"), || model.ops.iter().all(|op| op.resolved.is_none() || op.handle.is_finished())).await;
        rig.quiet().await;
        for (number, op) in model.ops.iter().enumerate() {
            assert_eq!(op.handle.is_finished(), op.resolved.is_some(), "{label} step {index}: op {number} {:?}", op.state);
            if let (Some(want_out), true) = (op.resolved, op.handle.is_finished()) {
                let outcome = within(rig.db.outcome(op.receipt.clone())).await.unwrap();
                assert_eq!(kind_of(&outcome), wire_of(want_out), "{label} step {index}: op {number}");
                if check == Check::RollbackClean && !matches!(act, Act::Close) && matches!(want_out, Out::Rejected | Out::Discarded) {
                    assert!(rig.row(&op.receipt.optimistic_value.clone().unwrap().id).await.is_none(), "{label}: op {number} left its optimism");
                }
            }
        }
        if matches!(act, Act::Close) { return; }
    }
    if check == Check::Idle {
        let before = rig.server.mutations();
        rig.clock.advance(3_600_000);
        rig.quiet().await;
        assert_eq!(rig.server.mutations(), before, "{label}: requests without an event");
    }
}

fn traces() -> Vec<Vec<Act>> {
    let mut all = vec![];
    let mut seed: u64 = 17;
    for code in (0..ALPHABET.len().pow(4)).step_by(53) {
        all.push((0..4).map(|place| ALPHABET[(code / ALPHABET.len().pow(place as u32)) % ALPHABET.len()]).collect::<Vec<_>>());
    }
    for _ in 0..6 {
        let mut trace = vec![];
        for _ in 0..8 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            trace.push(ALPHABET[((seed >> 33) as usize) % (ALPHABET.len() - 1)]);
        }
        all.push(trace);
    }
    all
}

async fn run_all(check: Check) {
    let traces: Vec<Vec<Act>> = traces().into_iter().map(|trace| trace.into_iter().flat_map(|act| if matches!(act, Act::Create) { vec![act] } else { vec![Act::Create, act] }).take(8).collect()).collect();
    for chunk in traces.chunks(16) {
        step(&format!("{} traces start", chunk.len()));
        let running: Vec<_> = chunk.iter().cloned().map(|trace| tokio::spawn(async move { run_trace(&trace, check).await })).collect();
        for trace in running { within(trace).await.unwrap(); }
    }
}

#[test]
fn out_prop_outcome_once() { bounded("out_prop_outcome_once", out_prop_outcome_once_run) }

async fn out_prop_outcome_once_run() { run_all(Check::Outcomes).await }

#[test]
fn out_prop_rollback_clean() { bounded("out_prop_rollback_clean", out_prop_rollback_clean_run) }

async fn out_prop_rollback_clean_run() { run_all(Check::RollbackClean).await }

#[test]
fn out_prop_idle() { bounded("out_prop_idle", out_prop_idle_run) }

async fn out_prop_idle_run() { run_all(Check::Idle).await }

#[test]
fn out_prop_row_order() { bounded("out_prop_row_order", out_prop_row_order_run) }

async fn out_prop_row_order_run() {
    for first_reply in [Reply::Ok, Reply::Refuse] {
        step(&format!("row order {first_reply:?}"));
        let rig = Rig::open(OnFailure::Rollback).await;
        rig.server.hold(Kind::Mutate, true);
        let first = rig.create("a").await;
        rig.server.wait_held(Kind::Mutate, 1).await;
        let id = temp_id(&first);
        let second = rig.update(&id, "b").await.unwrap();
        let third = rig.update(&id, "c").await.unwrap();
        rig.quiet().await;
        assert_eq!(rig.server.mutations(), 1, "only the first operation of a row is in the network");

        rig.server.answer(Kind::Mutate, first_reply);
        for receipt in [&first, &second, &third] {
            rig.server.hold(Kind::Mutate, false);
            tokio::time::timeout(std::time::Duration::from_secs(5), rig.db.outcome(receipt.clone())).await.unwrap_or_else(|_| panic!("the outcome of {:?} did not come after {first_reply:?}", receipt.operation)).unwrap();
        }

        assert!(!rig.server.overlapped(), "two operations of one row were in the network together");
        let updates: Vec<_> = rig.server.seen().iter().filter(|seen| seen.operation == "UpdateMoment").map(|seen| seen.variables["text"].as_str().unwrap_or("").to_string()).collect();
        assert!(updates.is_empty() || updates == ["b", "c"], "{updates:?}");
    }
}

#[test]
fn out_prop_intent_kept() { bounded("out_prop_intent_kept", out_prop_intent_kept_run) }

async fn out_prop_intent_kept_run() {
    for acts in [vec![Act::Off, Act::Create, Act::Create], vec![Act::Create, Act::Off, Act::Create, Act::On]] {
        step(&format!("intent kept {acts:?}"));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db.sqlite").to_string_lossy().to_string();
        let first = Rig::open_at(OnFailure::Rollback, Some(path.clone()), Server::new()).await;
        first.server.hold(Kind::Mutate, true);
        let mut model = Model { ops: vec![], online: true };
        for (index, act) in acts.iter().enumerate() {
            match act {
                Act::Off => { first.db.set_online(false).unwrap(); model_apply(&mut model, *act); }
                Act::On => { first.db.set_online(true).unwrap(); model_apply(&mut model, *act); }
                _ => {
                    let ctx = ctx_of(&model);
                    let committed = want(want(St::None, &ctx, In::Submit).next, &ctx, In::Committed);
                    let receipt = first.create(&format!("t{index}")).await;
                    let db = first.db.clone();
                    let asked = receipt.clone();
                    model.ops.push(Op { text: format!("t{index}"), state: committed.next, requesting: sends(&committed), resolved: None, receipt, handle: tokio::spawn(async move { db.outcome(asked).await.unwrap() }) });
                }
            }
        }
        first.quiet().await;
        let survivors = model.ops.iter().filter(|op| matches!(op.state, St::Queued | St::Sending)).count();

        within(first.db.close_database()).await.unwrap();
        drop(first);
        let server = Server::new();
        let second = Rig::open_at(OnFailure::Rollback, Some(path), server.clone()).await;
        crate::test_wait::yield_until("the restored intents reached the server", || server.count("MakeMoment") >= survivors).await;
        second.quiet().await;

        assert_eq!(server.count("MakeMoment"), survivors, "{acts:?}");
        drop(second);
    }
}
