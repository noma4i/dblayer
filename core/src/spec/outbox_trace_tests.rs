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
enum Check { Outcomes, RollbackClean, Idle, StatusColumn, Operations }

async fn run_trace(trace: &[Act], check: Check) {
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let mut model = Model { ops: vec![], online: true };
    let label = format!("{trace:?}");
    let queue = (check == Check::Operations).then(|| rig.db.subscribe_values(rig.epoch(), crate::ffi::ReadRequest::Operations));
    let mut seen: Vec<(i64, String)> = Vec::new();
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
        if let (Some(queue), false) = (&queue, matches!(act, Act::Close)) {
            let stored = rig.db.op_statuses();
            while seen != stored {
                let Some(crate::ffi::ReadValue::Operations { items, .. }) = within(queue.pull()).await.unwrap() else { panic!("{label} step {index}: the stream gave no queue value") };
                seen = items.iter().map(|item| (item.seq, format!("{:?}", item.status).to_lowercase())).collect();
            }
        }
        if check == Check::StatusColumn && !matches!(act, Act::Close) {
            let reached: Vec<String> = rig.server.seen().iter().filter_map(|seen| seen.variables["text"].as_str().map(str::to_string)).collect();
            let expected: Vec<&str> = model.ops.iter().filter(|op| op.resolved.is_none()).map(|op| if reached.contains(&op.text) { "sending" } else { "pending" }).collect();
            let stored: Vec<String> = rig.db.op_statuses().into_iter().map(|(_, status)| status).collect();
            assert_eq!(stored, expected, "{label} step {index}: the status column");
        }
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

/// P-OUT-33: the column `op.status` of an operation is `pending` until its first request is written and `sending` after it: the server saw the request of every `sending` operation and of no `pending` one.
#[test]
fn out_prop_status_column() { bounded("out_prop_status_column", out_prop_status_column_run) }

async fn out_prop_status_column_run() { run_all(Check::StatusColumn).await }

/// P-DLV-24: every value of the queue stream is the rows of `op` at its revision, and the stream reaches the stored rows after every step.
#[test]
fn dlv_prop_operations_mirror() { bounded("dlv_prop_operations_mirror", dlv_prop_operations_mirror_run) }

async fn dlv_prop_operations_mirror_run() { run_all(Check::Operations).await }

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

#[test]
fn out_prop_offline_queue() { bounded("out_prop_offline_queue", out_prop_offline_queue_run) }

async fn out_prop_offline_queue_run() {
    for (action, operation) in [("create", "MakeMoment"), ("update", "UpdateMoment"), ("destroy", "DeleteMoment")] {
        step(&format!("offline queue {action}"));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db.sqlite").to_string_lossy().to_string();
        let first = Rig::open_at(OnFailure::Rollback, Some(path.clone()), Server::new()).await;
        let seed = first.create("seed").await;
        assert_eq!(kind_of(&first.outcome(&seed).await), "Confirmed");
        let landed = first.update(&temp_id(&seed), "landed").await.unwrap();
        assert_eq!(kind_of(&first.outcome(&landed).await), "Confirmed");
        let real = temp_id(&landed);
        let before = first.server.mutations();
        first.db.set_online(false).unwrap();
        let receipt = match action {
            "create" => first.create("queued").await,
            "update" => first.update(&real, "edited").await.unwrap(),
            _ => within(first.db.destroy(first.epoch(), "Moment".into(), real.clone())).await.unwrap(),
        };
        first.quiet().await;
        assert_eq!(first.server.mutations(), before, "{action}: nothing leaves while the network is away");
        let target = if action == "create" { temp_id(&receipt) } else { real.clone() };
        let visible = |row: Option<crate::ffi::WireRow>| match (action, row) {
            ("destroy", row) => row.is_none(),
            ("update", Some(row)) => format!("{:?}", row.cells).contains("edited"),
            (_, row) => row.is_some(),
        };
        assert!(visible(first.row(&target).await), "{action}: the optimism is visible before the close");
        within(first.db.close_database()).await.unwrap();
        drop(first);

        let server = Server::new();
        server.hold(Kind::Mutate, true);
        let second = Rig::open_at(OnFailure::Rollback, Some(path), server.clone()).await;
        server.wait_held(Kind::Mutate, 1).await;
        assert!(visible(second.row(&target).await), "{action}: the optimism is visible after the reopen");
        server.hold(Kind::Mutate, false);
        server.answer(Kind::Mutate, Reply::Ok);
        second.quiet().await;
        assert_eq!((server.count(operation), server.mutations()), (1, 1), "{action}: sent once on the network that returned, no call of the platform");
        drop(second);
    }
}

#[test]
fn out_queued_drain_limit_api() { bounded("out_queued_drain_limit_api", out_queued_drain_limit_api_run) }

async fn out_queued_drain_limit_api_run() {
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    rig.db.set_online(false).unwrap();
    let mut receipts = Vec::new();
    for number in 0..5 { receipts.push(rig.create(&format!("m{number}")).await); }
    rig.quiet().await;
    assert_eq!(rig.server.mutations(), 0, "nothing leaves while the network is away");
    rig.db.set_online(true).unwrap();
    for sent in 1..=5 {
        rig.server.wait_held(Kind::Mutate, 1).await;
        assert_eq!(rig.server.mutations(), sent, "Session puts one request in the network at a time, the queue gives it the next");
        rig.server.answer(Kind::Mutate, Reply::Ok);
    }
    let texts: Vec<_> = rig.server.seen().iter().filter(|seen| seen.operation == "MakeMoment").map(|seen| seen.variables["text"].as_str().unwrap_or("").to_string()).collect();
    assert_eq!(texts, ["m0", "m1", "m2", "m3", "m4"], "the requests reach the transport in the order of the writes");
    rig.server.hold(Kind::Mutate, false);
    for receipt in &receipts {
        assert_eq!(kind_of(&rig.outcome(receipt).await), "Confirmed");
    }
    assert_eq!(rig.server.count("MakeMoment"), 5);
}

async fn queue_until(queue: &crate::ffi::ReadStream, wanted: impl Fn(&[crate::ffi::OperationInfo]) -> bool) -> Vec<crate::ffi::OperationInfo> {
    loop {
        let Some(crate::ffi::ReadValue::Operations { items, .. }) = within(queue.pull()).await.unwrap() else { panic!("the queue stream ended") };
        if wanted(&items) { return items; }
    }
}

/// P-DLV-24 through the database: an offline `update` is `pending`, goes `sending`, a refusal with `on_failure: keep` leaves it `failed` with its error, `retry` by the fields of the info sends it again and it leaves the queue.
#[test]
fn p_dlv_24_the_queue_stream_follows_an_update_through_refusal_and_retry() { bounded("p_dlv_24_the_queue_stream_follows_an_update_through_refusal_and_retry", p_dlv_24_run) }

async fn p_dlv_24_run() {
    use crate::ffi::OperationQueueStatus::{Failed, Pending, Sending};
    let rig = Rig::open(OnFailure::Keep).await;
    let seed = rig.create("seed").await;
    assert_eq!(kind_of(&rig.outcome(&seed).await), "Confirmed");
    let landed = rig.update(&temp_id(&seed), "landed").await.unwrap();
    assert_eq!(kind_of(&rig.outcome(&landed).await), "Confirmed");
    let real = temp_id(&landed);
    let queue = rig.db.subscribe_values(rig.epoch(), crate::ffi::ReadRequest::Operations);
    rig.db.set_online(false).unwrap();
    rig.update(&real, "edited").await.unwrap();
    let queued = queue_until(&queue, |items| items.len() == 1).await;
    assert_eq!((queued[0].status, queued[0].action.as_str(), queued[0].model.as_str(), queued[0].row_id.as_str(), queued[0].attempts), (Pending, "update", "Moment", real.as_str(), 0));
    rig.server.hold(Kind::Mutate, true);
    rig.db.set_online(true).unwrap();
    rig.server.wait_held(Kind::Mutate, 1).await;
    assert_eq!(queue_until(&queue, |items| items.len() == 1 && items[0].status == Sending).await[0].seq, queued[0].seq);
    rig.server.answer(Kind::Mutate, Reply::Refuse);
    let failed = queue_until(&queue, |items| items.iter().any(|item| item.status == Failed)).await;
    assert!(failed[0].error.is_some() && failed[0].seq == queued[0].seq, "{failed:?}");
    let before = rig.server.count("UpdateMoment");
    rig.server.hold(Kind::Mutate, true);
    within(rig.db.retry(rig.epoch(), failed[0].model.clone(), failed[0].row_id.clone(), Some(failed[0].action.clone()), None)).await.unwrap();
    rig.server.wait_held(Kind::Mutate, 1).await;
    rig.server.answer(Kind::Mutate, Reply::Ok);
    queue_until(&queue, |items| items.is_empty()).await;
    assert_eq!(rig.server.count("UpdateMoment"), before + 1, "the retry sent the same operation again");
}
