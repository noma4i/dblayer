use super::ref_outbox::{step_of, Ctx, Eff, In, Obs, Out, Policy, St, Step};
use super::rig::{quiet, until, within, Kind, Reply, Rig, Server};
use crate::ffi::{CommandRequest, WireOperationOutcome, WireOperationReceipt, WireSyncState};
use crate::schema::OnFailure;

fn want(state: St, ctx: &Ctx, input: In) -> Step { step_of(state, ctx, input).unwrap_or_else(|| panic!("no table cell for {state:?} x {input:?}")) }

fn offline() -> Ctx { Ctx { online: false, ..Ctx::ready() } }

fn sends(step: &Step) -> bool { step.eff.contains(&Eff::Send) }

fn wire_of(out: Out) -> &'static str {
    match out {
        Out::Confirmed => "Confirmed",
        Out::Rejected | Out::Failed => "Rejected",
        Out::Interrupted | Out::Discarded => "Interrupted",
    }
}

fn kind_of(outcome: &WireOperationOutcome) -> &'static str {
    match outcome {
        WireOperationOutcome::Confirmed { .. } => "Confirmed",
        WireOperationOutcome::Rejected { .. } => "Rejected",
        WireOperationOutcome::Interrupted => "Interrupted",
        WireOperationOutcome::DeliveryUnknown => "DeliveryUnknown",
    }
}

fn temp_id(receipt: &WireOperationReceipt) -> String { receipt.optimistic_value.clone().expect("the receipt carries the optimistic row").id }

#[tokio::test(flavor = "multi_thread")]
async fn out_submit() {
    let submitted = want(St::None, &offline(), In::Submit);
    let committed = want(submitted.next, &offline(), In::Committed);
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.db.set_online(false).unwrap();

    let receipt = within(async { rig.create("hello").await }).await;

    assert_eq!(submitted.obs, []);
    assert!(committed.obs.contains(&Obs::Receipt) && receipt.operation.is_some());
    assert!(committed.obs.contains(&Obs::OptimismVisibleAtRevision) && rig.row(&temp_id(&receipt)).await.is_some());
    quiet().await;
    assert_eq!(sends(&committed), rig.server.mutations() > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn out_submit_stale() {
    let cell = want(St::None, &Ctx { epoch_current: false, ..Ctx::ready() }, In::Submit);
    let rig = Rig::open(OnFailure::Rollback).await;
    let request = CommandRequest { epoch: rig.epoch() + 1, model: "Moment".into(), values_json: r#"{"text":"x"}"#.into(), args_json: "{}".into(), upload: None };

    let result = within(rig.db.send(request)).await;

    assert_eq!(cell.obs, [Obs::Fails(super::ref_outbox::Fault::NoSession)]);
    assert!(matches!(result, Err(crate::ffi::DbError::NoSession { .. })), "{result:?}");
    assert_eq!(rig.server.mutations(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn out_submitting_committed() {
    let cell = want(St::Submitting, &Ctx::ready(), In::Committed);
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);

    let receipt = rig.create("hello").await;

    assert!(cell.obs.contains(&Obs::Receipt));
    assert_eq!(receipt.epoch, rig.epoch());
    assert!(receipt.operation.is_some() && receipt.revision > 0);
    assert!(cell.obs.contains(&Obs::OptimismVisibleAtRevision) && rig.row(&temp_id(&receipt)).await.is_some());
    assert!(sends(&cell));
    rig.server.wait_held(Kind::Mutate, 1).await;
    assert_eq!(rig.server.count("MakeMoment"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn out_queued_send() {
    let from_online = want(St::Queued, &offline(), In::Online(true));
    let from_online_ready = want(St::Queued, &Ctx::ready(), In::Online(true));
    let from_row = want(St::Queued, &Ctx::ready(), In::RowFreed);
    assert_eq!((from_online.next, from_online_ready.next, from_row.next), (St::Queued, St::Sending, St::Sending));

    let rig = Rig::open(OnFailure::Rollback).await;
    rig.db.set_online(false).unwrap();
    let receipt = rig.create("a").await;
    quiet().await;
    assert_eq!(rig.server.mutations(), 0);
    rig.db.set_online(true).unwrap();
    rig.server.wait_seen("MakeMoment", 1).await;
    assert_eq!(rig.outcome(&receipt).await, rig.outcome(&receipt).await);

    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let first = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    let second = rig.update(&temp_id(&first), "b").await.unwrap();
    quiet().await;
    assert_eq!(rig.server.count("UpdateMoment"), 0);
    rig.server.hold(Kind::Mutate, false);
    rig.server.answer(Kind::Mutate, Reply::Ok);
    rig.server.wait_seen("UpdateMoment", 1).await;
    assert_eq!(kind_of(&rig.outcome(&second).await), wire_of(Out::Confirmed));
}

#[tokio::test(flavor = "multi_thread")]
async fn out_queued_offline() {
    let cell = want(St::Queued, &Ctx::ready(), In::Online(false));
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.db.set_online(false).unwrap();
    let receipt = rig.create("a").await;

    rig.db.set_online(false).unwrap();
    quiet().await;

    assert_eq!((cell.next, cell.eff.is_empty()), (St::Queued, true));
    assert_eq!(rig.server.mutations(), 0);
    assert!(rig.pending(&receipt).await);
    assert!(rig.row(&temp_id(&receipt)).await.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn out_sending_response() {
    let cell = want(St::Sending, &Ctx::ready(), In::Response);
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let receipt = rig.create("hello").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    assert!(rig.pending(&receipt).await);

    rig.server.answer(Kind::Mutate, Reply::Ok);

    assert_eq!(cell.next, St::Landing);
    assert_eq!(kind_of(&rig.outcome(&receipt).await), "Confirmed");
    assert!(rig.row("srv1").await.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn out_sending_temp() {
    let cell = want(St::Sending, &Ctx::ready(), In::TransportError { temporary: true });
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.script("MakeMoment", Reply::Temp);
    let receipt = rig.create("hello").await;
    rig.server.wait_seen("MakeMoment", 1).await;

    quiet().await;
    assert_eq!((cell.next, cell.eff.contains(&Eff::ScheduleTick)), (St::Sending, true));
    assert_eq!(rig.server.count("MakeMoment"), 1);
    assert!(rig.pending(&receipt).await);
    until("the backoff send", || { rig.clock.advance(60_000); rig.server.count("MakeMoment") >= 2 }).await;

    assert_eq!(kind_of(&rig.outcome(&receipt).await), "Confirmed");
}

#[tokio::test(flavor = "multi_thread")]
async fn out_sending_reject_rollback() {
    let ctx = Ctx { policy: Policy::Rollback, ..Ctx::ready() };
    let cell = want(St::Sending, &ctx, In::TransportError { temporary: false });
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.script("MakeMoment", Reply::Refuse);

    let receipt = rig.create("hello").await;
    let outcome = rig.outcome(&receipt).await;

    assert_eq!(cell.next, St::RollingBack(super::ref_outbox::Why::Rejected));
    assert_eq!(kind_of(&outcome), wire_of(Out::Rejected));
    assert!(rig.row(&temp_id(&receipt)).await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn out_sending_offline() {
    let cell = want(St::Sending, &Ctx::ready(), In::Online(false));
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let receipt = rig.create("hello").await;
    rig.server.wait_held(Kind::Mutate, 1).await;

    rig.db.set_online(false).unwrap();
    until("the request is cancelled", || rig.server.cancelled() >= 1).await;
    assert!(cell.eff.contains(&Eff::Cancel) && rig.pending(&receipt).await);
    rig.db.set_online(true).unwrap();
    rig.server.wait_seen("MakeMoment", 2).await;

    rig.server.hold(Kind::Mutate, false);
    rig.server.answer(Kind::Mutate, Reply::Ok);
    assert_eq!(kind_of(&rig.outcome(&receipt).await), "Confirmed");
}

#[tokio::test(flavor = "multi_thread")]
async fn out_landing_committed() {
    let cell = want(St::Landing, &Ctx::ready(), In::Committed);
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let first = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    let second = rig.update(&temp_id(&first), "b").await.unwrap();

    rig.server.hold(Kind::Mutate, false);
    rig.server.answer(Kind::Mutate, Reply::Ok);

    assert_eq!(kind_of(&rig.outcome(&first).await), wire_of(Out::Confirmed));
    assert!(cell.obs.contains(&Obs::ResponseRowsVisibleAtRevision) && rig.row("srv1").await.is_some());
    assert!(cell.eff.contains(&Eff::SendNextOfRow));
    rig.server.wait_seen("UpdateMoment", 1).await;
    assert_eq!(kind_of(&rig.outcome(&second).await), "Confirmed");
}

#[tokio::test(flavor = "multi_thread")]
async fn out_rollback_committed() {
    let cell = want(St::RollingBack(super::ref_outbox::Why::Rejected), &Ctx::ready(), In::Committed);
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let first = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    let second = rig.update(&temp_id(&first), "b").await.unwrap();

    rig.server.hold(Kind::Mutate, false);
    rig.server.answer(Kind::Mutate, Reply::Refuse);

    assert_eq!(kind_of(&rig.outcome(&first).await), wire_of(Out::Rejected));
    assert!(cell.obs.contains(&Obs::OptimismGoneAtRevision) && rig.row(&temp_id(&first)).await.is_none());
    assert!(cell.eff.contains(&Eff::SendNextOfRow));
    rig.server.wait_seen("UpdateMoment", 1).await;
    let _ = rig.outcome(&second).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn out_failed_retry() {
    let ctx = Ctx { policy: Policy::Keep, ..Ctx::ready() };
    let failed = want(St::Sending, &ctx, In::TransportError { temporary: false });
    let cell = want(St::Failed, &ctx, In::Retry);
    let rig = Rig::open(OnFailure::Keep).await;
    rig.server.script("MakeMoment", Reply::Refuse);
    let receipt = rig.create("hello").await;
    let id = temp_id(&receipt);
    quiet().await;
    let row = rig.row(&id).await.expect("the keep policy keeps the row");
    assert!(failed.obs.contains(&Obs::RowCarriesError));
    assert_eq!((row.sync.state, row.sync.error.is_some()), (WireSyncState::Failed, true));

    rig.server.hold(Kind::Mutate, true);
    let retried = within(rig.db.retry(rig.epoch(), "Moment".into(), id.clone(), None, None)).await.unwrap();
    rig.server.wait_held(Kind::Mutate, 1).await;

    assert_eq!(cell.next, St::Queued);
    assert!(cell.obs.contains(&Obs::RowErrorCleared) && rig.row(&id).await.unwrap().sync.error.is_none());
    assert!(sends(&cell));
    rig.server.hold(Kind::Mutate, false);
    rig.server.answer(Kind::Mutate, Reply::Ok);
    assert_eq!(kind_of(&rig.outcome(&retried).await), "Confirmed");
    assert_eq!(rig.server.count("MakeMoment"), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn out_closing() {
    let sending = want(St::Sending, &Ctx::ready(), In::Closing);
    let queued = want(St::Queued, &Ctx::ready(), In::Closing);
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let first = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    let second = rig.update(&temp_id(&first), "b").await.unwrap();

    within(rig.db.close_database()).await.unwrap();

    assert_eq!((sending.next, queued.next), (St::Done, St::Queued));
    assert_eq!(kind_of(&rig.outcome(&first).await), wire_of(Out::Interrupted));
    assert_eq!(kind_of(&rig.outcome(&second).await), wire_of(Out::Interrupted));
    assert!(sending.eff.contains(&Eff::Cancel) && sending.eff.contains(&Eff::Drained));
    until("the request is cancelled", || rig.server.cancelled() >= 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn out_outcome_done() {
    let rig = Rig::open(OnFailure::Rollback).await;
    let receipt = rig.create("a").await;
    let first = rig.outcome(&receipt).await;

    let again = tokio::time::timeout(std::time::Duration::from_secs(1), rig.db.outcome(receipt.clone())).await.expect("a finished operation answers at once").unwrap();

    assert_eq!(kind_of(&first), "Confirmed");
    assert_eq!(first, again);
}

#[tokio::test(flavor = "multi_thread")]
async fn out_outcome_wait() {
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let receipt = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;

    assert!(rig.pending(&receipt).await);
    rig.server.answer(Kind::Mutate, Reply::Ok);

    assert_eq!(kind_of(&rig.outcome(&receipt).await), "Confirmed");
}

#[tokio::test(flavor = "multi_thread")]
async fn out_open_restores() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite").to_string_lossy().to_string();
    let first = Rig::open_at(OnFailure::Rollback, Some(path.clone()), Server::new()).await;
    first.db.set_online(false).unwrap();
    first.create("a").await;
    first.create("b").await;
    within(first.db.close_database()).await.unwrap();
    drop(first);

    let server = Server::new();
    let second = Rig::open_at(OnFailure::Rollback, Some(path), server.clone()).await;
    server.wait_seen("MakeMoment", 2).await;

    let texts: Vec<_> = server.seen().iter().map(|seen| seen.variables["text"].as_str().unwrap_or("").to_string()).collect();
    assert_eq!(texts, ["a", "b"]);
    drop(second);
}

#[tokio::test(flavor = "multi_thread")]
async fn out_open_format_change() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite").to_string_lossy().to_string();
    let first = Rig::open_full(OnFailure::Rollback, Some(path.clone()), Server::new(), Some("1".into())).await;
    first.db.set_online(false).unwrap();
    let receipt = first.create("a").await;
    let id = temp_id(&receipt);
    within(first.db.close_database()).await.unwrap();
    drop(first);

    let server = Server::new();
    server.hold(Kind::Mutate, true);
    let second = Rig::open_full(OnFailure::Rollback, Some(path), server.clone(), Some("2".into())).await;
    server.wait_held(Kind::Mutate, 1).await;

    assert!(second.row(&id).await.is_none(), "the optimistic row of a stored create is not restored");
    server.hold(Kind::Mutate, false);
    server.answer(Kind::Mutate, Reply::Ok);
    within(async { while second.row("srv1").await.is_none() { tokio::time::sleep(std::time::Duration::from_millis(5)).await; } }).await;
}

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
            for op in model.ops.iter_mut().filter(|op| matches!(op.state, St::Queued | St::Sending)) {
                let step = want(op.state, &ctx, In::Online(model.online));
                op.state = step.next;
                op.requesting = model.online && step.next == St::Sending;
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
        let requesting = model.ops.iter().filter(|op| op.requesting).count();
        until(&format!("{label} step {index}: requests in flight {requesting}"), || rig.server.held(Kind::Mutate) == requesting).await;
        let expected: Vec<_> = model.ops.iter().map(|op| op.resolved.is_some()).collect();
        until(&format!("{label} step {index}: resolved {expected:?}"), || model.ops.iter().all(|op| op.resolved.is_none() || op.handle.is_finished())).await;
        quiet().await;
        for (number, op) in model.ops.iter().enumerate() {
            assert_eq!(op.handle.is_finished(), op.resolved.is_some(), "{label} step {index}: op {number} {:?}", op.state);
            if let (Some(want_out), true) = (op.resolved, op.handle.is_finished()) {
                let outcome = within(rig.db.outcome(op.receipt.clone())).await.unwrap();
                assert_eq!(kind_of(&outcome), wire_of(want_out), "{label} step {index}: op {number}");
                if check == Check::RollbackClean && matches!(want_out, Out::Rejected | Out::Discarded) {
                    assert!(rig.row(&op.receipt.optimistic_value.clone().unwrap().id).await.is_none(), "{label}: op {number} left its optimism");
                }
            }
        }
        if matches!(act, Act::Close) { return; }
    }
    if check == Check::Idle {
        let before = rig.server.mutations();
        rig.clock.advance(3_600_000);
        quiet().await;
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
    for trace in traces() {
        let trace: Vec<Act> = trace.into_iter().flat_map(|act| if matches!(act, Act::Create) { vec![act] } else { vec![Act::Create, act] }).take(8).collect();
        let label = format!("{trace:?}");
        tokio::time::timeout(std::time::Duration::from_secs(30), run_trace(&trace, check)).await.unwrap_or_else(|_| panic!("{label}: the trace did not finish within 30 seconds"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn out_prop_outcome_once() { run_all(Check::Outcomes).await }

#[tokio::test(flavor = "multi_thread")]
async fn out_prop_rollback_clean() { run_all(Check::RollbackClean).await }

#[tokio::test(flavor = "multi_thread")]
async fn out_prop_idle() { run_all(Check::Idle).await }

#[tokio::test(flavor = "multi_thread")]
async fn out_prop_row_order() {
    for first_reply in [Reply::Ok, Reply::Refuse] {
        let rig = Rig::open(OnFailure::Rollback).await;
        rig.server.hold(Kind::Mutate, true);
        let first = rig.create("a").await;
        rig.server.wait_held(Kind::Mutate, 1).await;
        let id = temp_id(&first);
        let second = rig.update(&id, "b").await.unwrap();
        let third = rig.update(&id, "c").await.unwrap();
        quiet().await;
        assert_eq!(rig.server.mutations(), 1, "only the first operation of a row is in the network");

        rig.server.answer(Kind::Mutate, first_reply);
        for receipt in [&first, &second, &third] {
            rig.server.hold(Kind::Mutate, false);
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), rig.db.outcome(receipt.clone())).await;
        }

        assert!(!rig.server.overlapped(), "two operations of one row were in the network together");
        let updates: Vec<_> = rig.server.seen().iter().filter(|seen| seen.operation == "UpdateMoment").map(|seen| seen.variables["text"].as_str().unwrap_or("").to_string()).collect();
        assert!(updates.is_empty() || updates == ["b", "c"], "{updates:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn out_prop_intent_kept() {
    for acts in [vec![Act::Off, Act::Create, Act::Create], vec![Act::Create, Act::Off, Act::Create, Act::On]] {
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
        quiet().await;
        let survivors = model.ops.iter().filter(|op| want(op.state, &ctx_of(&model), In::Closing).next == St::Queued).count();

        within(first.db.close_database()).await.unwrap();
        drop(first);
        let server = Server::new();
        let second = Rig::open_at(OnFailure::Rollback, Some(path), server.clone()).await;
        quiet().await;

        assert_eq!(server.count("MakeMoment"), survivors, "{acts:?}");
        drop(second);
    }
}
