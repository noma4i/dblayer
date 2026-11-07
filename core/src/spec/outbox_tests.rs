use super::ref_outbox::{step_of, Ctx, Eff, In, Obs, Out, Policy, St, Step};
use super::rig::{bounded, until, within, Kind, Reply, Rig, Server};
use crate::ffi::{CommandRequest, WireOperationOutcome, WireOperationReceipt, WireSyncState};
use crate::schema::OnFailure;

pub(super) fn want(state: St, ctx: &Ctx, input: In) -> Step { step_of(state, ctx, input).unwrap_or_else(|| panic!("no table cell for {state:?} x {input:?}")) }

pub(super) fn offline() -> Ctx { Ctx { online: false, ..Ctx::ready() } }

pub(super) fn sends(step: &Step) -> bool { step.eff.contains(&Eff::Send) }

pub(super) fn wire_of(out: Out) -> &'static str {
    match out {
        Out::Confirmed => "Confirmed",
        Out::Rejected | Out::Failed => "Rejected",
        Out::Interrupted | Out::Discarded => "Interrupted",
    }
}

pub(super) fn kind_of(outcome: &WireOperationOutcome) -> &'static str {
    match outcome {
        WireOperationOutcome::Confirmed { .. } => "Confirmed",
        WireOperationOutcome::Rejected { .. } => "Rejected",
        WireOperationOutcome::Interrupted => "Interrupted",
    }
}

pub(super) fn temp_id(receipt: &WireOperationReceipt) -> String { receipt.optimistic_value.clone().expect("the receipt carries the optimistic row").id }

#[test]
fn out_submit() { bounded("out_submit", out_submit_run) }

async fn out_submit_run() {
    let submitted = want(St::None, &offline(), In::Submit);
    let committed = want(submitted.next, &offline(), In::Committed);
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.db.set_online(false).unwrap();

    let receipt = within(async { rig.create("hello").await }).await;

    assert_eq!(submitted.obs, []);
    assert!(committed.obs.contains(&Obs::Receipt) && receipt.operation.is_some());
    assert!(committed.obs.contains(&Obs::OptimismVisibleAtRevision) && rig.row(&temp_id(&receipt)).await.is_some());
    rig.quiet().await;
    assert_eq!(sends(&committed), rig.server.mutations() > 0);
}

#[test]
fn out_submit_stale() { bounded("out_submit_stale", out_submit_stale_run) }

async fn out_submit_stale_run() {
    let cell = want(St::None, &Ctx { epoch_current: false, ..Ctx::ready() }, In::Submit);
    let rig = Rig::open(OnFailure::Rollback).await;
    let request = CommandRequest { epoch: rig.epoch() + 1, model: "Moment".into(), values_json: r#"{"text":"x"}"#.into(), args_json: "{}".into(), upload: None };

    let result = within(rig.db.send(request)).await;

    assert_eq!(cell.obs, [Obs::Fails(super::ref_outbox::Fault::NoSession)]);
    assert!(matches!(result, Err(crate::ffi::DbError::NoSession { .. })), "{result:?}");
    assert_eq!(rig.server.mutations(), 0);
}

#[test]
fn out_submitting_committed() { bounded("out_submitting_committed", out_submitting_committed_run) }

async fn out_submitting_committed_run() {
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

#[test]
fn out_queued_send() { bounded("out_queued_send", out_queued_send_run) }

async fn out_queued_send_run() {
    let from_online = want(St::Queued, &offline(), In::Online(true));
    let from_online_ready = want(St::Queued, &Ctx::ready(), In::Online(true));
    let from_row = want(St::Queued, &Ctx::ready(), In::RowFreed);
    assert_eq!((from_online.next, from_online_ready.next, from_row.next), (St::Queued, St::Sending, St::Sending));

    let rig = Rig::open(OnFailure::Rollback).await;
    rig.db.set_online(false).unwrap();
    let receipt = rig.create("a").await;
    rig.quiet().await;
    assert_eq!(rig.server.mutations(), 0);
    rig.db.set_online(true).unwrap();
    rig.server.wait_seen("MakeMoment", 1).await;
    assert_eq!(rig.outcome(&receipt).await, rig.outcome(&receipt).await);

    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let first = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    let second = rig.update(&temp_id(&first), "b").await.unwrap();
    rig.quiet().await;
    assert_eq!(rig.server.count("UpdateMoment"), 0);
    rig.server.hold(Kind::Mutate, false);
    rig.server.answer(Kind::Mutate, Reply::Ok);
    rig.server.wait_seen("UpdateMoment", 1).await;
    assert_eq!(kind_of(&rig.outcome(&second).await), wire_of(Out::Confirmed));
}

#[test]
fn out_queued_offline() { bounded("out_queued_offline", out_queued_offline_run) }

async fn out_queued_offline_run() {
    let cell = want(St::Queued, &Ctx::ready(), In::Online(false));
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.db.set_online(false).unwrap();
    let receipt = rig.create("a").await;

    rig.db.set_online(false).unwrap();
    rig.quiet().await;

    assert_eq!((cell.next, cell.eff.is_empty()), (St::Queued, true));
    assert_eq!(rig.server.mutations(), 0);
    assert!(rig.pending(&receipt).await);
    assert!(rig.row(&temp_id(&receipt)).await.is_some());
}

#[test]
fn out_sending_response() { bounded("out_sending_response", out_sending_response_run) }

async fn out_sending_response_run() {
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

#[test]
fn out_sending_destroy_not_found() { bounded("out_sending_destroy_not_found", out_sending_destroy_not_found_run) }

async fn out_sending_destroy_not_found_run() {
    let cell = want(St::Sending, &Ctx::ready(), In::Response);
    let rig = Rig::open(OnFailure::Rollback).await;
    let created = rig.create("hello").await;
    assert_eq!(kind_of(&rig.outcome(&created).await), "Confirmed");
    rig.server.script("DeleteMoment", Reply::NotFound);

    let receipt = within(rig.db.destroy(rig.epoch(), "Moment".into(), "srv1".into())).await.unwrap();
    let outcome = rig.outcome(&receipt).await;

    assert_eq!(cell.next, St::Landing);
    assert_eq!(kind_of(&outcome), "Confirmed");
    assert!(rig.row("srv1").await.is_none());
    assert_eq!(rig.server.count("DeleteMoment"), 1);
}

#[test]
fn out_sending_temp() { bounded("out_sending_temp", out_sending_temp_run) }

async fn out_sending_temp_run() {
    let cell = want(St::Sending, &Ctx::ready(), In::TransportError { temporary: true });
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.script("MakeMoment", Reply::Temp);
    let receipt = rig.create("hello").await;
    rig.server.wait_seen("MakeMoment", 1).await;

    rig.quiet().await;
    assert_eq!((cell.next, cell.eff.contains(&Eff::ScheduleTick)), (St::Sending, true));
    assert_eq!(rig.server.count("MakeMoment"), 1);
    assert!(rig.pending(&receipt).await);
    until("the backoff send", || { rig.clock.advance(60_000); rig.server.count("MakeMoment") >= 2 }).await;

    assert_eq!(kind_of(&rig.outcome(&receipt).await), "Confirmed");
}

#[test]
fn out_sending_reject_rollback() { bounded("out_sending_reject_rollback", out_sending_reject_rollback_run) }

async fn out_sending_reject_rollback_run() {
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

#[test]
fn out_sending_offline() { bounded("out_sending_offline", out_sending_offline_run) }

async fn out_sending_offline_run() {
    let cell = want(St::Sending, &Ctx::ready(), In::Online(false));
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let receipt = rig.create("hello").await;
    rig.server.wait_held(Kind::Mutate, 1).await;

    rig.db.set_online(false).unwrap();
    rig.quiet().await;
    assert!(cell.eff.is_empty() && cell.next == St::Sending && rig.pending(&receipt).await);
    rig.db.set_online(true).unwrap();
    crate::test_wait::yield_until("the Session cancelled the request with the network and sent it once more", || rig.server.count("MakeMoment") == 2).await;

    rig.server.hold(Kind::Mutate, false);
    rig.server.answer(Kind::Mutate, Reply::Ok);
    assert_eq!(kind_of(&rig.outcome(&receipt).await), "Confirmed");
}

#[test]
fn out_landing_committed() { bounded("out_landing_committed", out_landing_committed_run) }

async fn out_landing_committed_run() {
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

#[test]
fn out_rollback_committed() { bounded("out_rollback_committed", out_rollback_committed_run) }

async fn out_rollback_committed_run() {
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

#[test]
fn out_failed_retry() { bounded("out_failed_retry", out_failed_retry_run) }

async fn out_failed_retry_run() {
    let ctx = Ctx { policy: Policy::Keep, ..Ctx::ready() };
    let failed = want(St::MarkingFailed, &ctx, In::Committed);
    let marking = want(St::Failed, &ctx, In::Retry);
    let cell = want(St::MarkingRetry, &ctx, In::Committed);
    let rig = Rig::open(OnFailure::Keep).await;
    rig.server.script("MakeMoment", Reply::Refuse);
    let receipt = rig.create("hello").await;
    let id = temp_id(&receipt);
    within(async { while rig.row(&id).await.is_none_or(|row| row.sync.state != WireSyncState::Failed) { tokio::task::yield_now().await; } }).await;
    let row = rig.row(&id).await.expect("the keep policy keeps the row");
    assert!(failed.obs.contains(&Obs::RowCarriesError));
    assert_eq!((row.sync.state, row.sync.error.is_some()), (WireSyncState::Failed, true));

    rig.server.hold(Kind::Mutate, true);
    let retried = within(rig.db.retry(rig.epoch(), "Moment".into(), id.clone(), None, None)).await.unwrap();
    rig.server.wait_held(Kind::Mutate, 1).await;

    assert_eq!((marking.next, cell.next), (St::MarkingRetry, St::Sending));
    assert!(cell.obs.contains(&Obs::RowErrorCleared) && rig.row(&id).await.unwrap().sync.error.is_none());
    assert!(sends(&cell));
    rig.server.hold(Kind::Mutate, false);
    rig.server.answer(Kind::Mutate, Reply::Ok);
    assert_eq!(kind_of(&rig.outcome(&retried).await), "Confirmed");
    assert_eq!(rig.server.count("MakeMoment"), 2);
}

#[test]
fn out_closing() { bounded("out_closing", out_closing_run) }

async fn out_closing_run() {
    let sending = want(St::Sending, &Ctx::ready(), In::Closing);
    let queued = want(St::Queued, &Ctx::ready(), In::Closing);
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let first = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    let second = rig.update(&temp_id(&first), "b").await.unwrap();

    within(rig.db.close_database()).await.unwrap();

    assert_eq!((sending.next, queued.next), (St::Done, St::Done));
    assert_eq!(kind_of(&rig.outcome(&first).await), wire_of(Out::Interrupted));
    assert_eq!(kind_of(&rig.outcome(&second).await), wire_of(Out::Interrupted));
    assert!(sending.eff.contains(&Eff::Cancel) && sending.eff.contains(&Eff::Drained));
    until("the request is cancelled", || rig.server.cancelled() >= 1).await;
}

#[test]
fn out_outcome_done() { bounded("out_outcome_done", out_outcome_done_run) }

async fn out_outcome_done_run() {
    let rig = Rig::open(OnFailure::Rollback).await;
    let receipt = rig.create("a").await;
    let first = rig.outcome(&receipt).await;

    let again = tokio::time::timeout(std::time::Duration::from_secs(1), rig.db.outcome(receipt.clone())).await.expect("a finished operation answers at once").unwrap();

    assert_eq!(kind_of(&first), "Confirmed");
    assert_eq!(first, again);
}

#[test]
fn out_outcome_wait() { bounded("out_outcome_wait", out_outcome_wait_run) }

async fn out_outcome_wait_run() {
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let receipt = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;

    assert!(rig.pending(&receipt).await);
    rig.server.answer(Kind::Mutate, Reply::Ok);

    assert_eq!(kind_of(&rig.outcome(&receipt).await), "Confirmed");
}

#[test]
fn out_open_restores() { bounded("out_open_restores", out_open_restores_run) }

async fn out_open_restores_run() {
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

