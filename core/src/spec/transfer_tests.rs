use std::path::Path;
use std::sync::{Arc, Mutex};

use super::ref_transfer::{round_step, step_of, Ctx, Eff, In, Obs, Round, RoundIn, Shown, St, Step};
use super::rig::{quiet, until, within, Kind, Reply, Rig, Server};
use crate::ffi::{DbError, UploadAcceptedSink, WireOperationOutcome, WireOperationReceipt, WireUploadAccepted, WireUploadStatus};
use crate::ffi::upload_wire::WireUploadRequest;
use crate::schema::OnFailure;

const ROW: &str = "srv1";
const MEBIBYTE: u64 = 1024 * 1024;

async fn seed_rows(rig: &Rig, count: usize) -> Vec<String> {
    let mut ids = vec![];
    for number in 0..count {
        let receipt = rig.create(&format!("seed{number}")).await;
        match rig.outcome(&receipt).await {
            WireOperationOutcome::Confirmed { value: Some(row), .. } => ids.push(row.id),
            other => panic!("the seed row is not confirmed: {other:?}"),
        }
    }
    ids
}

async fn seeded(on_failure: OnFailure) -> Rig {
    let rig = Rig::open(on_failure).await;
    assert_eq!(seed_rows(&rig, 1).await, [ROW]);
    rig
}

fn want(state: St, ctx: &Ctx, input: In) -> Step { step_of(state, ctx, input).unwrap_or_else(|| panic!("no table cell for {state:?} x {input:?}")) }

fn source(directory: &Path, name: &str, size: u64) -> String {
    let path = directory.join(name);
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(size).unwrap();
    path.to_string_lossy().to_string()
}

fn request(path: &str, name: &str) -> WireUploadRequest { WireUploadRequest { path: path.into(), name: Some(name.into()), mime: Some("video/mp4".into()), variables_json: "{}".into() } }

async fn accept(rig: &Rig, path: &str, name: &str) -> WireOperationReceipt {
    within(rig.db.upload(rig.epoch(), "Moment".into(), ROW.into(), "upload".into(), request(path, name))).await.unwrap()
}

async fn shown(rig: &Rig) -> Option<WireUploadStatus> {
    rig.row(ROW).await.and_then(|row| row.uploads.into_iter().find(|entry| entry.action == "upload")).map(|entry| entry.upload.status)
}

async fn wait_shown(rig: &Rig, wanted: Option<WireUploadStatus>) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let now = shown(rig).await;
        if now == wanted { return; }
        assert!(std::time::Instant::now() < deadline, "uploads[upload] is {now:?}, expected {wanted:?}");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

fn shown_of(shape: Shown) -> Option<WireUploadStatus> {
    match shape {
        Shown::Copying => Some(WireUploadStatus::Preparing),
        Shown::Queued => Some(WireUploadStatus::Queued),
        Shown::Uploading => Some(WireUploadStatus::Uploading),
        Shown::Failed => Some(WireUploadStatus::Failed),
        Shown::Gone => None,
    }
}

fn shows(step: &Step) -> Option<Option<WireUploadStatus>> {
    step.obs.iter().find_map(|obs| if let Obs::Shows(shape) = obs { Some(shown_of(*shape)) } else { None })
}

struct Events(Mutex<Vec<WireUploadAccepted>>);

impl UploadAcceptedSink for Events {
    fn accepted(&self, event: WireUploadAccepted) { self.0.lock().unwrap().push(event); }
}

fn multipart_ctx() -> Ctx { Ctx { has_upload_id: true, ..Ctx::ready() } }

#[tokio::test(flavor = "multi_thread")]
async fn xfer_accept() {
    let cell = want(St::None, &Ctx::ready(), In::Accept);
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    rig.server.hold(Kind::Put, true);
    let path = source(directory.path(), "a.mp4", 1024);

    let receipt = accept(&rig, &path, "a.mp4").await;

    assert!(cell.obs.contains(&Obs::Receipt) && receipt.operation.is_some());
    assert!(cell.eff.contains(&Eff::Copy));
    assert!(shown(&rig).await.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_accept_stale() {
    let cell = want(St::None, &Ctx { epoch_current: false, ..Ctx::ready() }, In::Accept);
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    let path = source(directory.path(), "a.mp4", 1024);

    let before = rig.server.mutations();
    let result = within(rig.db.upload(rig.epoch() + 1, "Moment".into(), ROW.into(), "upload".into(), request(&path, "a.mp4"))).await;

    assert_eq!(cell.obs, [Obs::NoSession]);
    assert!(matches!(result, Err(DbError::NoSession { .. })), "{result:?}");
    assert_eq!(rig.server.mutations(), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_ready_start() {
    let cell = want(St::Ready, &Ctx::ready(), In::Start);
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    rig.server.hold(Kind::Put, true);
    accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;

    rig.server.wait_held(Kind::Put, 1).await;

    assert_eq!(cell.next, St::Uploading);
    assert_eq!(shown(&rig).await, shows(&cell).unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_ready_cancel() {
    let cell = want(St::Ready, &Ctx::ready(), In::Cancel);
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    rig.db.set_online(false).unwrap();
    accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;
    wait_shown(&rig, Some(WireUploadStatus::Queued)).await;

    let before = rig.server.mutations();

    let cancelled = within(rig.db.cancel_upload(rig.epoch(), "Moment".into(), ROW.into(), "upload".into())).await.unwrap();

    assert!(cell.obs.contains(&Obs::CancelTrue) && cancelled);
    assert_eq!(shown(&rig).await, shows(&cell).unwrap());
    assert_eq!(rig.server.mutations() + rig.server.held(Kind::Put), before);
}

async fn multipart_rig(directory: &Path) -> (Rig, String) {
    let rig = seeded(OnFailure::Rollback).await;
    let path = source(directory, "big.mp4", 11 * MEBIBYTE);
    (rig, path)
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_step_ok() {
    let cell = want(St::Uploading, &multipart_ctx(), In::StepOk { parts_left: true });
    let directory = tempfile::tempdir().unwrap();
    let (rig, path) = multipart_rig(directory.path()).await;
    rig.server.hold(Kind::Put, true);
    accept(&rig, &path, "big.mp4").await;
    rig.server.wait_held(Kind::Put, 3).await;

    rig.server.answer(Kind::Put, Reply::Ok);
    quiet().await;

    assert_eq!(cell.next, St::Uploading);
    assert_eq!(shown(&rig).await, Some(WireUploadStatus::Uploading));
    assert_eq!(rig.server.held(Kind::Put), 2);
    assert_eq!(rig.server.count("CompleteMultipart"), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_all_parts() {
    let cell = want(St::Uploading, &multipart_ctx(), In::StepOk { parts_left: false });
    let directory = tempfile::tempdir().unwrap();
    let (rig, path) = multipart_rig(directory.path()).await;
    rig.server.hold_op("CompleteMultipart");
    rig.server.hold(Kind::Put, true);
    accept(&rig, &path, "big.mp4").await;
    rig.server.wait_held(Kind::Put, 3).await;

    for _ in 0..3 { rig.server.answer(Kind::Put, Reply::Ok); }
    rig.server.wait_seen("CompleteMultipart", 1).await;

    assert_eq!(cell.next, St::Completing);
    assert!(cell.obs.contains(&Obs::Progress100));
    let upload = rig.row(ROW).await.unwrap().uploads.into_iter().find(|entry| entry.action == "upload").unwrap().upload;
    assert_eq!(upload.sent_bytes, upload.total_bytes);
    assert_eq!(upload.total_bytes, 11 * MEBIBYTE);
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_step_temp() {
    let cell = want(St::Uploading, &Ctx::ready(), In::StepError { temporary: true });
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    rig.server.auto(Kind::Put, Reply::Temp);
    accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;
    until("the first step", || rig.server.seen().iter().any(|seen| seen.kind == Kind::Put)).await;

    quiet().await;
    assert!(cell.eff.contains(&Eff::ScheduleBackoff) && cell.obs.is_empty());
    assert_eq!(rig.server.seen().iter().filter(|seen| seen.kind == Kind::Put).count(), 1);
    assert_ne!(shown(&rig).await, Some(WireUploadStatus::Failed));
    until("the backoff step", || { rig.clock.advance(60_000); rig.server.seen().iter().filter(|seen| seen.kind == Kind::Put).count() >= 2 }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_step_fail() {
    let cell = want(St::Uploading, &multipart_ctx(), In::StepError { temporary: false });
    let directory = tempfile::tempdir().unwrap();
    let (rig, path) = multipart_rig(directory.path()).await;
    rig.server.auto(Kind::Put, Reply::Refuse);
    accept(&rig, &path, "big.mp4").await;

    wait_shown(&rig, shows(&cell).unwrap()).await;

    assert_eq!(cell.next, St::Failed);
    assert!(cell.eff.contains(&Eff::Abort));
    rig.server.wait_seen("AbortMultipart", 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_step_offline() {
    let cell = want(St::Uploading, &Ctx::ready(), In::Online(false));
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    rig.server.hold(Kind::Put, true);
    accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;
    rig.server.wait_held(Kind::Put, 1).await;

    rig.db.set_online(false).unwrap();

    wait_shown(&rig, shows(&cell).unwrap()).await;
    assert!(cell.eff.contains(&Eff::StepCancelled));
    until("the step is cancelled", || rig.server.cancelled() >= 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_uploading_cancel() {
    let cell = want(St::Uploading, &multipart_ctx(), In::Cancel);
    let directory = tempfile::tempdir().unwrap();
    let (rig, path) = multipart_rig(directory.path()).await;
    rig.server.hold(Kind::Put, true);
    accept(&rig, &path, "big.mp4").await;
    rig.server.wait_held(Kind::Put, 1).await;

    let cancelled = within(rig.db.cancel_upload(rig.epoch(), "Moment".into(), ROW.into(), "upload".into())).await.unwrap();

    assert!(cell.obs.contains(&Obs::CancelTrue) && cancelled);
    assert!(cell.eff.contains(&Eff::Abort));
    rig.server.wait_seen("AbortMultipart", 1).await;
    wait_shown(&rig, None).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_completed() {
    let cell = want(St::Completing, &multipart_ctx(), In::Completed);
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    rig.server.hold_op("MakeMoment");
    accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;

    rig.server.wait_seen("MakeMoment", 1).await;

    assert_eq!(cell.next, St::Mutating);
    assert!(cell.eff.contains(&Eff::SubmitFinalMutation));
    let mutation = rig.server.seen().into_iter().filter(|seen| seen.operation == "MakeMoment").last().unwrap();
    assert!(mutation.variables["signedId"].is_string(), "{}", mutation.variables);
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_complete_fail() {
    let cell = want(St::Completing, &multipart_ctx(), In::CompleteError);
    let directory = tempfile::tempdir().unwrap();
    let (rig, path) = multipart_rig(directory.path()).await;
    rig.server.script("CompleteMultipart", Reply::Refuse);
    accept(&rig, &path, "big.mp4").await;

    wait_shown(&rig, shows(&cell).unwrap()).await;
    quiet().await;

    assert!(cell.eff.contains(&Eff::NoAbort));
    assert_eq!(rig.server.count("AbortMultipart"), 0);
    assert_eq!(rig.server.seen().iter().filter(|seen| seen.kind == Kind::Put).count(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_mutation_confirmed() {
    let cell = want(St::Mutating, &Ctx::ready(), In::MutationOutcome { confirmed: true });
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    let receipt = accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;

    let outcome = rig.outcome(&receipt).await;

    assert!(matches!(outcome, WireOperationOutcome::Confirmed { .. }), "{outcome:?}");
    assert_eq!(shown(&rig).await, shows(&cell).unwrap());
    assert!(cell.obs.contains(&Obs::RowWithServerFile) && rig.row("srv1").await.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_mutation_failed() {
    let cell = want(St::Mutating, &Ctx::ready(), In::MutationOutcome { confirmed: false });
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Keep).await;
    rig.server.script("MakeMoment", Reply::Refuse);

    accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;

    wait_shown(&rig, shows(&cell).unwrap()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_mutating_cancel() {
    let cell = want(St::Mutating, &Ctx::ready(), In::Cancel);
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    rig.server.hold_op("MakeMoment");
    accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;
    rig.server.wait_seen("MakeMoment", 1).await;

    let cancelled = within(rig.db.cancel_upload(rig.epoch(), "Moment".into(), ROW.into(), "upload".into())).await.unwrap();

    assert!(cell.obs.contains(&Obs::CancelFalse) && !cancelled);
}

async fn failed_after_all_parts(directory: &Path) -> Rig {
    let (rig, path) = multipart_rig(directory).await;
    rig.server.script("CompleteMultipart", Reply::Refuse);
    accept(&rig, &path, "big.mp4").await;
    wait_shown(&rig, Some(WireUploadStatus::Failed)).await;
    rig
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_retry_complete() {
    let cell = want(St::Failed, &Ctx { parts_all_confirmed: true, ..multipart_ctx() }, In::Retry);
    let directory = tempfile::tempdir().unwrap();
    let rig = failed_after_all_parts(directory.path()).await;

    let retried = within(rig.db.retry(rig.epoch(), "Moment".into(), ROW.into(), Some("upload".into()), None)).await.unwrap();

    assert!(cell.eff.contains(&Eff::Complete));
    rig.server.wait_seen("CompleteMultipart", 2).await;
    assert_eq!(rig.server.seen().iter().filter(|seen| seen.kind == Kind::Put).count(), 3);
    let _ = rig.outcome(&retried).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_retry() {
    let cell = want(St::Failed, &Ctx::ready(), In::Retry);
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    rig.server.auto(Kind::Put, Reply::Refuse);
    accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;
    wait_shown(&rig, Some(WireUploadStatus::Failed)).await;

    let retried = within(rig.db.retry(rig.epoch(), "Moment".into(), ROW.into(), Some("upload".into()), None)).await.unwrap();

    assert_eq!(cell.next, St::Ready);
    assert_eq!(kind_count(&rig), 2);
    assert!(matches!(rig.outcome(&retried).await, WireOperationOutcome::Confirmed { .. }));
}

fn kind_count(rig: &Rig) -> usize {
    let server: &Arc<Server> = &rig.server;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while server.seen().iter().filter(|seen| seen.kind == Kind::Put).count() < 2 {
        assert!(std::time::Instant::now() < deadline, "the second step did not come");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    server.seen().iter().filter(|seen| seen.kind == Kind::Put).count()
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_discard() {
    let cell = want(St::Failed, &Ctx { parts_all_confirmed: true, ..multipart_ctx() }, In::Discard);
    let directory = tempfile::tempdir().unwrap();
    let rig = failed_after_all_parts(directory.path()).await;

    within(rig.db.discard(rig.epoch(), "Moment".into(), ROW.into(), Some("upload".into()))).await.unwrap();

    assert!(cell.eff.contains(&Eff::Abort));
    rig.server.wait_seen("AbortMultipart", 1).await;
    wait_shown(&rig, shows(&cell).unwrap()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_expired() {
    let cell = want(St::Failed, &Ctx { age_days: 8, ..multipart_ctx() }, In::Expire);
    let directory = tempfile::tempdir().unwrap();
    let rig = failed_after_all_parts(directory.path()).await;

    rig.clock.advance(8 * 24 * 3_600_000);
    rig.db.set_online(false).unwrap();
    rig.db.set_online(true).unwrap();

    assert!(cell.eff.contains(&Eff::Abort));
    wait_shown(&rig, shows(&cell).unwrap()).await;
    rig.server.wait_seen("AbortMultipart", 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_closing() {
    let cell = want(St::Uploading, &Ctx::ready(), In::Closing);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite").to_string_lossy().to_string();
    let rig = Rig::open_at(OnFailure::Rollback, Some(path.clone()), Server::new()).await;
    seed_rows(&rig, 1).await;
    rig.server.hold(Kind::Put, true);
    let receipt = accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;
    rig.server.wait_held(Kind::Put, 1).await;

    within(rig.db.close_database()).await.unwrap();

    assert_eq!(cell.next, St::Ready);
    assert!(matches!(rig.outcome(&receipt).await, WireOperationOutcome::Interrupted));
    until("the step is cancelled", || rig.server.cancelled() >= 1).await;
    let server = Server::new();
    let reopened = Rig::open_at(OnFailure::Rollback, Some(path), server.clone()).await;
    until("the step continues at the next open", || server.seen().iter().any(|seen| seen.kind == Kind::Put)).await;
    drop(reopened);
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_round_request() {
    let cell = round_step(Round::None, 1, RoundIn::Needed(1), true).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    let events = Arc::new(Events(Mutex::new(vec![])));
    rig.db.set_upload_accepted_sink(events.clone());
    rig.server.hold(Kind::Put, true);

    accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;

    assert_eq!(cell.schedule_job, Some(1));
    until("the job request", || !events.0.lock().unwrap().is_empty()).await;
    assert_eq!(events.0.lock().unwrap()[0].epoch, rig.epoch());
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_round_started() {
    let cell = round_step(Round::Requested(1), 1, RoundIn::Started(7, 1), true).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    rig.server.hold(Kind::Put, true);
    accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;

    let owner = rig.db.round_begin(rig.epoch()).unwrap();

    assert!(matches!(cell.next, Round::Running { .. }));
    assert!(owner.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_round_refused() {
    let cell = round_step(Round::Requested(1), 1, RoundIn::Refused(1), true).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    let events = Arc::new(Events(Mutex::new(vec![])));
    rig.db.set_upload_accepted_sink(events.clone());
    rig.db.set_online(false).unwrap();
    accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;
    until("the job request", || !events.0.lock().unwrap().is_empty()).await;
    let event = events.0.lock().unwrap()[0].clone();

    rig.db.background_refused(rig.epoch(), event.items, "refused".into()).unwrap();

    assert!(cell.refused_error);
    let entry = rig.row(ROW).await.unwrap().uploads.into_iter().find(|entry| entry.action == "upload").unwrap();
    assert!(entry.upload.error.is_some(), "the refusal is not on the accepted operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_round_end() {
    let cell = round_step(Round::Running { id: 7, epoch: 1, next: None }, 1, RoundIn::Ended(7, 1), false).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    rig.server.hold(Kind::Put, true);
    accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;
    let owner = rig.db.round_begin(rig.epoch()).unwrap().expect("a round starts");

    let late = rig.db.round_end(rig.epoch(), owner).unwrap();

    assert_eq!((cell.next, cell.schedule_job), (Round::None, None));
    assert!(late.map_or(true, |groups| groups.is_empty()));
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_round_end_stale() {
    let cell = round_step(Round::Running { id: 7, epoch: 1, next: None }, 1, RoundIn::Ended(8, 1), false).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    rig.server.hold(Kind::Put, true);
    accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;
    let owner = rig.db.round_begin(rig.epoch()).unwrap().expect("a round starts");

    let _ = rig.db.round_end(rig.epoch(), owner + 1);

    assert!(matches!(cell.next, Round::Running { .. }));
    assert_ne!(rig.db.round_next(rig.epoch(), owner).unwrap(), crate::ffi::WireRoundStep::Stale, "a stale end stopped the running round");
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_outcome() {
    for refuse in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let rig = seeded(OnFailure::Keep).await;
        if refuse { rig.server.script("MakeMoment", Reply::Refuse); }
        let receipt = accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;

        let first = rig.outcome(&receipt).await;
        let second = rig.outcome(&receipt).await;

        assert_eq!(first, second);
        if refuse {
            assert!(matches!(first, WireOperationOutcome::Rejected { .. }), "{first:?}");
            assert_eq!(shown(&rig).await, Some(WireUploadStatus::Failed));
        } else {
            assert!(matches!(first, WireOperationOutcome::Confirmed { .. }), "{first:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_abort_rule() {
    let directory = tempfile::tempdir().unwrap();
    let (rig, path) = multipart_rig(directory.path()).await;
    rig.server.auto(Kind::Put, Reply::Refuse);
    accept(&rig, &path, "big.mp4").await;
    wait_shown(&rig, Some(WireUploadStatus::Failed)).await;
    rig.server.wait_seen("AbortMultipart", 1).await;
    assert_eq!(rig.server.count("AbortMultipart"), 1, "a refused part aborts once");

    let directory = tempfile::tempdir().unwrap();
    let rig = failed_after_all_parts(directory.path()).await;
    quiet().await;
    assert_eq!(rig.server.count("AbortMultipart"), 0, "no abort after CompleteError");
    within(rig.db.discard(rig.epoch(), "Moment".into(), ROW.into(), Some("upload".into()))).await.unwrap();
    rig.server.wait_seen("AbortMultipart", 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_round_covers() {
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    let events = Arc::new(Events(Mutex::new(vec![])));
    rig.db.set_upload_accepted_sink(events.clone());
    rig.server.hold(Kind::Put, true);
    let rows: Vec<String> = std::iter::once(ROW.to_string()).chain(seed_rows(&rig, 1).await).collect();
    for (name, row) in ["a.mp4", "b.mp4"].into_iter().zip(rows) {
        within(rig.db.upload(rig.epoch(), "Moment".into(), row, "upload".into(), request(&source(directory.path(), name, 1024), name))).await.unwrap();
    }

    until("a job request per accepted transfer", || events.0.lock().unwrap().iter().map(|event| event.items.len()).sum::<usize>() >= 2).await;

    assert!(events.0.lock().unwrap().iter().all(|event| event.epoch == rig.epoch()));
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_order() {
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    rig.server.hold(Kind::Put, true);
    let rows = seed_rows(&rig, 2).await;
    let rows: Vec<String> = std::iter::once(ROW.to_string()).chain(rows).collect();
    let names = ["a.mp4", "b.mp4", "c.mp4"];
    for (name, row) in names.into_iter().zip(rows) {
        within(rig.db.upload(rig.epoch(), "Moment".into(), row, "upload".into(), request(&source(directory.path(), name, 1024), name))).await.unwrap();
    }

    until("three creates", || rig.server.count("CreateBlob") >= 3).await;

    let order: Vec<_> = rig.server.seen().into_iter().filter(|seen| seen.operation == "CreateBlob").map(|seen| seen.variables["filename"].as_str().unwrap_or("").to_string()).collect();
    assert_eq!(order, names);
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_idle() {
    let directory = tempfile::tempdir().unwrap();
    let rig = seeded(OnFailure::Rollback).await;
    let receipt = accept(&rig, &source(directory.path(), "a.mp4", 1024), "a.mp4").await;
    let _ = rig.outcome(&receipt).await;
    quiet().await;
    let before = rig.server.seen().len();

    rig.clock.advance(3_600_000);
    quiet().await;

    assert_eq!(rig.server.seen().len(), before);
}
