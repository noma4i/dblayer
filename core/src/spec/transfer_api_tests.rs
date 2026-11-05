use super::ref_transfer::{round_step, step_of, Ctx, Obs, Round, RoundIn, St};
use super::rig::{Kind, Reply, Rig};
use crate::schema::OnFailure;
use crate::transport::NetworkPurpose;

#[tokio::test(flavor = "multi_thread")]
async fn xfer_copy_done() {
    let cell = step_of(St::Copying, &Ctx::ready(), super::ref_transfer::In::CopyDone).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.db.hold_copy_for_testing();
    rig.upload_small("a.mp4").await;
    assert_eq!(rig.upload_status().await, Some(crate::ffi::WireUploadStatus::Preparing));
    rig.db.release_copy_for_testing();
    assert_eq!(cell.next, St::Ready);
    rig.server.wait_seen("CreateBlob", 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_copy_failed() {
    let cell = step_of(St::Copying, &Ctx::ready(), super::ref_transfer::In::CopyFailed).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.db.hold_copy_for_testing();
    rig.upload_small("a.mp4").await;
    rig.db.fail_copy_for_testing();
    assert_eq!(cell.next, St::Failed);
    assert_eq!(rig.upload_status().await, Some(crate::ffi::WireUploadStatus::Failed));
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_copy_cancel() {
    let cell = step_of(St::Copying, &Ctx::ready(), super::ref_transfer::In::Cancel).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.db.hold_copy_for_testing();
    rig.upload_small("a.mp4").await;
    assert!(rig.db.cancel_upload(rig.epoch(), "Moment".into(), "42".into(), "upload".into()).await.unwrap());
    assert!(cell.obs.contains(&Obs::CancelTrue));
    assert!(rig.db.copy_cancelled_for_testing());
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_epoch_ended() {
    let cell = step_of(St::Uploading, &Ctx::ready(), super::ref_transfer::In::EpochEnded).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Put, true);
    let receipt = rig.upload_small("a.mp4").await;
    rig.server.wait_held(Kind::Put, 1).await;
    rig.db.end_session_for_testing(crate::ffi::EpochCause::SessionReset).await.unwrap();
    assert_eq!(cell.next, St::Done);
    assert_eq!(rig.outcome(&receipt).await, crate::ffi::WireOperationOutcome::Interrupted);
    assert!(rig.server.cancelled() >= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_round_needed_running() {
    let cell = round_step(Round::Running { id: 7, epoch: 1, next: None }, 1, RoundIn::Needed(1), true).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    let events = rig.watch_job_requests();
    rig.upload_small("a.mp4").await;
    let owner = rig.db.round_begin(rig.epoch()).unwrap().unwrap();
    rig.upload_small("b.mp4").await;
    assert_eq!(cell.schedule_job, None);
    assert_eq!(events.count_after_begin(owner), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_round_needed_new_epoch() {
    let cell = round_step(Round::Running { id: 7, epoch: 1, next: None }, 2, RoundIn::Needed(2), true).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.upload_small("a.mp4").await;
    let owner = rig.db.round_begin(rig.epoch()).unwrap().unwrap();
    rig.db.end_session_for_testing(crate::ffi::EpochCause::SessionReset).await.unwrap();
    rig.sign_in_again().await;
    rig.upload_small("b.mp4").await;
    assert!(matches!(cell.next, Round::Running { next: Some(2), .. }));
    assert!(rig.db.round_end(rig.epoch() - 2, owner).unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_round_end_next() {
    let cell = round_step(Round::Running { id: 7, epoch: 1, next: Some(2) }, 2, RoundIn::Ended(7, 1), true).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.upload_small("a.mp4").await;
    let owner = rig.db.round_begin(rig.epoch()).unwrap().unwrap();
    rig.db.end_session_for_testing(crate::ffi::EpochCause::SessionReset).await.unwrap();
    rig.sign_in_again().await;
    rig.upload_small("b.mp4").await;
    let late = rig.db.round_end(rig.epoch() - 2, owner).unwrap().unwrap();
    assert_eq!(cell.schedule_job, Some(2));
    assert_eq!(late.iter().map(|group| group.epoch).collect::<Vec<_>>(), [rig.epoch()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_round_epoch_ended() {
    let cell = round_step(Round::Running { id: 7, epoch: 1, next: None }, 2, RoundIn::EpochEnded(1), true).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.upload_small("a.mp4").await;
    let owner = rig.db.round_begin(rig.epoch()).unwrap().unwrap();
    let epoch = rig.epoch();
    rig.db.end_session_for_testing(crate::ffi::EpochCause::SessionReset).await.unwrap();
    assert!(matches!(cell.next, Round::Running { .. }));
    assert!(rig.db.round_end(epoch, owner).is_ok(), "the platform ends the round with the epoch it began in");
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_purpose() {
    let rig = Rig::open(OnFailure::Rollback).await;
    let receipt = rig.upload_small("a.mp4").await;
    let _ = rig.outcome(&receipt).await;
    let steps: Vec<_> = rig.server.seen().into_iter().filter(|seen| matches!(seen.operation.as_str(), "CreateBlob" | "MakeMoment" | "put" | "InitMultipart" | "PresignParts" | "CompleteMultipart" | "AbortMultipart")).collect();
    assert!(steps.iter().all(|seen| seen.purpose == format!("{:?}", NetworkPurpose::Upload)), "{steps:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_epoch() {
    for hold in [Kind::Put, Kind::Mutate] {
        let rig = Rig::open(OnFailure::Rollback).await;
        rig.server.hold(hold, true);
        rig.upload_small("a.mp4").await;
        rig.server.wait_held(hold, 1).await;
        rig.db.end_session_for_testing(crate::ffi::EpochCause::SessionReset).await.unwrap();
        let before = rig.server.seen().len();
        rig.server.hold(hold, false);
        rig.clock.advance(3_600_000);
        super::rig::quiet().await;
        assert_eq!(rig.server.seen().len(), before, "a transfer of an ended epoch sent a request");
        let _ = Reply::Ok;
    }
}
