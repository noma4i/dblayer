use super::ref_outbox::{step_of, Ctx, Fault, In, Obs, Out, Policy, Reason, St, Why};
use super::rig::{quiet, Kind, Reply, Rig};
use crate::ffi::{DbError, StoreRejection, WireOperationOutcome};
use crate::schema::OnFailure;

fn temp_id(receipt: &crate::ffi::WireOperationReceipt) -> String { receipt.optimistic_value.clone().unwrap().id }

#[tokio::test(flavor = "multi_thread")]
async fn out_submitting_rejected() {
    for reason in [Reason::StaleGeneration, Reason::NoSession, Reason::Storage] {
        let cell = step_of(St::Submitting, &Ctx::ready(), In::Rejected(reason)).unwrap();
        let rig = Rig::open(OnFailure::Rollback).await;
        rig.db.reject_next_commit_for_testing(StoreRejection::of(reason));
        let result = rig.db.send(rig.request("hello")).await;
        assert_eq!(cell.obs, [Obs::Fails(Fault::Reason(reason))]);
        assert!(matches!(result, Err(DbError::Rejected { .. })), "{result:?}");
        assert!(rig.server.mutations() == 0);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn out_queued_discard() {
    let cell = step_of(St::Queued, &Ctx::ready(), In::Discard).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.db.set_online(false).unwrap();
    let receipt = rig.create("a").await;
    rig.db.discard(rig.epoch(), "Moment".into(), temp_id(&receipt), None).await.unwrap();
    assert_eq!(cell.next, St::RollingBack(Why::Discarded));
    assert_eq!(rig.outcome(&receipt).await, WireOperationOutcome::Discarded);
    assert!(rig.row(&temp_id(&receipt)).await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn out_queued_models_reset() {
    let cell = step_of(St::Queued, &Ctx::ready(), In::ModelsReset).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.db.set_online(false).unwrap();
    let receipt = rig.create("a").await;
    rig.db.reset_models_for_testing(vec!["Moment".into()]).await.unwrap();
    assert_eq!(cell.next, St::Queued);
    assert!(rig.row(&temp_id(&receipt)).await.is_none());
    rig.db.set_online(true).unwrap();
    rig.server.wait_seen("MakeMoment", 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn out_sending_reject_keep() {
    let ctx = Ctx { policy: Policy::Keep, ..Ctx::ready() };
    let cell = step_of(St::Sending, &ctx, In::TransportError { temporary: false }).unwrap();
    let rig = Rig::open(OnFailure::Keep).await;
    rig.server.script("MakeMoment", Reply::Refuse);
    let receipt = rig.create("a").await;
    assert!(cell.obs.contains(&Obs::Outcome(Out::Failed)));
    assert!(matches!(rig.outcome(&receipt).await, WireOperationOutcome::Failed { .. }));
    assert!(rig.row(&temp_id(&receipt)).await.unwrap().sync.error.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn out_sending_discard() {
    let cell = step_of(St::Sending, &Ctx::ready(), In::Discard).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let receipt = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    let result = rig.db.discard(rig.epoch(), "Moment".into(), temp_id(&receipt), None).await;
    assert_eq!(cell.obs, [Obs::Fails(Fault::InFlight)]);
    assert!(matches!(result, Err(DbError::InFlight { .. })), "{result:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn out_landing_stale_generation() {
    let cell = step_of(St::Landing, &Ctx::ready(), In::Rejected(Reason::StaleGeneration)).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let receipt = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    rig.db.reset_models_for_testing(vec!["Moment".into()]).await.unwrap();
    rig.server.answer(Kind::Mutate, Reply::Ok);
    assert_eq!(cell.next, St::Landing);
    assert!(matches!(rig.outcome(&receipt).await, WireOperationOutcome::Confirmed { .. }));
    assert!(rig.row("srv1").await.is_some(), "the confirmed answer lands in the new generation");
}

#[tokio::test(flavor = "multi_thread")]
async fn out_landing_no_session() {
    let cell = step_of(St::Landing, &Ctx::ready(), In::Rejected(Reason::NoSession)).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let receipt = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    rig.db.reject_next_commit_for_testing(StoreRejection::NoSession);
    rig.server.answer(Kind::Mutate, Reply::Ok);
    assert_eq!(cell.obs, [Obs::Outcome(Out::Interrupted)]);
    assert_eq!(rig.outcome(&receipt).await, WireOperationOutcome::Interrupted);
}

#[tokio::test(flavor = "multi_thread")]
async fn out_landing_storage() {
    let cell = step_of(St::Landing, &Ctx::ready(), In::Rejected(Reason::Storage)).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let receipt = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    rig.db.reject_next_commit_for_testing(StoreRejection::Storage);
    rig.server.answer(Kind::Mutate, Reply::Ok);
    quiet().await;
    assert_eq!(cell.next, St::Landing);
    assert!(rig.pending(&receipt).await);
    rig.clock.advance(60_000);
    assert!(matches!(rig.outcome(&receipt).await, WireOperationOutcome::Confirmed { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn out_rollback_stale_generation() {
    let cell = step_of(St::RollingBack(Why::Rejected), &Ctx::ready(), In::Rejected(Reason::StaleGeneration)).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let receipt = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    rig.db.reject_next_commit_for_testing(StoreRejection::StaleGeneration);
    rig.server.answer(Kind::Mutate, Reply::Refuse);
    assert_eq!(cell.next, St::RollingBack(Why::Rejected));
    assert!(matches!(rig.outcome(&receipt).await, WireOperationOutcome::Rejected { .. }));
    assert!(rig.row(&temp_id(&receipt)).await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn out_rollback_no_session() {
    let cell = step_of(St::RollingBack(Why::Rejected), &Ctx::ready(), In::Rejected(Reason::NoSession)).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let receipt = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    rig.db.reject_next_commit_for_testing(StoreRejection::NoSession);
    rig.server.answer(Kind::Mutate, Reply::Refuse);
    assert_eq!(cell.obs, [Obs::Outcome(Out::Interrupted)]);
    assert_eq!(rig.outcome(&receipt).await, WireOperationOutcome::Interrupted);
}

#[tokio::test(flavor = "multi_thread")]
async fn out_failed_discard() {
    let ctx = Ctx { policy: Policy::Keep, ..Ctx::ready() };
    let cell = step_of(St::Failed, &ctx, In::Discard).unwrap();
    let rig = Rig::open(OnFailure::Keep).await;
    rig.server.script("MakeMoment", Reply::Refuse);
    let receipt = rig.create("a").await;
    quiet().await;
    rig.db.discard(rig.epoch(), "Moment".into(), temp_id(&receipt), None).await.unwrap();
    assert_eq!(cell.next, St::RollingBack(Why::Discarded));
    assert_eq!(rig.outcome(&receipt).await, WireOperationOutcome::Discarded);
    assert!(rig.row(&temp_id(&receipt)).await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn out_epoch_ended() {
    let cell = step_of(St::Sending, &Ctx::ready(), In::EpochEnded).unwrap();
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let receipt = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    rig.db.end_session_for_testing(crate::ffi::EpochCause::SessionReset).await.unwrap();
    assert_eq!(cell.obs, [Obs::Outcome(Out::Interrupted)]);
    assert_eq!(rig.outcome(&receipt).await, WireOperationOutcome::Interrupted);
    assert!(rig.server.cancelled() >= 1);
    rig.server.hold(Kind::Mutate, false);
    quiet().await;
    assert_eq!(rig.server.mutations(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn out_outcome_stale() {
    let rig = Rig::open(OnFailure::Rollback).await;
    let receipt = rig.create("a").await;
    rig.db.end_session_for_testing(crate::ffi::EpochCause::SessionReset).await.unwrap();
    let result = rig.db.outcome(receipt).await;
    assert!(matches!(result, Err(DbError::NoSession { .. })), "{result:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn out_prop_no_send_after_end() {
    for hold in [false, true] {
        let rig = Rig::open(OnFailure::Rollback).await;
        rig.db.set_online(false).unwrap();
        rig.server.hold(Kind::Mutate, hold);
        rig.create("a").await;
        rig.db.end_session_for_testing(crate::ffi::EpochCause::SessionReset).await.unwrap();
        let before = rig.server.mutations();
        rig.db.set_online(true).unwrap();
        quiet().await;
        assert_eq!(rig.server.mutations(), before, "an operation of an ended epoch reached the transport");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn out_prop_confirmed_lands() {
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let receipt = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    rig.db.reset_models_for_testing(vec!["Moment".into()]).await.unwrap();
    rig.server.answer(Kind::Mutate, Reply::Ok);
    let WireOperationOutcome::Confirmed { revision, .. } = rig.outcome(&receipt).await else { panic!("not confirmed") };
    assert!(rig.row_at_revision("srv1", revision).await.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn out_prop_membership_revision() {
    let rig = Rig::open(OnFailure::Rollback).await;
    rig.server.hold(Kind::Mutate, true);
    let receipt = rig.create("a").await;
    rig.server.wait_held(Kind::Mutate, 1).await;
    rig.server.script_list_answer("moments", 0, vec!["srv9"]);
    rig.db.refresh(rig.epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    assert!(rig.member_of_list("feed", &temp_id(&receipt)).await, "an older list answer removed the membership of a pending operation");
}
