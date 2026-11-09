//! Owner-level tests of the owner `Transfer`: the saved answer of a restored step (D-269).

use super::transfer_harness::*;
use crate::owners::session::AuthorOutcome;
use crate::owners::write::transfer::{StoredTransfer, TransferEffect, TransferInput, TransferStage};

const KEY: &str = "Moment:r1:upload";
const STEP: u64 = 7;

fn saved() -> StoredTransfer {
    let mut found = stored(1, KEY, 2);
    found.pending_step = Some(STEP);
    found
}

fn held() -> Twin {
    let (mut twin, _) = Twin::opened(vec![saved()], Default::default());
    twin.online(true);
    twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    twin
}

fn step_effect(out: &Out) -> &TransferEffect {
    out.runs.iter().find(|run| matches!(run, TransferEffect::Step { .. })).expect("one step effect")
}

#[test]
fn xfer_step_answer_response() {
    let mut twin = held();
    let answered = twin.post(TransferInput::StepAnswer { request_id: STEP, outcome: AuthorOutcome::Response { body: "{\"ok\":true}".into(), kind: crate::transport::AnswerKind::Answered { sequence: None } } });
    assert_eq!((answered.steps().len(), twin.stage(1)), (0, TransferStage::PersistingAttempt), "Response holds no step until the attempt is written");
    let started = twin.commit();
    assert_eq!((started.steps().len(), twin.stage(1)), (1, TransferStage::Uploading), "the committed attempt sends one step");

    let mut plain = held();
    plain.post(TransferInput::StepAnswer { request_id: STEP, outcome: AuthorOutcome::Dropped });
    let dropped = plain.commit();
    assert_ne!(step_effect(&started), step_effect(&dropped), "D-269: the seeded Response records the saved answer, so its Step carries the pair, unlike the plain Dropped step");
}

#[test]
fn xfer_step_answer_failed() {
    for outcome in [AuthorOutcome::Dropped, AuthorOutcome::Error { class: crate::owners::contract::ErrorClass::Permanent, error: crate::owners::session::SessionError::Http { status: 0 }, pending_retained: false }] {
        let mut twin = held();
        let answered = twin.post(TransferInput::StepAnswer { request_id: STEP, outcome });
        assert_eq!((answered.steps().len(), twin.stage(1)), (0, TransferStage::PersistingAttempt), "Error or Dropped clears the held pair and writes a plain attempt first");
        let started = twin.commit();
        assert_eq!((started.steps().len(), twin.stage(1)), (1, TransferStage::Uploading), "the committed attempt sends one plain step");
    }
}

#[test]
fn xfer_step_answer_stray() {
    let mut twin = held();
    let out = twin.post(TransferInput::StepAnswer { request_id: STEP + 1, outcome: AuthorOutcome::Dropped });
    assert!(Twin::quiet(&out), "a request id of no held transfer changes nothing: {out:?}");
    assert_eq!(twin.stage(1), TransferStage::Ready, "the held transfer stays Ready");
}
