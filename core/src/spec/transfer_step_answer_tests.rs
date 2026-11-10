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

/// xfer_step_answer_restart (K1, D-246/D-269, transfer.md:108 rule 2): Session committed the pair answer and
/// removed pending, then the process died before the Transfer PersistingAttempt commit. On reopen the saved
/// answer must be read by request_id and applied as the step answer (Restored carries it), so the step is served
/// from the save without a fresh Session capture: the attempt is written on open and the step goes out once.
/// Prod keeps the answer only in Transfer memory (transfer.seeded) and the restored row carries no saved answer,
/// so the restored held pair waits for Session to re-drive it, repeating the step (a second blob).
#[test]
fn xfer_step_answer_restart() {
    let mut answered = saved();
    answered.saved_answer = Some((STEP, crate::store::session_state::StoredAnswer::Ok("{\"ok\":true}".into())));
    let (mut twin, _) = Twin::opened(vec![answered], Default::default());
    twin.online(true);
    let ready = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert_eq!(
        (ready.writes(), twin.stage(1)),
        (1, TransferStage::PersistingAttempt),
        "rule 2: the saved pair answer is applied on open, the attempt is written without a fresh StepAnswer: {ready:?}",
    );
    let started = twin.commit();
    assert_eq!(
        (started.steps().len(), twin.stage(1)),
        (1, TransferStage::Uploading),
        "the committed attempt sends the step once, served from the saved answer",
    );
}

#[test]
fn xfer_step_answer_stray() {
    let mut twin = held();
    let out = twin.post(TransferInput::StepAnswer { request_id: STEP + 1, outcome: AuthorOutcome::Dropped });
    assert!(Twin::quiet(&out), "a request id of no held transfer changes nothing: {out:?}");
    assert_eq!(twin.stage(1), TransferStage::Ready, "the held transfer stays Ready");
}
