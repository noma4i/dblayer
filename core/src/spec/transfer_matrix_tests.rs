//! Owner-level tests of the owner `Transfer`: rows of the matrix: writes the Store refuses, discard against a confirmed mutation, `urgent` in power saving.

use std::collections::BTreeMap;

use super::transfer_harness::*;
use crate::owners::contract::RejectReason;
use crate::owners::store::UploadKind;
use crate::owners::write::outbox::{Fault, OutboxOutcome, ReserveTarget};
use crate::owners::write::transfer::{Shown, TransferAnswer, TransferId, TransferInput, TransferStage};

const KEY: &str = "Moment:r1:upload";

fn stage_in(name: &str, twin: &mut Twin) -> TransferId {
    match name {
        "PersistingCopy" => twin.persisting_copy(KEY, 2),
        "PersistingPart" => {
            let id = twin.uploading(KEY, 2);
            twin.step_ok(id, 1, Some("u"));
            id
        }
        "PersistingComplete" => twin.persisting_complete(KEY),
        "PersistingFailure" => twin.persisting_failure(KEY),
        "PersistingRetry" => {
            let id = twin.failed(KEY);
            twin.retry(KEY, None);
            id
        }
        "Removing" => {
            let id = twin.uploading(KEY, 2);
            twin.cancel(KEY);
            id
        }
        other => panic!("{other}"),
    }
}

#[test]
fn xfer_persisting_invalid() {
    for name in ["PersistingPart", "PersistingComplete"] {
        let mut twin = Twin::new();
        let id = stage_in(name, &mut twin);
        let out = twin.reject(RejectReason::Invalid(String::new()));
        assert_eq!(twin.stage(id), TransferStage::PersistingFailure, "{name}");
        assert_eq!(out.kinds(), vec![UploadKind::Failed], "{name}: nothing is observable before Committed");
        assert!(out.shows().is_empty(), "{name}");
        twin.commit();
        assert_eq!(twin.stage(id), TransferStage::Failed, "{name}");
        if name == "PersistingComplete" {
            let (_, retried) = twin.retry(KEY, None);
            let after = [retried, twin.commit(), twin.commit()];
            let sent = (after.iter().map(|out| out.steps().len()).sum::<usize>(), after.iter().map(|out| out.completes()).sum::<usize>());
            assert_eq!(sent, (0, 1), "the retry goes straight to `complete`, no part is sent");
        }
    }
}

#[test]
fn xfer_persisting_failure_invalid() {
    let mut twin = Twin::new();
    let id = twin.persisting_failure(KEY);
    let error = twin.t(id).error.clone().expect("the failure has a cause");
    let (waiter, _) = twin.outcome(id, 1);
    let out = twin.reject(RejectReason::Invalid(String::new()));
    assert_eq!(twin.stage(id), TransferStage::Failed);
    assert!(matches!(out.shows().as_slice(), [Shown::Failed { error: found }] if *found == error), "{:?}", out.shows());
    assert!(matches!(out.replies(waiter).as_slice(), [TransferAnswer::Outcome(OutboxOutcome::Failed { error: found }, None)] if *found == error));
    assert_eq!((out.writes(), out.round_idle()), (0, vec![id]), "the mark stays in memory");
}

#[test]
fn xfer_mutation_confirmed_discard_waiting() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let (reply, out) = twin.discard(KEY);
    assert_eq!((out.outbox_discards(), out.replies(reply).len()), (1, 0));
    let out = twin.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 3, result: None, row_id: None });
    assert_eq!(twin.stage(id), TransferStage::Removing);
    assert_eq!(Twin::fault(&out, reply), Some(Fault::NotFound), "the mutation was confirmed before the discard");
    let late = twin.post(TransferInput::DiscardOutcome { key: KEY.into(), result: Ok(()) });
    assert!(Twin::quiet(&late), "{late:?}");
}

#[test]
fn xfer_mutation_interrupted_discard_waiting() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let (reply, _) = twin.discard(KEY);
    let out = twin.mutation_outcome(KEY, OutboxOutcome::Ended(crate::ffi::EndCause::SessionEnded));
    assert_eq!(twin.stage(id), TransferStage::Done);
    assert_eq!(Twin::fault(&out, reply), Some(Fault::Ended(crate::ffi::EndCause::SessionEnded)));
    assert_eq!(out.writes(), 0, "the row stays in the base");
}

#[test]
fn xfer_failed_retry_restore_failed() {
    let mut row = stored(1, KEY, 2);
    row.restore_failed = Some("layout".into());
    let (mut twin, _) = Twin::opened(vec![row], BTreeMap::new());
    assert_eq!(twin.stage(1), TransferStage::Failed);
    let (reply, out) = twin.retry(KEY, None);
    assert_eq!(Twin::fault(&out, reply), Some(Fault::RestoreFailed));
    assert_eq!(out.writes(), 0);
    assert_eq!(twin.stage(1), TransferStage::Failed);
}

#[test]
fn xfer_completing_power_saving_urgent() {
    let mut twin = Twin::new();
    let reply = twin.reply();
    let mut urgent = item(KEY, 1);
    urgent.urgent = true;
    twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(urgent), reply });
    let id = twin.state().open_id(KEY).expect("an urgent transfer");
    twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(id) });
    twin.commit();
    twin.copy_done(id);
    twin.commit();
    twin.commit();
    twin.step_ok(id, 1, None);
    twin.commit();
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Completing);
    let out = twin.post(TransferInput::PowerSaving(true));
    assert_eq!(twin.stage(id), TransferStage::Completing);
    assert!(Twin::quiet(&out), "an urgent transfer goes on: {out:?}");
    let mut plain = Twin::new();
    let id = plain.completing(KEY);
    plain.post(TransferInput::PowerSaving(true));
    assert_eq!(plain.stage(id), TransferStage::Ready, "a plain one waits for the end of power saving");
}

