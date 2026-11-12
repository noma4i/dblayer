//! Owner-level tests of the owner `Transfer`: the shown value of a transfer reaches `sync_upload` (D-226).

use super::transfer_harness::*;
use crate::owners::write::transfer::{Shown, TransferInput};

const KEY: &str = "Moment:r1:upload";

fn live(shows: Vec<Shown>) -> Vec<Shown> { shows.into_iter().filter(|shown| *shown != Shown::Gone).collect() }

#[test]
fn xfer_every_show_is_stored() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let steps = [
        twin.post(TransferInput::RoundStatus { epoch: EPOCH, transfer_ids: vec![id], refused: Some("denied".into()) }),
        twin.post(TransferInput::RoundStatus { epoch: EPOCH, transfer_ids: vec![id], refused: None }),
        twin.online(false),
        twin.online(true),
        twin.step_ok(id, 1, None),
    ];
    let mut shown = 0;
    for (index, out) in steps.iter().enumerate() {
        shown += out.shows().len();
        assert_eq!(out.stored_shows(), live(out.shows()), "step {index}: the value the readers get is the value of the row");
    }
    assert!(shown >= 2, "the steps show nothing: {shown}");
}

#[test]
fn xfer_show_publishes_after_committed() {
    let mut twin = Twin::new();
    twin.uploading(KEY, 3);
    twin.hold_shown = true;
    let out = twin.online(false);
    let batches = out.shown_batches();
    assert_eq!((batches.len(), out.shows()), (1, Vec::new()), "a value that waits for the Store is not published before Committed");
    let committed = twin.commit_shown(batches[0]);
    assert_eq!(committed.shows(), live(out.stored_shows()), "Committed publishes the value of its batch");
}

#[test]
fn xfer_show_rejected_keeps_the_value_and_retries() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.hold_shown = true;
    let out = twin.online(false);
    let rejected = twin.reject_shown(out.shown_batches()[0]);
    assert_eq!(rejected.shows(), Vec::new(), "a refused value is not published");
    let again = twin.post(TransferInput::RoundStatus { epoch: EPOCH, transfer_ids: vec![id], refused: Some("denied".into()) });
    assert_eq!((out.stored_shows(), again.stored_shows()), (vec![Shown::Queued], vec![Shown::Failed { error: "refused".into() }]), "the Store still has the old value: the next show of another value writes it");
}

/// P-XFER-50: a shown value the Store fails to write is written again after the backoff, and Committed publishes it.
#[test]
fn xfer_show_storage_refusal_is_written_again() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.hold_shown = true;
    let out = twin.online(false);
    let rejected = twin.reject_shown(out.shown_batches()[0]);
    assert_eq!((rejected.shows(), rejected.shown.len(), rejected.timers), (Vec::new(), 0, 1), "the refused value is not published and one retry is scheduled: {rejected:?}");
    let again = twin.tick_retry(id);
    assert_eq!((again.stored_shows(), again.shows()), (out.stored_shows(), Vec::new()), "the retry writes the same value, nothing is published before Committed: {again:?}");
    let committed = twin.commit_shown(again.shown_batches()[0]);
    assert_eq!(committed.shows(), live(out.stored_shows()), "Committed of the retry publishes the value");
}

/// P-XFER-50: a shown value refused for a moved generation is written again at once with the generations the Store named.
#[test]
fn xfer_show_stale_generation_is_written_again() {
    let mut twin = Twin::new();
    twin.uploading(KEY, 3);
    twin.hold_shown = true;
    let out = twin.online(false);
    let moved = crate::owners::contract::RejectReason::StaleGeneration { models: vec!["Moment".into()], generations: [("Moment".to_string(), 7)].into() };
    let refused = twin.refuse_shown(out.shown_batches()[0], moved);
    assert_eq!((refused.stored_shows(), refused.shows(), refused.timers), (out.stored_shows(), Vec::new(), 0), "the same value goes again at once: {refused:?}");
    assert_eq!(refused.shown.iter().map(|batch| batch.basis.get("Moment").copied()).collect::<Vec<_>>(), vec![Some(7)], "with the generation the Store named");
}

/// P-XFER-50: a shown value refused as invalid data or after the end of the epoch is not written again.
#[test]
fn xfer_show_refusal_without_retry() {
    use crate::owners::contract::RejectReason;
    for reason in [RejectReason::Invalid("bad row".into()), RejectReason::NoSession, RejectReason::Closed] {
        let mut twin = Twin::new();
        twin.uploading(KEY, 3);
        twin.hold_shown = true;
        let out = twin.online(false);
        let refused = twin.refuse_shown(out.shown_batches()[0], reason.clone());
        assert!(Twin::quiet(&refused) && refused.shown.is_empty(), "{reason:?}: nothing is written, scheduled or published: {refused:?}");
    }
}

/// P-XFER-51: a refused write of the round error changes nothing: the error stays in memory and the next round status writes it.
#[test]
fn xfer_round_error_refusal_is_quiet() {
    use crate::owners::contract::RejectReason;
    for reason in [RejectReason::Storage("disk".into()), RejectReason::Invalid("bad row".into()), RejectReason::StaleGeneration { models: vec!["Moment".into()], generations: [("Moment".to_string(), 7)].into() }, RejectReason::NoSession, RejectReason::Closed] {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        let denied = twin.post(TransferInput::RoundStatus { epoch: EPOCH, transfer_ids: vec![id], refused: Some("denied".into()) });
        assert_eq!(denied.round_errors(), vec![(KEY.to_string(), Some("denied".to_string()))], "{reason:?}");
        let refused = twin.reject(reason.clone());
        assert!(Twin::quiet(&refused) && refused.shown.is_empty(), "{reason:?}: {refused:?}");
        let cleared = twin.post(TransferInput::RoundStatus { epoch: EPOCH, transfer_ids: vec![id], refused: None });
        assert_eq!(cleared.round_errors(), vec![(KEY.to_string(), None)], "{reason:?}: the next round status is written");
    }
}
