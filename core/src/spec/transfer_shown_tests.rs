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
