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
    let shown: usize = steps.iter().map(|out| out.shows().len()).sum();
    assert!(shown >= 2, "the steps show nothing: {shown}");
    for (index, out) in steps.iter().enumerate() {
        assert_eq!(out.stored_shows(), live(out.shows()), "step {index}: the value the readers get is the value of the row");
    }
}
