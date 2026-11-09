//! Owner-level tests of the two-sided row selector without an action (D-259): the Transfer side of `SelectRow`, `SelectConfirmed` and the end of the epoch.

use super::transfer_harness::*;
use crate::ffi::EndCause;
use crate::owners::write::outbox::{Fault, OutboxInput, SelectKind};
use crate::owners::write::transfer::{TransferAnswer, TransferInput};

const KEY: &str = "Moment:r1:upload";

fn select_row(reply: u64, kind: SelectKind, outbox_min: Option<(u64, String)>, version: u64) -> TransferInput {
    TransferInput::SelectRow { model: "Moment".into(), rows: vec!["r1".into()], outbox_min, kind, reply, version }
}

/// The `SelectedTransfer{reply, transfer_id, version}` messages the owner handed to Outbox.
fn to_transfer(out: &Out) -> Vec<(u64, u64, u64)> {
    out.outbox.iter().filter_map(|input| if let OutboxInput::SelectedTransfer { reply, transfer_id, version } = input { Some((*reply, *transfer_id, *version)) } else { None }).collect()
}

fn to_outbox(out: &Out) -> Vec<(u64, String, String, SelectKind, u64)> {
    out.outbox.iter().filter_map(|input| if let OutboxInput::SelectedOutbox { reply, row_id, action, kind, version, .. } = input { Some((*reply, row_id.clone(), action.clone(), *kind, *version)) } else { None }).collect()
}

/// A `SelectRow` won by an unfinished transfer of the row: no change, `SelectedTransfer` to Outbox, nothing to the caller.
#[test]
fn xfer_select_transfer() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 2);
    let before = twin.stage(id);
    let reply = twin.reply();
    let out = twin.post(select_row(reply, SelectKind::Discard, None, 9));
    assert_eq!((out.outbox.len(), to_transfer(&out)), (1, vec![(reply, id, 9)]));
    assert_eq!((out.all_replies(), twin.stage(id)), (0, before));
}

/// `SelectConfirmed` of a waiting reply: the choice acts as the Retry of that transfer and answers the caller by that cell.
#[test]
fn xfer_select_confirmed() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 2);
    let reply = twin.reply();
    let won = twin.post(select_row(reply, SelectKind::Retry, None, 3));
    assert_eq!((won.outbox.len(), to_transfer(&won), won.all_replies()), (1, vec![(reply, id, 3)], 0));
    let acted = twin.post(TransferInput::SelectConfirmed { reply });
    assert!(matches!(acted.replies(reply).as_slice(), [TransferAnswer::Receipt(_)]));
}

/// A second `SelectRow` of a reply already in `selecting`: the first choice drops, the new `outbox_min` decides.
#[test]
fn xfer_select_replaced() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 2);
    let reply = twin.reply();
    let first = twin.post(select_row(reply, SelectKind::Discard, None, 1));
    assert_eq!((first.outbox.len(), to_transfer(&first)), (1, vec![(reply, id, 1)]));
    let second = twin.post(select_row(reply, SelectKind::Discard, Some((0, "update".into())), 2));
    assert_eq!((second.outbox.len(), to_outbox(&second)), (1, vec![(reply, "r1".into(), "update".into(), SelectKind::Discard, 2)]));
    let stray = twin.post(TransferInput::SelectConfirmed { reply });
    assert!(Twin::quiet(&stray));
}

/// `SelectConfirmed` of a reply in no `selecting` (R2): nothing.
#[test]
fn xfer_select_confirmed_stray() {
    let mut twin = Twin::new();
    twin.uploading(KEY, 2);
    let out = twin.post(TransferInput::SelectConfirmed { reply: 4242 });
    assert!(Twin::quiet(&out));
}

/// The end of the epoch clears `selecting` and answers the waiting call with the end cause (D-264).
#[test]
fn xfer_select_epoch_end() {
    for (closing, cause) in [(false, EndCause::SessionEnded), (true, EndCause::Closed)] {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 2);
        let reply = twin.reply();
        let won = twin.post(select_row(reply, SelectKind::Discard, None, 5));
        assert_eq!((won.outbox.len(), to_transfer(&won), won.all_replies()), (1, vec![(reply, id, 5)], 0));
        let ended = if closing { twin.close() } else { twin.end_epoch() };
        assert_eq!(ended.replies(reply), vec![TransferAnswer::Err(Fault::Ended(cause))]);
    }
}
