//! Owner-level tests of the owner `Transfer`: the acceptance of a file, the copy of the source and the common write of the acceptance.

use super::transfer_harness::*;
use crate::owners::contract::{Online, RejectReason};
use crate::owners::store::UploadKind;
use crate::owners::write::outbox::{Fault, OutboxInput, OutboxOutcome, ReserveTarget};
use crate::owners::write::transfer::{Shown, TransferAnswer, TransferId, TransferInput, TransferStage};
use crate::runtime::Time;

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Moment:r2:upload";

fn batch_input(keys: &[&str], reply: u64) -> TransferInput { TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: keys.iter().map(|key| item(key, 2)).collect(), reply, now: Time(0) } }

fn accepting_batch(twin: &mut Twin, keys: &[&str]) -> (Vec<TransferId>, u64, Out) {
    let reply = twin.reply();
    let out = twin.post(batch_input(keys, reply));
    let ids = keys.iter().map(|key| twin.state().open_id(key).expect("an accepting element")).collect();
    (ids, reply, out)
}

fn copying_batch(twin: &mut Twin, keys: &[&str]) -> (Vec<TransferId>, u64) {
    let (ids, reply, _) = accepting_batch(twin, keys);
    twin.reserve_batch(&ids);
    twin.commit();
    (ids, reply)
}

fn refuses_calls(twin: &mut Twin, key: &str) {
    for (name, (reply, out)) in [("cancel", twin.cancel(key)), ("discard", twin.discard(key)), ("retry", twin.retry(key, None))] {
        assert_eq!(Twin::fault(&out, reply), Some(Fault::InFlight), "{name}");
        assert_eq!(out.writes(), 0, "{name}");
    }
}

#[test]
fn xfer_accept_while_restoring() {
    let mut twin = Twin::bare(1);
    let reply = twin.reply();
    let out = twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(item(KEY, 2)), reply, now: Time(0) });
    assert!(Twin::quiet(&out), "the call waits for the rows of the base: {out:?}");
    let out = twin.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default(), orphans: Vec::new(), batches: Default::default() });
    assert_eq!((out.writes(), out.classified(), out.all_replies()), (0, 1, 0));
    assert_eq!(out.outbox.len(), 1);
}

#[test]
fn xfer_held_accept_end_cause() {
    for (closing, expected) in [(false, Fault::SessionEnded), (true, Fault::Closed)] {
        let mut twin = Twin::bare(1);
        let reply = twin.reply();
        let pending = twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(item(KEY, 2)), reply, now: Time(0) });
        assert!(pending.replies(reply).is_empty());
        let ended = if closing { twin.close() } else { twin.end_epoch() };
        assert_eq!(Twin::fault(&ended, reply), Some(expected));
    }
}

#[test]
fn xfer_accept() {
    let mut twin = Twin::new();
    let reply = twin.reply();
    let out = twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(item(KEY, 3)), reply, now: Time(0) });
    let id = twin.state().open_id(KEY).unwrap();
    assert_eq!(twin.stage(id), TransferStage::Reserving);
    assert!(matches!(out.outbox.as_slice(), [OutboxInput::ReserveOp { target: ReserveTarget::Transfer(found), .. }] if *found == id));
    assert_eq!((out.writes(), out.all_replies(), out.shows().len()), (0, 0, 0));
    assert!(out.replies(reply).is_empty());
}

#[test]
fn xfer_accept_stale() {
    let mut twin = Twin::new();
    let out = twin.post(TransferInput::Accept { epoch: EPOCH + 1, item: Box::new(item(KEY, 2)), reply: 9, now: Time(0) });
    assert_eq!(Twin::fault(&out, 9), Some(Fault::NoSession));
    assert_eq!((out.writes(), twin.state().open_id(KEY)), (0, None));
}

#[test]
fn xfer_accept_invalid() {
    let mut twin = Twin::new();
    let mut empty = item(KEY, 2);
    empty.source.as_mut().unwrap().path.clear();
    let out = twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(empty), reply: 9, now: Time(0) });
    assert_eq!(Twin::fault(&out, 9), Some(Fault::Invalid));
    let out = twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(item(KEY, 0)), reply: 10, now: Time(0) });
    assert_eq!(Twin::fault(&out, 10), Some(Fault::Invalid), "a source without parts");
    assert_eq!((out.writes(), twin.state().open_id(KEY)), (0, None));
}

#[test]
fn xfer_accept_batch() {
    let mut twin = Twin::new();
    let (ids, reply, out) = accepting_batch(&mut twin, &[KEY, OTHER]);
    assert_eq!((out.writes(), out.outbox.len(), out.all_replies()), (0, 3, 0));
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Reserving));
    let out = twin.reserve_batch(&ids);
    assert_eq!((out.writes(), out.rows().len()), (1, 2));
    assert!(out.replies(reply).is_empty());
}

#[test]
fn xfer_accept_batch_invalid() {
    let mut twin = Twin::new();
    let mut broken = item(OTHER, 2);
    broken.source.as_mut().unwrap().parts = 0;
    let out = twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: vec![item(KEY, 2), broken], reply: 9, now: Time(0) });
    assert_eq!(Twin::fault(&out, 9), Some(Fault::Invalid));
    assert_eq!((out.writes(), twin.state().open_id(KEY), twin.state().open_id(OTHER)), (0, None, None), "no element is accepted");
    let out = twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH + 1, items: vec![item(KEY, 2)], reply: 10, now: Time(0) });
    assert_eq!(Twin::fault(&out, 10), Some(Fault::NoSession));
}

/// xfer_accept_batch_invalid (D-196): the size of a batch has one predicate; the owner refuses min-1 and max+1 without a write, and accepts min and max. The call of the platform asks the same predicate before it plans the rows.
#[test]
fn xfer_accept_batch_size_domain() {
    let joint = joint();
    let keys: Vec<String> = (0..=joint.maximum + 1).map(|index| format!("Moment:r{index}:upload")).collect();
    for count in [joint.minimum - 1, joint.minimum, joint.maximum, joint.maximum + 1] {
        let inside = count >= joint.minimum && count <= joint.maximum;
        assert_eq!(crate::owners::write::transfer::batch_size_admitted((joint.minimum, joint.maximum), count), inside, "{count} items");
        let mut twin = Twin::new();
        let items: Vec<_> = keys.iter().take(count).map(|key| item(key, 2)).collect();
        let out = twin.post(TransferInput::AcceptBatch { joint: Box::new(super::transfer_harness::joint()), epoch: EPOCH, items, reply: 9, now: Time(0) });
        match inside {
            true => assert_eq!(Twin::fault(&out, 9), None, "{count} items are accepted"),
            false => assert_eq!((Twin::fault(&out, 9), out.writes()), (Some(Fault::Invalid), 0), "{count} items are refused without a write"),
        }
    }
}

#[test]
fn xfer_accepting_committed() {
    let mut twin = Twin::new();
    let (id, _, _) = twin.accepting(KEY, 2);
    let out = twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Copying);
    assert_eq!((out.copies(), out.shows(), out.round_needed()), (1, vec![Shown::Copying], vec![vec![id]]));
    let attempt_id = twin.attempt(id);
    assert!(out.runs.iter().any(|run| matches!(run, crate::owners::write::transfer::TransferEffect::Copy { transfer_id, attempt_id: found, .. } if *transfer_id == id && *found == attempt_id)));
}

#[test]
fn xfer_accepting_rejected() {
    let mut twin = Twin::new();
    let (id, reply, _) = twin.accepting(KEY, 2);
    let out = twin.reject(RejectReason::Storage("disk".into()));
    assert_eq!(Twin::fault(&out, reply), Some(Fault::Rejected(RejectReason::Storage("disk".into()))));
    assert_eq!((twin.stage(id), out.copies(), out.shows().len()), (TransferStage::Done, 0, 0));
    let (ids, batch_reply, _) = accepting_batch(&mut twin, &[OTHER, "Moment:r3:upload"]);
    twin.reserve_batch(&ids);
    let out = twin.reject(RejectReason::NoSession);
    assert_eq!(out.all_replies(), 1, "a batch has one answer and no receipt");
    assert_eq!(Twin::fault(&out, batch_reply), Some(Fault::SessionEnded), "the refusal reports the ended session");
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Done));
}

#[test]
fn xfer_accepting_other() {
    let mut twin = Twin::new();
    let (id, reply, _) = twin.accepting(KEY, 2);
    refuses_calls(&mut twin, KEY);
    let out = twin.close();
    assert_eq!((out.drained(), out.all_replies(), twin.stage(id)), (0, 0, TransferStage::Accepting), "Closing waits for the answer of the Store");
    let out = twin.commit();
    assert_eq!(out.replies(reply), vec![TransferAnswer::Err(Fault::Closed)]);
    assert_eq!((out.drained(), out.copies(), twin.stage(id)), (1, 0, TransferStage::Done));
}

#[test]
fn xfer_copy_done() {
    let mut twin = Twin::new();
    let id = twin.copying(KEY, 2);
    let out = twin.copy_done(id);
    assert_eq!(twin.stage(id), TransferStage::PersistingCopy);
    let rows = out.rows();
    assert_eq!((rows.len(), rows[0].1, rows[0].0.size, rows[0].0.sha256.as_str()), (1, UploadKind::SyncUpload, 10, "h"));
    assert_eq!(out.all_replies(), 0, "the receipt waits for the write of the copy");
}

#[test]
fn xfer_copy_done_batch_wait() {
    let mut twin = Twin::new();
    let (ids, _) = copying_batch(&mut twin, &[KEY, OTHER]);
    let out = twin.copy_done(ids[0]);
    assert_eq!(twin.stage(ids[0]), TransferStage::CopyReady);
    assert!(Twin::quiet(&out), "{out:?}");
}

#[test]
fn xfer_copy_done_batch_last() {
    let mut twin = Twin::new();
    let (ids, _) = copying_batch(&mut twin, &[KEY, OTHER]);
    twin.copy_done(ids[0]);
    let out = twin.copy_done(ids[1]);
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::PersistingCopy));
    assert_eq!((out.writes(), out.rows().len(), out.all_replies()), (1, 2, 0), "one common write of the acceptance");
}

#[test]
fn xfer_copy_failed_batch() {
    let mut twin = Twin::new();
    let (ids, reply) = copying_batch(&mut twin, &[KEY, OTHER]);
    let out = twin.copy_failed(ids[0]);
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Removing));
    assert_eq!((out.copy_cancels(), out.removed().len(), out.writes(), out.all_replies()), (1, 2, 1, 0), "the metadata of the whole batch goes before any answer");
    let out = twin.commit();
    assert_eq!(out.all_replies(), 1);
    assert_eq!(Twin::fault(&out, reply), Some(Fault::Copy("unreadable".into())));
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Done));
}

#[test]
fn xfer_copy_failed() {
    let mut twin = Twin::new();
    let (id, reply, _) = twin.accepting(KEY, 2);
    twin.commit();
    let out = twin.copy_failed(id);
    assert_eq!((twin.stage(id), out.removed(), out.all_replies()), (TransferStage::Removing, vec![KEY.to_string()], 0));
    let out = twin.commit();
    assert_eq!(Twin::fault(&out, reply), Some(Fault::Copy("unreadable".into())));
    assert_eq!((twin.stage(id), out.shows(), out.deletes()), (TransferStage::Done, vec![Shown::Gone], 1));
}

#[test]
fn xfer_copy_failed_restored() {
    let mut row = stored(1, KEY, 2);
    row.copying = true;
    let (mut twin, out) = Twin::opened(vec![row], Default::default());
    assert_eq!((out.copies(), twin.stage(1)), (1, TransferStage::Copying));
    let out = twin.copy_failed(1);
    assert_eq!(twin.stage(1), TransferStage::PersistingFailure);
    assert_eq!((out.kinds(), out.removed().len()), (vec![UploadKind::Failed], 0), "a restored transfer has no call that waits: it is failed, not removed");
    twin.commit();
    assert_eq!(twin.stage(1), TransferStage::Failed);
}

#[test]
fn xfer_copy_cancel() {
    for cancel in [true, false] {
        let mut twin = Twin::new();
        let (id, accept_reply, _) = twin.accepting(KEY, 2);
        twin.commit();
        let (reply, out) = if cancel { twin.cancel(KEY) } else { twin.discard(KEY) };
        assert_eq!((twin.stage(id), out.copy_cancels(), out.removed(), out.all_replies()), (TransferStage::Removing, 1, vec![KEY.to_string()], 0));
        let out = twin.commit();
        assert_eq!(out.replies(reply), vec![if cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok }]);
        assert_eq!(out.replies(accept_reply), vec![TransferAnswer::Err(Fault::Interrupted)], "the call that waited for the copy is interrupted");
        assert_eq!(twin.stage(id), TransferStage::Done);
    }
}

#[test]
fn xfer_copy_cancel_batch() {
    let mut twin = Twin::new();
    let (ids, _) = copying_batch(&mut twin, &[KEY, OTHER]);
    for (reply, out) in [twin.cancel(KEY), twin.discard(KEY)] {
        assert_eq!(Twin::fault(&out, reply), Some(Fault::InFlight));
    }
    assert_eq!(twin.stage(ids[0]), TransferStage::Copying);
}

#[test]
fn xfer_copy_retry() {
    let mut twin = Twin::new();
    let id = twin.copying(KEY, 2);
    let (reply, out) = twin.retry(KEY, None);
    assert_eq!((Twin::fault(&out, reply), twin.stage(id)), (Some(Fault::InFlight), TransferStage::Copying), "a transfer without a durable copy has no receipt");
}

#[test]
fn xfer_copy_ready_refuses() {
    let mut twin = Twin::new();
    let (ids, _) = copying_batch(&mut twin, &[KEY, OTHER]);
    twin.copy_done(ids[0]);
    refuses_calls(&mut twin, KEY);
    assert_eq!(twin.stage(ids[0]), TransferStage::CopyReady);
}

#[test]
fn xfer_copy_ready_other() {
    let mut twin = Twin::new();
    let (ids, _) = copying_batch(&mut twin, &[KEY, OTHER]);
    twin.copy_done(ids[0]);
    let mut inputs = twin.stray_results(ids[0]);
    inputs.extend([
        TransferInput::MutationOutcome { transfer_id: ids[0], outcome: OutboxOutcome::Confirmed { revision: 1, result: None, row_id: None } },
        TransferInput::Online(Online(true)),
        TransferInput::PowerSaving(true),
        TransferInput::SlotFreed,
        TransferInput::CopyDone { transfer_id: ids[0], attempt_id: 999, size: 1, sha256: "x".into(), place: crate::store::files::Location::Directory, cached: crate::owners::write::transfer::CachedBlob::Miss },
        TransferInput::CopyFailed { transfer_id: ids[0], attempt_id: 999, error: "late".into() },
    ]);
    twin.ignores(ids[0], inputs);
}

#[test]
fn xfer_copy_stray() {
    let mut twin = Twin::new();
    let id = twin.copying(KEY, 2);
    twin.ignores(
        id,
        vec![
            TransferInput::CopyDone { transfer_id: id, attempt_id: 999, size: 1, sha256: "x".into(), place: crate::store::files::Location::Directory, cached: crate::owners::write::transfer::CachedBlob::Miss },
            TransferInput::CopyFailed { transfer_id: id, attempt_id: 999, error: "late".into() },
            TransferInput::CopyDone { transfer_id: id + 50, attempt_id: twin.attempt(id), size: 1, sha256: "x".into(), place: crate::store::files::Location::Directory, cached: crate::owners::write::transfer::CachedBlob::Miss },
        ],
    );
}

#[test]
fn xfer_copy_other() {
    let mut twin = Twin::new();
    let id = twin.copying(KEY, 2);
    let mut inputs = twin.stray_results(id);
    inputs.extend([
        TransferInput::MutationOutcome { transfer_id: id, outcome: OutboxOutcome::Confirmed { revision: 1, result: None, row_id: None } },
        TransferInput::Online(Online(true)),
        TransferInput::PowerSaving(true),
        TransferInput::SlotFreed,
    ]);
    twin.ignores(id, inputs);
}

#[test]
fn xfer_persisting_copy_committed() {
    let mut twin = Twin::offline();
    let (id, reply, _) = twin.accepting(KEY, 2);
    twin.commit();
    twin.copy_done(id);
    let out = twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Ready);
    let receipts = out.replies(reply);
    assert!(matches!(receipts.as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.cycle_id == 1 && receipt.op_id == id), "{receipts:?}");
    assert_eq!(out.shows(), vec![Shown::Queued]);
    let (ids, reply) = copying_batch(&mut twin, &[OTHER, "Moment:r3:upload"]);
    twin.copy_done(ids[0]);
    twin.copy_done(ids[1]);
    let out = twin.commit();
    assert_eq!(out.all_replies(), 1);
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipts(receipts)] if receipts.len() == 2 && receipts[0].op_id == ids[0] && receipts[1].op_id == ids[1]), "N receipts in the order of the elements");
}

#[test]
fn xfer_persisting_copy_rejected() {
    let mut twin = Twin::new();
    let (id, reply, _) = twin.accepting(KEY, 2);
    twin.commit();
    twin.copy_done(id);
    let out = twin.reject(RejectReason::Storage("disk".into()));
    assert_eq!((twin.stage(id), out.removed(), out.all_replies()), (TransferStage::Removing, vec![KEY.to_string()], 0));
    let out = twin.commit();
    assert_eq!(Twin::fault(&out, reply), Some(Fault::Rejected(RejectReason::Storage("storage".into()))));
    assert_eq!(twin.stage(id), TransferStage::Done);
    let (ids, _) = copying_batch(&mut twin, &[OTHER, "Moment:r3:upload"]);
    twin.copy_done(ids[0]);
    twin.copy_done(ids[1]);
    let out = twin.reject(RejectReason::Storage("disk".into()));
    assert_eq!(out.removed().len(), 2, "the whole batch is removed");
}

#[test]
fn xfer_persisting_copy_refuses() {
    let mut twin = Twin::new();
    let id = twin.persisting_copy(KEY, 2);
    refuses_calls(&mut twin, KEY);
    assert_eq!(twin.stage(id), TransferStage::PersistingCopy);
}
