//! Owner-level tests of the owner `Transfer`: a retry with a new source of a transfer that has a copy (D-226).

use super::transfer_harness::*;
use crate::owners::contract::RejectReason;
use crate::owners::write::outbox::Fault;
use crate::owners::write::transfer::{staged_file_id, Source, TransferAnswer, TransferInput, TransferStage};

const KEY: &str = "Moment:r1:upload";

fn fresh() -> Source { Source { path: "/new/video".into(), parts: 2 } }

#[test]
fn xfer_retry_source_replaces_copy() {
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    let (reply, out) = twin.retry(KEY, Some(fresh()));
    assert_eq!((twin.stage(id), out.staged_copies(), out.writes(), out.deleted()), (TransferStage::Copying, 1, 0, Vec::<String>::new()), "the new copy goes to a temporary file, nothing is written");
    twin.copy_done(id);
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.promotions(), out.all_replies()), (TransferStage::PersistingCopy, 1, 0), "the call waits for the swap of the copy");
    let out = twin.post(TransferInput::StagedPromoted { transfer_id: id });
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.cycle_id == 2), "{:?}", out.replies(reply));
    assert_eq!(twin.stage(id), TransferStage::PersistingAttempt, "the swapped transfer starts its upload at once");
}

#[test]
fn xfer_retry_source_orphans_old() {
    let mut row = stored(1, KEY, 2);
    row.failed = Some("refused".into());
    row.upload_id = Some("previous-upload".into());
    row.confirmed_parts = 1;
    let (mut twin, _) = Twin::opened(vec![row], Default::default());
    twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    twin.retry(KEY, Some(fresh()));
    let out = twin.copy_done(1);
    assert_eq!(out.orphans_written(), vec![("previous-upload".into(), KEY.into())]);
    assert!(!out.rows().is_empty());
    assert_eq!(out.writes(), 1);
    assert!(out.orphan_aborts().is_empty());
}

#[test]
fn xfer_retry_source_failure_keeps_copy() {
    for store_fails in [false, true] {
        let mut twin = Twin::new();
        let id = twin.failed(KEY);
        let old = twin.state().transfers[&id].source.clone();
        let (reply, _) = twin.retry(KEY, Some(fresh()));
        let out = if store_fails { twin.copy_done(id); twin.reject(RejectReason::Storage("disk".into())) } else { twin.copy_failed(id) };
        let fault = Twin::fault(&out, reply);
        assert!(matches!(fault, Some(Fault::Copy(_) | Fault::Rejected(_))), "store_fails={store_fails}: {fault:?}");
        assert_eq!((twin.stage(id), twin.state().transfers[&id].source.clone(), out.deleted()), (TransferStage::Failed, old, vec![staged_file_id(id)]), "store_fails={store_fails}: the old copy and the metadata stay, only the temporary file goes");
    }
}

#[test]
fn xfer_retry_source_marks_the_swap_in_the_row() {
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    twin.retry(KEY, Some(fresh()));
    let out = twin.copy_done(id);
    assert!(!out.rows().is_empty() && out.rows().iter().all(|(row, _)| row.staged), "the commit of the metadata carries the marker");
    twin.commit();
    let out = twin.post(TransferInput::StagedPromoted { transfer_id: id });
    assert_eq!(out.staged_cleared(), vec![KEY.to_string()], "after the rename the marker goes");
}

#[test]
fn xfer_restored_staged_finishes_the_swap() {
    let mut row = stored(1, KEY, 2);
    row.staged = true;
    let (mut twin, out) = Twin::opened(vec![row], Default::default());
    let id = 1;
    assert_eq!((out.promotions(), out.deleted(), twin.stage(id) == TransferStage::Failed), (1, Vec::<String>::new(), false), "a crash between Committed and the rename does not end as missing_file");
    let out = twin.post(TransferInput::StagedPromoted { transfer_id: id });
    assert_eq!(out.staged_cleared(), vec![KEY.to_string()]);
}

#[test]
fn xfer_staged_promote_failure() {
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    let old = twin.state().transfers[&id].source.clone();
    let (reply, _) = twin.retry(KEY, Some(fresh()));
    twin.copy_done(id);
    twin.commit();
    let out = twin.post(TransferInput::StagedPromoteFailed { transfer_id: id, detail: "rename".into() });
    assert_eq!((out.batches.len(), out.runs.len()), (1, 0), "a failed rename writes the old row back before any reply or file effect: {out:?}");
    let out = twin.commit();
    assert!(matches!(Twin::fault(&out, reply), Some(Fault::Rejected(RejectReason::Storage(ref detail))) if detail == "rename"), "the retry call learns the swap failed after the old row is committed: {:?}", Twin::fault(&out, reply));
    assert_eq!((twin.stage(id), twin.state().transfers[&id].source.clone(), out.deleted()), (TransferStage::Failed, old, vec![staged_file_id(id)]), "a failed rename keeps the old copy and metadata, only the temporary file goes");
}

#[test]
fn xfer_staged_clear_rejected() {
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    twin.retry(KEY, Some(fresh()));
    twin.copy_done(id);
    twin.commit();
    twin.post(TransferInput::StagedPromoted { transfer_id: id });
    assert_eq!(twin.stage(id), TransferStage::PersistingAttempt, "after the swap the transfer uploads the new copy");
    let out = twin.reject(RejectReason::Storage("disk".into()));
    assert_eq!((twin.stage(id), out.removed()), (TransferStage::PersistingAttempt, Vec::<String>::new()), "a rejected clear of the staged marker keeps the transfer, the marker stays for the next restart to clear");
}

/// xfer_staged_promote_failure_restart (K1, D-226, transfer.md:455): the new metadata is already Committed
/// with staged = true; the rename of the staged file to the key fails. The spec row says the failure is durable:
/// the temporary file is deleted and Failed with the old copy remains. The old copy and its metadata must be
/// written back and the staged marker cleared before the retry reply, so a restart reads the old copy and not
/// the new metadata over a missing file. Prod restores the old row only in memory and emits only DeleteFile,
/// with no Store batch.
#[test]
fn xfer_staged_promote_failure_restart() {
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    let old_source = twin.state().transfers[&id].source.clone().map(|source| source.path);
    let (_, _) = twin.retry(KEY, Some(fresh()));
    twin.copy_done(id);
    twin.commit();
    let out = twin.post(TransferInput::StagedPromoteFailed { transfer_id: id, detail: "rename".into() });
    let written = out.rows().into_iter().next().map(|(row, _)| (row.staged, row.source.clone()));
    assert_eq!((out.writes(), written, out.deleted()), (1, Some((false, old_source)), Vec::new()), "a failed rename writes the old copy back (staged cleared, old source) before any file effect: {out:?}");
    let committed = twin.commit();
    assert_eq!((committed.deleted(), twin.stage(id)), (vec![staged_file_id(id)], TransferStage::Failed), "Committed deletes the staged file and leaves Failed with the old copy: {committed:?}");
}

/// xfer_staged_clear_rejected_keeps_upload (K1, D-226, transfer.md:27): the retry reply is handed out and the
/// transfer started before the Committed of the batch that clears staged. On Rejected(Storage) of that clear
/// the accepted upload and its file must stay, with no network started before Committed and the marker left for
/// the next restart to clear.
#[test]
fn xfer_staged_clear_rejected_keeps_upload() {
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    twin.retry(KEY, Some(fresh()));
    twin.copy_done(id);
    twin.commit();
    twin.post(TransferInput::StagedPromoted { transfer_id: id });
    assert_eq!(twin.stage(id), TransferStage::PersistingAttempt, "after the swap the transfer uploads the new copy");
    let out = twin.reject(RejectReason::Storage("disk".into()));
    assert_eq!(
        (twin.stage(id), out.removed(), out.deleted(), out.steps().len()),
        (TransferStage::PersistingAttempt, Vec::<String>::new(), Vec::<String>::new(), 0),
        "a rejected staged-clear keeps the accepted upload and its file, starts no network before Committed: {out:?}",
    );
}

/// xfer_retry_invalid_source (H08): a retry of a failed transfer with a source that has no file (empty path, zero parts) is refused with Invalid and stays Failed.
#[test]
fn xfer_retry_invalid_source() {
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    let (reply, out) = twin.retry(KEY, Some(Source { path: String::new(), parts: 0 }));
    assert_eq!((Twin::fault(&out, reply), twin.stage(id)), (Some(Fault::Invalid), TransferStage::Failed), "an invalid source is refused and the transfer stays Failed: {out:?}");
}
