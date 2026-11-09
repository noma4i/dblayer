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
