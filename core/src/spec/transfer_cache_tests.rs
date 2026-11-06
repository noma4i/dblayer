//! Owner-level tests of the owner `Transfer`: the cache of server blobs by the content of a copy (D-67).

use super::transfer_harness::*;
use crate::store::files::Location;
use crate::owners::store::{StoreOp, UploadKind};
use crate::owners::write::transfer::{CachedBlob, Shown, TransferAnswer, TransferId, TransferInput, TransferStage};

const KEY: &str = "Moment:r1:upload";

fn copied(twin: &mut Twin, id: TransferId, cached: CachedBlob) -> Out {
    let attempt_id = twin.attempt(id);
    twin.post(TransferInput::CopyDone { transfer_id: id, attempt_id, size: 10, sha256: "h".into(), place: Location::Directory, cached })
}

fn cache_ops(out: &Out) -> Vec<StoreOp> { out.batches.iter().flat_map(|batch| batch.ops.iter()).filter(|op| matches!(op, StoreOp::BlobCachePut { .. } | StoreOp::BlobCacheRemove { .. })).cloned().collect() }

/// The request for the steps or the `complete` of a file never goes out.
fn no_upload_requests(out: &Out) -> bool { out.steps().is_empty() && out.completes() == 0 }

#[test]
fn xfer_copy_cached_skips_steps() {
    let mut twin = Twin::new();
    let id = twin.copying(KEY, 2);
    let out = copied(&mut twin, id, CachedBlob::Fresh { blob_id: "blob-1".into() });
    assert_eq!(twin.stage(id), TransferStage::PersistingCopy);
    assert_eq!((out.writes(), out.kinds()), (1, vec![UploadKind::Completed]), "the copy and the finished upload are one transaction");
    let row = out.rows()[0].0;
    assert_eq!((row.signed_id.as_deref(), row.size, row.sha256.as_str(), row.completed), (Some("blob-1"), 10, "h", true));
    assert!(no_upload_requests(&out) && out.submits() == 0 && out.all_replies() == 0, "nothing goes out before Committed: {out:?}");
}

#[test]
fn xfer_persisting_copy_cached_committed() {
    let mut twin = Twin::new();
    let (id, reply, _) = twin.accepting(KEY, 2);
    twin.commit();
    copied(&mut twin, id, CachedBlob::Fresh { blob_id: "blob-1".into() });
    let out = twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Mutating);
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(_)]), "{:?}", out.replies(reply));
    assert_eq!((out.submits(), no_upload_requests(&out)), (1, true));
    assert_eq!(out.shows(), vec![Shown::Uploading { progress: 100 }]);
}

#[test]
fn xfer_copy_stale_cache_removed() {
    let mut twin = Twin::new();
    let id = twin.copying(KEY, 2);
    let out = copied(&mut twin, id, CachedBlob::Stale);
    assert_eq!(cache_ops(&out), vec![StoreOp::BlobCacheRemove { content_sha256: "h".into() }]);
    assert_eq!(out.kinds(), vec![UploadKind::SyncUpload]);
    let out = twin.commit();
    assert_eq!(twin.stage(id), TransferStage::PersistingAttempt);
    assert_eq!(out.steps().len(), 0);
    assert_eq!(twin.commit().steps().len(), 1);
}

#[test]
fn xfer_blob_cache_by_content() {
    let mut twin = Twin::new();
    let id = twin.copying(KEY, 1);
    assert!(cache_ops(&copied(&mut twin, id, CachedBlob::Miss)).is_empty(), "a miss writes nothing to the cache");
    twin.commit();
    twin.commit();
    twin.step_ok(id, 1, None);
    twin.commit();
    twin.commit();
    let written = twin.completed(id);
    assert_eq!(cache_ops(&written), vec![StoreOp::BlobCachePut { content_sha256: "h".into(), blob_id: "signed".into(), created_at: 0 }], "the completed upload is remembered by its content");
    assert_eq!(twin.commit().submits(), 1);
    assert_eq!(twin.stage(id), TransferStage::Mutating);
}
