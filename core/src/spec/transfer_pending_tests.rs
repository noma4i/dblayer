use super::transfer_harness::*;
use crate::owners::write::outbox::{Fault, ReserveTarget};
use crate::owners::write::transfer::{Source, Steps, SuppliedSource, TransferAnswer, TransferInput, TransferStage};
use crate::runtime::Time;

const KEY: &str = "Moment:r1:upload";

fn supplied(source: Source) -> SuppliedSource { SuppliedSource { source, prepare: None, steps: Steps { path: "/upload".into(), body: String::new() }, values: Default::default(), local: String::new(), variables: Default::default(), wrap: None, max_bytes: None } }

fn pending(twin: &mut Twin) -> (u64, u64) {
    let mut request = item(KEY, 1);
    request.source = None;
    request.steps = None;
    request.mutation.body = r#"{"variables":{"signedId":"__dblayer_signed_id__"}}"#.into();
    let reply = twin.reply();
    twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(request), reply });
    let id = twin.state().open_id(KEY).unwrap();
    twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(id) });
    (id, reply)
}

#[test]
fn xfer_pending_accepted() {
    let mut twin = Twin::offline();
    let (id, reply) = pending(&mut twin);
    let out = twin.commit();
    assert_eq!(twin.stage(id), TransferStage::AwaitingSource);
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.cycle_id == 1));
    assert_eq!((out.copies(), out.round_needed().len(), out.steps().len()), (0, 0, 0));
    let mut batch = Twin::offline();
    let mut first = item(KEY, 1);
    let mut second = item("Moment:r2:upload", 1);
    first.source = None;
    second.source = None;
    first.steps = None;
    second.steps = None;
    batch.post(TransferInput::AcceptBatch { epoch: EPOCH, items: vec![first, second], joint: Box::new(joint()), reply: 8 });
    let ids = [batch.state().open_id(KEY).unwrap(), batch.state().open_id("Moment:r2:upload").unwrap()];
    batch.reserve_batch(&ids);
    let accepted = batch.commit();
    assert!(ids.iter().all(|id| batch.stage(*id) == TransferStage::AwaitingSource));
    assert!(matches!(accepted.replies(8).as_slice(), [TransferAnswer::Receipts(receipts)] if receipts.iter().map(|r| r.op_id).collect::<Vec<_>>() == ids));
    assert_eq!((accepted.copies(), accepted.round_needed().len(), accepted.steps().len()), (0, 0, 0));
}

#[test]
fn xfer_a_refused_supply_changes_nothing() {
    {
        let mut twin = Twin::offline();
        let source = Source { path: "/source".into(), parts: 1 };
        let out = twin.post(TransferInput::SupplySource { epoch: EPOCH, key: KEY.into(), source: supplied(source), reply: 9 });
        assert_eq!(Twin::fault(&out, 9), Some(Fault::NotFound));
        let out = twin.post(TransferInput::SourceFailed { epoch: EPOCH, key: KEY.into(), message: "failed".into(), reply: 10 });
        assert_eq!(Twin::fault(&out, 10), Some(Fault::NotFound));
        assert_eq!(out.writes(), 0);
    }
    {
        let mut twin = Twin::offline();
        let (id, _) = pending(&mut twin);
        twin.commit();
        let out = supply(&mut twin, EPOCH + 1, 9);
        assert_eq!(Twin::fault(&out, 9), Some(Fault::NoSession));
        assert_eq!((twin.stage(id), out.copies(), out.writes()), (TransferStage::AwaitingSource, 0, 0));
    }
    {
        let mut twin = Twin::offline();
        let id = twin.accepted(KEY, 1);
        let out = supply(&mut twin, EPOCH, 9);
        assert_eq!(Twin::fault(&out, 9), Some(Fault::Invalid));
        assert_eq!((twin.stage(id), out.writes(), out.copies()), (TransferStage::Ready, 0, 0));
    }
    {
        let mut twin = Twin::offline();
        let (id, _) = pending(&mut twin);
        twin.commit();
        for source in [Source { path: String::new(), parts: 1 }, Source { path: "/source".into(), parts: 0 }] {
            let out = twin.post(TransferInput::SupplySource { epoch: EPOCH, key: KEY.into(), source: supplied(source), reply: 9 });
            assert_eq!(Twin::fault(&out, 9), Some(Fault::Invalid));
            assert_eq!((twin.stage(id), out.copies(), out.writes()), (TransferStage::AwaitingSource, 0, 0));
        }
    }
}

#[test]
fn xfer_pending_retry() {
    let mut twin = Twin::offline();
    let (id, _) = pending(&mut twin);
    twin.commit();
    let (reply, out) = twin.retry(KEY, None);
    assert_eq!(Twin::fault(&out, reply), Some(Fault::InFlight));
    assert_eq!((twin.stage(id), out.writes()), (TransferStage::AwaitingSource, 0));
}

fn supply(twin: &mut Twin, epoch: u64, reply: u64) -> Out {
    twin.post(TransferInput::SupplySource { epoch, key: KEY.into(), source: supplied(Source { path: "/source".into(), parts: 1 }), reply })
}

#[test]
fn xfer_supply_starts_copy() {
    let mut twin = Twin::offline();
    let (id, _) = pending(&mut twin);
    twin.commit();
    let out = supply(&mut twin, EPOCH, 9);
    assert_eq!((twin.stage(id), out.copies(), out.round_needed()), (TransferStage::Copying, 1, vec![vec![id]]));
    assert!(out.replies(9).is_empty());
    assert!(out.shows().is_empty());
}

/// `back` of the supply: `AwaitingSource` after `SupplySource`, `Failed` after `Retry` with a source of a transfer without a copy.
fn supplied_with_back(twin: &mut Twin, back: TransferStage) -> (u64, u64) {
    if back == TransferStage::AwaitingSource {
        let (id, _) = pending(twin);
        twin.commit();
        supply(twin, EPOCH, 9);
        return (id, 9);
    }
    let id = source_failed(twin);
    let (reply, _) = twin.retry(KEY, Some(Source { path: "/source".into(), parts: 1 }));
    (id, reply)
}

#[test]
fn xfer_supply_copy_failed() {
    for back in [TransferStage::AwaitingSource, TransferStage::Failed] {
        let mut twin = Twin::offline();
        let (id, reply) = supplied_with_back(&mut twin, back);
        let out = twin.copy_failed(id);
        assert_eq!((twin.stage(id), out.writes(), out.deletes()), (back, 0, 1));
        assert_eq!(Twin::fault(&out, reply), Some(Fault::Copy("unreadable".into())));
        assert!(out.shows().is_empty());
        if back == TransferStage::Failed {
            assert_eq!(twin.state().transfers[&id].error.as_deref(), Some("source_failed:preparation"));
        }
    }
}

#[test]
fn xfer_cancel_of_a_pending_transfer_or_batch() {
    {
        for cancel in [true, false] {
            let mut twin = Twin::offline();
            let (id, _) = pending(&mut twin);
            twin.commit();
            let (reply, out) = if cancel { twin.cancel(KEY) } else { twin.discard(KEY) };
            assert_eq!((out.removed(), out.rolled_back(), out.copies(), out.steps().len()), (vec![KEY.to_string()], vec![id], 0, 0));
            let out = twin.commit();
            assert_eq!(out.replies(reply), vec![if cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok }]);
        }
    }
    {
        let mut twin = Twin::offline();
        let ids = pending_batch(&mut twin);
        let (reply, out) = twin.cancel(KEY);
        assert_eq!((out.removed(), out.rolled_back(), out.copies(), out.cancel_steps()), (vec![KEY.to_string()], vec![ids[0]], 0, 0));
        assert_eq!(twin.commit().replies(reply), vec![TransferAnswer::Cancelled(true)]);
        assert_eq!(twin.stage(ids[1]), TransferStage::AwaitingSource);
    }
}

#[test]
fn xfer_pending_outcome() {
    let mut twin = Twin::offline();
    let (id, _) = pending(&mut twin);
    twin.commit();
    let (reply, out) = twin.outcome(id, 1);
    assert!(out.replies(reply).is_empty());
    assert_eq!((twin.stage(id), out.writes()), (TransferStage::AwaitingSource, 0));
}

#[test]
fn xfer_a_pending_transfer_ends_with_its_epoch_or_the_close() {
    {
        let mut twin = Twin::offline();
        let (id, _) = pending(&mut twin);
        twin.commit();
        let (reply, _) = twin.outcome(id, 1);
        let out = twin.close();
        assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(crate::owners::write::outbox::OutboxOutcome::Ended(crate::ffi::EndCause::Closed), None)]);
        assert_eq!((out.drained(), out.removed().len()), (1, 0));
    }
    {
        let mut twin = Twin::offline();
        let (id, _) = pending(&mut twin);
        twin.commit();
        let (reply, _) = twin.outcome(id, 1);
        let out = twin.end_epoch();
        assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(crate::owners::write::outbox::OutboxOutcome::Ended(crate::ffi::EndCause::SessionEnded), None)]);
        assert_eq!(twin.stage(id), TransferStage::Done);
    }
}

#[test]
fn xfer_supply_copy_done() {
    let mut twin = Twin::offline();
    let (id, _) = pending(&mut twin);
    twin.commit();
    let mut source = supplied(Source { path: "/source".into(), parts: 1 });
    source.values.rows.push(crate::owners::store::EffectRow { model: "Moment".into(), row_id: "r1".into(), values: r#"{"text":"ready"}"#.into() });
    source.variables.insert("token".into(), serde_json::json!("v"));
    source.local = r#"{"seen":true}"#.into();
    twin.post(TransferInput::SupplySource { epoch: EPOCH, key: KEY.into(), source, reply: 9 });
    let out = twin.copy_done(id);
    assert_eq!((twin.stage(id), out.writes(), out.rows().len()), (TransferStage::PersistingCopy, 1, 1));
    assert!(out.replies(9).is_empty());
    assert!(out.batches[0].ops.iter().any(|op| matches!(op, crate::owners::store::StoreOp::Effects { op_id, effects, .. } if *op_id == id && effects.rows.iter().any(|row| row.values.contains("ready")))));
    assert!(out.batches[0].ops.iter().any(|op| matches!(op, crate::owners::store::StoreOp::LocalFields { model, row_id, values } if model.as_str() == "Moment" && row_id.as_str() == "r1" && values.contains("seen"))), "the supplied local values are written by the copy transaction: {:?}", out.batches[0].ops);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&out.rows()[0].0.body).unwrap()["variables"]["token"], "v");
}

#[test]
fn xfer_supply_committed() {
    let mut twin = Twin::offline();
    let (id, _) = pending(&mut twin);
    twin.commit();
    supply(&mut twin, EPOCH, 9);
    twin.copy_done(id);
    let out = twin.commit();
    assert_eq!(out.replies(9), vec![TransferAnswer::Ok]);
    assert_eq!((twin.stage(id), out.shows()), (TransferStage::Ready, vec![crate::owners::write::transfer::Shown::Queued { progress: 0 }]));
}

#[test]
fn xfer_supply_rejected() {
    use crate::owners::contract::RejectReason;
    for reason in [RejectReason::Storage("disk".into()), RejectReason::Invalid("bad row".into())] {
        for back in [TransferStage::AwaitingSource, TransferStage::Failed] {
            let mut twin = Twin::offline();
            let (id, reply) = supplied_with_back(&mut twin, back);
            twin.copy_done(id);
            let out = twin.reject(reason.clone());
            assert_eq!(twin.stage(id), back, "{reason:?}");
            assert_eq!(Twin::fault(&out, reply), Some(Fault::Rejected(reason.clone())));
            assert_eq!(out.deletes(), 1, "{reason:?}");
            assert!(out.shows().is_empty(), "{reason:?}");
            if back == TransferStage::Failed {
                assert_eq!(twin.state().transfers[&id].error.as_deref(), Some("source_failed:preparation"), "{reason:?}");
            }
        }
    }
}

/// P-DATA-101: the Store refuses the write of a failed source as invalid, the caller of the failure gets the text of the Store.
#[test]
fn xfer_source_failed_write_invalid() {
    let mut twin = Twin::offline();
    pending(&mut twin);
    twin.commit();
    twin.post(TransferInput::SourceFailed { epoch: EPOCH, key: KEY.into(), message: "preparation".into(), reply: 9 });
    let out = twin.reject(crate::owners::contract::RejectReason::Invalid("bad row".into()));
    assert_eq!(Twin::fault(&out, 9), Some(Fault::Rejected(crate::owners::contract::RejectReason::Invalid("bad row".into()))));
}

#[test]
fn xfer_source_failed() {
    let mut twin = Twin::offline();
    let (id, _) = pending(&mut twin);
    twin.commit();
    let out = twin.post(TransferInput::SourceFailed { epoch: EPOCH, key: KEY.into(), message: "preparation".into(), reply: 9 });
    assert_eq!((twin.stage(id), out.writes()), (TransferStage::PersistingFailure, 1));
    assert!(out.replies(9).is_empty());
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.replies(9)), (TransferStage::Failed, vec![TransferAnswer::Ok]));
    let (reply, outcome) = twin.outcome(id, 1);
    assert!(matches!(outcome.replies(reply).as_slice(), [TransferAnswer::Outcome(crate::owners::write::outbox::OutboxOutcome::Failed { error }, _)] if error == "source_failed:preparation"));
}

#[test]
fn xfer_supply_busy() {
    let mut twin = Twin::offline();
    let (id, _) = pending(&mut twin);
    twin.commit();
    supply(&mut twin, EPOCH, 9);
    for phase in 0..2 {
        if phase == 1 { twin.copy_done(id); }
        let (cancel, cancelled) = twin.cancel(KEY);
        let (discard, discarded) = twin.discard(KEY);
        let (retry, retried) = twin.retry(KEY, None);
        let supplied = supply(&mut twin, EPOCH, 20);
        let failed = twin.post(TransferInput::SourceFailed { epoch: EPOCH, key: KEY.into(), message: "failed".into(), reply: 21 });
        for (reply, out) in [(retry, retried), (20, supplied), (21, failed)] {
            assert_eq!(Twin::fault(&out, reply), Some(Fault::InFlight));
        }
        for (reply, out) in [(cancel, cancelled), (discard, discarded)] {
            assert!(out.replies(reply).is_empty() && out.writes() == 0, "the removal waits for the stage");
        }
    }
}

#[test]
fn xfer_pending_signals() {
    let mut twin = Twin::offline();
    let (id, _) = pending(&mut twin);
    twin.commit();
    for input in [TransferInput::Online(crate::owners::contract::Online(true)), TransferInput::PowerSaving(true), TransferInput::SlotFreed] {
        let out = twin.post(input);
        assert_eq!((twin.stage(id), out.steps().len(), out.round_needed().len(), out.timers), (TransferStage::AwaitingSource, 0, 0, 0));
    }
    let inputs = twin.stray_results(id);
    twin.ignores(id, inputs);
}

#[test]
fn xfer_pending_never_expires() {
    let mut twin = Twin::offline();
    let (id, _) = pending(&mut twin);
    twin.commit();
    for now in [7 * DAY, 7 * DAY + 1, 60 * DAY] {
        twin.det.tick(Time(now));
        let out = twin.post(TransferInput::Tick { kind: crate::owners::write::transfer::TransferTick::Expire });
        assert_eq!((twin.stage(id), out.writes()), (TransferStage::AwaitingSource, 0), "a waiting source has no expiry (owner decision)");
    }
}

#[test]
fn xfer_pending_batch_mixed() {
    let mut twin = Twin::offline();
    let mut waiting = item("Moment:r2:upload", 1);
    waiting.source = None;
    waiting.steps = None;
    twin.post(TransferInput::AcceptBatch { epoch: EPOCH, items: vec![item(KEY, 1), waiting], joint: Box::new(joint()), reply: 9 });
    let ids = [twin.state().open_id(KEY).unwrap(), twin.state().open_id("Moment:r2:upload").unwrap()];
    twin.reserve_batch(&ids);
    let out = twin.commit();
    assert_eq!((twin.stage(ids[0]), twin.stage(ids[1])), (TransferStage::Copying, TransferStage::AwaitingSource));
    assert_eq!((out.copies(), out.round_needed()), (1, vec![vec![ids[0]]]));
    assert!(out.replies(9).is_empty());
    twin.copy_done(ids[0]);
    let out = twin.commit();
    assert!(matches!(out.replies(9).as_slice(), [TransferAnswer::Receipts(receipts)] if receipts.iter().map(|r| r.op_id).collect::<Vec<_>>() == ids));
}

#[test]
fn xfer_supply_end() {
    for close in [false, true] {
        for persist in [false, true] {
            let mut twin = Twin::offline();
            let (id, _) = pending(&mut twin);
            twin.commit();
            supply(&mut twin, EPOCH, 9);
            if persist { twin.copy_done(id); }
            let out = if close { twin.close() } else { twin.end_epoch() };
            assert_eq!(out.replies(9), vec![TransferAnswer::Err(Fault::Ended(if close { crate::ffi::EndCause::Closed } else { crate::ffi::EndCause::SessionEnded }))]);
            assert_eq!(twin.stage(id), TransferStage::Done);
            assert_eq!(out.deletes(), 1);
            assert_eq!(out.copy_cancels(), usize::from(!persist));
        }
    }
}

#[test]
fn xfer_pending_interest() {
    let mut twin = Twin::offline();
    let (_, _) = pending(&mut twin);
    twin.commit();
    let out = twin.post(TransferInput::StatusInterestPlus { key: KEY.into(), stream_id: 43 });
    assert_eq!(out.statuses(), vec![(43, crate::owners::write::transfer::Shown::WaitingSource)]);
    let blob = crate::owners::read::deliveries::BlobStatus::of(&out.statuses()[0].1);
    assert_eq!((blob.state.as_str(), blob.wait_reason), ("waiting", Some(crate::owners::write::upload::WaitReason::SourcePending)));
}

fn cached(twin: &mut Twin, id: u64) -> Out {
    twin.post(TransferInput::CopyDone { transfer_id: id, attempt_id: twin.attempt(id), size: 10, sha256: "h".into(), place: crate::store::files::Location::Directory, cached: crate::owners::write::transfer::CachedBlob::Fresh { blob_id: "blob".into() } })
}

#[test]
fn xfer_a_cached_supply_skips_the_copy() {
    {
        let mut twin = Twin::offline();
        let (id, _) = pending(&mut twin);
        twin.commit();
        supply(&mut twin, EPOCH, 9);
        let out = cached(&mut twin, id);
        assert_eq!((twin.stage(id), out.writes()), (TransferStage::PersistingCopy, 1));
        assert!(out.rows().iter().any(|(row, _)| row.completed && row.signed_id.as_deref() == Some("blob")));
        assert!(out.batches[0].ops.iter().any(|op| matches!(op, crate::owners::store::StoreOp::Intent { .. })));
    }
    {
        let mut twin = Twin::offline();
        let (id, _) = pending(&mut twin);
        twin.commit();
        supply(&mut twin, EPOCH, 9);
        cached(&mut twin, id);
        let out = twin.commit();
        assert_eq!((twin.stage(id), out.replies(9), out.submit_ops().len()), (TransferStage::Mutating, vec![TransferAnswer::Ok], 1));
    }
}

fn pending_batch(twin: &mut Twin) -> [u64; 2] {
    let mut first = item(KEY, 1);
    let mut second = item("Moment:r2:upload", 1);
    first.source = None;
    second.source = None;
    first.steps = None;
    second.steps = None;
    twin.post(TransferInput::AcceptBatch { epoch: EPOCH, items: vec![first, second], joint: Box::new(joint()), reply: 8 });
    let ids = [twin.state().open_id(KEY).unwrap(), twin.state().open_id("Moment:r2:upload").unwrap()];
    twin.reserve_batch(&ids);
    twin.commit();
    ids
}

#[test]
fn xfer_supply_cached_batch() {
    let mut twin = Twin::offline();
    let ids = pending_batch(&mut twin);
    supply(&mut twin, EPOCH, 9);
    cached(&mut twin, ids[0]);
    let out = twin.commit();
    assert_eq!((twin.stage(ids[0]), twin.stage(ids[1]), out.submit_ops().len()), (TransferStage::BatchReady, TransferStage::AwaitingSource, 0));
    assert_eq!(out.replies(9), vec![TransferAnswer::Ok]);
}

fn source_failed(twin: &mut Twin) -> u64 {
    let (id, _) = pending(twin);
    twin.commit();
    twin.post(TransferInput::SourceFailed { epoch: EPOCH, key: KEY.into(), message: "preparation".into(), reply: 9 });
    twin.commit();
    id
}

#[test]
fn source_failed_retry_keeps_the_new_cycle_after_copy() {
    let mut twin = Twin::offline();
    let id = source_failed(&mut twin);
    let (reply, started) = twin.retry(KEY, Some(Source { path: "/source".into(), parts: 1 }));
    assert_eq!((started.writes(), started.copies()), (0, 1));
    let pending = twin.copy_done(id);
    assert_eq!(pending.rows()[0].0.cycle_id, 2);
    assert!(matches!(twin.commit().replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.cycle_id == 2));
}

#[test]
fn xfer_open_pending_source() {
    let mut row = stored(1, KEY, 1);
    row.awaiting_source = true;
    row.source = None;
    row.steps = None;
    row.file_present = false;
    let (mut twin, out) = Twin::opened(vec![row], Default::default());
    assert_eq!((twin.stage(1), out.copies(), out.steps().len()), (TransferStage::PersistingFailure, 0, 0));
    twin.commit();
    assert_eq!(twin.stage(1), TransferStage::Failed);
}

#[test]
fn xfer_wait_supply() {
    let mut twin = Twin::offline();
    let (id, _) = pending(&mut twin);
    twin.commit();
    supply(&mut twin, EPOCH, 9);
    assert!(twin.copy_done(id).replies(9).is_empty());
    assert_eq!(twin.commit().replies(9), vec![TransferAnswer::Ok]);
}

#[test]
fn xfer_wait_fail_source() {
    let mut twin = Twin::offline();
    pending(&mut twin);
    twin.commit();
    let out = twin.post(TransferInput::SourceFailed { epoch: EPOCH, key: KEY.into(), message: "preparation".into(), reply: 9 });
    assert!(out.replies(9).is_empty());
    assert_eq!(twin.commit().replies(9), vec![TransferAnswer::Ok]);
    for close in [false, true] {
        let mut twin = Twin::offline();
        pending(&mut twin);
        twin.commit();
        twin.post(TransferInput::SourceFailed { epoch: EPOCH, key: KEY.into(), message: "preparation".into(), reply: 9 });
        let out = if close { twin.close() } else { twin.end_epoch() };
        assert_eq!(out.replies(9), vec![TransferAnswer::Err(Fault::Ended(if close { crate::ffi::EndCause::Closed } else { crate::ffi::EndCause::SessionEnded }))]);
    }
}
