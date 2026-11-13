//! Owner-level tests of the owner `Transfer`: the multipart uploads of removed transfers (`orphans`), the names of copies, the first start of a small file and the restart of a multipart upload.

use crate::owners::contract::RejectReason;
use crate::test_wait::permutations;
use super::transfer_harness::*;
use crate::owners::write::transfer::{TransferId, TransferInput, TransferStage};

const KEY: &str = "Moment:r1:upload";

/// `Uploading` part 2 of 3 with the multipart upload `u1` on the server.
fn with_upload(twin: &mut Twin, key: &str) -> TransferId {
    let id = twin.uploading(key, 3);
    twin.step_ok(id, 1, Some("u1"));
    twin.commit();
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Uploading);
    id
}

fn orphan_restart(orphans: Vec<(String, String)>) -> Twin {
    let mut twin = Twin::bare(1);
    twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())], complete: false });
    twin.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default(), orphans, batches: Default::default(), held_orphan: None });
    twin
}

#[test]
fn xfer_discard_deferred_abort() {
    let mut twin = Twin::new();
    let id = with_upload(&mut twin, KEY);
    twin.online(false);
    assert_eq!(twin.stage(id), TransferStage::Ready);
    let (reply, out) = twin.cancel(KEY);
    assert_eq!(out.orphans_written().len(), 1);
    let out = twin.commit();
    assert_eq!((out.replies(reply).len(), out.orphan_aborts().len()), (1, 0), "the call answers without a network call");
    assert_eq!(twin.online(true).orphan_aborts(), vec!["u1".to_string()], "the abort goes with the network");
}

#[test]
fn xfer_orphan_abort_waits_declaration() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Event { Online, Offline, Restored, RestoreReady, Register }
    use Event::*;
    let orders: Vec<_> = permutations(&[Online, Offline, Restored, RestoreReady, Register]).into_iter().filter(|order| order.iter().position(|e| *e == Restored) < order.iter().position(|e| *e == RestoreReady)).collect();
    for order in orders {
        let mut twin = Twin::bare(1);
        let mut aborts = Vec::new();
        let mut declared = false;
        for event in order.iter().copied().chain([Online]) {
            let out = match event {
                Online => twin.online(true),
                Offline => twin.online(false),
                Restored => twin.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default(), orphans: vec![("u9".into(), KEY.into())], batches: Default::default(), held_orphan: None }),
                RestoreReady => twin.post(TransferInput::RestoreReady { epoch: EPOCH }),
                Register => twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())], complete: false }),
            };
            declared |= event == Register;
            assert!(declared || out.orphan_aborts().is_empty(), "{order:?} at {event:?}: an abort before the declaration would be refused for good, got {:?}", out.orphan_aborts());
            aborts.extend(out.orphan_aborts());
        }
        assert_eq!(aborts, ["u9".to_string()], "{order:?}: the orphan abort goes out once");
    }
}

/// transfer.md:13, lifecycle.md:525 (D-280): the answer of a restored abort pair comes as a step answer; Transfer finds the debt by the identity of the pair and settles it as AbortDone.
#[test]
fn xfer_restored_orphan_answer() {
    let mut twin = orphan_restart(vec![("u9".into(), KEY.into())]);
    twin.online(true);
    let request_id = crate::owners::write::wire::abort_request_id("u9");
    let out = twin.post(TransferInput::StepAnswer { request_id, outcome: crate::owners::session::AuthorOutcome::Response { body: "{\"data\":{\"abortMultipart\":{\"ok\":true}}}".into(), kind: crate::transport::AnswerKind::Answered { sequence: None }, pending_retained: false } });
    assert_eq!(out.orphans_removed(), vec!["u9".to_string()], "the confirmed step answer of the abort pair removes the debt: {out:?}");
    twin.commit();
    twin.online(false);
    assert!(twin.online(true).orphan_aborts().is_empty(), "an answered abort is not sent again");
}

/// transfer.md:13, 330-331 (D-280): a restored abort answer that carries errors or no root is a refusal, the debt stays and no RemoveOrphan is written.
#[test]
fn xfer_restored_orphan_answer_refused() {
    for body in ["{\"errors\":[{\"message\":\"no\"}]}", "{}", "{\"data\":{\"other\":{}}}"] {
        let mut twin = orphan_restart(vec![("u9".into(), KEY.into())]);
        twin.online(true);
        let request_id = crate::owners::write::wire::abort_request_id("u9");
        let out = twin.post(TransferInput::StepAnswer { request_id, outcome: crate::owners::session::AuthorOutcome::Response { body: body.into(), kind: crate::transport::AnswerKind::Answered { sequence: None }, pending_retained: false } });
        assert!(out.orphans_removed().is_empty(), "{body}: an unconfirmed abort keeps the debt: {out:?}");
    }
}

/// store.md:93-95, transfer.md:458-459 (D-277): the Control batch of a minted debt refused with Storage is written again by its timer after the epoch ended, as a Control batch.
#[test]
fn xfer_minted_control_rewritten_after_end() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.online(false);
    minted(&mut twin, id, "u1");
    let refused = twin.reject(RejectReason::NoSession);
    assert_eq!(refused.orphans_written(), vec![("u1".to_string(), KEY.to_string())], "the debt goes to the Store as a Control batch");
    let ended = twin.end_epoch();
    assert!(ended.orphans_written().is_empty(), "the end of the epoch writes the pending debt no second time: {ended:?}");
    let storage = twin.reject(RejectReason::Storage("busy".into()));
    assert_eq!((storage.orphans_written().len(), storage.timers, storage.orphan_aborts().len()), (0, 1, 0), "a Storage refusal of the Control batch arms the retry of the write: {storage:?}");
    let again = minted_due(&mut twin, "u1");
    let control: Vec<_> = again.batches.iter().filter(|batch| batch.stamp == crate::runtime::EpochStamp::Control).collect();
    assert_eq!((control.len(), again.orphans_written()), (1, vec![("u1".to_string(), KEY.to_string())]), "the ended epoch does not suppress the write and the batch is Control: {again:?}");
    twin.commit();
    twin.close();
    let mut reopened = orphan_restart(vec![("u1".into(), KEY.into())]);
    assert_eq!(reopened.online(true).orphan_aborts(), vec!["u1".to_string()], "the durable debt is aborted once after the restart");
}

#[test]
fn xfer_orphan_abort_owed() {
    let mut twin = orphan_restart(vec![("u9".into(), KEY.into())]);
    assert_eq!(twin.online(true).orphan_aborts(), vec!["u9".to_string()]);
    twin.online(false);
    assert!(twin.online(true).orphan_aborts().is_empty(), "the abort in flight is not sent twice");
    assert!(twin.post(TransferInput::RestoreReady { epoch: EPOCH }).orphan_aborts().is_empty());
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), result: Ok(()) });
    assert_eq!((out.orphans_removed(), out.orphan_aborts()), (vec!["u9".to_string()], Vec::<String>::new()), "the answer ends the debt");
    twin.commit();
    twin.online(false);
    assert!(twin.online(true).orphan_aborts().is_empty());
}

#[test]
fn xfer_orphan_abort_owed_resent() {
    let mut twin = orphan_restart(vec![("u9".into(), KEY.into())]);
    twin.online(true);
    twin.online(false);
    twin.online(true);
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), result: Err(crate::owners::write::transfer::AbortFault { temporary: true, status: None }) });
    assert_eq!(out.orphan_aborts(), vec!["u9".to_string()], "the Online(true) before the failed answer owes an abort");
    twin.online(false);
    assert!(twin.online(true).orphan_aborts().is_empty(), "the resent abort is still in flight");
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), result: Err(crate::owners::write::transfer::AbortFault { temporary: true, status: None }) });
    assert_eq!(out.orphan_aborts(), vec!["u9".to_string()], "the new Online(true) is owed in its turn");
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), result: Err(crate::owners::write::transfer::AbortFault { temporary: true, status: None }) });
    assert!(out.orphan_aborts().is_empty(), "the debt is paid once");
}

#[test]
fn xfer_orphan_abort_owed_offline() {
    let mut twin = orphan_restart(vec![("u9".into(), KEY.into())]);
    twin.online(true);
    twin.online(false);
    twin.online(true);
    twin.online(false);
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), result: Err(crate::owners::write::transfer::AbortFault { temporary: true, status: None }) });
    assert!(out.orphan_aborts().is_empty(), "no abort while offline");
    assert_eq!(twin.online(true).orphan_aborts(), vec!["u9".to_string()], "the next network sends it once");
}

#[test]
fn xfer_prop_orphan_abort() {
    for (state, discard) in [("uploading", false), ("uploading", true), ("completing", false), ("completing", true)] {
        let mut twin = Twin::new();
        let id = if state == "uploading" {
            with_upload(&mut twin, KEY)
        } else {
            let id = twin.uploading(KEY, 2);
            twin.step_ok(id, 1, Some("u1"));
            twin.commit();
            twin.commit();
            twin.step_ok(id, 2, None);
            twin.commit();
            twin.commit();
            assert_eq!(twin.stage(id), TransferStage::Completing);
            id
        };
        let (_, out) = if discard { twin.discard(KEY) } else { twin.cancel(KEY) };
        let label = format!("{state} discard={discard}");
        assert_eq!(out.orphans_written(), vec![("u1".to_string(), KEY.to_string())], "{label}: one orphan, with the removal");
        let committed = twin.commit();
        assert_eq!((committed.orphan_aborts(), committed.completes()), (vec!["u1".to_string()], 0), "{label}: abort after Committed, no complete after the removal");
        assert_eq!(twin.stage(id), TransferStage::Done, "{label}");
        let lost = twin.post(TransferInput::AbortDone { upload_id: "u1".into(), result: Err(crate::owners::write::transfer::AbortFault { temporary: true, status: None }) });
        assert!(lost.orphans_removed().is_empty(), "{label}: the row stays until the server answers");
        twin.online(false);
        assert_eq!(twin.online(true).orphan_aborts(), vec!["u1".to_string()], "{label}: the abort comes back with the network");
    }
}

#[test]
fn xfer_key_path_chars() {
    let key = "Moment:a/../b:c:upload";
    let mut twin = Twin::new();
    let id = twin.uploading(key, 1);
    let (_, out) = twin.cancel(key);
    assert_eq!(out.removed(), vec![key.to_string()]);
    let out = twin.commit();
    assert_eq!(out.delete_paths(), vec![format!("transfers/{id}")], "the path of the copy holds the id of the transfer and nothing of the key");
}

fn minted(twin: &mut Twin, id: TransferId, upload_id: &str) -> Out {
    let attempt_id = twin.state().transfers.get(&id).map_or(0, |transfer| transfer.attempt_id);
    twin.post(TransferInput::UploadMinted { transfer_id: id, attempt_id, upload_id: upload_id.into(), key: KEY.into() })
}

#[test]
fn xfer_discard_before_the_cancelled_attempt_answers() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    minted(&mut twin, id, "u1");
    twin.commit();
    twin.online(false);
    let (reply, out) = twin.discard(KEY);
    assert_eq!((out.orphans_written(), out.removed(), out.writes()), (vec![("u1".to_string(), KEY.to_string())], vec![KEY.to_string()], 1), "the removal carries the orphan of the minted upload");
    assert_eq!(twin.stage(id), TransferStage::Removing);
    let attempt_id = twin.attempt(id);
    twin.ignores(id, vec![TransferInput::StepError { transfer_id: id, attempt_id, temporary: true, error: super::transfer_harness::refusal() }]);
    assert_eq!(twin.commit().replies(reply).len(), 1);
    assert_eq!(twin.online(true).orphan_aborts(), vec!["u1".to_string()]);
}

#[test]
fn xfer_upload_minted_late() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let (_, out) = twin.discard(KEY);
    assert!(out.orphans_written().is_empty(), "no upload id is known at the discard");
    let out = minted(&mut twin, id, "u1");
    assert_eq!((out.orphans_written(), out.writes(), out.orphan_aborts().len()), (vec![("u1".to_string(), KEY.to_string())], 1, 0), "in Removing the orphan is written, no abort before Committed");
    twin.commit();
    let out = twin.commit();
    assert_eq!(out.orphan_aborts(), vec!["u1".to_string()], "the abort goes once the orphan is written");
    let gone = twin.post(TransferInput::AbortDone { upload_id: "u1".into(), result: Ok(()) });
    assert_eq!(gone.orphans_removed(), vec!["u1".to_string()]);
    twin.commit();
    let mut after = Twin::new();
    let id = after.uploading(KEY, 3);
    after.discard(KEY);
    after.commit();
    let out = minted(&mut after, id, "u2");
    assert_eq!(out.orphans_written(), vec![("u2".to_string(), KEY.to_string())], "after the removal the orphan is written too");
    assert_eq!(after.commit().orphan_aborts(), vec!["u2".to_string()]);
}

#[test]
fn xfer_upload_minted_dup() {
    let mut twin = Twin::new();
    let id = with_upload(&mut twin, KEY);
    let out = minted(&mut twin, id, "u9");
    assert!(Twin::quiet(&out) && twin.t(id).upload_id.as_deref() == Some("u1"), "a recorded upload id stays: {out:?}");
    let mut late = Twin::new();
    let id = late.uploading(KEY, 3);
    late.discard(KEY);
    minted(&mut late, id, "u1");
    assert!(Twin::quiet(&minted(&mut late, id, "u1")), "the same id twice writes one orphan");
}

#[test]
fn xfer_singleton_row_upload() {
    use crate::owners::write::outbox::OutboxOutcome;
    use crate::owners::write::transfer::TransferAnswer;
    let key = "Profile:main:avatar";
    let mut twin = Twin::new();
    let id = twin.mutating(key);
    twin.mutation_outcome(key, OutboxOutcome::Confirmed { revision: 3, result: Some(r#"{"avatarUrl":"u"}"#.into()), row_id: None });
    twin.commit();
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Confirmed { revision: 3, result: Some(r#"{"avatarUrl":"u"}"#.into()), row_id: None }, None)], "the outcome carries the returns of the final mutation");
    let failed_key = "Profile:main:cover";
    let failed = twin.mutating(failed_key);
    let error = r#"{"kind":"Rejected","message":"no","extensions":{"code":"BAD"}}"#;
    twin.mutation_outcome(failed_key, OutboxOutcome::Rejected { error: error.into() });
    twin.commit();
    let (reply, out) = twin.outcome(failed, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Failed { error: error.into() }, None)], "the error and its extensions reach the outcome");
}

fn minted_due(twin: &mut Twin, upload_id: &str) -> Out {
    let attempt_id = twin.state().minted_attempt(upload_id).expect("the write of the minted id is owed");
    twin.det.tick(crate::runtime::Time(1));
    twin.post(TransferInput::Tick { kind: crate::owners::write::transfer::TransferTick::Minted { upload_id: upload_id.into(), attempt_id } })
}

/// xfer_minted_write_storage (P-XFER-41, D-192): the Store refuses the write of the id of a live transfer. The debt stays and nothing is aborted.
#[test]
fn xfer_minted_write_storage() {
    for reason in [RejectReason::Storage("disk".into()), RejectReason::StaleGeneration { models: vec!["Moment".into()], generations: [("Moment".into(), 0)].into() }] {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        twin.online(false);
        let out = minted(&mut twin, id, "u1");
        assert_eq!(out.writes(), 1, "{reason:?}");

        let refused = twin.reject(reason.clone());
        assert_eq!((refused.timers, refused.orphan_aborts().len(), refused.orphans_written().len(), twin.t(id).upload_id.as_deref()), (1, 0, 0, Some("u1")), "{reason:?}: the debt is kept and scheduled, nothing is aborted: {refused:?}");
        assert!(minted(&mut twin, id, "u1").runs.is_empty(), "{reason:?}: the same id again is no new debt");
        assert_eq!(minted_due(&mut twin, "u1").writes(), 1, "{reason:?}: the write goes again");
    }
}

/// xfer_minted_write_interrupted (D-277): the Store refuses the epoch batch of the minted id with NoSession or Closed. The same step writes the debt as a Control batch `UploadOrphan`; nothing is scheduled or aborted before its Committed, and the first network admission after it aborts the id.
#[test]
fn xfer_minted_write_interrupted() {
    for reason in [RejectReason::NoSession, RejectReason::Closed] {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        twin.online(false);
        let out = minted(&mut twin, id, "u1");
        assert_eq!(out.writes(), 1, "{reason:?}");
        let refused = twin.reject(reason.clone());
        let control: Vec<_> = refused.batches.iter().filter(|batch| batch.stamp == crate::runtime::EpochStamp::Control).collect();
        assert_eq!((control.len(), refused.orphans_written(), refused.timers, refused.orphan_aborts().len(), twin.t(id).upload_id.as_deref()), (1, vec![("u1".to_string(), KEY.to_string())], 0, 0, Some("u1")), "{reason:?}: the debt goes out as one Control batch, nothing else: {refused:?}");
        assert!(twin.online(true).orphan_aborts().is_empty(), "{reason:?}: no abort before the Control batch is committed");
        twin.online(false);
        twin.commit();
        assert_eq!(twin.online(true).orphan_aborts(), vec!["u1".to_string()], "{reason:?}: the first network admission after the durable debt aborts it");
    }
}

/// xfer_minted_write_invalid (P-DATA-97, D-399): the Store refuses the write of the minted id as invalid and would refuse the same batch again. The debt goes out at once as a Control batch `UploadOrphan`, nothing is scheduled and nothing is aborted.
#[test]
fn xfer_minted_write_invalid() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.online(false);
    let out = minted(&mut twin, id, "u1");
    assert_eq!(out.writes(), 1);
    let refused = twin.reject(RejectReason::Invalid(String::new()));
    let control: Vec<_> = refused.batches.iter().filter(|batch| batch.stamp == crate::runtime::EpochStamp::Control).collect();
    assert_eq!((control.len(), refused.orphans_written(), refused.timers, refused.orphan_aborts().len(), twin.t(id).upload_id.as_deref()), (1, vec![("u1".to_string(), KEY.to_string())], 0, 0, Some("u1")), "the record goes out at once: {refused:?}");
}

/// xfer_minted_write_other_no_abort (K1, D-192, transfer.md:201, 459): the minted upload_id of a LIVE transfer
/// is written to Store, and that write is Rejected(Invalid | StaleGeneration) while online. The upload_id is not
/// an orphan yet: the transfer stays alive and the rewrite is scheduled. No abort may leave before the row says
/// the upload is an orphan; the abort is owed only after a removal records it. Prod inserts the live id into
/// orphans and sends the abort at once (send_orphans while online), deleting the server multipart of a transfer
/// that keeps uploading.
#[test]
fn xfer_minted_write_other_no_abort() {
    for (reason, timers) in [(RejectReason::Invalid(String::new()), 0), (RejectReason::StaleGeneration { models: vec!["Moment".into()], generations: [("Moment".into(), 0)].into() }, 1)] {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        let out = minted(&mut twin, id, "u1");
        assert_eq!(out.writes(), 1, "{reason:?}: the minted id of the live transfer is written");
        let refused = twin.reject(reason.clone());
        assert_eq!(
            (refused.orphan_aborts(), refused.aborts(), refused.timers, twin.t(id).upload_id.as_deref()),
            (Vec::<String>::new(), 0, timers, Some("u1")),
            "{reason:?}: no abort leaves for the live upload, the debt is kept: {refused:?}",
        );
        if matches!(reason, RejectReason::Invalid(_)) {
            assert_eq!(refused.orphans_written().len(), 1, "the record of the debt is written at once");
            let storage = twin.reject(RejectReason::Storage("disk".into()));
            assert_eq!((storage.orphan_aborts(), storage.aborts(), storage.timers), (Vec::<String>::new(), 0, 1), "the refused record waits for its backoff: {storage:?}");
            let again = minted_due(&mut twin, "u1");
            assert_eq!((again.writes(), again.orphans_written()), (1, vec![("u1".to_string(), KEY.to_string())]), "the tick writes the record again, not the row: {again:?}");
            let committed = twin.commit();
            assert_eq!((committed.orphan_aborts(), committed.aborts()), (Vec::<String>::new(), 0), "the record of a live upload is no abort: {committed:?}");
            assert!(twin.online(true).orphan_aborts().is_empty(), "the network does not abort it either");
        }
    }
}

/// Drives the single part of an upload whose id `u1` is refused as `Invalid`, up to `Completing`, answering every batch but the Control record.
fn invalid_record_open(twin: &mut Twin) -> (TransferId, crate::owners::contract::BatchId) {
    let id = twin.uploading(KEY, 1);
    minted(twin, id, "u1");
    let refused = twin.reject(RejectReason::Invalid(String::new()));
    let record = refused.batches.iter().find(|batch| batch.stamp == crate::runtime::EpochStamp::Control).expect("the record batch").id;
    (id, record)
}

/// xfer_minted_recorded_orphan_released_without_abort (D-399, P-XFER-49): the record of an id that a live transfer holds is removed without abort, once a row of the transfer carries the id, or once the upload is complete.
#[test]
fn xfer_minted_recorded_orphan_released_without_abort() {
    let mut twin = Twin::new();
    let (id, _) = invalid_record_open(&mut twin);
    assert!(twin.commit().orphans_removed().is_empty(), "the record alone releases nothing");
    let part = twin.step_ok(id, 1, None);
    let mut removed = part.orphans_removed();
    let mut aborted = part.orphan_aborts();
    while twin.has_pending() {
        let out = twin.commit();
        removed.extend(out.orphans_removed());
        aborted.extend(out.orphan_aborts());
    }
    assert_eq!((removed, aborted), (vec!["u1".to_string()], Vec::<String>::new()), "a durable row of the transfer replaces the record");

    let mut twin = Twin::new();
    let (id, record) = invalid_record_open(&mut twin);
    let first = twin.step_ok(id, 1, None);
    let mut removed = first.orphans_removed();
    let mut aborted = first.orphan_aborts();
    let mut later: std::collections::VecDeque<_> = first.batches.into_iter().map(|batch| batch.id).collect();
    while let Some(batch) = later.pop_front() {
        let out = twin.commit_shown(batch);
        removed.extend(out.orphans_removed());
        aborted.extend(out.orphan_aborts());
        later.extend(out.batches.into_iter().map(|batch| batch.id));
    }
    assert_eq!(twin.stage(id), TransferStage::Completing);
    let done = twin.completed(id);
    let out = twin.commit_shown(record);
    removed.extend(done.orphans_removed().into_iter().chain(out.orphans_removed()));
    aborted.extend(done.orphan_aborts().into_iter().chain(out.orphan_aborts()));
    assert_eq!((removed, aborted), (vec!["u1".to_string()], Vec::<String>::new()), "the record of a complete upload goes without abort once: {out:?}");
}

/// xfer_minted_recorded_orphan_aborted_when_the_upload_is_gone (D-399): the record was written, the transfer leaves before any row carries the id. In a live epoch the id is aborted by the removal, at the end of the epoch nothing is written again.
#[test]
fn xfer_minted_recorded_orphan_aborted_when_the_upload_is_gone() {
    let mut twin = Twin::new();
    let (_, _) = invalid_record_open(&mut twin);
    twin.commit();
    let (_, discarded) = twin.discard(KEY);
    let mut aborted = discarded.orphan_aborts();
    while twin.has_pending() {
        aborted.extend(twin.commit().orphan_aborts());
    }
    assert_eq!(aborted, vec!["u1".to_string()], "the removal aborts the upload once");

    let mut twin = Twin::new();
    let (_, _) = invalid_record_open(&mut twin);
    twin.commit();
    let ended = twin.end_epoch_by(crate::runtime::Cause::SessionReset);
    assert_eq!((ended.orphans_written(), ended.orphan_aborts()), (Vec::new(), Vec::<String>::new()), "the end of the epoch writes the record no second time: {ended:?}");
}

/// xfer_minted_recorded_orphan_restored (D-399, `Restored`): a record whose id a stored row of a transfer holds goes without abort, a record without such a row is aborted after the open.
#[test]
fn xfer_minted_recorded_orphan_restored() {
    for (held, removed, aborted) in [("u1", vec!["u1".to_string()], Vec::<String>::new()), ("u2", Vec::new(), vec!["u1".to_string()])] {
        let mut row = stored(1, KEY, 2);
        row.upload_id = Some(held.into());
        let mut twin = Twin::bare(1);
        twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())], complete: false });
        let opened = twin.post(TransferInput::Restored { uploads: vec![row], outcomes: Default::default(), orphans: vec![("u1".into(), KEY.into())], batches: Default::default(), held_orphan: None });
        let ready = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
        let online = twin.online(true);
        let mut found = opened.orphan_aborts();
        found.extend(ready.orphan_aborts());
        found.extend(online.orphan_aborts());
        assert_eq!((opened.orphans_removed(), found), (removed, aborted), "row holds {held}: {opened:?}");
    }
}

/// xfer_minted_write_retry (P-XFER-41, D-192): the tick of a refused write sends the write again with the current row, and Committed of the repeat settles the debt and keeps the id durable.
#[test]
fn xfer_minted_write_retry() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.online(false);
    minted(&mut twin, id, "u1");
    twin.reject(RejectReason::Storage("disk".into()));

    let again = minted_due(&mut twin, "u1");
    assert_eq!(again.writes(), 1, "the write goes again");
    twin.commit();
    assert_eq!(twin.state().minted_attempt("u1"), None, "Committed settles the debt");
    let (_, discarded) = twin.discard(KEY);
    assert_eq!(discarded.orphans_written(), vec![("u1".to_string(), KEY.to_string())], "the removal carries the orphan of the durable id");
}

/// xfer_minted_debt_ends_with_its_upload (P-XFER-49): the debt of a refused write of the minted id belongs to an upload that is not complete. After complete is confirmed the end of the epoch writes no orphan of that id and no abort leaves.
#[test]
fn xfer_minted_debt_ends_with_its_upload() {
    for (case, confirmed, closing) in [("complete confirmed", false, false), ("complete confirmed, close", false, true), ("mutation confirmed", true, false)] {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 1);
        minted(&mut twin, id, "u1");
        let refused = twin.reject(RejectReason::Storage("disk".into()));
        assert_eq!((refused.timers, twin.t(id).upload_id.as_deref()), (1, Some("u1")), "{case}: the debt is kept and scheduled");
        twin.step_ok(id, 1, None);
        while twin.has_pending() {
            twin.commit();
        }
        twin.completed(id);
        while twin.has_pending() {
            twin.commit();
        }
        assert_eq!(twin.stage(id), TransferStage::Mutating, "{case}");
        if confirmed {
            twin.mutation_outcome(KEY, crate::owners::write::outbox::OutboxOutcome::Confirmed { revision: 1, result: None, row_id: None });
            while twin.has_pending() {
                twin.commit();
            }
        }
        let ended = twin.end_epoch_by(if closing { crate::runtime::Cause::Close } else { crate::runtime::Cause::SessionReset });
        assert_eq!((ended.orphans_written(), ended.orphan_aborts()), (Vec::new(), Vec::<String>::new()), "{case}: a complete upload leaves no orphan: {ended:?}");
    }
}

/// xfer_minted_write_interrupted_after_complete (P-XFER-49): the write of the minted id is still in flight when complete is confirmed. A NoSession or Closed refusal of that write leaves no orphan of the complete upload.
#[test]
fn xfer_minted_write_interrupted_after_complete() {
    for reason in [RejectReason::NoSession, RejectReason::Closed] {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 1);
        assert_eq!(minted(&mut twin, id, "u1").writes(), 1, "{reason:?}: the write of the minted id is in flight");
        let mut later: std::collections::VecDeque<_> = twin.step_ok(id, 1, None).batches.into_iter().map(|batch| batch.id).collect();
        while let Some(batch) = later.pop_front() {
            later.extend(twin.commit_shown(batch).batches.into_iter().map(|batch| batch.id));
        }
        assert_eq!(twin.stage(id), TransferStage::Completing, "{reason:?}");
        twin.completed(id);
        assert_eq!(twin.stage(id), TransferStage::PersistingComplete, "{reason:?}");
        let refused = twin.reject(reason.clone());
        assert_eq!((refused.orphans_written(), refused.orphan_aborts()), (Vec::new(), Vec::<String>::new()), "{reason:?}: a complete upload leaves no orphan: {refused:?}");
    }
}

/// xfer_minted_debt_ends_with_a_durable_row (P-XFER-49): the write of the minted id was refused, then a later write of the row of the transfer carries the id and is committed. The id is durable: the tick writes nothing and the close writes no orphan of the live upload.
#[test]
fn xfer_minted_debt_ends_with_a_durable_row() {
    for closing in [false, true] {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        minted(&mut twin, id, "u1");
        let refused = twin.reject(RejectReason::Storage("disk".into()));
        assert_eq!(refused.timers, 1, "the debt is scheduled");
        let attempt_id = twin.state().minted_attempt("u1").expect("the write of the minted id is owed");
        let part = twin.step_ok(id, 1, None);
        assert_eq!(part.rows().iter().map(|(row, _)| row.upload_id.clone()).collect::<Vec<_>>(), vec![Some("u1".to_string())], "the row of the confirmed part carries the minted id: {part:?}");
        while twin.has_pending() {
            twin.commit();
        }
        let after = if closing {
            twin.end_epoch_by(crate::runtime::Cause::Close)
        } else {
            twin.det.tick(crate::runtime::Time(1));
            twin.post(TransferInput::Tick { kind: crate::owners::write::transfer::TransferTick::Minted { upload_id: "u1".into(), attempt_id } })
        };
        assert_eq!((after.writes(), after.orphans_written()), (0, Vec::new()), "closing {closing}: the id is durable in the row of the transfer: {after:?}");
    }
}

/// xfer_minted_write_committed (P-XFER-41, D-192): the id of a removed transfer is aborted only after its orphan record is durable. A refused write is repeated, and the abort comes after Committed of the repeat.
#[test]
fn xfer_minted_write_committed() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.discard(KEY);
    twin.commit();
    let out = minted(&mut twin, id, "u1");
    assert_eq!((out.orphans_written(), out.orphan_aborts().len()), (vec![("u1".to_string(), KEY.to_string())], 0));

    let refused = twin.reject(RejectReason::Storage("disk".into()));
    assert_eq!((refused.timers, refused.orphan_aborts().len()), (1, 0), "no abort before the orphan is durable: {refused:?}");

    let again = minted_due(&mut twin, "u1");
    assert_eq!((again.orphans_written(), again.orphan_aborts().len()), (vec![("u1".to_string(), KEY.to_string())], 0), "the same record is written again");
    let committed = twin.commit();
    assert_eq!(committed.orphan_aborts(), vec!["u1".to_string()], "the abort goes once the record is durable");
    let refused_abort = twin.post(TransferInput::AbortDone { upload_id: "u1".into(), result: Err(crate::owners::write::transfer::AbortFault { temporary: true, status: None }) });
    assert_eq!(refused_abort.orphans_removed().len(), 0, "an unanswered abort keeps the record");
}

#[test]
fn xfer_orphan_remove_rejected() {
    let mut twin = orphan_restart(vec![("u9".into(), KEY.into())]);
    twin.online(true);
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), result: Ok(()) });
    assert_eq!(out.orphans_removed(), vec!["u9".to_string()]);
    twin.online(false);
    assert!(twin.online(true).orphan_aborts().is_empty(), "no second abort while the removal is not committed");
    twin.reject(RejectReason::Storage("disk".into()));
    twin.online(false);
    assert_eq!(twin.online(true).orphan_aborts(), vec!["u9".to_string()], "the refused removal keeps the debt and the abort goes again");
}

fn init_answer(twin: &mut Twin, body: &str, request_id: u64) -> Out {
    let outcome = crate::owners::session::AuthorOutcome::Response { body: body.into(), kind: crate::transport::AnswerKind::Answered { sequence: None }, pending_retained: false };
    twin.post(TransferInput::StepAnswer { request_id, outcome })
}

const INIT: &str = r#"{"data":{"initMultipart":{"key":"k","uploadId":"u1"}}}"#;

fn removed_with_init_in_flight(discard: bool) -> (Twin, u64) {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let request_id = crate::owners::write::wire::step_request_id(id, twin.t(id).cycle_id, crate::owners::write::upload::UploadStage::Init, 1);
    if discard { twin.discard(KEY) } else { twin.cancel(KEY) };
    twin.commit();
    (twin, request_id)
}

/// A transfer that failed its `Init` step and was retried: the cycle grew, the request id is the one of the cycle before.
fn failed_then_retried() -> (Twin, u64) {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let cycle = twin.t(id).cycle_id;
    let request_id = crate::owners::write::wire::step_request_id(id, cycle, crate::owners::write::upload::UploadStage::Init, 1);
    twin.step_error(id, false);
    while twin.has_pending() { twin.commit(); }
    assert_eq!(twin.stage(id), TransferStage::Failed);
    let (reply, out) = twin.retry(KEY, None);
    assert_ne!(Twin::fault(&out, reply), Some(crate::owners::write::outbox::Fault::InFlight), "the retry of a failed transfer starts a new cycle");
    while twin.has_pending() { twin.commit(); }
    assert!(twin.t(id).cycle_id > cycle, "the retry gave a new cycle");
    (twin, request_id)
}

/// xfer_released_init_answer_owes_abort: the answer of a released `Init` pair that created a multipart upload no transfer owns writes its debt like a late `UploadMinted`, then the abort goes; an answer without the upload, with errors, or for an upload that has an owner writes nothing.
#[test]
fn xfer_released_init_answer_owes_abort() {
    let owed = crate::owners::write::upload_codec::multipart_id("k", "u1");
    let (mut twin, request_id) = removed_with_init_in_flight(true);
    let out = init_answer(&mut twin, INIT, request_id);
    assert_eq!((out.orphans_written(), out.writes(), out.orphan_aborts().len()), (vec![(owed.clone(), "k".to_string())], 1, 0), "the debt is written, no abort before Committed");
    assert_eq!(twin.commit().orphan_aborts(), vec![owed.clone()], "the abort goes once the debt is written");
    assert!(Twin::quiet(&init_answer(&mut twin, INIT, request_id)), "the same answer twice owes once");

    for (name, body) in [("no upload id", r#"{"data":{"initMultipart":{"key":"k"}}}"#), ("errors", r#"{"errors":[{"message":"no"}],"data":null}"#), ("another root", r#"{"data":{"other":{"key":"k","uploadId":"u1"}}}"#), ("not json", "{")] {
        let (mut twin, request_id) = removed_with_init_in_flight(true);
        assert!(Twin::quiet(&init_answer(&mut twin, body, request_id)), "{name}: nothing is owed");
    }
    let (mut twin, request_id) = removed_with_init_in_flight(true);
    let dropped = twin.post(TransferInput::StepAnswer { request_id, outcome: crate::owners::session::AuthorOutcome::Dropped });
    assert!(Twin::quiet(&dropped), "a dropped pair owes nothing");

    let (mut owner, request_id) = failed_then_retried();
    let live = owner.state().open_id(KEY).expect("the retried transfer");
    minted(&mut owner, live, &owed);
    assert!(Twin::quiet(&init_answer(&mut owner, INIT, request_id)), "the upload of a live transfer has its owner, an answer of an older cycle owes nothing for it");
    let (mut other, request_id) = failed_then_retried();
    let out = init_answer(&mut other, INIT, request_id);
    assert_eq!(out.orphans_written(), vec![(owed, "k".to_string())], "an answer of an older cycle of a live transfer, for an upload nobody owns, is a debt");
}

/// xfer_removed_with_init_in_flight_owes_abort: the removal of a transfer whose `Init` step is in flight (or was cancelled by the network and waits) releases the pair of the step once, so its answer reaches Transfer; no release when the upload is known, when the step was answered, or when a step goes back to `Ready` by the network.
#[test]
fn xfer_removed_with_init_in_flight_owes_abort() {
    for discard in [true, false] {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        let request_id = crate::owners::write::wire::step_request_id(id, twin.t(id).cycle_id, crate::owners::write::upload::UploadStage::Init, 1);
        let (_, out) = if discard { twin.discard(KEY) } else { twin.cancel(KEY) };
        let removed = twin.commit();
        assert_eq!([out.releases(), removed.releases()].concat(), vec![request_id], "discard {discard}: one Release of the Init pair");
    }
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let request_id = crate::owners::write::wire::step_request_id(id, twin.t(id).cycle_id, crate::owners::write::upload::UploadStage::Init, 1);
    assert!(twin.online(false).releases().is_empty(), "the network sends the step back, the pair is replayed by the same id");
    let (_, out) = twin.discard(KEY);
    assert_eq!(out.releases(), vec![request_id], "the removal of the waiting transfer releases the cancelled pair");

    let mut twin = Twin::new();
    with_upload(&mut twin, KEY);
    let (_, out) = twin.discard(KEY);
    assert!(out.releases().is_empty(), "the upload is known: its debt is in the removal batch");
}

/// P-XFER-52 (xfer_prop_init_multipart_never_unowned): in every order of the discard, the report of the live step and the answer of the released pair, the multipart upload of `Init` is owed once.
#[test]
fn xfer_prop_init_multipart_never_unowned() {
    let owed = crate::owners::write::upload_codec::multipart_id("k", "u1");
    for order in permutations(&[0, 1, 2]) {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        let request_id = crate::owners::write::wire::step_request_id(id, twin.t(id).cycle_id, crate::owners::write::upload::UploadStage::Init, 1);
        let mut written = Vec::new();
        for event in &order {
            let out = match event {
                0 => twin.discard(KEY).1,
                1 => minted(&mut twin, id, &owed),
                _ => init_answer(&mut twin, INIT, request_id),
            };
            written.extend(out.orphans_written());
            while twin.has_pending() { written.extend(twin.commit().orphans_written()); }
        }
        assert_eq!(written.into_iter().map(|(upload_id, _)| upload_id).collect::<Vec<_>>(), vec![owed.clone()], "{order:?}: one debt of the upload");
    }
}
