//! Owner-level tests of the owner `Transfer`: the multipart uploads of removed transfers (`orphans`), the names of copies, the first start of a small file and the restart of a multipart upload.

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
    twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())] });
    twin.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default(), orphans, batches: Default::default() });
    twin
}

#[test]
fn xfer_discard_online_abort() {
    let mut twin = Twin::new();
    with_upload(&mut twin, KEY);
    let (reply, out) = twin.discard(KEY);
    assert_eq!((out.orphans_written(), out.removed(), out.writes()), (vec![("u1".to_string(), KEY.to_string())], vec![KEY.to_string()], 1), "the row of the transfer goes and the orphan is written in one batch");
    assert!(out.orphan_aborts().is_empty() && out.aborts() == 0 && out.replies(reply).is_empty(), "nothing leaves before Committed: {out:?}");
    let out = twin.commit();
    assert_eq!((out.orphan_aborts(), out.replies(reply).len()), (vec!["u1".to_string()], 1));
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
fn xfer_orphan_abort_sent() {
    let mut twin = orphan_restart(vec![("u9".into(), KEY.into())]);
    assert!(twin.online(false).orphan_aborts().is_empty());
    let out = twin.online(true);
    assert_eq!(out.orphan_aborts(), vec!["u9".to_string()]);
    let again = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert!(again.orphan_aborts().is_empty(), "an abort in flight is not sent twice");
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
                Restored => twin.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default(), orphans: vec![("u9".into(), KEY.into())], batches: Default::default() }),
                RestoreReady => twin.post(TransferInput::RestoreReady { epoch: EPOCH }),
                Register => twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())] }),
            };
            declared |= event == Register;
            assert!(declared || out.orphan_aborts().is_empty(), "{order:?} at {event:?}: an abort before the declaration would be refused for good, got {:?}", out.orphan_aborts());
            aborts.extend(out.orphan_aborts());
        }
        assert_eq!(aborts, ["u9".to_string()], "{order:?}: the orphan abort goes out once");
    }
}

#[test]
fn xfer_orphan_abort_done() {
    let mut twin = orphan_restart(vec![("u9".into(), KEY.into())]);
    twin.online(true);
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), answered: true });
    assert_eq!(out.orphans_removed(), vec!["u9".to_string()]);
    twin.commit();
    twin.online(false);
    assert!(twin.online(true).orphan_aborts().is_empty(), "an answered abort is not sent again");
}

#[test]
fn xfer_orphan_abort_retry_later() {
    let mut twin = orphan_restart(vec![("u9".into(), KEY.into())]);
    twin.online(true);
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), answered: false });
    assert!(Twin::quiet(&out), "a temporary failure writes nothing and sets no timer: {out:?}");
    twin.online(false);
    assert_eq!(twin.online(true).orphan_aborts(), vec!["u9".to_string()], "the next network sends it again");
}

#[test]
fn xfer_orphan_abort_owed() {
    let mut twin = orphan_restart(vec![("u9".into(), KEY.into())]);
    assert_eq!(twin.online(true).orphan_aborts(), vec!["u9".to_string()]);
    twin.online(false);
    assert!(twin.online(true).orphan_aborts().is_empty(), "the abort in flight is not sent twice");
    assert!(twin.post(TransferInput::RestoreReady { epoch: EPOCH }).orphan_aborts().is_empty());
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), answered: true });
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
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), answered: false });
    assert_eq!(out.orphan_aborts(), vec!["u9".to_string()], "the Online(true) before the failed answer owes an abort");
    twin.online(false);
    assert!(twin.online(true).orphan_aborts().is_empty(), "the resent abort is still in flight");
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), answered: false });
    assert_eq!(out.orphan_aborts(), vec!["u9".to_string()], "the new Online(true) is owed in its turn");
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), answered: false });
    assert!(out.orphan_aborts().is_empty(), "the debt is paid once");
}

#[test]
fn xfer_orphan_abort_owed_offline() {
    let mut twin = orphan_restart(vec![("u9".into(), KEY.into())]);
    twin.online(true);
    twin.online(false);
    twin.online(true);
    twin.online(false);
    let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), answered: false });
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
        let lost = twin.post(TransferInput::AbortDone { upload_id: "u1".into(), answered: false });
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

#[test]
fn xfer_ready_start_small() {
    let mut twin = Twin::offline();
    let id = twin.accepted(KEY, 1);
    assert_eq!(twin.stage(id), TransferStage::Ready);
    let out = twin.online(true);
    assert_eq!((twin.stage(id), out.steps().len()), (TransferStage::PersistingAttempt, 0));
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.steps().iter().map(|(part, _)| *part).collect::<Vec<_>>(), out.shows()), (TransferStage::Uploading, vec![Some(1)], vec![crate::owners::write::transfer::Shown::Uploading { progress: 0 }]));
}

#[test]
fn xfer_step_forbidden_part() {
    let mut twin = Twin::new();
    let id = with_upload(&mut twin, KEY);
    assert_eq!(twin.t(id).confirmed_parts, 1);
    let out = twin.step_error(id, true);
    assert!(out.steps().is_empty() && out.timers == 1, "the refused part waits for its timer: {out:?}");
    let out = twin.tick_retry(id);
    assert!(out.steps().is_empty());
    let out = twin.commit();
    assert_eq!(out.steps().iter().map(|(part, _)| *part).collect::<Vec<_>>(), vec![Some(2)], "only part 2 asks for a new URL");
    assert_eq!((twin.t(id).upload_id.as_deref(), twin.t(id).attempts), (Some("u1"), 2));
}

#[test]
fn xfer_restart_stale_presign() {
    let mut row = stored(1, KEY, 4);
    row.upload_id = Some("u1".into());
    row.confirmed_parts = 2;
    row.etags = vec![Some("e1".into()), Some("e2".into())];
    let (mut twin, _) = Twin::opened(vec![row], Default::default());
    twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    let out = twin.online(true);
    assert!(out.steps().is_empty());
    let out = twin.commit();
    assert_eq!(out.steps().iter().map(|(part, _)| *part).collect::<Vec<_>>(), vec![Some(3)], "the first step after a restart asks only for the unfinished part");
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
    twin.ignores(id, vec![TransferInput::StepError { transfer_id: id, attempt_id, temporary: true }]);
    assert_eq!(twin.commit().replies(reply).len(), 1);
    assert_eq!(twin.online(true).orphan_aborts(), vec!["u1".to_string()]);
}

#[test]
fn xfer_upload_minted() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.online(false);
    assert_eq!(twin.stage(id), TransferStage::Ready);
    let out = minted(&mut twin, id, "u1");
    assert_eq!((out.writes(), twin.t(id).upload_id.as_deref()), (1, Some("u1")), "the id of the cancelled attempt is written");
    twin.commit();
    let (_, out) = twin.discard(KEY);
    assert_eq!((out.orphans_written(), out.removed()), (vec![("u1".to_string(), KEY.to_string())], vec![KEY.to_string()]), "the removal carries the orphan");
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
    let gone = twin.post(TransferInput::AbortDone { upload_id: "u1".into(), answered: true });
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
    twin.mutation_outcome(key, OutboxOutcome::Confirmed { revision: 3, result: Some(r#"{"avatarUrl":"u"}"#.into()) });
    twin.commit();
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Confirmed { revision: 3, result: Some(r#"{"avatarUrl":"u"}"#.into()) })], "the outcome carries the returns of the final mutation");
    let failed_key = "Profile:main:cover";
    let failed = twin.mutating(failed_key);
    let error = r#"{"kind":"Rejected","message":"no","extensions":{"code":"BAD"}}"#;
    twin.mutation_outcome(failed_key, OutboxOutcome::Rejected { error: error.into() });
    twin.commit();
    let (reply, out) = twin.outcome(failed, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Failed { error: error.into() })], "the error and its extensions reach the outcome");
}
