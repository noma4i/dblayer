use super::*;
use crate::owners::store::{MemoryExpiry, RowValue, StoreOp};

fn bare_batch(author: BatchAuthor, id: u64, stamp: EpochStamp, ops: Vec<StoreOp>) -> StoreBatch {
    StoreBatch { author, id, stamp, basis: BTreeMap::new(), rows: Vec::new(), intents: 0, credential: None, ops }
}

fn login_batch(id: u64, landing: bool) -> StoreBatch {
    use crate::owners::session::{OperationClass, PendingCall};
    let digest = "000000000000002a000000000000000000000000000000000000000000000000".to_string();
    let ops = if landing {
        vec![StoreOp::Session { digest: Some(digest), sequence: Some(1) }, StoreOp::RemovePending]
    } else {
        let pending = PendingCall { request_id: 10, sequence: 1, class: OperationClass::Login, operation: "login".into(), path: "/login".into(), body: "{}".into(), document: None, credential: Some(42), purpose: 0, author: None, owner: None, epoch: 1, reply: Some(1) };
        vec![StoreOp::Pending { pending, credential_digest: digest.clone(), wire_id: "wire".into(), response_key: "key".into() }, StoreOp::Session { digest: Some(digest), sequence: Some(0) }]
    };
    StoreBatch { author: BatchAuthor::Session, id, stamp: EpochStamp::Epoch(1), basis: BTreeMap::new(), rows: Vec::new(), intents: 1, credential: Some(42), ops }
}

fn login_rows(db: &crate::store::Store) -> (i64, Option<(String, i64)>) {
    let inner = db.lock();
    let pending = inner.connection.query_row("SELECT count(*) FROM encrypted_pending", [], |row| row.get(0)).unwrap();
    let session = crate::store::session_state::session(&inner.connection).unwrap().map(|row| (row.credential_digest, row.sequence));
    (pending, session)
}

/// A login batch after the end of its epoch is refused and leaves the real SQLite without pending and session rows.
fn login_after_epoch_end(persist_first: bool) {
    let mut twin = Twin::live();
    let clock = std::sync::Arc::new(crate::runtime::ManualClock::default());
    let db = crate::store::Store::open_models(None, Vec::new(), clock).unwrap();
    if persist_first {
        let batch = login_batch(10, false);
        let effects = twin.send(StoreInput::Commit(batch.clone()));
        assert!(matches!(effects.as_slice(), [StoreEffect::Write { .. }]));
        let written = db.write_batch(&batch, db.files(), None).unwrap();
        let revision = written.revision;
        let effects = twin.send(StoreInput::Written { author: BatchAuthor::Session, batch: 10, result: Ok(WriteDone { revision, notice: written.notice, ..WriteDone::default() }) });
        assert!(effects.iter().any(|effect| matches!(effect, StoreEffect::Reply { batch: 10, reply: StoreReply::Committed { .. }, .. })));
        let (pending, session) = login_rows(&db);
        assert_eq!(pending, 1);
        assert_eq!(session, Some(("000000000000002a000000000000000000000000000000000000000000000000".into(), 0)));
    }
    twin.send(StoreInput::EpochEnded(EpochEnded { epoch: 1, cause: Cause::SessionReset }));
    let effects = twin.send(StoreInput::ClearAccount { epoch_old: 1, cause: Cause::SessionReset, generation_new: 1, erase_id: 3 });
    let io_id = effects.iter().find_map(|effect| match effect { StoreEffect::EraseAccount { io_id, .. } => Some(*io_id), _ => None }).unwrap();
    let revision = db.erase_account(crate::identity::Generation::from_value(1), true).unwrap();
    let effects = twin.send(StoreInput::Erased { io_id, result: Ok(Done { revision: revision as u64 }) });
    assert!(effects.iter().any(|effect| matches!(effect, StoreEffect::ClearSecret { .. })));
    twin.send(StoreInput::Erased { io_id, result: Ok(Done { revision: 0 }) });
    assert_eq!(login_rows(&db), (0, None));
    let effects = twin.send(StoreInput::Commit(login_batch(11, persist_first)));
    assert!(matches!(effects.as_slice(), [StoreEffect::Reply { author: BatchAuthor::Session, batch: 11, reply: StoreReply::Rejected(RejectReason::NoSession), .. }]));
    assert_eq!(login_rows(&db), (0, None));
}

#[test]
fn store_login_pending_after_epoch_end() { login_after_epoch_end(false); }

#[test]
fn store_login_landing_after_epoch_end() { login_after_epoch_end(true); }

#[test]
fn store_commit_ok() {
    let mut twin = Twin::live();
    let actions = twin.commit(1, &["a"]);
    assert_eq!(Twin::committed(&actions), Some(1));
    let rows = Some([("row_Moment".to_string(), "a".to_string())].into());
    assert!(actions.contains(&Action::Committed { id: 1, revision: 1, rows: rows.clone() }));
    assert!(actions.contains(&Action::BroadcastCommitted { revision: 1, all: false, tables: ["row_Moment".to_string()].into(), models: ["Moment".to_string()].into(), rows }));
    assert_eq!(twin.commit(1, &["b"]).len(), 3);
    assert_eq!(twin.state().revision(), 2);
}

/// Two rows of one batch are one transaction: one revision.
#[test]
fn store_commit_of_two_rows_is_one_revision() {
    let mut twin = Twin::live();
    twin.commit(1, &["a", "b"]);
    assert_eq!(twin.state().revision(), 1);
}

/// A batch that arrives while another is in SQLite waits and starts after `Written`.
#[test]
fn store_commit_while_writing_waits() {
    let mut twin = Twin::live();
    let batch = Twin::moment_batch();
    assert_eq!(twin.feed(Request::Commit { id: 1, batch: batch.clone() }), vec![Action::Write(1)]);
    assert!(twin.feed(Request::Commit { id: 2, batch }).is_empty(), "the second batch waits for the first");
    let done = twin.feed(Request::Written { id: 1, result: Ok(1) });
    assert!(done.contains(&Action::Write(2)), "the deferred batch starts after Written");
}

#[test]
fn store_commit_of_another_epoch_is_refused() {
    assert_eq!(Twin::live().commit(7, &["a"]), vec![Action::Rejected { id: 1, reason: Reason::NoSession }]);
}

/// The batch of a stale generation never reaches SQLite, the fresh one is committed.
#[test]
fn store_commit_with_a_stale_generation_is_refused() {
    let mut twin = Twin::live();
    twin.clear_account(Cause::IdentityChanged, 3, true);
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 2);
    let actions = twin.commit_batch(batch, true);
    assert_eq!(Twin::rejected(&actions), Some(Reason::StaleGeneration(vec!["Moment".into()])));
    assert_eq!(twin.stale_writes, 0, "a stale batch never reaches SQLite");
    let mut fresh = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    fresh.basis.insert("Moment".into(), 3);
    assert!(twin.commit_batch(fresh, true).iter().any(|action| matches!(action, Action::Committed { .. })));
}

/// A batch with rows of a model needs a basis for it.
#[test]
fn store_commit_without_a_basis_is_invalid() {
    let mut twin = Twin::live();
    let actions = twin.commit_batch(Batch::rows(1, vec![("Moment".into(), "a".into())]), true);
    assert_eq!(Twin::rejected(&actions), Some(Reason::Invalid));
    assert_eq!(twin.state().revision(), 0);
    let batch = bare_batch(BatchAuthor::Window, 2, EpochStamp::Epoch(1), vec![StoreOp::Rows { model: "Moment".into(), rows: vec![RowValue { row_id: "b".into(), values: "{}".into() }] }]);
    let effects = twin.send(StoreInput::Commit(batch));
    assert!(effects.iter().any(|effect| matches!(effect, StoreEffect::Reply { reply: StoreReply::Rejected(RejectReason::Invalid(text)), .. } if text == "the batch has no basis for a model of its rows")), "the refusal names its cause: {effects:?}");
    assert!(!effects.iter().any(|effect| matches!(effect, StoreEffect::Write { .. })));
}

#[test]
fn store_commit_sqlite_error_is_a_storage_refusal() {
    let mut twin = Twin::live();
    let actions = twin.commit_batch(Twin::moment_batch(), false);
    assert_eq!(Twin::rejected(&actions), Some(Reason::Storage));
    assert_eq!(twin.state().revision(), 0);
}

/// A Control batch of owner data is written whatever the epoch and answered once with the Control stamp; a Control batch with an op of an epoch is refused.
#[test]
fn store_control_batch_ignores_the_epoch() {
    for stale in [false, true] {
        let mut twin = Twin::live();
        if stale { twin.send(StoreInput::EpochEnded(EpochEnded { epoch: 1, cause: Cause::ServerEnded })); }
        let orphan = bare_batch(BatchAuthor::Transfer, 9, EpochStamp::Control, vec![StoreOp::UploadOrphan { upload_id: "u1".into(), key: "Moment:r1:upload".into() }]);
        let effects = twin.send(StoreInput::Commit(orphan));
        assert!(effects.iter().any(|effect| matches!(effect, StoreEffect::Write { batch, .. } if batch.id == 9 && batch.stamp == EpochStamp::Control)), "stale={stale}: the Control batch is written without an epoch check: {effects:?}");
        assert!(!effects.iter().any(|effect| matches!(effect, StoreEffect::Reply { reply: StoreReply::Rejected(_), .. })), "stale={stale}: no refusal: {effects:?}");
        let effects = twin.send(StoreInput::Written { author: BatchAuthor::Transfer, batch: 9, result: Ok(WriteDone { revision: 1, ..WriteDone::default() }) });
        let committed = effects.iter().filter(|effect| matches!(effect, StoreEffect::Reply { author: BatchAuthor::Transfer, batch: 9, stamp: EpochStamp::Control, reply: StoreReply::Committed { .. } })).count();
        assert_eq!(committed, 1, "stale={stale}: exactly one Committed with the Control stamp: {effects:?}");
        assert_eq!(twin.state().revision(), 1, "stale={stale}: the revision follows the write");
    }
    let mut twin = Twin::live();
    let wrong = bare_batch(BatchAuthor::Transfer, 9, EpochStamp::Control, vec![StoreOp::RemoveUpload { key: "Moment:r1:upload".into() }]);
    let effects = twin.send(StoreInput::Commit(wrong));
    assert!(effects.iter().any(|effect| matches!(effect, StoreEffect::Reply { stamp: EpochStamp::Control, reply: StoreReply::Rejected(RejectReason::Invalid(text)), .. } if text == "the control batch carries rows or an operation of an epoch")), "an op of the epoch under a Control stamp is refused with its cause: {effects:?}");
    assert!(!effects.iter().any(|effect| matches!(effect, StoreEffect::Write { .. })));
}

/// Batch ids are unique within the author, the answer goes to the pair `(author, id)`.
#[test]
fn store_commit_answers_the_pair_of_author_and_id() {
    let mut twin = Twin::live();
    let batch = |author| bare_batch(author, 1, EpochStamp::Epoch(1), Vec::new());
    twin.send(StoreInput::Commit(batch(BatchAuthor::Session)));
    twin.send(StoreInput::Commit(batch(BatchAuthor::Outbox)));
    let written = twin.send(StoreInput::Written { author: BatchAuthor::Session, batch: 1, result: Ok(WriteDone { revision: 1, ..WriteDone::default() }) });
    assert!(written.iter().any(|effect| matches!(effect, StoreEffect::Reply { author: BatchAuthor::Session, batch: 1, stamp: EpochStamp::Epoch(1), reply: StoreReply::Committed { .. } })), "the Session batch is answered");
    assert!(!written.iter().any(|effect| matches!(effect, StoreEffect::Reply { author: BatchAuthor::Outbox, .. })), "the Outbox batch with the same id still waits");
    let written = twin.send(StoreInput::Written { author: BatchAuthor::Outbox, batch: 1, result: Ok(WriteDone { revision: 2, ..WriteDone::default() }) });
    assert!(written.iter().any(|effect| matches!(effect, StoreEffect::Reply { author: BatchAuthor::Outbox, batch: 1, stamp: EpochStamp::Epoch(1), reply: StoreReply::Committed { .. } })), "the Outbox batch is answered with the epoch of the batch");
}

#[test]
fn store_commit_arms_the_expiry_after_the_reply() {
    let mut twin = Twin::live();
    twin.send(StoreInput::Commit(bare_batch(BatchAuthor::Feed, 2, EpochStamp::Epoch(1), Vec::new())));
    let result = WriteDone { revision: 7, expiring: vec![MemoryExpiry { model: "Moment".into(), row_id: "a".into(), written_at: 100, expires_after: 50 }], ..Default::default() };
    let effects = twin.send(StoreInput::Written { author: BatchAuthor::Feed, batch: 2, result: Ok(result) });
    let reply = effects.iter().position(|effect| matches!(effect, StoreEffect::Reply { .. })).unwrap();
    let timer = effects.iter().position(|effect| matches!(effect, StoreEffect::ArmExpiry { model, row_id, revision: 7, deadline: 150, .. } if model == "Moment" && row_id == "a")).unwrap();
    assert!(reply < timer);
}

#[test]
fn store_commit_disarms_the_expiry_of_a_removed_row() {
    let mut twin = Twin::live();
    twin.send(StoreInput::Commit(bare_batch(BatchAuthor::Outbox, 2, EpochStamp::Epoch(1), Vec::new())));
    let result = WriteDone { revision: 7, removed_memory: vec![("Moment".into(), "a".into())], ..Default::default() };
    let effects = twin.send(StoreInput::Written { author: BatchAuthor::Outbox, batch: 2, result: Ok(result) });
    assert!(effects.iter().any(|effect| matches!(effect, StoreEffect::DisarmExpiry { model, row_id, .. } if model == "Moment" && row_id == "a")));
}

/// Answers of SQLite and of reads to ids the Store never issued change nothing.
#[test]
fn store_stray_answers_are_ignored() {
    let mut twin = Twin::live();
    for request in [Request::Written { id: 77, result: Ok(5) }, Request::Erased { io_id: 99, result: Ok(5) }, Request::ReadFinished(3)] {
        assert!(twin.feed(request.clone()).is_empty(), "{request:?}");
    }
    assert_eq!(twin.state().revision(), 0);
}
