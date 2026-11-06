use super::*;

#[test]
fn store_boot_new() {
    let mut twin = Twin::new();
    let opened = twin.boot(Boot::New, (0, 0, None));
    assert!(matches!(opened, Some(StoreEffect::OpenLifecycle(_))));
}

#[test]
fn store_boot_same() {
    let mut twin = Twin::new();
    let Some(StoreEffect::OpenLifecycle(message)) = twin.boot(Boot::Same, (4, 2, Some(7))) else { panic!("the boot opens Lifecycle") };
    assert_eq!((message.saved_epoch, message.saved_generation, message.saved_identity), (4, 2, Some(7)));
}

/// store_boot_same: the working generation of every model and the revision come from the database, not only the fields for Lifecycle.
#[test]
fn store_boot_same_restores_generation_and_revision() {
    let mut twin = Twin::new();
    twin.reference.saved_revision = 9;
    let Some(StoreEffect::OpenLifecycle(message)) = twin.boot(Boot::Same, (4, 7, Some(7))) else { panic!("the boot opens Lifecycle") };
    assert_eq!(message.saved_generation, 7);
    assert_eq!((twin.state().generation("Moment"), twin.state().revision()), (7, 9), "a batch with basis 6 is stale and the next revision follows 9");
}

#[test]
fn store_boot_changed_models() {
    let mut twin = Twin::new();
    let Some(StoreEffect::OpenLifecycle(message)) = twin.boot(Boot::ChangedModels, (4, 2, Some(7))) else { panic!("the boot opens Lifecycle") };
    assert_eq!(message.saved_generation, 3, "the cache is erased in the Boot transaction: the generation grows");
    assert_eq!(twin.state().generation("Moment"), 3);
    assert_eq!(twin.state().revision(), 1);
}

#[test]
fn store_boot_format_change() {
    let mut twin = Twin::new();
    let Some(StoreEffect::OpenLifecycle(message)) = twin.boot(Boot::FormatChange, (4, 2, None)) else { panic!("the boot opens Lifecycle") };
    assert_eq!(message.saved_generation, 3);
    assert_eq!(twin.state().revision(), 1);
}

#[test]
fn store_boot_corrupt_manifest() {
    let mut twin = Twin::new();
    assert!(matches!(twin.boot(Boot::CorruptManifest, (0, 0, None)), Some(StoreEffect::OpenCorrupt(_))));
    assert_eq!(twin.state().mode(), StoreMode::Closed);
    assert_eq!(twin.state().revision(), 0, "the database is not changed");
}

#[test]
fn store_boot_local_model_changed() {
    let mut twin = Twin::new();
    let Some(StoreEffect::OpenLifecycle(message)) = twin.boot(Boot::LocalModelChanged, (4, 2, Some(7))) else { panic!("the boot opens Lifecycle") };
    assert_eq!((message.saved_generation, twin.state().revision()), (2, 0), "the server cache and the generation stay");
}

#[test]
fn store_boot_error() {
    let mut twin = Twin::new();
    assert!(matches!(twin.boot(Boot::SqliteError, (0, 0, None)), Some(StoreEffect::OpenFailed(_))));
    assert_eq!(twin.state().mode(), StoreMode::Closed);
}

#[test]
fn store_commit_while_writing() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 0);
    let first = twin.feed(Request::Commit { id: 1, batch: batch.clone() });
    assert_eq!(first, vec![Action::Write(1)]);
    let second = twin.feed(Request::Commit { id: 2, batch });
    assert!(second.is_empty(), "the second batch waits for the first");
    let done = twin.feed(Request::Written { id: 1, result: Ok(1) });
    assert!(done.contains(&Action::Write(2)), "the deferred batch starts after Written");
    twin.feed(Request::Written { id: 2, result: Ok(2) });
}

#[test]
fn store_commit_stale_epoch() { assert_eq!(Twin::live().commit(7, &["a"]), vec![Action::Rejected { id: 1, reason: Reason::NoSession }]); }

#[test]
fn store_commit_no_basis() {
    let mut twin = Twin::live();
    let batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    let actions = twin.commit_batch(batch, true);
    assert_eq!(Twin::rejected(&actions), Some(Reason::Invalid));
    assert_eq!(twin.state().revision(), 0);
    let effects = twin.send(StoreInput::Commit(StoreBatch {
        author: BatchAuthor::Window, id: 2, epoch: 1, basis: BTreeMap::new(), rows: Vec::new(), intents: 0, credential: None,
        ops: vec![crate::owners::store::StoreOp::Rows { model: "Moment".into(), rows: vec![crate::owners::store::RowValue { row_id: "b".into(), values: "{}".into() }] }],
    }));
    assert!(effects.iter().any(|effect| matches!(effect, StoreEffect::Reply { reply: StoreReply::Rejected(RejectReason::Invalid), .. })));
    assert!(!effects.iter().any(|effect| matches!(effect, StoreEffect::Write { .. })));
}

#[test]
fn store_commit_arms_expiry() {
    let mut twin = Twin::live();
    twin.send(StoreInput::Commit(StoreBatch { author: BatchAuthor::Feed, id: 2, epoch: 1, basis: BTreeMap::new(), rows: Vec::new(), intents: 0, credential: None, ops: Vec::new() }));
    let result = crate::owners::store::WriteDone { revision: 7, expiring: vec![crate::owners::store::MemoryExpiry { model: "Moment".into(), row_id: "a".into(), written_at: 100, expires_after: 50 }], ..Default::default() };
    let effects = twin.send(StoreInput::Written { author: BatchAuthor::Feed, batch: 2, result: Ok(result) });
    let reply = effects.iter().position(|effect| matches!(effect, StoreEffect::Reply { .. })).unwrap();
    let timer = effects.iter().position(|effect| matches!(effect, StoreEffect::ArmExpiry { model, row_id, revision: 7, deadline: 150, .. } if model == "Moment" && row_id == "a")).unwrap();
    assert!(reply < timer);
}

#[test]
fn store_commit_disarms_expiry() {
    let mut twin = Twin::live();
    twin.send(StoreInput::Commit(StoreBatch { author: BatchAuthor::Outbox, id: 2, epoch: 1, basis: BTreeMap::new(), rows: Vec::new(), intents: 0, credential: None, ops: Vec::new() }));
    let result = crate::owners::store::WriteDone { revision: 7, removed_memory: vec![("Moment".into(), "a".into())], ..Default::default() };
    let effects = twin.send(StoreInput::Written { author: BatchAuthor::Outbox, batch: 2, result: Ok(result) });
    assert!(effects.iter().any(|effect| matches!(effect, StoreEffect::DisarmExpiry { model, row_id, .. } if model == "Moment" && row_id == "a")));
}

#[test]
fn store_commit_stale_generation() {
    let mut twin = Twin::live();
    twin.clear_models(3, &["Moment"], true);
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 2);
    let actions = twin.commit_batch(batch, true);
    assert_eq!(Twin::rejected(&actions), Some(Reason::StaleGeneration(vec!["Moment".into()])));
    assert_eq!(twin.stale_writes, 0, "a stale batch never reaches SQLite");
}

#[test]
fn store_commit_write() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 0);
    twin.batch += 1;
    assert_eq!(twin.feed(Request::Commit { id: 1, batch }), vec![Action::Write(1)]);
}

#[test]
fn store_commit_ok() {
    let mut twin = Twin::live();
    let actions = twin.commit(1, &["a"]);
    assert_eq!(Twin::committed(&actions), Some(1));
    let rows = Some([("Moment".to_string(), "a".to_string())].into());
    assert!(actions.contains(&Action::Committed { id: 1, revision: 1, rows: rows.clone() }));
    assert!(actions.contains(&Action::BroadcastCommitted { revision: 1, all: false, rows }));
    assert_eq!(twin.commit(1, &["b"]).len(), 3);
    assert_eq!(twin.state().revision(), 2);
}

#[test]
fn store_commit_sqlite_error() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 0);
    let actions = twin.commit_batch(batch, false);
    assert_eq!(Twin::rejected(&actions), Some(Reason::Storage));
    assert_eq!(twin.state().revision(), 0);
}

#[test]
fn store_written_stray() {
    let mut twin = Twin::live();
    assert!(twin.feed(Request::Written { id: 77, result: Ok(5) }).is_empty());
    assert_eq!(twin.state().revision(), 0);
}

#[test]
fn store_clear_while_writing() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 0);
    twin.feed(Request::Commit { id: 1, batch });
    assert!(twin.feed(Request::ClearAccount { epoch_old: 1, cause: RefCause::IdentityChanged, generation_new: 1, erase_id: 1 }).is_empty());
    assert!(twin.feed(Request::ClearModels { epoch: 1, generation_new: 1, models: names(&["Moment"]), erase_id: 2 }).is_empty());
    let done = twin.feed(Request::Written { id: 1, result: Ok(1) });
    assert!(done.iter().any(|action| matches!(action, Action::EraseAccount { .. })), "the account erase starts after Written");
}

#[test]
fn store_clear_account_stale() {
    let mut twin = Twin::live();
    twin.clear_account(RefCause::IdentityChanged, 4, true);
    let actions = twin.clear_account(RefCause::IdentityChanged, 4, true);
    assert_eq!(actions, vec![Action::ClearFailed { erase_id: 2, closed: false }]);
}

#[test]
fn store_clear_account_erase() {
    let mut twin = Twin::live();
    let actions = twin.feed(Request::ClearAccount { epoch_old: 1, cause: RefCause::SessionReset, generation_new: 1, erase_id: 1 });
    assert_eq!(actions, vec![Action::EraseAccount { io_id: 2, generation_new: 1, erase_encrypted: true }]);
}

#[test]
fn store_clear_account() {
    let mut twin = Twin::live();
    let actions = twin.clear_account(RefCause::IdentityChanged, 4, true);
    assert!(actions.contains(&Action::Cleared { epoch_old: 1, erase_id: 1 }));
    assert!(actions.contains(&Action::BroadcastCommitted { revision: 1, all: true, rows: None }));
    assert_eq!(twin.state().generation("Moment"), 4);
}

#[test]
fn store_clear_account_then_secret() {
    let mut twin = Twin::live();
    let actions = twin.clear_account(RefCause::SessionReset, 4, true);
    let secret = actions.iter().position(|action| matches!(action, Action::ClearSecret { .. })).expect("the secret is cleared after SQLite");
    let cleared = actions.iter().position(|action| matches!(action, Action::Cleared { .. })).expect("Cleared follows");
    assert!(secret < cleared, "Cleared waits for the secret");
}

#[test]
fn store_clear_secret_ok() { Twin::live().clear_account(RefCause::ServerEnded, 2, true); }

#[test]
fn store_clear_secret_error() {
    let mut twin = Twin::live();
    let first = twin.feed(Request::ClearAccount { epoch_old: 1, cause: RefCause::SessionReset, generation_new: 1, erase_id: 1 });
    let Some(Action::EraseAccount { io_id, .. }) = first.first().cloned() else { panic!("erase starts") };
    twin.feed(Request::Erased { io_id, result: Ok(1) });
    let done = twin.feed(Request::Erased { io_id, result: Err(()) });
    assert!(done.iter().any(|action| matches!(action, Action::Cleared { .. })), "the orphan secret does not hold Cleared back");
}

#[test]
fn store_clear_account_error() {
    let mut twin = Twin::live();
    let actions = twin.clear_account(RefCause::SessionReset, 2, false);
    assert!(actions.contains(&Action::ClearFailed { erase_id: 1, closed: false }));
    assert_eq!(twin.state().revision(), 0);
}

#[test]
fn store_clear_models_stale() {
    let mut twin = Twin::live();
    twin.clear_models(2, &["Moment"], true);
    assert_eq!(twin.clear_models(2, &["Moment"], true), vec![Action::ClearFailed { erase_id: 2, closed: false }]);
}

#[test]
fn store_clear_models_erase() {
    let mut twin = Twin::live();
    let actions = twin.feed(Request::ClearModels { epoch: 1, generation_new: 1, models: names(&["Moment"]), erase_id: 1 });
    assert!(matches!(actions.as_slice(), [Action::EraseModels { .. }]));
}

#[test]
fn store_clear_models() {
    let mut twin = Twin::live();
    twin.commit(1, &["a"]);
    let actions = twin.clear_models(2, &["Moment"], true);
    assert!(actions.contains(&Action::ModelsCleared { generation_new: 2, erase_id: 1 }));
    assert_eq!(twin.state().generation("Moment"), 2);
    assert_eq!(twin.state().generation("Other"), 0, "other models keep their generation");
}

#[test]
fn store_clear_models_error() {
    let mut twin = Twin::live();
    let actions = twin.clear_models(2, &["Moment"], false);
    assert!(actions.contains(&Action::ClearFailed { erase_id: 1, closed: false }));
    assert_eq!(twin.state().generation("Moment"), 0);
}

#[test]
fn store_erased_stray() {
    let mut twin = Twin::live();
    assert!(twin.feed(Request::Erased { io_id: 99, result: Ok(5) }).is_empty());
}

#[test]
fn store_publish_queued() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 0);
    twin.feed(Request::Commit { id: 1, batch });
    let queued = twin.feed(Request::Published { publication_id: 9, epoch: 2, generation: 0, active: true });
    assert!(queued.is_empty(), "the save waits for the write");
    let done = twin.feed(Request::Written { id: 1, result: Ok(1) });
    assert!(done.iter().any(|action| matches!(action, Action::SavePublished { publication_id: 9, .. })), "the save starts after Written");
}

#[test]
fn store_epoch() {
    let mut twin = Twin::new();
    twin.publish(5, 3);
    assert_eq!(twin.state().epoch(), 5);
    assert!(twin.history.iter().any(|effect| matches!(effect, StoreEffect::Saved { .. })));
}

#[test]
fn store_epoch_restricting_kept() {
    let mut twin = Twin::live();
    twin.feed(Request::EpochEnded { epoch: 1 });
    twin.feed(Request::Published { publication_id: 8, epoch: 1, generation: 0, active: false });
    let saved = twin.feed(Request::Erased { io_id: 2, result: Ok(0) });
    assert_eq!(saved, vec![Action::Saved(8)]);
    assert_eq!(twin.state().epoch(), 2, "a restricting publication does not bring the ended epoch back");
    assert_eq!(Twin::rejected(&twin.commit(1, &["a"])), Some(Reason::NoSession));
}

#[test]
fn store_epoch_save_failed() {
    let mut twin = Twin::new();
    twin.feed(Request::Published { publication_id: 1, epoch: 5, generation: 3, active: true });
    let actions = twin.feed(Request::Erased { io_id: 1, result: Err(()) });
    assert_eq!(actions, vec![Action::SaveFailed { publication_id: 1, closed: false }]);
    assert_eq!(twin.state().epoch(), 0, "a failed save leaves the epoch of the Store");
}


#[test]
fn store_read_direct_pending() {
    for (identity, pending, direct) in [(None, true, true), (Some(7), true, false), (None, false, false)] {
        let mut twin = Twin::new();
        twin.reference.pending = pending;
        twin.boot(Boot::Same, (4, 2, identity));
        let effects = twin.feed(Request::ReadIntents { epoch: 5 });
        assert_eq!(effects, vec![Action::ReadIntents { epoch: 5, direct }], "{identity:?} {pending}");
    }
}
