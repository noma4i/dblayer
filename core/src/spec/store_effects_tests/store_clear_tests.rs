use super::*;

/// A clearing that arrives while a batch is in SQLite starts after `Written`.
#[test]
fn store_clear_while_writing_waits() {
    let mut twin = Twin::live();
    twin.feed(Request::Commit { id: 1, batch: Twin::moment_batch() });
    assert!(twin.feed(Request::ClearAccount { epoch_old: 1, cause: Cause::IdentityChanged, generation_new: 1, erase_id: 1 }).is_empty());
    let done = twin.feed(Request::Written { id: 1, result: Ok(1) });
    assert!(done.iter().any(|action| matches!(action, Action::EraseAccount { .. })), "the account erase starts after Written");
}

/// A generation that is not above the last one is refused.
#[test]
fn store_clear_account_with_an_old_generation_fails() {
    let mut twin = Twin::live();
    twin.clear_account(Cause::IdentityChanged, 4, true);
    let actions = twin.clear_account(Cause::IdentityChanged, 4, true);
    assert_eq!(actions, vec![Action::ClearFailed { erase_id: 2, closed: false }]);
}

#[test]
fn store_clear_account_raises_every_generation() {
    let mut twin = Twin::live();
    let actions = twin.clear_account(Cause::IdentityChanged, 4, true);
    assert!(actions.contains(&Action::Cleared { epoch_old: 1, erase_id: 1 }));
    assert!(actions.contains(&Action::BroadcastCommitted { revision: 1, all: true, tables: Default::default(), models: Default::default(), rows: None }));
    assert_eq!((twin.state().generation("Moment"), twin.state().generation("Other")), (4, 4), "after ClearAccount every model has generation_new");
}

/// `SessionReset` erases the encrypted tables, then clears the secret; `Cleared` waits for the secret.
#[test]
fn store_clear_account_clears_the_secret_before_cleared() {
    let mut twin = Twin::live();
    let first = twin.feed(Request::ClearAccount { epoch_old: 1, cause: Cause::SessionReset, generation_new: 4, erase_id: 1 });
    assert_eq!(first, vec![Action::EraseAccount { io_id: 2, generation_new: 4, erase_encrypted: true }]);
    let mut twin = Twin::live();
    let actions = twin.clear_account(Cause::SessionReset, 4, true);
    let secret = actions.iter().position(|action| matches!(action, Action::ClearSecret { .. })).expect("the secret is cleared after SQLite");
    let cleared = actions.iter().position(|action| matches!(action, Action::Cleared { .. })).expect("Cleared follows");
    assert!(secret < cleared, "Cleared waits for the secret");
}

#[test]
fn store_clear_secret_error_does_not_hold_cleared_back() {
    let mut twin = Twin::live();
    let first = twin.feed(Request::ClearAccount { epoch_old: 1, cause: Cause::SessionReset, generation_new: 1, erase_id: 1 });
    let Some(Action::EraseAccount { io_id, .. }) = first.first().cloned() else { panic!("erase starts") };
    twin.feed(Request::Erased { io_id, result: Ok(1) });
    let done = twin.feed(Request::Erased { io_id, result: Err(()) });
    assert!(done.iter().any(|action| matches!(action, Action::Cleared { .. })), "the orphan secret does not hold Cleared back");
}

#[test]
fn store_clear_account_sqlite_error_fails_the_clearing() {
    let mut twin = Twin::live();
    let actions = twin.clear_account(Cause::SessionReset, 2, false);
    assert!(actions.contains(&Action::ClearFailed { erase_id: 1, closed: false }));
    assert_eq!(twin.state().revision(), 0);
}

/// A save of a publication that arrives while a batch is in SQLite starts after `Written`.
#[test]
fn store_publish_while_writing_waits() {
    let mut twin = Twin::live();
    twin.feed(Request::Commit { id: 1, batch: Twin::moment_batch() });
    assert!(twin.feed(Request::Published { publication_id: 9, epoch: 2, generation: 0, active: true }).is_empty(), "the save waits for the write");
    let done = twin.feed(Request::Written { id: 1, result: Ok(1) });
    assert!(done.iter().any(|action| matches!(action, Action::SavePublished { publication_id: 9, .. })), "the save starts after Written");
}

/// Publications saved one after another are answered in order, a failed save leaves the epoch of the previous one.
#[test]
fn store_publications_are_saved_and_answered_in_order() {
    let mut twin = Twin::new();
    for (publication_id, epoch) in [(1, 3), (2, 4), (3, 5)] {
        twin.feed(Request::Published { publication_id, epoch, generation: 0, active: true });
    }
    assert_eq!(twin.state().epoch(), 0, "nothing is saved before the answer");
    twin.feed(Request::Erased { io_id: 1, result: Ok(0) });
    twin.feed(Request::Erased { io_id: 2, result: Err(()) });
    assert_eq!(twin.state().epoch(), 3, "the failed save leaves the previous epoch");
    twin.feed(Request::Erased { io_id: 3, result: Ok(0) });
    let answers: Vec<&StoreEffect> = twin.history.iter().filter(|effect| matches!(effect, StoreEffect::SavePublished { .. } | StoreEffect::Saved { .. } | StoreEffect::SaveFailed { .. })).collect();
    assert_eq!(answers.len(), 6, "three starts and three answers: {answers:?}");
    assert_eq!(twin.state().epoch(), 5);
}

/// A publication that restricts the session does not bring the ended epoch back; the commits of the ended epoch stay refused.
#[test]
fn store_restricting_publication_keeps_the_epoch_ended() {
    let mut twin = Twin::live();
    twin.feed(Request::EpochEnded { epoch: 1 });
    twin.feed(Request::Published { publication_id: 8, epoch: 1, generation: 0, active: false });
    assert_eq!(twin.feed(Request::Erased { io_id: 2, result: Ok(0) }), vec![Action::Saved(8)]);
    assert_eq!(twin.state().epoch(), 2);
    assert_eq!(Twin::rejected(&twin.commit(1, &["a"])), Some(Reason::NoSession));
}

/// `EpochEnded` waits for the write in flight; a late end of an older epoch changes nothing.
#[test]
fn store_epoch_end_waits_for_the_write() {
    let mut twin = Twin::live();
    twin.feed(Request::Commit { id: 1, batch: Twin::moment_batch() });
    twin.feed(Request::EpochEnded { epoch: 1 });
    assert_eq!(twin.state().epoch(), 1, "EpochEnded waits for Written");
    twin.feed(Request::Written { id: 1, result: Ok(1) });
    assert_eq!(twin.state().epoch(), 2);
    twin.feed(Request::EpochEnded { epoch: 0 });
    assert_eq!(twin.state().epoch(), 2);
    assert_eq!(Twin::rejected(&twin.commit(1, &["a"])), Some(Reason::NoSession), "the commits of the ended epoch are refused");
}
