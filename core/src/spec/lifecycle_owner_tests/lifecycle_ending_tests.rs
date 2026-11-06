use super::*;

#[test]
fn life_ending_reset_joins() { ending().feed(reset(2)); }

#[test]
fn life_ending_reset_erases_credential() {
    let mut twin = session(1);
    twin.feed(credential(2));
    twin.settle();
    twin.feed(reset(5));
    let first = cleared(&twin);
    let effects = twin.feed(first);
    assert!(effects.iter().any(|e| matches!(e, RefEffect::ClearAccount { cause: RefCause::SessionReset, .. })), "the credential is erased by the second clearing");
    twin.settle();
    let second = cleared(&twin);
    twin.feed(second);
    twin.settle();
    assert_eq!(twin.published().identity, None);
    assert!(twin.replies().contains(&(5, Ok(()))));
}

#[test]
fn life_ending_reset_after_failure() {
    let mut twin = ending();
    let failed = clear_failed(&twin);
    twin.feed(failed);
    twin.feed(reset(2));
}

#[test]
fn life_ending_server_ended() { ending().feed(Event::ServerEnded(2)); }

#[test]
fn life_ending_reset_models() { ending().feed(reset_models(&["Moment"], 2)); }

#[test]
fn life_ending_failed_reset_models() {
    let mut twin = ending();
    let failed = clear_failed(&twin);
    twin.feed(failed);
    twin.feed(reset_models(&["Moment"], 2));
    assert!(twin.replies().contains(&(2, Err(LifecycleError::Storage))));
}

#[test]
fn life_ending_cleared_then_credential() {
    let mut twin = session(1);
    twin.feed(credential(2));
    twin.feed(reset(5));
    let first = cleared(&twin);
    twin.feed(first);
}

#[test]
fn life_ending_cleared_signed_out() {
    let mut twin = ending();
    let done = cleared(&twin);
    twin.feed(done);
    assert!(twin.replies().is_empty(), "the reset is answered after Saved");
    twin.settle();
    assert_eq!(twin.replies(), vec![(1, Ok(()))]);
}

#[test]
fn life_ending_cleared_next_identity() {
    let mut twin = session(1);
    twin.feed(credential(2));
    twin.settle();
    let done = cleared(&twin);
    twin.feed(done);
    assert_eq!(*twin.state(), RefState::Session { epoch: 4, identity: 2 });
}

#[test]
fn life_ending_clear_failed() {
    let mut twin = ending();
    let failed = clear_failed(&twin);
    twin.feed(failed);
    assert_eq!(twin.published().state, PubState::EndingFailed);
    assert!(twin.replies().contains(&(1, Err(LifecycleError::Storage))));
}

#[test]
fn life_ending_models_cleared() {
    let mut twin = resetting();
    twin.feed(reset(2));
    let models = twin.erases().into_iter().find(|erase| erase.kind == RefEraseKind::Models).expect("the models erase is open").id;
    twin.feed(Event::ModelsCleared(1, models));
    assert!(twin.replies().contains(&(1, Ok(()))));
}

#[test]
fn life_ending_models_clear_failed() {
    let mut twin = resetting();
    twin.feed(reset(2));
    let models = twin.erases().into_iter().find(|erase| erase.kind == RefEraseKind::Models).expect("the models erase is open").id;
    twin.feed(Event::ClearFailed(models));
    assert!(twin.replies().contains(&(1, Err(LifecycleError::Storage))));
}

#[test]
fn life_ending_stray_erase() { ending().feed_all(stray_erase_events()); }

#[test]
fn life_ending_close() { ending().feed(close(2)); }

#[test]
fn life_ending_drained() { ending().feed(Event::Drained(RefOwner::Feed)); }

#[test]
fn life_resetting_credential() { resetting().feed(credential(2)); }

#[test]
fn life_resetting_credential_signed_out() { resetting_signed_out().feed(credential(2)); }

#[test]
fn life_resetting_same_credential() { resetting().feed(credential(1)); }

#[test]
fn life_resetting_session_reset() { resetting().feed(reset(2)); }

#[test]
fn life_resetting_server_ended() { resetting().feed(Event::ServerEnded(2)); }

#[test]
fn life_resetting_stale_server_ended() {
    resetting().feed(Event::ServerEnded(1));
    resetting_signed_out().feed(Event::ServerEnded(1));
}

#[test]
fn life_resetting_reset_models() { resetting().feed(reset_models(&["Other"], 2)); }

#[test]
fn life_resetting_cleared_queued() {
    let mut twin = resetting();
    twin.feed(reset_models(&["Other"], 2));
    let first = models_cleared(&twin, 1);
    let effects = twin.feed(first);
    assert!(effects.iter().any(|e| matches!(e, RefEffect::ClearModels { generation_new: 2, .. })), "the queued models are erased next");
    twin.settle();
    let second = models_cleared(&twin, 2);
    twin.feed(second);
    twin.settle();
    assert_eq!(twin.replies(), vec![(1, Ok(())), (2, Ok(()))]);
}

#[test]
fn life_resetting_cleared() {
    let mut twin = resetting();
    let done = models_cleared(&twin, 1);
    twin.feed(done);
    twin.settle();
    assert_eq!(twin.published().generation, 1);
    assert_eq!(*twin.state(), RefState::Session { epoch: 2, identity: 1 });
}

#[test]
fn life_resetting_cleared_then_start() {
    let mut twin = resetting_signed_out();
    twin.feed(credential(2));
    let done = models_cleared(&twin, 1);
    twin.feed(done);
    assert_eq!(*twin.state(), RefState::Session { epoch: 2, identity: 2 });
}

#[test]
fn life_resetting_clear_failed() {
    let mut twin = resetting();
    let failed = clear_failed(&twin);
    twin.feed(failed);
    assert_eq!(twin.published().generation, 0, "the generation stays");
    assert!(twin.replies().contains(&(1, Err(LifecycleError::Storage))));
}

#[test]
fn life_resetting_clear_failed_then_start() {
    let mut twin = resetting_signed_out();
    twin.feed(credential(2));
    let failed = clear_failed(&twin);
    twin.feed(failed);
    assert_eq!(twin.published().identity, Some(2), "the committed credential is not lost");
}

#[test]
fn life_resetting_stray_erase() { resetting().feed_all(stray_erase_events()); }

#[test]
fn life_resetting_close() {
    let mut twin = resetting();
    twin.feed(reset_models(&["Other"], 2));
    twin.feed(close(3));
    assert!(twin.replies().contains(&(2, Err(LifecycleError::Interrupted))));
}

/// life_resetting_credential_open_saved: the epoch of an `open` that passed `Saved` ends with a new identity, `open` answers `Ok`.
#[test]
fn life_resetting_credential_open_saved() {
    let mut twin = Twin::open(0, 0, Some(1), Some(7));
    twin.feed(Event::Saved(1));
    twin.feed(reset_models(&["Moment"], 8));
    twin.feed(credential(2));
    assert!(twin.replies().contains(&(7, Ok(()))), "{:?}", twin.replies());
}

/// life_session_credential_open_unsaved: the first publication of `open` is not saved when the identity changes, `open` is interrupted.
#[test]
fn life_session_credential_open_unsaved() {
    let mut twin = Twin::open(0, 0, Some(1), Some(7));
    twin.feed(credential(2));
    assert!(twin.replies().contains(&(7, Err(LifecycleError::Interrupted))), "{:?}", twin.replies());
}

#[test]
fn life_resetting_drained() { resetting().feed(Event::Drained(RefOwner::Feed)); }

#[test]
fn life_closing_last_drained() {
    let mut twin = closing_session();
    let owners = OWNERS;
    for owner in owners {
        twin.feed(Event::Drained(owner));
    }
    assert!(matches!(twin.state(), RefState::Closing { store_closing: true, .. }));
    assert!(twin.feed(Event::Drained(RefOwner::Feed)).is_empty(), "a repeated Drained closes the store a second time");
}

#[test]
fn life_closing_drained() { closing_session().feed(Event::Drained(RefOwner::Feed)); }

#[test]
fn life_closing_clear_done_last() {
    let mut twin = ending();
    twin.feed(close(2));
    twin.feed_all(drain_all());
    let done = cleared(&twin);
    let effects = twin.feed(done);
    assert!(effects.contains(&RefEffect::StoreClose), "the store closes after the last erase");
}

#[test]
fn life_closing_clear_done() {
    let mut twin = ending();
    twin.feed(close(2));
    let done = cleared(&twin);
    twin.feed(done);
    assert!(twin.replies().contains(&(1, Ok(()))), "the wait of the erase is answered by its own outcome");
}

#[test]
fn life_closing_stray_erase() { closing_session().feed_all(stray_erase_events()); }

#[test]
fn life_closing_store_closed() {
    let mut twin = closing_session();
    twin.feed_all(drain_all());
    twin.feed(Event::ObserversStopped);
    twin.feed(Event::StoreClosed);
    assert_eq!(*twin.state(), RefState::Closed);
    assert!(twin.replies().contains(&(1, Ok(()))));
}

#[test]
fn life_closing_store_closed_waits_observers() {
    let mut twin = closing_session();
    twin.feed_all(drain_all());
    twin.feed(Event::StoreClosed);
    assert!(matches!(twin.state(), RefState::Closing { store_closed: true, .. }));
}

#[test]
fn life_closing_observers_stopped_last() {
    let mut twin = closing_session();
    twin.feed_all(drain_all());
    twin.feed(Event::StoreClosed);
    twin.feed(Event::ObserversStopped);
    assert_eq!(*twin.state(), RefState::Closed);
}

#[test]
fn life_closing_observers_stopped() {
    let mut twin = closing_session();
    twin.feed(Event::ObserversStopped);
    assert!(matches!(twin.state(), RefState::Closing { observers_stopped: true, .. }));
}

#[test]
fn life_closing_close_joins() { closing_session().feed(close(2)); }

#[test]
fn life_closing_saved() {
    let mut twin = session(1);
    twin.feed(reset(2));
    twin.feed(close(3));
    let erase = cleared(&twin);
    twin.feed(erase);
    twin.settle();
    assert!(twin.replies().contains(&(2, Ok(()))));
}

#[test]
fn life_closing_refuses() {
    let mut twin = closing_session();
    twin.feed(reset(5));
    twin.feed(reset_models(&["Moment"], 6));
    twin.feed_all([credential(2), Event::ServerEnded(2)]);
    assert!(twin.replies().contains(&(5, Err(LifecycleError::Closed))));
    assert!(twin.replies().contains(&(6, Err(LifecycleError::Closed))));
}

#[test]
fn life_closing_saved_stray() { closing_session().feed_all([Event::Saved(999), Event::SaveFailed(999)]); }

#[test]
fn life_saved() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    let effects = twin.feed(Event::Saved(1));
    assert!(effects.contains(&RefEffect::EpochStarted(1, None, 0)));
    assert_eq!(twin.replies(), vec![(7, Ok(()))]);
    assert_eq!(*twin.cell(), *twin.published());
}

#[test]
fn life_saved_stale() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    twin.feed(credential(1));
    twin.save_all();
    assert!(twin.replies().contains(&(7, Err(LifecycleError::Interrupted))), "the publication of an ended epoch answers Interrupted once");
    assert!(!twin.history.iter().any(|e| matches!(e, LifecycleEffect::EpochStarted { epoch: 1, .. })), "EpochStarted of an ended epoch does not go out");
}

#[test]
fn life_saved_stray() { session(1).feed_all([Event::Saved(999), Event::SaveFailed(999)]); }

#[test]
fn life_save_failed() {
    for saved_identity in [None, Some(9)] {
        let mut twin = Twin::open(0, 0, saved_identity, Some(7));
        twin.feed(Event::SaveFailed(1));
        assert_eq!(twin.cell().state, PubState::StorageFailed, "{saved_identity:?}");
        assert_eq!(twin.replies(), vec![(7, Err(LifecycleError::Storage))], "{saved_identity:?}: open answers Err(Storage) once");
        assert!(!twin.history.iter().any(|e| matches!(e, LifecycleEffect::EpochStarted { .. } | LifecycleEffect::ReadIntents { .. })), "{saved_identity:?}: no work of the epoch starts");
    }
}

#[test]
fn life_save_failed_open() {
    let mut twin = Twin::open(0, 0, Some(9), Some(7));
    twin.feed(Event::SaveFailed(1));
    twin.feed_all([Event::Started(RefOwner::Outbox, 1), Event::Started(RefOwner::Transfer, 1), Event::RestoreClassified(RefOwner::Session, 1)]);
    assert_eq!(twin.replies(), vec![(7, Err(LifecycleError::Storage))], "a late restore answer does not answer open again");
}

#[test]
fn life_save_failed_stale() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    twin.feed(credential(1));
    twin.feed(Event::Saved(2));
    let before = twin.cell().clone();
    twin.feed(Event::SaveFailed(1));
    assert_eq!(*twin.cell(), before, "the later publication stays");
    assert!(twin.replies().contains(&(7, Err(LifecycleError::Storage))));
}

#[test]
fn life_owner_started() {
    let mut twin = Twin::open(0, 0, Some(9), Some(7));
    twin.feed(Event::Saved(1));
    assert!(twin.feed(Event::Started(RefOwner::Outbox, 1)).is_empty());
}

#[test]
fn life_owners_started_read_intents() {
    let mut twin = Twin::open(0, 0, Some(9), Some(7));
    twin.feed(Event::Saved(1));
    twin.feed(Event::Started(RefOwner::Outbox, 1));
    assert_eq!(twin.feed(Event::Started(RefOwner::Transfer, 1)), vec![RefEffect::ReadIntents(1)]);
}

#[test]
fn life_restore_classified() {
    let mut twin = Twin::open(0, 0, Some(9), Some(7));
    twin.feed(Event::Saved(1));
    assert!(twin.feed(Event::RestoreClassified(RefOwner::Session, 1)).is_empty());
}

#[test]
fn life_restore_classified_last() {
    let mut twin = Twin::open(0, 0, Some(9), Some(7));
    twin.settle();
    assert_eq!(twin.replies(), vec![(7, Ok(()))]);
}

#[test]
fn life_restore_stray() {
    let mut twin = Twin::open(0, 0, Some(9), Some(7));
    twin.feed(Event::Saved(1));
    twin.feed_all([Event::Started(RefOwner::Outbox, 99), Event::RestoreClassified(RefOwner::Window, 1), Event::Started(RefOwner::Calls, 1)]);
    twin.feed(close(3));
    twin.feed(Event::RestoreClassified(RefOwner::Outbox, 1));
}
