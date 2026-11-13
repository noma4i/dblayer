use super::*;

/// `Boot` of a database that opens: the message to Lifecycle, the generation and the revision the Store takes from the database.
#[test]
fn store_boot_opens_lifecycle_with_the_saved_state() {
    // (found, saved, revision) -> (saved_epoch, saved_generation, saved_identity) of the message, generation of Moment, revision
    let cases = [
        (Boot::New, (0, 0, None), 0, (0, 0, None), 0, 0),
        (Boot::Same, (4, 2, Some(7)), 0, (4, 2, Some(7)), 2, 0),
        (Boot::Same, (4, 7, Some(7)), 9, (4, 7, Some(7)), 7, 9),
        (Boot::ChangedModels, (4, 2, Some(7)), 0, (4, 3, Some(7)), 3, 1),
        (Boot::FormatChange, (4, 2, None), 0, (4, 3, None), 3, 1),
        (Boot::LocalModelChanged, (4, 2, Some(7)), 0, (4, 2, Some(7)), 2, 0),
    ];
    for (found, saved, revision, message, generation, state_revision) in cases {
        let mut twin = Twin::new();
        let Some(StoreEffect::OpenLifecycle(opened)) = twin.boot(found.clone(), saved, false, revision) else { panic!("{found:?}: the boot opens Lifecycle") };
        assert_eq!((opened.saved_epoch, opened.saved_generation, opened.saved_identity), message, "{found:?}: the cache of a changed model set is erased in the Boot transaction, the generation grows");
        assert_eq!((twin.state().generation(), twin.state().revision()), (generation, state_revision), "{found:?}");
    }
}

/// A database that cannot be used closes the Store without a change.
#[test]
fn store_boot_failure_closes_the_store() {
    let mut twin = Twin::new();
    assert!(matches!(twin.boot(Boot::CorruptManifest, (0, 0, None), false, 0), Some(StoreEffect::OpenCorrupt(_))));
    assert_eq!((twin.state().mode(), twin.state().revision()), (StoreMode::Closed, 0), "the database is not changed");
    let mut twin = Twin::new();
    assert!(matches!(twin.boot(Boot::SqliteError, (0, 0, None), false, 0), Some(StoreEffect::OpenFailed(_))));
    assert_eq!(twin.state().mode(), StoreMode::Closed);
}

/// Only an unowned pending login without an identity is read directly.
#[test]
fn store_read_intents_direct_only_for_a_pending_login() {
    for (identity, pending, direct) in [(None, true, true), (Some(7), true, false), (None, false, false)] {
        let mut twin = Twin::new();
        twin.boot(Boot::Same, (4, 2, identity), pending, 0);
        let effects = twin.feed(Request::ReadIntents { epoch: 5 });
        assert_eq!(effects, vec![Action::ReadIntents { epoch: 5, direct }], "{identity:?} {pending}");
    }
    let mut twin = Twin::live();
    assert_eq!(twin.feed(Request::ReadIntents { epoch: 1 }), vec![Action::ReadIntents { epoch: 1, direct: false }]);
}
