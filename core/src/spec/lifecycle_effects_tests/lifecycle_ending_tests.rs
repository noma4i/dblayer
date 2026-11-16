use super::*;

#[test]
fn life_credential_starts_the_session() {
    let mut twin = Twin::new();
    twin.feed(credential(1));
    assert!(twin.history.iter().any(|effect| matches!(effect, LifecycleEffect::EpochEnded { cause: Cause::SessionStarted, .. })), "a stream open without a session ends when the account is entered");
    twin.settle();
    assert_eq!(*twin.phase(), LifecyclePhase::Session { epoch: 2, identity: 1 });
    assert!(twin.feed(credential(1)).is_empty(), "the same credential changes nothing");
}

#[test]
fn life_reset_clears_and_answers_after_saved() {
    let mut twin = ending();
    assert_eq!(twin.published().state, PublishedState::Ending);
    assert_eq!(twin.published().epoch, 2, "the calls of the ended epoch stay out");
    let done = cleared(&twin);
    twin.feed(done);
    assert!(twin.replies().is_empty(), "the reset is answered after Saved");
    twin.settle();
    assert_eq!(twin.replies(), vec![(1, Ok(()))]);
}

#[test]
fn life_second_reset_joins_the_first() {
    let mut twin = ending();
    twin.feed(reset(2));
    let done = cleared(&twin);
    twin.feed(done);
    twin.settle();
    assert_eq!(twin.replies(), vec![(1, Ok(())), (2, Ok(()))], "both resets are answered");
}

#[test]
fn life_credential_after_failure() {
    let mut twin = ending();
    let failed = clear_failed(&twin);
    twin.feed(failed);
    assert_eq!(twin.published().state, PublishedState::EndingFailed);
    assert!(twin.replies().contains(&(1, Err(LifecycleError::Storage))));
    let effects = twin.feed(credential(2));
    assert!(effects.iter().any(|effect| matches!(effect, LifecycleEffect::ClearAccount { .. })), "the clearing repeats");
    twin.settle();
    let done = cleared(&twin);
    twin.feed(done);
    twin.settle();
    assert_eq!(twin.published().identity, Some(2), "the committed credential is not lost");
}
