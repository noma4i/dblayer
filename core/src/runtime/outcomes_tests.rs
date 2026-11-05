use super::*;

#[test]
fn settle_delivers_value_once() {
    let outcomes = Outcomes::<u32>::new();
    let mut waiter = outcomes.register(1);
    assert_eq!(waiter.try_take(), None);
    assert!(outcomes.settle(waiter.id(), 7));
    assert!(!outcomes.settle(waiter.id(), 8));
    assert_eq!(waiter.try_take(), Some(Outcome::Value(7)));
    assert_eq!(waiter.try_take(), None);
}

#[test]
fn epoch_end_terminates_only_ids_of_that_epoch_and_older() {
    let outcomes = Outcomes::<u32>::new();
    let mut old = outcomes.register(1);
    let mut current = outcomes.register(2);
    assert_eq!(outcomes.end_epoch(1, Cause::IdentityChanged), 1);
    assert_eq!(old.try_take(), Some(Outcome::EpochEnded(Cause::IdentityChanged)));
    assert_eq!(current.try_take(), None);
    assert!(!outcomes.settle(old.id(), 1));
}

#[test]
fn register_after_terminal_event_is_answered_at_once() {
    let outcomes = Outcomes::<u32>::new();
    outcomes.end_epoch(3, Cause::SessionReset);
    let mut late = outcomes.register(3);
    assert_eq!(late.try_take(), Some(Outcome::EpochEnded(Cause::SessionReset)));
    outcomes.close();
    let mut after_close = outcomes.register(4);
    assert_eq!(after_close.try_take(), Some(Outcome::Closing));
    assert_eq!(outcomes.open_count(), 0);
}

#[tokio::test]
async fn wait_returns_the_terminal_outcome() {
    let outcomes = Outcomes::<u32>::new();
    let waiter = outcomes.register(1);
    outcomes.close();
    assert_eq!(waiter.wait().await, Outcome::Closing);
}
