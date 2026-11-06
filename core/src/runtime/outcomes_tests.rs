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
    assert_eq!(crate::test_wait::within(waiter.wait()).await, Outcome::Closing);
}

#[cfg(dblayer_loom)]
mod model {
    use loom::thread;

    use crate::test_wait::join_within;

    use super::*;

    #[test]
    fn loom_settle_against_end_of_epoch_gives_the_id_one_outcome() {
        loom::model(|| {
            let outcomes = Outcomes::<u32>::new();
            let mut waiter = outcomes.register(1);
            let id = waiter.id();
            let settling = {
                let outcomes = outcomes.clone();
                thread::spawn(move || outcomes.settle(id, 7))
            };
            let ending = {
                let outcomes = outcomes.clone();
                thread::spawn(move || outcomes.end_epoch(1, Cause::SessionReset))
            };
            let settled = join_within(settling);
            join_within(ending);
            let outcome = waiter.try_take().unwrap();
            assert_eq!(settled, outcome == Outcome::Value(7));
            assert!(matches!(outcome, Outcome::Value(7) | Outcome::EpochEnded(Cause::SessionReset)));
            assert_eq!(waiter.try_take(), None);
            assert_eq!(outcomes.open_count(), 0);
        });
    }

    #[test]
    fn loom_register_against_close_and_end_of_epoch_never_leaves_a_wait_open() {
        loom::model(|| {
            let outcomes = Outcomes::<u32>::new();
            let registering = {
                let outcomes = outcomes.clone();
                thread::spawn(move || outcomes.register(1))
            };
            let ending = {
                let outcomes = outcomes.clone();
                thread::spawn(move || outcomes.end_epoch(1, Cause::ServerEnded))
            };
            let closing = {
                let outcomes = outcomes.clone();
                thread::spawn(move || outcomes.close())
            };
            let mut waiter = join_within(registering);
            join_within(ending);
            join_within(closing);
            assert!(matches!(waiter.try_take(), Some(Outcome::EpochEnded(Cause::ServerEnded) | Outcome::Closing)));
            assert_eq!(waiter.try_take(), None);
            assert_eq!(outcomes.open_count(), 0);
        });
    }

    #[test]
    fn loom_an_id_of_a_later_epoch_survives_the_end_of_an_earlier_one_until_close() {
        loom::model(|| {
            let outcomes = Outcomes::<u32>::new();
            let mut later = outcomes.register(2);
            let ending = {
                let outcomes = outcomes.clone();
                thread::spawn(move || outcomes.end_epoch(1, Cause::SessionReset))
            };
            let id = later.id();
            let settling = {
                let outcomes = outcomes.clone();
                thread::spawn(move || outcomes.settle(id, 5))
            };
            join_within(ending);
            assert!(join_within(settling));
            assert_eq!(later.try_take(), Some(Outcome::Value(5)));
        });
    }
}
