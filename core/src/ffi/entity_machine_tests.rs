use super::*;

fn failed(message: &str) -> EntityStage { EntityStage::Failed { message: message.into() } }

#[test]
fn deliveries_follow_revision_order_and_a_failure_keeps_the_accepted_value() {
    let machine = EntityMachine::new(false);
    let seen = machine.accept(7, true, None);
    assert!(seen.applied && seen.stage == EntityStage::Present && seen.revision == 7 && !seen.stale);
    let older = machine.accept(5, true, None);
    assert!(!older.applied && older.stage == EntityStage::Present && older.revision == 7);
    let hurt = machine.fail("bad row".into(), None);
    assert!(!hurt.applied && !hurt.cleared && hurt.stage == failed("bad row") && hurt.revision == 7);
    let same = machine.accept(7, false, None);
    assert!(same.applied && same.stage == EntityStage::Absent);
    let older_removal = machine.accept(6, false, None);
    assert!(!older_removal.applied && older_removal.stage == EntityStage::Absent);
}

#[test]
fn a_seed_applies_only_while_newer_and_before_any_delivery() {
    let machine = EntityMachine::new(false);
    let retained = machine.seed(4);
    assert!(retained.applied && retained.stale && retained.stage == EntityStage::Present && retained.revision == 4);
    assert!(!machine.seed(3).applied);
    let cached = machine.seed(6);
    assert!(cached.applied && cached.revision == 6);
    let fresh = machine.accept(6, true, None);
    assert!(fresh.applied && !fresh.stale);
    let late = machine.seed(99);
    assert!(!late.applied && late.revision == 6 && !late.stale);
}

#[test]
fn a_closed_entity_ignores_every_later_event() {
    let machine = EntityMachine::new(false);
    machine.accept(1, true, None);
    let closed = machine.database_closed();
    assert!(closed.cleared && closed.stage == EntityStage::Closed && closed.revision == i64::MIN);
    for verdict in [machine.accept(9, true, None), machine.seed(9), machine.fail("late".into(), None), machine.reset(), machine.unopened(1)] {
        assert!(!verdict.applied && !verdict.cleared && verdict.stage == EntityStage::Closed && verdict.revision == i64::MIN);
    }
}

#[test]
fn a_singleton_accepts_only_the_current_feed_and_a_reset_replaces_it() {
    let machine = EntityMachine::new(true);
    assert!(!machine.accept(1, true, None).applied);
    let first = machine.begin_feed().feed;
    assert!(machine.accept(1, true, Some(first)).applied);
    assert!(!machine.accept(2, true, Some(first + 1)).applied);
    let reset = machine.reset();
    assert!(reset.cleared && reset.stage == EntityStage::Opening && reset.revision == i64::MIN && reset.feed == 0);
    assert!(!machine.accept(i64::MAX, true, Some(first)).applied);
    assert!(machine.fail("old feed".into(), Some(first)).stage == EntityStage::Opening);
    let second = machine.begin_feed().feed;
    assert_ne!(first, second);
    assert!(!machine.accept(i64::MAX, true, Some(first)).applied);
    let next = machine.accept(1, true, Some(second));
    assert!(next.applied && next.stage == EntityStage::Present && next.revision == 1);
    assert_eq!(machine.fail("new feed".into(), Some(second)).stage, failed("new feed"));
}

#[test]
fn reset_and_close_differ_and_a_plain_entity_keeps_its_state_on_reset() {
    let singleton = EntityMachine::new(true);
    let feed = singleton.begin_feed().feed;
    singleton.accept(3, true, Some(feed));
    assert_eq!(singleton.reset().stage, EntityStage::Opening);
    assert_eq!(singleton.database_closed().stage, EntityStage::Closed);
    let plain = EntityMachine::new(false);
    plain.accept(3, true, None);
    let kept = plain.reset();
    assert!(!kept.cleared && kept.stage == EntityStage::Present && kept.revision == 3);
}

#[test]
fn an_unopened_feed_closes_only_when_it_is_current() {
    let machine = EntityMachine::new(true);
    let first = machine.begin_feed().feed;
    machine.reset();
    let second = machine.begin_feed().feed;
    assert_eq!(machine.unopened(first).stage, EntityStage::Opening);
    assert_eq!(machine.unopened(second).stage, EntityStage::Closed);
}

#[test]
fn a_feed_of_one_machine_changes_nothing_in_another() {
    let first = EntityMachine::new(true);
    let old = first.begin_feed().feed;
    first.database_closed();
    let second = EntityMachine::new(true);
    let current = second.begin_feed().feed;
    assert_ne!(old, current);
    assert!(!second.accept(1, true, Some(old)).applied);
    assert_eq!(second.fail("old".into(), Some(old)).stage, EntityStage::Opening);
    assert_eq!(second.unopened(old).stage, EntityStage::Opening);
    assert!(second.accept(1, true, Some(current)).applied);
}
