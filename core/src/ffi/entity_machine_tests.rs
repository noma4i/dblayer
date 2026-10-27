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
    assert!(closed.cleared && closed.stage == EntityStage::Closed && closed.revision == 1);
    for verdict in [machine.accept(9, true, None), machine.seed(9), machine.fail("late".into(), None), machine.reset()] {
        assert!(!verdict.applied && !verdict.cleared && verdict.stage == EntityStage::Closed && verdict.revision == 1);
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
    assert!(reset.cleared && reset.stage == EntityStage::Opening && reset.revision == 1 && reset.feed == 0);
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
fn reset_and_close_differ_and_reset_opens_each_entity_without_lowering_revision() {
    let singleton = EntityMachine::new(true);
    let feed = singleton.begin_feed().feed;
    singleton.accept(3, true, Some(feed));
    assert_eq!(singleton.reset().stage, EntityStage::Opening);
    assert_eq!(singleton.database_closed().stage, EntityStage::Closed);
    let plain = EntityMachine::new(false);
    plain.accept(3, true, None);
    let kept = plain.reset();
    assert!(kept.cleared && kept.stage == EntityStage::Opening && kept.revision == 3 && !kept.settled);
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
    assert!(second.accept(1, true, Some(current)).applied);
}

#[test]
fn the_read_outcome_moves_from_not_loaded_to_loaded_or_failed_and_a_wait_settles_with_it() {
    let plain = EntityMachine::new(false);
    let idle = plain.accept(1, true, None);
    assert!(idle.read == EntityRead::NotLoaded && idle.settled);
    let other = EntityMachine::new(false);
    assert!(other.read_started().read == EntityRead::NotLoaded && !other.read_started().settled);
    let offline = other.read_ended(Some(EntityRefreshError::Offline), None);
    assert!(offline.read == EntityRead::Failed { error: EntityRefreshError::Offline } && offline.settled);
    other.read_started();
    let landed = other.read_ended(None, None);
    assert!(landed.read == EntityRead::Loaded && !landed.settled, "a read that ends before the first snapshot waits for it");
    assert!(other.accept(1, true, None).settled);
    let cached = EntityMachine::new(false);
    cached.read_started();
    assert!(cached.seed(2).settled, "a value already there ends the wait");

    let singleton = EntityMachine::new(true);
    let feed = singleton.begin_feed().feed;
    assert!(!singleton.accept(1, false, Some(feed)).settled);
    assert!(!singleton.read_ended(None, Some(feed + 1)).settled, "another feed ends nothing");
    assert!(singleton.read_ended(None, Some(feed)).read == EntityRead::Loaded);
    let reset = singleton.reset();
    assert!(reset.read == EntityRead::NotLoaded && !reset.settled);
    let again = singleton.begin_feed().feed;
    let failed = singleton.fail("boom".into(), Some(again));
    assert!(failed.settled && failed.read == EntityRead::Failed { error: EntityRefreshError::Failed { message: "boom".into() } });
}
