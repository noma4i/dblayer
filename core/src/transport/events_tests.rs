use super::*;
use std::time::Duration;

#[test]
fn faults_match_spec_and_default_event_publishes() {
    let event = EventSpec::new("messageCreated");
    let no_merge_key = EventSpec::new("messageCreated").debounced(Duration::from_millis(300));
    let with_merge_key = EventSpec::new("messageCreated").debounced(Duration::from_millis(300)).merged_by_row();
    let no_root_write = EventSpec::new("messageCreated").without_root_write();
    assert_eq!(event.fault(), None);
    assert!(event.publishes_after_commit());
    assert_eq!(EventSpec::new("").fault(), Some(EventFault::EmptyName));
    assert_eq!(EventSpec::new("$x").fault(), Some(EventFault::ReservedName));
    assert_eq!(no_merge_key.fault(), Some(EventFault::DebounceWithoutMergeKey));
    assert_eq!(with_merge_key.fault(), None);
    assert_eq!(no_root_write.fault(), Some(EventFault::NoRootWrite));
    assert!(!no_root_write.publishes_after_commit());
}

#[test]
fn verdict_follows_failure_priority() {
    let event = EventSpec::new("messageCreated");
    let no_root_write = EventSpec::new("messageCreated").without_root_write();
    assert_eq!(event.verdict(false, false, false), PublishVerdict::Publish);
    assert_eq!(no_root_write.verdict(false, false, false), PublishVerdict::SkipNoRootWrite);
    assert_eq!(event.verdict(true, true, true), PublishVerdict::SkipPlanningError);
    assert_eq!(event.verdict(false, true, true), PublishVerdict::SkipApplyError);
    assert_eq!(event.verdict(false, false, true), PublishVerdict::SkipStaleGeneration);
}

#[test]
fn bucket_key_uses_merge_key_when_available() {
    let merged = EventSpec::new("chatUpdated").merged_by_row();
    let plain = EventSpec::new("chatUpdated");
    assert_eq!(merged.bucket_key("c1"), "chatUpdated:c1");
    assert_eq!(plain.bucket_key("c1"), "chatUpdated");
}

#[test]
fn routable_excludes_faulted_events() {
    let valid = EventSpec::new("created");
    let invalid = EventSpec::new("");
    assert_eq!(routable(&[valid.clone(), invalid]), vec![valid]);
}

#[test]
fn names_preserve_input_order_and_duplicates() {
    let specs = [EventSpec::new("updated"), EventSpec::new("updated"), EventSpec::new("created")];
    assert_eq!(event_names(&specs), vec!["updated", "updated", "created"]);
}

#[test]
fn max_interval_returns_largest_present_value() {
    let specs = [
        EventSpec::new("updated").debounced(Duration::from_millis(300)).merged_by_row(),
        EventSpec::new("created").debounced(Duration::from_millis(500)).merged_by_row(),
    ];
    assert_eq!(max_debounce(&specs), Some(Duration::from_millis(500)));
    assert_eq!(max_debounce(&[EventSpec::new("created")]), None);
}
