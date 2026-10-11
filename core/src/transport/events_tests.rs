use super::*;
use std::time::Duration;

#[test]
fn faults_match_spec_and_default_event_has_none() {
    let event = EventSpec::new("messageCreated");
    let no_merge_key = EventSpec::new("messageCreated").debounced(Duration::from_millis(300));
    let with_merge_key = EventSpec::new("messageCreated").debounced(Duration::from_millis(300)).merged_by_row();
    assert_eq!(event.fault(), None);
    assert_eq!(EventSpec::new("").fault(), Some(EventFault::EmptyName));
    assert_eq!(EventSpec::new("$x").fault(), Some(EventFault::ReservedName));
    assert_eq!(no_merge_key.fault(), Some(EventFault::DebounceWithoutMergeKey));
    assert_eq!(with_merge_key.fault(), None);
}

#[test]
fn bucket_key_uses_merge_key_when_available() {
    let merged = EventSpec::new("chatUpdated").merged_by_row();
    let plain = EventSpec::new("chatUpdated");
    assert_eq!(merged.bucket_key("c1"), "chatUpdated:c1");
    assert_eq!(plain.bucket_key("c1"), "chatUpdated");
}
