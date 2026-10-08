use super::*;
use std::time::Duration;

#[test]
fn parses_a_count_and_a_unit() {
    assert_eq!(parse_duration("5.seconds"), Some(Duration::from_secs(5)));
    assert_eq!(parse_duration("1.day"), Some(Duration::from_secs(86_400)));
}

#[test]
fn rejects_text_without_a_count_or_a_known_unit() {
    assert_eq!(parse_duration("soon"), None);
    assert_eq!(parse_duration("5.fortnights"), None);
}
