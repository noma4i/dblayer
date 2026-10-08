use super::freshness::*;
use super::error::SyncError;
use std::time::Duration;

#[test]
fn parses_seconds_minutes_hours_and_days() {
    assert_eq!(parse_duration("30.seconds"), Some(Duration::from_secs(30)));
    assert_eq!(parse_duration("5.minutes"), Some(Duration::from_secs(300)));
    assert_eq!(parse_duration("1.minute"), Some(Duration::from_secs(60)));
    assert_eq!(parse_duration("2.hours"), Some(Duration::from_secs(7200)));
    assert_eq!(parse_duration("1.day"), Some(Duration::from_secs(86_400)));
}

#[test]
fn rejects_malformed_durations() {
    assert_eq!(parse_duration("soon"), None);
    assert_eq!(parse_duration("x.minutes"), None);
    assert_eq!(parse_duration("5.fortnights"), None);
    assert_eq!(parse_duration(""), None);
}

#[test]
fn literal_duration_resolves() {
    assert_eq!(stale_window("90.seconds"), Ok(Duration::from_secs(90)));
}

#[test]
fn unknown_name_is_an_error() {
    assert_eq!(stale_window("eventually"), Err(SyncError::UnknownFreshness("eventually".to_string())));
}
