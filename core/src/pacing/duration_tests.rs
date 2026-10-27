use super::*;
use std::time::Duration;

#[test]
fn parses_a_count_and_a_unit() {
    assert_eq!(parse_duration("5.seconds"), Some(Duration::from_secs(5)));
    assert_eq!(parse_duration("1.day"), Some(Duration::from_secs(86_400)));
}

#[test]
fn rejects_text_without_a_count_or_a_known_unit() {
    for text in ["soon", "5.fortnights", "18446744073709551615.days", "18446744073709551615.hours", "18446744073709551615.minutes", "18446744073709551615.seconds"] {
        assert_eq!(parse_duration(text), None, "{text}");
    }
}

#[test]
fn parses_milliseconds() {
    assert_eq!(parse_duration("300.milliseconds"), Some(Duration::from_millis(300)));
    assert_eq!(parse_duration("1.millisecond"), Some(Duration::from_millis(1)));
}
