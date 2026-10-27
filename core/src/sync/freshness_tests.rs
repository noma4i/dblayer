use super::freshness::*;
use super::error::SyncError;
use std::time::Duration;

#[test]
fn durations_parse_by_unit_and_malformed_input_is_rejected() {
    let cases = [
        ("30.seconds", Some(Duration::from_secs(30))),
        ("5.minutes", Some(Duration::from_secs(300))),
        ("1.minute", Some(Duration::from_secs(60))),
        ("2.hours", Some(Duration::from_secs(7200))),
        ("1.day", Some(Duration::from_secs(86_400))),
        ("soon", None),
        ("x.minutes", None),
        ("5.fortnights", None),
        ("", None),
    ];
    for (input, expected) in cases {
        assert_eq!(parse_duration(input), expected, "{input:?}");
    }
}

#[test]
fn a_stale_window_is_a_literal_duration_and_any_name_is_an_error() {
    let cases = [
        ("90.seconds", Ok(Duration::from_secs(90))),
        ("eventually", Err(SyncError::UnknownFreshness("eventually".to_string()))),
        ("18446744073709551615.days", Err(SyncError::UnknownFreshness("18446744073709551615.days".to_string()))),
    ];
    for (input, expected) in cases {
        assert_eq!(stale_window(input), expected, "{input:?}");
    }
}
