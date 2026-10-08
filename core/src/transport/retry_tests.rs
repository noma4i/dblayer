use super::*;
use std::time::Duration;

#[test]
fn retry_class_follows_code_and_budgets_limit_retries() {
    assert!(retriable_codes().contains(&"TIMEOUT"));
    assert_eq!(RetryClass::classify(true, None, &[]), RetryClass::Network);
    assert_eq!(RetryClass::classify(false, Some(503), &[]), RetryClass::Server);
    assert_eq!(RetryClass::classify(false, Some(400), &["TIMEOUT"]), RetryClass::Retriable);
    assert_eq!(RetryClass::classify(false, Some(400), &[]), RetryClass::Fatal);
    assert_eq!(RetryClass::classify(false, None, &[""]), RetryClass::Fatal);
    assert!(RetryClass::Network.allows_retry(2, false));
    assert!(!RetryClass::Network.allows_retry(3, false));
    assert!(!RetryClass::Fatal.allows_retry(0, false));
    assert!(!RetryClass::Network.allows_retry(0, true));
    assert_eq!(RetryClass::Network.budget(), 3);
    assert_eq!(RetryClass::Server.budget(), 2);
    assert_eq!(RetryClass::Retriable.budget(), 2);
    assert_eq!(RetryClass::Fatal.budget(), 0);
}

#[test]
fn delay_grows_and_stops() {
    let cases = [(0, 1), (1, 2), (4, 16), (5, 30), (10, 30)];
    for (attempt, expected) in cases {
        assert_eq!(retry_delay(attempt), Duration::from_secs(expected));
    }
}

#[test]
fn server_error_boundaries() {
    assert_eq!(RetryClass::classify(false, Some(500), &[]), RetryClass::Server);
    assert_eq!(RetryClass::classify(false, Some(599), &[]), RetryClass::Server);
    assert_eq!(RetryClass::classify(false, Some(600), &[]), RetryClass::Fatal);
    assert_eq!(RetryClass::classify(false, Some(499), &[]), RetryClass::Fatal);
}

#[test]
fn transport_error_classifies_by_variant() {
    assert_eq!(RetryClass::of_error(&TransportError::Network("x".into())), RetryClass::Network);
    assert_eq!(
        RetryClass::of_error(&TransportError::Http { status: 502, body: String::new() }),
        RetryClass::Server
    );
    assert_eq!(RetryClass::of_error(&TransportError::Decode("x".into())), RetryClass::Fatal);
}
