use super::*;
use std::time::Duration;

const PLAIN: Backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(30));

#[test]
fn delay_doubles_until_the_cap() {
    for (attempt, expected) in [(0, 1), (1, 2), (4, 16), (5, 30), (10, 30), (200, 30)] {
        assert_eq!(PLAIN.delay(attempt), Duration::from_secs(expected));
    }
}

#[test]
fn jitter_only_shortens_the_delay_and_repeats_for_one_seed() {
    let jittered = Backoff::new(Duration::from_secs(1), Duration::from_secs(15)).with_jitter(0.5, 7);
    for attempt in 0..20 {
        let plain = Backoff::new(Duration::from_secs(1), Duration::from_secs(15)).delay(attempt);
        let delay = jittered.delay(attempt);
        assert!(delay <= plain && delay >= plain.mul_f64(0.5), "attempt {attempt}: {delay:?} vs {plain:?}");
        assert_eq!(delay, jittered.delay(attempt));
    }
    let other = jittered.with_jitter(0.5, 8);
    assert!((0..20).any(|attempt| other.delay(attempt) != jittered.delay(attempt)));
    assert!((0..20).any(|attempt| jittered.delay(attempt) != PLAIN.delay(attempt).min(Duration::from_secs(15))));
}

#[test]
fn attempts_stop_at_the_limit() {
    let limited = PLAIN.limited(3);
    assert!(limited.allows(2));
    assert!(!limited.allows(3));
    assert!(PLAIN.allows(u32::MAX - 1));
}
