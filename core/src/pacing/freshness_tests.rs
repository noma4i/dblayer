use super::*;
use std::time::Duration;

fn secs(value: u64) -> Duration {
    Duration::from_secs(value)
}

#[test]
fn empty_result_uses_empty_policy_or_falls_back_to_landed() {
    assert_eq!(selected_policy(true, "push", Some("emptyRetry")), "emptyRetry");
    assert_eq!(selected_policy(true, "push", None), "push");
    assert_eq!(selected_policy(false, "push", Some("emptyRetry")), "push");
}

#[test]
fn freshness_expires_at_duration_boundary_and_future_landing_stays_fresh() {
    let window = secs(300);
    assert!(is_fresh(secs(1000), secs(1299), window));
    assert!(!is_fresh(secs(1000), secs(1300), window));
    assert!(is_fresh(secs(1010), secs(1000), window));
    assert_eq!(expires_at(secs(100), secs(7200)), secs(7300));
}
