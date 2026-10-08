use super::*;
use std::time::Duration;

fn secs(value: u64) -> Duration {
    Duration::from_secs(value)
}

fn table() -> FreshnessTable {
    FreshnessTable::new([
        FreshnessPolicy::new("push", secs(300)),
        FreshnessPolicy::new("catalog", secs(7200)),
        FreshnessPolicy::new("probe", secs(0)),
    ])
}

#[test]
fn missing_lists_required_names_without_a_policy() {
    let table = FreshnessTable::new([FreshnessPolicy::new("probe", secs(0))]);
    assert_eq!(table.missing(&["probe", "catalog", "push"]), vec!["catalog", "push"]);
}

#[test]
fn resolve_fails_for_unknown_policy() {
    assert_eq!(
        FreshnessTable::new([FreshnessPolicy::new("probe", secs(0))]).resolve("catalog"),
        Err(UnknownFreshnessPolicy("catalog".into()))
    );
    assert_eq!(table().resolve("catalog"), Ok(secs(7200)));
}

#[test]
fn later_policy_with_same_name_replaces_earlier() {
    let table = FreshnessTable::new([FreshnessPolicy::new("a", secs(1)), FreshnessPolicy::new("a", secs(2))]);
    assert_eq!(table.resolve("a"), Ok(secs(2)));
}

#[test]
fn stale_at_exact_duration_and_fresh_before() {
    let table = table();
    assert_eq!(table.is_stale(secs(100), secs(100 + 299), "push"), Ok(false));
    assert_eq!(table.is_stale(secs(100), secs(100 + 300), "push"), Ok(true));
    assert_eq!(table.is_stale(secs(100), secs(100), "probe"), Ok(true));
    assert_eq!(table.is_stale(secs(100), secs(100), "nope"), Err(UnknownFreshnessPolicy("nope".into())));
}

#[test]
fn policy_with_zero_duration_is_always_stale_even_for_future_landing() {
    assert_eq!(table().is_stale(secs(500), secs(100), "probe"), Ok(true));
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
