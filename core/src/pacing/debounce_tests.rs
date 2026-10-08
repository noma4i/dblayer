use super::*;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
struct Counter {
    id: &'static str,
    value: i32,
}

fn counter(id: &'static str, value: i32) -> Counter {
    Counter { id, value }
}

fn plain() -> Debounce<Counter> {
    Debounce::new(Duration::from_secs(1), |item: &Counter| item.id.to_string())
}

fn summing(interval: Duration) -> Debounce<Counter> {
    Debounce::new(interval, |item: &Counter| item.id.to_string())
        .merging(|previous, incoming| counter(previous.id, previous.value + incoming.value))
}

#[test]
fn appends_incoming_payload_when_pending_is_empty() {
    assert_eq!(plain().coalesce(vec![], counter("a", 1)), vec![counter("a", 1)]);
}

#[test]
fn replaces_matching_payload_without_merge() {
    let result = plain().coalesce(vec![counter("a", 1), counter("b", 2)], counter("a", 9));
    assert_eq!(result, vec![counter("a", 9), counter("b", 2)]);
}

#[test]
fn merges_matching_payloads() {
    let result = summing(Duration::from_secs(1)).coalesce(vec![counter("a", 1), counter("b", 2)], counter("a", 9));
    assert_eq!(result, vec![counter("a", 10), counter("b", 2)]);
}

#[test]
fn coalesces_all_payloads_and_stores_interval() {
    let rule = summing(Duration::from_millis(300));
    let result = rule.coalesce_all(vec![counter("a", 1), counter("b", 2), counter("a", 3)]);
    assert_eq!(result, vec![counter("a", 4), counter("b", 2)]);
    assert_eq!(rule.interval, Duration::from_millis(300));
}
