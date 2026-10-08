use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[test]
fn cancel_calls_on_cancel_once() {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let handle = SubscriptionHandle::new(move || {
        seen.fetch_add(1, Ordering::SeqCst);
    });
    handle.cancel();
    handle.cancel();
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dropping_subscription_cancels_and_closed_stream_ends() {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut subscription = Subscription::new(
        receiver,
        SubscriptionHandle::new(move || {
            seen.fetch_add(1, Ordering::SeqCst);
        }),
    );
    sender.send(SubscriptionEvent::Ready).unwrap();
    drop(sender);
    assert_eq!(subscription.next().await, Some(SubscriptionEvent::Ready));
    assert_eq!(subscription.next().await, None);
    drop(subscription);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn transient_failures_report_degraded_only_on_fourth() {
    let mut state = SubscriptionRetry::default();
    let mut reports = Vec::new();
    for _ in 0..6 {
        let outcome = state.record_failure(SubscriptionFault::Transient);
        reports.push(outcome.reports_degraded);
        state = outcome.state;
    }
    assert_eq!(reports, vec![false, false, false, true, false, false]);
    assert_eq!(state.attempts, 6);
}

#[test]
fn graphql_and_internal_fault_report_only_once() {
    for fault in [SubscriptionFault::Graphql, SubscriptionFault::Internal] {
        let first = SubscriptionRetry::default().record_failure(fault);
        let second = first.state.record_failure(fault);
        assert!(first.reports_degraded);
        assert_eq!(first.state.attempts, 0);
        assert!(!second.reports_degraded);
        assert_eq!(second.state.attempts, 0);
    }
}

#[test]
fn confirmed_resets_attempts_and_reported_faults() {
    let mut state = SubscriptionRetry::default();
    for _ in 0..4 {
        state = state.record_failure(SubscriptionFault::Transient).state;
    }
    let confirmed = state.confirmed();
    let outcome = confirmed.record_failure(SubscriptionFault::Transient);
    assert_eq!(confirmed.attempts, 0);
    assert!(confirmed.reported_faults.is_empty());
    assert!(!outcome.reports_degraded);
    assert_eq!(outcome.state.attempts, 1);
}

#[test]
fn failure_kind_maps_to_fault() {
    assert_eq!(SubscriptionFailureKind::Transport.fault(), SubscriptionFault::Transient);
    assert_eq!(SubscriptionFailureKind::Graphql.fault(), SubscriptionFault::Graphql);
    assert_eq!(SubscriptionFailureKind::Internal.fault(), SubscriptionFault::Internal);
}
