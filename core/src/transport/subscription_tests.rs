use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use crate::test_wait::within;

#[test]
fn cancel_calls_on_cancel_once() {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let handle = ServerSubscriptionHandle::new(move || {
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
    let mut subscription = ServerSubscription::new(
        receiver,
        ServerSubscriptionHandle::new(move || {
            seen.fetch_add(1, Ordering::SeqCst);
        }),
    );
    sender.send(SubscriptionEvent::Ready).unwrap();
    drop(sender);
    assert_eq!(within(subscription.next()).await, Some(SubscriptionEvent::Ready));
    assert_eq!(within(subscription.next()).await, None);
    drop(subscription);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}
