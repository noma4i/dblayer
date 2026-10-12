use std::sync::Mutex;
use tokio::sync::mpsc;

use super::SubscriptionSink;
use crate::transport::{SubscriptionEvent, SubscriptionFailureKind};

fn sink() -> (SubscriptionSink, mpsc::UnboundedReceiver<SubscriptionEvent>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (SubscriptionSink { sender: Mutex::new(Some(sender)) }, receiver)
}

#[test]
fn a_frame_without_data_fails_the_subscription_as_internal_and_an_enveloped_frame_passes() {
    let (sink, mut receiver) = sink();

    let rejected = sink.event(r#"{"messageCreated":{"message":{"id":"9"}}}"#.to_string());
    let failure = match receiver.try_recv() {
        Ok(SubscriptionEvent::Failed(failure)) => Some(failure),
        _ => None,
    };
    assert!(rejected.is_err());
    assert_eq!(failure.map(|failure| failure.kind), Some(SubscriptionFailureKind::Internal));

    sink.event(r#"{"data":{"messageCreated":{"message":{"id":"9"}}}}"#.to_string()).unwrap();
    assert!(matches!(receiver.try_recv(), Ok(SubscriptionEvent::Next(_))));
}

#[test]
fn inv_panic_01_a_subscription_sink_survives_a_poisoned_lock() {
    let (sink, mut receiver) = sink();
    crate::db::fixtures::poison(&sink.sender);
    sink.ready();
    assert!(matches!(receiver.try_recv(), Ok(SubscriptionEvent::Ready)));
    sink.closed();
}
