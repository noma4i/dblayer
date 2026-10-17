use std::sync::Mutex;
use tokio::sync::mpsc;

use super::SubscriptionSink;
use crate::transport::{SubscriptionEvent, SubscriptionFailureKind};

fn sink() -> (SubscriptionSink, mpsc::UnboundedReceiver<SubscriptionEvent>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (SubscriptionSink { sender: Mutex::new(Some(sender)) }, receiver)
}

#[test]
fn subscription_failures_keep_graphql_extensions() {
    let (sink, mut receiver) = sink();
    let extensions = crate::transport::GraphqlExtensions { code: Some("NOT_FOUND".into()), transaction_disposition: Some(crate::transport::TransactionDisposition::Keep) };
    sink.failed(super::DbError::Graphql { messages: vec!["missing".into()], extensions: Some(extensions.clone()) });
    let Ok(SubscriptionEvent::Failed(failure)) = receiver.try_recv() else { panic!("missing failure") };
    assert!(matches!(failure.error, crate::transport::TransportError::Graphql { extensions: Some(found), .. } if found == extensions));
    sink.event(r#"{"errors":[{"message":"missing","extensions":{"code":"NOT_FOUND","transactionDisposition":"KEEP"}}]}"#.into()).unwrap();
    let Ok(SubscriptionEvent::Failed(failure)) = receiver.try_recv() else { panic!("missing frame failure") };
    assert!(matches!(failure.error, crate::transport::TransportError::Graphql { extensions: Some(found), .. } if found == extensions));
}

struct FakeSocket;

#[async_trait::async_trait]
impl super::Transport for FakeSocket {
    async fn open_cable(&self) -> Result<String, super::DbError> { Ok("socket-1".into()) }
    async fn cable_send(&self, token: String, text: String) -> Result<(), super::DbError> {
        assert_eq!(token, "socket-1");
        assert_eq!(text, "ciphertext");
        Ok(())
    }
    async fn cable_next(&self, token: String) -> Option<super::EncryptedSocketEvent> {
        assert_eq!(token, "socket-1");
        Some(super::EncryptedSocketEvent::Closed { code: 4503, reason: "unavailable".into() })
    }
    async fn cable_close(&self, token: String) { assert_eq!(token, "socket-1"); }
    async fn query(&self, _: super::TransportRequest) -> Result<String, super::DbError> { unreachable!() }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, super::DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, _: std::sync::Arc<super::SubscriptionSink>) -> Result<String, super::DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) { unreachable!() }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: Vec<u8>, _: std::sync::Arc<super::ByteProgress>) -> Result<super::PutReply, super::DbError> { unreachable!() }
}

#[tokio::test]
async fn raw_socket_bridge_preserves_ciphertext_and_close_codes() {
    use crate::transport::Transport;
    let transport = super::ForeignTransport(std::sync::Arc::new(FakeSocket));
    let socket = transport.open_cable().await.unwrap();
    socket.send("ciphertext".into()).await.unwrap();
    assert!(matches!(socket.next().await, Some(crate::transport::SocketEvent::Closed { code: 4503, reason }) if reason == "unavailable"));
    socket.close().await;
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
