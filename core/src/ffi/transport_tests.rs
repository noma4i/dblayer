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
    let extensions = crate::transport::GraphqlExtensions { code: Some("NOT_FOUND".into()), transaction_disposition: Some(crate::transport::TransactionDisposition::Keep), fields: Default::default() };
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

struct LateToken {
    issued: Mutex<Vec<String>>,
    handed_back: Mutex<Vec<String>>,
    delivered: Mutex<std::sync::mpsc::Receiver<()>>,
    announce: std::sync::mpsc::Sender<()>,
}

impl LateToken {
    async fn issue(self: &std::sync::Arc<Self>) -> Result<String, super::DbError> {
        let (reply, result) = tokio::sync::oneshot::channel();
        let foreign = self.clone();
        std::thread::spawn(move || {
            crate::db::locked(&foreign.issued).push("token-1".into());
            let _ = reply.send("token-1".to_string());
            let _ = foreign.announce.send(());
        });
        result.await.map_err(|_| super::DbError::Interrupted { detail: "reply dropped".into() })
    }
}

#[async_trait::async_trait]
impl super::Transport for std::sync::Arc<LateToken> {
    async fn open_cable(&self) -> Result<String, super::DbError> { self.issue().await }
    async fn cable_send(&self, _: String, _: String) -> Result<(), super::DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<super::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, token: String) {
        crate::db::locked(&self.handed_back).push(token);
        let _ = self.announce.send(());
    }
    async fn query(&self, _: super::TransportRequest) -> Result<String, super::DbError> { unreachable!() }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, super::DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, _: std::sync::Arc<super::SubscriptionSink>) -> Result<String, super::DbError> { self.issue().await }
    fn unsubscribe(&self, token: String) { crate::db::locked(&self.handed_back).push(token); }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: Vec<u8>, _: std::sync::Arc<super::ByteProgress>) -> Result<super::PutReply, super::DbError> { unreachable!() }
}

fn late_token() -> (std::sync::Arc<LateToken>, super::ForeignTransport) {
    let (announce, delivered) = std::sync::mpsc::channel();
    let foreign = std::sync::Arc::new(LateToken { issued: Mutex::new(Vec::new()), handed_back: Mutex::new(Vec::new()), delivered: Mutex::new(delivered), announce });
    let transport = super::ForeignTransport(std::sync::Arc::new(foreign.clone()));
    (foreign, transport)
}

#[test]
fn a_token_issued_before_the_core_dropped_the_subscribe_call_is_handed_back() {
    use crate::transport::Transport;
    use std::future::Future;
    let (foreign, transport) = late_token();
    let mut call = Box::pin(transport.subscribe(crate::transport::GraphQLRequest::new("subscription s { s }", "s", serde_json::json!({}))));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(call.as_mut().poll(&mut context).is_pending());
    crate::db::locked(&foreign.delivered).recv().unwrap();
    drop(call);
    assert_eq!(*crate::db::locked(&foreign.issued), *crate::db::locked(&foreign.handed_back));
}

#[test]
fn a_cable_token_issued_before_the_core_dropped_the_open_call_is_closed() {
    use crate::transport::Transport;
    use std::future::Future;
    let (foreign, transport) = late_token();
    let mut call = Box::pin(transport.open_cable());
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(call.as_mut().poll(&mut context).is_pending());
    crate::db::locked(&foreign.delivered).recv().unwrap();
    drop(call);
    crate::db::locked(&foreign.delivered).recv_timeout(std::time::Duration::from_secs(10)).unwrap();
    assert_eq!(*crate::db::locked(&foreign.issued), *crate::db::locked(&foreign.handed_back));
}
