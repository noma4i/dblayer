use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::SubscriptionSink;
use crate::transport::{SubscriptionEvent, SubscriptionFailureKind};
use crate::test_wait::{recv_within, within};

fn sink() -> (SubscriptionSink, crate::transport::SubscriptionReceiver) {
    let (sender, receiver) = crate::transport::subscription_channel();
    (SubscriptionSink::new(crate::ffi::database::executor().unwrap().handle().clone(), sender, |_| Box::pin(async {})), receiver)
}

#[test]
fn subscription_failures_keep_graphql_extensions() {
    let (sink, mut receiver) = sink();
    let extensions = crate::transport::GraphqlExtensions { code: Some("NOT_FOUND".into()), transaction_disposition: Some(crate::transport::TransactionDisposition::Keep), fields: Default::default() };
    sink.failed(super::DbError::Graphql { messages: vec!["missing".into()], extensions: Some(extensions.clone()) });
    let Ok(SubscriptionEvent::Failed(failure)) = receiver.try_recv().map(|(event, _)| event) else { panic!("missing failure") };
    assert!(matches!(failure.error, crate::transport::TransportError::Graphql { extensions: Some(found), .. } if found == extensions));
    sink.event(r#"{"errors":[{"message":"missing","extensions":{"code":"NOT_FOUND","transactionDisposition":"KEEP"}}]}"#.into()).unwrap();
    let Ok(SubscriptionEvent::Failed(failure)) = receiver.try_recv().map(|(event, _)| event) else { panic!("missing frame failure") };
    assert!(matches!(failure.error, crate::transport::TransportError::Graphql { extensions: Some(found), .. } if found == extensions));
}

#[test]
fn subscription_sink_wakes_its_receiver_with_the_sender_lock_free() {
    use crate::test_wait::{mutex_free, ProbeWaker};
    use std::task::{Context, Waker};
    let (sink, mut receiver) = sink();
    let sink = Arc::new(sink);
    let probe = ProbeWaker::new({
        let sink = sink.clone();
        move || mutex_free(&sink.sender)
    });
    let waker = Waker::from(probe.clone());
    let mut context = Context::from_waker(&waker);
    let mut next = Box::pin(crate::transport::SubscriptionReceiver::recv(&mut receiver));
    assert!(next.as_mut().poll(&mut context).is_pending());
    sink.ready();
    assert_eq!((probe.wakes(), probe.violations()), (1, 0), "an event wakes the receiver with the sender lock free");
    assert!(matches!(next.as_mut().poll(&mut context), std::task::Poll::Ready(Some((SubscriptionEvent::Ready, _)))));
    drop(next);
    let mut next = Box::pin(crate::transport::SubscriptionReceiver::recv(&mut receiver));
    assert!(next.as_mut().poll(&mut context).is_pending());
    sink.closed();
    assert_eq!((probe.wakes(), probe.violations()), (2, 0), "closing wakes the receiver with the sender lock free");
    assert!(matches!(next.as_mut().poll(&mut context), std::task::Poll::Ready(None)), "the receiver sees the end once");
    sink.ready();
    assert_eq!(probe.wakes(), 2, "an event after the close reaches nobody");

    let (sender_owner, mut receiver) = self::sink();
    let probe = ProbeWaker::new(|| true);
    let waker = Waker::from(probe.clone());
    let mut next = Box::pin(crate::transport::SubscriptionReceiver::recv(&mut receiver));
    assert!(next.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    drop(sender_owner);
    assert_eq!((probe.wakes(), probe.violations()), (1, 0), "sender drop wakes without tracked guards");
    assert!(matches!(next.as_mut().poll(&mut Context::from_waker(&waker)), std::task::Poll::Ready(None)));
}

struct FakeSocket;

struct FloodSubscription;

#[async_trait::async_trait]
impl super::Transport for FloodSubscription {
    async fn open_cable(&self, _: Arc<super::CableLease>, _url: String) -> Result<(), super::DbError> { unreachable!() }
    async fn cable_send(&self, _: String, _: String) -> Result<(), super::DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<super::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) { unreachable!() }
    async fn query(&self, _: super::TransportRequest) -> Result<String, super::DbError> { unreachable!() }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, super::DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, sink: Arc<super::SubscriptionSink>) -> Result<(), super::DbError> {
        sink.opened("flood".into())?;
        sink.ready();
        for number in 1..=300 { sink.event(format!(r#"{{"data":{{"value":{number}}}}}"#))?; }
        sink.failed(super::DbError::Network { detail: "closed".into() });
        Ok(())
    }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, super::DbError> { unreachable!() }
}

#[tokio::test]
async fn subscription_overflow_reports_the_lost_range_before_failure() {
    use crate::transport::Transport;
    let transport = super::ForeignTransport(Arc::new(FloodSubscription));
    let mut subscription = within(transport.subscribe(request())).await.unwrap();
    assert_eq!(within(subscription.next()).await, Some(SubscriptionEvent::Ready));
    for number in 1..=255 {
        let Some(SubscriptionEvent::Next(response)) = within(subscription.next()).await else { panic!("missing event {number}") };
        assert_eq!(response.data.unwrap()["value"], number);
    }
    assert_eq!(within(subscription.next()).await, Some(SubscriptionEvent::Lost { first: 256, last: 300 }));
    assert!(matches!(within(subscription.next()).await, Some(SubscriptionEvent::Failed(_))));
}

#[async_trait::async_trait]
impl super::Transport for FakeSocket {
    async fn open_cable(&self, lease: Arc<super::CableLease>, _url: String) -> Result<(), super::DbError> { lease.opened("socket-1".into()) }
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
    async fn subscribe(&self, _: super::TransportRequest, _: std::sync::Arc<super::SubscriptionSink>) -> Result<(), super::DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) { unreachable!() }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: std::sync::Arc<super::ByteProgress>) -> Result<super::PutReply, super::DbError> { unreachable!() }
}

#[tokio::test]
async fn raw_socket_bridge_preserves_ciphertext_and_close_codes() {
    use crate::transport::Transport;
    let transport = super::ForeignTransport(std::sync::Arc::new(FakeSocket));
    let socket = transport.open_cable("wss://test.invalid/cable").await.unwrap();
    socket.send("ciphertext".into()).await.unwrap();
    assert!(matches!(within(socket.next()).await, Some(crate::transport::SocketEvent::Closed { code: 4503, reason }) if reason == "unavailable"));
    socket.close().await;
}

#[test]
fn a_frame_without_data_fails_the_subscription_as_internal_and_an_enveloped_frame_passes() {
    let (sink, mut receiver) = sink();

    let rejected = sink.event(r#"{"messageCreated":{"message":{"id":"9"}}}"#.to_string());
    let failure = match receiver.try_recv().map(|(event, _)| event) {
        Ok(SubscriptionEvent::Failed(failure)) => Some(failure),
        _ => None,
    };
    assert!(rejected.is_err());
    assert_eq!(failure.map(|failure| failure.kind), Some(SubscriptionFailureKind::Internal));

    sink.event(r#"{"data":{"messageCreated":{"message":{"id":"9"}}}}"#.to_string()).unwrap();
    assert!(matches!(receiver.try_recv().map(|(event, _)| event), Ok(SubscriptionEvent::Next(_))));
}

#[test]
fn inv_panic_01_a_subscription_sink_survives_a_poisoned_lock() {
    let (sink, mut receiver) = sink();
    crate::store::spec_fixtures::poison(&sink.sender);
    sink.ready();
    assert!(matches!(receiver.try_recv().map(|(event, _)| event), Ok(SubscriptionEvent::Ready)));
    sink.closed();
}

struct EarlyFailure;

#[async_trait::async_trait]
impl super::Transport for EarlyFailure {
    async fn open_cable(&self, _: Arc<super::CableLease>, _url: String) -> Result<(), super::DbError> { unreachable!() }
    async fn cable_send(&self, _: String, _: String) -> Result<(), super::DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<super::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) { unreachable!() }
    async fn query(&self, _: super::TransportRequest) -> Result<String, super::DbError> { unreachable!() }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, super::DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, sink: Arc<super::SubscriptionSink>) -> Result<(), super::DbError> {
        sink.opened("early".into())?;
        sink.failed(super::DbError::Session { code: crate::transport::SessionCode::Unauthenticated, detail: "refused".into() });
        Ok(())
    }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, super::DbError> { unreachable!() }
}

#[tokio::test]
async fn a_failure_the_platform_reports_before_subscribe_returns_reaches_the_subscription() {
    use crate::transport::Transport;
    let transport = super::ForeignTransport(Arc::new(EarlyFailure));
    let mut subscription = transport.subscribe(crate::transport::GraphQLRequest::new("subscription { x }", "X", serde_json::json!({}))).await.unwrap();
    let Some(SubscriptionEvent::Failed(failure)) = within(subscription.next()).await else { panic!("the early failure was lost") };
    assert!(failure.is_terminal());
}

type Script<A> = Box<dyn FnOnce(Arc<A>) -> Pin<Box<dyn Future<Output = Result<(), super::DbError>> + Send>> + Send>;

struct Ordered {
    subscribe: Mutex<Option<Script<super::SubscriptionSink>>>,
    open: Mutex<Option<Script<super::CableLease>>>,
    events: Mutex<std::sync::mpsc::Sender<String>>,
    closes: std::sync::atomic::AtomicUsize,
}

impl Ordered {
    fn say(&self, event: String) { let _ = crate::lock::lock(&self.events).send(event); }
}

#[async_trait::async_trait]
impl super::Transport for Ordered {
    async fn open_cable(&self, lease: Arc<super::CableLease>, _url: String) -> Result<(), super::DbError> {
        let script = crate::lock::lock(&self.open).take().expect("open script");
        script(lease).await
    }
    async fn cable_send(&self, _: String, _: String) -> Result<(), super::DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<super::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, token: String) {
        if self.closes.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 && token == "stuck" {
            self.say(format!("close-started:{token}"));
            std::future::pending::<()>().await;
        }
        self.say(format!("close:{token}"));
    }
    async fn query(&self, _: super::TransportRequest) -> Result<String, super::DbError> { unreachable!() }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, super::DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, sink: Arc<super::SubscriptionSink>) -> Result<(), super::DbError> {
        let script = crate::lock::lock(&self.subscribe).take().expect("subscribe script");
        script(sink).await
    }
    fn unsubscribe(&self, token: String) { self.say(format!("unsubscribe:{token}")); }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, super::DbError> { unreachable!() }
}

fn boxed(work: impl Future<Output = Result<(), super::DbError>> + Send + 'static) -> Pin<Box<dyn Future<Output = Result<(), super::DbError>> + Send>> { Box::pin(work) }

fn ordered(subscribe: Option<Script<super::SubscriptionSink>>, open: Option<Script<super::CableLease>>) -> (super::ForeignTransport, std::sync::mpsc::Receiver<String>) {
    let (events, heard) = std::sync::mpsc::channel();
    let transport = Ordered { subscribe: Mutex::new(subscribe), open: Mutex::new(open), events: Mutex::new(events), closes: std::sync::atomic::AtomicUsize::new(0) };
    (super::ForeignTransport(Arc::new(transport)), heard)
}

fn heard(events: &std::sync::mpsc::Receiver<String>) -> String { events.recv_timeout(Duration::from_secs(10)).expect("the transport was told nothing") }

fn request() -> crate::transport::GraphQLRequest { crate::transport::GraphQLRequest::new("subscription s { s }", "s", serde_json::json!({})) }

fn pending<F: Future>(call: &mut Pin<Box<F>>) -> bool {
    call.as_mut().poll(&mut std::task::Context::from_waker(std::task::Waker::noop())).is_pending()
}

fn opened_then_stuck() -> Script<super::SubscriptionSink> {
    Box::new(|sink| boxed(async move { sink.opened("t1".into())?; std::future::pending::<()>().await; Ok(()) }))
}

fn opened_after_a_gate() -> (Script<super::SubscriptionSink>, std::sync::mpsc::Sender<()>) {
    let (go, gate) = std::sync::mpsc::channel::<()>();
    let script: Script<super::SubscriptionSink> = Box::new(move |sink| {
        std::thread::spawn(move || { recv_within(&gate); sink.opened("t1".into()).unwrap(); });
        boxed(async { std::future::pending::<()>().await; Ok(()) })
    });
    (script, go)
}

#[test]
fn a_subscription_token_opened_before_the_call_is_dropped_is_unsubscribed() {
    use crate::transport::Transport;
    let (transport, events) = ordered(Some(opened_then_stuck()), None);
    let mut call = Box::pin(transport.subscribe(request()));
    assert!(pending(&mut call));
    drop(call);
    assert_eq!(heard(&events), "unsubscribe:t1");
}

#[test]
fn a_subscription_token_opened_after_the_call_was_dropped_is_unsubscribed() {
    use crate::transport::Transport;
    let (script, go) = opened_after_a_gate();
    let (transport, events) = ordered(Some(script), None);
    let mut call = Box::pin(transport.subscribe(request()));
    assert!(pending(&mut call));
    drop(call);
    go.send(()).unwrap();
    assert_eq!(heard(&events), "unsubscribe:t1");
}

#[tokio::test]
async fn a_cancelled_subscription_unsubscribes_its_token() {
    use crate::transport::Transport;
    let (transport, events) = ordered(Some(Box::new(|sink| boxed(async move { sink.opened("t1".into())?; sink.ready(); Ok(()) }))), None);
    let subscription = transport.subscribe(request()).await.unwrap();
    drop(subscription);
    assert_eq!(heard(&events), "unsubscribe:t1");
}

#[tokio::test]
async fn a_call_that_fails_after_opened_still_unsubscribes_its_token() {
    use crate::transport::Transport;
    let (transport, events) = ordered(Some(Box::new(|sink| boxed(async move { sink.opened("t1".into())?; Err(super::DbError::Network { detail: "refused".into() }) }))), None);
    assert!(transport.subscribe(request()).await.is_err());
    assert_eq!(heard(&events), "unsubscribe:t1");
}

#[tokio::test]
async fn a_call_that_finishes_without_opened_fails_and_unsubscribes_a_late_token() {
    use crate::transport::Transport;
    let (kept, sinks) = std::sync::mpsc::channel();
    let (transport, events) = ordered(Some(Box::new(move |sink| boxed(async move { kept.send(sink).unwrap(); Ok(()) }))), None);
    assert!(transport.subscribe(request()).await.is_err());
    recv_within(&sinks).opened("late".into()).unwrap();
    assert_eq!(heard(&events), "unsubscribe:late");
}

#[test]
fn a_cable_token_opened_before_the_call_is_dropped_is_closed() {
    use crate::transport::Transport;
    let script: Script<super::CableLease> = Box::new(|lease| boxed(async move { lease.opened("c1".into())?; std::future::pending::<()>().await; Ok(()) }));
    let (transport, events) = ordered(None, Some(script));
    let mut call = Box::pin(transport.open_cable("wss://test.invalid/cable"));
    assert!(pending(&mut call));
    drop(call);
    assert_eq!(heard(&events), "close:c1");
}

#[test]
fn a_cable_token_opened_after_the_call_was_dropped_is_closed() {
    use crate::transport::Transport;
    let (go, gate) = std::sync::mpsc::channel::<()>();
    let script: Script<super::CableLease> = Box::new(move |lease| {
        std::thread::spawn(move || { recv_within(&gate); lease.opened("c1".into()).unwrap(); });
        boxed(async { std::future::pending::<()>().await; Ok(()) })
    });
    let (transport, events) = ordered(None, Some(script));
    let mut call = Box::pin(transport.open_cable("wss://test.invalid/cable"));
    assert!(pending(&mut call));
    drop(call);
    go.send(()).unwrap();
    assert_eq!(heard(&events), "close:c1");
}

#[tokio::test]
async fn a_cable_socket_dropped_without_close_closes_its_token() {
    use crate::transport::Transport;
    let (transport, events) = ordered(None, Some(Box::new(|lease| boxed(async move { lease.opened("c1".into()) }))));
    let socket = transport.open_cable("wss://test.invalid/cable").await.unwrap();
    drop(socket);
    assert_eq!(heard(&events), "close:c1");
}

#[tokio::test]
async fn a_cancelled_close_leaves_the_token_to_the_dropped_socket() {
    use crate::transport::Transport;
    let (transport, events) = ordered(None, Some(Box::new(|lease| boxed(async move { lease.opened("stuck".into()) }))));
    let socket = transport.open_cable("wss://test.invalid/cable").await.unwrap();
    let mut closing = Box::pin(socket.close());
    assert!(pending(&mut closing));
    assert_eq!(heard(&events), "close-started:stuck");
    drop(closing);
    drop(socket);
    assert_eq!(heard(&events), "close:stuck");
}
