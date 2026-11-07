use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::test_wait::{recv_within, within};

struct FakeSocket;

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
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: std::sync::Arc<super::ByteProgress>) -> Result<crate::transport::PutReply, super::DbError> { unreachable!() }
}

#[tokio::test]
async fn raw_socket_bridge_preserves_ciphertext_and_close_codes() {
    use crate::transport::Upstream;
    let transport = super::ForeignTransport(std::sync::Arc::new(FakeSocket));
    let socket = transport.open_cable("wss://test.invalid/cable").await.unwrap();
    socket.send("ciphertext".into()).await.unwrap();
    assert!(matches!(within(socket.next()).await, Some(crate::transport::SocketEvent::Closed { code: 4503, reason }) if reason == "unavailable"));
    socket.close().await;
}

type Script<A> = Box<dyn FnOnce(Arc<A>) -> Pin<Box<dyn Future<Output = Result<(), super::DbError>> + Send>> + Send>;

struct Ordered {
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
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<crate::transport::PutReply, super::DbError> { unreachable!() }
}

fn boxed(work: impl Future<Output = Result<(), super::DbError>> + Send + 'static) -> Pin<Box<dyn Future<Output = Result<(), super::DbError>> + Send>> { Box::pin(work) }

fn ordered(open: Option<Script<super::CableLease>>) -> (super::ForeignTransport, std::sync::mpsc::Receiver<String>) {
    let (events, heard) = std::sync::mpsc::channel();
    let transport = Ordered { open: Mutex::new(open), events: Mutex::new(events), closes: std::sync::atomic::AtomicUsize::new(0) };
    (super::ForeignTransport(Arc::new(transport)), heard)
}

fn heard(events: &std::sync::mpsc::Receiver<String>) -> String { events.recv_timeout(Duration::from_secs(10)).expect("the transport was told nothing") }

fn pending<F: Future>(call: &mut Pin<Box<F>>) -> bool {
    call.as_mut().poll(&mut std::task::Context::from_waker(std::task::Waker::noop())).is_pending()
}

#[test]
fn a_cable_token_opened_before_the_call_is_dropped_is_closed() {
    use crate::transport::Upstream;
    let script: Script<super::CableLease> = Box::new(|lease| boxed(async move { lease.opened("c1".into())?; std::future::pending::<()>().await; Ok(()) }));
    let (transport, events) = ordered(Some(script));
    let mut call = Box::pin(transport.open_cable("wss://test.invalid/cable"));
    assert!(pending(&mut call));
    drop(call);
    assert_eq!(heard(&events), "close:c1");
}

#[test]
fn a_cable_token_opened_after_the_call_was_dropped_is_closed() {
    use crate::transport::Upstream;
    let (go, gate) = std::sync::mpsc::channel::<()>();
    let script: Script<super::CableLease> = Box::new(move |lease| {
        std::thread::spawn(move || { recv_within(&gate); lease.opened("c1".into()).unwrap(); });
        boxed(async { std::future::pending::<()>().await; Ok(()) })
    });
    let (transport, events) = ordered(Some(script));
    let mut call = Box::pin(transport.open_cable("wss://test.invalid/cable"));
    assert!(pending(&mut call));
    drop(call);
    go.send(()).unwrap();
    assert_eq!(heard(&events), "close:c1");
}

#[tokio::test]
async fn a_cable_socket_dropped_without_close_closes_its_token() {
    use crate::transport::Upstream;
    let (transport, events) = ordered(Some(Box::new(|lease| boxed(async move { lease.opened("c1".into()) }))));
    let socket = transport.open_cable("wss://test.invalid/cable").await.unwrap();
    drop(socket);
    assert_eq!(heard(&events), "close:c1");
}

#[tokio::test]
async fn a_cancelled_close_leaves_the_token_to_the_dropped_socket() {
    use crate::transport::Upstream;
    let (transport, events) = ordered(Some(Box::new(|lease| boxed(async move { lease.opened("stuck".into()) }))));
    let socket = transport.open_cable("wss://test.invalid/cable").await.unwrap();
    let mut closing = Box::pin(socket.close());
    assert!(pending(&mut closing));
    assert_eq!(heard(&events), "close-started:stuck");
    drop(closing);
    drop(socket);
    assert_eq!(heard(&events), "close:stuck");
}
