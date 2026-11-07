use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::test_wait::{within, yield_until};
use crate::transport::{SocketEvent, Upstream};
use crate::ffi::cable::{PlatformSocket, CableEvents, CableOutcome, MAILBOX_FRAMES};
use super::ForeignTransport;
use crate::ffi::{ByteProgress, Transport};
use crate::transport::{HttpRequest, PutFile, RawFailure, RawOutcome, TransferIdentity};

#[derive(Default)]
struct Link {
    events: Mutex<Option<Arc<dyn CableEvents>>>,
    sent: Mutex<Vec<String>>,
    closes: Mutex<Vec<(u16, String)>>,
    headers: Mutex<HashMap<String, String>>,
    cancels: Mutex<u32>,
}

impl Link {
    fn events(&self) -> Arc<dyn CableEvents> { crate::lock::lock(&self.events).clone().expect("the cable is open") }
}

impl PlatformSocket for Link {
    fn send(&self, text: String) -> bool { crate::lock::lock(&self.sent).push(text); true }
    fn close(&self, code: u16, reason: String) { crate::lock::lock(&self.closes).push((code, reason)); }
    fn cancel(&self) { *crate::lock::lock(&self.cancels) += 1; }
}

struct Port {
    link: Arc<Link>,
    refusal: Option<(RawFailure, Option<u16>)>,
    put: RawOutcome,
}

#[async_trait::async_trait]
impl Transport for Port {
    fn app_headers(&self) -> HashMap<String, String> { HashMap::from([("X-App".into(), "yes".into())]) }
    async fn send(&self, _: HttpRequest) -> RawOutcome { unreachable!() }
    async fn open_cable(&self, _: String, headers: HashMap<String, String>, _: u64, events: Arc<dyn CableEvents>) -> CableOutcome {
        *crate::lock::lock(&self.link.headers) = headers;
        if let Some((failure, handshake_status)) = &self.refusal { return CableOutcome::Failed { failure: failure.clone(), handshake_status: *handshake_status }; }
        *crate::lock::lock(&self.link.events) = Some(events);
        CableOutcome::Open { connection: self.link.clone() }
    }
    async fn put(&self, _: crate::ffi::PutTarget, _: PutFile, _: TransferIdentity, _: Arc<ByteProgress>) -> RawOutcome { self.put.clone() }
}

fn opened(refusal: Option<(RawFailure, Option<u16>)>) -> (ForeignTransport, Arc<Link>) {
    let link = Arc::new(Link::default());
    (ForeignTransport::beside_session(Arc::new(Port { link: link.clone(), refusal, put: RawOutcome::response(200, "") }), Arc::new(crate::runtime::SystemClock)), link)
}

const URL: &str = "wss://test.invalid/cable";

#[tokio::test]
async fn raw_socket_bridge_preserves_ciphertext_and_close_codes() {
    let (transport, link) = opened(None);
    let socket = transport.open_cable(URL).await.unwrap();
    link.events().frame("hello".into());
    socket.send("ciphertext".into()).await.unwrap();
    assert_eq!(*crate::lock::lock(&link.sent), ["ciphertext"]);
    assert!(matches!(within(socket.next()).await, Some(SocketEvent::Text(text)) if text == "hello"));
    link.events().ended(Some(4503), Some("unavailable".into()), None);
    assert!(matches!(within(socket.next()).await, Some(SocketEvent::Closed { code: 4503, reason }) if reason == "unavailable"));
    assert!(within(socket.next()).await.is_none());
}

#[tokio::test]
async fn a_binary_frame_ends_the_cable_with_the_unsupported_data_code() {
    let (transport, link) = opened(None);
    let socket = transport.open_cable(URL).await.unwrap();
    link.events().binary(vec![1, 2]);
    assert!(matches!(within(socket.next()).await, Some(SocketEvent::Closed { code: 1003, .. })));
    assert_eq!(crate::lock::lock(&link.closes).iter().map(|(code, _)| *code).collect::<Vec<_>>(), [1003]);
}

#[tokio::test]
async fn a_full_mailbox_keeps_its_frames_then_ends_the_cable() {
    let (transport, link) = opened(None);
    let socket = transport.open_cable(URL).await.unwrap();
    for number in 0..MAILBOX_FRAMES + 40 { link.events().frame(number.to_string()); }
    for number in 0..MAILBOX_FRAMES {
        assert!(matches!(within(socket.next()).await, Some(SocketEvent::Text(text)) if text == number.to_string()));
    }
    assert!(matches!(within(socket.next()).await, Some(SocketEvent::Closed { code: 1013, .. })));
    assert!(within(socket.next()).await.is_none());
    assert_eq!(crate::lock::lock(&link.closes).iter().map(|(code, _)| *code).collect::<Vec<_>>(), [1013]);
}

#[tokio::test]
async fn a_dropped_socket_closes_its_connection_once() {
    let (transport, link) = opened(None);
    let socket = transport.open_cable(URL).await.unwrap();
    socket.close().await;
    assert!(within(socket.next()).await.is_none());
    drop(socket);
    assert_eq!(crate::lock::lock(&link.closes).iter().map(|(code, _)| *code).collect::<Vec<_>>(), [1000]);
    let (transport, link) = opened(None);
    drop(transport.open_cable(URL).await.unwrap());
    assert_eq!(crate::lock::lock(&link.closes).iter().map(|(code, _)| *code).collect::<Vec<_>>(), [1001]);
}

#[tokio::test]
async fn a_closed_socket_is_cancelled_when_the_peer_does_not_answer_within_the_drain() {
    let clock = crate::runtime::ManualClock::default();
    let link = Arc::new(Link::default());
    let transport = ForeignTransport::beside_session(Arc::new(Port { link: link.clone(), refusal: None, put: RawOutcome::response(200, "") }), Arc::new(clock.clone()));
    let socket = transport.open_cable(URL).await.unwrap();
    socket.close().await;
    clock.advance(crate::owners::read::CLOSE_DRAIN / 2);
    assert_eq!(*crate::lock::lock(&link.cancels), 0);
    yield_until("the connection is cancelled after the drain", || { clock.advance(crate::owners::read::CLOSE_DRAIN); *crate::lock::lock(&link.cancels) == 1 }).await;
}

#[tokio::test]
async fn a_refused_upgrade_is_an_http_error_and_a_tls_refusal_a_security_error() {
    let refused = (RawFailure { domain: "java.net.ProtocolException".into(), code: None, message: "forbidden".into() }, Some(403));
    let error = opened(Some(refused)).0.open_cable(URL).await.err().unwrap();
    assert_eq!(error, crate::transport::TransportError::Http { status: 403, body: "forbidden".into() });
    let tls = (RawFailure { domain: "NSURLErrorDomain".into(), code: Some(-1202), message: "untrusted".into() }, None);
    let error = opened(Some(tls)).0.open_cable(URL).await.err().unwrap();
    assert_eq!(error, crate::transport::TransportError::Security { code: crate::transport::SecurityCode::TlsTrust, detail: "untrusted".into() });
}

#[tokio::test]
async fn an_ended_socket_is_an_abnormal_close_by_its_raw_error_or_no_error() {
    let (transport, link) = opened(None);
    let socket = transport.open_cable(URL).await.unwrap();
    link.events().ended(None, None, Some(RawFailure { domain: "NSURLErrorDomain".into(), code: Some(-1005), message: "lost".into() }));
    assert!(matches!(within(socket.next()).await, Some(SocketEvent::Closed { code: 1006, reason }) if reason == "lost"));
    let (transport, link) = opened(None);
    let socket = transport.open_cable(URL).await.unwrap();
    link.events().ended(None, None, None);
    assert!(matches!(within(socket.next()).await, Some(SocketEvent::Closed { code: 1006, .. })));
}

#[tokio::test]
async fn the_cable_carries_the_app_headers_and_the_library_user_agent() {
    let (transport, link) = opened(None);
    let _socket = transport.open_cable(URL).await.unwrap();
    let headers = crate::lock::lock(&link.headers).clone();
    assert_eq!(headers.get("X-App").map(String::as_str), Some("yes"));
    assert_eq!(headers.get("User-Agent").map(String::as_str), Some(crate::transport::user_agent(None).as_str()));
}

#[tokio::test]
async fn a_failed_put_is_interrupted_only_when_the_core_cancelled_it() {
    let failed = RawOutcome::failed(crate::transport::RawFailureKind::Io, "connection reset");
    let link = Arc::new(Link::default());
    let transport = ForeignTransport::beside_session(Arc::new(Port { link, refusal: None, put: failed }), Arc::new(crate::runtime::SystemClock));
    let request = |cancel: crate::transport::CancelToken| crate::transport::PutRequest {
        url: "https://files.test/blob".into(),
        headers: Vec::new(),
        file: PutFile { path: "/none".into(), offset: 0, length: 0 },
        identity: TransferIdentity { operation_id: 1, item_id: "a".into(), step: crate::transport::TransferStep::Put },
        cancel,
        sent: Arc::new(|_| {}),
    };
    assert_eq!(transport.put(request(crate::transport::CancelToken::new())).await.unwrap_err(), crate::transport::TransportError::Network("connection reset".into()));
    let cancelled = crate::transport::CancelToken::new();
    cancelled.cancel();
    assert!(matches!(transport.put(request(cancelled)).await.unwrap_err(), crate::transport::TransportError::Interrupted(_)));
}
