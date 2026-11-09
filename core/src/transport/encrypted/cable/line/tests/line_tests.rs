//! P-FEED-25 and P-FEED-27 at the cable: a connection the core gave up is closed by the call that opened it, once, wherever the give-up falls.
//! feed_open_after_abandon_closes: the open outcome that comes after the give-up is awaited, its socket gets 1 `close`, the call fails with `Interrupted`.
//! feed_open_cable_close_orders: give-up during the handshake, before the call starts, or a handshake the peer ends: 1 `close`, 0 `cancel`, no task apart from the call.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Notify;

use super::super::{CableSocket, SocketEvent};
use super::*;
use crate::test_wait::within;
use crate::transport::{GraphQLRequest, PutReply, PutRequest, Response};

#[derive(Default)]
struct Socket {
    closes: AtomicUsize,
    cancels: AtomicUsize,
    waiting: Notify,
    peer_ends: bool,
    raw_close: bool,
}

#[async_trait]
impl CableSocket for Socket {
    async fn send(&self, _: String) -> Result<(), TransportError> { Ok(()) }

    async fn next(&self) -> Option<SocketEvent> {
        self.waiting.notify_one();
        if self.raw_close { return Some(SocketEvent::Closed { code: Some(4401), reason: Some("expired".into()), failure: Some(crate::transport::RawFailure::of_kind(crate::transport::RawFailureKind::Io, "lost")) }); }
        if self.peer_ends { return None; }
        std::future::pending().await
    }

    async fn close(&self) { self.closes.fetch_add(1, Ordering::SeqCst); }

    async fn cancel(&self) { self.cancels.fetch_add(1, Ordering::SeqCst); }
}

#[tokio::test]
async fn feed_handshake_close_keeps_raw_event() {
    let (_, lines) = lines(Socket { raw_close: true, ..Socket::default() }, false);
    let outcome = within(lines.connect(&CancelToken::new())).await;
    let Err(CableConnectError { closed: Some((code, reason, failure)), .. }) = outcome else { panic!("missing close event") };
    assert_eq!((code, reason.as_deref(), failure.map(|failure| failure.message)), (Some(4401), Some("expired"), Some("lost".into())));
}

struct Platform {
    socket: Arc<Socket>,
    entered: Notify,
    release: Option<Notify>,
}

#[async_trait]
impl Upstream for Platform {
    async fn open_cable(&self, _: &str) -> Result<Arc<dyn CableSocket>, TransportError> {
        self.entered.notify_one();
        if let Some(release) = &self.release { within(release.notified()).await; }
        Ok(self.socket.clone())
    }

    async fn query(&self, _: GraphQLRequest) -> Result<Response, TransportError> { unreachable!() }

    async fn mutate(&self, _: GraphQLRequest) -> Result<Response, TransportError> { unreachable!() }

    async fn put(&self, _: PutRequest) -> Result<PutReply, TransportError> { unreachable!() }
}

fn lines(socket: Socket, held: bool) -> (Arc<Platform>, EncryptedLines) {
    let platform = Arc::new(Platform { socket: Arc::new(socket), entered: Notify::new(), release: held.then(Notify::new) });
    let config = crate::transport::encrypted::tests::protocol_tests::config();
    (platform.clone(), EncryptedLines::new(platform, config, Arc::new(|| Ok("credential".into()))))
}

fn counts(platform: &Platform) -> (usize, usize) { (platform.socket.closes.load(Ordering::SeqCst), platform.socket.cancels.load(Ordering::SeqCst)) }

#[tokio::test]
async fn feed_open_after_abandon_closes() {
    let (platform, lines) = lines(Socket::default(), true);
    let cancel = CancelToken::new();
    let connecting = lines.connect(&cancel);
    tokio::pin!(connecting);
    let early = tokio::select! { outcome = &mut connecting => Some(outcome), () = platform.entered.notified() => None };
    assert!(early.is_none(), "the call waits for the open outcome");
    cancel.cancel();
    assert_eq!(counts(&platform), (0, 0), "the socket does not exist before the open outcome");
    platform.release.as_ref().expect("held open").notify_one();
    let outcome = within(&mut connecting).await;
    assert!(matches!(outcome, Err(CableConnectError { error: TransportError::Interrupted(_), .. })));
    assert_eq!(counts(&platform), (1, 0), "the socket that opened after the give-up is closed once and never cancelled");
}

#[tokio::test]
async fn feed_open_cable_close_orders() {
    for order in ["during the handshake", "before the call", "peer ends the handshake"] {
        let (platform, lines) = lines(Socket { peer_ends: order == "peer ends the handshake", ..Socket::default() }, false);
        let cancel = CancelToken::new();
        if order == "before the call" { cancel.cancel(); }
        let connecting = lines.connect(&cancel);
        tokio::pin!(connecting);
        if order == "during the handshake" {
            let early = tokio::select! { outcome = &mut connecting => Some(outcome), () = platform.socket.waiting.notified() => None };
            assert!(early.is_none(), "{order}: the handshake waits for the server");
            cancel.cancel();
        }
        let outcome = within(&mut connecting).await;
        let refused = if order == "peer ends the handshake" { matches!(outcome, Err(CableConnectError { error: TransportError::Network(_), .. })) } else { matches!(outcome, Err(CableConnectError { error: TransportError::Interrupted(_), .. })) };
        assert!(refused, "{order}: the connection did not open with the cause of the order");
        assert_eq!(counts(&platform), (1, 0), "{order}: 1 close, 0 cancel");
    }
}
