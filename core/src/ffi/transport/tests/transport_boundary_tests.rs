//! D-492 and D-493: the port boundary rotates the connections after a failure of the network class and ends every exchange at the core bound.
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::ffi::cable::{CableEvents, CableOutcome};
use crate::ffi::{ByteProgress, Transport};
use crate::runtime::ManualClock;
use crate::test_wait::within;
use crate::transport::{GraphQLRequest, HttpRequest, PutFile, RawFailure, RawFailureKind, RawOutcome, TransferIdentity, TransportError, Upstream, WireIds};
use super::ForeignTransport;

/// A port that answers from a script. `None` in the script is a call that never answers. It models its connections: a request takes the current one, a rotation replaces it.
struct Scripted {
    log: Mutex<Vec<String>>,
    script: Mutex<VecDeque<Option<RawOutcome>>>,
    cable: Mutex<Option<CableOutcome>>,
    entered: tokio::sync::Notify,
    dropped: AtomicBool,
    connection: AtomicU64,
}

struct Dropped<'a>(&'a AtomicBool);

impl Drop for Dropped<'_> {
    fn drop(&mut self) { self.0.store(true, Ordering::SeqCst); }
}

impl Scripted {
    fn new(script: Vec<Option<RawOutcome>>) -> Arc<Self> {
        Arc::new(Self { log: Mutex::default(), script: Mutex::new(script.into()), cable: Mutex::default(), entered: tokio::sync::Notify::new(), dropped: AtomicBool::new(false), connection: AtomicU64::new(0) })
    }

    fn log(&self) -> Vec<String> { crate::lock::lock(&self.log).clone() }
}

#[async_trait::async_trait]
impl Transport for Scripted {
    fn app_headers(&self) -> HashMap<String, String> { HashMap::new() }
    fn rotate_connections(&self) {
        self.connection.fetch_add(1, Ordering::SeqCst);
        crate::lock::lock(&self.log).push("rotate".into());
    }
    async fn send(&self, _: HttpRequest) -> RawOutcome {
        crate::lock::lock(&self.log).push(format!("send on {}", self.connection.load(Ordering::SeqCst)));
        let next = crate::lock::lock(&self.script).pop_front().expect("a scripted outcome");
        self.entered.notify_one();
        match next {
            Some(outcome) => outcome,
            None => {
                let _dropped = Dropped(&self.dropped);
                std::future::pending::<()>().await;
                unreachable!("a call that never answers ends only when the core drops it")
            }
        }
    }
    async fn open_cable(&self, _: String, _: HashMap<String, String>, _: u64, _: Arc<dyn CableEvents>) -> CableOutcome {
        crate::lock::lock(&self.cable).take().expect("a scripted cable outcome")
    }
    async fn put(&self, _: crate::ffi::PutTarget, _: PutFile, _: TransferIdentity, _: Arc<ByteProgress>) -> RawOutcome { unreachable!() }
}

fn clocked(port: &Arc<Scripted>) -> (ForeignTransport, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(Duration::from_secs(1_000)));
    (ForeignTransport::plain(port.clone(), Some("https://graphql.test/graphql".into()), clock.clone()).unwrap(), clock)
}

fn request() -> GraphQLRequest { GraphQLRequest::new("query Q { x }", "Q", serde_json::json!({})).by(WireIds::bootstrap()) }

const ANSWER: &str = r#"{"data":{"x":1}}"#;

fn failed(kind: RawFailureKind) -> Option<RawOutcome> { Some(RawOutcome::failed(kind, "scripted")) }

/// D-492: Timeout, Io and Unreachable rotate the connections once, before the outcome reaches the author: on the query and on the cable open.
#[tokio::test]
async fn transport_network_failure_rotates_connections() {
    for kind in [RawFailureKind::Timeout, RawFailureKind::Io, RawFailureKind::Unreachable] {
        let port = Scripted::new(vec![failed(kind)]);
        let (transport, _) = clocked(&port);
        assert!(matches!(within(transport.query(request())).await, Err(TransportError::Network(_))), "{kind:?}");
        assert_eq!(port.log(), ["send on 0", "rotate"], "{kind:?}");
        let port = Scripted::new(vec![]);
        *crate::lock::lock(&port.cable) = Some(CableOutcome::Failed { failure: RawFailure::of_kind(kind, "scripted"), handshake_status: None });
        let (transport, _) = clocked(&port);
        assert!(within(transport.open_cable("wss://test.invalid/cable")).await.is_err(), "{kind:?}");
        assert_eq!(port.log(), ["rotate"], "{kind:?}");
    }
}

/// D-492: a cancelled call, a trust failure, a size limit and any HTTP answer leave the connections alone.
#[tokio::test]
async fn transport_answer_does_not_rotate() {
    let outcomes = vec![RawOutcome::response(200, ANSWER), RawOutcome::response(404, "no"), RawOutcome::response(503, "down"), RawOutcome::failed(RawFailureKind::Cancelled, "x"), RawOutcome::failed(RawFailureKind::Tls, "x"), RawOutcome::failed(RawFailureKind::ResponseLimit, "x")];
    let count = outcomes.len();
    let port = Scripted::new(outcomes.into_iter().map(Some).collect());
    let (transport, _) = clocked(&port);
    for _ in 0..count { let _ = within(transport.query(request())).await; }
    assert!(!port.log().iter().any(|line| line == "rotate"), "{:?}", port.log());
    let port = Scripted::new(vec![]);
    *crate::lock::lock(&port.cable) = Some(CableOutcome::Failed { failure: RawFailure::of_kind(RawFailureKind::Io, "refused"), handshake_status: Some(403) });
    let (transport, _) = clocked(&port);
    assert!(within(transport.open_cable("wss://test.invalid/cable")).await.is_err());
    assert!(port.log().is_empty(), "an HTTP answer of the upgrade does not rotate");
}

/// D-493: a call that never answers ends with `Timeout` once, at 30 s of the core clock; the port future is dropped.
#[tokio::test]
async fn transport_exchange_total_deadline() {
    let port = Scripted::new(vec![None]);
    let (transport, clock) = clocked(&port);
    let transport = Arc::new(transport);
    let call = { let transport = transport.clone(); tokio::spawn(async move { transport.query(request()).await }) };
    within(port.entered.notified()).await;
    clock.advance(Duration::from_millis(29_999));
    assert!(!port.dropped.load(Ordering::SeqCst), "the platform call lives until the bound");
    clock.advance(Duration::from_millis(1));
    assert!(matches!(within(call).await.unwrap(), Err(TransportError::Network(_))));
    assert!(port.dropped.load(Ordering::SeqCst), "the platform task is cancelled");
    assert_eq!(port.log(), ["send on 0", "rotate"], "the bound is a Timeout: it rotates like one");
}

/// P-NET-15: after a failure of the network class no request gets the connection of before the failure.
#[tokio::test]
async fn transport_prop_no_reuse_after_failure() {
    let port = Scripted::new(vec![failed(RawFailureKind::Io), Some(RawOutcome::response(200, ANSWER)), failed(RawFailureKind::Timeout), Some(RawOutcome::response(200, ANSWER))]);
    let (transport, _) = clocked(&port);
    for _ in 0..4 { let _ = within(transport.query(request())).await; }
    assert_eq!(port.log(), ["send on 0", "rotate", "send on 1", "send on 1", "rotate", "send on 2"]);
}

/// P-NET-16: every call of the HTTP port ends within 30 s of the core clock, whatever the platform does.
#[tokio::test]
async fn transport_prop_exchange_bounded() {
    let port = Scripted::new(vec![None, None]);
    let (transport, clock) = clocked(&port);
    let transport = Arc::new(transport);
    let query = { let transport = transport.clone(); tokio::spawn(async move { transport.query(request()).await }) };
    within(port.entered.notified()).await;
    let mutation = { let transport = transport.clone(); tokio::spawn(async move { transport.mutate(request()).await }) };
    within(port.entered.notified()).await;
    clock.advance(Duration::from_millis(crate::transport::REQUEST_TIMEOUT_MS));
    assert!(within(query).await.unwrap().is_err());
    assert!(matches!(within(mutation).await.unwrap(), Err(TransportError::Interrupted(_))), "a mutation that may have reached the server stays Interrupted");
}
