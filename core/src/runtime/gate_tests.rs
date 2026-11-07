use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use std::future::Future;
use std::pin::Pin;

use super::EpochGate;
use crate::transport::{
    CancelToken, GraphQLRequest, PutFile, PutRequest, PutReply, Response, Upstream, TransportError, TransferIdentity, TransferStep,
};

/// A transport that starts its request in the call itself, as a platform transport behind the bridge does: the counter moves before the future exists.
#[derive(Default)]
struct Counting(AtomicUsize);

type Sent<'a, T> = Pin<Box<dyn Future<Output = Result<T, TransportError>> + Send + 'a>>;

impl Counting {
    fn started<'a, T: Send + 'a>(&'a self) -> Sent<'a, T> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(TransportError::Network("counted".into())) })
    }
}

impl Upstream for Counting {
    fn query<'a, 'b>(&'a self, _: GraphQLRequest) -> Sent<'b, Response> where 'a: 'b, Self: 'b { self.started() }
    fn mutate<'a, 'b>(&'a self, _: GraphQLRequest) -> Sent<'b, Response> where 'a: 'b, Self: 'b { self.started() }
    fn put<'a, 'b>(&'a self, _: PutRequest) -> Sent<'b, PutReply> where 'a: 'b, Self: 'b { self.started() }
}

fn graphql() -> GraphQLRequest { GraphQLRequest::new("query Q { q }", "Q", serde_json::Value::Null) }

fn put() -> PutRequest {
    PutRequest {
        url: "http://upload.invalid/part".into(),
        headers: Vec::new(),
        file: PutFile { path: "unused".into(), offset: 0, length: 0 },
        identity: TransferIdentity { operation_id: 1, item_id: "item".into(), step: TransferStep::Part { number: 1 } },
        cancel: CancelToken::new(),
        sent: Arc::new(|_| {}),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn life_gate_closed_before_poll() {
    for kind in ["query", "mutate", "put"] {
        let gate = Arc::new(EpochGate::default());
        gate.open(1);
        let transport = Arc::new(Counting::default());
        gate.attach(transport.clone());
        let (started, running) = tokio::sync::oneshot::channel();
        let (ended, waiting) = tokio::sync::oneshot::channel::<()>();
        let task = crate::task::spawn_on(&tokio::runtime::Handle::current(), {
            let gate = gate.clone();
            async move {
                let _ = started.send(());
                let _ = waiting.await;
                match kind {
                    "query" => gate.query(Some(1), graphql()).await.map(drop),
                    "mutate" => gate.mutate(Some(1), graphql()).await.map(drop),
                    "put" => gate.put(Some(1), put()).await.map(drop),
                    other => unreachable!("{other}"),
                }
            }
        });
        crate::test_wait::within(running).await.unwrap();
        gate.close(1);
        let _ = ended.send(());
        let result = crate::test_wait::within(task).await.unwrap();
        assert!(matches!(result, Err(TransportError::Interrupted(_))), "{kind}: {result:?}");
        assert_eq!(transport.0.load(Ordering::SeqCst), 0, "{kind}: the transport was called");
    }
}

#[tokio::test]
async fn an_open_gate_of_the_epoch_sends_and_a_later_epoch_does_not() {
    let gate = EpochGate::default();
    gate.open(2);
    let transport = Arc::new(Counting::default());
    gate.attach(transport.clone());
    assert!(matches!(gate.query(Some(2), graphql()).await, Err(TransportError::Network(_))));
    assert!(matches!(gate.query(Some(1), graphql()).await, Err(TransportError::Interrupted(_))));
    assert_eq!(transport.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_cut_of_a_session_request_grows_with_a_reset_a_server_end_and_the_close_but_not_with_a_login() {
    let gate = EpochGate::default();
    let transport = Counting::default();
    let cut = gate.cut();
    gate.open(1);
    gate.end(1, crate::runtime::Cause::SessionStarted);
    assert!(matches!(super::send_in_cut(&gate, cut, || transport.query(graphql())).await, Ok(Err(TransportError::Network(_)))), "the login of the epoch does not cut the request");
    for (epoch, cause) in [(2, crate::runtime::Cause::SessionReset), (3, crate::runtime::Cause::ServerEnded)] {
        let before = gate.cut();
        gate.open(epoch);
        gate.end(epoch, cause);
        assert_eq!(gate.cut(), before + 1, "{cause:?}");
        assert!(matches!(super::send_in_cut(&gate, before, || transport.query(graphql())).await, Err(super::GateClosed)));
    }
    let before = gate.cut();
    gate.shutdown();
    assert!(matches!(super::send_in_cut(&gate, before, || transport.query(graphql())).await, Err(super::GateClosed)));
    assert_eq!(transport.0.load(Ordering::SeqCst), 1, "only the first request reached the transport");
}

/// Parks every call at the entry until released and records, per call, whether the gate still admitted the epoch when the call arrived.
#[derive(Default)]
struct Parking {
    entries: std::sync::Mutex<Vec<(&'static str, bool)>>,
    release: tokio::sync::Notify,
    gate: std::sync::OnceLock<std::sync::Weak<EpochGate>>,
}

impl Parking {
    fn entered(&self, verb: &'static str) {
        let admitted = self.gate.get().and_then(std::sync::Weak::upgrade).is_some_and(|gate| gate.admits(1));
        self.entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push((verb, admitted));
    }

    fn trace(&self) -> Vec<(&'static str, bool)> { self.entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone() }

    fn parked<'a, 'b, T: Send + 'b>(&'a self, verb: &'static str) -> Sent<'b, T> where 'a: 'b {
        self.entered(verb);
        Box::pin(async move { crate::test_wait::within(self.release.notified()).await; Err(TransportError::Network("parked".into())) })
    }
}

impl Upstream for Parking {
    fn query<'a, 'b>(&'a self, _: GraphQLRequest) -> Sent<'b, Response> where 'a: 'b, Self: 'b { self.parked("query") }
    fn mutate<'a, 'b>(&'a self, _: GraphQLRequest) -> Sent<'b, Response> where 'a: 'b, Self: 'b { self.parked("mutate") }
    fn put<'a, 'b>(&'a self, _: PutRequest) -> Sent<'b, PutReply> where 'a: 'b, Self: 'b { self.parked("put") }
}

/// P-RT-14: a close while a request is inside the transport lets that request finish and sends nothing after it: no later call of any verb reaches the old transport, and no task keeps the transport once its call returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rt_prop_close_against_query_no_old_transport() {
    for first in ["query", "mutate", "put"] {
        let gate = Arc::new(EpochGate::default());
        gate.open(1);
        let transport = Arc::new(Parking::default());
        let _ = transport.gate.set(Arc::downgrade(&gate));
        gate.attach(transport.clone());
        let flight = |gate: Arc<EpochGate>, verb: &'static str| crate::task::spawn_on(crate::task::Carry::Detached, &tokio::runtime::Handle::current(), async move {
            match verb {
                "query" => gate.query(Some(1), graphql()).await.map(drop),
                "mutate" => gate.mutate(Some(1), graphql()).await.map(drop),
                _ => gate.put(Some(1), put()).await.map(drop),
            }
        });
        let inside = flight(gate.clone(), first);
        crate::test_wait::yield_until("the first request is inside the transport", || transport.trace().len() == 1).await;
        gate.close(1);
        for later in ["query", "mutate", "put"] {
            let result = crate::test_wait::within(flight(gate.clone(), later)).await.unwrap();
            assert!(matches!(result, Err(TransportError::Interrupted(_))), "{first} then {later}: {result:?}");
        }
        transport.release.notify_one();
        let finished = crate::test_wait::within(inside).await.unwrap();
        assert!(matches!(finished, Err(TransportError::Network(_))), "{first}: the request inside the transport finishes: {finished:?}");
        let trace = transport.trace();
        assert_eq!(trace, vec![(first, true)], "{first}: late calls reached the old transport, trace (verb, gate admitted at entry): {trace:?}");
        assert_eq!(Arc::strong_count(&transport), 2, "{first}: a task still holds the transport after its call returned");
    }
}
