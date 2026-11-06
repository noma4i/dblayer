use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use std::future::Future;
use std::pin::Pin;

use super::{send_in_epoch, EpochGate};
use crate::transport::{
    CancelToken, GraphQLRequest, PutFile, PutRequest, PutResponse, Response, ServerSubscription, Transport, TransportError, TransferIdentity, TransferStep,
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

impl Transport for Counting {
    fn query<'a, 'b>(&'a self, _: GraphQLRequest) -> Sent<'b, Response> where 'a: 'b, Self: 'b { self.started() }
    fn mutate<'a, 'b>(&'a self, _: GraphQLRequest) -> Sent<'b, Response> where 'a: 'b, Self: 'b { self.started() }
    fn subscribe<'a, 'b>(&'a self, _: GraphQLRequest) -> Sent<'b, ServerSubscription> where 'a: 'b, Self: 'b { self.started() }
    fn put<'a, 'b>(&'a self, _: PutRequest) -> Sent<'b, PutResponse> where 'a: 'b, Self: 'b { self.started() }
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
    for kind in ["query", "mutate", "subscribe", "put"] {
        let gate = Arc::new(EpochGate::default());
        gate.open(1);
        let transport = Arc::new(Counting::default());
        let (started, running) = tokio::sync::oneshot::channel();
        let (ended, waiting) = tokio::sync::oneshot::channel::<()>();
        let task = crate::task::spawn_on(crate::task::Carry::Detached, &tokio::runtime::Handle::current(), {
            let (gate, transport) = (gate.clone(), transport.clone());
            async move {
                let _ = started.send(());
                let _ = waiting.await;
                match kind {
                    "query" => send_in_epoch(&gate, Some(1), || transport.query(graphql())).await.map(drop),
                    "mutate" => send_in_epoch(&gate, Some(1), || transport.mutate(graphql())).await.map(drop),
                    "subscribe" => send_in_epoch(&gate, Some(1), || transport.subscribe(graphql())).await.map(drop),
                    "put" => send_in_epoch(&gate, Some(1), || transport.put(put())).await.map(drop),
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
    let transport = Counting::default();
    assert!(matches!(send_in_epoch(&gate, Some(2), || transport.query(graphql())).await, Err(TransportError::Network(_))));
    assert!(matches!(send_in_epoch(&gate, Some(1), || transport.query(graphql())).await, Err(TransportError::Interrupted(_))));
    assert_eq!(transport.0.load(Ordering::SeqCst), 1);
}
