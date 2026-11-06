use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use super::{send_in_epoch, EpochGate};
use crate::transport::{
    CancelToken, GraphQLRequest, PutFile, PutRequest, PutResponse, Response, ServerSubscription, Transport, TransportError, TransferIdentity, TransferStep,
};

#[derive(Default)]
struct Counting(AtomicUsize);

#[async_trait]
impl Transport for Counting {
    async fn query(&self, _: GraphQLRequest) -> Result<Response, TransportError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(TransportError::Network("counted".into()))
    }
    async fn mutate(&self, _: GraphQLRequest) -> Result<Response, TransportError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(TransportError::Network("counted".into()))
    }
    async fn subscribe(&self, _: GraphQLRequest) -> Result<ServerSubscription, TransportError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(TransportError::Network("counted".into()))
    }
    async fn put(&self, _: PutRequest) -> Result<PutResponse, TransportError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(TransportError::Network("counted".into()))
    }
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
                    "query" => send_in_epoch(&gate, Some(1), transport.query(graphql())).await.map(drop),
                    "mutate" => send_in_epoch(&gate, Some(1), transport.mutate(graphql())).await.map(drop),
                    "subscribe" => send_in_epoch(&gate, Some(1), transport.subscribe(graphql())).await.map(drop),
                    "put" => send_in_epoch(&gate, Some(1), transport.put(put())).await.map(drop),
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
    assert!(matches!(send_in_epoch(&gate, Some(2), transport.query(graphql())).await, Err(TransportError::Network(_))));
    assert!(matches!(send_in_epoch(&gate, Some(1), transport.query(graphql())).await, Err(TransportError::Interrupted(_))));
    assert_eq!(transport.0.load(Ordering::SeqCst), 1);
}
