use crate::ffi::tape::StreamDelivery;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use super::rig_tests::{bound, plain_models};
use crate::ffi::encrypted_tests::ThroughServer;
use crate::ffi::session_guards_tests::{Rig, SOCIAL_BODY};
use crate::ffi::{ByteProgress, CableLease, Config, Database, DbError, EncryptedSocketEvent, PutReply, ReadRequest, SubscriptionSink, Transport, TransportRequest};
use crate::transport::encrypted::cable::peer_tests::CablePeer;

struct Peer {
    cable: CablePeer,
    subscribed: mpsc::UnboundedSender<String>,
    closed: mpsc::UnboundedSender<String>,
}

#[async_trait]
impl Transport for Peer {
    async fn open_cable(&self, lease: Arc<CableLease>, _url: String) -> Result<(), DbError> { self.cable.open(&lease).map(drop) }
    async fn cable_send(&self, token: String, text: String) -> Result<(), DbError> {
        if self.cable.receive(&token, &text)?.is_some() { let _ = self.subscribed.send(token); }
        Ok(())
    }
    async fn cable_next(&self, token: String) -> Option<EncryptedSocketEvent> { self.cable.next(&token).await }
    async fn cable_close(&self, token: String) { let _ = self.closed.send(token); }
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { unreachable!() }
}

struct Rows;
impl crate::ffi::tape::Reader for Rows { fn delivered(&self, _: StreamDelivery) {} }

#[tokio::test(flavor = "multi_thread")]
async fn feed_prop_census_from_owners() {
    let rig = Rig::new();
    let config = crate::transport::encrypted::protocol_tests::config();
    let (subscribed, mut subscriptions) = mpsc::unbounded_channel();
    let (closed, mut closes) = mpsc::unbounded_channel();
    let peer = Arc::new(Peer { cable: CablePeer::new(config.clone()), subscribed, closed });
    let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, plain_models(), Some(peer), Some(config), Some(rig.credentials.clone()), Some(Arc::new(ThroughServer(rig.server.clone()))), None).await.unwrap();
    database.register_operations(r#"[{"name":"Notices","document":"subscription Notices { notices { id } }","rootPath":"notices"}]"#.into()).unwrap();
    database.set_online(true).unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    let stream = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Notice".into() });
    let token = crate::test_wait::within(subscriptions.recv()).await.unwrap();
    let active = bound(database.subscription_census()).await.unwrap();
    assert_eq!(active.subscriptions, 1);
    drop(stream);
    assert_eq!(crate::test_wait::within(closes.recv()).await.as_deref(), Some(token.as_str()));
    let life = database.life_handle();
    assert!(crate::test_wait::within(tokio::task::spawn_blocking(move || life.barrier(std::time::Duration::from_secs(5)))).await.unwrap());
    let released = bound(database.subscription_census()).await.unwrap();
    assert_eq!((released.observed_queries, released.subscriptions, released.retained_observers), (0, 0, 0));
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_old_encrypted_cable_cannot_deliver_into_the_next_account() {
    let rig = Rig::new();
    let config = crate::transport::encrypted::protocol_tests::config();
    let (subscribed, mut subscriptions) = mpsc::unbounded_channel();
    let (closed, mut closes) = mpsc::unbounded_channel();
    let peer = Arc::new(Peer { cable: CablePeer::new(config.clone()), subscribed, closed });
    let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, plain_models(), Some(peer.clone()), Some(config), Some(rig.credentials.clone()), Some(Arc::new(ThroughServer(rig.server.clone()))), None).await.unwrap();
    database.register_operations(r#"[{"name":"Notices","document":"subscription Notices { notices { id } }","rootPath":"notices"}]"#.into()).unwrap();
    database.set_online(true).unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    let query = r#"{"model":"Notice","relation":"recent","params":{}}"#;
    let first = database.subscribe_reading(database.session_epoch(), ReadRequest::List { query_json: query.into() }, Arc::new(Rows));
    let old = crate::test_wait::within(subscriptions.recv()).await.unwrap();
    bound(database.session_reset()).await.unwrap();
    assert_eq!(crate::test_wait::within(closes.recv()).await.as_deref(), Some(old.as_str()));
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    let second = database.subscribe_reading(database.session_epoch(), ReadRequest::List { query_json: query.into() }, Arc::new(Rows));
    let current = crate::test_wait::within(subscriptions.recv()).await.unwrap();
    assert_ne!(current, old);
    peer.cable.event(&old, serde_json::json!({"data":{"notices":{"id":"old"}}}));
    assert!(bound(database.read_query(database.session_epoch(), query.into())).await.unwrap().is_empty());
    database.cancel_stream(first);
    database.cancel_stream(second);
    database.close_now();
}
