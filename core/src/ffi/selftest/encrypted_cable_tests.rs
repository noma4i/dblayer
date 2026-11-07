use crate::ffi::tape::StreamDelivery;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use super::rig_tests::{bound};
use super::rig_models::plain_models;
use crate::ffi::encrypted_tests::ThroughServer;
use crate::ffi::session_guards_tests::{Rig, SOCIAL_BODY};
use crate::ffi::{ByteProgress, Config, Database, DbError, PutReply, ReadRequest, FakeTransport, TransportRequest};
use crate::transport::encrypted::cable::peer_tests::{CablePeer, PeerEvent};

pub(super) struct Peer {
    pub(super) cable: CablePeer,
}

impl Peer {
    pub(super) fn new(config: crate::transport::EncryptedConfig, subscribed: mpsc::UnboundedSender<String>, closed: mpsc::UnboundedSender<String>) -> Self {
        Self { cable: CablePeer::observed(config, move |event| { let _ = match event { PeerEvent::Subscribed(token) => subscribed.send(token), PeerEvent::Closed(token) => closed.send(token) }; }) }
    }
}

#[async_trait]
impl FakeTransport for Peer {
    fn cable(&self) -> Option<&CablePeer> { Some(&self.cable) }
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
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
    let peer = Arc::new(Peer::new(config.clone(), subscribed, closed));
    let database = Database::open(Config { path: None, data_version: None }, plain_models(), crate::ffi::OpenPorts { transport: Some(peer), encrypted: Some(config), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()) }).await.unwrap();
    database.register_operations(r#"[{"name":"Notices","document":"subscription Notices { notices { id } }","rootPath":"notices"}]"#.into()).await.unwrap();
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
    let peer = Arc::new(Peer::new(config.clone(), subscribed, closed));
    let database = Database::open(Config { path: None, data_version: None }, plain_models(), crate::ffi::OpenPorts { transport: Some(peer.clone()), encrypted: Some(config), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()) }).await.unwrap();
    database.register_operations(r#"[{"name":"Notices","document":"subscription Notices { notices { id } }","rootPath":"notices"}]"#.into()).await.unwrap();
    database.set_online(true).unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    let query = r#"{"model":"Notice","relation":"recent","params":{}}"#;
    let first = database.subscribe_reading(database.session_epoch(), ReadRequest::List { query: crate::ffi::QuerySpec::json(query) }, Arc::new(Rows));
    let old = crate::test_wait::within(subscriptions.recv()).await.unwrap();
    bound(database.session_reset()).await.unwrap();
    assert_eq!(crate::test_wait::within(closes.recv()).await.as_deref(), Some(old.as_str()));
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    let second = database.subscribe_reading(database.session_epoch(), ReadRequest::List { query: crate::ffi::QuerySpec::json(query) }, Arc::new(Rows));
    let current = crate::test_wait::within(subscriptions.recv()).await.unwrap();
    assert_ne!(current, old);
    peer.cable.event(&old, serde_json::json!({"data":{"notices":{"id":"old"}}}));
    assert!(bound(database.read_query(database.session_epoch(), crate::ffi::QuerySpec::json(query))).await.unwrap().is_empty());
    database.cancel_stream(first);
    database.cancel_stream(second);
    database.close_now();
}
