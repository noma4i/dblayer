use std::sync::Arc;

use tokio::sync::mpsc;

use super::encrypted_cable_tests::Peer;
use super::rig_tests::bound;
use crate::ffi::session_guards_tests::{Rig, SOCIAL_BODY};
use crate::ffi::tape::{Reader, StreamDelivery};
use crate::ffi::encrypted_tests::ThroughServer;
use crate::ffi::{Config, Database, ReadRequest};

struct Quiet;
impl Reader for Quiet { fn delivered(&self, _: StreamDelivery) {} }

const OPERATIONS: &str = r#"[{"name":"UserTyping","document":"subscription UserTyping { userTyping { id } }","rootPath":"userTyping"},{"name":"MomentEvents","document":"subscription MomentEvents { momentEvents { id } }","rootPath":"momentEvents"}]"#;
const PROFILE: &str = r#"{"model":"User","relation":"profile","params":{}}"#;
const RECENT: &str = r#"{"model":"Moment","relation":"recent","params":{}}"#;

async fn signed(rig: &Rig, peer: Arc<Peer>) -> Arc<Database> {
    let config = crate::transport::encrypted::protocol_tests::config();
    let models = include_str!("wrapper_models.inc").to_string();
    let database = Database::open(Config { path: None, data_version: None }, crate::ffi::specs(models), crate::ffi::OpenPorts { transport: Some(peer), encrypted: Some(config), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
    database.register_operations(OPERATIONS.into()).await.unwrap();
    database.set_online(true).unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    database
}

fn peer() -> (Arc<Peer>, mpsc::UnboundedReceiver<String>, mpsc::UnboundedReceiver<String>) {
    let (subscribed, subscriptions) = mpsc::unbounded_channel();
    let (closed, closes) = mpsc::unbounded_channel();
    (Arc::new(Peer::new(crate::transport::encrypted::protocol_tests::config(), subscribed, closed)), subscriptions, closes)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_reader_that_stops_ends_the_channel_of_its_relation_in_the_core() {
    let rig = Rig::new();
    let (peer, mut subscriptions, mut closes) = peer();
    let database = signed(&rig, peer).await;
    let stream = database.subscribe_reading(database.session_epoch(), ReadRequest::List { query: crate::ffi::QuerySpec::json(PROFILE) }, Arc::new(Quiet));
    let token = crate::test_wait::within(subscriptions.recv()).await.unwrap();
    assert_eq!(bound(database.subscription_census()).await.unwrap().subscriptions, 1);
    database.cancel_stream(stream);
    assert_eq!(crate::test_wait::within(closes.recv()).await.as_deref(), Some(token.as_str()));
    assert_eq!(bound(database.subscription_census()).await.unwrap().subscriptions, 0);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_of_a_relation_without_updates_adds_no_channel() {
    let rig = Rig::new();
    let (peer, mut subscriptions, _closes) = peer();
    let database = signed(&rig, peer).await;
    let first = database.subscribe_reading(database.session_epoch(), ReadRequest::List { query: crate::ffi::QuerySpec::json(PROFILE) }, Arc::new(Quiet));
    let second = database.subscribe_reading(database.session_epoch(), ReadRequest::List { query: crate::ffi::QuerySpec::json(RECENT) }, Arc::new(Quiet));
    crate::test_wait::within(subscriptions.recv()).await.unwrap();
    let census = bound(database.subscription_census()).await.unwrap();
    assert_eq!(census.subscriptions, 1, "a relation without `updates` and a model without `changed_by` has no channel");
    database.cancel_stream(first);
    database.cancel_stream(second);
    database.close_now();
}
