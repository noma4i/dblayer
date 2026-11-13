use std::sync::Arc;

use serde_json::json;
use tokio::sync::mpsc;

use super::encrypted_cable_tests::Peer;
use super::rig_tests::bound;
use crate::ffi::tests::encrypted_tests::ThroughServer;
use crate::ffi::tests::session_guards_tests::{Rig, SOCIAL_BODY};
use crate::ffi::tape::{Reader, StreamDelivery};
use crate::ffi::{Config, Database, ReadRequest};
use crate::schema::{FieldSpec, ModelSpec, RelationSpec, RemoteShape, UpdatesSpec, VariableSource};
use crate::value::ScalarKind;

struct Quiet;
impl Reader for Quiet { fn delivered(&self, _: StreamDelivery) {} }

fn page_models() -> String {
    let mut models: Vec<serde_json::Value> = serde_json::from_str(include_str!("wrapper_models.inc")).unwrap();
    let mut page = ModelSpec::new("ScreenPage", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("slug", ScalarKind::Str)]);
    let mut by_slug = RelationSpec::new("by_slug", &["slug"]);
    let mut remote = crate::schema_fixtures::remote("ScreenPageBySlug", RemoteShape::Single, "1.minute");
    remote.variables.insert("slug".into(), VariableSource::Param { name: "slug".into() });
    by_slug.remote = Some(remote);
    by_slug.updates.push(UpdatesSpec { apply: None, subscription: "ScreenPageUpdated".into(), deleted_by: None, deleted_unless: None, set: serde_json::Map::new(), removes: false, debounce: None, root: None, id: vec!["slug".into()] });
    page.indexes.push(vec!["slug".into()]);
    page.relations.push(by_slug);
    models.push(serde_json::to_value(page).unwrap());
    serde_json::to_string(&models).unwrap()
}

fn window(slug: &str) -> ReadRequest { ReadRequest::List { query: crate::ffi::QuerySpec::json(json!({"model":"ScreenPage", "relation":"by_slug", "params":{"slug":slug}}).to_string()) } }

#[tokio::test(flavor = "multi_thread")]
async fn a_window_subscribes_its_channel_with_the_variables_of_the_window() {
    let rig = Rig::new();
    let config = crate::transport::encrypted::tests::protocol_tests::config();
    let (subscribed, mut subscriptions) = mpsc::unbounded_channel();
    let (closed, _closes) = mpsc::unbounded_channel();
    let peer = Arc::new(Peer::new(config.clone(), subscribed, closed));
    let database = Database::open(Config { path: None }, crate::ffi::specs(page_models()), crate::ffi::OpenPorts { partial_operations: true, transport: Some(peer.clone()), encrypted: Some(config), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
    database.register_operations(json!([{"name":"ScreenPageUpdated", "document":"subscription ScreenPageUpdated($slug: String!) { screenPageUpdated(slug: $slug) { id slug } }", "rootPath":"screenPageUpdated"}]).to_string()).await.unwrap();
    database.set_online(true).await.unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    let epoch = database.session_epoch();
    let streams = [window("home"), window("home"), window("about")].map(|request| database.subscribe_reading(epoch, request, Arc::new(Quiet)));
    let token = crate::test_wait::within(subscriptions.recv()).await.unwrap();
    crate::test_wait::within(subscriptions.recv()).await.unwrap();
    assert_eq!(bound(database.subscription_census()).await.unwrap().subscriptions, 2, "two windows with the same variables are one channel");
    let mut variables: Vec<serde_json::Value> = peer.cable.subscribes(&token).iter().map(|frame| frame["variables"].clone()).collect();
    variables.sort_by_key(ToString::to_string);
    assert_eq!(variables, [json!({"slug":"about"}), json!({"slug":"home"})]);
    streams.into_iter().for_each(|stream| database.cancel_stream(stream));
    database.close_now();
}
