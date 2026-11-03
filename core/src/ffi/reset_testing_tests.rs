use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Map;

use super::load_tests::relation;
use super::{ByteProgress, CableLease, Config, Database, DbError, EncryptedSocketEvent, EntityDelivery, EntityFetch, EntityListener, EntityRequest, LiveChange, PutReply, QueryListener, SubscriptionSink, TestClock, Transport, TransportRequest};
use crate::schema::{FieldSpec, ModelSpec, NodeRead, RelationSpec, UpdatesSpec, VariableSource, NODE_PARAM};
use crate::test_wait::{within, yield_until};
use crate::value::ScalarKind;

const UNAVAILABLE: &str = "transport unavailable during reset";

struct Backend {
    calls: Mutex<Vec<String>>,
    hold_nodes: Option<Arc<tokio::sync::Semaphore>>,
    sinks: Mutex<Vec<Arc<SubscriptionSink>>>,
}

impl Backend {
    fn new(hold_nodes: Option<Arc<tokio::sync::Semaphore>>) -> Arc<Self> {
        Arc::new(Self { calls: Mutex::new(Vec::new()), hold_nodes, sinks: Mutex::new(Vec::new()) })
    }

    fn count(&self, operation: &str) -> usize { self.calls.lock().unwrap().iter().filter(|name| *name == operation).count() }
    fn names(&self) -> Vec<String> { self.calls.lock().unwrap().clone() }
}

#[async_trait]
impl Transport for Backend {
    async fn open_cable(&self, _: Arc<CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "no socket".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { None }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, request: TransportRequest) -> Result<String, DbError> {
        self.calls.lock().unwrap().push(request.operation_name.clone());
        if request.operation_name == "Node" {
            if let Some(hold) = &self.hold_nodes { hold.acquire().await.unwrap().forget(); }
            return Ok(r#"{"data":{"node":{"__typename":"Moment","id":"m1","text":"a"}}}"#.into());
        }
        Ok(r#"{"data":{"moments":[{"id":"m1","text":"a"}]}}"#.into())
    }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, request: TransportRequest, sink: Arc<SubscriptionSink>) -> Result<(), DbError> {
        self.calls.lock().unwrap().push(request.operation_name);
        sink.opened("token".into())?;
        sink.ready();
        self.sinks.lock().unwrap().push(sink);
        Ok(())
    }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: String, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { unreachable!() }
}

#[derive(Default)]
struct Switch(Mutex<Option<Arc<Backend>>>);

impl Switch {
    fn set(&self, target: Option<Arc<Backend>>) { *self.0.lock().unwrap() = target; }
    fn selected(&self) -> Result<Arc<Backend>, DbError> { self.0.lock().unwrap().clone().ok_or_else(|| DbError::Interrupted { detail: UNAVAILABLE.into() }) }
}

#[async_trait]
impl Transport for Switch {
    async fn open_cable(&self, lease: Arc<CableLease>) -> Result<(), DbError> { self.selected()?.open_cable(lease).await }
    async fn cable_send(&self, token: String, text: String) -> Result<(), DbError> { self.selected()?.cable_send(token, text).await }
    async fn cable_next(&self, token: String) -> Option<EncryptedSocketEvent> { self.selected().ok()?.cable_next(token).await }
    async fn cable_close(&self, token: String) { if let Ok(target) = self.selected() { target.cable_close(token).await; } }
    async fn query(&self, request: TransportRequest) -> Result<String, DbError> { self.selected()?.query(request).await }
    async fn mutate(&self, request: TransportRequest) -> Result<String, DbError> { self.selected()?.mutate(request).await }
    async fn subscribe(&self, request: TransportRequest, sink: Arc<SubscriptionSink>) -> Result<(), DbError> { self.selected()?.subscribe(request, sink).await }
    fn unsubscribe(&self, token: String) { if let Ok(target) = self.selected() { target.unsubscribe(token); } }
    async fn put(&self, url: String, headers: HashMap<String, String>, file: crate::transport::PutFile, tag: String, progress: Arc<ByteProgress>) -> Result<PutReply, DbError> { self.selected()?.put(url, headers, file, tag, progress).await }
}

struct Silent;
impl QueryListener for Silent { fn changed(&self, _: LiveChange) {} }
impl EntityListener for Silent { fn changed(&self, _: EntityDelivery) {} }

#[derive(Default)]
struct Deliveries(Mutex<Vec<super::WireEntitySnapshot>>);
impl EntityListener for Deliveries {
    fn changed(&self, delivery: EntityDelivery) { self.0.lock().unwrap().extend(delivery.snapshots); }
}

fn models() -> String {
    let mut moment = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    moment.relations.push(relation("feed", "5.minutes"));
    let mut node = RelationSpec::new("node", &[]);
    node.text_params = vec![NODE_PARAM.to_string()];
    node.node = Some(NodeRead::One);
    let mut remote = relation("node", "5.minutes").remote.unwrap();
    remote.operation = "Node".into();
    remote.shape = crate::schema::RemoteShape::Single;
    remote.variables.insert("id".into(), VariableSource::Param { name: NODE_PARAM.into() });
    node.remote = Some(remote);
    moment.relations.push(node);

    let updates = |name: &str| UpdatesSpec { apply: None, subscription: name.into(), deleted_by: None, deleted_unless: None, set: Map::new(), removes: false, debounce: None, root: None, id: Vec::new() };
    let mut notice = ModelSpec::new("Notice", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    notice.storage = crate::schema::Storage::Memory;
    let mut recent = RelationSpec::global("recent");
    recent.updates.push(updates("Notices"));
    notice.relations.push(recent);
    notice.listens.push(updates("Listened"));
    serde_json::to_string(&vec![moment, notice]).unwrap()
}

const OPERATIONS: &str = r#"[
  {"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"},
  {"name":"Node","document":"query Node($id: ID!) { node(id: $id) { ... on Moment { id text } } }","rootPath":"node"},
  {"name":"Notices","document":"subscription Notices { notices { id } }","rootPath":"notices"},
  {"name":"Listened","document":"subscription Listened { listened { id } }","rootPath":"listened"}]"#;

const NOTICES: &str = r#"{"model":"Notice","relation":"recent","params":{}}"#;

async fn prepare(db: &Database, switch: &Switch, next: &Arc<Backend>) {
    switch.set(None);
    db.reset_for_testing().await.unwrap();
    within(db.settle_for_testing()).await.unwrap();
    switch.set(Some(next.clone()));
}

fn fetch(id: &str, lease: u64) -> Vec<EntityRequest> { vec![EntityRequest { id: id.into(), lease, fetch: EntityFetch::Force }] }

#[tokio::test]
async fn reset_for_testing_returns_the_database_to_a_fresh_open() {
    let switch = Arc::new(Switch::default());
    let held_reply = Arc::new(tokio::sync::Semaphore::new(0));
    let previous = Backend::new(Some(held_reply));
    switch.set(Some(previous.clone()));
    let db = Database::open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, models(), Some(switch.clone() as Arc<dyn Transport>), None, TestClock::new(1_000_000), None, None, None, None).await.unwrap();
    db.register_operations(OPERATIONS.into()).unwrap();
    db.set_listening(false).unwrap();
    db.sync().unwrap().mark_open_baseline();

    let _held_notices = db.live_query(NOTICES.into(), Arc::new(Silent));
    yield_until("the subscription of the previous test", || previous.count("Notices") == 1).await;
    db.set_listening(true).unwrap();
    yield_until("the model subscription of the previous test", || previous.count("Listened") == 1).await;
    let entities = db.live_entities("Moment".into(), Arc::new(Silent)).unwrap();
    entities.retain(fetch("m1", 1)).unwrap();
    yield_until("the node read in flight", || previous.count("Node") == 1).await;

    let first = Backend::new(None);
    prepare(&db, &switch, &first).await;
    assert!(db.db.connectivity().is_online());
    assert_eq!(db.testing_busy().unwrap(), None);
    assert!(first.names().is_empty(), "work of the previous test called the next transport: {:?}", first.names());
    db.resume().await.unwrap();
    within(db.settle_for_testing()).await.unwrap();
    assert_eq!(first.count("Notices") + first.count("Listened"), 0, "subscriptions of the previous test opened on the next transport: {:?}", first.names());
    let _attached = db.live_query(NOTICES.into(), Arc::new(Silent));
    yield_until("the subscription of the observer", || first.count("Notices") == 1).await;
    db.set_listening(true).unwrap();
    yield_until("the model subscription after listening is switched on", || first.count("Listened") == 1).await;

    entities.release(vec![super::EntityLease { id: "m1".into(), lease: 1 }]).unwrap();
    db.load("Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let seen = Arc::new(Deliveries::default());
    let later = db.live_entities("Moment".into(), seen.clone()).unwrap();
    later.retain(vec![EntityRequest { id: "m1".into(), lease: 50, fetch: EntityFetch::None }]).unwrap();
    yield_until("the first value of the new handle", || seen.0.lock().unwrap().iter().any(|snapshot| snapshot.lease == 50)).await;
    let first_value = seen.0.lock().unwrap().iter().find(|snapshot| snapshot.lease == 50).map(|snapshot| snapshot.value.is_some());
    assert_eq!(first_value, Some(true), "the first value of a handle created after the row was stored was empty");

    db.set_online(false).unwrap();
    entities.retain(fetch("m3", 3)).unwrap();
    yield_until("the parked refresh", || db.testing_busy().unwrap().is_none()).await;
    let second = Backend::new(None);
    prepare(&db, &switch, &second).await;
    assert!(db.db.connectivity().is_online(), "the reset kept the network off");
    db.set_online(true).unwrap();
    within(db.settle_for_testing()).await.unwrap();
    assert_eq!(second.count("Node"), 0, "a refresh parked before the reset reached the next transport: {:?}", second.names());

    db.logout().await.unwrap();
    assert!(!db.streams.snapshot().signed_in());
    let third = Backend::new(None);
    prepare(&db, &switch, &third).await;
    assert!(!db.streams.snapshot().signed_in(), "the reset reopened a session that a logout ended");
    assert!(third.names().is_empty(), "a parked database sent {:?}", third.names());
    entities.retain(fetch("m2", 2)).unwrap();
    within(entities.load_initial("m2".into(), 2)).await.unwrap();
    assert!(db.streams.snapshot().signed_in(), "the explicit call did not open the session");
    yield_until("the node read of the new test", || third.count("Node") >= 1).await;
    within(db.settle_for_testing()).await.unwrap();
}
