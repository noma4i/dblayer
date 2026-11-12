use crate::ffi::{FakeTransport};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;



use tokio::sync::Notify;

use super::tape::{Rows, Tape};
use super::{Config, Database, DbError, ReadRequest, TestClock, Transport};
use crate::identity::RowId;
use crate::schema::{Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape, RemoteSpec};
use crate::value::ScalarKind;
use crate::test_wait::within;

pub(in super::super) struct Counting {
    pub(in super::super) queries: AtomicUsize,
    started: Notify,
    hold: Option<Arc<tokio::sync::Semaphore>>,
    gate: Option<Arc<tokio::sync::Semaphore>>,
}

#[async_trait::async_trait]
impl FakeTransport for Counting {
    async fn query(&self, request: super::TransportRequest) -> Result<String, DbError> {
        if let Some(gate) = &self.gate { gate.acquire().await.unwrap().forget(); }
        self.queries.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        if let Some(hold) = &self.hold { hold.acquire().await.unwrap().forget(); }
        Ok(if request.operation_name == "Node" { r#"{"data":{"node":{"__typename":"Moment","id":"m1","text":"a"}}}"# } else { r#"{"data":{"moments":[{"id":"m1","text":"a"}]}}"# }.into())
    }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

pub(in super::super) fn relation(name: &str, stale: &str) -> RelationSpec {
    let mut relation = RelationSpec::global(name);
    relation.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: stale.into(),
        empty_stale_time: None, selects_page_info: false, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    relation
}

pub(in super::super) async fn rig() -> (Arc<Database>, Arc<Counting>) {
    let (db, transport, _) = open_rig(None, None, None).await;
    (db, transport)
}

pub(in super::super) async fn held_rig()-> (Arc<Database>, Arc<Counting>, Arc<tokio::sync::Semaphore>) {
    let hold = Arc::new(tokio::sync::Semaphore::new(0));
    let (db, transport, _) = open_rig(None, Some(hold.clone()), None).await;
    (db, transport, hold)
}

async fn joined_the_flight(db: &Database, transport: &Counting, already: usize) {
    tokio::select! {
        () = crate::test_wait::settled(&db, "the second load waits in the window", || db.life.waiting_calls() > already) => {}
        () = transport.started.notified() => panic!("the second load made its own request instead of joining"),
    }
    let life = db.life_handle();
    tokio::task::spawn_blocking(move || life.barrier()).await.unwrap();
}

async fn rig_with_clock() -> (Arc<Database>, Arc<Counting>, Arc<TestClock>) {
    let clock = TestClock::new(1_000_000);
    let (db, transport, _) = open_rig(Some(clock.clone()), None, None).await;
    (db, transport, clock)
}

pub(in super::super) async fn open_rig(clock: Option<Arc<TestClock>>, hold: Option<Arc<tokio::sync::Semaphore>>, gate: Option<Arc<tokio::sync::Semaphore>>) -> (Arc<Database>, Arc<Counting>, Option<Arc<TestClock>>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    model.relations.extend([relation("feed", "5.minutes"), relation("quick", "200.milliseconds")]);
    let transport = Arc::new(Counting { queries: AtomicUsize::new(0), started: Notify::new(), hold: hold.clone(), gate });
    let config = Config { path: None, data_version: None };
    let models = serde_json::to_string(&vec![model]).unwrap();
    let transport_handle = Some(transport.clone() as Arc<dyn Transport>);
    let db = match &clock {
        Some(clock) => Database::open_with_clock(config, crate::ffi::specs(models), crate::ffi::OpenPorts { transport: transport_handle, endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, clock.clone()).await,
        None => Database::open(config, crate::ffi::specs(models), crate::ffi::OpenPorts { transport: transport_handle, endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await,
    }
    .unwrap();
    db.register_operations(r#"[{"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"}]"#.into()).await.unwrap();
    db.set_online(true).unwrap();
    (db, transport, clock)
}

async fn load(db: &Database, relation: &str) -> Result<(), DbError> { db.load(db.session_epoch(), "Moment".into(), relation.into(), "{}".into()).await }
fn stored(db: &Database) -> bool { db.db.find("Moment", &RowId::new("m1").unwrap()).unwrap().is_some() }
fn queries(transport: &Counting) -> usize { transport.queries.load(Ordering::SeqCst) }

#[tokio::test]
async fn settled_waits_for_a_load_that_outlives_a_data_reset() {
    let hold = Arc::new(tokio::sync::Semaphore::new(0));
    let clock = TestClock::new(1_000_000);
    let (db, transport, _) = open_rig(Some(clock), Some(hold.clone()), None).await;
    let load = tokio::spawn({ let db = db.clone(); async move { load(&db, "feed").await } });
    within(transport.started.notified()).await;
    db.reset_data().await;
    let ended = within(load).await.unwrap();
    assert!(matches!(ended, Err(DbError::NoSession { .. })), "the explicit load waited for its request after the reset (D-263): {ended:?}");
    within(crate::ffi::settled(&db)).await.unwrap();
    hold.add_permits(1);
    assert!(!stored(&db));

    let hold = Arc::new(tokio::sync::Semaphore::new(0));
    let (db, transport, _) = open_rig(Some(TestClock::new(1_000_000)), Some(hold.clone()), None).await;
    let listener = Arc::new(Tape::default());
    let _live = db.watch_query(r#"{"model":"Moment","relation":"feed","params":{}}"#, listener.clone());
    within(transport.started.notified()).await;
    hold.add_permits(1);
    crate::ffi::settled(&db).await.unwrap();
    crate::test_wait::settled(&db, "the loaded row reached the listener", || listener.heard().iter().any(|seen| seen.size == 1)).await;

    let hold = Arc::new(tokio::sync::Semaphore::new(0));
    let transport = Arc::new(Counting { queries: AtomicUsize::new(0), started: Notify::new(), hold: Some(hold.clone()), gate: None });
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut node = RelationSpec::new("node", &[]);
    node.text_params = vec![crate::schema::NODE_PARAM.to_string()];
    node.node = Some(crate::schema::NodeRead::One);
    let mut remote = relation("node", "5.minutes").remote.unwrap();
    remote.operation = "Node".into();
    remote.shape = RemoteShape::Single;
    remote.variables.insert("id".into(), crate::schema::VariableSource::Param { name: crate::schema::NODE_PARAM.into() });
    node.remote = Some(remote);
    model.relations.push(node);
    let db = Database::open_with_clock(Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), crate::ffi::OpenPorts { transport: Some(transport.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, TestClock::new(1_000_000)).await.unwrap();
    db.register_operations(r#"[{"name":"Node","document":"query Node($id: ID!) { node(id: $id) { ... on Moment { id text } } }","rootPath":"node"}]"#.into()).await.unwrap();
    db.set_online(true).unwrap();
    let listener = Arc::new(Rows::default());
    let _entity = db.watch(ReadRequest::Entity { model: "Moment".into(), id: "m1".into(), fetch: super::ReadFetch::Force }, listener.clone());
    within(transport.started.notified()).await;
    let settling = tokio::spawn({ let db = db.clone(); async move { crate::ffi::settled(&db).await } });
    crate::ffi::owners_drained(&db).await.unwrap();
    assert_eq!(db.life.requests_in_flight(), 1, "the held request is the only one on its way");
    assert!(!settling.is_finished(), "settled returned while the request was held");
    hold.add_permits(1);
    within(settling).await.unwrap().unwrap();
    crate::test_wait::settled(&db, "the entity row", || listener.len() == 1).await;
}

#[tokio::test]
async fn inv_fresh_12_load_requests_only_a_window_that_is_not_fresh() {
    let (db, transport, _clock) = rig_with_clock().await;
    load(&db, "feed").await.unwrap();
    assert_eq!(queries(&transport), 1, "never loaded");
    load(&db, "feed").await.unwrap();
    assert_eq!(queries(&transport), 1, "fresh");
}

#[tokio::test]
async fn inv_fresh_13_load_during_a_live_connect_shares_its_request() {
    let (db, transport, hold) = held_rig().await;
    let _live = db.watch_query(r#"{"model":"Moment","relation":"feed","params":{}}"#, Arc::new(Rows::default()));
    within(transport.started.notified()).await;
    let already = db.life.waiting_calls();
    let sharing = tokio::spawn(crate::runtime::counted(Some(db.life.launch()), { let db = db.clone(); async move { load(&db, "feed").await } }));
    joined_the_flight(&db, &transport, already).await;
    hold.add_permits(1);
    sharing.await.unwrap().unwrap();
    assert_eq!(queries(&transport), 1);
}

#[test]
fn inv_api_04_connecting_starts_the_load_and_only_refresh_asks_again() { crate::test_wait::paused(async {
    let (db, transport, _) = open_rig(None, None, None).await;
    let query = r#"{"model":"Moment","relation":"feed","params":{}}"#;
    let first = db.watch_query(query, Arc::new(Rows::default()));
    first.wait_for_initial_load().await.unwrap();
    assert_eq!(queries(&transport), 1);
    let second = db.watch_query(query, Arc::new(Rows::default()));
    second.wait_for_initial_load().await.unwrap();
    assert_eq!(queries(&transport), 1);
    db.refresh(db.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into()).await.unwrap();
    assert_eq!(queries(&transport), 2);
    first.cancel();
    second.cancel();
}) }

#[tokio::test]
async fn cancelling_a_live_query_finishes_its_initial_wait() {
    let (db, transport, _hold) = held_rig().await;
    let live = db.watch_query(r#"{"model":"Moment","relation":"feed"}"#, Arc::new(Rows::default()));
    within(transport.started.notified()).await;
    live.cancel();
    let result = live.wait_for_initial_load().await;
    assert!(matches!(result, Err(DbError::Interrupted { .. })));
}

#[tokio::test]
async fn inv_fresh_14_load_keeps_no_subscription() {
    let (db, _transport) = rig().await;
    let before = db.subscription_census().await.unwrap();
    load(&db, "feed").await.unwrap();
    let after = db.subscription_census().await.unwrap();
    assert_eq!((after.observed_queries, after.subscriptions), (before.observed_queries, before.subscriptions));
}

#[tokio::test]
async fn inv_fresh_13_a_cancelled_caller_does_not_cancel_the_load_another_waits_for() {
    let (db, transport, hold) = held_rig().await;
    let cancelled = tokio::spawn({ let db = db.clone(); async move { load(&db, "feed").await } });
    within(transport.started.notified()).await;
    let already = db.life.waiting_calls();
    let waiter = tokio::spawn(crate::runtime::counted(Some(db.life.launch()), { let db = db.clone(); async move { load(&db, "feed").await } }));
    joined_the_flight(&db, &transport, already).await;
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled(), "the first caller finished before the abort");
    hold.add_permits(2);
    waiter.await.unwrap().unwrap();
    assert_eq!(queries(&transport), 1);
    assert!(stored(&db));
}

#[tokio::test]
async fn inv_fresh_15_a_test_clock_moves_fresh_for_without_sleeping() {
    for (advance_ms, expected) in [(5 * 60_000, 2), (5 * 60_000 - 1, 1)] {
        let (db, transport, clock) = rig_with_clock().await;
        load(&db, "feed").await.unwrap();
        clock.advance(advance_ms);
        load(&db, "feed").await.unwrap();
        assert_eq!(queries(&transport), expected, "advanced by {advance_ms} ms");
    }
}


#[tokio::test(flavor = "multi_thread")]
async fn an_entity_stream_is_not_loaded_when_its_first_empty_snapshot_arrives_before_the_transport_answers() {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut node = RelationSpec::new("node", &[]);
    node.text_params = vec![crate::schema::NODE_PARAM.to_string()];
    node.node = Some(crate::schema::NodeRead::One);
    let mut remote = relation("node", "5.minutes").remote.unwrap();
    remote.operation = "Node".into();
    remote.shape = RemoteShape::Single;
    remote.variables.insert("id".into(), crate::schema::VariableSource::Param { name: crate::schema::NODE_PARAM.into() });
    node.remote = Some(remote);
    model.relations.push(node);
    let hold = Arc::new(tokio::sync::Semaphore::new(0));
    let transport = Arc::new(Counting { queries: AtomicUsize::new(0), started: Notify::new(), hold: Some(hold.clone()), gate: None });
    let db = Database::open(Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), crate::ffi::OpenPorts { transport: Some(transport.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
    db.register_operations(r#"[{"name":"Node","document":"query Node($id: ID!) { node(id: $id) { ... on Moment { id text } } }","rootPath":"node"}]"#.into()).await.unwrap();
    db.set_online(true).unwrap();
    let listener = Arc::new(Tape::default());
    let entity = db.watch(ReadRequest::Entity { model: "Moment".into(), id: "m1".into(), fetch: super::ReadFetch::Force }, listener.clone());
    within(transport.started.notified()).await;
    crate::test_wait::settled(&db, "the empty first snapshot", || listener.last().is_some()).await;
    let mut loaded = std::pin::pin!(entity.wait_for_initial_load());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    crate::ffi::owners_drained(&db).await.unwrap();
    assert_eq!(db.life.requests_in_flight(), 1, "the held request is the only one on its way");
    assert!(std::future::Future::poll(loaded.as_mut(), &mut cx).is_pending(), "loaded() returned before the transport answered");
    hold.add_permits(1);
    within(loaded).await.unwrap();
    assert_eq!(listener.last().map(|seen| seen.size), Some(1), "loaded() returned without the row: {:?}", listener.heard().len());
}
