use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Map;
use tokio::sync::Notify;

use super::stream::tape::{Rows, Tape};
use super::{Config, Database, DbError, StreamDelivery, StreamRequest, StreamValue, TestClock, Transport, ValueSink};
use crate::identity::RowId;
use crate::schema::{Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape, RemoteSpec};
use crate::value::ScalarKind;
use crate::test_wait::within;

pub(super) struct Counting {
    pub(super) queries: AtomicUsize,
    started: Notify,
    hold: Option<Arc<tokio::sync::Semaphore>>,
    gate: Option<Arc<tokio::sync::Semaphore>>,
    subscriptions: Option<std::sync::mpsc::Sender<Arc<super::SubscriptionSink>>>,
}

#[async_trait::async_trait]
impl Transport for Counting {
    async fn open_cable(&self, _: Arc<super::CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, request: super::TransportRequest) -> Result<String, DbError> {
        if let Some(gate) = &self.gate { gate.acquire().await.unwrap().forget(); }
        self.queries.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        if let Some(hold) = &self.hold { hold.acquire().await.unwrap().forget(); }
        Ok(if request.operation_name == "Node" { r#"{"data":{"node":{"__typename":"Moment","id":"m1","text":"a"}}}"# } else { r#"{"data":{"moments":[{"id":"m1","text":"a"}]}}"# }.into())
    }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, sink: Arc<super::SubscriptionSink>) -> Result<(), DbError> {
        sink.opened("notice".into())?;
        sink.ready();
        if let Some(sender) = &self.subscriptions { sender.send(sink).unwrap(); }
        Ok(())
    }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: String, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

#[tokio::test]
async fn a_data_reset_ends_the_held_stream_with_its_epoch_and_a_new_stream_reloads() {
    let (db, transport) = rig().await;
    let listener = Arc::new(Tape::default());
    let query = r#"{"model":"Moment","relation":"feed","params":{},"order":[{"field":"text","direction":"asc"}]}"#;
    let live = db.watch_query(query, listener.clone());
    within(live.wait_for_initial_load()).await.unwrap();
    crate::test_wait::yield_until("the first row", || listener.heard().iter().any(|seen| seen.size == 1)).await;
    db.db.reset_all().unwrap();
    let ended = Arc::new(Rows::default());
    let _watching = db.watch_query(query, ended.clone());
    crate::test_wait::yield_until("the reset and the reloaded row", || ended.last().is_some_and(|seen| seen.size == 1)).await;
    assert_eq!(transport.queries.load(Ordering::SeqCst), 2);
    live.cancel();
}

#[tokio::test]
async fn a_terminal_subscription_failure_reaches_the_query_listener_once_without_http() {
    struct Listener(std::sync::mpsc::Sender<DbError>);
    impl ValueSink for Listener {
        fn delivered(&self, delivery: StreamDelivery) {
            if let StreamValue::Snapshot { state, .. } | StreamValue::Delta { state, .. } | StreamValue::State { state } = delivery.value {
                if let Some(error) = state.error { self.0.send(error).unwrap(); }
            }
        }
    }
    for shared_changes in [false, true] {
    let (opened, subscriptions) = std::sync::mpsc::channel();
    let transport = Arc::new(Counting { queries: AtomicUsize::new(0), started: Notify::new(), hold: None, gate: None, subscriptions: Some(opened) });
    let mut model = ModelSpec::new("Notice", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    model.storage = crate::schema::Storage::Memory;
    let mut relation = if shared_changes { relation("recent", "5.minutes") } else { RelationSpec::global("recent") };
    relation.updates.push(crate::schema::UpdatesSpec { apply: None, subscription: "Notices".into(), deleted_by: None, deleted_unless: None, set: Map::new(), removes: false, debounce: None, root: None, id: Vec::new() });
    if shared_changes { relation.updates.clear(); }
    model.relations.push(relation);
    let mut models = vec![model];
    if shared_changes {
        models[0].changes.push(crate::schema::ChangeRead { area: "NOTICE".into(), relation: "recent".into(), parameter: None, removes_on: vec![], observed: false, payload: None, id_of: None });
        let mut other = models[0].clone();
        other.key = crate::identity::ModelKey::new("AnotherNotice");
        models.push(other);
        models[0].change_feed = Some(crate::schema::ChangeFeed { subscription: "Notices".into(), root: "notices.entries".into(), area: "topic".into(), kind: "action".into(), id: "recordId".into() });
    }
    let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&models).unwrap(), Some(transport.clone()), None, None, None, None, None).await.unwrap();
    database.register_operations(r#"[{"name":"Notices","document":"subscription Notices { notices { id } }","rootPath":"notices"},{"name":"Feed","document":"query Feed { moments { id } }","rootPath":"moments"}]"#.into()).unwrap();
    let (failures, received) = std::sync::mpsc::channel();
    let live = database.watch_query(r#"{"model":"Notice","relation":"recent","params":{}}"#, Arc::new(Listener(failures)));
    live.wait_for_initial_load().await.unwrap();
    let initial_reads = transport.queries.load(Ordering::SeqCst);
    let sink = subscriptions.recv_timeout(Duration::from_secs(2)).unwrap();
    sink.failed(DbError::Session { code: super::SessionCode::Unauthenticated, detail: "session_unknown".into() });
    let error = received.recv_timeout(Duration::from_secs(2)).expect("query listener receives terminal subscription failure");
    assert!(matches!(error, DbError::Session { code: super::SessionCode::Unauthenticated, detail } if detail == "session_unknown"));
    assert!(subscriptions.try_recv().is_err());
    assert_eq!(transport.queries.load(Ordering::SeqCst), initial_reads);
    database.resume().await.unwrap();
    let resumed = subscriptions.recv_timeout(Duration::from_secs(2)).expect("resume restarts the stopped subscription");
    resumed.failed(DbError::Session { code: super::SessionCode::Unauthenticated, detail: "second_session".into() });
    assert!(matches!(received.recv_timeout(Duration::from_secs(2)).expect("observer receives the next session failure"), DbError::Session { detail, .. } if detail == "second_session"));
    live.cancel();
    }
}

#[tokio::test]
async fn a_local_memory_live_query_keeps_a_subscription_watch_without_an_http_load() {
    let mut model = ModelSpec::new("Notice", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("body", ScalarKind::Str)]);
    model.storage = crate::schema::Storage::Memory;
    let mut relation = RelationSpec::global("recent");
    relation.updates.push(crate::schema::UpdatesSpec { apply: None, subscription:"BulletinAdded".into(), deleted_by:None, deleted_unless:None, set:Map::new(), removes:false, debounce:None, root:None, id:Vec::new() });
    model.relations.push(relation);
    let transport = Arc::new(Counting { queries:AtomicUsize::new(0), started:Notify::new(), hold: None, gate: None, subscriptions: None });
    let db = Database::open(Config { path:None, data_version:None, cache_rows:8 }, serde_json::to_string(&vec![model]).unwrap(), Some(transport), None, None, None, None, None).await.unwrap();
    db.register_operations(r#"[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body } }","rootPath":"bulletinAdded"}]"#.into()).unwrap();
    let query = r#"{"model":"Notice","relation":"recent","params":{},"order":[{"field":"body","direction":"asc"}]}"#;
    let live = db.watch_query(query, Arc::new(Rows::default()));
    assert_eq!(db.stream_probe(live.id), Some((true, false)), "a local memory query holds a subscription watch and starts no load");
}

pub(super) fn relation(name: &str, stale: &str) -> RelationSpec {
    let mut relation = RelationSpec::global(name);
    relation.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: stale.into(),
        empty_stale_time: None, selects_page_info: false, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    relation
}

pub(super) async fn rig() -> (Arc<Database>, Arc<Counting>) {
    let (db, transport, _) = open_rig(None, None, None).await;
    (db, transport)
}

pub(super) async fn held_rig()-> (Arc<Database>, Arc<Counting>, Arc<tokio::sync::Semaphore>) {
    let hold = Arc::new(tokio::sync::Semaphore::new(0));
    let (db, transport, _) = open_rig(None, Some(hold.clone()), None).await;
    (db, transport, hold)
}

async fn joined_the_flight(db: &Database, transport: &Counting, already: usize) {
    tokio::select! {
        () = db.sync().unwrap().wait_joined(already + 1) => {}
        () = transport.started.notified() => panic!("the second load made its own request instead of joining"),
    }
}

async fn rig_with_clock() -> (Arc<Database>, Arc<Counting>, Arc<TestClock>) {
    let clock = TestClock::new(1_000_000);
    let (db, transport, _) = open_rig(Some(clock.clone()), None, None).await;
    (db, transport, clock)
}

pub(super) async fn open_rig(clock: Option<Arc<TestClock>>, hold: Option<Arc<tokio::sync::Semaphore>>, gate: Option<Arc<tokio::sync::Semaphore>>) -> (Arc<Database>, Arc<Counting>, Option<Arc<TestClock>>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    model.relations.extend([relation("feed", "5.minutes"), relation("quick", "200.milliseconds")]);
    let transport = Arc::new(Counting { queries: AtomicUsize::new(0), started: Notify::new(), hold: hold.clone(), gate, subscriptions: None });
    let config = Config { path: None, data_version: None, cache_rows: 8 };
    let models = serde_json::to_string(&vec![model]).unwrap();
    let transport_handle = Some(transport.clone() as Arc<dyn Transport>);
    let db = match &clock {
        Some(clock) => Database::open_with_clock(config, models, transport_handle, None, clock.clone(), None, None, None, None).await,
        None => Database::open(config, models, transport_handle, None, None, None, None, None).await,
    }
    .unwrap();
    db.register_operations(r#"[{"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"}]"#.into()).unwrap();
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
    db.db.reset_all().unwrap();
    assert!(within(load).await.unwrap().is_err(), "the explicit load waited for its request after the reset");
    within(crate::ffi::settled(&db)).await.unwrap();
    assert!(db.streams.quiet(), "the ended load left a lease: {:?}", db.streams.held());
    hold.add_permits(1);
    assert!(!stored(&db));

    let hold = Arc::new(tokio::sync::Semaphore::new(0));
    let (db, transport, _) = open_rig(Some(TestClock::new(1_000_000)), Some(hold.clone()), None).await;
    let listener = Arc::new(Tape::default());
    let _live = db.watch_query(r#"{"model":"Moment","relation":"feed","params":{}}"#, listener.clone());
    within(transport.started.notified()).await;
    hold.add_permits(1);
    crate::ffi::settled(&db).await.unwrap();
    crate::test_wait::yield_until("the loaded row reached the listener", || listener.heard().iter().any(|seen| seen.size == 1)).await;

    let hold = Arc::new(tokio::sync::Semaphore::new(0));
    let transport = Arc::new(Counting { queries: AtomicUsize::new(0), started: Notify::new(), hold: Some(hold.clone()), gate: None, subscriptions: None });
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
    let db = Database::open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![model]).unwrap(), Some(transport.clone()), None, TestClock::new(1_000_000), None, None, None, None).await.unwrap();
    db.register_operations(r#"[{"name":"Node","document":"query Node($id: ID!) { node(id: $id) { ... on Moment { id text } } }","rootPath":"node"}]"#.into()).unwrap();
    let listener = Arc::new(Rows::default());
    let _entity = db.watch(StreamRequest::Entity { model: "Moment".into(), id: "m1".into(), fetch: super::EntityFetch::Force }, listener.clone());
    within(transport.started.notified()).await;
    let settling = tokio::spawn({ let db = db.clone(); async move { crate::ffi::settled(&db).await } });
    for _ in 0..200 { tokio::task::yield_now().await; }
    assert!(!settling.is_finished());
    hold.add_permits(1);
    within(settling).await.unwrap().unwrap();
    crate::test_wait::yield_until("the entity row", || listener.len() == 1).await;
}

#[tokio::test]
async fn inv_fresh_12_load_requests_only_a_window_that_is_not_fresh() {
    let (db, transport, clock) = rig_with_clock().await;
    load(&db, "feed").await.unwrap();
    assert_eq!(queries(&transport), 1, "never loaded");
    load(&db, "feed").await.unwrap();
    assert_eq!(queries(&transport), 1, "fresh");

    load(&db, "quick").await.unwrap();
    assert_eq!(queries(&transport), 2);
    clock.advance(300);
    load(&db, "quick").await.unwrap();
    assert_eq!(queries(&transport), 3, "fresh_for elapsed");

    db.db.shared.store.invalidate_family_now("Moment", "feed").unwrap();
    load(&db, "feed").await.unwrap();
    assert_eq!(queries(&transport), 4, "invalidated");
}

#[tokio::test]
async fn inv_fresh_13_load_during_a_live_connect_shares_its_request() {
    let (db, transport, hold) = held_rig().await;
    let _live = db.watch_query(r#"{"model":"Moment","relation":"feed","params":{}}"#, Arc::new(Rows::default()));
    within(transport.started.notified()).await;
    let already = db.sync().unwrap().joined_loads();
    let sharing = tokio::spawn({ let db = db.clone(); async move { load(&db, "feed").await } });
    joined_the_flight(&db, &transport, already).await;
    hold.add_permits(1);
    sharing.await.unwrap().unwrap();
    assert_eq!(queries(&transport), 1);
}

#[tokio::test(start_paused = true)]
async fn inv_api_04_connecting_starts_the_load_and_only_refresh_asks_again() {
    let (db, transport, _) = open_rig(None, None, None).await;
    let query = r#"{"model":"Moment","relation":"feed","params":{}}"#;
    let first = db.watch_query(query, Arc::new(Rows::default()));
    first.wait_for_initial_load().await.unwrap();
    assert_eq!(queries(&transport), 1);
    let second = db.watch_query(query, Arc::new(Rows::default()));
    second.wait_for_initial_load().await.unwrap();
    assert_eq!(queries(&transport), 1);
    db.refresh(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    assert_eq!(queries(&transport), 2);
    first.cancel();
    second.cancel();
}

#[tokio::test(start_paused = true)]
async fn refreshing_on_connect_fetches_once_even_when_the_window_is_fresh() {
    for preloaded in [false, true] {
        let hold = Arc::new(tokio::sync::Semaphore::new(0));
        let gate = Arc::new(tokio::sync::Semaphore::new(usize::from(preloaded)));
        let (db, transport, _) = open_rig(None, Some(hold.clone()), Some(gate.clone())).await;
        if preloaded {
            hold.add_permits(1);
            load(&db, "feed").await.unwrap();
            within(transport.started.notified()).await;
        }
        let before = queries(&transport);
        tokio::spawn({ let gate = gate.clone(); async move { tokio::time::sleep(Duration::from_secs(3)).await; gate.add_permits(1); } });
        let seen = Arc::new(Tape::default());
        let live = db.watch_query(r#"{"model":"Moment","relation":"feed","params":{},"refreshOnConnect":true}"#, seen.clone());
        if preloaded {
            crate::test_wait::yield_until("the cached row", || seen.last().is_some()).await;
            assert_eq!(seen.last().unwrap().size, 1, "cached rows stay visible during the forced load");
        }
        within(transport.started.notified()).await;
        hold.add_permits(1);
        live.wait_for_initial_load().await.unwrap();
        assert!(stored(&db));
        load(&db, "feed").await.unwrap();
        assert_eq!(queries(&transport) - before, 1, "preloaded={preloaded}");
        live.cancel();
    }
}

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
    let before = db.subscription_census();
    load(&db, "feed").await.unwrap();
    let after = db.subscription_census();
    assert_eq!((after.observed_queries, after.subscriptions), (before.observed_queries, before.subscriptions));
}

#[tokio::test]
async fn inv_fresh_13_a_cancelled_caller_does_not_cancel_the_load_another_waits_for() {
    let (db, transport, hold) = held_rig().await;
    let cancelled = tokio::spawn({ let db = db.clone(); async move { load(&db, "feed").await } });
    within(transport.started.notified()).await;
    let already = db.sync().unwrap().joined_loads();
    let waiter = tokio::spawn({ let db = db.clone(); async move { load(&db, "feed").await } });
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
    for (advance_ms, expected) in [(5 * 60_000 + 1_000, 2), (5 * 60_000 - 1_000, 1)] {
        let (db, transport, clock) = rig_with_clock().await;
        load(&db, "feed").await.unwrap();
        clock.advance(advance_ms);
        load(&db, "feed").await.unwrap();
        assert_eq!(queries(&transport), expected, "advanced by {advance_ms} ms");
    }
}

struct Interrupting {
    inner: Counting,
    on_open: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

#[async_trait::async_trait]
impl Transport for Interrupting {
    async fn open_cable(&self, lease: Arc<super::CableLease>) -> Result<(), DbError> { self.inner.open_cable(lease).await }
    async fn cable_send(&self, token: String, text: String) -> Result<(), DbError> { self.inner.cable_send(token, text).await }
    async fn cable_next(&self, token: String) -> Option<crate::ffi::EncryptedSocketEvent> { self.inner.cable_next(token).await }
    async fn cable_close(&self, token: String) { self.inner.cable_close(token).await }
    async fn query(&self, request: super::TransportRequest) -> Result<String, DbError> { self.inner.query(request).await }
    async fn mutate(&self, request: super::TransportRequest) -> Result<String, DbError> { self.inner.mutate(request).await }
    async fn subscribe(&self, request: super::TransportRequest, sink: Arc<super::SubscriptionSink>) -> Result<(), DbError> {
        self.inner.subscribe(request, sink).await?;
        let hook = self.on_open.lock().unwrap().take();
        if let Some(hook) = hook { hook(); }
        Ok(())
    }
    fn unsubscribe(&self, token: String) { self.inner.unsubscribe(token) }
    async fn put(&self, url: String, headers: std::collections::HashMap<String, String>, file: crate::transport::PutFile, tag: String, progress: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { self.inner.put(url, headers, file, tag, progress).await }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_that_a_reset_ends_between_its_open_and_its_delivery_hands_the_failure_to_the_connection_that_replaced_it() {
    let (opened, subscriptions) = std::sync::mpsc::channel();
    let counting = Counting { queries: AtomicUsize::new(0), started: Notify::new(), hold: None, gate: None, subscriptions: Some(opened) };
    let database_slot: Arc<std::sync::Mutex<Option<Arc<Database>>>> = Arc::default();
    let hook_slot = database_slot.clone();
    let transport = Arc::new(Interrupting { inner: counting, on_open: std::sync::Mutex::new(Some(Box::new(move || {
        let database = hook_slot.lock().unwrap().clone().unwrap();
        let life = database.db.shared.store.lifecycle.clone();
        let before = life.snapshot().notice;
        let resetting = std::thread::spawn(move || database.db.reset_all());
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while life.snapshot().notice == before && std::time::Instant::now() < deadline { std::thread::yield_now(); }
        drop(resetting);
    }))) });
    let mut model = ModelSpec::new("Notice", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    model.storage = crate::schema::Storage::Memory;
    let mut relation = RelationSpec::global("recent");
    relation.updates.push(crate::schema::UpdatesSpec { apply: None, subscription: "Notices".into(), deleted_by: None, deleted_unless: None, set: Map::new(), removes: false, debounce: None, root: None, id: Vec::new() });
    model.relations.push(relation);
    let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![model]).unwrap(), Some(transport), None, None, None, None, None).await.unwrap();
    *database_slot.lock().unwrap() = Some(database.clone());
    database.register_operations(r#"[{"name":"Notices","document":"subscription Notices { notices { id } }","rootPath":"notices"}]"#.into()).unwrap();
    let (failures, mut received) = tokio::sync::mpsc::unbounded_channel();
    struct Async(tokio::sync::mpsc::UnboundedSender<DbError>);
    impl ValueSink for Async {
        fn delivered(&self, delivery: StreamDelivery) {
            if let StreamValue::Snapshot { state, .. } | StreamValue::Delta { state, .. } | StreamValue::State { state } = delivery.value {
                if let Some(error) = state.error { let _ = self.0.send(error); }
            }
        }
    }
    let query = r#"{"model":"Notice","relation":"recent","params":{}}"#;
    let ended = database.watch_query(query, Arc::new(Async(failures.clone())));
    let _ = within(ended.wait_for_initial_load()).await;
    let live = database.watch_query(query, Arc::new(Async(failures)));
    within(live.wait_for_initial_load()).await.unwrap();
    let mut sinks = Vec::new();
    crate::test_wait::yield_until("the replacing connection opened", || { sinks.extend(subscriptions.try_iter()); sinks.len() >= 2 }).await;
    for sink in &sinks {
        sink.failed(DbError::Session { code: super::SessionCode::Unauthenticated, detail: "session_unknown".into() });
    }
    let first = within(received.recv()).await.expect("the connection that replaced the ended one delivers the failure");
    assert!(matches!(first, DbError::Session { detail, .. } if detail == "session_unknown"));
    assert!(within(async { tokio::time::sleep(Duration::from_millis(200)).await; received.try_recv().ok() }).await.is_none(), "the failure was delivered more than once");
    live.cancel();
}
