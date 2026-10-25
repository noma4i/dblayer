use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::sync::Notify;

use super::{Config, Database, DbError, LiveChange, QueryListener, TestClock, Transport};
use crate::identity::RowId;
use crate::schema::{Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape, RemoteSpec};
use crate::value::ScalarKind;

pub(super) struct Counting {
    queries: AtomicUsize,
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
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> {
        if let Some(gate) = &self.gate { gate.acquire().await.unwrap().forget(); }
        self.queries.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        if let Some(hold) = &self.hold { hold.acquire().await.unwrap().forget(); }
        Ok(r#"{"data":{"moments":[{"id":"m1","text":"a"}]}}"#.into())
    }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, sink: Arc<super::SubscriptionSink>) -> Result<(), DbError> {
        sink.opened("notice".into())?;
        sink.ready();
        if let Some(sender) = &self.subscriptions { sender.send(sink).unwrap(); }
        Ok(())
    }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: Vec<u8>, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

struct Silent;
impl QueryListener for Silent {
    fn changed(&self, _: LiveChange) {}
}

#[tokio::test]
async fn a_terminal_subscription_failure_reaches_the_query_listener_once_without_http() {
    struct Listener(std::sync::mpsc::Sender<DbError>);
    impl QueryListener for Listener {
        fn changed(&self, change: LiveChange) {
            if let Some(error) = change.error { self.0.send(error).unwrap(); }
        }
    }
    for shared_changes in [false, true] {
    let (opened, subscriptions) = std::sync::mpsc::channel();
    let transport = Arc::new(Counting { queries: AtomicUsize::new(0), started: Notify::new(), hold: None, gate: None, subscriptions: Some(opened) });
    let mut model = ModelSpec::new("Notice", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    model.storage = crate::schema::Storage::Memory;
    let mut relation = if shared_changes { relation("recent", "5.minutes") } else { RelationSpec::global("recent") };
    relation.updates.push(crate::schema::UpdatesSpec { apply: None, subscription: "Notices".into(), deleted_by: None, set: Map::new(), removes: false, debounce: None, root: None, id: None });
    if shared_changes { relation.updates.clear(); }
    model.relations.push(relation);
    let mut models = vec![model];
    if shared_changes {
        models[0].changes.push(crate::schema::ChangeRead { area: "NOTICE".into(), relation: "recent".into(), parameter: None, removes_on: vec![] });
        let mut other = models[0].clone();
        other.key = crate::identity::ModelKey::new("AnotherNotice");
        models.push(other);
        models[0].change_feed = Some(crate::schema::ChangeFeed { subscription: "Notices".into(), root: "notices.entries".into(), area: "topic".into(), kind: "action".into(), id: "recordId".into() });
    }
    let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&models).unwrap(), Some(transport.clone()), None, None, None).await.unwrap();
    database.register_operations(r#"[{"name":"Notices","document":"subscription Notices { notices { id } }","rootPath":"notices"},{"name":"Feed","document":"query Feed { moments { id } }","rootPath":"moments"}]"#.into()).unwrap();
    let (failures, received) = std::sync::mpsc::channel();
    let live = database.live_query(r#"{"model":"Notice","relation":"recent","params":{}}"#.into(), Arc::new(Listener(failures)));
    live.wait_for_initial_load().await.unwrap();
    let initial_reads = transport.queries.load(Ordering::SeqCst);
    let sink = subscriptions.recv_timeout(Duration::from_secs(2)).unwrap();
    sink.failed(DbError::Session { code: super::SessionCode::Unauthenticated, detail: "session_unknown".into() });
    let error = received.recv_timeout(Duration::from_secs(2)).expect("query listener receives terminal subscription failure");
    assert!(matches!(error, DbError::Session { code: super::SessionCode::Unauthenticated, detail } if detail == "session_unknown"));
    assert!(subscriptions.try_recv().is_err());
    assert_eq!(transport.queries.load(Ordering::SeqCst), initial_reads);
    database.listen().unwrap();
    let resumed = subscriptions.recv_timeout(Duration::from_secs(2)).expect("listen restarts the stopped subscription");
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
    relation.updates.push(crate::schema::UpdatesSpec { apply: None, subscription:"BulletinAdded".into(), deleted_by:None, set:Map::new(), removes:false, debounce:None, root:None, id:None });
    model.relations.push(relation);
    let transport = Arc::new(Counting { queries:AtomicUsize::new(0), started:Notify::new(), hold: None, gate: None, subscriptions: None });
    let db = Database::open(Config { path:None, data_version:None, cache_rows:8 }, serde_json::to_string(&vec![model]).unwrap(), Some(transport), None, None, None).await.unwrap();
    db.register_operations(r#"[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body } }","rootPath":"bulletinAdded"}]"#.into()).unwrap();
    for window in [false, true] {
        let query = r#"{"model":"Notice","relation":"recent","params":{},"order":[{"field":"body","direction":"asc"}]}"#.into();
        let live = if window { db.live_window(query, 10, Arc::new(Silent)) } else { db.live_query(query, Arc::new(Silent)) };
        let handles = live.handles.lock().unwrap();
        assert!(handles.as_ref().unwrap()._watch.is_some(), "window={window}");
        assert!(handles.as_ref().unwrap().load.is_none());
    }
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

async fn held_rig() -> (Arc<Database>, Arc<Counting>, Arc<tokio::sync::Semaphore>) {
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

async fn open_rig(clock: Option<Arc<TestClock>>, hold: Option<Arc<tokio::sync::Semaphore>>, gate: Option<Arc<tokio::sync::Semaphore>>) -> (Arc<Database>, Arc<Counting>, Option<Arc<TestClock>>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    model.relations.extend([relation("feed", "5.minutes"), relation("quick", "200.milliseconds")]);
    let transport = Arc::new(Counting { queries: AtomicUsize::new(0), started: Notify::new(), hold: hold.clone(), gate, subscriptions: None });
    let config = Config { path: None, data_version: None, cache_rows: 8 };
    let models = serde_json::to_string(&vec![model]).unwrap();
    let transport_handle = Some(transport.clone() as Arc<dyn Transport>);
    let db = match &clock {
        Some(clock) => Database::open_with_clock(config, models, transport_handle, clock.clone()).await,
        None => Database::open(config, models, transport_handle, None, None, None).await,
    }
    .unwrap();
    db.register_operations(r#"[{"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"}]"#.into()).unwrap();
    (db, transport, clock)
}

async fn load(db: &Database, relation: &str) -> Result<(), DbError> { db.load("Moment".into(), relation.into(), "{}".into()).await }
fn stored(db: &Database) -> bool { db.db.find("Moment", &RowId::new("m1").unwrap()).unwrap().is_some() }
fn queries(transport: &Counting) -> usize { transport.queries.load(Ordering::SeqCst) }

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

    let key = db.sync().unwrap().window_key("Moment", "feed", &Map::<String, Value>::new()).unwrap();
    db.db.shared.store.invalidate_window(&key).unwrap();
    load(&db, "feed").await.unwrap();
    assert_eq!(queries(&transport), 4, "invalidated");
}

#[tokio::test]
async fn inv_fresh_13_load_during_a_live_connect_shares_its_request() {
    let (db, transport, hold) = held_rig().await;
    let _live = db.live_query(r#"{"model":"Moment","relation":"feed","params":{}}"#.into(), Arc::new(Silent));
    transport.started.notified().await;
    let already = db.sync().unwrap().joined_loads();
    let sharing = tokio::spawn({ let db = db.clone(); async move { load(&db, "feed").await } });
    joined_the_flight(&db, &transport, already).await;
    hold.add_permits(1);
    sharing.await.unwrap().unwrap();
    assert_eq!(queries(&transport), 1);
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
            transport.started.notified().await;
        }
        let before = queries(&transport);
        tokio::spawn({ let gate = gate.clone(); async move { tokio::time::sleep(Duration::from_secs(3)).await; gate.add_permits(1); } });
        let live = db.live_query(r#"{"model":"Moment","relation":"feed","params":{},"refreshOnConnect":true}"#.into(), Arc::new(Silent));
        if preloaded {
            assert_eq!(live.current().await.size, 1, "cached rows stay visible during the forced load");
        }
        transport.started.notified().await;
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
    let live = db.live_query(r#"{"model":"Moment","relation":"feed"}"#.into(), Arc::new(Silent));
    transport.started.notified().await;
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
    transport.started.notified().await;
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
