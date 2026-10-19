use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::sync::Notify;

use super::{Config, Database, DbError, LiveChange, QueryListener, TestClock, Transport};
use crate::identity::RowId;
use crate::schema::{Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape, RemoteSpec};
use crate::value::ScalarKind;

struct Counting {
    queries: AtomicUsize,
    started: Notify,
    delay: Duration,
    subscriptions: Option<std::sync::mpsc::Sender<Arc<super::SubscriptionSink>>>,
}

#[async_trait::async_trait]
impl Transport for Counting {
    async fn open_cable(&self) -> Result<String, DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        tokio::time::sleep(self.delay).await;
        Ok(r#"{"data":{"moments":[{"id":"m1","text":"a"}]}}"#.into())
    }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, sink: Arc<super::SubscriptionSink>) -> Result<String, DbError> {
        sink.ready();
        if let Some(sender) = &self.subscriptions { sender.send(sink).unwrap(); }
        Ok("notice".into())
    }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: Vec<u8>, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

struct Silent;
impl QueryListener for Silent {
    fn changed(&self, _: LiveChange) {}
}

#[test]
fn a_terminal_subscription_failure_reaches_the_query_listener_once_without_http() {
    struct Listener(std::sync::mpsc::Sender<DbError>);
    impl QueryListener for Listener {
        fn changed(&self, change: LiveChange) {
            if let Some(error) = change.error { self.0.send(error).unwrap(); }
        }
    }
    for shared_changes in [false, true] {
    let (opened, subscriptions) = std::sync::mpsc::channel();
    let transport = Arc::new(Counting { queries: AtomicUsize::new(0), started: Notify::new(), delay: Duration::ZERO, subscriptions: Some(opened) });
    let mut model = ModelSpec::new("Notice", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    model.storage = crate::schema::Storage::Memory;
    let mut relation = if shared_changes { relation("recent", "5.minutes") } else { RelationSpec::global("recent") };
    relation.updates.push(crate::schema::UpdatesSpec { subscription: "Notices".into(), deleted_by: None, set: Map::new(), removes: false, debounce: None, root: None, id: None });
    if shared_changes { relation.updates.clear(); }
    model.relations.push(relation);
    let mut models = vec![model];
    if shared_changes {
        models[0].changes.push(crate::schema::ChangeRead { area: "NOTICE".into(), relation: "recent".into(), parameter: None, removes_on: vec![] });
        let mut other = models[0].clone();
        other.key = crate::identity::ModelKey::new("AnotherNotice");
        models.push(other);
    }
    let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&models).unwrap(), Some(transport.clone()), None, None, None).unwrap();
    database.register_operations(r#"[{"name":"Notices","document":"subscription Notices { notices { id } }","rootPath":"notices"},{"name":"Feed","document":"query Feed { moments { id } }","rootPath":"moments"}]"#.into()).unwrap();
    let (failures, received) = std::sync::mpsc::channel();
    let live = database.live_query(r#"{"model":"Notice","relation":"recent","params":{}}"#.into(), Arc::new(Listener(failures))).unwrap();
    super::database::executor().unwrap().block_on(live.wait_for_initial_load()).unwrap();
    let initial_reads = transport.queries.load(Ordering::SeqCst);
    let sink = subscriptions.recv_timeout(Duration::from_secs(2)).unwrap();
    sink.failed(DbError::Session { code: super::SessionCode::Unauthenticated, detail: "session_unknown".into() });
    let error = received.recv_timeout(Duration::from_secs(2)).expect("query listener receives terminal subscription failure");
    assert!(matches!(error, DbError::Session { code: super::SessionCode::Unauthenticated, detail } if detail == "session_unknown"));
    assert!(received.recv_timeout(Duration::from_millis(50)).is_err());
    assert!(subscriptions.try_recv().is_err());
    assert_eq!(transport.queries.load(Ordering::SeqCst), initial_reads);
    database.listen().unwrap();
    let resumed = subscriptions.recv_timeout(Duration::from_secs(2)).expect("listen restarts the stopped subscription");
    resumed.failed(DbError::Session { code: super::SessionCode::Unauthenticated, detail: "second_session".into() });
    assert!(matches!(received.recv_timeout(Duration::from_secs(2)).expect("observer receives the next session failure"), DbError::Session { detail, .. } if detail == "second_session"));
    live.cancel();
    }
}

#[test]
fn a_local_memory_live_query_keeps_a_subscription_watch_without_an_http_load() {
    let mut model = ModelSpec::new("Notice", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("body", ScalarKind::Str)]);
    model.storage = crate::schema::Storage::Memory;
    let mut relation = RelationSpec::global("recent");
    relation.updates.push(crate::schema::UpdatesSpec { subscription:"ViewerNotice".into(), deleted_by:None, set:Map::new(), removes:false, debounce:None, root:None, id:None });
    model.relations.push(relation);
    let transport = Arc::new(Counting { queries:AtomicUsize::new(0), started:Notify::new(), delay:Duration::ZERO, subscriptions: None });
    let db = Database::open(Config { path:None, data_version:None, cache_rows:8 }, serde_json::to_string(&vec![model]).unwrap(), Some(transport), None, None, None).unwrap();
    db.register_operations(r#"[{"name":"ViewerNotice","document":"subscription ViewerNotice { viewerNotice { body } }","rootPath":"viewerNotice"}]"#.into()).unwrap();
    for window in [false, true] {
        let query = r#"{"model":"Notice","relation":"recent","params":{},"order":[{"field":"body","direction":"asc"}]}"#.into();
        let live = if window { db.live_window(query, 10, Arc::new(Silent)) } else { db.live_query(query, Arc::new(Silent)) }.unwrap();
        let handles = live.handles.lock().unwrap();
        assert!(handles.as_ref().unwrap()._watch.is_some(), "window={window}");
        assert!(handles.as_ref().unwrap().load.is_none());
    }
}

fn relation(name: &str, stale: &str) -> RelationSpec {
    let mut relation = RelationSpec::global(name);
    relation.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: stale.into(),
        empty_stale_time: None, selects_page_info: false, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    relation
}

fn rig(delay: Duration) -> (Arc<Database>, Arc<Counting>) {
    let (db, transport, _) = open_rig(delay, None);
    (db, transport)
}

fn rig_with_clock() -> (Arc<Database>, Arc<Counting>, Arc<TestClock>) {
    let clock = TestClock::new(1_000_000);
    let (db, transport, _) = open_rig(Duration::ZERO, Some(clock.clone()));
    (db, transport, clock)
}

fn open_rig(delay: Duration, clock: Option<Arc<TestClock>>) -> (Arc<Database>, Arc<Counting>, Option<Arc<TestClock>>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    model.relations.extend([relation("feed", "5.minutes"), relation("quick", "200.milliseconds")]);
    let transport = Arc::new(Counting { queries: AtomicUsize::new(0), started: Notify::new(), delay, subscriptions: None });
    let config = Config { path: None, data_version: None, cache_rows: 8 };
    let models = serde_json::to_string(&vec![model]).unwrap();
    let transport_handle = Some(transport.clone() as Arc<dyn Transport>);
    let db = match &clock {
        Some(clock) => Database::open_with_clock(config, models, transport_handle, clock.clone()),
        None => Database::open(config, models, transport_handle, None, None, None),
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
    let (db, transport) = rig(Duration::ZERO);
    load(&db, "feed").await.unwrap();
    assert_eq!(queries(&transport), 1, "never loaded");
    load(&db, "feed").await.unwrap();
    assert_eq!(queries(&transport), 1, "fresh");

    load(&db, "quick").await.unwrap();
    assert_eq!(queries(&transport), 2);
    tokio::time::sleep(Duration::from_millis(300)).await;
    load(&db, "quick").await.unwrap();
    assert_eq!(queries(&transport), 3, "fresh_for elapsed");

    let key = db.sync().unwrap().window_key("Moment", "feed", &Map::<String, Value>::new()).unwrap();
    db.db.shared.store.invalidate_window(&key).unwrap();
    load(&db, "feed").await.unwrap();
    assert_eq!(queries(&transport), 4, "invalidated");
}

#[tokio::test]
async fn inv_fresh_13_parallel_loads_make_one_request_and_all_return_after_it() {
    let (db, transport) = rig(Duration::from_millis(150));
    let mut loads = tokio::task::JoinSet::new();
    for _ in 0..10 {
        let db = db.clone();
        loads.spawn(async move { load(&db, "feed").await.map(|()| stored(&db)) });
    }
    while let Some(done) = loads.join_next().await {
        assert!(done.unwrap().unwrap(), "returned before the rows landed");
    }
    assert_eq!(queries(&transport), 1);
}

#[tokio::test]
async fn inv_fresh_13_load_during_a_live_connect_shares_its_request() {
    let (db, transport) = rig(Duration::from_millis(150));
    let _live = db.live_query(r#"{"model":"Moment","relation":"feed","params":{}}"#.into(), Arc::new(Silent)).unwrap();
    transport.started.notified().await;
    load(&db, "feed").await.unwrap();
    assert_eq!(queries(&transport), 1);
}

#[tokio::test]
async fn refreshing_on_connect_fetches_once_even_when_the_window_is_fresh() {
    for preloaded in [false, true] {
        let (db, transport) = rig(Duration::from_millis(20));
        if preloaded {
            load(&db, "feed").await.unwrap();
            transport.started.notified().await;
        }
        let before = queries(&transport);
        let live = db.live_query(r#"{"model":"Moment","relation":"feed","params":{},"refreshOnConnect":true}"#.into(), Arc::new(Silent)).unwrap();
        if preloaded {
            assert_eq!(live.current().await.rows.len(), 1, "cached rows stay visible during the forced load");
        }
        let started = tokio::time::timeout(Duration::from_secs(2), transport.started.notified()).await;
        assert!(started.is_ok(), "preloaded={preloaded}, requests={}, expected=1", queries(&transport) - before);
        live.wait_for_initial_load().await.unwrap();
        assert!(stored(&db));
        load(&db, "feed").await.unwrap();
        assert_eq!(queries(&transport) - before, 1, "preloaded={preloaded}");
        live.cancel();
    }
}

#[tokio::test]
async fn cancelling_a_live_query_finishes_its_initial_wait() {
    let (db, transport) = rig(Duration::from_secs(30));
    let live = db.live_query(r#"{"model":"Moment","relation":"feed"}"#.into(), Arc::new(Silent)).unwrap();
    transport.started.notified().await;
    live.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), live.wait_for_initial_load()).await.unwrap();
    assert!(matches!(result, Err(DbError::Interrupted { .. })));
}

#[tokio::test]
async fn inv_fresh_14_load_keeps_no_subscription() {
    let (db, _transport) = rig(Duration::ZERO);
    let before = db.subscription_census();
    load(&db, "feed").await.unwrap();
    let after = db.subscription_census();
    assert_eq!((after.observed_queries, after.subscriptions), (before.observed_queries, before.subscriptions));
}

#[tokio::test]
async fn inv_fresh_13_a_cancelled_caller_does_not_cancel_the_load_another_waits_for() {
    let (db, transport) = rig(Duration::from_millis(150));
    let cancelled = tokio::spawn({ let db = db.clone(); async move { load(&db, "feed").await } });
    transport.started.notified().await;
    let waiter = tokio::spawn({ let db = db.clone(); async move { load(&db, "feed").await } });
    cancelled.abort();
    waiter.await.unwrap().unwrap();
    assert_eq!(queries(&transport), 1);
    assert!(stored(&db));
}

#[tokio::test]
async fn inv_fresh_15_a_test_clock_moves_fresh_for_without_sleeping() {
    for (advance_ms, expected) in [(5 * 60_000 + 1_000, 2), (5 * 60_000 - 1_000, 1)] {
        let (db, transport, clock) = rig_with_clock();
        load(&db, "feed").await.unwrap();
        clock.advance(advance_ms);
        load(&db, "feed").await.unwrap();
        assert_eq!(queries(&transport), expected, "advanced by {advance_ms} ms");
    }
}

#[derive(Debug, PartialEq)]
enum Heard {
    Rows(usize),
    State { settled: bool },
}

struct SlowRows(Arc<std::sync::Mutex<Vec<Heard>>>);
impl QueryListener for SlowRows {
    fn changed(&self, change: LiveChange) {
        if !change.rows.is_empty() { std::thread::sleep(Duration::from_millis(200)); }
        self.0.lock().unwrap().push(Heard::Rows(change.rows.len()));
    }
}

struct StateTape(Arc<std::sync::Mutex<Vec<Heard>>>);
impl super::StateListener for StateTape {
    fn changed(&self, state: super::RelationStateChange) {
        self.0.lock().unwrap().push(Heard::State { settled: state.loaded_at.is_some() });
    }
}

#[tokio::test]
async fn inv_observe_20_a_settled_state_follows_the_rows_that_the_load_wrote() {
    let (db, _) = rig(Duration::ZERO);
    let tape = Arc::new(std::sync::Mutex::new(Vec::new()));
    let query = r#"{"model":"Moment","relation":"feed","params":{}}"#;
    let _rows = db.live_query(query.into(), Arc::new(SlowRows(tape.clone()))).unwrap();
    let _state = db.live_state("Moment".into(), "feed".into(), "{}".into(), Arc::new(StateTape(tape.clone()))).unwrap();
    for _ in 0..100 {
        if tape.lock().unwrap().contains(&Heard::State { settled: true }) { break; }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let heard = tape.lock().unwrap();
    let settled = heard.iter().position(|item| *item == Heard::State { settled: true }).expect("the state settled");
    assert!(heard[..settled].contains(&Heard::Rows(1)), "the state settled before the rows of the load were delivered: {heard:?}");
}
