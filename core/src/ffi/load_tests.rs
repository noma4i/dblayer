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
}

#[async_trait::async_trait]
impl Transport for Counting {
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        tokio::time::sleep(self.delay).await;
        Ok(r#"{"data":{"moments":[{"id":"m1","text":"a"}]}}"#.into())
    }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, _: Arc<super::SubscriptionSink>) -> Result<String, DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: Vec<u8>, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

struct Silent;
impl QueryListener for Silent {
    fn changed(&self, _: LiveChange) {}
}

fn relation(name: &str, stale: &str) -> RelationSpec {
    let mut relation = RelationSpec::global(name);
    relation.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: stale.into(),
        empty_stale_time: None, selects_page_info: false, updates: Vec::new(), page_size: None, variables: Default::default(), root: None,
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
    let transport = Arc::new(Counting { queries: AtomicUsize::new(0), started: Notify::new(), delay });
    let config = Config { path: None, data_version: None, cache_rows: 8 };
    let models = serde_json::to_string(&vec![model]).unwrap();
    let transport_handle = Some(transport.clone() as Arc<dyn Transport>);
    let db = match &clock {
        Some(clock) => Database::open_with_clock(config, models, transport_handle, clock.clone()),
        None => Database::open(config, models, transport_handle),
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
