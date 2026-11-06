//! The loads of a relation through `Database` go through the owner `Windows`: win_idle_interest_load, win_idle_interest_fresh,
//! win_idle_load_more, win_idle_load_more_end, win_loading_join_waiter, win_loading_leave, win_idle_interest_offline (a refresh keeps its accepted fail-at-once rule).
//! The observable values are the requests of the transport, the snapshot with its revision and phase, and the result of the call.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Semaphore;

use super::{Config, Database, DbError, LoadMoreResult, ReadPhase, ReadRequest, ReadStream, ReadValue, TestClock, Transport};
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape, RemoteSpec};
use crate::value::ScalarKind;

const LIMIT: Duration = Duration::from_secs(5);
const LIST: &str = r#"{"model":"Moment","relation":"feed","params":{},"order":[{"field":"id","direction":"asc"}]}"#;

/// Two pages: the page `n` holds the row `m{n}` and the cursor `c{n}`, only the first page has a next one.
struct Pages {
    asked: Mutex<Vec<String>>,
    gate: Arc<Semaphore>,
    gated: bool,
    count: AtomicUsize,
    last_page: AtomicUsize,
    response: Mutex<Option<String>>,
    panics: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl Transport for Pages {
    async fn open_cable(&self, _: Arc<super::CableLease>, _url: String) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, request: super::TransportRequest) -> Result<String, DbError> {
        let number = self.count.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panics.load(Ordering::SeqCst), "the transport failed");
        self.asked.lock().unwrap().push(request.variables_json);
        if self.gated { self.gate.acquire().await.unwrap().forget(); }
        if let Some(response) = self.response.lock().unwrap().clone() { return Ok(response); }
        let has_next = number < self.last_page.load(Ordering::SeqCst);
        Ok(format!(r#"{{"data":{{"feed":{{"nodes":[{{"id":"m{number}","text":"t"}}],"pageInfo":{{"hasNextPage":{has_next},"endCursor":"c{number}"}}}}}}}}"#))
    }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, _: Arc<super::SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

impl Pages {
    fn requests(&self) -> usize { self.count.load(Ordering::SeqCst) }
}

async fn open(gated: bool) -> (Arc<Database>, Arc<Pages>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None },
        stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: true, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    model.relations.push(feed);
    let transport = Arc::new(Pages { asked: Mutex::new(Vec::new()), gate: Arc::new(Semaphore::new(0)), gated, count: AtomicUsize::new(0), last_page: AtomicUsize::new(1), response: Mutex::new(None), panics: std::sync::atomic::AtomicBool::new(false) });
    let db = Database::open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![model]).unwrap(), crate::ffi::OpenPorts { transport: Some(transport.clone()), ..Default::default() }, TestClock::new(1_000_000)).await.unwrap();
    db.register_operations(r#"[{"name":"Feed","document":"query Feed { feed { nodes { id text } pageInfo { hasNextPage endCursor } } }","rootPath":"feed"}]"#.into()).unwrap();
    db.set_online(true).unwrap();
    (db, transport)
}

async fn first_value(db: &Database) -> (Arc<ReadStream>, ReadValue) {
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::List { query_json: LIST.into() });
    let value = tokio::time::timeout(LIMIT, stream.pull()).await.expect("a value within 5 s").unwrap().expect("a value");
    (stream, value)
}

async fn begin(db: &Database) -> Arc<super::loading::LoadRequest> { db.begin_load(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).unwrap() }

async fn refresh(db: &Database) -> Result<(), DbError> { db.refresh(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await }

async fn more(db: &Database) -> Result<LoadMoreResult, DbError> { db.load_more(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await }

#[tokio::test(flavor = "multi_thread")]
async fn a_load_asks_the_first_page_once_and_the_snapshot_shows_the_revision_of_its_landing() {
    let (db, transport) = open(false).await;
    tokio::time::timeout(LIMIT, begin(&db).await.result()).await.expect("the load ends within 5 s").unwrap();
    assert_eq!(transport.requests(), 1);
    let (_stream, ReadValue::Snapshot { revision, rows, state }) = first_value(&db).await else { panic!("the first value is a snapshot") };
    assert_eq!(rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(), ["m0"]);
    assert_eq!((state.phase, state.loaded), (ReadPhase::Loaded, Some(revision)));
    tokio::time::timeout(LIMIT, begin(&db).await.result()).await.expect("the load ends within 5 s").unwrap();
    assert_eq!(transport.requests(), 1, "a fresh window answers a second load without a request");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_asks_the_first_page_again_even_when_the_window_is_fresh() {
    let (db, transport) = open(false).await;
    tokio::time::timeout(LIMIT, begin(&db).await.result()).await.unwrap().unwrap();
    tokio::time::timeout(LIMIT, refresh(&db)).await.expect("the refresh ends within 5 s").unwrap();
    assert_eq!(transport.requests(), 2);
    let asked = transport.asked.lock().unwrap().clone();
    assert_eq!(asked[0], asked[1], "both requests ask the first page");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_load_more_asks_the_page_after_the_cursor_and_the_last_page_says_no_more() {
    let (db, transport) = open(false).await;
    tokio::time::timeout(LIMIT, begin(&db).await.result()).await.unwrap().unwrap();
    assert_eq!(tokio::time::timeout(LIMIT, more(&db)).await.expect("the call ends within 5 s").unwrap(), LoadMoreResult::Loaded { has_more: false });
    assert_eq!(transport.requests(), 2);
    let asked = transport.asked.lock().unwrap().clone();
    assert!(asked[1].contains("c0"), "the second request carries the cursor of the first page: {asked:?}");
    let (_stream, ReadValue::Snapshot { rows, .. }) = first_value(&db).await else { panic!("the first value is a snapshot") };
    assert_eq!(rows.len(), 2, "both pages are in the window");
    assert_eq!(more(&db).await.unwrap(), LoadMoreResult::NoMore);
    assert_eq!(transport.requests(), 2, "the end of the pages asks nothing");
}

#[tokio::test(flavor = "multi_thread")]
async fn inv_loadm_18_win_loading_load_more_reports_the_page_it_landed() {
    let (db, transport) = open(true).await;
    transport.last_page.store(2, Ordering::SeqCst);
    let first = begin(&db).await;
    crate::test_wait::yield_until("the first page left", || transport.requests() == 1).await;
    let mut next = std::pin::pin!(more(&db));
    assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(next.as_mut().poll(cx))).await.is_pending());
    transport.gate.add_permits(2);
    crate::test_wait::within(first.result()).await.unwrap();
    assert_eq!(crate::test_wait::within(next).await.unwrap(), LoadMoreResult::Loaded { has_more: true });
    assert_eq!(transport.requests(), 2);
    transport.gate.add_permits(1);
    assert_eq!(crate::test_wait::within(more(&db)).await.unwrap(), LoadMoreResult::Loaded { has_more: false });
    assert_eq!(crate::test_wait::within(more(&db)).await.unwrap(), LoadMoreResult::NoMore);
    let asked = transport.asked.lock().unwrap().clone();
    assert_eq!(asked.len(), 3);
    assert!(asked[1].contains("c0") && asked[2].contains("c1"), "each call asks its next page: {asked:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn win_loading_error_preserves_graphql_messages_and_extensions() {
    for (next, code) in [(false, "FORBIDDEN"), (true, "FORBIDDEN"), (false, "INVALID_CURSOR"), (true, "INVALID_CURSOR")] {
        let (db, transport) = open(false).await;
        crate::test_wait::within(begin(&db).await.result()).await.unwrap();
        let response = serde_json::json!({"errors": [
            {"message": "first refusal", "extensions": {"code": code, "transactionDisposition": "FINISH", "field": "feed"}},
            {"message": "second refusal"}
        ]}).to_string();
        let expected = crate::transport::graphql_extensions(response.clone()).unwrap();
        *transport.response.lock().unwrap() = Some(response);
        let error = if next { crate::test_wait::within(more(&db)).await.unwrap_err() } else { crate::test_wait::within(refresh(&db)).await.unwrap_err() };
        let DbError::Graphql { messages, extensions } = error else { panic!("the call must preserve the GraphQL error: {error:?}") };
        assert_eq!(messages, ["first refusal", "second refusal"]);
        assert_eq!(extensions, expected);
        assert_eq!(transport.requests(), if code == "INVALID_CURSOR" { 3 } else { 2 });
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn two_loads_of_one_window_share_one_request_and_a_cancel_of_one_leaves_the_other() {
    let (db, transport) = open(true).await;
    let (first, second) = (begin(&db).await, begin(&db).await);
    crate::test_wait::yield_until("the request left", || transport.requests() == 1).await;
    first.cancel();
    assert!(matches!(first.result().await, Err(DbError::Interrupted { .. })), "a cancelled call ends as interrupted");
    transport.gate.add_permits(1);
    tokio::time::timeout(LIMIT, second.result()).await.expect("the second call ends within 5 s").unwrap();
    assert_eq!(transport.requests(), 1, "both calls waited for the same request");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_load_without_the_network_waits_and_asks_when_the_network_returns_while_a_refresh_fails_at_once() {
    let (db, transport) = open(false).await;
    db.set_online(false).unwrap();
    let call = begin(&db).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(transport.requests(), 0, "no request leaves while the network is away");
    assert!(matches!(refresh(&db).await, Err(DbError::Network { .. })), "a refresh needs the network now");
    db.set_online(true).unwrap();
    tokio::time::timeout(LIMIT, call.result()).await.expect("the load ends within 5 s").unwrap();
    assert_eq!(transport.requests(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_in_the_window_transport_ends_the_refresh_with_an_error_and_closes_the_database() {
    let (db, transport) = open(false).await;
    transport.panics.store(true, Ordering::SeqCst);
    let result = crate::test_wait::within(refresh(&db)).await;
    assert!(result.is_err(), "{result:?}");
    crate::test_wait::yield_until("Lifecycle did not close after the task panic", || db.life.published().closed).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_in_the_window_read_ends_the_refresh_with_an_error_and_closes_the_database() {
    let (db, transport) = open(false).await;
    db.db.panic_reads.store(crate::store::PANIC_WINDOW_READ, Ordering::SeqCst);
    let result = crate::test_wait::within(refresh(&db)).await;
    assert!(result.is_err(), "{result:?}");
    assert_eq!(transport.requests(), 0, "the failed window read sent nothing");
    crate::test_wait::yield_until("Lifecycle did not close after the window read panic", || db.life.published().closed).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_in_the_snapshot_read_ends_the_wait_of_the_stream_with_an_error_and_closes_the_database() {
    let (db, _transport) = open(false).await;
    db.db.panic_reads.store(crate::store::PANIC_VIEW_READ, Ordering::SeqCst);
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::List { query_json: LIST.into() });
    let value = crate::test_wait::within(stream.pull()).await;
    assert!(matches!(&value, Ok(Some(ReadValue::Snapshot { state, .. })) if state.phase == ReadPhase::Failed) || value.is_err(), "the stream ends its wait with an error: {value:?}");
    crate::test_wait::yield_until("Lifecycle did not close after the snapshot read panic", || db.life.published().closed).await;
}
