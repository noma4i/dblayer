//! The loads of a relation through `Database` go through the owner `Windows`: win_idle_interest_load, win_idle_interest_fresh,
//! win_idle_load_more, win_idle_load_more_end, win_loading_join_waiter, win_loading_leave, win_idle_interest_offline (a refresh keeps its accepted fail-at-once rule).
//! The observable values are the requests of the transport, the snapshot with its revision and phase, and the result of the call.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;

use super::{Config, Database, DbError, LoadMoreResult, ReadPhase, ReadRequest, ReadStream, ReadValue, TestClock, FakeTransport};
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape, RemoteSpec};
use crate::value::ScalarKind;

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
impl FakeTransport for Pages {
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
    let db = Database::open_with_clock(Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), crate::ffi::OpenPorts { transport: Some(transport.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, TestClock::new(1_000_000)).await.unwrap();
    db.register_operations(r#"[{"name":"Feed","document":"query Feed { feed { nodes { id text } pageInfo { hasNextPage endCursor } } }","rootPath":"feed"}]"#.into()).await.unwrap();
    db.set_online(true).unwrap();
    (db, transport)
}

async fn first_value(db: &Database) -> (Arc<ReadStream>, ReadValue) {
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::List { query: crate::ffi::QuerySpec::json(LIST) });
    let value = crate::test_wait::within(stream.pull_next()).await.expect("a value");
    (stream, value)
}

async fn begin(db: &Database) -> Arc<super::loading::LoadRequest> { db.begin_load(db.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into()).unwrap() }

async fn refresh(db: &Database) -> Result<(), DbError> { db.refresh(db.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into()).await }

async fn more(db: &Database) -> Result<LoadMoreResult, DbError> { db.load_more(db.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into()).await }

#[tokio::test(flavor = "multi_thread")]
async fn a_load_asks_the_first_page_once_and_the_snapshot_shows_the_revision_of_its_landing() {
    let (db, transport) = open(false).await;
    crate::test_wait::within(begin(&db).await.result()).await.unwrap();
    assert_eq!(transport.requests(), 1);
    let (_stream, ReadValue::Snapshot { revision, rows, state, .. }) = first_value(&db).await else { panic!("the first value is a snapshot") };
    assert_eq!(rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(), ["m0"]);
    assert_eq!((state.phase, state.loaded), (ReadPhase::Loaded, Some(revision)));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_asks_the_first_page_again_even_when_the_window_is_fresh() {
    let (db, transport) = open(false).await;
    crate::test_wait::within(begin(&db).await.result()).await.unwrap();
    crate::test_wait::within(refresh(&db)).await.unwrap();
    assert_eq!(transport.requests(), 2);
    let asked = transport.asked.lock().unwrap().clone();
    assert_eq!(asked[0], asked[1], "both requests ask the first page");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_load_more_asks_the_page_after_the_cursor_and_the_last_page_says_no_more() {
    let (db, transport) = open(false).await;
    crate::test_wait::within(begin(&db).await.result()).await.unwrap();
    assert_eq!(crate::test_wait::within(more(&db)).await.unwrap(), LoadMoreResult::Loaded { has_more: false });
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
    crate::test_wait::settled(&db, "the first page left", || transport.requests() == 1).await;
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
async fn a_load_without_the_network_waits_and_asks_when_the_network_returns_while_a_refresh_fails_at_once() {
    let (db, transport) = open(false).await;
    db.set_online(false).unwrap();
    let call = begin(&db).await;
    crate::test_wait::within(crate::ffi::settled(&db)).await.unwrap();
    assert_eq!(transport.requests(), 0, "no request leaves while the network is away");
    db.set_online(true).unwrap();
    crate::test_wait::within(call.result()).await.unwrap();
    assert_eq!(transport.requests(), 1);
}

/// net_prop_offline_decided_by_owner: `Err(Offline)` is the answer of Windows to its own `online`, `ffi` has no precheck.
#[tokio::test(flavor = "multi_thread")]
async fn net_prop_offline_decided_by_owner() {
    let (db, transport) = open(false).await;
    db.set_online(false).unwrap();
    assert!(matches!(refresh(&db).await, Err(DbError::Offline { detail }) if detail == "Moment.feed refresh needs the network"));
    assert!(matches!(more(&db).await, Err(DbError::Offline { detail }) if detail == "Moment.feed load more needs the network"));
    assert_eq!(transport.requests(), 0, "no request leaves while the owner is offline");
    db.set_online(true).unwrap();
    crate::test_wait::within(refresh(&db)).await.unwrap();
    assert_eq!(transport.requests(), 1, "the owner took the call once it was online");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_in_the_window_transport_ends_the_refresh_with_an_error_and_closes_the_database() {
    let (db, transport) = open(false).await;
    transport.panics.store(true, Ordering::SeqCst);
    let result = crate::test_wait::within(refresh(&db)).await;
    assert!(matches!(result, Err(DbError::Runtime { .. } | DbError::Interrupted { .. })), "{result:?}");
    crate::test_wait::settled(&db, "Lifecycle did not close after the task panic", || db.life.published().closed).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_in_the_window_read_ends_the_refresh_with_an_error_and_closes_the_database() {
    let (db, _transport) = open(false).await;
    db.db.panic_reads.store(crate::store::PANIC_WINDOW_READ, Ordering::SeqCst);
    let result = crate::test_wait::within(refresh(&db)).await;
    assert!(matches!(result, Err(DbError::Closed { .. })), "the panic closes the database, the wait ends Closed (D-263, P-RT-12): {result:?}");
    crate::test_wait::settled(&db, "Lifecycle did not close after the window read panic", || db.life.published().closed).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_in_the_snapshot_read_ends_the_wait_of_the_stream_with_an_error_and_closes_the_database() {
    let (db, _transport) = open(false).await;
    db.db.panic_reads.store(crate::store::PANIC_VIEW_READ, Ordering::SeqCst);
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::List { query: crate::ffi::QuerySpec::json(LIST) });
    let value = crate::test_wait::within(stream.pull_next()).await;
    assert!(matches!(&value, Some(ReadValue::Snapshot { state, .. }) if state.phase == ReadPhase::Failed) || matches!(&value, Some(ReadValue::Ended { .. })), "the stream ends its wait with an error or its end: {value:?}");
    crate::test_wait::settled(&db, "Lifecycle did not close after the snapshot read panic", || db.life.published().closed).await;
}

/// P-DATA-39 (R18): a stream loads the next page of its own relation and its first page again, and a stream without a relation refuses both.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_loads_more_and_refreshes_its_own_relation() {
    let (db, transport) = open(false).await;
    crate::test_wait::within(begin(&db).await.result()).await.unwrap();
    let (stream, _) = first_value(&db).await;
    assert_eq!(crate::test_wait::within(stream.load_more()).await.unwrap(), LoadMoreResult::Loaded { has_more: false });
    assert_eq!(transport.requests(), 2);
    assert!(transport.asked.lock().unwrap()[1].contains("c0"), "the next page follows the cursor of the first");
    assert_eq!(crate::test_wait::within(stream.load_more()).await.unwrap(), LoadMoreResult::NoMore);
    crate::test_wait::within(stream.refresh()).await.unwrap();
    assert_eq!(transport.requests(), 3, "the refresh asks the first page again");

    let entity = db.subscribe_values(db.session_epoch(), ReadRequest::Entity { model: "Moment".into(), id: "1".into(), fetch: super::ReadFetch::Never });
    assert!(matches!(crate::test_wait::within(entity.refresh()).await, Err(DbError::Invalid { detail }) if detail.contains("no relation")));
    assert!(matches!(crate::test_wait::within(entity.load_more()).await, Err(DbError::Invalid { .. })));
}

/// A database without a transfer owner shows a `Blob` stream as gone at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_blob_stream_without_a_transfer_owner_is_gone() {
    let (db, _) = open(false).await;
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::Blob { model: "Moment".into(), id: "m1".into(), action: "upload".into() });
    match crate::test_wait::within(stream.pull_next()).await.expect("a value") {
        ReadValue::Status { state, progress, .. } => assert_eq!((state.as_str(), progress), ("gone", 0)),
        other => panic!("{other:?}"),
    }
}

/// D-172 (P-BND-04): a row the stream cannot look up shows `failed` with the error of the lookup, not an empty status.
#[tokio::test(flavor = "multi_thread")]
async fn boundary_value_orders() {
    let (db, _) = open(false).await;
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::Blob { model: "Moment".into(), id: " ".into(), action: "upload".into() });
    loop {
        if let ReadValue::Status { state, error, .. } = crate::test_wait::within(stream.pull_next()).await.expect("a value") {
            assert_eq!(state, "failed");
            assert!(error.is_some_and(|error| !error.is_empty()));
            break;
        }
    }
}

/// P-NET-08: the owner of the window decides a call that needs the network, the `Connectivity` value of the Store decides nothing.
#[tokio::test(flavor = "multi_thread")]
async fn life_closed_stream_releases_store() {
    let (db, _transport) = open(false).await;
    let (stream, _first) = first_value(&db).await;
    db.close_database().await.unwrap();
    assert!(matches!(crate::test_wait::within(stream.refresh()).await, Err(DbError::Closed { .. })));
    assert!(matches!(crate::test_wait::within(stream.load_more()).await, Err(DbError::Closed { .. })));
    assert!(matches!(crate::test_wait::within(stream.pull_next()).await, Some(ReadValue::Ended { cause: super::EndCause::Closed })));
    let (database, store) = (Arc::downgrade(&db), Arc::downgrade(&db.db));
    let owners = db.life.owners_handle();
    drop(db);
    crate::test_wait::settled(&owners, "the stream keeps the Database", || database.upgrade().is_none()).await;
    crate::test_wait::settled(&owners, "the stream keeps the Store", || store.upgrade().is_none()).await;
    drop(stream);
}
