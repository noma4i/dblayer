use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;


use super::rig_tests::{bound, Gate, Rig};
use super::rig_held::Held;
use crate::ffi::tests::lifecycle_tests::Rows;
use crate::ffi::{ByteProgress, Config, Database, DbError, LoadMoreResult, OpenDatabase, PutReply, TestClock, FakeTransport, TransportRequest};
use crate::test_wait::settled;
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape, RemoteSpec, Storage};

use crate::value::ScalarKind;

struct Pages {
    queries: AtomicUsize,
    gates: Mutex<HashMap<usize, Arc<Gate>>>,
    clock: Arc<TestClock>,
}

#[async_trait]
impl FakeTransport for Pages {
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> {
        let number = self.queries.fetch_add(1, Ordering::SeqCst);
        let gate = self.gates.lock().unwrap().remove(&number);
        if let Some(gate) = gate { gate.pass().await; }
        Ok(format!(r#"{{"data":{{"feed":{{"nodes":[{{"id":"m{number}","text":"t"}}],"pageInfo":{{"hasNextPage":true,"endCursor":"c{number}"}}}}}}}}"#))
    }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { unreachable!() }
}

const FEED: &str = r#"{"model":"Moment","relation":"feed","params":{}}"#;

async fn open_pages() -> (Arc<Database>, Arc<Pages>) { open_pages_limited(None).await }

async fn open_pages_limited(max_pages: Option<u32>) -> (Arc<Database>, Arc<Pages>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages, cursor: None },
        stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: true, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    model.relations.push(feed);
    let clock = TestClock::new(1_000_000);
    let pages = Arc::new(Pages { queries: AtomicUsize::new(0), gates: Mutex::new(HashMap::new()), clock: clock.clone() });
    let database = OpenDatabase::new().open_with_clock(Config { path: None }, crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), crate::ffi::OpenPorts { partial_operations: true, transport: Some(pages.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, clock, crate::ffi::OpenOptions::default(), None).await.unwrap();
    database.register_operations(r#"[{"name":"Feed","document":"query Feed { feed { nodes { id text } pageInfo { hasNextPage endCursor } } }","rootPath":"feed"}]"#.into()).await.unwrap();
    database.set_online(true).await.unwrap();
    (database, pages)
}

#[derive(Default)]
struct StateRows {
    rows: Rows,
    has_more: Mutex<Option<bool>>,
}

impl crate::ffi::tape::Reader for StateRows {
    fn delivered(&self, delivery: crate::ffi::tape::StreamDelivery) {
        match &delivery.value {
            crate::ffi::ReadValue::Snapshot { state, .. } | crate::ffi::ReadValue::Delta { state, .. } | crate::ffi::ReadValue::State { state, .. } => *self.has_more.lock().unwrap() = Some(state.has_more),
            _ => {}
        }
        crate::ffi::tape::Reader::delivered(&self.rows, delivery);
    }
}

/// D-366: a window at its declared page limit shows `hasMore = false`, `loadMore` ends with `NoMore` and asks nothing, and only `load_all` keeps the page limit error.
#[tokio::test(flavor = "multi_thread")]
async fn a_window_at_its_page_limit_shows_no_more_and_load_more_ends() {
    let (database, pages) = open_pages_limited(Some(2)).await;
    let rows = Arc::new(StateRows::default());
    let live = database.watch_query(FEED, rows.clone());
    bound(live.wait_for_initial_load()).await.unwrap();
    settled(&database, "first page shows more", || *rows.has_more.lock().unwrap() == Some(true)).await;
    let more = || database.load_more(database.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into());
    let second = bound(more()).await.unwrap();
    assert!(matches!(second, LoadMoreResult::Loaded { .. }), "{second:?}");
    settled(&database, "the limit shows no more", || *rows.has_more.lock().unwrap() == Some(false)).await;
    let requests = pages.queries.load(Ordering::SeqCst);
    let third = bound(more()).await.unwrap();
    assert!(matches!(third, LoadMoreResult::NoMore), "{third:?}");
    assert_eq!(pages.queries.load(Ordering::SeqCst), requests, "the call at the limit asked the server");
    let all = bound(database.load_all(database.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into(), false)).await;
    assert!(matches!(all, Err(DbError::Invalid { .. })), "{all:?}");
    live.cancel();
    database.close_now();
}

fn ids(rows: &Rows) -> Vec<String> {
    let mut ids: Vec<String> = rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect();
    ids.sort();
    ids
}

/// C13, I08, I10: page 1 loads, then a load-more and a refresh overlap and their answers arrive in both orders. Afterwards the rows hold no duplicate, a reset follows, and a new window of the same key reads one fresh page with no error.
#[tokio::test(flavor = "multi_thread")]
async fn overlapping_load_more_and_refresh_in_both_answer_orders_leave_a_consistent_chain_and_a_clean_reset() {
    for load_more_first in [true, false] {
        let case = format!("load_more_first={load_more_first}");
        let (database, pages) = open_pages().await;
        bound(database.load(database.session_epoch(), "Moment".into(), "feed".into(), "{}".into())).await.unwrap();
        let rows = Arc::new(Rows::default());
        let live = database.watch_query(FEED, rows.clone());
        bound(live.wait_for_initial_load()).await.unwrap();
        pages.clock.advance(1_000);
        let (first, second) = (Gate::new(), Gate::new());
        pages.gates.lock().unwrap().insert(1, first.clone());
        pages.gates.lock().unwrap().insert(2, second.clone());
        let (more, refresh) = (database.clone(), database.clone());
        let load_more = move || tokio::spawn(async move { more.load_more(more.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into()).await });
        let reload = move || tokio::spawn(async move { refresh.refresh(refresh.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into()).await });
        let (a, b) = if load_more_first {
            let a = load_more();
            first.entered().await;
            (a, reload())
        } else {
            let b = reload();
            first.entered().await;
            (load_more(), b)
        };
        for _ in 0..2 { first.release(); second.release(); }
        let (a, b) = (bound(a).await.unwrap(), bound(b).await.unwrap());
        assert!(a.is_ok() || b.is_ok(), "{case}: both overlapping calls failed: {a:?} {b:?}");

        let listed = ids(&rows);
        let mut unique = listed.clone();
        unique.dedup();
        assert_eq!(listed, unique, "{case}: a row is listed twice: {listed:?}");
        let result = bound(database.load_more(database.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into())).await.unwrap();
        assert!(matches!(result, LoadMoreResult::Loaded { .. } | LoadMoreResult::NoMore), "{case}: {result:?}");
        live.cancel();
        database.close_now();
    }
}

/// C16, I05, I12, I34: repeating online, power saving and an offline-online pair adds no request, with held handles and without them.
#[tokio::test(flavor = "multi_thread")]
async fn repeated_online_and_power_saving_signals_add_no_request() {
    for observed in [true, false] {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let held = Held::open(&rig.database);
        held.shows(&rig, "A").await;
        held.lists_show(&rig, "A").await;
        if !observed { held.cancel_all(); }
        rig.settle().await;
        let baseline = rig.world.hits();
        let sink = rig.database.online_sink().unwrap();

        for _ in 0..3 {
            rig.database.set_online(true).await.unwrap();
            sink.set_power_saving(false);
            rig.settle().await;
        }
        rig.database.set_online(false).await.unwrap();
        rig.settle().await;
        rig.database.set_online(true).await.unwrap();
        rig.settle().await;

        assert_eq!(rig.world.hits(), baseline, "observed={observed}: a repeated signal sent {:?}", rig.world.log().iter().skip(baseline).collect::<Vec<_>>());
        rig.database.close_now();
    }
}

fn presence() -> ModelSpec {
    ModelSpec {
        storage: Storage::Memory,
        expire_after: Some("30.seconds".into()),
        ..ModelSpec::new("presence", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("room", ScalarKind::Str).optional()])
    }
}

/// C18, I31: a memory row renews its expiry with an equal write and expires on the new boundary.
#[tokio::test(flavor = "multi_thread")]
async fn memory_expiry_follows_the_last_write() {
    let models = vec![presence()];
    let clock = TestClock::new(1_000_000);
    let database = OpenDatabase::new().open_with_clock(Config { path: None }, crate::ffi::specs(serde_json::to_string(&models).unwrap()), crate::ffi::OpenPorts::default(), clock.clone(), crate::ffi::OpenOptions::default(), None).await.unwrap();

    bound(database.create(database.session_epoch(), "presence".into(), r#"{"id":"p1","room":"a"}"#.into(), "{}".into())).await.unwrap();
    clock.advance(29_000);
    bound(database.update(database.session_epoch(), "presence".into(), "p1".into(), r#"{"room":"a"}"#.into(), "{}".into())).await.unwrap();
    clock.advance(29_999);
    assert!(database.peek_row(database.session_epoch(), "presence".into(), "p1".into()).await.unwrap().is_some(), "an equal write did not renew the expiry");
    clock.advance(1);
    settled(&database, "the row outlived its new boundary", || database.peek_row_now(database.session_epoch(), "presence".into(), "p1".into()).unwrap().is_none()).await;
}

/// P-DATA-124, D-367: destroy of a memory row that expired is a success without a delivery, twice; a disk model keeps NotFound.
#[tokio::test(flavor = "multi_thread")]
async fn data_destroy_of_a_gone_memory_row_is_a_success() {
    let note = ModelSpec::new("note", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    let models = vec![presence(), note];
    let clock = TestClock::new(1_000_000);
    let database = OpenDatabase::new().open_with_clock(Config { path: None }, crate::ffi::specs(serde_json::to_string(&models).unwrap()), crate::ffi::OpenPorts::default(), clock.clone(), crate::ffi::OpenOptions::default(), None).await.unwrap();
    bound(database.create(database.session_epoch(), "presence".into(), r#"{"id":"p1","room":"a"}"#.into(), "{}".into())).await.unwrap();
    assert!(database.peek_row(database.session_epoch(), "presence".into(), "p1".into()).await.unwrap().is_some());
    clock.advance(30_000);
    settled(&database, "the row outlived its expiry", || database.peek_row_now(database.session_epoch(), "presence".into(), "p1".into()).unwrap().is_none()).await;

    for attempt in 1..=2 {
        let answer = bound(database.destroy(database.session_epoch(), "presence".into(), "p1".into())).await;
        assert!(answer.is_ok(), "destroy {attempt} of the expired memory row failed: {answer:?}");
    }

    let missing = bound(database.destroy(database.session_epoch(), "note".into(), "n1".into())).await;
    assert!(matches!(missing, Err(DbError::NotFound { .. })), "a destroy of a missing disk row: {missing:?}");
    database.close_now();
}

/// R-11: the app returns to the foreground and refreshes the profile while its views are held. The refusal is typed, the credential and the views stay, and the next refresh succeeds. The server subscription of an encrypted session runs on the cable socket, which this rig does not speak, so it is not part of this sequence.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_refresh_keeps_the_views_and_the_credential() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let held = Held::open(&rig.database);
    held.shows(&rig, "A").await;
    held.lists_show(&rig, "A").await;
    rig.settle().await;
    rig.world.fail_next("MyProfile", super::rig_tests::Fault::Refuse { code: "UNAUTHENTICATED", confirmed: false });
    rig.clock.advance(1_000);

    let refused = bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), Some("profile".into()), "{}".into())).await.unwrap_err();

    assert!(matches!(refused, DbError::Unauthorized { .. }), "UNAUTHENTICATED on the refresh path is Unauthorized (D-01): {refused:?}");
    rig.settle().await;
    assert!(rig.database.signed_in_digest().is_some(), "the refusal cleared the credential");
    assert!(held.single.0.lock().unwrap().iter().all(|row| crate::ffi::tests::lifecycle_tests::named(row, "A")) && held.entity.0.lock().unwrap().iter().all(|row| crate::ffi::tests::lifecycle_tests::named(row, "A")) && !held.entities_empty(), "the refusal removed the value of a held entity");
    held.lists_show(&rig, "A").await;
    bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), Some("profile".into()), "{}".into())).await.unwrap();
    held.shows(&rig, "A").await;
    rig.database.close_now();
}
