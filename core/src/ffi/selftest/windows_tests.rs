use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;

use super::rig_tests::{bound, Gate, Held, Rig};
use crate::cloud::fixtures::{device, note, specs, FakeCloud};
use crate::ffi::lifecycle_tests::Rows;
use crate::ffi::{ByteProgress, CableLease, Config, Database, DbError, EncryptedSocketEvent, LoadMoreResult, OpenDatabase, PutReply, SubscriptionSink, TestClock, Transport, TransportRequest};
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape, RemoteSpec, Storage};
use crate::test_wait::yield_until;
use crate::value::ScalarKind;

struct Pages {
    queries: AtomicUsize,
    gates: Mutex<HashMap<usize, Arc<Gate>>>,
}

#[async_trait]
impl Transport for Pages {
    async fn open_cable(&self, _: Arc<CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> {
        let number = self.queries.fetch_add(1, Ordering::SeqCst);
        let gate = self.gates.lock().unwrap().remove(&number);
        if let Some(gate) = gate { gate.pass().await; }
        Ok(format!(r#"{{"data":{{"feed":{{"nodes":[{{"id":"m{number}","text":"t"}}],"pageInfo":{{"hasNextPage":true,"endCursor":"c{number}"}}}}}}}}"#))
    }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: String, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { unreachable!() }
}

const FEED: &str = r#"{"model":"Moment","relation":"feed","params":{}}"#;

async fn open_pages() -> (Arc<Database>, Arc<Pages>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None },
        stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: true, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    model.relations.push(feed);
    let pages = Arc::new(Pages { queries: AtomicUsize::new(0), gates: Mutex::new(HashMap::new()) });
    let database = OpenDatabase::new().open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![model]).unwrap(), Some(pages.clone()), None, TestClock::new(1_000_000), false, false, None, None, None, None).await.unwrap();
    database.register_operations(r#"[{"name":"Feed","document":"query Feed { feed { nodes { id text } pageInfo { hasNextPage endCursor } } }","rootPath":"feed"}]"#.into()).unwrap();
    (database, pages)
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
        bound(database.load("Moment".into(), "feed".into(), "{}".into())).await.unwrap();
        let rows = Arc::new(Rows::default());
        let live = database.live_query(FEED.into(), rows.clone());
        bound(live.wait_for_initial_load()).await.unwrap();
        let (first, second) = (Gate::new(), Gate::new());
        pages.gates.lock().unwrap().insert(1, first.clone());
        pages.gates.lock().unwrap().insert(2, second.clone());
        let (more, refresh) = (database.clone(), database.clone());
        let load_more = move || tokio::spawn(async move { more.load_more("Moment".into(), "feed".into(), "{}".into()).await });
        let reload = move || tokio::spawn(async move { refresh.refresh("Moment".into(), "feed".into(), "{}".into()).await });
        let (a, b) = if load_more_first {
            let a = load_more();
            first.entered_for(&case).await;
            (a, reload())
        } else {
            let b = reload();
            first.entered_for(&case).await;
            (load_more(), b)
        };
        yield_until("the second call reached the transport or joined", || pages.queries.load(Ordering::SeqCst) >= 3 || a.is_finished() || b.is_finished()).await;
        for _ in 0..2 { first.release(); second.release(); }
        let (a, b) = (bound(a).await.unwrap(), bound(b).await.unwrap());
        assert!(a.is_ok() || b.is_ok(), "{case}: both overlapping calls failed: {a:?} {b:?}");

        let listed = ids(&rows);
        let mut unique = listed.clone();
        unique.dedup();
        assert_eq!(listed, unique, "{case}: a row is listed twice: {listed:?}");
        bound(database.reset_for_testing()).await.unwrap();
        bound(database.settle_for_testing()).await.unwrap();
        let before = pages.queries.load(Ordering::SeqCst);
        let fresh_rows = Arc::new(Rows::default());
        let fresh = database.live_query(FEED.into(), fresh_rows.clone());
        bound(fresh.wait_for_initial_load()).await.unwrap_or_else(|error| panic!("{case}: the window after the reset inherited {error:?}"));
        assert!(fresh_rows.1.lock().unwrap().iter().all(|change| change.error.is_none()), "{case}: the window after the reset delivered an error");
        assert!(pages.queries.load(Ordering::SeqCst) - before <= 1, "{case}: the window after the reset read more than once");
        let result = bound(database.load_more("Moment".into(), "feed".into(), "{}".into())).await.unwrap();
        assert!(matches!(result, LoadMoreResult::Loaded { .. } | LoadMoreResult::NoMore | LoadMoreResult::AlreadyLoading), "{case}: {result:?}");
        live.cancel();
        fresh.cancel();
        database.shutdown();
    }
}

/// C16, I05, I12, I34: repeating online, listening, power saving and an offline-online pair adds no request, with held handles and without them.
#[tokio::test(flavor = "multi_thread")]
async fn repeated_online_listening_and_power_saving_signals_add_no_request() {
    for observed in [true, false] {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let held = Held::open(&rig.database);
        held.shows("A").await;
        held.lists_show("A").await;
        if !observed { held.cancel_all(); }
        rig.settle().await;
        let baseline = rig.world.hits();
        let sink = rig.database.online_sink().unwrap();

        for _ in 0..3 {
            rig.database.set_online(true).unwrap();
            rig.database.set_listening(true).unwrap();
            sink.set_power_saving(false);
            rig.settle().await;
        }
        rig.database.set_online(false).unwrap();
        rig.settle().await;
        rig.database.set_online(true).unwrap();
        rig.settle().await;

        assert_eq!(rig.world.hits(), baseline, "observed={observed}: a repeated signal sent {:?}", rig.world.log().iter().skip(baseline).collect::<Vec<_>>());
        rig.database.shutdown();
    }
}

fn presence() -> ModelSpec {
    ModelSpec {
        storage: Storage::Memory,
        expire_after: Some("30.seconds".into()),
        ..ModelSpec::new("presence", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("room", ScalarKind::Str).optional()])
    }
}

/// C18, I31, I33: a memory row renews its expiry with an equal write and expires on the new boundary. A cloud pull answered after a reset restores no row, and nothing runs after the close.
#[tokio::test(flavor = "multi_thread")]
async fn memory_expiry_follows_the_last_write_and_a_cloud_reply_after_a_reset_restores_nothing() {
    let fake = FakeCloud::new();
    let remote = device(&fake, 1_000);
    let id = note(&remote, json!({"title": "old account"}));
    remote.cloud.sync_now().await.unwrap();
    let mut models = specs();
    models.push(presence());
    let clock = TestClock::new(1_000_000);
    let database = OpenDatabase::new().open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&models).unwrap(), None, None, clock.clone(), false, false, None, None, None, None).await.unwrap();

    bound(database.create("presence".into(), r#"{"id":"p1","room":"a"}"#.into(), "{}".into())).await.unwrap();
    clock.advance(29_000);
    bound(database.update("presence".into(), "p1".into(), r#"{"room":"a"}"#.into(), "{}".into())).await.unwrap();
    clock.advance(29_000);
    bound(database.maintain()).await.unwrap();
    assert!(database.peek_row("presence".into(), "p1".into()).unwrap().is_some(), "an equal write did not renew the expiry");
    clock.advance(2_000);
    bound(database.maintain()).await.unwrap();
    assert!(database.peek_row("presence".into(), "p1".into()).unwrap().is_none(), "the row outlived its new boundary");

    let db = database.db.clone();
    fake.before_pull_reply(move || db.reset_all().unwrap());
    let link = database.connect_cloud(fake.clone()).unwrap();
    yield_until("the cloud pass ran", || fake.calls() > 0).await;
    bound(database.settle_for_testing()).await.unwrap();
    assert!(database.peek_row("notes".into(), id.clone()).unwrap().is_none(), "the cloud reply after the reset restored a row");

    link.stop();
    database.shutdown();
    let calls = fake.calls();
    clock.advance(3_600_000);
    link.remote_changed();
    assert_eq!(fake.calls(), calls, "the cloud ran after the close");
}
