use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;


use super::rig_tests::{bound, Gate, Held, Rig};
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
    async fn open_cable(&self, _: Arc<CableLease>, _url: String) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
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
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { unreachable!() }
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
    let database = OpenDatabase::new().open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![model]).unwrap(), Some(pages.clone()), None, TestClock::new(1_000_000), None, None, None, None).await.unwrap();
    database.register_operations(r#"[{"name":"Feed","document":"query Feed { feed { nodes { id text } pageInfo { hasNextPage endCursor } } }","rootPath":"feed"}]"#.into()).unwrap();
    database.set_online(true).unwrap();
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
        bound(database.load(database.session_epoch(), "Moment".into(), "feed".into(), "{}".into())).await.unwrap();
        let rows = Arc::new(Rows::default());
        let live = database.watch_query(FEED, rows.clone());
        bound(live.wait_for_initial_load()).await.unwrap();
        let (first, second) = (Gate::new(), Gate::new());
        pages.gates.lock().unwrap().insert(1, first.clone());
        pages.gates.lock().unwrap().insert(2, second.clone());
        let (more, refresh) = (database.clone(), database.clone());
        let load_more = move || tokio::spawn(async move { more.load_more(more.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await });
        let reload = move || tokio::spawn(async move { refresh.refresh(refresh.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await });
        let (a, b) = if load_more_first {
            let a = load_more();
            first.entered_for(&case).await;
            (a, reload())
        } else {
            let b = reload();
            first.entered_for(&case).await;
            (load_more(), b)
        };
        for _ in 0..2 { first.release(); second.release(); }
        let (a, b) = (bound(a).await.unwrap(), bound(b).await.unwrap());
        assert!(a.is_ok() || b.is_ok(), "{case}: both overlapping calls failed: {a:?} {b:?}");

        let listed = ids(&rows);
        let mut unique = listed.clone();
        unique.dedup();
        assert_eq!(listed, unique, "{case}: a row is listed twice: {listed:?}");
        database.reset_models_of(&["Moment"]).await;
        bound(crate::ffi::settled(&database)).await.unwrap();
        let before = pages.queries.load(Ordering::SeqCst);
        let fresh_rows = Arc::new(Rows::default());
        let fresh = database.watch_query(FEED, fresh_rows.clone());
        bound(fresh.wait_for_initial_load()).await.unwrap_or_else(|error| panic!("{case}: the window after the reset inherited {error:?}"));
        assert!(fresh_rows.1.lock().unwrap().iter().all(|change| change.error.is_none()), "{case}: the window after the reset delivered an error");
        assert!(pages.queries.load(Ordering::SeqCst) - before <= 1, "{case}: the window after the reset read more than once");
        let result = bound(database.load_more(database.session_epoch(), "Moment".into(), "feed".into(), "{}".into())).await.unwrap();
        assert!(matches!(result, LoadMoreResult::Loaded { .. } | LoadMoreResult::NoMore | LoadMoreResult::AlreadyLoading), "{case}: {result:?}");
        live.cancel();
        fresh.cancel();
        database.close_now();
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
            sink.set_power_saving(false);
            rig.settle().await;
        }
        rig.database.set_online(false).unwrap();
        rig.settle().await;
        rig.database.set_online(true).unwrap();
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
    let database = OpenDatabase::new().open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&models).unwrap(), None, None, clock.clone(), None, None, None, None).await.unwrap();

    bound(database.create(database.session_epoch(), "presence".into(), r#"{"id":"p1","room":"a"}"#.into(), "{}".into())).await.unwrap();
    clock.advance(29_000);
    bound(database.update(database.session_epoch(), "presence".into(), "p1".into(), r#"{"room":"a"}"#.into(), "{}".into())).await.unwrap();
    clock.advance(29_000);
    assert!(database.peek_row(database.session_epoch(), "presence".into(), "p1".into()).unwrap().is_some(), "an equal write did not renew the expiry");
    clock.advance(2_000);
    let expired = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while database.peek_row(database.session_epoch(), "presence".into(), "p1".into()).unwrap().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(expired.is_ok(), "the row outlived its new boundary");

}

/// R-11: the app returns to the foreground and refreshes the profile while its views are held. The refusal is typed, the credential and the views stay, and the next refresh succeeds. The server subscription of an encrypted session runs on the cable socket, which this rig does not speak, so it is not part of this sequence.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_refresh_keeps_the_views_and_the_credential() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    held.lists_show("A").await;
    rig.settle().await;
    rig.world.fail_next("MyProfile", super::rig_tests::Fault::Refuse { code: "UNAUTHENTICATED", confirmed: false });

    let refused = bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.unwrap_err();

    assert!(matches!(refused, DbError::Session { code: crate::ffi::SessionCode::Unauthenticated, .. }), "{refused:?}");
    rig.settle().await;
    assert!(rig.database.session.as_ref().unwrap().signed_in().unwrap().is_some(), "the refusal cleared the credential");
    assert!(held.single.0.lock().unwrap().iter().all(|row| crate::ffi::lifecycle_tests::named(row, "A")) && held.entity.0.lock().unwrap().iter().all(|row| crate::ffi::lifecycle_tests::named(row, "A")) && !held.entities_empty(), "the refusal removed the value of a held entity");
    held.lists_show("A").await;
    bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.unwrap();
    held.shows("A").await;
    rig.database.close_now();
}
