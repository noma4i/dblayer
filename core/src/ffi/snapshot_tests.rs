use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::load_tests::relation;
use super::stream::tape::{folded, Seen, Tape};
use super::{Config, Database, DbError, LivePhase, StreamRequest, TestClock, Transport};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;

const ONE_ROW: &str = r#"{"data":{"moments":[{"id":"m1","text":"a"}]}}"#;
const NO_ROWS: &str = r#"{"data":{"moments":[]}}"#;
const REFUSED: &str = r#"{"errors":[{"message":"nope"}]}"#;
const FEED: &str = r#"{"model":"Moment","relation":"feed","params":{}}"#;

struct Replying(Mutex<VecDeque<String>>);

#[async_trait::async_trait]
impl Transport for Replying {
    async fn open_cable(&self, _: Arc<super::CableLease>, _url: String) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> { Ok(self.0.lock().unwrap().pop_front().expect("a queued reply")) }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, _: Arc<super::SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

fn tape() -> Arc<Tape> {
    Arc::default()
}

fn heard(tape: &Tape) -> Vec<Seen> {
    tape.heard()
}

async fn open(replies: &[&str]) -> (Arc<Database>, Arc<TestClock>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    model.relations.push(relation("feed", "5.minutes"));
    let transport = Arc::new(Replying(Mutex::new(replies.iter().map(|reply| reply.to_string()).collect())));
    let clock = TestClock::new(1_000_000);
    let db = Database::open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![model]).unwrap(), Some(transport), None, clock.clone(), None, None, None, None).await.unwrap();
    db.register_operations(r#"[{"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"}]"#.into()).unwrap();
    (db, clock)
}

fn consistent(changes: &[Seen]) {
    let mut last = 0;
    for change in changes {
        assert!(change.seq > last, "seq {} after {last}", change.seq);
        last = change.seq;
        assert!(!(change.phase == Some(LivePhase::Loaded) && change.size == 0), "Loaded with no rows at seq {}", change.seq);
        assert!(!(change.phase == Some(LivePhase::Empty) && change.size > 0), "Empty with rows at seq {}", change.seq);
    }
}

#[tokio::test]
async fn inv_loadm_16_the_first_value_of_a_loaded_window_holds_its_rows_and_the_loaded_phase() {
    let (db, _) = open(&[ONE_ROW]).await;
    db.load(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let listener = tape();
    let live = db.watch_query(FEED, listener.clone());
    live.wait_for_initial_load().await.unwrap();
    let changes = heard(&listener);
    assert_eq!((changes[0].phase, changes[0].rows.len(), changes[0].size), (Some(LivePhase::Loaded), 1, 1), "first value {:?}", changes[0].phase);
    assert!(changes[0].loaded_at.is_some());
    consistent(&changes);
    assert_eq!(listener.last().unwrap().seq, changes.last().unwrap().seq);
}

#[tokio::test]
async fn inv_loadm_16_a_cold_window_never_shows_loaded_or_empty_before_its_rows_and_ends_loaded_or_empty() {
    for (reply, phase, size) in [(ONE_ROW, LivePhase::Loaded, 1), (NO_ROWS, LivePhase::Empty, 0)] {
        let (db, _) = open(&[reply]).await;
        let listener = tape();
        let live = db.watch_query(FEED, listener.clone());
        tokio::time::timeout(Duration::from_secs(5), live.wait_for_initial_load()).await.expect("the initial wait ends").unwrap();
        let changes = heard(&listener);
        consistent(&changes);
        let last = changes.last().unwrap();
        assert_eq!((last.phase, last.size, last.error.is_none()), (Some(phase), size, true), "{reply}");
        assert!(last.loaded_at.is_some(), "{reply}");
        if size > 0 {
            assert!(changes.iter().all(|change| change.phase != Some(LivePhase::Empty)), "an Empty frame on the way to one row");
        }
    }
}

#[tokio::test]
async fn inv_loadm_16_a_background_failure_is_loaded_with_the_error_when_rows_exist_and_failed_without() {
    let (db, clock) = open(&[ONE_ROW, REFUSED]).await;
    db.load(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    clock.advance(6 * 60_000);
    let listener = tape();
    let live = db.watch_query(FEED, listener.clone());
    live.wait_for_initial_load().await.unwrap();
    let last = heard(&listener).pop().unwrap();
    assert_eq!((last.phase, last.size, last.error_kind.as_deref()), (Some(LivePhase::Loaded), 1, Some("server")));
    assert!(matches!(last.error, Some(DbError::Graphql { .. })), "the typed error survives: {:?}", last.error);

    let (cold, _) = open(&[REFUSED]).await;
    let listener = tape();
    let live = cold.watch_query(FEED, listener.clone());
    assert!(matches!(live.wait_for_initial_load().await, Err(DbError::Graphql { .. })), "the load error of the connect reaches the waiter");
    let failed = heard(&listener).pop().unwrap();
    assert_eq!((failed.phase, failed.size, failed.error_kind.as_deref()), (Some(LivePhase::Failed), 0, Some("server")));
}

#[tokio::test]
async fn inv_loadm_16_every_subscription_call_tells_a_failed_open_in_its_first_snapshot() {
    let (db, _) = open(&[]).await;
    let bad = r#"{"model":"Moment","where":{"type":"equal","field":"nope","value":1}}"#;
    let requests = [
        StreamRequest::Query { query_json: bad.into() },
        StreamRequest::Query { query_json: "not json".into() },
        StreamRequest::Count { query_json: bad.into() },
        StreamRequest::Entity { model: "Moment".into(), id: String::new(), fetch: super::EntityFetch::Never },
        StreamRequest::Related { query_json: r#"{"model":"Moment"}"#.into(), name: "nope".into() },
        StreamRequest::Query { query_json: r#"{"model":"Nope"}"#.into() },
    ];
    for (index, request) in requests.into_iter().enumerate() {
        let listener = tape();
        let subscription = db.watch(request, listener.clone());
        crate::test_wait::yield_until("the failed stream delivers its first value", || !listener.heard().is_empty()).await;
        let first = listener.heard().into_iter().next().unwrap();
        assert_eq!((first.phase, first.error_kind.as_deref(), first.size, first.seq), (Some(LivePhase::Failed), Some("invalid"), 0, 1), "call {index}");
        assert!(first.error.is_some(), "call {index}");
        assert!(subscription.wait_for_initial_load().await.is_err(), "call {index}: a stream that ended has no initial load to wait for");
    }
    crate::test_wait::yield_until("the reader read every end", || db.stream_census().open == 0).await;
}

#[tokio::test]
async fn inv_loadm_16_a_refresh_that_changes_no_row_still_shows_its_state_once_the_group_has_read_through_it() {
    let (db, clock) = open(&[ONE_ROW, ONE_ROW]).await;
    db.load(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let listener = tape();
    let live = db.watch_query(FEED, listener.clone());
    live.wait_for_initial_load().await.unwrap();
    let first_loaded_at = heard(&listener).last().and_then(|change| change.loaded_at).expect("the first frame is loaded");
    clock.advance(60_000);
    db.refresh(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let shown = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = listener.last().expect("the first value arrived");
            if current.loaded_at.is_some_and(|at| at > first_loaded_at) && !current.refreshing {
                return current;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the state of the refresh reaches the listener although no row changed");
    assert_eq!((shown.phase, shown.size), (Some(LivePhase::Loaded), 1));
    consistent(&heard(&listener));
}

async fn open_with_authors(replies: &[&str]) -> (Arc<Database>, Arc<TestClock>) {
    let mut user = ModelSpec::new("User", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).optional()]);
    user.storage = crate::schema::Storage::Disk;
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional(), FieldSpec::scalar("userId", ScalarKind::Id).nullable().indexed()]);
    model.associations.push(crate::schema::AssociationSpec::new("author", crate::schema::AssociationKind::BelongsTo, "User", "userId"));
    model.relations.push(relation("feed", "5.minutes"));
    let transport = Arc::new(Replying(Mutex::new(replies.iter().map(|reply| reply.to_string()).collect())));
    let clock = TestClock::new(1_000_000);
    let db = Database::open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![user, model]).unwrap(), Some(transport), None, clock.clone(), None, None, None, None).await.unwrap();
    db.register_operations(r#"[{"name":"Feed","document":"query Feed { moments { id text author { id name } } }","rootPath":"moments"}]"#.into()).unwrap();
    (db, clock)
}

#[tokio::test]
async fn inv_loadm_16_a_refresh_that_only_changes_a_related_row_still_shows_its_state() {
    const ANN: &str = r#"{"data":{"moments":[{"id":"m1","text":"a","author":{"id":"u1","name":"Ann"}}]}}"#;
    const BOB: &str = r#"{"data":{"moments":[{"id":"m1","text":"a","author":{"id":"u1","name":"Bob"}}]}}"#;
    let (db, clock) = open_with_authors(&[ANN, BOB]).await;
    db.load(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let listener = tape();
    let live = db.watch_query(FEED, listener.clone());
    live.wait_for_initial_load().await.unwrap();
    let first_loaded_at = heard(&listener).last().and_then(|change| change.loaded_at).expect("the first frame is loaded");
    clock.advance(60_000);
    db.refresh(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let shown = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = listener.last().expect("the first value arrived");
            if current.loaded_at.is_some_and(|at| at > first_loaded_at) && !current.refreshing {
                return current;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the state of the refresh reaches the listener although no row of the query changed");
    assert_eq!((shown.phase, shown.size), (Some(LivePhase::Loaded), 1));
}

#[tokio::test]
async fn related_rows_and_the_source_load_arrive_in_one_snapshot_with_the_typed_error_and_release_on_cancel() {
    const AUTHORED: &str = r#"{"data":{"moments":[{"id":"m1","text":"a","author":{"id":"u1","name":"Ann"}}]}}"#;
    const RELATED_FEED: &str = r#"{"model":"Moment","relation":"feed","params":{}}"#;
    let (db, _) = open_with_authors(&[AUTHORED]).await;
    let listener = tape();
    let live = db.watch(StreamRequest::Related { query_json: RELATED_FEED.into(), name: "author".into() }, listener.clone());
    tokio::time::timeout(Duration::from_secs(5), live.wait_for_initial_load()).await.expect("the initial wait ends").unwrap();
    let frames = heard(&listener);
    let shown = frames.last().unwrap();
    assert_eq!((shown.phase, shown.size, shown.total), (Some(LivePhase::Loaded), 1, None));
    let snapshots = folded(&frames);
    assert!(snapshots.iter().all(|(phase, ids, _)| *phase != Some(LivePhase::Loaded) || ids == &["u1"]), "no Loaded snapshot without the related row: {snapshots:?}");
    assert_eq!(snapshots.last().map(|(_, ids, _)| ids.clone()), Some(vec!["u1".to_string()]));
    consistent(&frames);
    live.cancel();
    assert_eq!((db.subscription_census().subscriptions, db.subscription_census().observed_queries), (0, 0));

    let (refused, _) = open_with_authors(&[REFUSED]).await;
    let listener = tape();
    let live = refused.watch(StreamRequest::Related { query_json: RELATED_FEED.into(), name: "author".into() }, listener.clone());
    let waited = tokio::time::timeout(Duration::from_secs(5), live.wait_for_initial_load()).await.expect("the initial wait ends");
    assert!(matches!(waited, Err(DbError::Graphql { .. })), "the load error reaches the waiter");
    let failed = heard(&listener).pop().unwrap();
    assert_eq!((failed.phase, failed.size), (Some(LivePhase::Failed), 0));
    assert!(matches!(failed.error, Some(DbError::Graphql { .. })), "the typed error survives: {:?}", failed.error);
    live.cancel();
}
