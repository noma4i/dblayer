use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::load_tests::relation;
use super::tape::{Seen, Tape};
use super::{Config, Database, DbError, ReadPhase, ReadRequest, ReadState, ReadStream, ReadValue, TestClock, Transport};
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
    db.set_online(true).unwrap();
    (db, clock)
}

/// The first state of the stream that is loaded at a revision of `above` or later; the call fails after 5 seconds.
async fn loaded_state(stream: &ReadStream, above: i64) -> ReadState {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (ReadValue::Snapshot { state, .. } | ReadValue::Delta { state, .. } | ReadValue::State { state, .. }) = stream.pull().await.unwrap().expect("a value") else { continue };
            if state.phase == ReadPhase::Loaded && state.loaded.is_some_and(|at| at >= above) { return state; }
        }
    })
    .await
    .expect("the loaded state of the window reaches the stream")
}

fn consistent(changes: &[Seen]) {
    let mut last = 0;
    for change in changes {
        assert!(change.seq > last, "seq {} after {last}", change.seq);
        last = change.seq;
        assert!(change.phase != Some(ReadPhase::Loaded) || change.loaded.is_some(), "Loaded without a load time at seq {}", change.seq);
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
    assert_eq!((changes[0].phase, changes[0].rows.len(), changes[0].size), (Some(ReadPhase::Loaded), 1, 1), "first value {:?}", changes[0].phase);
    assert!(changes[0].loaded.is_some());
    consistent(&changes);
    assert_eq!(listener.last().unwrap().seq, changes.last().unwrap().seq);
}

#[tokio::test]
async fn inv_loadm_16_a_cold_window_never_shows_loaded_or_empty_before_its_rows_and_ends_loaded_or_empty() {
    for (reply, phase, size) in [(ONE_ROW, ReadPhase::Loaded, 1), (NO_ROWS, ReadPhase::Loaded, 0)] {
        let (db, _) = open(&[reply]).await;
        let listener = tape();
        let live = db.watch_query(FEED, listener.clone());
        tokio::time::timeout(Duration::from_secs(5), live.wait_for_initial_load()).await.expect("the initial wait ends").unwrap();
        let changes = heard(&listener);
        consistent(&changes);
        let last = changes.last().unwrap();
        assert_eq!((last.phase, last.size, last.error.is_none()), (Some(phase), size, true), "{reply}");
        assert!(last.loaded.is_some(), "{reply}");
        if size > 0 {
            assert!(changes.iter().all(|change| !(change.phase == Some(ReadPhase::Loaded) && change.size == 0)), "a Loaded frame without rows on the way to one row");
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
    assert_eq!((last.phase, last.size, last.error.as_ref().map(|_| "server")), (Some(ReadPhase::Loaded), 1, Some("server")));
    assert!(last.error.is_some(), "the typed error survives: {:?}", last.error);

    let (cold, _) = open(&[REFUSED]).await;
    let listener = tape();
    let live = cold.watch_query(FEED, listener.clone());
    let waited = live.wait_for_initial_load().await;
    assert!(waited.is_err(), "the load error of the connect reaches the waiter: {waited:?}");
    let failed = heard(&listener).pop().unwrap();
    assert_eq!(waited.unwrap_err().to_string(), failed.error.as_ref().unwrap().to_string(), "the waiter and the state show the same error");
    assert_eq!((failed.phase, failed.size, failed.error.as_ref().map(|_| "server")), (Some(ReadPhase::Failed), 0, Some("server")));
}

#[tokio::test]
async fn dlv_state_error_typed() {
    let (cold, _) = open(&[REFUSED]).await;
    let listener = tape();
    let live = cold.watch_query(FEED, listener.clone());
    let waiter = live.wait_for_initial_load().await.expect_err("the load is refused");
    let failed = heard(&listener).pop().unwrap();
    assert_eq!(failed.error, Some(waiter), "the state carries the typed error of the load, not its text");
}

#[tokio::test]
async fn inv_loadm_16_a_refresh_that_changes_no_row_still_shows_its_state_once_the_group_has_read_through_it() {
    let (db, clock) = open(&[ONE_ROW, ONE_ROW]).await;
    db.load(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::List { query_json: FEED.into() });
    let first_loaded = loaded_state(&stream, 0).await.loaded.expect("the first value is loaded");
    clock.advance(60_000);
    db.refresh(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let shown = loaded_state(&stream, first_loaded).await;
    assert_eq!(shown.phase, ReadPhase::Loaded);
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
    db.set_online(true).unwrap();
    (db, clock)
}

#[tokio::test]
async fn inv_loadm_16_a_refresh_that_only_changes_a_related_row_still_shows_its_state() {
    const ANN: &str = r#"{"data":{"moments":[{"id":"m1","text":"a","author":{"id":"u1","name":"Ann"}}]}}"#;
    const BOB: &str = r#"{"data":{"moments":[{"id":"m1","text":"a","author":{"id":"u1","name":"Bob"}}]}}"#;
    let (db, clock) = open_with_authors(&[ANN, BOB]).await;
    db.load(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::List { query_json: FEED.into() });
    let first_loaded = loaded_state(&stream, 0).await.loaded.expect("the first value is loaded");
    clock.advance(60_000);
    db.refresh(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let shown = loaded_state(&stream, first_loaded).await;
    assert_eq!(shown.phase, ReadPhase::Loaded);
}
