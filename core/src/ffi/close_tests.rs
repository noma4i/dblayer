use std::sync::Arc;

use serde_json::json;

use super::open_slot::OpenSlot;
use super::tape::Rows;
use super::{Config, Database, DbError, ReadEnd, ReadRequest, ReadValue};
use crate::runtime::{Clock, ManualClock};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::test_wait::yield_until;
use crate::transport::MockTransport;
use crate::value::ScalarKind;

fn config() -> Config { Config { path: None, data_version: None } }

async fn local_notes() -> Arc<Database> {
    let mut model = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.storage = Storage::Local;
    Database::open(config(), crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), crate::ffi::OpenPorts::default()).await.unwrap()
}

pub(super) async fn with_transport(clock: Arc<dyn Clock>) -> (Arc<Database>, Arc<MockTransport>) {
    let transport = Arc::new(MockTransport::new());
    let mut notice = ModelSpec::new("Notice", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    notice.listens.push(serde_json::from_value(json!({"subscription":"Signal"})).unwrap());
    let wired = transport.clone();
    let database = Database::start_with(config(), crate::ffi::specs(serde_json::to_string(&vec![notice]).unwrap()), clock, move |db, _| {
        super::signed_session_tests::signed_session(db);
        Ok(super::database::Wiring { transport: Some(wired), encrypted: None, cable: None, seal: None })
    }).unwrap();
    database.register_operations(r#"[{"name":"Signal","document":"subscription Signal { signal { id } }","rootPath":"signal"}]"#.into()).await.unwrap();
    (database, transport)
}

#[tokio::test]
async fn closing_the_database_ends_every_open_stream_with_closed_and_a_late_stream_too() {
    let database = local_notes().await;
    let rows = Arc::new(Rows::default());
    let _live = database.watch_query(r#"{"model":"Note"}"#, rows.clone());
    yield_until("the first snapshot of a local list", || rows.last().is_some()).await;
    database.close_now();
    yield_until("the list ended", || !rows.ended().is_empty()).await;
    assert_eq!(rows.ended(), vec![ReadEnd::Closed]);
    let late = Arc::new(Rows::default());
    database.watch_query(r#"{"model":"Note"}"#, late.clone());
    yield_until("a stream opened after the close ended", || !late.ended().is_empty()).await;
    assert_eq!(late.ended(), vec![ReadEnd::Closed]);
    assert_eq!(database.stream_census().open, 0);
}

#[tokio::test]
async fn close_releases_old_engine() {
    let clock = Arc::new(ManualClock::new(std::time::Duration::from_secs(1)));
    let (database, transport) = with_transport(clock.clone()).await;
    let slot = OpenSlot::new();
    database.hold(slot.acquire().unwrap());
    let sink = database.online_sink().unwrap();
    let owner = Arc::downgrade(&database.life);
    database.close_now();
    sink.set_online(false);
    let reopened = slot.acquire();
    assert!(reopened.is_ok(), "the slot is free after the close");
    assert!(slot.acquire().is_err(), "and has exactly one owner after the reopen");
    drop(database);
    yield_until("the old database has no owner while the platform still holds the sink", || owner.upgrade().is_none()).await;
    let requests = transport.call_count("Signal");
    clock.advance(std::time::Duration::from_secs(3600));
    for _ in 0..16 { tokio::task::yield_now().await; }
    assert_eq!(transport.call_count("Signal"), requests, "advancing the clock starts no old work");
    drop(sink);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_close_succeeds_and_the_slot_frees_once() {
    let database = local_notes().await;
    let slot = OpenSlot::new();
    database.hold(slot.acquire().unwrap());
    database.close_database().await.unwrap();
    database.close_database().await.unwrap();
    assert!(slot.acquire().is_ok(), "the slot stayed taken after the close finished");
}

#[tokio::test]
async fn life_closed_stream_releases_store() {
    let database = local_notes().await;
    let stream = database.subscribe_values(database.session_epoch(), ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Note"}"#) });
    let (store, life) = (Arc::downgrade(&database.db), Arc::downgrade(&database.life));
    database.close_database().await.unwrap();
    drop(database);
    yield_until("a live stream keeps neither the store nor the life of a closed database", || store.upgrade().is_none() && life.upgrade().is_none()).await;
    assert!(matches!(stream.refresh().await, Err(DbError::Closed { .. })));
    assert!(matches!(stream.load_more().await, Err(DbError::Closed { .. })));
    let mut last = None;
    while let Some(value) = stream.pull().await { last = Some(value); }
    assert!(matches!(last, Some(ReadValue::Ended { cause: ReadEnd::Closed })));
}
