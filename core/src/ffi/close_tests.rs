use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::json;

use crate::lifecycle::{Member, Lifecycle};
use super::live_feed::LiveFeed;
use super::open_slot::OpenSlot;
use super::{Config, Database, DbError, LiveChange, LiveSubscription, LivePhase, QueryListener};
use crate::cloud::fixtures::FakeCloud;
use crate::pacing::{Clock, ManualClock};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::test_wait::{join_within, within, yield_until};
use crate::transport::{MockTransport, SubscriptionEvent};
use crate::value::ScalarKind;

struct Silent;
impl QueryListener for Silent {
    fn changed(&self, _: LiveChange) {}
}

struct Counting(AtomicUsize, Option<Arc<Lifecycle>>, Mutex<Vec<usize>>, std::sync::OnceLock<crate::lifecycle::Registration>);
impl Member for Counting {
    fn close(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        if let Some(streams) = &self.1 { crate::db::locked(&self.2).push(streams.enrolled()); }
    }
    fn registration(&self) -> &std::sync::OnceLock<crate::lifecycle::Registration> { &self.3 }
}

fn config() -> Config { Config { path: None, data_version: None, cache_rows: 8 } }

async fn local_notes() -> Arc<Database> {
    let mut model = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.storage = Storage::Local;
    Database::open(config(), serde_json::to_string(&vec![model]).unwrap(), None, None, None, None, None, None).await.unwrap()
}

fn with_transport(clock: Arc<dyn Clock>) -> (Arc<Database>, Arc<MockTransport>) {
    let transport = Arc::new(MockTransport::new());
    let mut notice = ModelSpec::new("Notice", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    notice.listens.push(serde_json::from_value(json!({"subscription":"Signal"})).unwrap());
    let wired = transport.clone();
    let database = Database::start_with(config(), serde_json::to_string(&vec![notice]).unwrap(), None, clock, move |_, _| Ok((Some(wired), None))).unwrap();
    database.register_operations(r#"[{"name":"Signal","document":"subscription Signal { signal { id } }","rootPath":"signal"}]"#.into()).unwrap();
    (database, transport)
}

#[test]
fn the_open_streams_close_each_member_once_and_refuses_late_members() {
    let streams = Arc::new(Lifecycle::default());
    let first = Arc::new(Counting(AtomicUsize::new(0), Some(streams.clone()), Mutex::default(), Default::default()));
    let dropped = Arc::new(Counting(AtomicUsize::new(0), None, Mutex::default(), Default::default()));
    streams.enroll(&first);
    streams.enroll(&dropped);
    drop(dropped);
    assert_eq!(streams.enrolled(), 1);
    let closing = streams.clone();
    join_within(std::thread::spawn(move || { closing.close(); closing.close(); }));
    assert_eq!(first.0.load(Ordering::SeqCst), 1);
    assert_eq!(*crate::db::locked(&first.2), [0], "the member list was read from inside close, so the lock was free");
    let late = Arc::new(Counting(AtomicUsize::new(0), None, Mutex::default(), Default::default()));
    streams.enroll(&late);
    assert_eq!(late.0.load(Ordering::SeqCst), 1);
    assert_eq!(streams.enrolled(), 0);
}

#[test]
fn enrolling_a_stream_does_the_same_work_with_existing_streams() {
    let streams = Arc::new(Lifecycle::default());
    let members: Vec<_> = (0..128).map(|_| Arc::new(Counting(AtomicUsize::new(0), None, Mutex::default(), Default::default()))).collect();
    for member in &members { streams.enroll(member); }
    let before = streams.work();
    let next = Arc::new(Counting(AtomicUsize::new(0), None, Mutex::default(), Default::default()));
    streams.enroll(&next);
    assert_eq!(streams.work() - before, 0);
    assert_eq!(streams.enrolled(), members.len() + 1);
    drop(members);
    drop(next);
    assert_eq!(streams.enrolled(), 0);
}

#[tokio::test]
async fn stream_close_terminates_waiters() {
    let database = local_notes().await;
    let query = r#"{"model":"Note"}"#;
    database.create("Note".into(), r#"{"text":"kept"}"#.into(), "{}".into()).await.unwrap();
    let queue = database.changes(query.into()).unwrap();
    within(queue.next()).await.expect("initial delivery");
    let waiters: Vec<_> = (0..2).map(|_| { let queue = queue.clone(); tokio::spawn(within(async move { queue.next().await })) }).collect();
    let unopened = Arc::new(LiveSubscription { feed: LiveFeed::new(Arc::new(Silent)), handles: Mutex::new(None), registration: Default::default() });
    database.streams.enroll(&unopened);
    let current = { let live = unopened.clone(); tokio::spawn(within(async move { live.current().await })) };
    let initial = { let live = unopened.clone(); tokio::spawn(within(async move { live.wait_for_initial_load().await })) };
    tokio::task::yield_now().await;
    assert!(waiters.iter().all(|waiter| !waiter.is_finished()) && !current.is_finished() && !initial.is_finished(), "every waiter is parked before the close");
    database.shutdown();
    for waiter in waiters { assert!(within(waiter).await.unwrap().is_none()); }
    let closed = within(current).await.unwrap();
    assert_eq!(closed.phase, Some(LivePhase::Closed));
    assert!(matches!(closed.error, Some(DbError::Interrupted { .. })));
    assert!(matches!(within(initial).await.unwrap(), Err(DbError::Interrupted { .. })));
    let late = database.changes(query.into()).unwrap();
    assert!(within(late.next()).await.is_none());
}

#[tokio::test]
async fn closing_the_database_ends_a_waiting_model_event_next() {
    let (database, transport) = with_transport(Arc::new(crate::pacing::SystemClock));
    transport.script_subscription("Signal", vec![SubscriptionEvent::Ready]);
    let feed = within(database.open_model_events("Notice".into())).await.unwrap();
    let waiting = tokio::spawn(within({ let feed = feed.clone(); async move { feed.next().await } }));
    tokio::task::yield_now().await;
    database.shutdown();
    assert!(matches!(within(waiting).await.unwrap(), Ok(None)));
}

#[tokio::test]
async fn closing_the_database_ends_a_waiting_blob_stream() {
    use std::io::Write;
    let transport = Arc::new(MockTransport::new());
    let wired = transport.clone();
    let database = Database::start_with(config(), "[]".into(), Some(crate::upload::UploadOperations::empty().create_blob("CreateBlob")), Arc::new(crate::pacing::SystemClock), move |_, _| Ok((Some(wired), None))).unwrap();
    database.register_operations(r#"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { id } }","rootPath":"createBlob"}]"#.into()).unwrap();
    database.set_online(false).unwrap();
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(b"blob").unwrap();
    let id = database.sync.as_ref().unwrap().upload_blob("test", "CreateBlob", crate::upload::UploadFile::new(file.path().to_str().unwrap())).await.unwrap();
    let changes = database.clone().blob_changes(id).await.unwrap();
    assert!(within(changes.clone().next()).await.unwrap().is_some());
    let waiting = tokio::spawn(within({ let changes = changes.clone(); async move { changes.next().await } }));
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    database.shutdown();
    assert!(within(waiting).await.unwrap().unwrap().is_none());
}

#[tokio::test]
async fn close_releases_old_engine() {
    let clock = Arc::new(ManualClock::new(std::time::Duration::from_secs(1)));
    let (database, transport) = with_transport(clock.clone());
    let slot = OpenSlot::new();
    database.hold(slot.acquire().unwrap());
    let cloud = FakeCloud::new();
    let link = database.connect_cloud(cloud.clone()).unwrap();
    yield_until("the cloud pass ran", || cloud.calls() > 0).await;
    assert!(link.cloud.is_running());
    let sink = database.online_sink().unwrap();
    let owner = Arc::downgrade(&database.sync.as_ref().unwrap().inner);
    let connectivity = database.db.connectivity();
    database.shutdown();
    assert!(!link.cloud.is_running(), "the cloud task stops with the database");
    sink.set_online(false);
    assert!(connectivity.is_online(), "a late call through the retained sink is refused");
    let reopened = slot.acquire();
    assert!(reopened.is_ok(), "the slot is free after the close");
    assert!(slot.acquire().is_err(), "and has exactly one owner after the reopen");
    drop(database);
    yield_until("the old database has no owner while the platform still holds the sink", || owner.upgrade().is_none()).await;
    let (calls, requests) = (cloud.calls(), transport.call_count("Signal"));
    clock.advance(std::time::Duration::from_secs(3600));
    link.remote_changed();
    for _ in 0..16 { tokio::task::yield_now().await; }
    assert_eq!((cloud.calls(), transport.call_count("Signal")), (calls, requests), "advancing the clock starts no old work");
    drop(sink);
}

#[tokio::test]
async fn dropping_the_last_cloud_handle_stops_its_task() {
    let (database, _transport) = with_transport(Arc::new(crate::pacing::SystemClock));
    let cloud = FakeCloud::new();
    let link = database.connect_cloud(cloud.clone()).unwrap();
    yield_until("the cloud pass ran", || cloud.calls() > 0).await;
    assert!(Arc::strong_count(&cloud) > 1);
    drop(link);
    yield_until("the cloud task released its io", || Arc::strong_count(&cloud) == 1).await;
}
