use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::json;

use crate::lifecycle::{Holder, Lifecycle};
use super::open_slot::OpenSlot;
use super::stream::tape::Rows;
use super::{Config, Database, DbError, StreamEnd, StreamRequest};
use crate::pacing::{Clock, ManualClock};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::test_wait::{join_within, within, yield_until};
use crate::transport::{MockTransport, SubscriptionEvent};
use crate::value::ScalarKind;

struct Counting(AtomicUsize, Option<Arc<Lifecycle>>, Mutex<Vec<usize>>, std::sync::OnceLock<crate::lifecycle::Registration>);
impl Holder for Counting {
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
    let (database, transport) = with_transport(Arc::new(crate::pacing::SystemClock));
    transport.script_subscription("Signal", vec![]);
    let rows = Arc::new(Rows::default());
    let live = database.watch_query(r#"{"model":"Notice"}"#, rows.clone());
    within(live.wait_for_initial_load()).await.unwrap();
    let pending = database.watch(StreamRequest::Events { model: "Notice".into() }, Arc::new(Rows::default()));
    let waiting = { let pending = pending.clone(); tokio::spawn(within(async move { pending.wait_for_initial_load().await })) };
    tokio::task::yield_now().await;
    database.close_now();
    let waited = within(waiting).await.unwrap();
    assert!(matches!(waited, Err(DbError::Interrupted { .. })), "a wait for a stream outlived the close: {waited:?}");
    yield_until("the list ended", || !rows.ended().is_empty()).await;
    assert_eq!(rows.ended(), vec![StreamEnd::Closed]);
    let late = Arc::new(Rows::default());
    database.watch_query(r#"{"model":"Notice"}"#, late.clone());
    yield_until("a stream opened after the close ended", || !late.ended().is_empty()).await;
    assert_eq!(late.ended(), vec![StreamEnd::Closed]);
    assert_eq!(database.stream_census().open, 0);
}

#[tokio::test]
async fn closing_the_database_ends_a_model_event_stream_with_one_outcome() {
    let (database, transport) = with_transport(Arc::new(crate::pacing::SystemClock));
    transport.script_subscription("Signal", vec![SubscriptionEvent::Ready]);
    let rows = Arc::new(Rows::default());
    let live = database.watch(StreamRequest::Events { model: "Notice".into() }, rows.clone());
    within(live.wait_for_initial_load()).await.ok();
    database.close_now();
    yield_until("the event stream ended", || !rows.ended().is_empty()).await;
    assert_eq!(rows.ended(), vec![StreamEnd::Closed]);
}

#[tokio::test]
async fn closing_the_database_ends_a_blob_status_stream_with_one_outcome() {
    use std::io::Write;
    let transport = Arc::new(MockTransport::new());
    let wired = transport.clone();
    let database = Database::start_with(config(), "[]".into(), Some(crate::upload::UploadOperations::empty().create_blob("CreateBlob")), Arc::new(crate::pacing::SystemClock), move |_, _| Ok((Some(wired), None))).unwrap();
    database.register_operations(r#"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { id } }","rootPath":"createBlob"}]"#.into()).unwrap();
    database.set_online(false).unwrap();
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(b"blob").unwrap();
    let id = { let sync = database.sync.as_ref().unwrap(); crate::sync::rig::app(sync, sync.upload_blob(&database.db.admit().unwrap(), "test", "CreateBlob", crate::upload::UploadFile::new(file.path().to_str().unwrap()))).await.unwrap() };
    let rows = Arc::new(Rows::default());
    database.watch(StreamRequest::Blob { id }, rows.clone());
    yield_until("the first status arrived", || rows.values().iter().any(|value| matches!(value, super::StreamValue::Status { .. }))).await;
    assert!(matches!(rows.values().as_slice(), [super::StreamValue::State { state }, super::StreamValue::Status { .. }, ..] if state.settled), "the stream opens with one settled state, then the status: {:?}", rows.values());
    database.close_now();
    yield_until("the status stream ended", || !rows.ended().is_empty()).await;
    assert_eq!(rows.ended(), vec![StreamEnd::Closed]);
}

#[tokio::test]
async fn close_releases_old_engine() {
    let clock = Arc::new(ManualClock::new(std::time::Duration::from_secs(1)));
    let (database, transport) = with_transport(clock.clone());
    let slot = OpenSlot::new();
    database.hold(slot.acquire().unwrap());
    let sink = database.online_sink().unwrap();
    let owner = Arc::downgrade(&database.sync.as_ref().unwrap().inner);
    let connectivity = database.db.connectivity();
    database.close_now();
    sink.set_online(false);
    assert!(connectivity.is_online(), "a late call through the retained sink is refused");
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

struct Journal {
    log: Mutex<Vec<String>>,
    hold: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    entered: std::sync::mpsc::Sender<()>,
    registration: std::sync::OnceLock<crate::lifecycle::Registration>,
}

impl Holder for Journal {
    fn close(&self) {}
    fn registration(&self) -> &std::sync::OnceLock<crate::lifecycle::Registration> { &self.registration }
    fn changed(&self, snapshot: &crate::lifecycle::Snapshot) {
        let held = self.hold.lock().unwrap().take();
        if let Some(release) = held {
            self.entered.send(()).unwrap();
            crate::test_wait::recv_within(&release);
        }
        self.log.lock().unwrap().push(format!("{:?}", snapshot.change));
    }
}

#[test]
fn a_transition_returns_after_its_callbacks_ran_even_while_another_thread_delivers() {
    use crate::lifecycle::Change;
    let streams = Arc::new(Lifecycle::default());
    let (entered, entered_receiver) = std::sync::mpsc::channel();
    let (release, release_receiver) = std::sync::mpsc::channel();
    let journal = Arc::new(Journal { log: Mutex::default(), hold: Mutex::new(Some(release_receiver)), entered, registration: Default::default() });
    let at_enrollment = streams.enroll(&journal);
    assert!(at_enrollment.session.is_none() && !at_enrollment.closed);
    let content = |models: Option<Vec<String>>| { let streams = streams.clone(); std::thread::spawn(move || { let permit = streams.begin_blocking(crate::lifecycle::Boundary::Content, None).unwrap(); permit.defer(Change::Cleared(models)); permit.finish(); }) };
    let delivering = content(None);
    entered_receiver.recv_timeout(std::time::Duration::from_secs(5)).expect("the first callback started");
    let ending = content(Some(Vec::new()));
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert!(!ending.is_finished(), "the end of the session returned before its callbacks ran");
    release.send(()).unwrap();
    join_within(delivering);
    join_within(ending);
    assert_eq!(*journal.log.lock().unwrap(), ["Cleared(None)", "Cleared(Some([]))"]);
}

#[test]
fn a_transition_walks_only_the_holders_open_now() {
    let streams = Arc::new(Lifecycle::default());
    let crowd: Vec<_> = (0..4096).map(|_| Arc::new(Counting(AtomicUsize::new(0), None, Mutex::default(), Default::default()))).collect();
    for member in &crowd { streams.enroll(member); }
    drop(crowd);
    let open: Vec<_> = (0..3).map(|_| Arc::new(Counting(AtomicUsize::new(0), None, Mutex::default(), Default::default()))).collect();
    for member in &open { streams.enroll(member); }
    let before = streams.work();
    let permit = streams.begin_blocking(crate::lifecycle::Boundary::Content, None).unwrap();
    permit.defer(crate::lifecycle::Change::Cleared(None));
    permit.finish();
    assert_eq!(streams.work() - before, 3, "a reset visited holders that were closed long ago");
}

struct Blocking {
    entered: std::sync::mpsc::Sender<()>,
    hold: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    closed: AtomicUsize,
    registration: std::sync::OnceLock<crate::lifecycle::Registration>,
}

impl Holder for Blocking {
    fn close(&self) {
        let held = self.hold.lock().unwrap().take();
        if let Some(release) = held {
            self.entered.send(()).unwrap();
            crate::test_wait::recv_within(&release);
        }
        self.closed.fetch_add(1, Ordering::SeqCst);
    }
    fn registration(&self) -> &std::sync::OnceLock<crate::lifecycle::Registration> { &self.registration }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_shutdown_returns_after_the_first_closed_every_member_and_the_slot_frees_then() {
    let database = local_notes().await;
    let slot = OpenSlot::new();
    database.hold(slot.acquire().unwrap());
    let (entered, entered_receiver) = std::sync::mpsc::channel();
    let (release, release_receiver) = std::sync::mpsc::channel();
    let member = Arc::new(Blocking { entered, hold: Mutex::new(Some(release_receiver)), closed: AtomicUsize::new(0), registration: Default::default() });
    database.streams.enroll(&member);
    let first = { let database = database.clone(); std::thread::spawn(move || database.close_now()) };
    crate::test_wait::recv_within(&entered_receiver);
    let second = { let database = database.clone(); std::thread::spawn(move || database.close_now()) };
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert!(!second.is_finished(), "the second shutdown returned while the first still closed its members");
    assert!(slot.acquire().is_err(), "the slot was free while a member was still closing");
    release.send(()).unwrap();
    join_within(first);
    join_within(second);
    assert_eq!(member.closed.load(Ordering::SeqCst), 1);
    assert!(slot.acquire().is_ok(), "the slot stayed taken after the close finished");
}

struct Reentering {
    database: Mutex<Option<Arc<Database>>>,
    slot: Arc<OpenSlot>,
    slot_taken_inside: AtomicUsize,
    registration: std::sync::OnceLock<crate::lifecycle::Registration>,
}

impl Holder for Reentering {
    fn close(&self) {
        let database = self.database.lock().unwrap().take();
        if let Some(database) = database {
            database.close_now();
            self.slot_taken_inside.store(usize::from(self.slot.acquire().is_err()), Ordering::SeqCst);
        }
    }
    fn registration(&self) -> &std::sync::OnceLock<crate::lifecycle::Registration> { &self.registration }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_member_that_shuts_the_database_down_again_does_not_wait_for_itself_or_free_the_slot() {
    let database = local_notes().await;
    let slot = OpenSlot::new();
    database.hold(slot.acquire().unwrap());
    let member = Arc::new(Reentering { database: Mutex::new(Some(database.clone())), slot: slot.clone(), slot_taken_inside: AtomicUsize::new(0), registration: Default::default() });
    database.streams.enroll(&member);
    let closing = { let database = database.clone(); std::thread::spawn(move || database.close_now()) };
    join_within(closing);
    assert_eq!(member.slot_taken_inside.load(Ordering::SeqCst), 1, "the inner shutdown freed the slot before the outer close finished");
    assert!(slot.acquire().is_ok(), "the outer shutdown did not free the slot");
}
