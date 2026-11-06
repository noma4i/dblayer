use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;

use serde_json::json;

use super::rig_tests::{bound, Fault, Rig, Switchable, World};
use crate::ffi::lifecycle_tests::named;
use crate::ffi::tape::StreamDelivery;
use crate::ffi::{CommandRequest, Database, DbError, ReadEnd, ReadFetch, ReadPhase, ReadRequest, ReadValue, RowPlace, RowRename, WireOperationOutcome, WireRow, WireSyncState};
use crate::test_wait::{within, yield_until};

/// What a platform reader holds: the deliveries in order, replayed into the rows its screen shows.
#[derive(Default)]
struct Reader {
    log: Mutex<Vec<StreamDelivery>>,
    threads: Mutex<Vec<ThreadId>>,
    lose_next: AtomicBool,
    changed: tokio::sync::Notify,
}

impl crate::ffi::tape::Reader for Reader {
    fn delivered(&self, delivery: StreamDelivery) {
        self.threads.lock().unwrap().push(std::thread::current().id());
        if self.lose_next.swap(false, Ordering::SeqCst) { return; }
        self.log.lock().unwrap().push(delivery);
        self.changed.notify_one();
    }
}

fn apply_delta(rows: &mut Vec<WireRow>, carried: Vec<WireRow>, indexes: &[u32], removed: &[String], placed: &[RowPlace], replaced: &[RowRename], size: u32) {
    let by_id: HashMap<String, WireRow> = carried.iter().map(|row| (row.id.clone(), row.clone())).collect();
    for pair in replaced {
        let at = rows.iter().position(|row| row.id == pair.from).expect("a replaced row is held");
        rows[at] = by_id[&pair.to].clone();
    }
    rows.retain(|row| !removed.contains(&row.id));
    let mut taken = HashMap::new();
    for place in placed {
        if let Some(at) = rows.iter().position(|row| row.id == place.id) { taken.insert(place.id.clone(), rows.remove(at)); }
    }
    let mut ordered: Vec<&RowPlace> = placed.iter().collect();
    ordered.sort_by_key(|place| place.index);
    for place in ordered {
        let row = taken.remove(&place.id).or_else(|| by_id.get(&place.id).cloned()).expect("a placed row is known");
        rows.insert(place.index as usize, row);
    }
    for (row, index) in carried.into_iter().zip(indexes) {
        let index = *index as usize;
        if rows[index].id == row.id && rows[index].cells == row.cells && rows[index].sync == row.sync { continue; }
        rows[index] = row;
    }
    assert_eq!(rows.len(), size as usize, "the delta leaves the size it names");
}

impl Reader {
    fn open() -> Arc<Reader> { Arc::new(Reader::default()) }
    fn deliveries(&self) -> Vec<StreamDelivery> { self.log.lock().unwrap().clone() }
    fn len(&self) -> usize { self.log.lock().unwrap().len() }

    /// The rows after the last snapshot and the deltas that chain from it. A delta whose base is not the held revision is a gap and is counted.
    fn replay(&self) -> (Vec<WireRow>, usize) {
        let (mut rows, mut revision, mut gaps) = (Vec::new(), None, 0);
        for delivery in self.deliveries() {
            match delivery.value {
                ReadValue::Snapshot { revision: held, rows: whole, .. } => { rows = whole; revision = Some(held); }
                ReadValue::Delta { base, revision: next, size, rows: carried, indexes: row_indexes, removed, placed, replaced, .. } => {
                    if revision != Some(base) { gaps += 1; continue; }
                    apply_delta(&mut rows, carried, &row_indexes, &removed, &placed, &replaced, size);
                    revision = Some(next);
                }
                _ => {}
            }
        }
        (rows, gaps)
    }

    fn rows(&self) -> Vec<WireRow> { self.replay().0 }
    fn gaps(&self) -> usize { self.replay().1 }
    fn shows(&self, account: &str) -> bool { let rows = self.rows(); !rows.is_empty() && rows.iter().all(|row| named(row, account)) }
    fn ever_showed(&self, account: &str) -> bool {
        self.deliveries().iter().any(|delivery| match &delivery.value {
            ReadValue::Snapshot { rows, .. } | ReadValue::Delta { rows, .. } => rows.iter().any(|row| named(row, account)),
            _ => false,
        })
    }
    fn ended(&self) -> Vec<ReadEnd> {
        self.deliveries().into_iter().filter_map(|delivery| if let ReadValue::Ended { cause } = delivery.value { Some(cause) } else { None }).collect()
    }
    fn texts(&self) -> Vec<String> {
        self.rows().iter().map(|row| row.cells.iter().filter_map(|cell| if let crate::ffi::Cell::Text { value } = cell { Some(value.clone()) } else { None }).collect::<Vec<_>>().join("|")).collect()
    }
}

fn profile() -> ReadRequest { ReadRequest::List { query_json: r#"{"model":"User","relation":"profile","params":{}}"#.into() } }

fn users() -> ReadRequest { ReadRequest::List { query_json: r#"{"model":"User","relation":"all","params":{}}"#.into() } }

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_one_load_interest_keeps_the_other_call_alive() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let before = rig.world.count("Users");
    let held = rig.world.hold_next("Users");
    let idle = rig.database.life.waiting_calls();
    let first = rig.database.begin_load(epoch, "User".into(), "all".into(), "{}".into()).unwrap();
    held.entered_for("the shared load").await;
    let second = rig.database.begin_load(epoch, "User".into(), "all".into(), "{}".into()).unwrap();
    yield_until("both calls wait for the shared load", || rig.database.life.waiting_calls() >= idle + 2).await;
    let owners = rig.database.life.clone();
    assert!(tokio::task::spawn_blocking(move || owners.barrier(std::time::Duration::from_secs(5))).await.unwrap(), "the owners handled both loads");
    first.cancel();
    held.release();
    bound(second.result()).await.unwrap();
    assert_eq!(rig.world.count("Users") - before, 1, "{:?}", rig.world.log());
    let rows = bound(rig.database.read_query(epoch, r#"{"model":"User","relation":"all","params":{}}"#.into())).await.unwrap();
    assert_eq!(ids_of(rows), ["user"]);
    rig.database.close_now();
}

#[derive(Default)]
struct CancelledLoadTransport {
    started: tokio::sync::Notify,
    cancelled: Arc<tokio::sync::Notify>,
}

struct CancelledLoad(Arc<tokio::sync::Notify>);

impl Drop for CancelledLoad {
    fn drop(&mut self) { self.0.notify_one(); }
}

#[async_trait::async_trait]
impl crate::ffi::Transport for CancelledLoadTransport {
    async fn open_cable(&self, _: Arc<crate::ffi::CableLease>, _url: String) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { None }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> {
        let _cancelled = CancelledLoad(self.cancelled.clone());
        self.started.notify_one();
        std::future::pending().await
    }
    async fn mutate(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: crate::ffi::TransportRequest, _: Arc<crate::ffi::SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<crate::ffi::ByteProgress>) -> Result<crate::ffi::PutReply, DbError> { unreachable!() }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_the_last_load_interest_cancels_the_request() {
    let transport = Arc::new(CancelledLoadTransport::default());
    let database = bound(Database::open(crate::ffi::Config { path: None, data_version: None, cache_rows: 32 }, crate::ffi::lifecycle_tests::models(), Some(transport.clone()), None, None, None, None)).await.unwrap();
    database.register_operations(json!([{"name":"Users","document":"query Users { users { id name } }"}]).to_string()).unwrap();
    database.set_online(true).unwrap();
    let request = database.begin_load(database.session_epoch(), "User".into(), "all".into(), "{}".into()).unwrap();
    bound(transport.started.notified()).await;
    request.cancel();
    bound(transport.cancelled.notified()).await;
    database.close_now();
}





#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_after_reopen_and_clock_rollback_keeps_the_new_response() {
    use super::rig_tests::{plain_models, plain_operations};
    let directory = tempfile::tempdir().unwrap();
    let config = crate::ffi::Config { path: Some(directory.path().join("cache.sqlite").to_string_lossy().into_owned()), data_version: None, cache_rows: 32 };
    let opener = crate::ffi::OpenDatabase::new();
    let world_a = World::new("server", "A", true);
    let switch = Switchable::new(Some(world_a.clone()), false);
    let first = bound(opener.open_with_clock(config.clone(), plain_models(), Some(switch.clone()), crate::ffi::TestClock::new(1_000_000), None, None, None, None)).await.unwrap();
    first.register_operations(plain_operations()).unwrap();
    first.set_online(true).unwrap();
    bound(first.refresh(first.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.unwrap();
    let query = r#"{"model":"User","relation":"profile","params":{}}"#.to_string();
    let rows = bound(first.read_query(first.session_epoch(), query.clone())).await.unwrap();
    assert!(rows.iter().any(|row| named(row, "A")));
    bound(first.close_database()).await.unwrap();
    let world_b = World::new("server", "B", true);
    switch.set(Some(world_b.clone()));
    let second = bound(opener.open_with_clock(config, plain_models(), Some(switch), crate::ffi::TestClock::new(100), None, None, None, None)).await.unwrap();
    second.register_operations(plain_operations()).unwrap();
    second.set_online(true).unwrap();
    bound(second.refresh(second.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.unwrap();
    let rows = bound(second.read_query(second.session_epoch(), query)).await.unwrap();
    assert!(rows.iter().any(|row| named(row, "B")));
    assert_eq!((world_a.count("MyProfile"), world_b.count("MyProfile")), (1, 1));
    second.close_now();
}

fn thread() -> ReadRequest {
    ReadRequest::List { query_json: r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"},"order":[{"field":"at","direction":"asc"}]}"#.into() }
}

pub(super) fn message(epoch: u64, text: &str, at: i64) -> CommandRequest {
    CommandRequest { epoch, model: "Message".into(), values_json: json!({"chatId":"c1","text":text,"at":at}).to_string(), args_json: "{}".into(), upload: None }
}

async fn settle(rig: &Rig) { rig.settle().await; }

#[tokio::test(flavor = "multi_thread")]
async fn a_profile_delivery_is_ready_with_its_rows_before_an_explicit_refresh() {
    let rig = Rig::plain(World::new("server", "A", true), false).await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    rig.database.subscribe_reading(epoch, profile(), reader.clone());
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if reader.deliveries().iter().any(|delivery| match &delivery.value {
                ReadValue::Snapshot { state, .. } | ReadValue::Delta { state, .. } | ReadValue::State { state, .. } => state.settled(),
                _ => false,
            }) { break; }
            changed.await;
        }
    }).await;
    assert!(reader.shows("A"));
    assert_eq!(rig.world.count("MyProfile"), 1);
    bound(rig.database.refresh(epoch, "User".into(), "profile".into(), "{}".into())).await.unwrap();
    assert_eq!(rig.world.count("MyProfile"), 2);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cold_profile_delivers_a_complete_first_frame_when_empty_or_failed() {
    for failed in [false, true] {
        let world = World::new("server", "A", true);
        if failed { world.fail_next("MyProfile", Fault::Business); }
        if !failed { world.empty_profile(); }
        let rig = Rig::plain(world, false).await;
        let reader = Reader::open();
        rig.database.subscribe_reading(rig.database.session_epoch(), profile(), reader.clone());
        bound(async {
            loop {
                let changed = reader.changed.notified();
                if reader.deliveries().iter().any(|delivery| match &delivery.value {
                    ReadValue::Snapshot { state, .. } | ReadValue::Delta { state, .. } | ReadValue::State { state, .. } => state.settled(),
                    _ => false,
                }) { break; }
                changed.await;
            }
        }).await;
        assert!(matches!(&reader.deliveries()[0].value, ReadValue::Snapshot { rows, .. } if rows.is_empty()), "first delivery is not a complete empty frame");
        let last = reader.deliveries().into_iter().filter_map(|delivery| match delivery.value {
            ReadValue::Snapshot { state, .. } | ReadValue::Delta { state, .. } | ReadValue::State { state, .. } if state.settled() => Some(state),
            _ => None,
        }).last().unwrap();
        assert_eq!(last.phase, if failed { ReadPhase::Failed } else { ReadPhase::Loaded });
        assert_eq!(last.error.is_some(), failed);
        rig.database.close_now();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cached_entity_remains_loaded_when_its_initial_network_read_fails() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let request = || ReadRequest::Entity { model: "User".into(), id: "user".into(), fetch: ReadFetch::Force };
    let first = rig.database.subscribe_reading(epoch, request(), Reader::open());
    bound(rig.database.loaded(first)).await.unwrap();
    rig.database.cancel_stream(first);
    rig.world.fail_next("UserNode", Fault::Business);
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, request(), reader.clone());
    let result = bound(rig.database.loaded(stream)).await;
    assert!(result.is_ok(), "a cached entity should remain loaded: {result:?}");
    assert_eq!(reader.rows().first().map(|row| row.id.as_str()), Some("user"));
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if reader.deliveries().iter().any(|delivery| match &delivery.value {
                ReadValue::Snapshot { state, .. } | ReadValue::Delta { state, .. } | ReadValue::State { state, .. } => state.error.is_some(),
                _ => false,
            }) { break; }
            changed.await;
        }
    }).await;
    assert!(reader.deliveries().iter().any(|delivery| match &delivery.value {
        ReadValue::Snapshot { state, .. } | ReadValue::Delta { state, .. } | ReadValue::State { state, .. } => state.phase == ReadPhase::Loaded && state.error.is_some(),
        _ => false,
    }));
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_held_entity_ends_its_load_wait_after_session_reset() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(rig.database.session_epoch(), ReadRequest::Entity { model: "User".into(), id: "user".into(), fetch: ReadFetch::Force }, reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    bound(rig.database.session_reset()).await.unwrap();
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if !reader.ended().is_empty() { break; }
            changed.await;
        }
    }).await;
    assert_eq!(reader.ended(), vec![ReadEnd::SessionEnded]);
    assert!(matches!(bound(rig.database.loaded(stream)).await, Err(DbError::Interrupted { .. })));
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_reader_before_its_first_load_interrupts_the_wait() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let gate = rig.world.hold_next("UserNode");
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(rig.database.session_epoch(), ReadRequest::Entity { model: "User".into(), id: "other".into(), fetch: ReadFetch::Force }, reader.clone());
    gate.entered().await;
    rig.database.cancel_stream(stream);
    gate.release();
    assert!(matches!(bound(rig.database.loaded(stream)).await, Err(DbError::Interrupted { .. })));
    assert!(reader.rows().is_empty());
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_delivery_wait_leaves_the_stream_running() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let gate = rig.world.hold_next("UserNode");
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(rig.database.session_epoch(), ReadRequest::Entity { model: "User".into(), id: "user".into(), fetch: ReadFetch::Force }, reader.clone());
    gate.entered_for("the entity request").await;
    let (started, waiting) = tokio::sync::oneshot::channel();
    let pending = { let reader = reader.clone(); tokio::spawn(async move {
        let _ = started.send(());
        loop {
            let changed = reader.changed.notified();
            if reader.deliveries().iter().any(|delivery| matches!(&delivery.value, ReadValue::Snapshot { state, .. } | ReadValue::Delta { state, .. } | ReadValue::State { state, .. } if state.settled())) { return; }
            changed.await;
        }
    }) };
    bound(waiting).await.unwrap();
    pending.abort();
    assert!(bound(pending).await.unwrap_err().is_cancelled());
    gate.release();
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if !reader.rows().is_empty() && reader.deliveries().iter().any(|delivery| matches!(&delivery.value, ReadValue::Snapshot { state, .. } | ReadValue::Delta { state, .. } | ReadValue::State { state, .. } if state.settled())) { break; }
            changed.await;
        }
    }).await;
    assert_eq!(ids_of(reader.rows()), ["user"]);
    rig.database.cancel_stream(stream);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mutation_answer_requires_one_reread_after_an_older_load() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let creation = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    let crate::ffi::WireOperationOutcome::Confirmed { value: Some(created), .. } = bound(rig.database.outcome(creation)).await.unwrap() else { panic!("the group was not confirmed") };
    rig.world.set_members("JOINED", &["u1", "u2"]);
    let query = json!({"model":"Member", "relation":"group_members", "params":{"groupId":created.id,"order":"JOINED"}}).to_string();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, ReadRequest::List { query_json: query }, reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    let before = rig.world.count("GroupMembers");
    let older = rig.world.hold_next("GroupMembers");
    let response = rig.world.hold_next("RemoveMember");
    let reread = rig.world.hold_next("GroupMembers");
    let refresh = { let database = rig.database.clone(); tokio::spawn(async move { database.refresh_stream(stream).await }) };
    older.entered().await;
    let action = { let database = rig.database.clone(); let id = created.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, "remove_member".into(), "{}".into(), json!({"userIds":["u1"]}).to_string()).await }) };
    response.entered().await;
    rig.world.set_members("JOINED", &["u2", "u3"]);
    response.release();
    let receipt = bound(action).await.unwrap().unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    assert_eq!(rig.world.count("GroupMembers"), before + 1);
    older.release();
    reread.entered().await;
    assert_eq!(rig.world.count("GroupMembers"), before + 2);
    reread.release();
    bound(refresh).await.unwrap().unwrap();
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if reader.rows().iter().map(|row| row.id.as_str()).collect::<Vec<_>>() == ["u2", "u3"] { break; }
            changed.await;
        }
    }).await;
    assert_eq!(rig.world.count("GroupMembers"), before + 2);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_receipt_revision_arrives_with_the_updated_row() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, profile(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    let receipt = bound(rig.database.update(epoch, "User".into(), "user".into(), json!({"name":"B"}).to_string(), "{}".into())).await.unwrap();
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if reader.deliveries().iter().any(|delivery| match &delivery.value {
                ReadValue::Snapshot { revision, .. } | ReadValue::Delta { revision, .. } => *revision >= receipt.revision,
                _ => false,
            }) { break; }
            changed.await;
        }
    }).await;
    assert!(reader.shows("B"));
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fresh_profile_is_read_again_after_expiry() {
    let rig = Rig::plain(World::new("server", "A", true), false).await;
    let epoch = rig.database.session_epoch();
    let load = || rig.database.load(epoch, "User".into(), "profile".into(), "{}".into());
    let requests = || rig.world.log().iter().filter(|call| call.operation == "MyProfile").count();
    bound(load()).await.unwrap();
    rig.clock.advance(299_000);
    bound(load()).await.unwrap();
    assert_eq!(requests(), 1, "a fresh window answers without a request");
    rig.clock.advance(2_000);
    bound(load()).await.unwrap();
    assert_eq!(requests(), 2, "an expired window asks again");
    rig.database.close_now();
}

fn chat_entry(index: usize) -> serde_json::Value {
    json!({"topic":"MESSAGE","action":"UPDATED","recordId":format!("m{index}"),"chat":{"id":format!("c{index}"),"title":"t","revision":1,"lists":["PRIMARY"]}})
}

/// The transport of a database with a session: the cable of the server is a peer that seals the frames.
struct CableTransport {
    cable: crate::transport::encrypted::cable::peer_tests::CablePeer,
    token: Mutex<Option<String>>,
    subscribed: tokio::sync::Notify,
}

impl CableTransport {
    fn new(config: crate::transport::EncryptedConfig) -> Self { Self { cable: crate::transport::encrypted::cable::peer_tests::CablePeer::new(config), token: Mutex::new(None), subscribed: tokio::sync::Notify::new() } }

    /// A data event of the open subscription, once the client subscribed.
    fn emit(&self, result: serde_json::Value) {
        let token = self.token.lock().unwrap().clone().expect("a subscription is open");
        assert!(self.cable.event(&token, result), "the subscription is open");
    }

    async fn subscription(&self) {
        loop {
            let notified = self.subscribed.notified();
            if self.token.lock().unwrap().is_some() { return; }
            bound(notified).await;
        }
    }
}

#[async_trait::async_trait]
impl crate::ffi::Transport for CableTransport {
    async fn open_cable(&self, lease: Arc<crate::ffi::CableLease>, _url: String) -> Result<(), DbError> { self.cable.open(&lease).map(drop) }
    async fn cable_send(&self, token: String, text: String) -> Result<(), DbError> {
        if self.cable.receive(&token, &text)?.is_some() {
            *self.token.lock().unwrap() = Some(token);
            self.subscribed.notify_one();
        }
        Ok(())
    }
    async fn cable_next(&self, token: String) -> Option<crate::ffi::EncryptedSocketEvent> { self.cable.next(&token).await }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: crate::ffi::TransportRequest, _: Arc<crate::ffi::SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<crate::ffi::ByteProgress>) -> Result<crate::ffi::PutReply, DbError> { unreachable!() }
}

/// S209: a reader of events that pulls nothing while 400 events arrive reads the kept events and the lost ranges, which add up to the 400 sent, then one end after the database closed.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_events_reader_reads_the_kept_events_and_one_exact_lost_range_then_one_end() {
    let (database, transport, _) = chat_events().await;
    let stream = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Chat".into() });
    let opened = bound(stream.pull()).await.unwrap();
    assert!(matches!(opened, Some(ReadValue::Ready)), "the first value is the open source: {opened:?}");

    let watcher = Reader::open();
    database.subscribe_reading(database.session_epoch(), ReadRequest::Events { model: "Chat".into() }, watcher.clone());
    transport.subscription().await;
    transport.emit(json!({"data":{"recordEvents":{"entries":(0..400).map(chat_entry).collect::<Vec<_>>()}}}));
    let seen = |reader: &Reader| reader.deliveries().iter().map(|delivery| match &delivery.value { ReadValue::Event { .. } => 1, ReadValue::Lost { first, last } => last - first + 1, _ => 0 }).sum::<u64>();
    yield_until("the events reached a reading stream", || seen(&watcher) == 400).await;
    let mut burst = Vec::new();
    while seen_values(&burst) < 400 { burst.push(bound(stream.pull()).await.unwrap().expect("the stream is open")); }
    let kept = burst.iter().filter(|value| matches!(value, ReadValue::Event { .. })).count() as u64;
    let lost: u64 = burst.iter().map(|value| match value { ReadValue::Lost { first, last } => last - first + 1, _ => 0 }).sum();
    assert_eq!(kept + lost, 400, "kept events and lost positions add up to the events sent");
    assert!(lost > 0, "a reader that pulled nothing lost the oldest events");
    database.close_now();
    assert!(matches!(bound(stream.pull()).await.unwrap(), Some(ReadValue::Ended { cause: ReadEnd::Closed })), "the stream ends once at the close");
    assert!(bound(stream.pull()).await.unwrap().is_none());
}

fn seen_values(values: &[ReadValue]) -> u64 { values.iter().map(|value| match value { ReadValue::Event { .. } => 1, ReadValue::Lost { first, last } => last - first + 1, _ => 0 }).sum() }

/// A database whose chats arrive through a change feed with the chat in its payload.
async fn chat_events() -> (Arc<Database>, Arc<CableTransport>, Arc<World>) {
    use crate::schema::{Coverage, FieldSpec, Membership, ModelSpec, NodeRead, RelationSpec, RemoteShape, VariableSource, NODE_PARAM};
    use crate::value::ScalarKind;
    let mut revision = FieldSpec::scalar("serverRevision", ScalarKind::Int);
    revision.from = Some("revision".into());
    let mut chat = ModelSpec::new("Chat", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("title", ScalarKind::Str), revision, FieldSpec::list("lists", ScalarKind::Str)]);
    chat.server_revision = Some("serverRevision".into());
    let mut node = RelationSpec::new("node", &[]);
    node.text_params = vec![NODE_PARAM.into()];
    node.node = Some(NodeRead::One);
    let mut remote = crate::schema_fixtures::remote("ChatNode", RemoteShape::Single, "5.minutes");
    remote.variables.insert("id".into(), VariableSource::Param { name: NODE_PARAM.into() });
    node.remote = Some(remote);
    let mut list = RelationSpec::new("list", &[]);
    list.text_params.push("list".into());
    list.membership.insert("list".into(), Membership::Contains("lists".into()));
    list.joins_node = true;
    list.joins_events = true;
    let mut listed = crate::schema_fixtures::remote("ChatList", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes");
    listed.variables.insert("list".into(), VariableSource::Param { name: "list".into() });
    list.remote = Some(listed);
    chat.relations.extend([node, list]);
    chat.changes.push(serde_json::from_value(json!({"area":"MESSAGE","relation":"node","payload":"chat"})).unwrap());
    chat.change_feed = Some(crate::schema::ChangeFeed { subscription: "RecordEvents".into(), root: "recordEvents.entries".into(), area: "topic".into(), kind: "action".into(), id: "recordId".into() });
    let world = World::new("server", "A", true);
    let mut encrypted = crate::transport::encrypted::protocol_tests::config();
    encrypted.endpoints.session_operations.insert("login_link".into(), crate::ffi::EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: crate::transport::EncryptedOperationClass::Authenticated });
    let transport = Arc::new(CableTransport::new(encrypted.clone()));
    let database = bound(Database::open_with_clock(crate::ffi::Config { path: None, data_version: None, cache_rows: 32 }, serde_json::to_string(&vec![chat]).unwrap(), Some(transport.clone()), crate::ffi::TestClock::new(1_000_000), Some(encrypted), Some(Arc::new(crate::ffi::session_guards_tests::WatchedHeld::default())), Some(world.clone()), None)).await.unwrap();
    database.register_operations(json!([
        {"name":"ChatList","document":"query ChatList($list: String) { chats(list: $list) { id title revision lists } }"},
        {"name":"ChatNode","document":"query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title revision lists } } }"},
        {"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId chat { id title revision lists } } } }"}
    ]).to_string()).unwrap();
    database.set_online(true).unwrap();
    bound(database.session_call("login_link".into(), json!({"account": world.account()}).to_string())).await.unwrap();
    (database, transport, world)
}

/// S208 (out_sending_destroy_not_found): a destroy the server answers only with NOT_FOUND is confirmed, the row is gone, and the open list of the model is read once more.
#[tokio::test(flavor = "multi_thread")]
async fn a_missing_destroyed_row_causes_one_reread_of_the_open_list() {
    let world = World::new("server", "A", true);
    let rig = Rig::signed_as(world.clone(), false, |models| {
        let user = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "User").unwrap();
        let mut action = crate::schema::ActionDeclaration::new("destroy", crate::schema::RootShape::Destroy);
        action.mutation = Some("DeleteGroup".into());
        action.variables.insert("id".into(), "id".into());
        user["actions"].as_array_mut().unwrap().push(serde_json::to_value(action).unwrap());
    }).await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, users(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    assert_eq!(ids_of(reader.rows()), ["user"]);
    let before = rig.world.count("Users");
    rig.world.fail_next("DeleteGroup", Fault::Answer(json!({"errors":[{"message":"gone","extensions":{"code":"NOT_FOUND"}}]})));
    let receipt = bound(rig.database.destroy(epoch, "User".into(), "user".into())).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the open list is read once more", || rig.world.count("Users") > before).await;
    settle(&rig).await;
    assert_eq!(rig.world.count("Users") - before, 1);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_event_before_after() {
    let (database, transport, world) = chat_events().await;
    let epoch = database.session_epoch();
    let reader = Reader::open();
    database.subscribe_reading(epoch, ReadRequest::Events { model: "Chat".into() }, reader.clone());
    yield_until("the events stream is open", || reader.deliveries().iter().any(|delivery| matches!(&delivery.value, ReadValue::Ready))).await;
    let params = |list: &str| json!({"list":list}).to_string();
    for list in ["PRIMARY", "SECONDARY"] { bound(database.load(epoch, "Chat".into(), "list".into(), params(list))).await.unwrap(); }
    let before = bound(database.node(epoch, "Chat".into(), "node".into(), "c1".into(), true)).await.unwrap().unwrap();
    assert!(named(&before, "before"));
    let query = |list: &str| json!({"model":"Chat","relation":"list","params":{"list":list}}).to_string();
    assert_eq!(ids_of(bound(database.read_query(epoch, query("SECONDARY"))).await.unwrap()), ["c1"]);
    transport.subscription().await;
    transport.emit(json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"m1","chat":{"id":"c1","title":"after","revision":2,"lists":["PRIMARY"]}}]}}}));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !reader.deliveries().iter().any(|delivery| matches!(&delivery.value, ReadValue::Event { .. })) {
        assert!(std::time::Instant::now() < deadline, "no event arrived: {:?}", reader.deliveries().iter().map(|delivery| format!("{:?}", delivery.value)).collect::<Vec<_>>());
        tokio::task::yield_now().await;
    }
    let (id, before_row, after_row) = reader.deliveries().into_iter().find_map(|delivery| match delivery.value { ReadValue::Event { id, before, after, .. } => Some((id, before, after)), _ => None }).unwrap();
    assert_eq!(id, "c1");
    assert!(named(&before_row.expect("the event carries the row before"), "before"));
    assert!(named(&after_row.expect("the event carries the committed row"), "after"));
    let primary = bound(database.read_query(epoch, query("PRIMARY"))).await.unwrap();
    assert_eq!(ids_of(primary.clone()), ["c1"]);
    assert!(named(&primary[0], "after"));
    assert!(bound(database.read_query(epoch, query("SECONDARY"))).await.unwrap().is_empty());
    let calls = world.log().into_iter().map(|entry| entry.operation).collect::<Vec<_>>();
    assert_eq!(calls, ["login_link", "ChatList", "ChatList", "ChatNode"]);
    database.close_now();
}


/// B01: after the login a stream delivers the account as one snapshot in the epoch of the session, in order, and the first value does not come from the calling thread.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_opened_after_the_login_starts_with_a_snapshot_of_the_account_in_its_epoch() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (reader, caller) = (Reader::open(), std::thread::current().id());
    let stream = rig.database.subscribe_reading(epoch, profile(), reader.clone());
    let newest = bound(rig.database.loaded(stream)).await.unwrap();
    yield_until("the stream shows A", || reader.shows("A")).await;
    let log = reader.deliveries();
    assert!(log.iter().any(|delivery| delivery.seq == newest), "loaded names {newest}, a sequence number the reader did not receive: {log:?}");
    assert!(matches!(log[0].value, ReadValue::Snapshot { .. }), "the first value is a snapshot: {:?}", log[0]);
    assert!(log.iter().all(|delivery| delivery.epoch == epoch && delivery.stream == stream), "every delivery names the stream and its epoch");
    assert!(log.iter().enumerate().all(|(index, delivery)| delivery.seq == index as u64 + 1), "the sequence counts from 1 with no gap");
    assert!(reader.threads.lock().unwrap().iter().all(|thread| *thread != caller), "a delivery ran on the thread that subscribed");
    rig.database.close_now();
}

/// B02: the sign-out ends the stream with one outcome and no value of the account follows. Nothing stays open in the core.
#[tokio::test(flavor = "multi_thread")]
async fn the_session_reset_ends_every_stream_with_one_outcome_and_no_account_value_follows() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (profile_log, users_log) = (Reader::open(), Reader::open());
    let streams = [rig.database.subscribe_reading(epoch, profile(), profile_log.clone()), rig.database.subscribe_reading(epoch, users(), users_log.clone())];
    for stream in streams { bound(rig.database.loaded(stream)).await.unwrap(); }
    yield_until("both streams show A", || profile_log.shows("A") && users_log.shows("A")).await;

    bound(rig.database.session_reset()).await.unwrap();
    settle(&rig).await;

    for log in [&profile_log, &users_log] {
        assert_eq!(log.ended(), vec![ReadEnd::SessionEnded], "exactly one outcome");
        assert!(matches!(log.deliveries().last().unwrap().value, ReadValue::Ended { .. }), "nothing follows the outcome");
    }
    assert_eq!(rig.database.stream_census().open, 0);
    assert_eq!(rig.database.subscription_census().await.unwrap().subscriptions, 0);
    let marks = (profile_log.len(), users_log.len());
    rig.world.forbid(true);
    settle(&rig).await;
    assert!(rig.world.violations().is_empty(), "a read followed the exit: {:?}", rig.world.violations());
    assert_eq!(marks, (profile_log.len(), users_log.len()));
    rig.database.close_now();
}

/// B03: the account switch A -> B gives new streams in a new epoch. The old projection ends, B never shows A and A never shows B.
#[tokio::test(flavor = "multi_thread")]
async fn an_account_switch_gives_new_streams_and_no_value_of_a_reaches_b() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch_a = rig.database.session_epoch();
    let (first_profile, first_users) = (Reader::open(), Reader::open());
    for (request, log) in [(profile(), &first_profile), (users(), &first_users)] {
        let stream = rig.database.subscribe_reading(epoch_a, request, log.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
    }
    yield_until("A shows", || first_profile.shows("A") && first_users.shows("A")).await;

    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let epoch_b = rig.database.session_epoch();
    assert_ne!(epoch_a, epoch_b, "the switch moved the epoch");
    let (second_profile, second_users) = (Reader::open(), Reader::open());
    for (request, log) in [(profile(), &second_profile), (users(), &second_users)] {
        let stream = rig.database.subscribe_reading(epoch_b, request, log.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
    }
    yield_until("B shows", || second_profile.shows("B") && second_users.shows("B")).await;
    settle(&rig).await;

    assert!(!second_profile.ever_showed("A") && !second_users.ever_showed("A"), "a value of A reached the projections of B");
    assert!(!first_profile.ever_showed("B") && !first_users.ever_showed("B"), "a value of B reached the projections of A");
    assert!(second_profile.deliveries().iter().all(|delivery| delivery.epoch == epoch_b));
    assert_eq!(first_profile.ended(), vec![ReadEnd::SessionEnded]);
    rig.database.close_now();
}

/// B04: a message sent offline shows at once, goes out once when the network returns, and keeps its position when the server confirms it.
#[tokio::test(flavor = "multi_thread")]
async fn a_message_sent_offline_goes_out_once_and_keeps_its_position() {
    let rig = Rig::encrypted().await;
    rig.world.seed_message("A", "m1", "first", 1);
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, thread(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    yield_until("the thread shows the first message", || reader.texts().iter().any(|text| text.contains("first"))).await;

    rig.database.set_online(false).unwrap();
    let receipt = bound(rig.database.send(message(epoch, "second", 2))).await.unwrap();
    yield_until("the thread shows the queued message", || reader.rows().len() == 2).await;
    let queued = reader.rows();
    assert_eq!(queued[1].id, receipt.optimistic_value.as_ref().unwrap().id, "the new message sits after the first");
    assert_eq!(queued[1].sync.state, WireSyncState::Pending);
    assert_eq!(receipt.optimistic_value.as_ref().unwrap().sync.state, WireSyncState::Pending);
    assert_eq!(rig.world.count("SendMessage"), 0, "the offline send reached the server");

    rig.database.set_online(true).unwrap();
    let outcome = bound(rig.database.outcome(receipt.clone())).await.unwrap();
    assert!(matches!(outcome, WireOperationOutcome::Confirmed { value: Some(row), .. } if row.id == "m2"));
    yield_until("the thread shows the confirmed message", || reader.rows().get(1).is_some_and(|row| row.id == "m2")).await;
    settle(&rig).await;

    assert_eq!(rig.world.count("SendMessage"), 1, "the message went out once");
    let confirmed = reader.rows();
    assert_eq!(confirmed.len(), 2, "one message, not a temporary row and a confirmed row");
    assert_eq!((confirmed[0].id.as_str(), confirmed[1].id.as_str()), ("m1", "m2"), "the position did not change");
    assert_eq!(confirmed[1].sync.state, WireSyncState::Synced);
    assert_eq!(reader.gaps(), 0);
    rig.database.close_now();
}

/// B05: an answer of the old epoch that arrives late changes nothing: the call ends as interrupted, the new projection never shows it, and a command of the old epoch is refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_answer_of_the_old_epoch_is_dropped_and_a_command_of_the_old_epoch_is_refused() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch_a = rig.database.session_epoch();
    let first = Reader::open();
    let stream = rig.database.subscribe_reading(epoch_a, users(), first.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    yield_until("A shows", || first.shows("A")).await;

    let gate = rig.world.hold_next("Users").as_account("A");
    let late = { let database = rig.database.clone(); tokio::spawn(async move { database.refresh_stream(stream).await }) };
    gate.entered().await;
    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let epoch_b = rig.database.session_epoch();
    let second = Reader::open();
    let next = rig.database.subscribe_reading(epoch_b, users(), second.clone());
    bound(rig.database.loaded(next)).await.unwrap();
    yield_until("B shows", || second.shows("B")).await;
    gate.release();

    let answer = bound(late).await.unwrap();
    assert!(matches!(answer, Err(DbError::Interrupted { .. })), "the late call of the old epoch ended as {answer:?}");
    settle(&rig).await;
    assert!(!second.ever_showed("A"), "the late answer of A reached the projection of B");
    assert!(second.shows("B"));
    let refused = bound(rig.database.send(message(epoch_a, "late", 3))).await;
    assert!(matches!(refused, Err(DbError::NoSession { .. })), "a command of the old epoch ran: {refused:?}");
    assert_eq!(rig.world.count("SendMessage"), 0);
    rig.database.close_now();
}

/// B06: the reader that cancels ends the stream in the core: no subscription stays and no value is delivered after.
#[tokio::test(flavor = "multi_thread")]
async fn the_reader_that_cancels_ends_the_stream_and_the_core_keeps_nothing() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, users(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    yield_until("A shows", || reader.shows("A")).await;
    assert_eq!(rig.database.stream_census().open, 1);

    rig.database.cancel_stream(stream);

    yield_until("the stream left the census", || rig.database.stream_census().open == 0).await;
    assert_eq!(rig.database.subscription_census().await.unwrap().subscriptions, 0);
    assert!(reader.ended().is_empty(), "a cancelled stream delivers no outcome");
    let mark = reader.len();
    rig.world.set_account("A2");
    bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), "all".into(), "{}".into())).await.unwrap();
    settle(&rig).await;
    assert_eq!(reader.len(), mark, "a value reached a cancelled reader");
    assert!(matches!(bound(rig.database.refresh_stream(stream)).await, Err(DbError::Interrupted { .. })));
    rig.database.close_now();
}

/// B07, spec F: an idle stream costs no request and no wakeup, a change of one row delivers one row, and the number of subscriptions is the number of readers.
#[tokio::test(flavor = "multi_thread")]
async fn work_counters_idle_zero_one_row_one_decode_and_subscriptions_equal_readers() {
    let rig = Rig::encrypted().await;
    for (at, text) in [(1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five")] { rig.world.seed_message("A", &format!("s{at}"), text, at); }
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (reader, other) = (Reader::open(), Reader::open());
    let stream = rig.database.subscribe_reading(epoch, thread(), reader.clone());
    let second = rig.database.subscribe_reading(epoch, profile(), other.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    bound(rig.database.loaded(second)).await.unwrap();
    yield_until("the thread holds 5 rows", || reader.rows().len() == 5).await;
    settle(&rig).await;
    assert_eq!(rig.database.stream_census().open, 2);

    let idle = (rig.world.hits(), rig.database.db.counters().observer_calls, reader.len(), other.len());
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert_eq!(idle, (rig.world.hits(), rig.database.db.counters().observer_calls, reader.len(), other.len()), "an idle stream did work");

    let mut costs = Vec::new();
    for (index, text) in ["six", "seven"].into_iter().enumerate() {
        let (before, mark) = (rig.database.db.counters().rows_delivered, reader.len());
        let receipt = bound(rig.database.send(message(epoch, text, 6 + index as i64))).await.unwrap();
        bound(rig.database.outcome(receipt)).await.unwrap();
        settle(&rig).await;
        let delta_rows: Vec<usize> = reader.deliveries().iter().skip(mark).filter_map(|delivery| if let ReadValue::Delta { rows, .. } = &delivery.value { Some(rows.len()) } else { None }).collect();
        assert!(!delta_rows.is_empty() && delta_rows.iter().all(|rows| *rows == 1), "a change of one row carried {delta_rows:?} rows");
        costs.push((delta_rows.len(), rig.database.db.counters().rows_delivered - before));
    }
    assert_eq!(costs[0].1 * costs[1].0, costs[1].1 * costs[0].0, "the rows per delta grew with the list: {costs:?}");
    assert!(costs[1].1 <= 2 * costs[1].0, "the core delivered more rows than the deltas carry: {costs:?}");
    rig.database.close_now();
}

/// B15, spec F: a write to one row of a list delivers nothing to the entity stream of another row, and one value to a count stream, however long the list is.
#[tokio::test(flavor = "multi_thread")]
async fn work_counters_an_entity_stream_ignores_other_rows_and_a_count_stream_gets_one_value_per_write() {
    let rig = Rig::encrypted().await;
    for (at, text) in [(1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five")] { rig.world.seed_message("A", &format!("s{at}"), text, at); }
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (entity, count, list) = (Reader::open(), Reader::open(), Reader::open());
    let streams = [
        rig.database.subscribe_reading(epoch, ReadRequest::Entity { model: "Message".into(), id: "s1".into(), fetch: crate::ffi::ReadFetch::Never }, entity.clone()),
        rig.database.subscribe_reading(epoch, ReadRequest::Count { query_json: r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#.into() }, count.clone()),
        rig.database.subscribe_reading(epoch, thread(), list.clone()),
    ];
    for stream in streams { bound(rig.database.loaded(stream)).await.unwrap(); }
    yield_until("the list holds 5 rows", || list.rows().len() == 5).await;
    settle(&rig).await;

    let mut costs = Vec::new();
    for (index, text) in ["six", "seven"].into_iter().enumerate() {
        let (entity_mark, count_mark) = (entity.len(), count.len());
        let receipt = bound(rig.database.send(message(epoch, text, 6 + index as i64))).await.unwrap();
        bound(rig.database.outcome(receipt)).await.unwrap();
        settle(&rig).await;
        let entity_values = entity.deliveries().len() - entity_mark;
        let counts: Vec<i64> = count.deliveries().iter().skip(count_mark).filter_map(|delivery| if let ReadValue::Count { count, .. } = &delivery.value { Some(*count as i64) } else { None }).collect();
        assert_eq!(entity_values, 0, "a write to another row reached the entity stream");
        assert_eq!(counts.last().copied(), Some(6 + index as i64), "the count stream shows {counts:?}");
        costs.push(counts.len());
    }
    assert_eq!(costs[0], costs[1], "the cost of one write to the count stream grew with the list: {costs:?}");
    assert!(costs[1] <= 2, "one write delivered {} count values", costs[1]);
    rig.database.close_now();
}

/// B10 (R2, old line command.rs:76-85): the outcome waits while the command is queued offline and is owned by the epoch of its receipt: it ends with the session, and a receipt of an old epoch stays ended.
#[tokio::test(flavor = "multi_thread")]
async fn the_outcome_of_an_offline_command_waits_and_ends_with_the_epoch_of_its_receipt() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    let receipt = bound(rig.database.send(message(epoch, "queued", 2))).await.unwrap();
    let outcome = { let database = rig.database.clone(); let receipt = receipt.clone(); tokio::spawn(async move { database.outcome(receipt).await }) };
    for _ in 0..50 { tokio::task::yield_now().await; }
    assert!(!outcome.is_finished(), "the outcome of a queued command returned before the command settled");
    bound(rig.database.session_reset()).await.unwrap();
    assert!(matches!(bound(outcome).await.unwrap().unwrap(), WireOperationOutcome::Interrupted));
    rig.database.set_online(true).unwrap();
    rig.login_ios("B").await;
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Interrupted));
    assert_eq!(rig.world.count("SendMessage"), 0);
    rig.database.close_now();
}

/// B11 (R3, old line stream/mod.rs refresh_stream): a stream request whose caller left does not outlive its epoch: the transition does not wait for the request.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_request_whose_caller_left_ends_with_its_epoch_and_holds_nothing() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, users(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    let gate = rig.world.hold_next("Users").as_account("A");
    let refresh = { let database = rig.database.clone(); tokio::spawn(async move { database.refresh_stream(stream).await }) };
    gate.entered().await;
    refresh.abort();
    let _ = refresh.await;
    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    gate.release();
    settle(&rig).await;
    rig.database.close_now();
}

/// B12 (R4, old line stream/open.rs connect): a stream whose source cannot be connected ends with one outcome and leaves nothing in the table.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_that_cannot_connect_ends_once_and_leaves_the_table() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let requests = [
        ReadRequest::Entity { model: "Nope".into(), id: "x".into(), fetch: crate::ffi::ReadFetch::Never },
        ReadRequest::Events { model: "Nope".into() },
        ReadRequest::Count { query_json: r#"{"model":"Nope"}"#.into() },
    ];
    for request in requests {
        let reader = Reader::open();
        let stream = rig.database.subscribe_reading(epoch, request.clone(), reader.clone());
        yield_until("the failed stream ended", || !reader.ended().is_empty()).await;
        assert_eq!(reader.ended(), vec![ReadEnd::Invalid], "{request:?} ended with more or less than one outcome");
        yield_until("the failed stream left the table", || rig.database.stream_census().open == 0).await;
        assert!(bound(rig.database.loaded(stream)).await.is_err(), "{request:?} reported a first load");
    }
    rig.database.close_now();
}

/// A reader whose first delivery after arming waits for a release.
struct Blocking {
    inner: Arc<Reader>,
    armed: AtomicBool,
    finished: AtomicBool,
    entered: Mutex<std::sync::mpsc::Sender<()>>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl crate::ffi::tape::Reader for Blocking {
    fn delivered(&self, delivery: StreamDelivery) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.lock().unwrap().send(()).unwrap();
            let _ = self.release.lock().unwrap().recv_timeout(std::time::Duration::from_secs(20));
            self.inner.delivered(delivery);
            self.finished.store(true, Ordering::SeqCst);
            return;
        }
        self.inner.delivered(delivery);
    }
}

/// B13 (R5, B5; old line stream/owner.rs cancelled): `cancel_stream` returns while a delivery in the reader is still running, and no delivery begins after it returns.
#[tokio::test(flavor = "multi_thread")]
async fn the_cancel_of_a_reader_returns_while_a_delivery_runs_and_none_is_admitted_after_it() {
    let rig = Rig::encrypted().await;
    rig.world.seed_message("A", "m1", "first", 1);
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (entered, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release) = std::sync::mpsc::channel();
    let blocking = Arc::new(Blocking { inner: Reader::open(), armed: AtomicBool::new(false), finished: AtomicBool::new(false), entered: Mutex::new(entered), release: Mutex::new(release) });
    let stream = rig.database.subscribe_reading(epoch, thread(), blocking.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    yield_until("the thread shows the first message", || blocking.inner.texts().iter().any(|text| text.contains("first"))).await;
    rig.database.set_online(false).unwrap();
    blocking.armed.store(true, Ordering::SeqCst);
    bound(rig.database.send(message(epoch, "second", 2))).await.unwrap();
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap()).await.unwrap();
    rig.database.cancel_stream(stream);
    assert!(!blocking.finished.load(Ordering::SeqCst), "the cancel waited for the reader to finish a delivery");
    release_tx.send(()).unwrap();
    yield_until("the delivery in progress finished", || blocking.finished.load(Ordering::SeqCst)).await;
    let after_cancel = blocking.inner.len();
    bound(rig.database.send(message(epoch, "third", 3))).await.unwrap();
    rig.settle().await;
    assert_eq!(blocking.inner.len(), after_cancel, "a delivery began after the cancel returned");
    assert!(!blocking.inner.texts().iter().any(|text| text.contains("third")));
    rig.database.close_now();
}

pub(super) fn group(name: &str) -> (String, String) { (json!({"name": name}).to_string(), "{}".into()) }

async fn confirmed_group(rig: &Rig, epoch: u64, values: String, args: String) -> WireRow {
    let receipt = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    match bound(rig.database.outcome(receipt)).await.unwrap() {
        WireOperationOutcome::Confirmed { value: Some(row), .. } => row,
        other => panic!("group create did not confirm: {other:?}"),
    }
}

/// K02 (B2, old lines ffi/database.rs update, destroy, read_query, read_row): a command or a one-off read that carries the epoch of A is refused after the session ended, also when it waited for the writer while the session ended, and it changes nothing for B.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::await_holding_lock)]
async fn a_command_and_a_read_of_a_left_account_are_refused_and_change_nothing() {
    let rig = Rig::encrypted().await;
    rig.world.seed_message("B", "b1", "hello", 1);
    rig.login_ios("A").await;
    let old = rig.database.session_epoch();
    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let current = rig.database.session_epoch();
    assert_ne!(old, current);
    let users = r#"{"model":"User","relation":"all","params":{}}"#;
    let refused = |result: Result<(), DbError>| matches!(result, Err(DbError::Interrupted { .. } | DbError::NoSession { .. }));
    assert!(refused(bound(rig.database.read_query(old, users.into())).await.map(|_| ())), "a read of A returned rows in the epoch of B");
    assert!(refused(bound(rig.database.read_row(old, "User".into(), "user".into())).await.map(|_| ())));
    assert!(refused(bound(rig.database.update(old, "User".into(), "user".into(), json!({"name":"hijacked"}).to_string(), "{}".into())).await.map(|_| ())));
    assert!(refused(bound(rig.database.destroy(old, "User".into(), "user".into())).await.map(|_| ())));
    assert!(refused(bound(rig.database.set_local(old, "User".into(), "user".into(), "name".into(), "\"x\"".into())).await));
    assert!(refused(bound(rig.database.upload(old, "User".into(), "user".into(), "n".into(), crate::ffi::upload_wire::WireUploadRequest { path: "/nowhere".into(), name: None, mime: None, variables_json: "{}".into() })).await.map(|_| ())));
    let seen = bound(rig.database.read_row(current, "User".into(), "user".into())).await.unwrap().expect("the row of B");
    assert!(named(&seen, "B"), "the row of B changed");

    rig.database.close_now();
}

/// K03 a send needs a session, and an action that is not a durable command does not wait for the network: it is refused and rolled back, and it never goes out later.
#[tokio::test(flavor = "multi_thread")]
async fn a_send_needs_a_session_and_a_plain_action_never_waits_for_the_network() {
    let rig = Rig::encrypted().await;
    let epoch = rig.database.session_epoch();
    let sent = bound(rig.database.send(message(epoch, "anonymous", 1))).await;
    assert!(matches!(sent, Err(DbError::NoSession { .. })), "a send without a session was accepted: {sent:?}");
    assert!(crate::ffi::queue_view_tests::queued(&rig.database).is_empty(), "a send without a session left an operation");
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("g");
    rig.database.set_online(false).unwrap();
    settle(&rig).await;
    let plain = bound(rig.database.create(epoch, "Group".into(), values, args)).await;
    assert!(matches!(plain, Err(DbError::Network { .. })), "a plain action was not refused while the network was away: {plain:?}");
    settle(&rig).await;
    assert!(crate::ffi::queue_view_tests::queued(&rig.database).is_empty(), "a plain action stayed queued while the network was away");
    assert_eq!(rig.database.db.row_count("Group").unwrap(), 0, "the row of the refused action stayed");
    rig.database.set_online(true).unwrap();
    settle(&rig).await;
    assert_eq!(rig.world.count("CreateGroup"), 0, "a plain action went out after the network returned");
    let receipt = bound(rig.database.send(message(epoch, "offline", 2))).await.unwrap();
    assert_eq!(receipt.optimistic_value.as_ref().unwrap().sync.state, WireSyncState::Pending);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn server_calls_without_a_transport_report_no_session() {
    let rig = Rig::encrypted().await;
    bound(rig.database.close_database()).await.unwrap();
    let database = rig.open_again(None).await.unwrap();
    let epoch = database.session_epoch();
    let (values, args) = group("friends");
    let create = bound(database.create(epoch, "Group".into(), values, args)).await;
    let load = bound(database.load(epoch, "User".into(), "profile".into(), "{}".into())).await;
    let send = bound(database.send(message(epoch, "hello", 1))).await;
    assert!(matches!(create, Err(DbError::NoSession { .. })), "create: {create:?}");
    assert!(matches!(load, Err(DbError::NoSession { .. })), "load: {load:?}");
    assert!(matches!(send, Err(DbError::NoSession { .. })), "send: {send:?}");
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn reset_clears_ephemeral_values_before_the_next_account_reads_the_same_id() {
    use crate::schema::{EphemeralSpec, FieldSpec};
    use crate::value::ScalarKind;
    let typing = serde_json::to_value(EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false)))).unwrap();
    let rig = Rig::encrypted_edited(false, false, false, |models| {
        let user = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "User").unwrap();
        user.as_object_mut().unwrap().entry("ephemeral").or_insert(json!([])).as_array_mut().unwrap().push(typing);
    }).await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    bound(rig.database.set_ephemeral(epoch, "User".into(), "user".into(), r#"{"typing":true}"#.into())).await.unwrap();
    let old = bound(rig.database.read_row(epoch, "User".into(), "user".into())).await.unwrap().unwrap();
    assert!(matches!(old.cells.last(), Some(crate::ffi::Cell::Bool { value: true })));

    bound(rig.database.session_reset()).await.unwrap();
    rig.world.set_account("B");
    rig.login_ios("B").await;
    let current = bound(rig.database.read_row(rig.database.session_epoch(), "User".into(), "user".into())).await.unwrap().unwrap();

    assert!(named(&current, "B"));
    assert!(matches!(current.cells.last(), Some(crate::ffi::Cell::Bool { value: false })));
    rig.database.close_now();
}

/// K04 (B4, old line ffi/database.rs create): a create the server answers goes through `create` with the epoch of the caller, needs the network, and returns the row the server confirmed.
#[tokio::test(flavor = "multi_thread")]
async fn a_server_create_returns_the_confirmed_row_and_needs_the_network() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let row = confirmed_group(&rig, epoch, values.clone(), args.clone()).await;
    assert!(!row.id.starts_with("tmp") && !row.id.contains("temp"), "the created row still has a temporary id: {}", row.id);
    assert!(named(&row, "friends"));
    assert_eq!(rig.world.count("CreateGroup"), 1);
    rig.database.set_online(false).unwrap();
    let offline = bound(rig.database.create(epoch, "Group".into(), values, args)).await;
    assert!(matches!(offline, Err(DbError::Network { .. })), "an offline server create did not fail at once: {offline:?}");
    assert_eq!(rig.world.count("CreateGroup"), 1);
    assert!(crate::ffi::queue_view_tests::queued(&rig.database).is_empty());
    rig.database.close_now();
}

/// A reader whose last reference decides where the platform object dies.
struct DropProbe { held: Arc<Mutex<Vec<String>>>, dropped: Arc<std::sync::atomic::AtomicUsize> }

impl crate::ffi::tape::Reader for DropProbe {
    fn delivered(&self, _: StreamDelivery) {}
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.held.lock().unwrap().extend(crate::lock::held_classes().into_iter().map(str::to_string));
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// K05 (B5, old line stream/owner.rs cancelled): the reader object is released after every library lock, also when the cancel drops the last reference.
#[tokio::test(flavor = "multi_thread")]
async fn the_cancel_of_a_reader_releases_its_object_outside_every_library_lock() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let held = Arc::new(Mutex::new(Vec::new()));
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stream = rig.database.subscribe_reading(epoch, users(), Arc::new(DropProbe { held: held.clone(), dropped: dropped.clone() }));
    bound(rig.database.loaded(stream)).await.unwrap();
    rig.database.cancel_stream(stream);
    yield_until("the stream left the census", || rig.database.stream_census().open == 0).await;
    yield_until("the reader object was released", || dropped.load(Ordering::SeqCst) == 1).await;
    assert_eq!(dropped.load(Ordering::SeqCst), 1, "the reader object was released more than once");
    assert!(held.lock().unwrap().is_empty(), "the reader object was released under {:?}", held.lock().unwrap());
    rig.database.close_now();
}

/// K06 (B6): the row of a durable command shows in the stream of its reader as pending before the server answers, and the server has not been asked.
#[tokio::test(flavor = "multi_thread")]
async fn a_queued_message_shows_as_pending_in_the_thread_before_the_server_answers() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, thread(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    rig.database.set_online(false).unwrap();
    bound(rig.database.send(message(epoch, "queued", 2))).await.unwrap();
    yield_until("the thread shows the queued message", || reader.rows().iter().any(|row| row.sync.state == WireSyncState::Pending)).await;
    assert_eq!(rig.world.count("SendMessage"), 0, "the message was shown only after the server answered");
    assert_eq!(reader.texts().len(), 1);
    rig.database.close_now();
}

/// K07 a stream that ended leaves no observer and no work behind: a write after it reads nothing and no retained observer is counted.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_list_stream_leaves_no_observer_and_a_later_write_does_no_reading() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, thread(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    rig.database.cancel_stream(stream);
    rig.database.release_stream(stream);
    let census = within(async {
        loop {
            let census = rig.database.subscription_census().await.unwrap();
            if (census.subscriptions, census.observed_queries, census.retained_observers) == (0, 0, 0) { return census; }
            tokio::task::yield_now().await;
        }
    }).await;
    assert_eq!((census.subscriptions, census.observed_queries, census.retained_observers), (0, 0, 0), "a left list stream kept an observer");
    let reads = rig.database.db.counters().observation_reads;
    rig.database.set_online(false).unwrap();
    bound(rig.database.send(message(epoch, "after", 3))).await.unwrap();
    settle(&rig).await;
    assert_eq!(rig.database.db.counters().observation_reads, reads, "a write after the last reader left woke an observer");
    rig.database.close_now();
}


/// K11 (offline finding B): a call that always goes to the server fails at once with `Network` while the network is away, sends nothing, and the entity stream shows the error in its state.
#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_and_a_load_more_fail_at_once_offline_and_the_entity_stream_shows_the_error() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let entity = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, ReadRequest::Entity { model: "User".into(), id: "user".into(), fetch: crate::ffi::ReadFetch::Force }, entity.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    let hits = rig.world.log().len();
    rig.database.set_online(false).unwrap();
    let refreshed = bound(rig.database.refresh_stream(stream)).await;
    assert!(matches!(refreshed, Err(DbError::Network { .. })), "refresh of a stream offline: {refreshed:?}");
    let refreshed = bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), "all".into(), "{}".into())).await;
    assert!(matches!(refreshed, Err(DbError::Network { .. })), "refresh offline: {refreshed:?}");
    let more = bound(rig.database.load_more(rig.database.session_epoch(), "User".into(), "all".into(), "{}".into())).await;
    assert!(matches!(more, Err(DbError::Network { .. })), "load more offline: {more:?}");
    yield_until("the entity stream shows the error", || entity.deliveries().iter().any(|delivery| match &delivery.value {
        ReadValue::State { state, .. } | ReadValue::Snapshot { state, .. } => state.error.is_some(),
        _ => false,
    })).await;
    assert_eq!(rig.world.log().len(), hits, "a call offline reached the server");
    rig.database.close_now();
}

/// K12 (consumer finding forward; old line ffi/database.rs act returned the receiver row): a forward into the same chat and into another chat leaves the source row unchanged, upserts the answer row with its own id in the target chat, and the call returns the answer row. The answer has the shape of the mutation of the app: `messageForward.message` with the id `forwarded-1`.
#[tokio::test(flavor = "multi_thread")]
async fn a_forward_leaves_the_source_unchanged_upserts_the_answer_row_and_returns_it() {
    for (case, target) in [("same chat", "c1"), ("other chat", "c2")] {
        let rig = Rig::encrypted().await;
        rig.world.seed_message("A", "m1", "hello", 1);
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let reader = Reader::open();
        let stream = rig.database.subscribe_reading(epoch, thread(), reader.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
        yield_until("the thread shows the source", || reader.texts().iter().any(|text| text.contains("hello"))).await;
        let before = bound(rig.database.read_row(epoch, "Message".into(), "m1".into())).await.unwrap().expect("the source row");
        let acted = bound(rig.database.act(epoch, "Message".into(), "m1".into(), "forward".into(), "{}".into(), json!({"targetChatId": target}).to_string())).await.unwrap();
        let outcome = bound(rig.database.outcome(acted)).await.unwrap();
        assert!(matches!(outcome, WireOperationOutcome::Confirmed { value: Some(row), .. } if row.id == "forwarded-1"), "{case}: the confirmed row differs");
        let after = bound(rig.database.read_row(epoch, "Message".into(), "m1".into())).await.unwrap().expect("the source row");
        assert_eq!(after.cells, before.cells, "{case}: the answer was written onto the source row");
        let forwarded = bound(rig.database.read_row(epoch, "Message".into(), "forwarded-1".into())).await.unwrap().expect("the answer row");
        assert!(forwarded.cells.contains(&crate::ffi::Cell::Text { value: target.into() }), "{case}: the answer row is not in the target chat");
        if target == "c1" {
            yield_until("the thread shows the source and the answer", || reader.rows().len() == 2).await;
        }
        rig.database.close_now();
    }
}

fn groups() -> ReadRequest { ReadRequest::List { query_json: r#"{"model":"Group","relation":"all","params":{}}"#.into() } }

pub(super) fn member_list(rig: &Rig, group: &str, order: &str) -> Vec<String> {
    let spec = rig.database.db.spec("Member").unwrap();
    let params = json!({"groupId": group, "order": order}).as_object().unwrap().clone();
    let key = spec.selection_key("group_members", &params).unwrap();
    rig.database.db.scope_members("Member", key.as_str()).unwrap().into_iter().map(|(id, _)| id).collect()
}

/// The members u1 u2 u3 of the group in both lists (JOINED: u1 u2 u3, OWNER_FIRST: u2 u1 u3) and the row u4 that is in neither list.
pub(super) fn seed_members(rig: &Rig, group: &str) {
    use crate::commit::{CommitEnvelope, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
    use crate::identity::{ModelKey, RowId};
    let db = &rig.database.db;
    let spec = db.spec("Member").unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for id in ["u1", "u2", "u3", "u4"] {
        let row = json!({"id": id, "name": id}).as_object().unwrap().clone();
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("Member"), id: RowId::new(id).unwrap(), row });
    }
    for (order, ids) in [("JOINED", ["u1", "u2", "u3"]), ("OWNER_FIRST", ["u2", "u1", "u3"])] {
        let params = json!({"groupId": group, "order": order}).as_object().unwrap().clone();
        let scope = ScopeRef::new(ModelKey::new("Member"), spec.selection_key("group_members", &params).unwrap());
        let mut last: Option<String> = None;
        for id in ids {
            let order_key = crate::value::fractional_between(last.as_deref(), None).unwrap();
            last = Some(order_key.clone());
            envelope.scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: RowId::new(id).unwrap(), order_key });
        }
    }
    db.land_in_tests(envelope).unwrap();
}

fn ids_of(rows: Vec<WireRow>) -> Vec<String> { rows.into_iter().map(|row| row.id).collect() }

pub(super) fn strings(ids: &[&str]) -> Vec<String> { ids.iter().map(|id| id.to_string()).collect() }

/// K32: a member added or removed ends with the member lists of the server in both orders: the added user appears in the lists the screen reads and the removed one leaves them, without a read by the app.
#[tokio::test(flavor = "multi_thread")]
async fn adding_and_removing_a_member_ends_with_the_member_lists_of_the_server_in_both_orders() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = confirmed_group(&rig, epoch, values, args).await;
    let orders = [("JOINED", ["u1", "u2", "u3"]), ("OWNER_FIRST", ["u2", "u1", "u3"])];
    for (order, ids) in orders { rig.world.set_members(order, &ids); }
    let mut readers = Vec::new();
    for (order, _) in orders {
        let (reader, query) = (Reader::open(), json!({"model": "Member", "relation": "group_members", "params": {"groupId": created.id, "order": order}}).to_string());
        let stream = rig.database.subscribe_reading(epoch, ReadRequest::List { query_json: query }, reader.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
        readers.push(reader);
    }
    let shown = |readers: &[Arc<Reader>]| readers.iter().map(|reader| reader.rows().into_iter().map(|row| row.id).collect::<Vec<_>>()).collect::<Vec<_>>();
    assert_eq!(shown(&readers), [strings(&["u1", "u2", "u3"]), strings(&["u2", "u1", "u3"])], "the lists before the add");
    type Case = (&'static str, [&'static [&'static str]; 2]);
    let cases: [Case; 2] = [("add_member", [&["u1", "u2", "u3", "u9"], &["u2", "u1", "u3", "u9"]]), ("remove_member", [&["u1", "u2", "u3"], &["u2", "u1", "u3"]])];
    for (action, ended) in cases {
        for (index, (order, _)) in orders.iter().enumerate() { rig.world.set_members(order, ended[index]); }
        let acted = bound(rig.database.act(epoch, "Group".into(), created.id.clone(), action.into(), "{}".into(), json!({"userIds": ["u9"]}).to_string())).await;
        assert!(acted.is_ok(), "{action}: the call failed: {:?}", acted.as_ref().err());
        yield_until(&format!("{action}: both lists end with the lists of the server"), || shown(&readers) == [strings(ended[0]), strings(ended[1])]).await;
    }
    rig.database.close_now();
}

/// K13 (B6, C05; old lines: no rule for these actions): add member and remove member change both member lists (JOINED and OWNER_FIRST) at once, before the server answers. The answer keeps the change, a refusal restores the ids and the order of both lists.
#[tokio::test(flavor = "multi_thread")]
async fn member_actions_change_both_member_lists_before_the_answer_and_a_refusal_restores_them() {
    let joined = |ids: &[&str]| ids.iter().map(|id| id.to_string()).collect::<Vec<_>>();
    type Case = (&'static str, &'static str, &'static str, [&'static [&'static str]; 2], [&'static [&'static str]; 2]);
    let cases: [Case; 2] = [
        ("add_member", "AddMember", "u4", [&["u1", "u2", "u3", "u4"], &["u2", "u1", "u3", "u4"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
        ("remove_member", "RemoveMember", "u2", [&["u1", "u3"], &["u1", "u3"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
    ];
    for (action, operation, user, changed, original) in cases {
        for refusal in [false, true] {
            let case = format!("{action} {}", if refusal { "refused" } else { "answered" });
            let rig = Rig::encrypted().await;
            rig.login_ios("A").await;
            let epoch = rig.database.session_epoch();
            let (values, args) = group("friends");
            let created = confirmed_group(&rig, epoch, values, args).await;
            seed_members(&rig, &created.id);
            let gate = if refusal { rig.world.hold_then(operation, Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next(operation) };
            let acting = { let database = rig.database.clone(); let id = created.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), json!({"userIds": [user]}).to_string()).await }) };
            gate.entered_for(&case).await;
            let receipt = bound(acting).await.unwrap().unwrap();
            assert_eq!((member_list(&rig, &created.id, "JOINED"), member_list(&rig, &created.id, "OWNER_FIRST")), (joined(changed[0]), joined(changed[1])), "{case}: the lists before the answer");
            gate.release();
            let result = bound(rig.database.outcome(receipt)).await.unwrap();
            settle(&rig).await;
            let expected = if refusal { original } else { changed };
            assert_eq!((member_list(&rig, &created.id, "JOINED"), member_list(&rig, &created.id, "OWNER_FIRST")), (joined(expected[0]), joined(expected[1])), "{case}: the lists after the outcome");
            assert_eq!(matches!(result, WireOperationOutcome::Rejected { .. }), refusal, "{case}: the operation returned {result:?}");
            rig.database.close_now();
        }
    }
}

/// K13 (B6, C05): leave removes the group from the list before the answer. The answer keeps it away, a refusal puts it back.
#[tokio::test(flavor = "multi_thread")]
async fn leave_removes_the_group_from_its_list_before_the_answer_and_a_refusal_restores_it() {
    for refusal in [false, true] {
        let case = if refusal { "refused" } else { "answered" };
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let created = confirmed_group(&rig, epoch, values, args).await;
        let reader = Reader::open();
        let stream = rig.database.subscribe_reading(epoch, groups(), reader.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
        yield_until("the list shows the group", || ids_of(reader.rows()) == strings(&[created.id.as_str()])).await;
        let gate = if refusal { rig.world.hold_then("LeaveGroup", Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next("LeaveGroup") };
        let acting = { let database = rig.database.clone(); let id = created.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, "leave".into(), "{}".into(), "{}".into()).await }) };
        gate.entered_for(case).await;
        yield_until("the group left the list before the answer", || ids_of(reader.rows()).is_empty()).await;
        gate.release();
        let receipt = bound(acting).await.unwrap().unwrap();
        let result = bound(rig.database.outcome(receipt)).await.unwrap();
        settle(&rig).await;
        assert_eq!(matches!(result, WireOperationOutcome::Rejected { .. }), refusal, "{case}: the operation returned {result:?}");
        if refusal {
            yield_until("the refusal put the group back", || ids_of(reader.rows()) == strings(&[created.id.as_str()])).await;
        } else {
            assert!(ids_of(reader.rows()).is_empty(), "{case}: the group came back after the answer");
        }
        rig.database.close_now();
    }
}

/// K13 (B6, C05): forward puts a local copy of the message into the thread of the target chat before the answer. The answer replaces the copy with its own row, a refusal removes the copy.
#[tokio::test(flavor = "multi_thread")]
async fn forward_shows_a_local_copy_in_the_target_thread_and_a_refusal_removes_it() {
    for refusal in [false, true] {
        let case = if refusal { "refused" } else { "answered" };
        let rig = Rig::encrypted().await;
        rig.world.seed_message("A", "m1", "hello", 1);
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let reader = Reader::open();
        let stream = rig.database.subscribe_reading(epoch, thread(), reader.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
        yield_until("the thread shows the source", || ids_of(reader.rows()) == strings(&["m1"])).await;
        let gate = if refusal { rig.world.hold_then("MessageForward", Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next("MessageForward") };
        let acting = { let database = rig.database.clone(); tokio::spawn(async move { database.act(epoch, "Message".into(), "m1".into(), "forward".into(), "{}".into(), json!({"targetChatId": "c1"}).to_string()).await }) };
        gate.entered_for(case).await;
        yield_until("the thread shows the copy before the answer", || ids_of(reader.rows()).len() == 2).await;
        let during = ids_of(reader.rows());
        assert!(during.contains(&"m1".to_string()), "{case}: the source left the thread: {during:?}");
        let copy = during.iter().find(|id| id.as_str() != "m1").unwrap();
        assert!(crate::identity::RowId::new(copy).unwrap().is_temp(), "{case}: the copy {copy} is not a temporary row");
        gate.release();
        let receipt = bound(acting).await.unwrap().unwrap();
        let result = bound(rig.database.outcome(receipt)).await.unwrap();
        settle(&rig).await;
        assert_eq!(matches!(result, WireOperationOutcome::Rejected { .. }), refusal, "{case}: the operation returned {result:?}");
        let expected = if refusal { strings(&["m1"]) } else { strings(&["m1", "forwarded-1"]) };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while ids_of(reader.rows()) != expected {
            assert!(std::time::Instant::now() < deadline, "{case}: expected {expected:?}, the thread shows {:?}", ids_of(reader.rows()));
            tokio::task::yield_now().await;
        }
        rig.database.close_now();
    }
}

type Call = for<'a> fn(&'a Database, u64) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DbError>> + 'a>>;

fn case(name: &'static str, call: Call) -> (&'static str, Call) { (name, call) }

/// K17 every entry of the database names the epoch of its caller and is refused after that session ended. Nothing reaches the server.
#[tokio::test(flavor = "multi_thread")]
async fn every_entry_of_a_left_account_is_refused_and_reaches_nothing() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let old = rig.database.session_epoch();
    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let before = rig.world.log().len();
    let calls = vec![
        case("run", |db, old| Box::pin(async move { bound(db.run(old, "MyProfile".into(), "{}".into())).await.map(drop) })),
        case("load", |db, old| Box::pin(async move { bound(db.load(old, "User".into(), "all".into(), "{}".into())).await })),
        case("refresh", |db, old| Box::pin(async move { bound(db.refresh(old, "User".into(), "all".into(), "{}".into())).await })),
        case("load_all", |db, old| Box::pin(async move { bound(db.load_all(old, "User".into(), "all".into(), "{}".into(), true)).await })),
        case("load_more", |db, old| Box::pin(async move { bound(db.load_more(old, "User".into(), "all".into(), "{}".into())).await.map(drop) })),
        case("node", |db, old| Box::pin(async move { bound(db.node(old, "User".into(), "node".into(), "user".into(), true)).await.map(drop) })),
        case("nodes", |db, old| Box::pin(async move { bound(db.nodes(old, "User".into(), "nodes".into(), vec!["user".into()])).await.map(drop) })),
        case("edge_values", |db, old| Box::pin(async move { bound(db.edge_values(old, "User".into(), "all".into(), "{}".into(), vec!["user".into()])).await.map(drop) })),
        case("peek_row", |db, old| Box::pin(async move { db.peek_row(old, "User".into(), "user".into()).map(drop) })),
        case("cancel_upload", |db, old| Box::pin(async move { bound(db.cancel_upload(old, "User".into(), "user".into(), "upload".into())).await.map(drop) })),
    ];
    for (name, call) in calls {
        let result = call(&rig.database, old).await;
        assert!(matches!(result, Err(DbError::Interrupted { .. } | DbError::NoSession { .. })), "{name} of a left account was not refused: {result:?}");
    }
    assert_eq!(rig.world.log().len(), before, "a refused entry reached the server");
    rig.database.close_now();
}

/// K18 (C04, old lines sync/send/flush.rs settle_delivery and online.rs set_online): an action that is not a durable command and whose request never reached the server ends with the network error and rolls back. Nothing replays it when the network returns.
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_action_that_never_reached_the_server_ends_with_the_network_error_and_is_not_replayed() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = confirmed_group(&rig, epoch, values, args).await;
    rig.world.fail_next("LeaveGroup", Fault::Down);
    let receipt = bound(rig.database.act(epoch, "Group".into(), created.id.clone(), "leave".into(), "{}".into(), "{}".into())).await.unwrap();
    let outcome = bound(rig.database.outcome(receipt)).await.unwrap();
    assert!(matches!(outcome, WireOperationOutcome::Rejected { failure } if failure.kind == "network"));
    let after = bound(rig.database.read_row(epoch, "Group".into(), created.id.clone())).await.unwrap();
    assert!(after.is_some(), "the group stayed removed after the failed request");
    assert!(crate::ffi::queue_view_tests::queued(&rig.database).is_empty(), "the failed action stayed queued");
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    settle(&rig).await;
    assert_eq!(rig.world.log().iter().filter(|entry| entry.operation == "LeaveGroup").count(), 1, "the action went out again after the network returned");
    rig.database.close_now();
}

/// K09 a list stream opened with refresh on connect loads again once when the network returns, without a call of the app. A stream without the flag asks nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_that_refreshes_on_connect_loads_again_once_when_the_network_returns() {
    let rig = Rig::encrypted().await;
    rig.world.seed_message("A", "m1", "first", 1);
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let list = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, ReadRequest::List { query_json: r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"},"refreshOnConnect":true}"#.into() }, list.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    settle(&rig).await;
    let before = rig.world.count("Thread");
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    yield_until("the list loaded again", || rig.world.count("Thread") > before).await;
    settle(&rig).await;
    assert_eq!(rig.world.count("Thread"), before + 1, "the stream loaded more or less than once on reconnect");
    rig.database.cancel_stream(stream);
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    for _ in 0..50 { tokio::task::yield_now().await; }
    settle(&rig).await;
    assert_eq!(rig.world.count("Thread"), before + 1, "a cancelled stream loaded on reconnect");
    rig.database.close_now();
}

/// K10 (out_sending_temp, out_sending_tick): a durable command whose request failed on the network is sent again by the backoff, the clock delivers it, and one pair reaches the server.
#[tokio::test(flavor = "multi_thread")]
async fn a_command_that_failed_on_the_network_is_delivered_by_the_backoff_and_the_clock() {
    for failures in [1, 2] {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        for _ in 0..failures { rig.world.fail_next("SendMessage", super::rig_tests::Fault::Down); }
        let receipt = bound(rig.database.send(message(epoch, "queued", 2))).await.unwrap();
        let done = Arc::new(AtomicBool::new(false));
        let waiting = { let database = rig.database.clone(); let done = done.clone(); tokio::spawn(async move { let outcome = database.outcome(receipt).await; done.store(true, Ordering::SeqCst); outcome }) };
        yield_until("the clock delivers the command", || { rig.clock.advance(3_600_000); done.load(Ordering::SeqCst) }).await;
        let outcome = bound(waiting).await.unwrap().unwrap();
        assert!(matches!(outcome, WireOperationOutcome::Confirmed { .. }), "{failures} failures: the command was not delivered: {outcome:?}");
        let sequences: std::collections::BTreeSet<_> = rig.world.log().iter().filter(|entry| entry.operation == "SendMessage").map(|entry| entry.sequence).collect();
        assert_eq!(sequences.len(), 1, "one pair reaches the server: {:?}", rig.world.log());
        rig.database.close_now();
    }
}

/// K23 (D01, old lines ffi/command.rs serve_in_epoch and sync/land.rs land_rows): a request that was accepted in the epoch of A, and whose answer waits at the server while another identity starts a session, lands nothing and ends refused. The epoch of A does not change, the generation does not either.
#[tokio::test(flavor = "multi_thread")]
async fn an_accepted_request_whose_answer_lands_after_the_next_account_started_writes_nothing() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    bound(rig.database.refresh(epoch, "User".into(), "profile".into(), "{}".into())).await.unwrap();
    let before = bound(rig.database.read_row(epoch, "User".into(), "user".into())).await.unwrap().expect("the row of A");
    assert!(named(&before, "A"));
    let gate = rig.world.hold_next("MyProfile");
    gate.as_account("late");
    let running = { let database = rig.database.clone(); tokio::spawn(async move { database.refresh(epoch, "User".into(), "profile".into(), "{}".into()).await }) };
    gate.entered_for("the accepted request").await;
    bound(rig.database.session_reset()).await.unwrap();
    assert_ne!(rig.database.session_epoch(), epoch);
    gate.release();
    let result = bound(running).await.unwrap();
    assert!(matches!(result, Err(DbError::Interrupted { .. } | DbError::NoSession { .. })), "the request of the epoch that left ended with {result:?}");
    settle(&rig).await;
    assert_eq!(rig.database.db.row_count("User").unwrap(), 0, "the answer of the epoch that left was written");
    rig.database.close_now();
}

/// Fable review 3 (old lines sync/send/flush.rs land_response): a create the server confirmed keeps its row when the cache of another model clears between the answer and its landing. The epoch stays, only the generation moves.
#[tokio::test(flavor = "multi_thread")]
async fn a_confirmed_create_keeps_its_row_when_another_model_resets_before_it_lands() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let gate = rig.world.hold_next("SendMessage");
    let receipt = bound(rig.database.send(message(epoch, "kept", 9))).await.unwrap();
    gate.entered_for("the send").await;
    let generation = rig.database.db.generation();
    bound(rig.database.reset_models_of(&["User"])).await;
    assert_ne!(rig.database.db.generation(), generation, "the reset of another model did not move the generation");
    assert_eq!(rig.database.session_epoch(), epoch, "the reset of another model ended the epoch");
    gate.release();
    let outcome = bound(rig.database.outcome(receipt)).await.unwrap();
    let WireOperationOutcome::Confirmed { value: Some(row), .. } = outcome else { panic!("the confirmed create ended with {outcome:?}") };
    settle(&rig).await;
    let found = rig.database.db.find("Message", &crate::identity::RowId::new(&row.id).unwrap()).unwrap();
    assert!(found.is_some(), "the row {} the server confirmed is gone", row.id);
    assert!(crate::ffi::queue_view_tests::queued(&rig.database).is_empty(), "the confirmed create is still queued");
    rig.database.close_now();
}

/// K25 (D04, old lines db/roll_back.rs restore and db/prior.rs reappearing): two operations change the same member of the same lists and both are refused. The lists end the way they were, in both orders of the two changes, the first refusal arriving while the second still holds its answer.
#[tokio::test(flavor = "multi_thread")]
async fn two_refused_changes_of_one_member_restore_the_original_lists_in_both_orders() {
    type Case = (&'static str, [(&'static str, &'static str); 2], &'static str, [&'static [&'static str]; 2], [&'static [&'static str]; 2]);
    let cases: [Case; 2] = [
        ("remove then add a member", [("remove_member", "RemoveMember"), ("add_member", "AddMember")], "u2", [&["u1", "u3", "u2"], &["u1", "u3", "u2"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
        ("add then remove a member", [("add_member", "AddMember"), ("remove_member", "RemoveMember")], "u4", [&["u1", "u2", "u3"], &["u2", "u1", "u3"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
    ];
    for (case, [first, second], user, while_second, original) in cases {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let created = confirmed_group(&rig, epoch, values, args).await;
        seed_members(&rig, &created.id);
        let (first_gate, second_gate) = (rig.world.hold_then(first.1, Fault::Refuse { code: "FORBIDDEN", confirmed: true }), rig.world.hold_then(second.1, Fault::Refuse { code: "FORBIDDEN", confirmed: true }));
        let args = json!({"userIds": [user]}).to_string();
        let start = |action: &'static str| { let database = rig.database.clone(); let (id, args) = (created.id.clone(), args.clone()); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), args).await }) };
        let first_call = start(first.0);
        first_gate.entered_for(case).await;
        let second_call = start(second.0);
        yield_until("the second change is queued", || crate::ffi::queue_view_tests::queued(&rig.database).len() == 2).await;
        first_gate.release();
        second_gate.entered_for(case).await;
        let lists = |rig: &Rig| (member_list(rig, &created.id, "JOINED"), member_list(rig, &created.id, "OWNER_FIRST"));
        let expected = |lists: [&[&str]; 2]| (strings(lists[0]), strings(lists[1]));
        assert_eq!(lists(&rig), expected(while_second), "{case}: the lists while the second change waits for its answer");
        second_gate.release();
        let first = bound(first_call).await.unwrap().unwrap();
        let second = bound(second_call).await.unwrap().unwrap();
        assert!(matches!(bound(rig.database.outcome(first)).await.unwrap(), WireOperationOutcome::Rejected { .. }));
        assert!(matches!(bound(rig.database.outcome(second)).await.unwrap(), WireOperationOutcome::Rejected { .. }));
        settle(&rig).await;
        assert_eq!(lists(&rig), expected(original), "{case}: the lists after both refusals");
        rig.database.close_now();
    }
}

/// K26 (E04 and the outcome of an accepted call, old lines db/local_write.rs join_scope_rows and leave_scope_rows, sync/send/mod.rs settled): the same change of a member made twice, once the first and once the second refused, leaves the member the way the accepted change wants, also while the second still holds its answer. The accepted call returns its success and the refused call its refusal.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_change_of_a_member_does_not_undo_the_same_change_accepted_and_each_call_gets_its_own_outcome() {
    type Case = (&'static str, (&'static str, &'static str), &'static str, [&'static [&'static str]; 2], bool);
    let cases: [Case; 4] = [
        ("add twice, first refused", ("add_member", "AddMember"), "u4", [&["u1", "u2", "u3", "u4"], &["u2", "u1", "u3", "u4"]], true),
        ("remove twice, first refused", ("remove_member", "RemoveMember"), "u2", [&["u1", "u3"], &["u1", "u3"]], true),
        ("add twice, second refused", ("add_member", "AddMember"), "u4", [&["u1", "u2", "u3", "u4"], &["u2", "u1", "u3", "u4"]], false),
        ("remove twice, second refused", ("remove_member", "RemoveMember"), "u2", [&["u1", "u3"], &["u1", "u3"]], false),
    ];
    for (case, (action, operation), user, expected, first_refused) in cases {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let created = confirmed_group(&rig, epoch, values, args).await;
        seed_members(&rig, &created.id);
        let held = |refused: bool| if refused { rig.world.hold_then(operation, Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next(operation) };
        let first_gate = held(first_refused);
        let second_gate = held(!first_refused);
        let args = json!({"userIds": [user]}).to_string();
        let start = || { let database = rig.database.clone(); let (id, args) = (created.id.clone(), args.clone()); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), args).await }) };
        let first_call = start();
        first_gate.entered_for(case).await;
        let second_call = start();
        yield_until("the second change is queued", || crate::ffi::queue_view_tests::queued(&rig.database).len() == 2).await;
        first_gate.release();
        second_gate.entered_for(case).await;
        let lists = |rig: &Rig| (member_list(rig, &created.id, "JOINED"), member_list(rig, &created.id, "OWNER_FIRST"));
        let wanted = (strings(expected[0]), strings(expected[1]));
        assert_eq!(lists(&rig), wanted, "{case}: the lists while the second change waits for its answer");
        second_gate.release();
        let (first, second) = (bound(first_call).await.unwrap().unwrap(), bound(second_call).await.unwrap().unwrap());
        let (first, second) = (bound(rig.database.outcome(first)).await.unwrap(), bound(rig.database.outcome(second)).await.unwrap());
        assert_eq!((matches!(first, WireOperationOutcome::Rejected { .. }), matches!(second, WireOperationOutcome::Rejected { .. })), (first_refused, !first_refused), "{case}: the outcomes differ");
        settle(&rig).await;
        assert_eq!(lists(&rig), wanted, "{case}: the lists after both answers");
        rig.database.close_now();
    }
}
