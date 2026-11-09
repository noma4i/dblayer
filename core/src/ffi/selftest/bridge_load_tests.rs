use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use serde_json::json;
use super::rig_tests::{bound, Fault, Rig, Switchable, World};
use crate::ffi::tests::lifecycle_tests::named;
use crate::ffi::tape::StreamDelivery;
use crate::ffi::{Database, DbError, EndCause, ReadFetch, ReadPhase, ReadRequest, ReadValue, WireOperationOutcome};
use crate::test_wait::settled;
use super::bridge_support_tests::*;

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_one_load_interest_keeps_the_other_call_alive() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let before = rig.world.count("Users");
    let held = rig.world.hold_next("Users");
    let idle = rig.database.life.waiting_calls();
    let first = rig.database.begin_load(epoch, "User".into(), Some("all".into()), "{}".into()).unwrap();
    held.entered().await;
    let second = rig.database.begin_load(epoch, "User".into(), Some("all".into()), "{}".into()).unwrap();
    settled(&rig, "both calls wait for the shared load", || rig.database.life.waiting_calls() >= idle + 2).await;
    let owners = rig.database.life.clone();
    tokio::task::spawn_blocking(move || owners.barrier()).await.unwrap();
    first.cancel();
    held.release();
    bound(second.result()).await.unwrap();
    assert_eq!(rig.world.count("Users") - before, 1, "{:?}", rig.world.log());
    let rows = bound(rig.database.read_query(epoch, crate::ffi::QuerySpec::json(r#"{"model":"User","relation":"all","params":{}}"#))).await.unwrap();
    assert_eq!(ids_of(rows), ["user"]);
    rig.database.close_now();
}

#[derive(Default)]
struct CancelledLoadTransport {
    started: tokio::sync::Notify,
    cancelled: Arc<tokio::sync::Notify>,
    ended: Arc<AtomicBool>,
}

struct CancelledLoad(Arc<tokio::sync::Notify>, Arc<AtomicBool>);

impl Drop for CancelledLoad {
    fn drop(&mut self) {
        self.1.store(true, Ordering::SeqCst);
        self.0.notify_one();
    }
}

#[async_trait::async_trait]
impl crate::ffi::FakeTransport for CancelledLoadTransport {
    async fn query(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> {
        let _cancelled = CancelledLoad(self.cancelled.clone(), self.ended.clone());
        self.started.notify_one();
        std::future::pending().await
    }
    async fn mutate(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<crate::ffi::ByteProgress>) -> Result<crate::ffi::PutReply, DbError> { unreachable!() }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_the_last_load_interest_cancels_the_request() {
    let transport = Arc::new(CancelledLoadTransport::default());
    let database = bound(Database::open(crate::ffi::Config { path: None, data_version: None }, crate::ffi::specs(crate::ffi::tests::lifecycle_tests::models()), crate::ffi::OpenPorts { transport: Some(transport.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() })).await.unwrap();
    database.register_operations(json!([{"name":"Users","document":"query Users { users { id name } }"}]).to_string()).await.unwrap();
    database.set_online(true).unwrap();
    let request = database.begin_load(database.session_epoch(), "User".into(), Some("all".into()), "{}".into()).unwrap();
    bound(transport.started.notified()).await;
    request.cancel();
    bound(transport.cancelled.notified()).await;
    database.close_now();
}

/// boundary_load_cancel_orders (D-138): the leave of the last registered interest waits in the owner for the calls that began before it, and goes on at their registration.
#[tokio::test(flavor = "multi_thread")]
async fn the_last_load_interest_leaves_through_the_owner_after_the_earlier_registrations() {
    let transport = Arc::new(CancelledLoadTransport::default());
    let database = bound(Database::open(crate::ffi::Config { path: None, data_version: None }, crate::ffi::specs(crate::ffi::tests::lifecycle_tests::models()), crate::ffi::OpenPorts { transport: Some(transport.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() })).await.unwrap();
    database.register_operations(json!([{"name":"Users","document":"query Users { users { id name } }"}]).to_string()).await.unwrap();
    database.set_online(true).unwrap();
    let request = database.begin_load(database.session_epoch(), "User".into(), Some("all".into()), "{}".into()).unwrap();
    bound(transport.started.notified()).await;
    let (_earlier, earlier_id) = database.life.reserve_load(database.session_epoch());
    request.cancel();
    let owners = database.life.clone();
    tokio::task::spawn_blocking(move || owners.barrier()).await.unwrap();
    assert!(!transport.ended.load(Ordering::SeqCst), "the window still has the interest until the earlier call registered");
    database.life.unreserve_load(earlier_id);
    bound(transport.cancelled.notified()).await;
    let owners = database.life.clone();
    tokio::task::spawn_blocking(move || owners.barrier()).await.unwrap();
    assert_eq!(database.life.pending(), 0);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_after_reopen_and_clock_rollback_keeps_the_new_response() {
    use super::rig_models::{plain_models, plain_operations};
    let directory = tempfile::tempdir().unwrap();
    let config = crate::ffi::Config { path: Some(directory.path().join("cache.sqlite").to_string_lossy().into_owned()), data_version: None };
    let opener = crate::ffi::OpenDatabase::new();
    let world_a = World::new("server", "A", true);
    let switch = Switchable::new(Some(world_a.clone()), false);
    let first = bound(opener.open_with_clock(config.clone(), crate::ffi::specs(plain_models()), crate::ffi::OpenPorts { transport: Some(switch.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, crate::ffi::TestClock::new(1_000_000), crate::ffi::OpenOptions::default(), None)).await.unwrap();
    first.register_operations(plain_operations()).await.unwrap();
    first.set_online(true).unwrap();
    bound(first.refresh(first.session_epoch(), "User".into(), Some("profile".into()), "{}".into())).await.unwrap();
    let query = r#"{"model":"User","relation":"profile","params":{}}"#.to_string();
    let rows = bound(first.read_query(first.session_epoch(), crate::ffi::QuerySpec::json(query.clone()))).await.unwrap();
    assert!(rows.iter().any(|row| named(row, "A")));
    bound(first.close_database()).await.unwrap();
    let world_b = World::new("server", "B", true);
    switch.set(Some(world_b.clone()));
    let second = bound(opener.open_with_clock(config, crate::ffi::specs(plain_models()), crate::ffi::OpenPorts { transport: Some(switch), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, crate::ffi::TestClock::new(100), crate::ffi::OpenOptions::default(), None)).await.unwrap();
    second.register_operations(plain_operations()).await.unwrap();
    second.set_online(true).unwrap();
    bound(second.refresh(second.session_epoch(), "User".into(), Some("profile".into()), "{}".into())).await.unwrap();
    let rows = bound(second.read_query(second.session_epoch(), crate::ffi::QuerySpec::json(query))).await.unwrap();
    assert!(rows.iter().any(|row| named(row, "B")));
    assert_eq!((world_a.count("MyProfile"), world_b.count("MyProfile")), (1, 1));
    second.close_now();
}

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
    bound(rig.database.refresh(epoch, "User".into(), Some("profile".into()), "{}".into())).await.unwrap();
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
        }).next_back().unwrap();
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
    assert_eq!(reader.ended(), vec![EndCause::SessionEnded]);
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
    gate.entered().await;
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
    let stream = rig.database.subscribe_reading(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(query) }, reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    let before = rig.world.count("GroupMembers");
    let older = rig.world.hold_next("GroupMembers");
    let response = rig.world.hold_next("RemoveMember");
    let reread = rig.world.hold_next("GroupMembers");
    let refresh = { let database = rig.database.clone(); rig.spawn(async move { database.refresh_stream(stream).await }) };
    older.entered().await;
    let action = { let database = rig.database.clone(); let id = created.id.clone(); rig.spawn(async move { database.act(epoch, "Group".into(), id, "remove_member".into(), "{}".into(), json!({"userIds":["u1"]}).to_string()).await }) };
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
    rig.clock.advance(299_999);
    bound(load()).await.unwrap();
    assert_eq!(requests(), 1, "a fresh window answers without a request");
    rig.clock.advance(1);
    bound(load()).await.unwrap();
    assert_eq!(requests(), 2, "an expired window asks again");
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
        rig.database.subscribe_reading(epoch, ReadRequest::Count { query: crate::ffi::QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#) }, count.clone()),
        rig.database.subscribe_reading(epoch, thread(), list.clone()),
    ];
    for stream in streams { bound(rig.database.loaded(stream)).await.unwrap(); }
    settled(&rig, "the list holds 5 rows", || list.rows().len() == 5).await;
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
    let waiting = rig.database.life.waiting_calls();
    let outcome = { let database = rig.database.clone(); let receipt = receipt.clone(); rig.spawn(async move { database.outcome(receipt).await }) };
    crate::test_wait::settled(&rig, "the outcome call waits for the command", || rig.database.life.waiting_calls() > waiting).await;
    crate::ffi::owners_drained(&rig.database).await.unwrap();
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
    let refresh = { let database = rig.database.clone(); rig.spawn(async move { database.refresh_stream(stream).await }) };
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
        ReadRequest::Count { query: crate::ffi::QuerySpec::json(r#"{"model":"Nope"}"#) },
    ];
    for request in requests {
        let reader = Reader::open();
        let stream = rig.database.subscribe_reading(epoch, request.clone(), reader.clone());
        settled(&rig, "the failed stream ended", || !reader.ended().is_empty()).await;
        assert!(matches!(reader.ended().as_slice(), [EndCause::Invalid { .. }]), "{request:?} ended with more or less than one outcome");
        settled(&rig, "the failed stream left the table", || rig.database.stream_census().open == 0).await;
        let loaded = bound(rig.database.loaded(stream)).await;
        assert!(matches!(loaded, Err(DbError::Invalid { .. })), "{request:?} reported a first load: {loaded:?}");
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
            crate::test_wait::recv_within(&self.release.lock().unwrap());
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
    settled(&rig, "the thread shows the first message", || blocking.inner.texts().iter().any(|text| text.contains("first"))).await;
    rig.database.set_online(false).unwrap();
    blocking.armed.store(true, Ordering::SeqCst);
    bound(rig.database.send(message(epoch, "second", 2))).await.unwrap();
    tokio::task::spawn_blocking(move || crate::test_wait::recv_within(&entered_rx)).await.unwrap();
    rig.database.cancel_stream(stream);
    assert!(!blocking.finished.load(Ordering::SeqCst), "the cancel waited for the reader to finish a delivery");
    release_tx.send(()).unwrap();
    settled(&rig, "the delivery in progress finished", || blocking.finished.load(Ordering::SeqCst)).await;
    let after_cancel = blocking.inner.len();
    bound(rig.database.send(message(epoch, "third", 3))).await.unwrap();
    rig.settle().await;
    assert_eq!(blocking.inner.len(), after_cancel, "a delivery began after the cancel returned");
    assert!(!blocking.inner.texts().iter().any(|text| text.contains("third")));
    rig.database.close_now();
}
