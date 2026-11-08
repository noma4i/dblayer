use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use serde_json::json;
use super::rig_tests::{bound, Rig};
use crate::ffi::tests::lifecycle_tests::named;
use crate::ffi::tape::StreamDelivery;
use crate::ffi::{DbError, ReadRequest, ReadValue, WireOperationOutcome, WireSyncState};
use crate::test_wait::{within, yield_until};
use super::bridge_support_tests::*;

/// K02 (B2, old lines ffi/database.rs update, destroy, read_query, read_row): a command or a one-off read that carries the epoch of A is refused after the session ended, also when it waited for the writer while the session ended, and it changes nothing for B.
#[tokio::test(flavor = "multi_thread")]
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
    assert!(refused(bound(rig.database.read_query(old, crate::ffi::QuerySpec::json(users))).await.map(|_| ())), "a read of A returned rows in the epoch of B");
    assert!(refused(bound(rig.database.read_row(old, "User".into(), "user".into())).await.map(|_| ())));
    assert!(refused(bound(rig.database.update(old, "User".into(), "user".into(), json!({"name":"hijacked"}).to_string(), "{}".into())).await.map(|_| ())));
    assert!(refused(bound(rig.database.destroy(old, "User".into(), "user".into())).await.map(|_| ())));
    assert!(refused(bound(rig.database.set_local(old, "User".into(), "user".into(), "name".into(), "\"x\"".into())).await));
    assert!(refused(bound(rig.database.upload(old, "User".into(), "user".into(), "n".into(), crate::ffi::upload_wire::WireUploadRequest { path: "/nowhere".into(), name: None, mime: None, variables_json: "{}".into() })).await.map(|_| ())));
    let seen = bound(rig.database.read_row(current, "User".into(), "user".into())).await.unwrap().expect("the row of B");
    assert!(named(&seen, "B"), "the row of B changed");

    rig.database.close_now();
}

/// K03 a send needs a session, and a plain action written while the network is away is queued with its optimistic row and goes out once when the network returns.
#[tokio::test(flavor = "multi_thread")]
async fn a_send_needs_a_session_and_a_plain_action_waits_for_the_network() {
    let rig = Rig::encrypted().await;
    let epoch = rig.database.session_epoch();
    let sent = bound(rig.database.send(message(epoch, "anonymous", 1))).await;
    assert!(matches!(sent, Err(DbError::NoSession { .. })), "a send without a session was accepted: {sent:?}");
    assert!(crate::ffi::tests::queue_view_tests::queued(&rig.database).is_empty(), "a send without a session left an operation");
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("g");
    rig.database.set_online(false).unwrap();
    settle(&rig).await;
    let plain = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    assert_eq!(plain.optimistic_value.as_ref().unwrap().sync.state, WireSyncState::Pending);
    settle(&rig).await;
    assert_eq!(crate::ffi::tests::queue_view_tests::queued(&rig.database).len(), 1, "a plain action was not queued while the network was away");
    assert_eq!(rig.world.count("CreateGroup"), 0, "a plain action went out without the network");
    rig.database.set_online(true).unwrap();
    assert!(matches!(bound(rig.database.outcome(plain)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    assert_eq!(rig.world.count("CreateGroup"), 1, "a plain action went out more or less than once after the network returned");
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
    let act = bound(database.act(epoch, "Group".into(), "g1".into(), "add_member".into(), "{}".into(), json!({"userIds":["u9"]}).to_string())).await;
    assert!(matches!(send, Err(DbError::NoSession { .. })), "send: {send:?}");
    assert!(matches!(act, Err(DbError::NoSession { .. })), "act: {act:?}");
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

/// K04 (B4, old line ffi/database.rs create): a create the server answers goes through `create` with the epoch of the caller and returns the row the server confirmed.
#[tokio::test(flavor = "multi_thread")]
async fn a_server_create_returns_the_confirmed_row() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let row = confirmed_group(&rig, epoch, values.clone(), args.clone()).await;
    assert!(!row.id.starts_with("tmp") && !row.id.contains("temp"), "the created row still has a temporary id: {}", row.id);
    assert!(named(&row, "friends"));
    assert_eq!(rig.world.count("CreateGroup"), 1);
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

/// K06 (B6): the row of a command shows in the stream of its reader as pending before the server answers, and the server has not been asked.
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
    assert!(matches!(refreshed, Err(DbError::Offline { .. })), "refresh of a stream offline: {refreshed:?}");
    let refreshed = bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), Some("all".into()), "{}".into())).await;
    assert!(matches!(refreshed, Err(DbError::Offline { .. })), "refresh offline: {refreshed:?}");
    let more = bound(rig.database.load_more(rig.database.session_epoch(), "User".into(), Some("all".into()), "{}".into())).await;
    assert!(matches!(more, Err(DbError::Offline { .. })), "load more offline: {more:?}");
    yield_until("the entity stream shows the error", || entity.deliveries().iter().any(|delivery| match &delivery.value {
        ReadValue::State { state, .. } | ReadValue::Snapshot { state, .. } => state.error.is_some(),
        _ => false,
    })).await;
    assert_eq!(rig.world.log().len(), hits, "a call offline reached the server");
    rig.database.close_now();
}

/// D-101: after `open` the transport makes no attempt and waits for no network: a `run` offline fails at once with `Network` and the server sees nothing, and the first `run` online goes out once.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_offline_after_open_fails_at_once_and_the_transport_makes_no_attempt() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let hits = rig.world.log().len();
    rig.database.set_online(false).unwrap();
    let run = bound(rig.database.run(epoch, "SessionProfile".into(), "{}".into())).await;
    assert!(matches!(run, Err(DbError::Offline { .. })), "run offline: {run:?}");
    assert_eq!(rig.world.log().len(), hits, "a run offline reached the server");
    rig.database.set_online(true).unwrap();
    bound(rig.database.run(epoch, "SessionProfile".into(), "{}".into())).await.unwrap();
    assert_eq!(rig.world.log().len(), hits + 1, "the run online made another number of attempts than 1");
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
