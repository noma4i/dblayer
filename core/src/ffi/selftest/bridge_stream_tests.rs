use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use serde_json::json;
use super::rig_tests::{bound, Fault, Rig, World};
use crate::ffi::tests::lifecycle_tests::named;
use crate::ffi::{Database, DbError, ReadEnd, ReadRequest, ReadValue, WireOperationOutcome, WireSyncState};
use crate::test_wait::settled;
use super::bridge_support_tests::*;

fn chat_entry(index: usize) -> serde_json::Value {
    json!({"topic":"MESSAGE","action":"UPDATED","recordId":format!("m{index}"),"chat":{"id":format!("c{index}"),"title":"t","revision":1,"lists":["PRIMARY"]}})
}

/// The transport of a database with a session: the cable of the server is a peer that seals the frames.
struct CableTransport {
    cable: crate::transport::encrypted::cable::tests::peer_tests::CablePeer,
    token: Arc<Mutex<Option<String>>>,
    subscribed: Arc<tokio::sync::Notify>,
}

impl CableTransport {
    fn new(config: crate::transport::EncryptedConfig) -> Self {
        let (token, subscribed) = (Arc::new(Mutex::new(None)), Arc::new(tokio::sync::Notify::new()));
        let observed = (token.clone(), subscribed.clone());
        let cable = crate::transport::encrypted::cable::tests::peer_tests::CablePeer::observed(config, move |event| {
            if let crate::transport::encrypted::cable::tests::peer_tests::PeerEvent::Subscribed(opened) = event {
                *observed.0.lock().unwrap() = Some(opened);
                observed.1.notify_one();
            }
        });
        Self { cable, token, subscribed }
    }

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
impl crate::ffi::FakeTransport for CableTransport {
    fn cable(&self) -> Option<&crate::transport::encrypted::cable::tests::peer_tests::CablePeer> { Some(&self.cable) }
    async fn query(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<crate::ffi::ByteProgress>) -> Result<crate::ffi::PutReply, DbError> { unreachable!() }
}

/// S209: a reader of events that pulls nothing while 400 events arrive reads the kept events and the lost ranges, which add up to the 400 sent, then one end after the database closed.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_events_reader_reads_the_kept_events_and_one_exact_lost_range_then_one_end() {
    let (database, transport, _) = chat_events().await;
    let stream = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Chat".into() });
    let opened = bound(stream.pull_next()).await;
    assert!(matches!(opened, Some(ReadValue::Ready)), "the first value is the open source: {opened:?}");

    let watcher = Reader::open();
    database.subscribe_reading(database.session_epoch(), ReadRequest::Events { model: "Chat".into() }, watcher.clone());
    transport.subscription().await;
    transport.emit(json!({"data":{"recordEvents":{"entries":(0..400).map(chat_entry).collect::<Vec<_>>()}}}));
    let seen = |reader: &Reader| reader.deliveries().iter().map(|delivery| match &delivery.value { ReadValue::Event { .. } => 1, ReadValue::Lost { first, last } => last - first + 1, _ => 0 }).sum::<u64>();
    settled(&database, "the events reached a reading stream", || seen(&watcher) == 400).await;
    let mut burst = Vec::new();
    while seen_values(&burst) < 400 { burst.push(bound(stream.pull_next()).await.expect("the stream is open")); }
    let kept = burst.iter().filter(|value| matches!(value, ReadValue::Event { .. })).count() as u64;
    let lost: u64 = burst.iter().map(|value| match value { ReadValue::Lost { first, last } => last - first + 1, _ => 0 }).sum();
    assert_eq!(kept + lost, 400, "kept events and lost positions add up to the events sent");
    assert!(lost > 0, "a reader that pulled nothing lost the oldest events");
    database.close_now();
    assert!(matches!(bound(stream.pull_next()).await, Some(ReadValue::Ended { cause: ReadEnd::Closed })), "the stream ends once at the close");
    assert!(bound(stream.pull_next()).await.is_none());
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
    let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
    encrypted.endpoints.session_operations.insert("login_link".into(), crate::ffi::EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: crate::transport::EncryptedOperationClass::Authenticated });
    let transport = Arc::new(CableTransport::new(encrypted.clone()));
    let database = bound(Database::open_with_clock(crate::ffi::Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![chat]).unwrap()), crate::ffi::OpenPorts { transport: Some(transport.clone()), encrypted: Some(encrypted), credentials: Some(Arc::new(crate::ffi::tests::session_guards_tests::WatchedHeld::default())), http: Some(world.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, crate::ffi::TestClock::new(1_000_000))).await.unwrap();
    database.register_operations(json!([
        {"name":"ChatList","document":"query ChatList($list: String) { chats(list: $list) { id title revision lists } }"},
        {"name":"ChatNode","document":"query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title revision lists } } }"},
        {"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId chat { id title revision lists } } } }"}
    ]).to_string()).await.unwrap();
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
    settled(&rig, "the open list is read once more", || rig.world.count("Users") > before).await;
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
    settled(&database, "the events stream is open", || reader.deliveries().iter().any(|delivery| matches!(&delivery.value, ReadValue::Ready))).await;
    let params = |list: &str| json!({"list":list}).to_string();
    for list in ["PRIMARY", "SECONDARY"] { bound(database.load(epoch, "Chat".into(), "list".into(), params(list))).await.unwrap(); }
    let before = bound(database.node(epoch, "Chat".into(), "node".into(), "c1".into(), true)).await.unwrap().unwrap();
    assert!(named(&before, "before"));
    let query = |list: &str| json!({"model":"Chat","relation":"list","params":{"list":list}}).to_string();
    assert_eq!(ids_of(bound(database.read_query(epoch, crate::ffi::QuerySpec::json(query("SECONDARY")))).await.unwrap()), ["c1"]);
    transport.subscription().await;
    transport.emit(json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"m1","chat":{"id":"c1","title":"after","revision":2,"lists":["PRIMARY"]}}]}}}));
    crate::test_wait::settled_showing(&database, "no event arrived: ", || format!("{:?}", reader.deliveries().iter().map(|delivery| format!("{:?}", delivery.value)).collect::<Vec<_>>()), || reader.deliveries().iter().any(|delivery| matches!(&delivery.value, ReadValue::Event { .. }))).await;
    let (id, before_row, after_row) = reader.deliveries().into_iter().find_map(|delivery| match delivery.value { ReadValue::Event { id, before, after, .. } => Some((id, before, after)), _ => None }).unwrap();
    assert_eq!(id, "c1");
    assert!(named(&before_row.expect("the event carries the row before"), "before"));
    assert!(named(&after_row.expect("the event carries the committed row"), "after"));
    let primary = bound(database.read_query(epoch, crate::ffi::QuerySpec::json(query("PRIMARY")))).await.unwrap();
    assert_eq!(ids_of(primary.clone()), ["c1"]);
    assert!(named(&primary[0], "after"));
    assert!(bound(database.read_query(epoch, crate::ffi::QuerySpec::json(query("SECONDARY")))).await.unwrap().is_empty());
    let calls = world.log().into_iter().map(|entry| entry.operation).collect::<Vec<_>>();
    assert_eq!(calls, ["login_link", "ChatList", "ChatList", "ChatNode"]);
    database.close_now();
}

/// P-FEED-14: an event that creates a row puts it into the window with `joins_events` in the same batch, into no other window, and no window is read again.
#[tokio::test(flavor = "multi_thread")]
async fn feed_event_insert_on() {
    let (database, transport, world) = chat_events().await;
    let epoch = database.session_epoch();
    let events = database.subscribe_values(epoch, ReadRequest::Events { model: "Chat".into() });
    assert!(matches!(bound(events.pull_next()).await, Some(ReadValue::Ready)));
    for list in ["PRIMARY", "SECONDARY"] { bound(database.load(epoch, "Chat".into(), "list".into(), json!({"list": list}).to_string())).await.unwrap(); }
    let query = |list: &str| json!({"model":"Chat","relation":"list","params":{"list":list}}).to_string();
    let before = (ids_of(bound(database.read_query(epoch, crate::ffi::QuerySpec::json(query("PRIMARY")))).await.unwrap()), ids_of(bound(database.read_query(epoch, crate::ffi::QuerySpec::json(query("SECONDARY")))).await.unwrap()));
    let reads = world.count("ChatList");
    transport.subscription().await;
    transport.emit(json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"m9","chat":{"id":"c9","title":"new","revision":1,"lists":["PRIMARY"]}}]}}}));
    loop {
        if matches!(bound(events.pull_next()).await, Some(ReadValue::Event { id, .. }) if id == "c9") { break; }
    }
    let (primary, secondary) = (ids_of(bound(database.read_query(epoch, crate::ffi::QuerySpec::json(query("PRIMARY")))).await.unwrap()), ids_of(bound(database.read_query(epoch, crate::ffi::QuerySpec::json(query("SECONDARY")))).await.unwrap()));
    assert!(primary.contains(&"c9".to_string()) && primary.len() == before.0.len() + 1, "the window of its list takes the row: {primary:?}");
    assert_eq!(secondary, before.1, "another list does not");
    assert_eq!(world.count("ChatList"), reads, "no window is read again");
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
    settled(&rig, "the stream shows A", || reader.shows("A")).await;
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
    settled(&rig, "both streams show A", || profile_log.shows("A") && users_log.shows("A")).await;

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
    settled(&rig, "A shows", || first_profile.shows("A") && first_users.shows("A")).await;

    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let epoch_b = rig.database.session_epoch();
    assert_ne!(epoch_a, epoch_b, "the switch moved the epoch");
    let (second_profile, second_users) = (Reader::open(), Reader::open());
    for (request, log) in [(profile(), &second_profile), (users(), &second_users)] {
        let stream = rig.database.subscribe_reading(epoch_b, request, log.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
    }
    settled(&rig, "B shows", || second_profile.shows("B") && second_users.shows("B")).await;
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
    settled(&rig, "the thread shows the first message", || reader.texts().iter().any(|text| text.contains("first"))).await;

    rig.database.set_online(false).unwrap();
    let receipt = bound(rig.database.send(message(epoch, "second", 2))).await.unwrap();
    settled(&rig, "the thread shows the queued message", || reader.rows().len() == 2).await;
    let queued = reader.rows();
    assert_eq!(queued[1].id, receipt.optimistic_value.as_ref().unwrap().id, "the new message sits after the first");
    assert_eq!(queued[1].sync.state, WireSyncState::Pending);
    assert_eq!(receipt.optimistic_value.as_ref().unwrap().sync.state, WireSyncState::Pending);
    assert_eq!(rig.world.count("SendMessage"), 0, "the offline send reached the server");

    rig.database.set_online(true).unwrap();
    let outcome = bound(rig.database.outcome(receipt.clone())).await.unwrap();
    assert!(matches!(outcome, WireOperationOutcome::Confirmed { value: Some(row), .. } if row.id == "m2"));
    settled(&rig, "the thread shows the confirmed message", || reader.rows().get(1).is_some_and(|row| row.id == "m2")).await;
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
    settled(&rig, "A shows", || first.shows("A")).await;

    let gate = rig.world.hold_next("Users").as_account("A");
    let late = { let database = rig.database.clone(); rig.spawn(async move { database.refresh_stream(stream).await }) };
    gate.entered().await;
    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let epoch_b = rig.database.session_epoch();
    let second = Reader::open();
    let next = rig.database.subscribe_reading(epoch_b, users(), second.clone());
    bound(rig.database.loaded(next)).await.unwrap();
    settled(&rig, "B shows", || second.shows("B")).await;
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
    settled(&rig, "A shows", || reader.shows("A")).await;
    assert_eq!(rig.database.stream_census().open, 1);

    rig.database.cancel_stream(stream);

    settled(&rig, "the stream left the census", || rig.database.stream_census().open == 0).await;
    assert_eq!(rig.database.subscription_census().await.unwrap().subscriptions, 0);
    assert!(reader.ended().is_empty(), "a cancelled stream delivers no outcome");
    let mark = reader.len();
    rig.world.set_account("A2");
    bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), Some("all".into()), "{}".into())).await.unwrap();
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
    settled(&rig, "the thread holds 5 rows", || reader.rows().len() == 5).await;
    settle(&rig).await;
    assert_eq!(rig.database.stream_census().open, 2);

    let idle = (rig.world.hits(), rig.database.db.counters().observer_calls, reader.len(), other.len());
    rig.clock.advance(3_600_000);
    settle(&rig).await;
    assert_eq!(idle, (rig.world.hits(), rig.database.db.counters().observer_calls, reader.len(), other.len()), "an idle stream did work");

    let mut costs = Vec::new();
    for (index, text) in ["six", "seven"].into_iter().enumerate() {
        let (before, mark) = (rig.database.db.counters().rows_delivered, reader.len());
        let receipt = bound(rig.database.send(message(epoch, text, 6 + index as i64))).await.unwrap();
        bound(rig.database.outcome(receipt)).await.unwrap();
        settle(&rig).await;
        let delta_rows: Vec<usize> = reader.deliveries().iter().skip(mark).filter_map(|delivery| if let ReadValue::Delta { edits, .. } = &delivery.value { Some(crate::ffi::RowEdit::carried(edits).len()) } else { None }).collect();
        assert!(!delta_rows.is_empty() && delta_rows.iter().all(|rows| *rows == 1), "a change of one row carried {delta_rows:?} rows");
        costs.push((delta_rows.len(), rig.database.db.counters().rows_delivered - before));
    }
    assert_eq!(costs[0].1 * costs[1].0, costs[1].1 * costs[0].0, "the rows per delta grew with the list: {costs:?}");
    assert!(costs[1].1 <= 2 * costs[1].0, "the core delivered more rows than the deltas carry: {costs:?}");
    rig.database.close_now();
}
