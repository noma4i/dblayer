use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::json;

use super::flow_fixtures::{add_chat, add_moment, add_profile, add_reactor_user, cell, land_event, texts};
use super::rig_tests::{bound, Fault, Rig, World};
use super::rig_models::add_reactions;
use crate::ffi::{Database, DbError, ReadFetch, ReadRequest, ReadStream, ReadValue, WireOperationOutcome};
use crate::ffi::wire::Cell;

pub(super) struct FeedTransport {
    cable: crate::transport::encrypted::cable::tests::peer_tests::CablePeer,
    pub(super) token: Arc<Mutex<Option<String>>>,
    opened: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::ffi::FakeTransport for FeedTransport {
    fn cable(&self) -> Option<&crate::transport::encrypted::cable::tests::peer_tests::CablePeer> { Some(&self.cable) }
    async fn query(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<crate::ffi::ByteProgress>) -> Result<crate::ffi::PutReply, DbError> { unreachable!() }
}

async fn feed_database() -> (Arc<Database>, Arc<FeedTransport>) {
    feed_database_with_listen(None).await
}

async fn feed_database_with_listen(listen: Option<serde_json::Value>) -> (Arc<Database>, Arc<FeedTransport>) {
    feed_database_with_models(listen, false, false).await
}

async fn feed_database_with_models(listen: Option<serde_json::Value>, fanout: bool, memory: bool) -> (Arc<Database>, Arc<FeedTransport>) {
    feed_database_in(World::new("server", "A", true), listen, fanout, memory).await
}

pub(super) async fn feed_database_in(world: Arc<World>, listen: Option<serde_json::Value>, fanout: bool, memory: bool) -> (Arc<Database>, Arc<FeedTransport>) {
    feed_database_changed_by(world, listen, fanout.then_some("chat"), memory, vec![json!({"area":"MESSAGE","relation":"node","payload":"chat","removes_on":["DELETED"]})]).await
}

async fn feed_database_changed_by(world: Arc<World>, listen: Option<serde_json::Value>, fanout: Option<&str>, memory: bool, changes: Vec<serde_json::Value>) -> (Arc<Database>, Arc<FeedTransport>) {
    use crate::schema::{Coverage, FieldSpec, Membership, ModelSpec, NodeRead, RelationSpec, RemoteShape, VariableSource, NODE_PARAM};
    use crate::value::ScalarKind;
    let mut revision = FieldSpec::scalar("serverRevision", ScalarKind::Int);
    revision.from = Some("revision".into());
    let mut chat = ModelSpec::new("Chat", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("title", ScalarKind::Str), revision, FieldSpec::list("lists", ScalarKind::Str)]);
    if memory { chat.storage = crate::schema::Storage::Memory; }
    chat.ephemeral.push(crate::schema::EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false))));
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
    let mut remote = crate::schema_fixtures::remote("ChatList", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes");
    remote.variables.insert("list".into(), VariableSource::Param { name: "list".into() });
    list.remote = Some(remote);
    chat.relations.extend([node, list]);
    chat.changes = changes.into_iter().map(|read| serde_json::from_value(read).unwrap()).collect();
    if let Some(listen) = listen { chat.listens.push(serde_json::from_value(listen).unwrap()); }
    chat.change_feed = Some(crate::schema::ChangeFeed { subscription: "RecordEvents".into(), root: "recordEvents.entries".into(), area: "topic".into(), kind: "action".into(), id: "recordId".into() });
    let models = if let Some(room_payload) = fanout {
        let mut room = chat.clone();
        room.key = crate::identity::ModelKey::new("Room");
        for read in &mut room.changes { read.payload = read.payload.as_ref().map(|_| room_payload.to_string()); }
        if room_payload != "chat" { room.server_revision = None; }
        vec![chat, room]
    } else { vec![chat] };
    let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
    encrypted.endpoints.session_operations.insert("login_link".into(), crate::ffi::EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: crate::ffi::EncryptedOperationClass::Authenticated });
    let (token, opened) = (Arc::new(Mutex::new(None)), Arc::new(tokio::sync::Notify::new()));
    let observed = (token.clone(), opened.clone());
    let cable = crate::transport::encrypted::cable::tests::peer_tests::CablePeer::observed(encrypted.clone(), move |event| {
        if let crate::transport::encrypted::cable::tests::peer_tests::PeerEvent::Subscribed(subscribed) = event { *observed.0.lock().unwrap() = Some(subscribed); observed.1.notify_one(); }
    });
    let transport = Arc::new(FeedTransport { cable, token, opened });
    let database = bound(Database::open_with_clock(crate::ffi::Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&models).unwrap()), crate::ffi::OpenPorts { transport: Some(transport.clone()), encrypted: Some(encrypted), credentials: Some(Arc::new(crate::ffi::tests::session_guards_tests::WatchedHeld::default())), http: Some(world), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, crate::ffi::TestClock::new(1_000_000))).await.unwrap();
    database.register_operations(json!([
        {"name":"ChatList","document":"query ChatList($list: String) { chats(list: $list) { id title revision lists } }"},
        {"name":"ChatNode","document":"query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title revision lists } } }"},
        {"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId chat { id title revision lists } } } }"}
    ]).to_string()).await.unwrap();
    database.set_online(true).unwrap();
    bound(database.session_call("login_link".into(), json!({"account":"A"}).to_string())).await.unwrap();
    (database, transport)
}

async fn feed_events() -> (Arc<Database>, Arc<FeedTransport>, Arc<ReadStream>, String) {
    let (database, transport) = feed_database().await;
    let stream = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Chat".into() });
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Ready)));
    let token = transport.token.lock().unwrap().clone().unwrap();
    (database, transport, stream, token)
}

async fn feed_listen_events(listen: serde_json::Value) -> (Arc<Database>, Arc<FeedTransport>, Arc<ReadStream>, String) {
    let (database, transport) = feed_database_with_listen(Some(listen)).await;
    let stream = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Chat".into() });
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Ready)));
    let token = transport.token.lock().unwrap().clone().unwrap();
    (database, transport, stream, token)
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_payload_lands() {
    let (database, transport, stream, token) = feed_events().await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"c1","chat":{"id":"c1","title":"after","revision":2,"lists":["PRIMARY"]}}]}}})));
    let value = bound(stream.pull()).await.unwrap();
    assert!(matches!(value, ReadValue::Event { position: 1, area, kind, id, before: None, after: Some(row) } if area == "MESSAGE" && kind == "UPDATED" && id == "c1" && crate::ffi::tests::lifecycle_tests::named(&row, "after")));
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_payload_incomplete() {
    let (database, transport, stream, token) = feed_events().await;
    assert_eq!(bound(database.subscription_census()).await.unwrap().refused_events, 0);
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"c1","chat":{"id":"c1"}}]}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Lost { first: 1, last: 1 })));
    assert_eq!(bound(database.subscription_census()).await.unwrap().refused_events, 1);
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_payload_removes() {
    let (database, transport, stream, token) = feed_events().await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"c1","chat":{"id":"c1","title":"before","revision":2,"lists":["PRIMARY"]}}]}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Event { after: Some(_), .. })));
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"DELETED","recordId":"c1"}]}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Event { id, before: Some(_), after: None, .. }) if id == "c1"));
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_changes_not_list() {
    let (database, transport, stream, token) = feed_events().await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":{}}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Lost { first: 1, last: 1 })));
    assert_eq!(bound(database.subscription_census()).await.unwrap().refused_events, 1);
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_changes_entry_incomplete() {
    let (database, transport, stream, token) = feed_events().await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","recordId":"c1"},{"topic":"MESSAGE","action":"UPDATED","recordId":"c2","chat":{"id":"c2","title":"after","revision":2,"lists":["PRIMARY"]}}]}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Event { position: 1, id, .. }) if id == "c2"));
    assert_eq!(bound(database.subscription_census()).await.unwrap().refused_events, 1);
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_changes_area_fanout() {
    let (database, transport) = feed_database_with_models(None, true, false).await;
    let chat = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Chat".into() });
    let room = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Room".into() });
    assert!(matches!(bound(chat.pull()).await, Some(ReadValue::Ready)));
    assert!(matches!(bound(room.pull()).await, Some(ReadValue::Ready)));
    let token = transport.token.lock().unwrap().clone().unwrap();
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"c1","chat":{"id":"c1","title":"after","revision":2,"lists":[]}}]}}})));
    for stream in [&chat, &room] {
        assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Event { position: 1, id, after: Some(row), .. }) if id == "c1" && crate::ffi::tests::lifecycle_tests::named(&row, "after")));
    }
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_event_without_id() {
    let (database, transport, stream, token) = feed_listen_events(json!({"subscription":"RecordEvents", "root":"recordEvents.item"})).await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"item":{"title":"missing","revision":1,"lists":[]}}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Lost { first: 1, last: 1 })));
    assert_eq!(bound(database.subscription_census()).await.unwrap().refused_events, 1);
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_composite_id() {
    let (database, transport, stream, token) = feed_listen_events(json!({"subscription":"RecordEvents", "root":"recordEvents.item", "id":["group", "entry"]})).await;
    for (revision, title) in [(1, "first"), (2, "second")] {
        assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"item":{"group":"g1","entry":"e1","title":title,"revision":revision,"lists":[]}}}})));
        assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Event { id, after: Some(row), .. }) if id == "g1:e1" && crate::ffi::tests::lifecycle_tests::named(&row, title)));
    }
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_composite_id_missing() {
    let (database, transport, stream, token) = feed_listen_events(json!({"subscription":"RecordEvents", "root":"recordEvents.item", "id":["group", "entry"]})).await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"item":{"group":"g1","title":"missing","revision":1,"lists":[]}}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Lost { first: 1, last: 1 })));
    assert_eq!(bound(database.subscription_census()).await.unwrap().refused_events, 1);
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_live_event_memory_no_id() {
    let listen = json!({"subscription":"RecordEvents", "root":"recordEvents.item"});
    let (database, transport) = feed_database_with_models(Some(listen), false, true).await;
    let stream = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Chat".into() });
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Ready)));
    let token = transport.token.lock().unwrap().clone().unwrap();
    let mut ids = Vec::new();
    for title in ["first", "second"] {
        assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"item":{"title":title,"revision":1,"lists":[]}}}})));
        let Some(ReadValue::Event { id, after: Some(row), .. }) = bound(stream.pull()).await else { panic!("the event was not delivered") };
        assert!(crate::ffi::tests::lifecycle_tests::named(&row, title));
        assert_eq!(uuid::Uuid::parse_str(&id).unwrap().get_version_num(), 4);
        ids.push(id);
    }
    assert_ne!(ids[0], ids[1]);
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_apply_values() {
    let listen = json!({"subscription":"RecordEvents", "root":"recordEvents.item", "set":{"typing":true}, "apply":{"effect":"upsert"}});
    let (database, transport, stream, token) = feed_listen_events(listen).await;
    let entity = database.subscribe_values(database.session_epoch(), ReadRequest::Entity { model: "Chat".into(), id: "c1".into(), fetch: ReadFetch::Never });
    assert!(matches!(bound(entity.pull()).await, Some(ReadValue::Snapshot { rows, .. }) if rows.is_empty()));
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"item":{"id":"c1","title":"after","revision":1,"lists":[]}}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Event { id, .. }) if id == "c1"));
    assert!(matches!(bound(entity.pull()).await, Some(ReadValue::Delta { edits, .. }) if matches!(crate::ffi::RowEdit::carried(&edits).as_slice(), [row] if row.cells.contains(&Cell::Bool { value: true }))));
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_apply_values_on_delete() {
    let listen = json!({"subscription":"RecordEvents", "root":"recordEvents.item", "set":{"typing":true}, "deleted_by":"deleted", "apply":{"effect":"upsert"}});
    let (database, transport, stream, token) = feed_listen_events(listen).await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"item":{"id":"c1","deleted":true}}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Event { id, after: None, .. }) if id == "c1"));
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"c1","chat":{"id":"c1","title":"after","revision":1,"lists":[]}}]}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Event { after: Some(_), .. })));
    let entity = database.subscribe_values(database.session_epoch(), ReadRequest::Entity { model: "Chat".into(), id: "c1".into(), fetch: ReadFetch::Never });
    assert!(matches!(bound(entity.pull()).await, Some(ReadValue::Snapshot { rows, .. }) if rows.len() == 1 && rows[0].cells.contains(&Cell::Bool { value: false })));
    bound(database.close_database()).await.unwrap();
}

async fn feed_conditional_delete(listen: serde_json::Value, field: &'static str, value: serde_json::Value) {
    let (database, transport, stream, token) = feed_listen_events(listen).await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"c1","chat":{"id":"c1","title":"before","revision":1,"lists":[]}}]}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Event { after: Some(_), .. })));
    let mut item = json!({"id":"c1"});
    item[field] = value;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"item":item}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Event { id, before: Some(_), after: None, .. }) if id == "c1"));
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_apply_delete_if() {
    feed_conditional_delete(json!({"subscription":"RecordEvents", "root":"recordEvents.item", "deleted_by":"gone", "apply":{"effect":"upsert"}}), "gone", json!(true)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_apply_delete_unless() {
    feed_conditional_delete(json!({"subscription":"RecordEvents", "root":"recordEvents.item", "deleted_unless":"active", "apply":{"effect":"upsert"}}), "active", json!(false)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_apply_ephemeral() {
    let listen = json!({"subscription":"RecordEvents", "root":"recordEvents.item", "apply":{"effect":"ephemeral", "fields":{"typing":"active"}}});
    let (database, transport, stream, token) = feed_listen_events(listen).await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"c1","chat":{"id":"c1","title":"before","revision":1,"lists":[]}}]}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Event { position: 1, .. })));
    let entity = database.subscribe_values(database.session_epoch(), ReadRequest::Entity { model: "Chat".into(), id: "c1".into(), fetch: ReadFetch::Never });
    assert!(matches!(bound(entity.pull()).await, Some(ReadValue::Snapshot { rows, .. }) if rows.len() == 1 && rows[0].cells.contains(&Cell::Bool { value: false })));
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"item":{"id":"c1","active":true}}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Event { position: 2, id, .. }) if id == "c1"));
    let Some(ReadValue::Delta { edits, .. }) = bound(entity.pull()).await else { panic!("the overlay did not reach the row") };
    let rows = crate::ffi::RowEdit::carried(&edits);
    assert!(rows.len() == 1 && rows[0].cells.contains(&Cell::Bool { value: true }) && crate::ffi::tests::lifecycle_tests::named(&rows[0], "before"), "the ephemeral value shows and the columns stay: {rows:?}");
    assert_eq!(bound(database.subscription_census()).await.unwrap().refused_events, 0);
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_apply_ephemeral_missing() {
    let listen = json!({"subscription":"RecordEvents", "root":"recordEvents.item", "apply":{"effect":"ephemeral", "fields":{"typing":"active"}}});
    let (database, transport, stream, token) = feed_listen_events(listen).await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"item":{"id":"c1"}}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Lost { first: 1, last: 1 })));
    assert_eq!(bound(database.subscription_census()).await.unwrap().refused_events, 1);
    bound(database.close_database()).await.unwrap();
}


async fn next_rows(rig: &Rig, stream: &ReadStream, expected: i64) -> i64 {
    let value = bound(stream.pull()).await.unwrap();
    let (revision, rows) = match value {
        ReadValue::Snapshot { revision, rows, .. } => (revision, rows),
        ReadValue::Delta { revision, edits, .. } => (revision, crate::ffi::RowEdit::carried(&edits)),
        other => panic!("unexpected value: {other:?}"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(cell(rig, "Moment", &rows[0], "visitsCount"), Cell::Int { value: expected });
    revision
}

async fn assert_quiet(db: &Database, streams: &[&Arc<ReadStream>]) {
    let mut pulls: Vec<_> = streams.iter().map(|stream| Box::pin(stream.pull())).collect();
    for pull in &mut pulls {
        assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(pull.as_mut(), cx).is_pending())).await);
    }
    let life = db.life_handle();
    bound(tokio::task::spawn_blocking(move || life.barrier())).await.unwrap();
    for pull in &mut pulls {
        assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(pull.as_mut(), cx).is_pending())).await);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_one_change_every_view_response_and_rejection() {
    let rig = Rig::encrypted_edited(false, false, false, |models| { add_chat(models); add_profile(models); add_moment(models); }).await;
    rig.login_ios("A").await;
    land_event(&rig, "Moment", json!({"id":"m1", "userId":"u1", "visitsCount":2, "reacted":false}));
    let db = &rig.database;
    let floor = db.db.revision_seq().unwrap();
    let epoch = db.session_epoch();
    let list = db.subscribe_values(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Moment"}"#) });
    let entity = db.subscribe_values(epoch, ReadRequest::Entity { model: "Moment".into(), id: "m1".into(), fetch: ReadFetch::Never });
    let count = db.subscribe_values(epoch, ReadRequest::Count { query: crate::ffi::QuerySpec::json(r#"{"model":"Moment"}"#) });
    let (first_list, first_entity) = (next_rows(&rig, &list, 2).await, next_rows(&rig, &entity, 2).await);
    assert!(first_list >= floor && first_entity >= floor, "the landing value is not older than its commit: {first_list} {first_entity} floor {floor}");
    assert!(matches!(bound(count.pull()).await, Some(ReadValue::Count { count: 1, .. })));
    let hold = rig.world.hold_then("MomentReact", Fault::Answer(json!({"data":{"momentReact":{"__typename":"Moment","id":"m1","visitsCount":4,"reacted":true}}})));
    let receipt = bound(db.act(epoch, "Moment".into(), "m1".into(), "react".into(), json!({"visitsCount":3,"reacted":true}).to_string(), "{}".into())).await.unwrap();
    hold.entered().await;
    let (local_list, local_entity) = (next_rows(&rig, &list, 3).await, next_rows(&rig, &entity, 3).await);
    assert!(local_list >= receipt.revision && local_entity >= receipt.revision, "the first value shows the optimistic row or the sending status that follows it: {local_list} {local_entity}");
    hold.release();
    assert!(matches!(bound(db.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    let (server_list, server_entity) = (next_rows(&rig, &list, 4).await, next_rows(&rig, &entity, 4).await);
    assert!(server_list > local_list && server_entity > local_entity, "the server value follows the optimistic one on each stream");
    let refused = rig.world.hold_then("MomentReact", Fault::Business);
    let receipt = bound(db.act(epoch, "Moment".into(), "m1".into(), "react".into(), json!({"visitsCount":5}).to_string(), "{}".into())).await.unwrap();
    refused.entered().await;
    let (draft_list, draft_entity) = (next_rows(&rig, &list, 5).await, next_rows(&rig, &entity, 5).await);
    assert!(draft_list >= receipt.revision && draft_entity >= receipt.revision, "the first value shows the optimistic row or the sending status that follows it: {draft_list} {draft_entity}");
    refused.release();
    assert!(matches!(bound(db.outcome(receipt)).await.unwrap(), WireOperationOutcome::Rejected { .. }));
    let (rollback_list, rollback_entity) = (next_rows(&rig, &list, 4).await, next_rows(&rig, &entity, 4).await);
    assert!(rollback_list > draft_list && rollback_entity > draft_entity, "the rollback follows the draft on each stream");
    assert_quiet(db, &[&list, &entity, &count]).await;
    assert_eq!(rollback_list, rollback_entity, "both streams end on one revision once settled (P-DLV-17)");
    db.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_one_change_every_view_feed() {
    let (db, transport) = feed_database().await;
    let epoch = db.session_epoch();
    bound(db.node(epoch, "Chat".into(), "node".into(), "c1".into(), true)).await.unwrap().unwrap();
    let events = db.subscribe_values(epoch, ReadRequest::Events { model: "Chat".into() });
    assert!(matches!(bound(events.pull()).await, Some(ReadValue::Ready)));
    let list = db.subscribe_values(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Chat"}"#) });
    let entity = db.subscribe_values(epoch, ReadRequest::Entity { model: "Chat".into(), id: "c1".into(), fetch: ReadFetch::Never });
    let count = db.subscribe_values(epoch, ReadRequest::Count { query: crate::ffi::QuerySpec::json(r#"{"model":"Chat"}"#) });
    for stream in [&list, &entity, &count] { bound(stream.pull()).await.unwrap(); }
    loop {
        let notified = transport.opened.notified();
        if transport.token.lock().unwrap().is_some() { break; }
        bound(notified).await;
    }
    let token = transport.token.lock().unwrap().clone().unwrap();
    let floor = db.db.revision_seq().unwrap();
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"m1","chat":{"id":"c1","title":"after","revision":2,"lists":["PRIMARY"]}}]}}})));
    let mut revisions = Vec::new();
    for stream in [&list, &entity] {
        let value = bound(stream.pull()).await.unwrap();
        let (revision, rows) = match value { ReadValue::Delta { revision, edits, .. } => (revision, crate::ffi::RowEdit::carried(&edits)), ReadValue::Snapshot { revision, rows, .. } => (revision, rows), other => panic!("{other:?}") };
        assert_eq!(rows.len(), 1);
        assert!(crate::ffi::tests::lifecycle_tests::named(&rows[0], "after"));
        revisions.push(revision);
    }
    assert!(revisions.iter().all(|revision| *revision > floor), "a stream showed the event before its commit: {revisions:?}, floor {floor}");
    assert_quiet(&db, &[&list, &entity, &count]).await;
    assert_eq!(revisions[0], revisions[1], "both streams end on one revision once settled (P-DLV-17)");
    db.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_one_change_every_view_related_row() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, |models| { add_chat(models); add_profile(models); add_reactions(models); add_reactor_user(models); }).await;
    land_event(&rig, "User", json!({"id":"u2","name":"Bob"}));
    land_event(&rig, "Message", json!({"id":"m1","chatId":"c1","text":"hello","at":1,"reactions":[{"body":"B","userId":"u2","user":{"name":"Old"}}]}));
    let db = &rig.database;
    let epoch = db.session_epoch();
    let list = db.subscribe_values(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Message"}"#) });
    let entity = db.subscribe_values(epoch, ReadRequest::Entity { model: "Message".into(), id: "m1".into(), fetch: ReadFetch::Never });
    let count = db.subscribe_values(epoch, ReadRequest::Count { query: crate::ffi::QuerySpec::json(r#"{"model":"Message"}"#) });
    for stream in [&list, &entity, &count] { bound(stream.pull()).await.unwrap(); }
    let hold = rig.world.hold_then("ProfileUpdate", Fault::Business);
    let receipt = bound(db.act(epoch, "User".into(), "u2".into(), "update".into(), json!({"name":"Robert"}).to_string(), json!({"name":"Robert"}).to_string())).await.unwrap();
    hold.entered().await;
    let mut revisions = Vec::new();
    for stream in [&list, &entity] {
        let value = bound(stream.pull()).await.unwrap();
        let (revision, rows) = match value { ReadValue::Delta { revision, edits, .. } => (revision, crate::ffi::RowEdit::carried(&edits)), ReadValue::Snapshot { revision, rows, .. } => (revision, rows), other => panic!("{other:?}") };
        assert_eq!(rows.len(), 1);
        assert!(texts(&cell(&rig, "Message", &rows[0], "reactions")).contains(&"Robert".to_string()));
        revisions.push(revision);
    }
    assert!(revisions.iter().all(|revision| *revision >= receipt.revision), "a stream showed the overlay before the receipt: {revisions:?}, receipt {}", receipt.revision);
    hold.release();
    assert!(matches!(bound(db.outcome(receipt)).await.unwrap(), WireOperationOutcome::Rejected { .. }));
    let mut restored = Vec::new();
    for stream in [&list, &entity] {
        let value = bound(stream.pull()).await.unwrap();
        let (revision, rows) = match value { ReadValue::Delta { revision, edits, .. } => (revision, crate::ffi::RowEdit::carried(&edits)), ReadValue::Snapshot { revision, rows, .. } => (revision, rows), other => panic!("{other:?}") };
        assert!(texts(&cell(&rig, "Message", &rows[0], "reactions")).contains(&"Bob".to_string()));
        restored.push(revision);
    }
    assert!(restored[0] > revisions[0] && restored[1] > revisions[1], "the restore follows the overlay on each stream");
    assert_quiet(db, &[&list, &entity, &count]).await;
    assert_eq!(restored[0], restored[1], "both streams end on one revision once settled (P-DLV-17)");
    db.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_live_overlay_position() {
    let listen = json!({"subscription":"RecordEvents", "root":"recordEvents.item", "apply":{"effect":"ephemeral", "fields":{"typing":"active"}}});
    let (database, transport, stream, token) = feed_listen_events(listen).await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"c1","chat":{"id":"c1","title":"t","revision":1,"lists":[]}}]}}})));
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Event { after: Some(_), .. })));
    let entity = database.subscribe_values(database.session_epoch(), ReadRequest::Entity { model: "Chat".into(), id: "c1".into(), fetch: ReadFetch::Never });
    let ReadValue::Snapshot { revision, rows, .. } = bound(entity.pull()).await.unwrap() else { panic!("the first value is a Snapshot") };
    assert!(rows[0].cells.contains(&Cell::Bool { value: false }));
    for (active, shown) in [(true, true), (false, false)] {
        assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"item":{"id":"c1","active":active}}}})));
        let value = loop {
            match bound(entity.pull()).await.unwrap() {
                ReadValue::Delta { revision: seen, edits, .. } => { assert_eq!(seen, revision, "an overlay-only batch keeps the revision"); break crate::ffi::RowEdit::carried(&edits); }
                ReadValue::State { .. } => {}
                other => panic!("an overlay-only batch is a Delta: {other:?}"),
            }
        };
        assert!(value[0].cells.contains(&Cell::Bool { value: shown }), "active={active}: {value:?}");
    }
    bound(database.close_database()).await.unwrap();
}

/// feed_prop_changed_by_reader_wants (P-FEED-29, D-145): a reader of a list of a model with `changed_by` and no `events` stream gets the subscription, and an event of the area rereads the list once.
#[tokio::test(flavor = "multi_thread")]
async fn feed_prop_changed_by_reader_wants() {
    let world = World::new("server", "A", true);
    let (database, transport) = feed_database_changed_by(world.clone(), None, None, false, vec![json!({"area":"CHAT","relation":"list","observed":true})]).await;
    let rows = Arc::new(crate::ffi::tape::Rows::default());
    let stream = database.watch_query(r#"{"model":"Chat","relation":"list","params":{"list":"PRIMARY"}}"#, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    bound(transport.opened.notified()).await;
    let token = transport.token.lock().unwrap().clone().unwrap();
    let before = world.count("ChatList");
    world.answer_with("ChatList", json!({"data":{"chats":[{"__typename":"Chat","id":"c1","title":"after","revision":2,"lists":["PRIMARY"]}]}}));
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"CHAT","action":"UPDATED","recordId":"c1"}]}}})));
    crate::test_wait::yield_until("the list shows the reread chat", || rows.len() == 1).await;
    assert_eq!(world.count("ChatList"), before + 1, "one request for one event");
    bound(database.close_database()).await.unwrap();
}

/// feed_prop_event_area_kind (P-FEED-28, D-146): the `events` stream gives the area and the kind of the record for each form of `changed_by`.
#[tokio::test(flavor = "multi_thread")]
async fn feed_prop_event_area_kind() {
    let changes = vec![
        json!({"area":"PAYLOAD","relation":"node","payload":"chat","removes_on":["GONE"]}),
        json!({"area":"NODE","relation":"node"}),
        json!({"area":"NAMED","relation":"list","parameter":"list"}),
        json!({"area":"OBSERVED","relation":"list","observed":true}),
    ];
    let (database, transport) = feed_database_changed_by(World::new("server", "A", true), None, None, false, changes).await;
    let stream = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Chat".into() });
    assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Ready)));
    let token = transport.token.lock().unwrap().clone().unwrap();
    let chat = json!({"id":"c1","title":"after","revision":2,"lists":[]});
    let forms = [("PAYLOAD", "UPDATED"), ("PAYLOAD", "GONE"), ("NODE", "CREATED"), ("NAMED", "UPDATED"), ("OBSERVED", "TOUCHED")];
    for (area, kind) in forms {
        assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":area,"action":kind,"recordId":"c1","chat":chat}]}}})));
        let Some(ReadValue::Event { area: seen_area, kind: seen_kind, id, .. }) = bound(stream.pull()).await else { panic!("no event for {area} {kind}") };
        assert_eq!((seen_area.as_str(), seen_kind.as_str(), id.as_str()), (area, kind, "c1"));
    }
    bound(database.close_database()).await.unwrap();
}

/// feed_payload_two_models_one_entry (D-151): one entry with two payload objects lands one row in each model that declares the area and gives each model one event.
#[tokio::test(flavor = "multi_thread")]
async fn feed_payload_two_models_one_entry() {
    let changes = vec![json!({"area":"MESSAGE","relation":"node","payload":"chat"})];
    let world = World::new("server", "A", true);
    let (database, transport) = feed_database_changed_by(world.clone(), None, Some("room"), false, changes).await;
    let chat = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Chat".into() });
    let room = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Room".into() });
    assert!(matches!(bound(chat.pull()).await, Some(ReadValue::Ready)));
    assert!(matches!(bound(room.pull()).await, Some(ReadValue::Ready)));
    let token = transport.token.lock().unwrap().clone().unwrap();
    let requests = world.count("ChatNode");
    let entry = json!({"topic":"MESSAGE","action":"CREATED","recordId":"m1","chat":{"__typename":"Chat","id":"1","title":"chat","revision":5,"lists":[]},"room":{"__typename":"Room","id":"r1","title":"room","revision":1,"lists":[]}});
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[entry]}}})));
    assert!(matches!(bound(chat.pull()).await, Some(ReadValue::Event { position: 1, area, kind, id, after: Some(row), .. }) if area == "MESSAGE" && kind == "CREATED" && id == "1" && crate::ffi::tests::lifecycle_tests::named(&row, "chat")));
    assert!(matches!(bound(room.pull()).await, Some(ReadValue::Event { position: 1, area, kind, id, after: Some(row), .. }) if area == "MESSAGE" && kind == "CREATED" && id == "r1" && crate::ffi::tests::lifecycle_tests::named(&row, "room")));
    for (model, id, title) in [("Chat", "1", "chat"), ("Room", "r1", "room")] {
        let entity = database.subscribe_values(database.session_epoch(), ReadRequest::Entity { model: model.into(), id: id.into(), fetch: ReadFetch::Never });
        assert!(matches!(bound(entity.pull()).await, Some(ReadValue::Snapshot { rows, .. }) if rows.len() == 1 && crate::ffi::tests::lifecycle_tests::named(&rows[0], title)), "{model} row");
    }
    assert_eq!(bound(database.subscription_census()).await.unwrap().refused_events, 0);
    assert_eq!(world.count("ChatNode"), requests);
    bound(database.close_database()).await.unwrap();
}
