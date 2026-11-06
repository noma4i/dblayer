use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::json;

use super::flow_fixtures::{add_chat, add_moment, add_profile, add_reactor_user, cell, land_event, texts};
use super::rig_tests::{add_reactions, bound, Fault, Rig, World};
use crate::ffi::{Database, DbError, ReadFetch, ReadRequest, ReadStream, ReadValue, WireOperationOutcome};
use crate::ffi::wire::Cell;

struct FeedTransport {
    cable: crate::transport::encrypted::cable::peer_tests::CablePeer,
    token: Mutex<Option<String>>,
    opened: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl crate::ffi::Transport for FeedTransport {
    async fn open_cable(&self, lease: Arc<crate::ffi::CableLease>, _: String) -> Result<(), DbError> { self.cable.open(&lease).map(drop) }
    async fn cable_send(&self, token: String, text: String) -> Result<(), DbError> {
        if self.cable.receive(&token, &text)?.is_some() { *self.token.lock().unwrap() = Some(token); self.opened.notify_one(); }
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

async fn feed_database() -> (Arc<Database>, Arc<FeedTransport>) {
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
    let mut remote = crate::schema_fixtures::remote("ChatList", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes");
    remote.variables.insert("list".into(), VariableSource::Param { name: "list".into() });
    list.remote = Some(remote);
    chat.relations.extend([node, list]);
    chat.changes.push(serde_json::from_value(json!({"area":"MESSAGE","relation":"node","payload":"chat","removes_on":["DELETED"]})).unwrap());
    chat.change_feed = Some(crate::schema::ChangeFeed { subscription: "RecordEvents".into(), root: "recordEvents.entries".into(), area: "topic".into(), kind: "action".into(), id: "recordId".into() });
    let world = World::new("server", "A", true);
    let mut encrypted = crate::transport::encrypted::protocol_tests::config();
    encrypted.endpoints.session_operations.insert("login_link".into(), crate::ffi::EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: crate::ffi::EncryptedOperationClass::Authenticated });
    let transport = Arc::new(FeedTransport { cable: crate::transport::encrypted::cable::peer_tests::CablePeer::new(encrypted.clone()), token: Mutex::new(None), opened: tokio::sync::Notify::new() });
    let database = bound(Database::open_with_clock(crate::ffi::Config { path: None, data_version: None, cache_rows: 32 }, serde_json::to_string(&vec![chat]).unwrap(), Some(transport.clone()), crate::ffi::TestClock::new(1_000_000), Some(encrypted), Some(Arc::new(crate::ffi::session_guards_tests::WatchedHeld::default())), Some(world), None)).await.unwrap();
    database.register_operations(json!([
        {"name":"ChatList","document":"query ChatList($list: String) { chats(list: $list) { id title revision lists } }"},
        {"name":"ChatNode","document":"query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title revision lists } } }"},
        {"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId chat { id title revision lists } } } }"}
    ]).to_string()).unwrap();
    database.set_online(true).unwrap();
    bound(database.session_call("login_link".into(), json!({"account":"A"}).to_string())).await.unwrap();
    (database, transport)
}

async fn feed_events() -> (Arc<Database>, Arc<FeedTransport>, Arc<ReadStream>, String) {
    let (database, transport) = feed_database().await;
    let stream = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Chat".into() });
    assert!(matches!(bound(stream.pull()).await.unwrap(), Some(ReadValue::Ready)));
    let token = transport.token.lock().unwrap().clone().unwrap();
    (database, transport, stream, token)
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_payload_lands() {
    let (database, transport, stream, token) = feed_events().await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"c1","chat":{"id":"c1","title":"after","revision":2,"lists":["PRIMARY"]}}]}}})));
    let value = bound(stream.pull()).await.unwrap().unwrap();
    assert!(matches!(value, ReadValue::Event { position: 1, area, kind, id, before: None, after: Some(row) } if area == "MESSAGE" && kind == "UPDATED" && id == "c1" && crate::ffi::lifecycle_tests::named(&row, "after")));
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_payload_incomplete() {
    let (database, transport, stream, token) = feed_events().await;
    assert_eq!(bound(database.subscription_census()).await.unwrap().refused_events, 0);
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"c1","chat":{"id":"c1"}}]}}})));
    assert!(matches!(bound(stream.pull()).await.unwrap(), Some(ReadValue::Lost { first: 1, last: 1 })));
    assert_eq!(bound(database.subscription_census()).await.unwrap().refused_events, 1);
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_payload_removes() {
    let (database, transport, stream, token) = feed_events().await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"c1","chat":{"id":"c1","title":"before","revision":2,"lists":["PRIMARY"]}}]}}})));
    assert!(matches!(bound(stream.pull()).await.unwrap(), Some(ReadValue::Event { after: Some(_), .. })));
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"DELETED","recordId":"c1"}]}}})));
    assert!(matches!(bound(stream.pull()).await.unwrap(), Some(ReadValue::Event { id, before: Some(_), after: None, .. }) if id == "c1"));
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_changes_not_list() {
    let (database, transport, stream, token) = feed_events().await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":{}}}})));
    assert!(matches!(bound(stream.pull()).await.unwrap(), Some(ReadValue::Lost { first: 1, last: 1 })));
    assert_eq!(bound(database.subscription_census()).await.unwrap().refused_events, 1);
    bound(database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_changes_entry_incomplete() {
    let (database, transport, stream, token) = feed_events().await;
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","recordId":"c1"},{"topic":"MESSAGE","action":"UPDATED","recordId":"c2","chat":{"id":"c2","title":"after","revision":2,"lists":["PRIMARY"]}}]}}})));
    assert!(matches!(bound(stream.pull()).await.unwrap(), Some(ReadValue::Event { position: 1, id, .. }) if id == "c2"));
    assert_eq!(bound(database.subscription_census()).await.unwrap().refused_events, 1);
    bound(database.close_database()).await.unwrap();
}

async fn next_rows(rig: &Rig, stream: &ReadStream, expected: i64) -> i64 {
    let value = bound(stream.pull()).await.unwrap().unwrap();
    let (revision, rows) = match value {
        ReadValue::Snapshot { revision, rows, .. } | ReadValue::Delta { revision, rows, .. } => (revision, rows),
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
    assert!(bound(tokio::task::spawn_blocking(move || life.barrier(std::time::Duration::from_secs(5)))).await.unwrap());
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
    let epoch = db.session_epoch();
    let list = db.subscribe_values(epoch, ReadRequest::List { query_json: r#"{"model":"Moment"}"#.into() });
    let entity = db.subscribe_values(epoch, ReadRequest::Entity { model: "Moment".into(), id: "m1".into(), fetch: ReadFetch::Never });
    let count = db.subscribe_values(epoch, ReadRequest::Count { query_json: r#"{"model":"Moment"}"#.into() });
    let (first_list, first_entity) = (next_rows(&rig, &list, 2).await, next_rows(&rig, &entity, 2).await);
    assert_eq!(first_list, first_entity);
    assert!(matches!(bound(count.pull()).await.unwrap(), Some(ReadValue::Count { count: 1, .. })));
    let hold = rig.world.hold_then("MomentReact", Fault::Answer(json!({"data":{"momentReact":{"__typename":"Moment","id":"m1","visitsCount":4,"reacted":true}}})));
    let receipt = bound(db.act(epoch, "Moment".into(), "m1".into(), "react".into(), json!({"visitsCount":3,"reacted":true}).to_string(), "{}".into())).await.unwrap();
    hold.entered_for("the answer").await;
    let (local_list, local_entity) = (next_rows(&rig, &list, 3).await, next_rows(&rig, &entity, 3).await);
    assert_eq!((local_list, local_entity), (receipt.revision, receipt.revision));
    hold.release();
    assert!(matches!(bound(db.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    let (server_list, server_entity) = (next_rows(&rig, &list, 4).await, next_rows(&rig, &entity, 4).await);
    assert_eq!(server_list, server_entity);
    assert!(server_list > local_list);
    let refused = rig.world.hold_then("MomentReact", Fault::Business);
    let receipt = bound(db.act(epoch, "Moment".into(), "m1".into(), "react".into(), json!({"visitsCount":5}).to_string(), "{}".into())).await.unwrap();
    refused.entered_for("the refusal").await;
    let (draft_list, draft_entity) = (next_rows(&rig, &list, 5).await, next_rows(&rig, &entity, 5).await);
    assert_eq!((draft_list, draft_entity), (receipt.revision, receipt.revision));
    refused.release();
    assert!(matches!(bound(db.outcome(receipt)).await.unwrap(), WireOperationOutcome::Rejected { .. }));
    let (rollback_list, rollback_entity) = (next_rows(&rig, &list, 4).await, next_rows(&rig, &entity, 4).await);
    assert_eq!(rollback_list, rollback_entity);
    assert!(rollback_list > draft_list);
    assert_quiet(db, &[&list, &entity, &count]).await;
    db.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_one_change_every_view_feed() {
    let (db, transport) = feed_database().await;
    let epoch = db.session_epoch();
    bound(db.node(epoch, "Chat".into(), "node".into(), "c1".into(), true)).await.unwrap().unwrap();
    let events = db.subscribe_values(epoch, ReadRequest::Events { model: "Chat".into() });
    assert!(matches!(bound(events.pull()).await.unwrap(), Some(ReadValue::Ready)));
    let list = db.subscribe_values(epoch, ReadRequest::List { query_json: r#"{"model":"Chat"}"#.into() });
    let entity = db.subscribe_values(epoch, ReadRequest::Entity { model: "Chat".into(), id: "c1".into(), fetch: ReadFetch::Never });
    let count = db.subscribe_values(epoch, ReadRequest::Count { query_json: r#"{"model":"Chat"}"#.into() });
    for stream in [&list, &entity, &count] { bound(stream.pull()).await.unwrap().unwrap(); }
    loop {
        let notified = transport.opened.notified();
        if transport.token.lock().unwrap().is_some() { break; }
        bound(notified).await;
    }
    let token = transport.token.lock().unwrap().clone().unwrap();
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"m1","chat":{"id":"c1","title":"after","revision":2,"lists":["PRIMARY"]}}]}}})));
    let mut revisions = Vec::new();
    for stream in [&list, &entity] {
        let value = bound(stream.pull()).await.unwrap().unwrap();
        let (revision, rows) = match value { ReadValue::Delta { revision, rows, .. } | ReadValue::Snapshot { revision, rows, .. } => (revision, rows), other => panic!("{other:?}") };
        assert_eq!(rows.len(), 1);
        assert!(crate::ffi::lifecycle_tests::named(&rows[0], "after"));
        revisions.push(revision);
    }
    assert_eq!(revisions[0], revisions[1]);
    assert_quiet(&db, &[&list, &entity, &count]).await;
    db.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_one_change_every_view_related_row() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, |models| { add_chat(models); add_profile(models); add_reactions(models); add_reactor_user(models); }).await;
    land_event(&rig, "User", json!({"id":"u2","name":"Bob"}));
    land_event(&rig, "Message", json!({"id":"m1","chatId":"c1","text":"hello","at":1,"reactions":[{"body":"B","userId":"u2","user":{"name":"Old"}}]}));
    let db = &rig.database;
    let epoch = db.session_epoch();
    let list = db.subscribe_values(epoch, ReadRequest::List { query_json: r#"{"model":"Message"}"#.into() });
    let entity = db.subscribe_values(epoch, ReadRequest::Entity { model: "Message".into(), id: "m1".into(), fetch: ReadFetch::Never });
    let count = db.subscribe_values(epoch, ReadRequest::Count { query_json: r#"{"model":"Message"}"#.into() });
    for stream in [&list, &entity, &count] { bound(stream.pull()).await.unwrap().unwrap(); }
    let hold = rig.world.hold_then("ProfileUpdate", Fault::Business);
    let receipt = bound(db.act(epoch, "User".into(), "u2".into(), "update".into(), json!({"name":"Robert"}).to_string(), json!({"name":"Robert"}).to_string())).await.unwrap();
    hold.entered_for("the refusal").await;
    let mut revisions = Vec::new();
    for stream in [&list, &entity] {
        let value = bound(stream.pull()).await.unwrap().unwrap();
        let (revision, rows) = match value { ReadValue::Delta { revision, rows, .. } | ReadValue::Snapshot { revision, rows, .. } => (revision, rows), other => panic!("{other:?}") };
        assert_eq!(rows.len(), 1);
        assert!(texts(&cell(&rig, "Message", &rows[0], "reactions")).contains(&"Robert".to_string()));
        revisions.push(revision);
    }
    assert_eq!(revisions, [receipt.revision, receipt.revision]);
    hold.release();
    assert!(matches!(bound(db.outcome(receipt)).await.unwrap(), WireOperationOutcome::Rejected { .. }));
    let mut restored = Vec::new();
    for stream in [&list, &entity] {
        let value = bound(stream.pull()).await.unwrap().unwrap();
        let (revision, rows) = match value { ReadValue::Delta { revision, rows, .. } | ReadValue::Snapshot { revision, rows, .. } => (revision, rows), other => panic!("{other:?}") };
        assert!(texts(&cell(&rig, "Message", &rows[0], "reactions")).contains(&"Bob".to_string()));
        restored.push(revision);
    }
    assert_eq!(restored[0], restored[1]);
    assert!(restored[0] > revisions[0]);
    assert_quiet(db, &[&list, &entity, &count]).await;
    db.close_now();
}
