use std::sync::{Arc, Mutex};

use serde_json::json;

use super::rig_tests::{bound, World};
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::ffi::{Database, DbError, ReadFetch, ReadRequest, ReadValue};

struct Cable {
    peer: crate::transport::encrypted::cable::tests::peer_tests::CablePeer,
    token: Arc<Mutex<Option<String>>>,
    opened: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::ffi::FakeTransport for Cable {
    fn cable(&self) -> Option<&crate::transport::encrypted::cable::tests::peer_tests::CablePeer> { Some(&self.peer) }
    async fn query(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<crate::ffi::ByteProgress>) -> Result<crate::ffi::PutReply, DbError> { unreachable!() }
}

async fn open(world: Arc<World>) -> (Arc<Database>, Arc<Cable>, Arc<crate::ffi::TestClock>) {
    use crate::schema::{FieldSpec, ModelSpec, NodeRead, RelationSpec, RemoteShape, VariableSource, NODE_PARAM};
    use crate::value::ScalarKind;
    let mut presence = ModelSpec::new("Presence", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("online", ScalarKind::Bool)]);
    let mut node = RelationSpec::new("node", &[]);
    node.text_params = vec![NODE_PARAM.into()];
    node.node = Some(NodeRead::One);
    let mut remote = crate::schema_fixtures::remote("PresenceNode", RemoteShape::Single, "5.minutes");
    remote.variables.insert("id".into(), VariableSource::Param { name: NODE_PARAM.into() });
    node.remote = Some(remote);
    let mut details = RelationSpec::new("details", &[]);
    details.text_params.push("key".into());
    let mut remote = crate::schema_fixtures::remote("PresenceDetails", RemoteShape::Single, "5.minutes");
    remote.variables.insert("id".into(), VariableSource::Param { name: "key".into() });
    details.remote = Some(remote);
    presence.relations.extend([node, details]);
    presence.changes = vec![serde_json::from_value(json!({"area":"PRESENCE","relation":"node","removes_on":["DELETED"]})).unwrap()];
    presence.change_feed = Some(crate::schema::ChangeFeed { subscription: "RecordEvents".into(), root: "recordEvents.entries".into(), area: "topic".into(), kind: "action".into(), id: "recordId".into() });
    let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
    encrypted.endpoints.session_operations.insert("login_link".into(), crate::ffi::EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: crate::ffi::EncryptedOperationClass::Authenticated });
    let token = Arc::new(Mutex::new(None));
    let (observed, cable_opened) = (token.clone(), Arc::new(tokio::sync::Notify::new()));
    let signal = cable_opened.clone();
    let peer = crate::transport::encrypted::cable::tests::peer_tests::CablePeer::observed(encrypted.clone(), move |event| {
        if let crate::transport::encrypted::cable::tests::peer_tests::PeerEvent::Subscribed(subscribed) = event { *observed.lock().unwrap() = Some(subscribed); signal.notify_one(); }
    });
    let cable = Arc::new(Cable { peer, token, opened: cable_opened });
    let clock = crate::ffi::TestClock::new(1_000_000);
    let database = bound(Database::open_with_clock(crate::ffi::Config { path: None }, crate::ffi::specs(serde_json::to_string(&vec![presence]).unwrap()), crate::ffi::OpenPorts { partial_operations: true, transport: Some(cable.clone()), encrypted: Some(encrypted), credentials: Some(Arc::new(crate::ffi::tests::session_guards_tests::WatchedHeld::default())), http: Some(world), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, clock.clone())).await.unwrap();
    database.register_operations(json!([
        {"name":"PresenceNode","document":"query PresenceNode($id: ID!) { node(id: $id) { ... on Presence { id online } } }"},
        {"name":"PresenceDetails","document":"query PresenceDetails($id: ID!) { presence(id: $id) { id online } }"},
        {"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }"}
    ]).to_string()).await.unwrap();
    database.set_online(true).await.unwrap();
    bound(database.session_call("login_link".into(), json!({"account":"A"}).to_string())).await.unwrap();
    (database, cable, clock)
}

fn node_answer(online: bool) -> serde_json::Value { json!({"data":{"node":{"__typename":"Presence","id":"p1","online":online}}}) }

fn online_of(rows: &Rows) -> Option<bool> {
    rows.0.lock().unwrap().iter().find(|row| row.id == "p1").and_then(|row| row.cells.iter().find_map(|cell| if let Cell::Bool { value } = cell { Some(*value) } else { None }))
}

async fn observe(loaded_by_node: bool, events_stream: bool, fetch: ReadFetch) -> (bool, usize, Option<bool>, u64) {
    let world = World::new("server", "A", true);
    let (database, cable, _) = open(world.clone()).await;
    let epoch = database.session_epoch();
    let stream = events_stream.then(|| database.subscribe_values(epoch, ReadRequest::Events { model: "Presence".into() }));
    if let Some(stream) = &stream { assert!(matches!(bound(stream.pull_next()).await, Some(ReadValue::Ready))); }
    if loaded_by_node {
        world.answer_with("PresenceNode", node_answer(true));
        bound(database.node(epoch, "Presence".into(), "node".into(), "p1".into(), true)).await.unwrap();
    } else {
        world.answer_with("PresenceDetails", json!({"data":{"presence":{"__typename":"Presence","id":"p1","online":true}}}));
        bound(database.load(epoch, "Presence".into(), "details".into(), json!({"key":"p1"}).to_string())).await.unwrap();
    }
    world.answer_with("PresenceNode", node_answer(true));
    let before_reader = world.count("PresenceNode");
    let never = matches!(fetch, ReadFetch::Never);
    let rows = Arc::new(Rows::default());
    let reader = database.watch(ReadRequest::Entity { model: "Presence".into(), id: "p1".into(), fetch }, rows.clone());
    bound(reader.wait_for_initial_load()).await.unwrap();
    assert_eq!(online_of(&rows), Some(true), "the reader shows the loaded row");
    crate::test_wait::signalled(&cable.opened, "the subscription opens", || cable.token.lock().unwrap().is_some()).await;
    let token = cable.token.lock().unwrap().clone();
    let asked = world.count("PresenceNode");
    if never { assert_eq!(asked, before_reader, "a reader without fetch sends no node request before the event"); }
    world.answer_with("PresenceNode", node_answer(false));
    if let Some(token) = &token { assert!(cable.peer.event(token, json!({"data":{"recordEvents":{"entries":[{"topic":"PRESENCE","action":"UPDATED","recordId":"p1"}]}}}))); }
    crate::test_wait::settled(&database, "the event is applied", || true).await;
    let seen = (token.is_some(), world.count("PresenceNode") - asked, online_of(&rows), bound(database.subscription_census()).await.unwrap().refused_events);
    drop(stream);
    bound(database.close_database()).await.unwrap();
    seen
}

/// The event `PRESENCE UPDATED p1` of `changed_by "PRESENCE", via: :node` rereads the node once and the reader of the row shows the new value: the row loaded by the `node` read or by `details`, with the reader alone or with an `events` stream.
#[tokio::test(flavor = "multi_thread")]
async fn node_event_rereads_the_row_whichever_read_loaded_it() {
    let mut table = Vec::new();
    for (loaded_by_node, events_stream, fetch) in [(true, false, ReadFetch::Never), (true, true, ReadFetch::Never), (false, false, ReadFetch::Never), (false, true, ReadFetch::Never), (true, false, ReadFetch::Stale), (false, false, ReadFetch::Stale)] {
        table.push(((loaded_by_node, events_stream, format!("{fetch:?}")), observe(loaded_by_node, events_stream, fetch).await));
    }
    let wanted: Vec<_> = table.iter().map(|(case, _)| (case.clone(), (true, 1usize, Some(false), 0u64))).collect();
    assert_eq!(table, wanted, "(loaded by node, events stream, fetch) -> (subscription open, node requests after the event, value of the reader, refused events)");
}

/// D-361 and the reconnect rule: after the subscription is lost and opens again, the row with an open reader (`fetch: never`) is reread once by its node and the reader shows the new value. A stored row that nobody reads is not requested. The loss is the network going away, or the server closing the cable.
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_subscription_rereads_the_rows_with_open_readers() {
    let mut table = Vec::new();
    for server_closes in [false, true] {
        let world = World::new("server", "A", true);
        let (database, cable, clock) = open(world.clone()).await;
        let epoch = database.session_epoch();
        for id in ["p1", "p2"] {
            world.answer_with("PresenceDetails", json!({"data":{"presence":{"__typename":"Presence","id":id,"online":true}}}));
            bound(database.load(epoch, "Presence".into(), "details".into(), json!({"key":id}).to_string())).await.unwrap();
        }
        let rows = Arc::new(Rows::default());
        let reader = database.watch(ReadRequest::Entity { model: "Presence".into(), id: "p1".into(), fetch: ReadFetch::Never }, rows.clone());
        bound(reader.wait_for_initial_load()).await.unwrap();
        crate::test_wait::signalled(&cable.opened, "the subscription opens", || cable.token.lock().unwrap().is_some()).await;
        let token = cable.token.lock().unwrap().take().unwrap();
        let asked = world.count("PresenceNode");
        world.answer_with("PresenceNode", node_answer(false));
        if server_closes {
            assert!(cable.peer.close(&token));
            crate::test_wait::settled(&database, "the closed cable is seen", || true).await;
            clock.advance(120_000);
        } else {
            database.set_online(false).await.unwrap();
            database.set_online(true).await.unwrap();
        }
        crate::test_wait::signalled(&cable.opened, "the subscription opens again", || cable.token.lock().unwrap().is_some()).await;
        crate::test_wait::settled(&database, "the reread after the reconnect lands", || online_of(&rows) == Some(false)).await;
        table.push((server_closes, world.count("PresenceNode") - asked, online_of(&rows)));
        drop(reader);
        bound(database.close_database()).await.unwrap();
    }
    assert_eq!(table, vec![(false, 1, Some(false)), (true, 1, Some(false))], "(the server closes the cable, node requests after the reconnect, value of the reader)");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_id_that_only_starts_with_temp_is_read_from_the_server() {
    let world = World::new("server", "A", true);
    let (database, _, _) = open(world.clone()).await;
    let epoch = database.session_epoch();
    world.answer_with("PresenceNode", json!({"data":{"node":{"__typename":"Presence","id":"temp-report","online":true}}}));
    let row = bound(database.node(epoch, "Presence".into(), "node".into(), "temp-report".into(), true)).await.unwrap();
    assert_eq!((world.count("PresenceNode"), row.map(|row| row.id)), (1, Some("temp-report".to_string())));
    bound(database.close_database()).await.unwrap();
}
