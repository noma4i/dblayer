use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::{json, Value};

use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tape::Rows;
use crate::test_wait::yield_until;
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::wire::{wire_fields, Cell, WireRow};
use crate::ffi::{Config, Database, DbError, EncryptedReply, FakeHttp, FakeTransport, ReadFetch, ReadRequest, ReadStream, ReadValue};
use crate::transport::encrypted::tests::fake_server_tests::{decrypt_request, seal_response};
use crate::transport::encrypted::cable::tests::peer_tests::{CablePeer, PeerEvent};

const SPEC: &str = include_str!("../../../../tests/corpus/consumer_yupi/spec.json");
const OPERATIONS: &str = include_str!("../../../../tests/corpus/consumer_yupi/operations.json");

struct Cable {
    peer: CablePeer,
}

#[async_trait::async_trait]
impl FakeTransport for Cable {
    fn cable(&self) -> Option<&CablePeer> { Some(&self.peer) }
    async fn query(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<crate::ffi::ByteProgress>) -> Result<crate::ffi::PutReply, DbError> { unreachable!() }
}

/// The server behind the encrypted path: it answers each operation with the data the test set, anything else with an empty `data`.
#[derive(Default)]
struct Server {
    answers: StdMutex<HashMap<String, Value>>,
    asked: StdMutex<Vec<String>>,
}

#[async_trait::async_trait]
impl FakeHttp for Server {
    async fn post(&self, path: String, body: String, _: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let request = decrypt_request(&body).unwrap();
        let answer = if path == "/api/auth/login_link" {
            json!({"sessionId": request["body"]["account"]})
        } else {
            let operation = request["body"]["operationName"].as_str().unwrap().to_string();
            self.asked.lock().unwrap().push(operation.clone());
            self.answers.lock().unwrap().get(&operation).cloned().unwrap_or_else(|| json!({"data": {}}))
        };
        Ok(EncryptedReply { status: 200, body: seal_response(&request, request["sequence"].as_i64(), answer) })
    }
}

/// The core on the schemas of the app: its spec, its operation documents, the real session path, the server only through the cable and the fake HTTP.
pub(super) struct Stand {
    pub(super) database: Arc<Database>,
    server: Arc<Server>,
    cable: Arc<Cable>,
    subscribed: Arc<StdMutex<Vec<String>>>,
}

impl Stand {
    pub(super) async fn open() -> Stand {
        let server = Arc::new(Server::default());
        let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
        encrypted.endpoints.session_operations.insert("login_link".into(), crate::ffi::EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: crate::ffi::EncryptedOperationClass::Authenticated });
        let subscribed = Arc::new(StdMutex::new(Vec::new()));
        let opened = subscribed.clone();
        let peer = CablePeer::observed(encrypted.clone(), move |event| { if let PeerEvent::Subscribed(token) = event { opened.lock().unwrap().push(token); } });
        let cable = Arc::new(Cable { peer });
        let ports = crate::ffi::OpenPorts { transport: Some(cable.clone()), encrypted: Some(encrypted), credentials: Some(Arc::new(WatchedHeld::default())), http: Some(server.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
        let database = bound(Database::open_with_clock(Config { path: None, data_version: None }, SPEC.into(), ports, crate::ffi::TestClock::new(1_000_000))).await.unwrap();
        database.register_operations(OPERATIONS.into()).await.unwrap();
        database.set_online(true).unwrap();
        bound(database.session_call("login_link".into(), json!({"account": "A"}).to_string())).await.unwrap();
        Stand { database, server, cable, subscribed }
    }

    /// The server answers the operation with `data` from now on.
    pub(super) fn answer(&self, operation: &str, data: Value) { self.server.answers.lock().unwrap().insert(operation.into(), json!({"data": data})); }

    /// How many times the core asked the server for the operation.
    pub(super) fn requests(&self, operation: &str) -> usize { self.server.asked.lock().unwrap().iter().filter(|asked| *asked == operation).count() }

    /// The events of the model: its stream opens the change feed of the app.
    pub(super) async fn events(&self, model: &str) -> (Arc<ReadStream>, String) {
        let stream = self.database.subscribe_values(self.database.session_epoch(), ReadRequest::Events { model: model.into() });
        assert!(matches!(bound(stream.pull()).await, Some(ReadValue::Ready)));
        yield_until("the feed subscription is open", || !self.subscribed.lock().unwrap().is_empty()).await;
        let token = self.subscribed.lock().unwrap().remove(0);
        (stream, token)
    }

    /// The entity of the app screens: a reader of the node of one row.
    pub(super) fn entity(&self, model: &str, id: &str, fetch: ReadFetch) -> Arc<Rows> {
        let rows = Arc::new(Rows::default());
        std::mem::forget(self.database.watch(ReadRequest::Entity { model: model.into(), id: id.into(), fetch }, rows.clone()));
        rows
    }

    /// The server pushes one entry of `viewerChanges.changes` on the open subscription.
    pub(super) fn push(&self, token: &str, entry: Value) {
        assert!(self.cable.peer.event(token, json!({"data": {"viewerChanges": {"changes": [entry]}}})), "the subscription is open");
    }

    pub(super) fn cell(&self, model: &str, row: &WireRow, field: &str) -> Cell {
        let spec = self.database.db.spec(model).unwrap();
        let at = wire_fields(spec).iter().position(|declared| declared.name == field).unwrap_or_else(|| panic!("{model} has no field {field}"));
        row.cells[at].clone()
    }

    pub(super) fn text_of(&self, rows: &Rows, model: &str, id: &str, field: &str) -> Option<String> {
        let rows = rows.0.lock().unwrap();
        let row = rows.iter().find(|row| row.id == id)?;
        match self.cell(model, row, field) { Cell::Text { value } => Some(value), _ => None }
    }

    pub(super) fn int_of(&self, rows: &Rows, model: &str, id: &str, field: &str) -> Option<i64> {
        let rows = rows.0.lock().unwrap();
        let row = rows.iter().find(|row| row.id == id)?;
        match self.cell(model, row, field) { Cell::Int { value } => Some(value), _ => None }
    }

    /// Every delivery the core owes after the last push has happened.
    pub(super) async fn settle(&self) { bound(crate::ffi::settled(&self.database)).await.unwrap(); }
}

pub(super) fn chat(id: &str, name: &str) -> Value {
    json!({"__typename": "Chat", "id": id, "uuid": format!("u-{id}"), "kind": "GROUP", "name": name, "viewerState": {"list": "PRIMARY", "lists": ["PRIMARY"], "premium": false, "pinned": false, "muted": false, "read": true}, "revision": 1, "messagesCount": 0, "membersCount": 2, "lastActivityAt": "2026-11-08T00:00:00Z", "createdAt": "2026-11-08T00:00:00Z", "updatedAt": "2026-11-08T00:00:00Z"})
}

pub(super) fn message(id: &str, chat_id: &str, text: &str) -> Value {
    json!({"__typename": "Message", "id": id, "chatId": chat_id, "kind": "TEXT", "body": text, "createdAt": "2026-11-08T00:00:00Z", "updatedAt": "2026-11-08T00:00:00Z"})
}

pub(super) fn moment(id: &str, visits: i64) -> Value {
    json!({"__typename": "Moment", "id": id, "uuid": format!("u-{id}"), "visitsCount": visits, "createdAt": "2026-11-08T00:00:00Z", "updatedAt": "2026-11-08T00:00:00Z"})
}
