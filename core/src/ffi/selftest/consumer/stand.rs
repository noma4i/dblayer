use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::{json, Value};

use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tape::Rows;
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::wire::{wire_fields, Cell, WireRow};
use crate::ffi::{Config, Database, DbError, EncryptedReply, FakeHttp, FakeTransport, ReadFetch, ReadRequest, ReadStream, ReadValue};
use crate::server_contract::{Admission, Contract, Request, SEQUENCE_CONFLICT};
use crate::transport::encrypted::tests::fake_server_tests::{decrypt_request, seal_response};
use crate::transport::encrypted::cable::tests::peer_tests::{CablePeer, PeerEvent};

pub(super) const SPEC: &str = include_str!("../../../../tests/corpus/consumer/spec.json");
pub(super) const OPERATIONS: &str = include_str!("../../../../tests/corpus/consumer/operations.json");

struct Cable {
    peer: CablePeer,
    puts: StdMutex<Vec<crate::transport::PutFile>>,
    put_script: StdMutex<VecDeque<(u16, Option<String>)>>,
    hold_put: std::sync::atomic::AtomicBool,
    put_started: tokio::sync::watch::Sender<usize>,
    put_release: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl FakeTransport for Cable {
    fn cable(&self) -> Option<&CablePeer> { Some(&self.peer) }
    async fn query(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: HashMap<String, String>, file: crate::transport::PutFile, _: crate::transport::TransferIdentity, progress: Arc<crate::ffi::ByteProgress>) -> Result<crate::ffi::PutReply, DbError> {
        self.puts.lock().unwrap().push(file.clone());
        progress.sent(2);
        crate::lock::effect(|| self.put_started.send_modify(|count| *count += 1));
        if self.hold_put.swap(false, std::sync::atomic::Ordering::SeqCst) { self.put_release.notified().await; }
        match self.put_script.lock().unwrap().pop_front() {
            Some((status, etag)) => Ok(crate::ffi::PutReply { status, etag }),
            None => panic!("unscripted PUT for {}", file.path),
        }
    }
}

/// The server behind the encrypted path: it answers each operation with the data the test set, anything else with an empty `data`. The pair and sequence rules are the server contract.
struct Server {
    answers: StdMutex<HashMap<String, Value>>,
    next_answers: StdMutex<HashMap<String, std::collections::VecDeque<Value>>>,
    next_failures: StdMutex<HashMap<String, std::collections::VecDeque<DbError>>>,
    asked: StdMutex<Vec<(String, Value)>>,
    sequenced: StdMutex<Vec<(String, i64)>>,
    request_ids: StdMutex<Vec<(String, Value)>>,
    contract: StdMutex<Contract<Value>>,
    conflicts: std::sync::atomic::AtomicUsize,
    held: tokio::sync::watch::Sender<HashSet<String>>,
    answered: tokio::sync::watch::Sender<usize>,
    received: tokio::sync::watch::Sender<usize>,
}

impl Default for Server {
    fn default() -> Self {
        Server { answers: StdMutex::default(), next_answers: StdMutex::default(), next_failures: StdMutex::default(), contract: StdMutex::new(Contract::adopting()), conflicts: 0.into(), asked: StdMutex::default(), sequenced: StdMutex::default(), request_ids: StdMutex::default(), held: tokio::sync::watch::channel(HashSet::new()).0, answered: tokio::sync::watch::channel(0).0, received: tokio::sync::watch::channel(0).0 }
    }
}

/// The hash the core sends for a document: sha256 of its text, the same as `graphql_body`.
fn document_hash(document: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(document.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

impl Server {
    /// The documents the app declared: a real server knows them, an undeclared document gets `PERSISTED_QUERY_NOT_FOUND`.
    fn register_documents(&self, operations: &str) {
        let declared: Vec<Value> = serde_json::from_str(operations).unwrap();
        let mut contract = self.contract.lock().unwrap();
        for operation in declared {
            if let Some(document) = operation["document"].as_str() { contract.register(&document_hash(document)); }
        }
    }
}

#[async_trait::async_trait]
impl FakeHttp for Server {
    async fn post(&self, path: String, body: String, _: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let request = decrypt_request(&body).unwrap();
        let answer = if path == "/api/auth/login_link" {
            let sequence = request["sequence"].as_i64().unwrap_or(0);
            self.sequenced.lock().unwrap().push(("login_link".into(), sequence));
            if sequence > 0 { self.contract.lock().unwrap().set_last(sequence); }
            json!({"sessionId": request["body"]["account"]})
        } else if path == "/api/auth/otp" {
            json!({"sent": true})
        } else {
            let operation = request["body"]["operationName"].as_str().unwrap().to_string();
            let contract_request = Request::from_wire("stand", &request);
            let admission = self.contract.lock().unwrap().admit(&contract_request);
            let sequence = request["sequence"].as_i64();
            match admission {
                Admission::Stored(stored) => return Ok(EncryptedReply { status: 200, body: seal_response(&request, sequence, stored) }),
                Admission::TwoBodies => panic!("the pair {} went with two bodies", contract_request.request_id),
                _ => {}
            }
            self.asked.lock().unwrap().push((operation.clone(), request["body"]["variables"].clone()));
            crate::lock::effect(|| self.received.send_modify(|count| *count += 1));
            self.sequenced.lock().unwrap().push((operation.clone(), sequence.unwrap_or(0)));
            self.request_ids.lock().unwrap().push((operation.clone(), request["requestId"].clone()));
            if let Admission::Refused(refusal) = admission {
                if refusal["error"] == SEQUENCE_CONFLICT { self.conflicts.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }
                return Ok(EncryptedReply { status: 200, body: seal_response(&request, None, refusal) });
            }
            let failure = self.next_failures.lock().unwrap().get_mut(&operation).and_then(|queue| queue.pop_front());
            if let Some(failure) = failure { return Err(failure); }
            let mut held = self.held.subscribe();
            held.wait_for(|operations| !operations.contains(&operation)).await.unwrap();
            let queued = self.next_answers.lock().unwrap().get_mut(&operation).and_then(|queue| queue.pop_front());
            let answer = queued.unwrap_or_else(|| self.answers.lock().unwrap().get(&operation).cloned().unwrap_or_else(|| json!({"data": {}})));
            crate::lock::effect(|| self.answered.send_modify(|count| *count += 1));
            self.contract.lock().unwrap().settle(&contract_request, answer.clone());
            answer
        };
        Ok(EncryptedReply { status: 200, body: seal_response(&request, request["sequence"].as_i64(), answer) })
    }
}

/// The core on the schemas of the app: its spec, its operation documents, the real session path, the server only through the cable and the fake HTTP.
pub(super) struct Stand {
    pub(super) database: Arc<Database>,
    server: Arc<Server>,
    cable: Arc<Cable>,
    subscribed: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<String>>,
    pub(super) clock: Arc<crate::ffi::TestClock>,
}

impl Stand {
    pub(super) async fn open() -> Stand {
        let stand = Stand::prepare().await;
        stand.sign_in().await;
        stand
    }

    /// The core with its operations declared and the network up, before the session: the preload of the app starts at `sign_in`.
    pub(super) async fn prepare() -> Stand { Stand::prepare_on(None, Arc::new(WatchedHeld::default()) as Arc<dyn crate::ffi::CredentialStore>).await }

    /// The same core on a file and a credential store that outlive the process: `reopen` is the next launch of the app.
    pub(super) async fn prepare_on(path: Option<String>, credentials: Arc<dyn crate::ffi::CredentialStore>) -> Stand {
        Self::prepare_contract(path, credentials, None).await
    }

    pub(super) async fn prepare_contract(path: Option<String>, credentials: Arc<dyn crate::ffi::CredentialStore>, contract: Option<Arc<StdMutex<crate::server_contract::CableContract>>>) -> Stand {
        Self::prepare_online(path, credentials, contract, true).await
    }

    pub(super) async fn prepare_online(path: Option<String>, credentials: Arc<dyn crate::ffi::CredentialStore>, contract: Option<Arc<StdMutex<crate::server_contract::CableContract>>>, online: bool) -> Stand {
        let server = Arc::new(Server::default());
        server.register_documents(OPERATIONS);
        let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
        encrypted.endpoints.session_operations.insert("otp".into(), crate::ffi::EncryptedSessionOperation { path: "/api/auth/otp".into(), class: crate::ffi::EncryptedOperationClass::Authenticated });
        encrypted.endpoints.session_operations.insert("login_link".into(), crate::ffi::EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: crate::ffi::EncryptedOperationClass::Authenticated });
        let (opened, subscribed) = tokio::sync::mpsc::unbounded_channel();
        let clock = crate::ffi::TestClock::new(1_000_000);
        let mut peer = CablePeer::observed(encrypted.clone(), move |event| { if let PeerEvent::Subscribed(token) = event { crate::lock::effect(|| { let _ = opened.send(token); }); } });
        if let Some(contract) = contract { peer = peer.with_contract(contract, clock.manual()); }
        let cable = Arc::new(Cable { peer, puts: StdMutex::default(), put_script: StdMutex::default(), hold_put: false.into(), put_started: tokio::sync::watch::channel(0).0, put_release: tokio::sync::Notify::new() });
        let ports = crate::ffi::OpenPorts { transport: Some(cable.clone()), encrypted: Some(encrypted), credentials: Some(credentials.clone()), http: Some(server.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
        let database = bound(Database::open_with_clock(Config { path, data_version: None }, SPEC.into(), ports, clock.clone())).await.unwrap();
        database.set_online(online).unwrap();
        database.register_operations(OPERATIONS.into()).await.unwrap();
        Stand { database, server, cable, subscribed: tokio::sync::Mutex::new(subscribed), clock }
    }

    pub(super) async fn sign_in(&self) {
        bound(self.database.session_call("login_link".into(), json!({"account": "A"}).to_string())).await.unwrap();
    }

    pub(super) fn empty_registry(&self) {
        let last = self.server.sequenced.lock().unwrap().iter().map(|(_, sequence)| *sequence).max().unwrap_or(0);
        let mut contract = self.server.contract.lock().unwrap();
        contract.forget();
        contract.set_last(last);
        self.server.conflicts.store(0, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn conflicts(&self) -> usize { self.server.conflicts.load(std::sync::atomic::Ordering::SeqCst) }

    /// The server answers the operation with `data` from now on.
    pub(super) fn answer(&self, operation: &str, data: Value) { self.server.answers.lock().unwrap().insert(operation.into(), json!({"data": data})); }

    pub(super) fn answer_multipart(&self) {
        self.answer("MultipartUploadInit", json!({"multipartUploadInit":{"multipartUpload":{"key":"key-1","uploadId":"upload-1","signedId":"signed","blobId":"b1"}}}));
        self.answer("MultipartUploadPresignParts", json!({"multipartUploadPresign":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}));
        self.answer("MultipartUploadComplete", json!({"multipartUploadComplete":{"signedId":"signed","blobId":"b1"}}));
        self.answer("MultipartUploadAbort", json!({"multipartUploadAbort":{"aborted":true}}));
    }

    /// The next request of the operation gets `answer` (the whole body, `data` or `errors`) once, then the standing answer again.
    pub(super) fn answer_next(&self, operation: &str, answer: Value) { self.server.next_answers.lock().unwrap().entry(operation.into()).or_default().push_back(answer); }
    pub(super) fn fail_next(&self, operation: &str, failure: DbError) { self.server.next_failures.lock().unwrap().entry(operation.into()).or_default().push_back(failure); }

    /// Every request the server received, in order: the operation and its `sequence`.
    pub(super) fn sequenced(&self) -> Vec<(String, i64)> { self.server.sequenced.lock().unwrap().clone() }

    pub(super) fn request_ids(&self) -> Vec<(String, Value)> { self.server.request_ids.lock().unwrap().clone() }

    /// How many times the core asked the server for the operation.
    pub(super) fn requests(&self, operation: &str) -> usize { self.server.asked.lock().unwrap().iter().filter(|(asked, _)| asked == operation).count() }

    pub(super) fn puts(&self) -> usize { self.cable.puts.lock().unwrap().len() }

    pub(super) fn put_files(&self) -> Vec<crate::transport::PutFile> { self.cable.puts.lock().unwrap().clone() }

    /// The next PUT the storage receives answers with `status` and `etag`; an unscripted PUT fails the test.
    pub(super) fn script_put(&self, status: u16, etag: Option<&str>) { self.cable.put_script.lock().unwrap().push_back((status, etag.map(str::to_string))); }

    pub(super) fn hold_put(&self) { self.cable.hold_put.store(true, std::sync::atomic::Ordering::SeqCst); }

    pub(super) async fn put_started(&self) { bound(self.cable.put_started.subscribe().wait_for(|count| *count > 0)).await.unwrap(); }

    pub(super) fn release_put(&self) { crate::lock::effect(|| self.cable.put_release.notify_one()); }

    /// How many times the core asked for the operation with the variable `name` equal to `value`.
    pub(super) fn requests_with(&self, operation: &str, name: &str, value: &str) -> usize { self.server.asked.lock().unwrap().iter().filter(|(asked, variables)| asked == operation && variables[name] == value).count() }

    /// Every request the server received, in order, with its variables.
    pub(super) fn asked(&self) -> Vec<(String, Value)> { self.server.asked.lock().unwrap().clone() }

    /// The server holds the answers of the operations until `release`.
    pub(super) fn hold(&self, operations: &[&str]) { crate::lock::effect(|| self.server.held.send_modify(|held| held.extend(operations.iter().map(|operation| operation.to_string())))); }

    pub(super) fn release(&self, operations: &[&str]) { crate::lock::effect(|| self.server.held.send_modify(|held| operations.iter().for_each(|operation| { held.remove(*operation); }))); }

    /// The server has answered `count` requests in all.
    pub(super) async fn answered(&self, count: usize) { bound(self.server.answered.subscribe().wait_for(|answered| *answered >= count)).await.unwrap(); }

    /// The server has received `count` requests in all, answered or held.
    pub(super) async fn received(&self, count: usize) { bound(self.server.received.subscribe().wait_for(|received| *received >= count)).await.unwrap(); }

    /// The next subscription the cable opened: its token.
    pub(super) async fn next_subscription(&self) -> String { let mut subscribed = self.subscribed.lock().await; bound(async { subscribed.recv().await }).await.expect("the cable is open") }

    /// The socket of the subscription breaks.
    pub(super) fn drop_cable(&self, token: &str) { assert!(self.cable.peer.close(token), "the socket is open"); }

    pub(super) fn expire_cable(&self, token: &str) { self.cable.peer.expire_idle(token); }

    pub(super) fn cable_connects(&self) -> usize { self.cable.peer.connects() }

    pub(super) fn cable_ping(&self, token: &str) { self.cable.peer.emit(token, json!({"t":"ping", "ts":1})); }

    /// The events of the model: its stream opens the change feed of the app.
    pub(super) async fn events(&self, model: &str) -> (Arc<ReadStream>, String) {
        let stream = self.database.subscribe_values(self.database.session_epoch(), ReadRequest::Events { model: model.into() });
        assert!(matches!(bound(stream.pull_next()).await, Some(ReadValue::Ready)));
        let token = self.next_subscription().await;
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
    json!({"__typename": "Chat", "id": id, "uuid": format!("u-{id}"), "kind": "GROUP", "name": name, "logoUrl": null, "description": null, "viewerState": {"list": "PRIMARY", "lists": ["PRIMARY"], "connectionStatus": null, "premium": false, "pinned": false, "muted": false, "mutedUntil": null, "read": true, "unreadCount": 0}, "revision": 1, "messagesCount": 0, "membersCount": 2, "lastActivityAt": "2026-11-08T00:00:00Z", "lastMessageAt": null, "connectedAt": null, "createdAt": "2026-11-08T00:00:00Z", "updatedAt": "2026-11-08T00:00:00Z"})
}

pub(super) fn message(id: &str, chat_id: &str, text: &str) -> Value {
    json!({"__typename": "Message", "id": id, "chatId": chat_id, "kind": "TEXT", "body": text, "status": "SENDING", "oneTime": false, "expired": false, "isSystem": false, "isForwarded": false, "isReply": false, "unread": false, "sequenceNumber": null, "createdAt": "2026-11-08T00:00:00Z", "updatedAt": "2026-11-08T00:00:00Z"})
}

pub(super) fn moment(id: &str, visits: i64) -> Value {
    json!({"__typename": "Moment", "id": id, "uuid": format!("u-{id}"), "kind": "BASIC", "status": "DRAFTED", "contentKind": "SAFE", "compassTitle": "", "compassUpdatedAt": null, "reacted": false, "shadowBanned": false, "shareUrl": "", "visitsCount": visits, "similarMomentsCount": 0, "unreadCount": null, "unreadSimilarMomentsCount": 0, "user": null, "media": null, "widgets": [], "createdAt": "2026-11-08T00:00:00Z", "updatedAt": "2026-11-08T00:00:00Z"})
}

impl crate::test_wait::Quiet for Stand {
    async fn quiet(&self) { crate::test_wait::Quiet::quiet(&self.database).await }
}
