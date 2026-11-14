use crate::ffi::{FakeTransport};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use tokio::sync::{Notify, Semaphore};

use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::server_contract::{Admission, Contract, Request};
use crate::ffi::{
    ByteProgress, Config, Database, DbError, FakeHttp, EncryptedReply, EncryptedSessionOperation, OpenDatabase, PutReply,
    TestClock, Transport, TransportRequest,
};

use super::rig_held::Held;
use super::rig_models::{add_reactions, plain_models, plain_operations};
use crate::transport::encrypted::tests::fake_server_tests::{decrypt_request, seal_response};

/// Every call of a sequence returns within this many seconds or the test fails instead of hanging.
pub(super) const BOUND: u64 = 8;

#[track_caller]
pub(super) fn bound<T>(work: impl std::future::Future<Output = T>) -> impl std::future::Future<Output = T> {
    let place = std::panic::Location::caller();
    async move { crate::test_wait::within_named(BOUND, &format!("bounded wait at {place}"), work).await }
}

pub(super) struct Gate {
    account: Mutex<Option<String>>,
    entered: Notify,
    has_entered: std::sync::atomic::AtomicBool,
    release: Semaphore,
}

impl Gate {
    pub(super) fn new() -> Arc<Self> { Arc::new(Self { account: Mutex::new(None), entered: Notify::new(), has_entered: std::sync::atomic::AtomicBool::new(false), release: Semaphore::new(0) }) }
    pub(super) async fn pass(&self) {
        self.has_entered.store(true, std::sync::atomic::Ordering::SeqCst);
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
    }
    pub(super) async fn entered(&self) { crate::test_wait::signalled(&self.entered, "the request enters the gate", || self.has_entered.load(std::sync::atomic::Ordering::SeqCst)).await }
    pub(super) fn release(&self) { self.release.add_permits(1) }
    /// The held request carries rows of this account, whatever the server shows to the others.
    pub(super) fn as_account(self: &Arc<Self>, account: &str) -> Arc<Self> {
        *self.account.lock().unwrap() = Some(account.into());
        self.clone()
    }
    fn account(&self) -> Option<String> { self.account.lock().unwrap().clone() }
}

#[derive(Clone)]
pub(super) enum Fault {
    Hold(Arc<Gate>),
    /// The request is held, then ends in the inner fault.
    HoldThen(Arc<Gate>, Box<Fault>),
    Refuse { code: &'static str, confirmed: bool },
    Http(u16),
    ResponseTooLarge(u64),
    AppliedResponseTooLarge(u64),
    Business,
    Answer(Value),
    /// The server answers 200 with a body that is no sealed answer.
    Unreadable,
    /// The request does not reach the server: the caller sees a network error.
    Down,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Entry {
    pub(super) operation: String,
    pub(super) account: String,
    pub(super) label: String,
    pub(super) sequence: Option<i64>,
    pub(super) request_id: Option<String>,
    pub(super) body: String,
    pub(super) marks_before: usize,
}

struct State {
    contract: Contract<Value>,
    account: String,
    credential: String,
    signed: bool,
    profile_empty: bool,
    forbidden: bool,
    calls: BTreeMap<String, usize>,
    violations: Vec<String>,
    log: Vec<Entry>,
    marks: Vec<String>,
    script: Vec<(String, Fault)>,
    messages: Vec<(String, Value)>,
    group_members: BTreeMap<String, Vec<String>>,
    profile: Map<String, Value>,
    blocked: Vec<String>,
    canned: BTreeMap<String, Value>,
}

/// A held request first waits at its gate, then ends in the inner fault, if any.
async fn pass_gate(fault: Option<Fault>) -> Option<Fault> {
    match fault {
        Some(Fault::Hold(gate)) => { gate.pass().await; None }
        Some(Fault::HoldThen(gate, inner)) => { gate.pass().await; Some(*inner) }
        other => other,
    }
}

impl Default for State {
    fn default() -> Self {
        let mut contract = Contract::adopting();
        contract.cache_everything();
        State { contract, account: String::new(), credential: String::new(), signed: false, profile_empty: false, forbidden: false, calls: BTreeMap::new(), violations: Vec::new(), log: Vec::new(), marks: Vec::new(), script: Vec::new(), messages: Vec::new(), group_members: BTreeMap::new(), profile: Map::new(), blocked: Vec::new(), canned: BTreeMap::new() }
    }
}

/// The server of one account: it answers the encrypted session path and, as a plain transport target, the queries of a harness.
pub(super) struct World {
    label: String,
    state: Mutex<State>,
}

impl World {
    pub(super) fn new(label: &str, account: &str, signed: bool) -> Arc<Self> {
        Arc::new(Self { label: label.into(), state: Mutex::new(State { account: account.into(), signed, ..State::default() }) })
    }
    pub(super) fn set_account(&self, account: &str) { self.state.lock().unwrap().account = account.into(); }
    pub(super) fn account(&self) -> String { self.state.lock().unwrap().account.clone() }
    pub(super) fn signed(&self) -> bool { self.state.lock().unwrap().signed }
    pub(super) fn empty_profile(&self) { self.state.lock().unwrap().profile_empty = true; }
    pub(super) fn forbid(&self, forbidden: bool) { self.state.lock().unwrap().forbidden = forbidden; }
    pub(super) fn violations(&self) -> Vec<String> { self.state.lock().unwrap().violations.clone() }
    pub(super) fn count(&self, operation: &str) -> usize { self.state.lock().unwrap().calls.get(operation).copied().unwrap_or(0) }
    pub(super) fn log(&self) -> Vec<Entry> { self.state.lock().unwrap().log.clone() }
    /// Notes a step of the test: `journal` prints the steps and the requests of the server in the order they happened.
    pub(super) fn mark(&self, step: &str) { self.state.lock().unwrap().marks.push(step.into()); }
    pub(super) fn journal(&self) -> String {
        let state = self.state.lock().unwrap();
        let mut lines = Vec::new();
        for (index, step) in state.marks.iter().enumerate() {
            lines.extend(state.log.iter().filter(|entry| entry.marks_before == index).map(|entry| format!("request {} {}", entry.operation, entry.body)));
            lines.push(format!("step {step}"));
        }
        lines.extend(state.log.iter().filter(|entry| entry.marks_before >= state.marks.len()).map(|entry| format!("request {} {}", entry.operation, entry.body)));
        lines.join("\n")
    }
    pub(super) fn hits(&self) -> usize { self.state.lock().unwrap().log.len() }
    /// A message of the thread `c1` that the server already holds for this account.
    pub(super) fn seed_message(&self, account: &str, id: &str, text: &str, at: i64) {
        self.state.lock().unwrap().messages.push((account.into(), json!({"__typename":"Message","id":id,"chatId":"c1","text":text,"at":at})));
    }
    /// The answer of `operation` from now on, whatever its variables.
    pub(super) fn answer_with(&self, operation: &str, answer: Value) { self.state.lock().unwrap().canned.insert(operation.into(), answer); }
    pub(super) fn set_profile(&self, fields: Value) { self.state.lock().unwrap().profile.extend(fields.as_object().unwrap().clone()); }
    pub(super) fn seed_authored_message(&self, account: &str, id: &str, text: &str, at: i64, author: &str) {
        self.state.lock().unwrap().messages.push((account.into(), json!({"__typename":"Message","id":id,"chatId":"c1","text":text,"at":at,"authorId":author})));
    }
    pub(super) fn seed_reacted_message(&self) {
        self.state.lock().unwrap().messages.push(("A".into(), json!({"__typename":"Message","id":"m1","chatId":"c1","text":"hello","at":1,"reactions":[{"userId":"u2","body":"B"}]})));
    }
    pub(super) fn set_members(&self, order: &str, ids: &[&str]) {
        self.state.lock().unwrap().group_members.insert(order.into(), ids.iter().map(|id| id.to_string()).collect());
    }
    pub(super) fn fail_next(&self, operation: &str, fault: Fault) { self.state.lock().unwrap().script.push((operation.into(), fault)); }
    pub(super) fn pending_faults(&self) -> usize { self.state.lock().unwrap().script.len() }
    pub(super) fn hold_then(&self, operation: &str, inner: Fault) -> Arc<Gate> {
        let gate = Gate::new();
        self.fail_next(operation, Fault::HoldThen(gate.clone(), Box::new(inner)));
        gate
    }
    pub(super) fn hold_next(&self, operation: &str) -> Arc<Gate> {
        let gate = Gate::new();
        self.fail_next(operation, Fault::Hold(gate.clone()));
        gate
    }

    fn enter(&self, operation: &str, sequence: Option<i64>, request_id: Option<String>, body: &Value) -> Option<(Value, Option<Fault>)> {
        let mut state = self.state.lock().unwrap();
        if state.forbidden {
            let frames = std::backtrace::Backtrace::force_capture().to_string().lines().filter(|line| line.contains("dblayer::owners") || line.contains("dblayer::ffi::database") || line.contains("dblayer::ffi::loading") || line.contains("dblayer::ffi::command") || line.contains("dblayer::ffi::read")).take(16).map(|line| line.trim().to_string()).collect::<Vec<_>>().join(" | ");
            let detail = format!("{} {operation} via {frames}", self.label);
            state.violations.push(detail);
            return None;
        }
        let account = state.account.clone();
        let marks_before = state.marks.len();
        state.log.push(Entry { operation: operation.into(), account: account.clone(), label: self.label.clone(), sequence, request_id, body: body.to_string(), marks_before });
        let position = state.script.iter().position(|(wanted, _)| wanted == operation || wanted == "*");
        let fault = position.map(|index| state.script.remove(index).1);
        let commits = matches!(fault, None | Some(Fault::Hold(_)));
        let held_account = match &fault {
            Some(Fault::Hold(gate) | Fault::HoldThen(gate, _)) => gate.account(),
            _ => None,
        };
        let account = held_account.unwrap_or(account);
        let mut row = json!({"__typename":"User","id":"user","name":account});
        row.as_object_mut().unwrap().extend(state.profile.clone());
        let answer = match operation {
            "login_link" | "social" => {
                if commits {
                    state.account = body["account"].as_str().unwrap_or("A").into();
                    state.signed = true;
                }
                json!({"sessionId": body["account"]})
            }
            "logout" => {
                if commits { state.signed = false; }
                json!({"revoked": true})
            }
            "otp" => json!({"sent": true}),
            _ if state.canned.contains_key(operation) => state.canned[operation].clone(),
            _ if !state.signed => json!({"error": "UNAUTHENTICATED"}),
            "SessionProfile" => json!({"data":{"sessionProfile":{"id":account}}}),
            "UserNode" => json!({"data":{"node":row}}),
            "MyProfile" => json!({"data":{"me":if state.profile_empty { Value::Null } else { row }}}),
            "Users" => json!({"data":{"users":[row]}}),
            "ChatList" if body["variables"]["list"] == "SECONDARY" => json!({"data":{"chats":[{"__typename":"Chat","id":"c1","title":"before","revision":1,"lists":["SECONDARY"]}]}}),
            "ChatList" => json!({"data":{"chats":[]}}),
            "ChatNode" => json!({"data":{"node":{"__typename":"Chat","id":"c1","title":"before","revision":1,"lists":["SECONDARY"]}}}),
            "UnregisterDevice" => json!({"data":{"unregisterDevice":{"ok":true}}}),
            "Thread" => json!({"data":{"thread": state.messages.iter().filter(|(owner, _)| *owner == account).map(|(_, message)| message.clone()).collect::<Vec<_>>()}}),
            "SendMessage" => {
                let variables = &body["variables"];
                let message = json!({"__typename":"Message","id":format!("m{}", state.messages.len() + 1),"chatId":variables["chatId"],"text":variables["text"],"at":variables["at"]});
                if commits { state.messages.push((account.clone(), message.clone())); }
                json!({"data":{"sendMessage":message}})
            }
            "MessageForward" => {
                let input = &body["variables"]["input"];
                let source = state.messages.iter().find(|(owner, message)| *owner == account && message["id"] == input["messageId"]).map(|(_, message)| message.clone());
                let text = source.as_ref().map_or(Value::Null, |message| message["text"].clone());
                let author = source.as_ref().map_or(Value::Null, |message| message["authorId"].clone());
                let message = json!({"__typename":"Message","id":"forwarded-1","chatId":input["chatId"],"text":text,"at":99,"authorId":"user","isForwarded":true,"forwardedFromUserId":author});
                if commits { state.messages.push((account.clone(), message.clone())); }
                json!({"data":{"messageForward":{"message":message,"chat":{"id":input["chatId"],"updatedAt":"2026-11-04T00:00:00Z"}}}})
            }
            "ProfileUpdate" => {
                if commits { if let Some(name) = body["variables"].get("name") { state.profile.insert("name".into(), name.clone()); } }
                json!({"data":{"profileUpdate":{"ok":true}}})
            }
            "AnswerConnection" => {
                let variables = &body["variables"];
                json!({"data":{"answerConnection":{"user":{"__typename":"User","id":variables["userId"],"connectionStatus":"CONNECTED"},"chat":{"__typename":"Chat","id":variables["chatId"],"connectionStatus":"CONNECTED"}}}})
            }
            "BlockUser" | "UnblockUser" => {
                let id = body["variables"]["id"].as_str().unwrap_or_default().to_string();
                if commits {
                    if operation == "BlockUser" { state.blocked.push(id.clone()); } else { state.blocked.retain(|blocked| *blocked != id); }
                }
                json!({"data":{"blockUser":{"id":id}}})
            }
            "BlockedUsers" => json!({"data":{"blockedUsers":state.blocked.iter().map(|id| json!({"__typename":"User","id":id,"name":id})).collect::<Vec<_>>()}}),
            "MomentReact" | "MomentReadCompass" | "MomentCreate" | "MomentDelete" => json!({"data":{"moment":{"__typename":"Moment","id":"m1"}}}),
            "SendGift" | "ChatSearch" => json!({"data":{"sendGift":null,"chatSearch":[]}}),
            "ChatUpdate" => json!({"data":{"chatUpdate":{"__typename":"Chat","id":body["variables"]["id"],"name":body["variables"]["name"]}}}),
            "ChatPin" => json!({"data":{"chatPin":{"__typename":"Chat","id":body["variables"]["id"],"pinned":body["variables"]["pinned"]}}}),
            "ChatMute" => json!({"data":{"chatMute":{"__typename":"Chat","id":body["variables"]["id"],"muted":body["variables"]["muted"]}}}),
            "ChatMarkRead" => json!({"data":{"chatMarkRead":{"__typename":"Chat","id":body["variables"]["id"],"unreadCount":0}}}),
            "MessageEdit" => {
                let variables = &body["variables"];
                let found = state.messages.iter().find(|(_, message)| message["id"] == variables["id"]).map(|(_, message)| message.clone()).unwrap_or(json!({}));
                json!({"data":{"messageEdit":{"__typename":"Message","id":variables["id"],"chatId":found["chatId"],"text":variables["text"],"at":found["at"]}}})
            }
            "MessageDelete" => json!({"data":{"messageDelete":{"id":body["variables"]["id"]}}}),
            "MessageReact" => json!({"data":{"messageReact":{"__typename":"Message","id":"m1","chatId":"c1","text":"hello","at":1}}}),
            "DeleteGroup" => json!({"data":{"deleteGroup":{"id":body["variables"]["id"]}}}),
            "CreateGroup" => {
                let variables = &body["variables"];
                let group = json!({"__typename":"Group","id":format!("g{}", state.messages.len() + 1),"name":variables["name"]});
                json!({"data":{"createGroup":group}})
            }
            "GroupMembers" => {
                let order = body["variables"]["order"].as_str().unwrap_or_default();
                let members: Vec<Value> = state.group_members.get(order).into_iter().flatten().map(|id| json!({"__typename":"Member","id":id,"name":id})).collect();
                json!({"data":{"groupMembers":members}})
            }
            "LeaveGroup" | "AddMember" | "RemoveMember" => {
                let (root, left, membership) = match operation {
                    "LeaveGroup" => ("leaveGroup", json!(true), Value::Null),
                    "AddMember" => ("addMember", json!(false), json!("added")),
                    _ => ("removeMember", json!(false), json!("removed")),
                };
                json!({"data":{ root: {"__typename":"Group","id":body["variables"]["groupId"],"name":"friends","left":left,"membership":membership}}})
            }
            "CreateBlob" | "InitMultipart" | "PresignParts" | "CompleteMultipart" | "AbortMultipart" => {
                let number = state.calls.get(operation).copied().unwrap_or(0) + 1;
                match operation {
                    "CreateBlob" => json!({"data":{"createBlob":{"url":format!("https://put.invalid/{number}"),"headers":[],"signedId":format!("sig{number}"),"blobId":format!("blob{number}")}}}),
                    "InitMultipart" => json!({"data":{"initMultipart":{"key":"k1","uploadId":"u1"}}}),
                    "PresignParts" => {
                        let parts: Vec<Value> = body["variables"]["partNumbers"].as_array().cloned().unwrap_or_default().into_iter().map(|part| json!({"partNumber":part,"url":format!("https://put.invalid/part/{part}"),"headers":[]})).collect();
                        json!({"data":{"presignParts":{"parts":parts}}})
                    }
                    "CompleteMultipart" => json!({"data":{"completeMultipart":{"signedId":"sigm","blobId":"blobm"}}}),
                    _ => json!({"data":{"abortMultipart":{"ok":true}}}),
                }
            }
            "ChatUpdatePhoto" => {
                let variables = &body["variables"];
                json!({"data":{"chatUpdatePhoto":{"__typename":"Chat","id":variables["id"],"logoUrl":format!("https://cdn/logo-{}", variables["signedId"].as_str().unwrap_or_default())}}})
            }
            "PUT" => json!({}),
            other => panic!("unexpected operation {other}"),
        };
        if commits && answer.get("data").is_some() {
            *state.calls.entry(operation.into()).or_default() += 1;
        }
        Some((answer, fault))
    }

    async fn plain(&self, operation: &str, body: &Value) -> Result<String, DbError> {
        let Some((answer, fault)) = self.enter(operation, None, None, body) else {
            return Err(DbError::Http { status: 400, detail: format!("forbidden {operation}") });
        };
        match pass_gate(fault).await {
            Some(Fault::Http(status)) => return Err(DbError::Http { status, detail: "refused".into() }),
            Some(Fault::ResponseTooLarge(limit)) => return Err(DbError::ResponseTooLarge { limit }),
            Some(Fault::AppliedResponseTooLarge(limit)) => return Err(DbError::ResponseTooLarge { limit }),
            Some(Fault::Business) => return Ok(json!({"errors": [{"message": "refused", "extensions": {"code": "REFUSED"}}]}).to_string()),
            Some(Fault::Answer(answer)) => return Ok(answer.to_string()),
            Some(Fault::Unreadable) => return Ok("not json".into()),
            Some(Fault::Refuse { code, .. }) => return Ok(json!({"errors": [{"message": code, "extensions": {"code": code}}]}).to_string()),
            Some(Fault::Down) => return Err(DbError::Network { detail: "the network is away".into() }),
            Some(Fault::Hold(_) | Fault::HoldThen(..)) | None => {}
        }
        Ok(answer.to_string())
    }
}

#[async_trait]
impl FakeHttp for World {
    async fn post(&self, path: String, body: String, _: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let request = decrypt_request(&body).unwrap();
        let sequence = request["sequence"].as_i64();
        let operation = match path.as_str() {
            "/api/auth/login_link" => "login_link".to_string(),
            "/api/auth/otp" => "otp".to_string(),
            "/api/logout" => "logout".to_string(),
            _ => request["body"]["operationName"].as_str().unwrap().to_string(),
        };
        let contract_request = Request::from_wire(&self.label, &request);
        if matches!(operation.as_str(), "login_link" | "social") && sequence == Some(1) {
            let credential = request["credential"].as_str().unwrap_or_default().to_string();
            let mut state = self.state.lock().unwrap();
            if state.credential != credential { state.credential = credential; state.contract.set_last(0); }
        }
        let admission = self.state.lock().unwrap().contract.admit(&contract_request);
        match admission {
            Admission::Stored(stored) => {
                let mut state = self.state.lock().unwrap();
                let account = state.account.clone();
                let marks_before = state.marks.len();
                state.log.push(Entry { operation: operation.clone(), account, label: self.label.clone(), sequence, request_id: Some(contract_request.request_id.clone()), body: request["body"].to_string(), marks_before });
                return Ok(EncryptedReply { status: 200, body: seal_response(&request, sequence, stored) });
            }
            Admission::TwoBodies => panic!("the pair {} went with two bodies", contract_request.request_id),
            Admission::Refused(refusal) => return Ok(EncryptedReply { status: 200, body: seal_response(&request, None, refusal) }),
            Admission::Fresh => {}
        }
        let Some((mut answer, fault)) = self.enter(&operation, sequence, Some(contract_request.request_id.clone()), &request["body"]) else {
            return Ok(EncryptedReply { status: 400, body: String::new() });
        };
        let mut sealed_sequence = sequence;
        match pass_gate(fault).await {
            Some(Fault::Http(status)) => return Ok(EncryptedReply { status, body: String::new() }),
            Some(Fault::ResponseTooLarge(limit)) => return Err(DbError::ResponseTooLarge { limit }),
            Some(Fault::AppliedResponseTooLarge(limit)) => {
                let mut state = self.state.lock().unwrap();
                state.contract.settle(&contract_request, answer.clone());
                *state.calls.entry(operation.clone()).or_default() += 1;
                return Err(DbError::ResponseTooLarge { limit });
            }
            Some(Fault::Business) => answer = json!({"errors": [{"message": "refused", "extensions": {"code": "REFUSED"}}]}),
            Some(Fault::Answer(value)) => answer = value,
            Some(Fault::Unreadable) => return Ok(EncryptedReply { status: 200, body: "not a sealed answer".into() }),
            Some(Fault::Down) => return Err(DbError::Network { detail: "the network is away".into() }),
            Some(Fault::Refuse { code, confirmed }) => {
                answer = json!({"error": code});
                if !confirmed { sealed_sequence = None; }
            }
            Some(Fault::Hold(_) | Fault::HoldThen(..)) | None => {}
        }
        if sealed_sequence == sequence { self.state.lock().unwrap().contract.settle(&contract_request, answer.clone()); }
        Ok(EncryptedReply { status: 200, body: seal_response(&request, sealed_sequence, answer) })
    }
}

/// The transport of a test harness: a target that the tests set and unset. An unset target throws, as the harness of the app does.
pub(super) struct Switchable {
    target: Mutex<Option<Arc<World>>>,
    invalid: bool,
    pub(super) events: super::rig_events::Events,
}

impl Switchable {
    pub(super) fn new(target: Option<Arc<World>>, invalid: bool) -> Arc<Self> {
        Arc::new(Self { target: Mutex::new(target), invalid, events: super::rig_events::Events::new() })
    }
    pub(super) fn set(&self, target: Option<Arc<World>>) { *self.target.lock().unwrap() = target; }
    fn current(&self) -> Result<Arc<World>, DbError> {
        self.target.lock().unwrap().clone().ok_or_else(|| match self.invalid {
            true => DbError::Invalid { detail: "No test transport".into() },
            false => DbError::Interrupted { detail: "transport unavailable during reset".into() },
        })
    }
}

#[async_trait]
impl FakeTransport for Switchable {
    fn cable(&self) -> Option<&crate::transport::encrypted::cable::tests::peer_tests::CablePeer> { Some(&self.events.peer) }
    async fn query(&self, request: TransportRequest) -> Result<String, DbError> { self.current()?.plain(&request.operation_name, &json!({"variables": serde_json::from_str::<Value>(&request.variables_json)?})).await }
    async fn mutate(&self, request: TransportRequest) -> Result<String, DbError> { self.current()?.plain(&request.operation_name, &json!({"variables": serde_json::from_str::<Value>(&request.variables_json)?})).await }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        self.current()?.plain("PUT", &Value::Null).await?;
        Ok(PutReply { status: 200, etag: Some("etag".into()) })
    }
}

pub(super) struct Rig {
    pub(super) opener: Arc<OpenDatabase>,
    pub(super) database: Arc<Database>,
    pub(super) world: Arc<World>,
    pub(super) switch: Arc<Switchable>,
    pub(super) clock: Arc<TestClock>,
}


fn config() -> Config { Config { path: None } }

impl Rig {
    /// The app path: the real EncryptedClient, only the HTTP exchange is the fake world.
    pub(super) async fn encrypted() -> Rig {
        Self::encrypted_with_member_page(false).await
    }

    pub(super) async fn encrypted_with_member_page(page: bool) -> Rig {
        Self::encrypted_with_options(page, false).await
    }

    pub(super) async fn encrypted_with_options(page: bool, keep_message: bool) -> Rig {
        Self::encrypted_for_reactions(page, keep_message, false).await
    }

    pub(super) async fn encrypted_for_reactions(page: bool, keep_message: bool, reactions: bool) -> Rig {
        Self::encrypted_edited(page, keep_message, reactions, |_| {}).await
    }

    /// The app path with the models changed by `edit` before the database opens.
    pub(super) async fn encrypted_edited(page: bool, keep_message: bool, reactions: bool, edit: impl FnOnce(&mut Value)) -> Rig {
        let world = World::new("server", "initial", false);
        let switch = Switchable::new(Some(world.clone()), false);
        let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
        encrypted.endpoints.session_operations.insert("login_link".into(), EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: crate::transport::EncryptedOperationClass::Authenticated });
        encrypted.endpoints.session_operations.insert("social".into(), EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: crate::transport::EncryptedOperationClass::Authenticated });
        encrypted.endpoints.session_operations.insert("otp".into(), EncryptedSessionOperation { path: "/api/auth/otp".into(), class: crate::transport::EncryptedOperationClass::Sessionless });
        let clock = TestClock::new(1_000_000);
        let opener = OpenDatabase::new();
        let mut models: Value = serde_json::from_str(&plain_models()).unwrap();
        if page {
            let member = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Member").unwrap();
            let relation = member["relations"].as_array_mut().unwrap().iter_mut().find(|relation| relation["name"] == "group_members").unwrap();
            relation["remote"]["shape"]["coverage"] = json!("page");
        }
        if keep_message {
            let message = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Message").unwrap();
            let create = message["actions"].as_array_mut().unwrap().iter_mut().find(|action| action["name"] == "create").unwrap();
            create["on_failure"] = json!("keep");
        }
        if reactions { add_reactions(&mut models); }
        edit(&mut models);
        let database = opener
            .open_with_clock(config(), crate::ffi::specs(models.to_string()), crate::ffi::OpenPorts { partial_operations: true, transport: Some(switch.clone()), encrypted: Some(encrypted), credentials: Some(Arc::new(WatchedHeld::default())), http: Some(world.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, clock.clone(), crate::ffi::OpenOptions::default(), None)
            .await
            .unwrap();
        database.register_operations(plain_operations()).await.unwrap();
        database.set_online(true).await.unwrap();
        Rig { opener, database, world, switch, clock }
    }

    /// A signed-in database with the reaction model on it.
    pub(super) async fn plain_for_reactions(first: Arc<World>) -> Rig {
        Self::signed_as(first, false, add_reactions).await
    }

    /// The harness path of the apps: a plain transport with a switchable target and no session client. It carries no mutation of a server model.
    pub(super) async fn bare(first: Arc<World>, invalid: bool) -> Rig {
        let switch = Switchable::new(Some(first.clone()), invalid);
        let clock = TestClock::new(1_000_000);
        let opener = OpenDatabase::new();
        let database = opener.open_with_clock(config(), crate::ffi::specs(plain_models()), crate::ffi::OpenPorts { partial_operations: true, transport: Some(switch.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, clock.clone(), crate::ffi::OpenOptions::default(), None).await.unwrap();
        database.register_operations(plain_operations()).await.unwrap();
        database.set_online(true).await.unwrap();
        Rig { opener, database, world: first, switch, clock }
    }

    /// A signed-in database on the world `first`.
    pub(super) async fn plain(first: Arc<World>, invalid: bool) -> Rig {
        Self::plain_with_create_policy(first, invalid, false).await
    }

    pub(super) async fn plain_with_create_policy(first: Arc<World>, invalid: bool, keep: bool) -> Rig {
        Self::plain_with_member_events(first, invalid, keep, false).await
    }

    pub(super) async fn plain_with_member_events(first: Arc<World>, invalid: bool, keep: bool, events: bool) -> Rig {
        Self::plain_with_group_destroy(first, invalid, keep, events, false).await
    }

    pub(super) async fn plain_with_group_destroy(first: Arc<World>, invalid: bool, keep: bool, events: bool, destroy: bool) -> Rig {
        Self::signed_as(first, invalid, move |models| {
            if keep {
                let group = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Group").unwrap();
                let create = group["actions"].as_array_mut().unwrap().iter_mut().find(|action| action["name"] == "create").unwrap();
                create["on_failure"] = json!("keep");
            }
            if events {
                let member = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Member").unwrap();
                let relation = member["relations"].as_array_mut().unwrap().iter_mut().find(|relation| relation["name"] == "group_members").unwrap();
                relation["updates"] = json!([{"subscription":"MemberChanged","set":{}}]);
            }
            if destroy {
                let group = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Group").unwrap();
                let mut action = crate::schema::ActionDeclaration::new("destroy", crate::schema::RootShape::Destroy);
                action.mutation = Some("DeleteGroup".into());
                action.variables.insert("id".into(), "id".into());
                group["actions"].as_array_mut().unwrap().push(serde_json::to_value(action).unwrap());
            }
        }).await
    }

    /// The app path with an existing world: a real session client on the fake HTTP of `first`, signed in as its account. `edit` changes the models before the database opens.
    pub(super) async fn signed_as(first: Arc<World>, invalid: bool, edit: impl FnOnce(&mut Value)) -> Rig {
        let switch = Switchable::new(Some(first.clone()), invalid);
        let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
        encrypted.endpoints.session_operations.insert("login_link".into(), EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: crate::transport::EncryptedOperationClass::Authenticated });
        let clock = TestClock::new(1_000_000);
        let opener = OpenDatabase::new();
        let mut models: Value = serde_json::from_str(&plain_models()).unwrap();
        edit(&mut models);
        let database = opener.open_with_clock(config(), crate::ffi::specs(models.to_string()), crate::ffi::OpenPorts { partial_operations: true, transport: Some(switch.clone()), encrypted: Some(encrypted), credentials: Some(Arc::new(WatchedHeld::default())), http: Some(first.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, clock.clone(), crate::ffi::OpenOptions::default(), None).await.unwrap();
        database.register_operations(plain_operations()).await.unwrap();
        database.set_online(true).await.unwrap();
        bound(database.session_call("login_link".into(), json!({"account": first.account()}).to_string())).await.unwrap();
        Rig { opener, database, world: first, switch, clock }
    }

    /// A test ends and the next one starts: unset the target, close the database, set the next target, open a new database.
    pub(super) async fn harness_cycle(&mut self, next: Option<Arc<World>>) {
        self.switch.set(None);
        bound(self.database.close_database()).await.unwrap();
        self.switch.set(next);
        let database = self.opener.open_with_clock(config(), crate::ffi::specs(plain_models()), crate::ffi::OpenPorts { partial_operations: true, transport: Some(self.switch.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, self.clock.clone(), crate::ffi::OpenOptions::default(), None).await.unwrap();
        database.register_operations(plain_operations()).await.unwrap();
        database.set_online(true).await.unwrap();
        self.database = database;
    }

    pub(super) async fn login_ios(&self, account: &str) {
        let database = &self.database;
        bound(database.session_call("login_link".into(), json!({"account":account}).to_string())).await.unwrap();
        bound(database.run(database.session_epoch(), "SessionProfile".into(), "{}".into())).await.unwrap();
        bound(database.refresh(database.session_epoch(), "User".into(), Some("profile".into()), "{}".into())).await.unwrap();
    }

    pub(super) async fn login_android(&self, account: &str) {
        let database = &self.database;
        bound(database.session_call("social".into(), json!({"account":account}).to_string())).await.unwrap();
        bound(database.refresh(database.session_epoch(), "User".into(), Some("profile".into()), "{}".into())).await.unwrap();
    }

    /// The exit of the app. `revoke`: unregister the device and the confirmed server logout come first. `cancel`: the Android app cancels its collectors after the session reset, the iOS app keeps them held.
    pub(super) async fn exit(&self, held: &Held, revoke: bool, cancel: bool) {
        let database = &self.database;
        if revoke {
            bound(database.run(database.session_epoch(), "UnregisterDevice".into(), "{}".into())).await.unwrap();
            bound(database.session_call("logout".into(), "{}".into())).await.unwrap();
            assert!(!self.world.signed(), "the server did not confirm the logout");
        }
        database.session_reset().await.unwrap();
        if cancel { held.cancel_streams(); }
    }

    /// A second open through the opener that already owns an open database.
    pub(super) async fn open_again(&self, transport: Option<Arc<dyn Transport>>) -> Result<Arc<Database>, DbError> {
        self.opener.open_with_clock(config(), crate::ffi::specs(plain_models()), crate::ffi::OpenPorts { partial_operations: true, transport, endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, TestClock::new(1_000_000), crate::ffi::OpenOptions::default(), None).await
    }

    pub(super) async fn settle(&self) { bound(crate::ffi::settled(&self.database)).await.unwrap(); }

}

impl Rig {
    /// Spawns a call of the test that the barrier counts from its spawn until it waits for the outside.
    pub(super) fn spawn<T: Send + 'static>(&self, work: impl std::future::Future<Output = T> + Send + 'static) -> tokio::task::JoinHandle<T> {
        tokio::spawn(crate::runtime::counted(Some(self.database.life.launch()), work))
    }
}

impl crate::test_wait::Quiet for Rig {
    async fn quiet(&self) { crate::test_wait::Quiet::quiet(&self.database).await }
}
