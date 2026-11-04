use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{Notify, Semaphore};

use crate::ffi::lifecycle_tests::{models, named, Rows};
use crate::ffi::session_guards_tests::WatchedHeld;
use crate::ffi::{
    ByteProgress, CableLease, Config, Database, DbError, EncryptedHttp, EncryptedReply, EncryptedSessionOperation, EncryptedSocketEvent, EntityFetch, OpenDatabase, PutReply, SessionCode, StreamRequest,
    SubscriptionSink, TestClock, Transport, TransportRequest,
};
use crate::ffi::stream::tape::Watched;
use crate::test_wait::{within_secs, yield_until};
use crate::transport::encrypted::fake_server_tests::{decrypt_request, seal_response};

/// Every call of a sequence returns within this many seconds or the test fails instead of hanging.
pub(super) const BOUND: u64 = 20;

/// Like `bound`, and the panic names the step that did not return.
pub(super) async fn named_bound<T>(step: &str, work: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(BOUND), work).await.unwrap_or_else(|_| panic!("{step}: did not return within {BOUND} seconds"))
}

pub(super) async fn bound<T>(work: impl std::future::Future<Output = T>) -> T { within_secs(BOUND, work).await }

pub(super) struct Gate {
    account: Mutex<Option<String>>,
    entered: Notify,
    release: Semaphore,
}

impl Gate {
    pub(super) fn new() -> Arc<Self> { Arc::new(Self { account: Mutex::new(None), entered: Notify::new(), release: Semaphore::new(0) }) }
    pub(super) async fn pass(&self) {
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
    }
    pub(super) async fn entered(&self) { within_secs(BOUND, self.entered.notified()).await }
    /// Waits for the request to reach the server and names the case when it never does.
    pub(super) async fn entered_for(&self, case: &str) {
        let notified = self.entered.notified();
        tokio::time::timeout(std::time::Duration::from_secs(BOUND), notified).await.unwrap_or_else(|_| panic!("{case}: the request never reached the server"));
    }
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
    /// The request does not reach the server: the caller sees a network error.
    Down,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Entry {
    pub(super) operation: String,
    pub(super) account: String,
    pub(super) label: String,
    pub(super) sequence: Option<i64>,
}

#[derive(Default)]
struct State {
    account: String,
    signed: bool,
    forbidden: bool,
    calls: BTreeMap<String, usize>,
    violations: Vec<String>,
    log: Vec<Entry>,
    script: Vec<(String, Fault)>,
    messages: Vec<(String, Value)>,
    group_members: BTreeMap<String, Vec<String>>,
}

/// A held request first waits at its gate, then ends in the inner fault, if any.
async fn pass_gate(fault: Option<Fault>) -> Option<Fault> {
    match fault {
        Some(Fault::Hold(gate)) => { gate.pass().await; None }
        Some(Fault::HoldThen(gate, inner)) => { gate.pass().await; Some(*inner) }
        other => other,
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
    pub(super) fn signed(&self) -> bool { self.state.lock().unwrap().signed }
    pub(super) fn forbid(&self, forbidden: bool) { self.state.lock().unwrap().forbidden = forbidden; }
    pub(super) fn violations(&self) -> Vec<String> { self.state.lock().unwrap().violations.clone() }
    pub(super) fn count(&self, operation: &str) -> usize { self.state.lock().unwrap().calls.get(operation).copied().unwrap_or(0) }
    pub(super) fn log(&self) -> Vec<Entry> { self.state.lock().unwrap().log.clone() }
    pub(super) fn hits(&self) -> usize { self.state.lock().unwrap().log.len() }
    /// A message of the thread `c1` that the server already holds for this account.
    pub(super) fn seed_message(&self, account: &str, id: &str, text: &str, at: i64) {
        self.state.lock().unwrap().messages.push((account.into(), json!({"__typename":"Message","id":id,"chatId":"c1","text":text,"at":at})));
    }
    pub(super) fn set_members(&self, order: &str, ids: &[&str]) {
        self.state.lock().unwrap().group_members.insert(order.into(), ids.iter().map(|id| id.to_string()).collect());
    }
    pub(super) fn fail_next(&self, operation: &str, fault: Fault) { self.state.lock().unwrap().script.push((operation.into(), fault)); }
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

    fn enter(&self, operation: &str, sequence: Option<i64>, body: &Value) -> Option<(Value, Option<Fault>)> {
        let mut state = self.state.lock().unwrap();
        if state.forbidden {
            let frames = std::backtrace::Backtrace::force_capture().to_string().lines().filter(|line| line.contains("dblayer::sync") || line.contains("dblayer::ffi") || line.contains("dblayer::transport")).take(8).map(|line| line.trim().to_string()).collect::<Vec<_>>().join(" | ");
            let detail = format!("{} {operation} via {frames}", self.label);
            state.violations.push(detail);
            return None;
        }
        let account = state.account.clone();
        state.log.push(Entry { operation: operation.into(), account: account.clone(), label: self.label.clone(), sequence });
        let position = state.script.iter().position(|(wanted, _)| wanted == operation || wanted == "*");
        let fault = position.map(|index| state.script.remove(index).1);
        let commits = matches!(fault, None | Some(Fault::Hold(_)));
        let held_account = match &fault {
            Some(Fault::Hold(gate) | Fault::HoldThen(gate, _)) => gate.account(),
            _ => None,
        };
        let account = held_account.unwrap_or(account);
        let row = json!({"__typename":"User","id":"user","name":account});
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
            _ if !state.signed => json!({"error": "UNAUTHENTICATED"}),
            "SessionProfile" => json!({"data":{"sessionProfile":{"id":account}}}),
            "UserNode" => json!({"data":{"node":row}}),
            "MyProfile" => json!({"data":{"me":row}}),
            "Users" => json!({"data":{"users":[row]}}),
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
                let message = json!({"__typename":"Message","id":"forwarded-1","chatId":input["chatId"],"text":text,"at":99});
                if commits { state.messages.push((account.clone(), message.clone())); }
                json!({"data":{"messageForward":{"message":message,"chat":{"id":input["chatId"],"updatedAt":"2026-11-04T00:00:00Z"}}}})
            }
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
            "PUT" => json!({}),
            other if other.starts_with("subscribe:") => json!({}),
            other => panic!("unexpected operation {other}"),
        };
        if commits && answer.get("data").is_some() {
            *state.calls.entry(operation.into()).or_default() += 1;
        }
        Some((answer, fault))
    }

    async fn plain(&self, operation: &str) -> Result<String, DbError> {
        let Some((answer, fault)) = self.enter(operation, None, &Value::Null) else {
            return Err(DbError::Http { status: 400, detail: format!("forbidden {operation}") });
        };
        match pass_gate(fault).await {
            Some(Fault::Refuse { code, .. }) => return Err(DbError::Session { code: SessionCode::Unauthenticated, detail: code.into() }),
            Some(Fault::Down) => return Err(DbError::Network { detail: "the network is away".into() }),
            Some(Fault::Hold(_) | Fault::HoldThen(..)) | None => {}
        }
        Ok(answer.to_string())
    }
}

#[async_trait]
impl EncryptedHttp for World {
    async fn post(&self, path: String, body: String, _: bool) -> Result<EncryptedReply, DbError> {
        let request = decrypt_request(&body).unwrap();
        let sequence = request["sequence"].as_i64();
        let operation = match path.as_str() {
            "/api/auth/login_link" => "login_link".to_string(),
            "/api/auth/otp" => "otp".to_string(),
            "/api/logout" => "logout".to_string(),
            _ => request["body"]["operationName"].as_str().unwrap().to_string(),
        };
        let Some((mut answer, fault)) = self.enter(&operation, sequence, &request["body"]) else {
            return Ok(EncryptedReply { status: 400, body: String::new() });
        };
        let mut sealed_sequence = sequence;
        match pass_gate(fault).await {
            Some(Fault::Down) => return Err(DbError::Network { detail: "the network is away".into() }),
            Some(Fault::Refuse { code, confirmed }) => {
                answer = json!({"error": code});
                if !confirmed { sealed_sequence = None; }
            }
            Some(Fault::Hold(_) | Fault::HoldThen(..)) | None => {}
        }
        Ok(EncryptedReply { status: 200, body: seal_response(&request, sealed_sequence, answer) })
    }
}

/// The transport of a test harness: a target that the tests set and unset. An unset target throws, as the harness of the app does.
pub(super) struct Switchable {
    target: Mutex<Option<Arc<World>>>,
    invalid: bool,
    tokens: AtomicUsize,
    attempts: AtomicUsize,
    failures: Mutex<Vec<String>>,
    sinks: Mutex<Vec<(String, Arc<SubscriptionSink>)>>,
    unsubscribed: Mutex<Vec<String>>,
}

impl Switchable {
    pub(super) fn new(target: Option<Arc<World>>, invalid: bool) -> Arc<Self> {
        Arc::new(Self { target: Mutex::new(target), invalid, tokens: AtomicUsize::new(0), attempts: AtomicUsize::new(0), failures: Mutex::new(Vec::new()), sinks: Mutex::new(Vec::new()), unsubscribed: Mutex::new(Vec::new()) })
    }
    pub(super) fn set(&self, target: Option<Arc<World>>) { *self.target.lock().unwrap() = target; }
    pub(super) fn unsubscribed(&self) -> Vec<String> { self.unsubscribed.lock().unwrap().clone() }
    pub(super) fn failures(&self) -> Vec<String> { self.failures.lock().unwrap().clone() }
    pub(super) fn attempts(&self) -> usize { self.attempts.load(Ordering::SeqCst) }
    pub(super) fn opened(&self) -> usize { self.tokens.load(Ordering::SeqCst) }
    fn current(&self) -> Result<Arc<World>, DbError> {
        self.target.lock().unwrap().clone().ok_or_else(|| match self.invalid {
            true => DbError::Invalid { detail: "No test transport".into() },
            false => DbError::Interrupted { detail: "transport unavailable during reset".into() },
        })
    }
}

#[async_trait]
impl Transport for Switchable {
    async fn open_cable(&self, _: Arc<CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { None }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, request: TransportRequest) -> Result<String, DbError> { self.current()?.plain(&request.operation_name).await }
    async fn mutate(&self, request: TransportRequest) -> Result<String, DbError> { self.current()?.plain(&request.operation_name).await }
    async fn subscribe(&self, request: TransportRequest, sink: Arc<SubscriptionSink>) -> Result<(), DbError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let opened = async {
            let world = self.current()?;
            world.plain(&format!("subscribe:{}", request.operation_name)).await?;
            let token = format!("{}-{}", world.label, self.tokens.fetch_add(1, Ordering::SeqCst));
            sink.opened(token.clone())?;
            sink.ready();
            self.sinks.lock().unwrap().push((token, sink));
            Ok(())
        };
        let result: Result<(), DbError> = opened.await;
        if let Err(error) = &result { self.failures.lock().unwrap().push(format!("{error:?}")); }
        result
    }
    fn unsubscribe(&self, token: String) { self.unsubscribed.lock().unwrap().push(token); }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: String, _: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        self.current()?.plain("PUT").await?;
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

const OPERATIONS: &str = r#"[
    {"name":"MyProfile","document":"query MyProfile { me { id name } }"},
    {"name":"UserNode","document":"query UserNode($id: ID!) { node(id: $id) { ... on User { id name } } }"},
    {"name":"Users","document":"query Users { users { id name } }"},
    {"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"},
    {"name":"UnregisterDevice","document":"mutation UnregisterDevice { unregisterDevice { ok } }"},
    {"name":"Thread","document":"query Thread($chatId: ID!) { thread(chatId: $chatId) { id chatId text at } }"},
    {"name":"SendMessage","document":"mutation SendMessage($chatId: ID!, $text: String!, $at: Int!) { sendMessage(chatId: $chatId, text: $text, at: $at) { id chatId text at } }"},
    {"name":"CreateGroup","document":"mutation CreateGroup($name: String!) { createGroup(name: $name) { id name } }"},
    {"name":"LeaveGroup","document":"mutation LeaveGroup($groupId: ID!) { leaveGroup(groupId: $groupId) { id name left membership } }"},
    {"name":"AddMember","document":"mutation AddMember($groupId: ID!, $userIds: [ID!]!) { addMember(groupId: $groupId, userIds: $userIds) { id name left membership } }"},
    {"name":"RemoveMember","document":"mutation RemoveMember($groupId: ID!, $userIds: [ID!]!) { removeMember(groupId: $groupId, userIds: $userIds) { id name left membership } }"},
    {"name":"GroupMembers","document":"query GroupMembers($groupId: ID!, $order: String!) { groupMembers(groupId: $groupId, order: $order) { id name } }"},
    {"name":"MessageForward","document":"mutation MessageForward($input: MessageForwardInput!) { messageForward(input: $input) { message { id chatId text at } chat { id updatedAt } } }"}
]"#;

/// The plain harness also holds a memory model that a server subscription feeds.
fn plain_models() -> String {
    let mut models: Vec<crate::schema::ModelSpec> = serde_json::from_str(&models()).unwrap();
    let mut notice = crate::schema::ModelSpec::new("Notice", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id)]);
    notice.storage = crate::schema::Storage::Memory;
    let mut relation = crate::schema::RelationSpec::global("recent");
    relation.updates.push(crate::schema::UpdatesSpec { apply: None, subscription: "Notices".into(), deleted_by: None, deleted_unless: None, set: serde_json::Map::new(), removes: false, debounce: None, root: None, id: Vec::new() });
    notice.relations.push(relation);
    models.push(notice);
    models.push(message_spec());
    models.push(group_spec());
    models.push(member_spec());
    serde_json::to_string(&models).unwrap()
}

/// The chat message of the bridge sequences: a thread read from the server and a create that works offline.
fn message_spec() -> crate::schema::ModelSpec {
    use crate::schema::{ActionDeclaration, Coverage, FieldSpec, RelationSpec, RemoteShape, RootShape};
    let mut message = crate::schema::ModelSpec::new("Message", vec![
        FieldSpec::scalar("id", crate::value::ScalarKind::Id),
        FieldSpec::scalar("chatId", crate::value::ScalarKind::Id).indexed(),
        FieldSpec::scalar("text", crate::value::ScalarKind::Str),
        FieldSpec::scalar("at", crate::value::ScalarKind::Int),
    ]);
    let mut thread = RelationSpec::new("thread", &["chatId"]);
    thread.remote = Some(crate::sync::rig::remote("Thread", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    message.relations.push(thread);
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("SendMessage".into());
    create.durable = true;
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    for field in ["chatId", "text", "at"] { create.variables.insert(field.into(), field.into()); }
    message.actions.push(create);
    let mut forward = ActionDeclaration::new("forward", RootShape::Upsert);
    forward.mutation = Some("MessageForward".into());
    forward.args.insert("targetChatId".into(), "string".into());
    forward.variables.insert("messageId".into(), "id".into());
    forward.variables.insert("chatId".into(), "targetChatId".into());
    forward.wrap = Some("input".into());
    forward.root = Some("messageForward.message".into());
    forward.copies_receiver_to = Some(crate::schema::ReceiverCopy { arg: "targetChatId".into(), field: "chatId".into() });
    message.actions.push(forward);
    message
}

/// A model whose create goes to the server and is not durable: it needs the network and the answer of the server.
fn group_spec() -> crate::schema::ModelSpec {
    use crate::schema::{ActionDeclaration, FieldSpec, RootShape};
    let mut left = FieldSpec::scalar("left", crate::value::ScalarKind::Bool);
    left.nullable = true;
    let mut membership = FieldSpec::scalar("membership", crate::value::ScalarKind::Str);
    membership.nullable = true;
    let mut group = crate::schema::ModelSpec::new("Group", vec![FieldSpec::scalar("id", crate::value::ScalarKind::Id), FieldSpec::scalar("name", crate::value::ScalarKind::Str), left, membership]);
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("CreateGroup".into());
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    create.variables.insert("name".into(), "name".into());
    group.actions.push(create);
    group.relations.push(crate::schema::RelationSpec::global("all"));
    let mut leave = ActionDeclaration::new("leave", RootShape::Upsert);
    leave.mutation = Some("LeaveGroup".into());
    leave.variables.insert("groupId".into(), "id".into());
    leave.ignore_response = true;
    leave.removes_receiver = true;
    group.actions.push(leave);
    for (name, mutation, root, kind) in [("add_member", "AddMember", "addMember", crate::schema::ScopeEffectKind::Join), ("remove_member", "RemoveMember", "removeMember", crate::schema::ScopeEffectKind::Leave)] {
        let mut action = ActionDeclaration::new(name, RootShape::Upsert);
        action.mutation = Some(mutation.into());
        action.args.insert("userIds".into(), "ids".into());
        action.variables.insert("groupId".into(), "id".into());
        action.variables.insert("userIds".into(), "userIds".into());
        action.root = Some(root.into());
        action.refetch.push("Member.group_members".into());
        action.scope_effects.push(crate::schema::ScopeEffect {
            kind,
            model: "Member".into(),
            scope: "group_members".into(),
            rows: "userIds".into(),
            with: [("groupId".to_string(), "id".to_string())].into_iter().collect(),
            order_param: "order".into(),
            orders: vec!["JOINED".into(), "OWNER_FIRST".into()],
        });
        group.actions.push(action);
    }
    group
}

/// The member of a group: two lists of the same rows, one ordered by the join and one with the owner first.
fn member_spec() -> crate::schema::ModelSpec {
    use crate::schema::{FieldSpec, RelationSpec};
    let mut member = crate::schema::ModelSpec::new("Member", vec![FieldSpec::scalar("id", crate::value::ScalarKind::Id), FieldSpec::scalar("name", crate::value::ScalarKind::Str)]);
    let mut members = RelationSpec::new("group_members", &[]);
    members.text_params = vec!["groupId".to_string(), "order".to_string()];
    members.remote = Some(crate::sync::rig::remote("GroupMembers", crate::schema::RemoteShape::List { coverage: crate::schema::Coverage::Complete }, "5.minutes"));
    member.relations.push(members);
    member
}

fn plain_operations() -> String {
    let mut operations: Vec<Value> = serde_json::from_str(OPERATIONS).unwrap();
    operations.push(json!({"name":"Notices","document":"subscription Notices { notices { id } }","rootPath":"notices"}));
    json!(operations).to_string()
}

fn config() -> Config { Config { path: None, data_version: None, cache_rows: 8 } }

impl Rig {
    /// The app path: the real EncryptedClient, only the HTTP exchange is the fake world.
    pub(super) async fn encrypted() -> Rig {
        let world = World::new("server", "initial", false);
        let switch = Switchable::new(Some(world.clone()), false);
        let mut encrypted = crate::transport::encrypted::protocol_tests::config();
        encrypted.endpoints.session_operations.insert("login_link".into(), EncryptedSessionOperation { path: "/api/auth/login_link".into(), sessionless: false });
        encrypted.endpoints.session_operations.insert("social".into(), EncryptedSessionOperation { path: "/api/auth/login_link".into(), sessionless: false });
        encrypted.endpoints.session_operations.insert("otp".into(), EncryptedSessionOperation { path: "/api/auth/otp".into(), sessionless: true });
        let clock = TestClock::new(1_000_000);
        let opener = OpenDatabase::new();
        let database = opener
            .open_with_clock(config(), plain_models(), Some(switch.clone()), None, clock.clone(), false, false, Some(encrypted), Some(Arc::new(WatchedHeld::default())), Some(world.clone()), None)
            .await
            .unwrap();
        database.register_operations(plain_operations()).unwrap();
        Rig { opener, database, world, switch, clock }
    }

    /// The harness path of the apps: a plain transport with a switchable target and no session client.
    pub(super) async fn plain(first: Arc<World>, invalid: bool) -> Rig {
        let switch = Switchable::new(Some(first.clone()), invalid);
        let clock = TestClock::new(1_000_000);
        let opener = OpenDatabase::new();
        let database = opener.open_with_clock(config(), plain_models(), Some(switch.clone()), None, clock.clone(), false, false, None, None, None, None).await.unwrap();
        database.register_operations(plain_operations()).unwrap();
        Rig { opener, database, world: first, switch, clock }
    }

    /// A test ends and the next one starts: unset the target, close the database, set the next target, open a new database.
    pub(super) async fn harness_cycle(&mut self, next: Option<Arc<World>>) {
        self.switch.set(None);
        bound(self.database.close_database()).await.unwrap();
        self.switch.set(next);
        let database = self.opener.open_with_clock(config(), plain_models(), Some(self.switch.clone()), None, self.clock.clone(), false, false, None, None, None, None).await.unwrap();
        database.register_operations(plain_operations()).unwrap();
        self.database = database;
    }

    pub(super) async fn login_ios(&self, account: &str) {
        let database = &self.database;
        bound(database.session_call("login_link".into(), json!({"account":account}).to_string())).await.unwrap();
        bound(database.run(database.session_epoch(), "SessionProfile".into(), "{}".into())).await.unwrap();
        bound(database.refresh(database.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.unwrap();
    }

    pub(super) async fn login_android(&self, account: &str) {
        let database = &self.database;
        bound(database.session_call("social".into(), json!({"account":account}).to_string())).await.unwrap();
        bound(database.refresh(database.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.unwrap();
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
        self.opener.open_with_clock(config(), plain_models(), transport, None, TestClock::new(1_000_000), false, false, None, None, None, None).await
    }

    pub(super) async fn settle(&self) { bound(crate::ffi::settled(&self.database)).await.unwrap(); }

}

/// The streams the app holds for the account it shows: the singleton and the id entity (CurrentUser), a list and a second list in another order.
/// A new account gets new streams on the same screens: `follow` opens them for the epoch now, the deliveries of every epoch stay in the same logs.
pub(super) struct Held {
    pub(super) single: Arc<Rows>,
    pub(super) entity: Arc<Rows>,
    pub(super) rows: Arc<Rows>,
    pub(super) window_rows: Arc<Rows>,
    open: Mutex<Vec<Arc<Watched>>>,
    stopped: std::sync::atomic::AtomicBool,
}

impl Held {
    pub(super) fn open(database: &Database) -> Held {
        let held = Held { single: Arc::default(), entity: Arc::default(), rows: Arc::default(), window_rows: Arc::default(), open: Mutex::new(Vec::new()), stopped: Default::default() };
        held.follow(database);
        held
    }

    /// Opens the streams for the epoch now. Streams of an epoch that has gone are cancelled first.
    pub(super) fn follow(&self, database: &Database) {
        let mut open = self.open.lock().unwrap();
        if open.first().is_some_and(|stream| stream.epoch == database.session_epoch()) { return; }
        self.stopped.store(false, Ordering::SeqCst);
        for stream in open.drain(..) { stream.cancel(); }
        open.push(database.watch(StreamRequest::Query { query_json: r#"{"model":"User","relation":"profile","params":{}}"#.into() }, self.single.clone()));
        open.push(database.watch(StreamRequest::Entity { model: "User".into(), id: "user".into(), fetch: EntityFetch::Force }, self.entity.clone()));
        open.push(database.watch_query(r#"{"model":"User","relation":"all","params":{}}"#, self.rows.clone()));
        open.push(database.watch_query(r#"{"model":"User","relation":"all","params":{},"order":[{"field":"name","direction":"asc"}]}"#, self.window_rows.clone()));
    }

    /// The app cancels its collectors and lists before the data clear.
    pub(super) fn cancel_streams(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        for stream in self.open.lock().unwrap().drain(..) { stream.cancel(); }
    }

    pub(super) fn cancel_all(&self) { self.cancel_streams(); }

    /// The stream of the main list.
    pub(super) fn query(&self) -> Arc<Watched> { self.open.lock().unwrap()[2].clone() }

    /// The singleton and the id entity are over: their streams ended with their account or the reader stopped them.
    pub(super) fn entities_empty(&self) -> bool { self.stopped.load(Ordering::SeqCst) || [&self.single, &self.entity].iter().all(|rows| Self::over(rows)) }

    fn over(rows: &Rows) -> bool { !rows.ended().is_empty() || rows.len() == 0 }

    fn shown(rows: &Rows, account: &str) -> bool {
        let rows = rows.0.lock().unwrap();
        !rows.is_empty() && rows.iter().all(|row| named(row, account))
    }

    pub(super) async fn shows(&self, account: &str) {
        yield_until(&format!("the held entities show {account}"), || Self::shown(&self.single, account) && Self::shown(&self.entity, account)).await;
    }

    pub(super) async fn lists_show(&self, account: &str) {
        for rows in [&self.rows, &self.window_rows] {
            yield_until(&format!("the held list shows {account}"), || Self::shown(rows, account)).await;
        }
    }

    /// Every kept view shows the account: both entities and both lists.
    pub(super) fn displays(&self, account: &str) -> bool {
        Self::shown(&self.single, account) && Self::shown(&self.entity, account) && Self::shown(&self.rows, account) && Self::shown(&self.window_rows, account)
    }

    /// No kept view shows a row of an account: every stream ended with its account, was stopped, or has no row.
    pub(super) fn is_clear(&self) -> bool {
        self.stopped.load(Ordering::SeqCst) || [&self.single, &self.entity, &self.rows, &self.window_rows].iter().all(|rows| Self::over(rows))
    }

    pub(super) fn deliveries(&self) -> (usize, usize, usize, usize) {
        let length = |rows: &Rows| rows.1.lock().unwrap().len();
        (length(&self.rows), length(&self.window_rows), length(&self.single), length(&self.entity))
    }

    fn showed(rows: &Rows, from: usize, account: &str) -> bool {
        rows.1.lock().unwrap().iter().skip(from).any(|seen| seen.rows.iter().any(|row| named(row, account)))
    }

    /// No delivery after `mark` shows the account.
    pub(super) fn quiet_since(&self, mark: (usize, usize, usize, usize), account: &str) -> bool {
        !Self::showed(&self.rows, mark.0, account) && !Self::showed(&self.window_rows, mark.1, account) && !Self::showed(&self.single, mark.2, account) && !Self::showed(&self.entity, mark.3, account)
    }

    /// The account never appears in a delivery of any held stream.
    pub(super) fn never_showed(&self, account: &str) -> bool {
        ![&self.rows, &self.window_rows, &self.single, &self.entity].iter().any(|rows| Self::showed(rows, 0, account))
    }
}
