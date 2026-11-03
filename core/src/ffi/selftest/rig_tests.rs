use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{Notify, Semaphore};

use crate::ffi::lifecycle_tests::{models, named, Rows};
use crate::ffi::session_guards_tests::WatchedHeld;
use crate::ffi::{
    ByteProgress, CableLease, Config, Database, DbError, EncryptedHttp, EncryptedReply, EncryptedSessionOperation, EncryptedSocketEvent, EntityDelivery, EntityFetch, EntityListener, EntityRequest,
    LiveChanges, LiveSubscription, OpenDatabase, PutReply, SessionCode, SubscriptionSink, TestClock, Transport, TransportRequest, WireEntitySnapshot, WireRow,
};
use crate::test_wait::{within_secs, yield_until};
use crate::transport::encrypted::fake_server_tests::{decrypt_request, seal_response};

/// Every call of a sequence returns within this many seconds or the test fails instead of hanging.
pub(super) const BOUND: u64 = 20;

pub(super) async fn bound<T>(work: impl std::future::Future<Output = T>) -> T { within_secs(BOUND, work).await }

pub(super) struct Gate {
    account: Mutex<Option<String>>,
    entered: Notify,
    release: Semaphore,
    count: AtomicUsize,
}

impl Gate {
    pub(super) fn new() -> Arc<Self> { Arc::new(Self { account: Mutex::new(None), entered: Notify::new(), release: Semaphore::new(0), count: AtomicUsize::new(0) }) }
    pub(super) async fn pass(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
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
    pub(super) fn count(&self) -> usize { self.count.load(Ordering::SeqCst) }
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
    Network,
    Status(u16),
    Refuse { code: &'static str, confirmed: bool },
    Graphql(&'static str),
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
    pub(super) fn calls(&self) -> BTreeMap<String, usize> { self.state.lock().unwrap().calls.clone() }
    pub(super) fn count(&self, operation: &str) -> usize { self.state.lock().unwrap().calls.get(operation).copied().unwrap_or(0) }
    pub(super) fn log(&self) -> Vec<Entry> { self.state.lock().unwrap().log.clone() }
    pub(super) fn hits(&self) -> usize { self.state.lock().unwrap().log.len() }
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
            let detail = format!("{} {operation}", self.label);
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
            Some(Fault::Network) => return Err(DbError::Network { detail: "offline".into() }),
            Some(Fault::Status(status)) => return Err(DbError::Http { status, detail: String::new() }),
            Some(Fault::Refuse { code, .. }) => return Err(DbError::Session { code: SessionCode::Unauthenticated, detail: code.into() }),
            Some(Fault::Graphql(code)) => return Ok(json!({"errors":[{"message":code,"extensions":{"code":code}}]}).to_string()),
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
            Some(Fault::Network) => return Err(DbError::Network { detail: "offline".into() }),
            Some(Fault::Status(status)) => return Ok(EncryptedReply { status, body: String::new() }),
            Some(Fault::Refuse { code, confirmed }) => {
                answer = json!({"error": code});
                if !confirmed { sealed_sequence = None; }
            }
            Some(Fault::Graphql(code)) => answer = json!({"errors":[{"message":code,"extensions":{"code":code}}]}),
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
    sinks: Mutex<Vec<(String, Arc<SubscriptionSink>)>>,
    unsubscribed: Mutex<Vec<String>>,
}

impl Switchable {
    pub(super) fn new(target: Option<Arc<World>>, invalid: bool) -> Arc<Self> {
        Arc::new(Self { target: Mutex::new(target), invalid, tokens: AtomicUsize::new(0), attempts: AtomicUsize::new(0), sinks: Mutex::new(Vec::new()), unsubscribed: Mutex::new(Vec::new()) })
    }
    pub(super) fn set(&self, target: Option<Arc<World>>) { *self.target.lock().unwrap() = target; }
    pub(super) fn unsubscribed(&self) -> Vec<String> { self.unsubscribed.lock().unwrap().clone() }
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
        let world = self.current()?;
        world.plain(&format!("subscribe:{}", request.operation_name)).await?;
        let token = format!("{}-{}", world.label, self.tokens.fetch_add(1, Ordering::SeqCst));
        sink.opened(token.clone())?;
        sink.ready();
        self.sinks.lock().unwrap().push((token, sink));
        Ok(())
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
    {"name":"UnregisterDevice","document":"mutation UnregisterDevice { unregisterDevice { ok } }"}
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
    serde_json::to_string(&models).unwrap()
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
            .open_with_clock(config(), models(), Some(switch.clone()), None, clock.clone(), false, false, Some(encrypted), Some(Arc::new(WatchedHeld::default())), Some(world.clone()), None)
            .await
            .unwrap();
        database.register_operations(OPERATIONS.into()).unwrap();
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

    /// The harness of the apps ends a test: unset the target, reset, settle, then set the next target.
    pub(super) async fn harness_cycle(&self, next: Option<Arc<World>>) {
        self.switch.set(None);
        bound(self.database.reset_for_testing()).await.unwrap();
        bound(self.database.settle_for_testing()).await.unwrap();
        self.switch.set(next);
    }

    pub(super) async fn login_ios(&self, account: &str) {
        let database = &self.database;
        bound(database.session_call("login_link".into(), json!({"account":account}).to_string())).await.unwrap();
        bound(database.logout()).await.unwrap();
        bound(database.run("SessionProfile".into(), "{}".into())).await.unwrap();
        bound(database.refresh("User".into(), "profile".into(), "{}".into())).await.unwrap();
    }

    pub(super) async fn login_android(&self, account: &str) {
        let database = &self.database;
        bound(database.session_call("social".into(), json!({"account":account}).to_string())).await.unwrap();
        bound(database.logout()).await.unwrap();
        bound(database.refresh("User".into(), "profile".into(), "{}".into())).await.unwrap();
    }

    /// The exit of the app. `revoke`: unregister the device and the confirmed server logout come first. `cancel`: the Android app cancels its collectors after the session reset, the iOS app keeps them held.
    pub(super) async fn exit(&self, held: &Held, revoke: bool, cancel: bool) {
        let database = &self.database;
        if revoke {
            bound(database.run("UnregisterDevice".into(), "{}".into())).await.unwrap();
            bound(database.session_call("logout".into(), "{}".into())).await.unwrap();
            assert!(!self.world.signed(), "the server did not confirm the logout");
        }
        database.session_reset().unwrap();
        if cancel { held.cancel_streams(); }
        bound(database.logout()).await.unwrap();
    }

    /// A second open through the opener that already owns an open database.
    pub(super) async fn open_again(&self, transport: Option<Arc<dyn Transport>>) -> Result<Arc<Database>, DbError> {
        self.opener.open_with_clock(config(), plain_models(), transport, None, TestClock::new(1_000_000), false, false, None, None, None, None).await
    }

    pub(super) async fn settle(&self) { bound(self.database.settle_for_testing()).await.unwrap(); }
}

/// Every delivery of the held entities, in order.
#[derive(Default)]
pub(super) struct EntityLog(pub(super) Mutex<Vec<WireEntitySnapshot>>);

impl EntityListener for EntityLog {
    fn changed(&self, delivery: EntityDelivery) { self.0.lock().unwrap().extend(delivery.snapshots); }
}

impl EntityLog {
    pub(super) fn latest(&self, lease: u64) -> Option<WireEntitySnapshot> { self.0.lock().unwrap().iter().rev().find(|snapshot| snapshot.lease == lease).cloned() }
    pub(super) fn ever_showed(&self, account: &str) -> bool { self.0.lock().unwrap().iter().any(|snapshot| snapshot.value.as_ref().is_some_and(|row| named(row, account))) }
    pub(super) fn len(&self) -> usize { self.0.lock().unwrap().len() }
    pub(super) fn shows(&self, account: &str) -> bool {
        (1..=2).all(|lease| self.latest(lease).is_some_and(|snapshot| snapshot.value.as_ref().is_some_and(|row| named(row, account)) && !matches!(snapshot.read, crate::ffi::EntityRead::Failed { .. })))
    }
    pub(super) fn empty(&self) -> bool { (1..=2).all(|lease| self.latest(lease).is_some_and(|snapshot| snapshot.value.is_none())) }
}

/// The handles the app holds across its whole life: the singleton and the id entity (CurrentUser), a list, a window and a changes feed.
pub(super) struct Held {
    pub(super) entities: Arc<crate::ffi::LiveEntities>,
    pub(super) log: Arc<EntityLog>,
    pub(super) query: Arc<LiveSubscription>,
    pub(super) rows: Arc<Rows>,
    pub(super) window: Arc<LiveSubscription>,
    pub(super) window_rows: Arc<Rows>,
    pub(super) changes: Arc<LiveChanges>,
}

impl Held {
    pub(super) fn open(database: &Database) -> Held {
        let log = Arc::new(EntityLog::default());
        let entities = database.live_entities("User".into(), log.clone()).unwrap();
        entities.singleton(r#"{"model":"User","relation":"profile","params":{}}"#.into(), 1).unwrap();
        entities.retain(vec![EntityRequest { id: "user".into(), lease: 2, fetch: EntityFetch::Force }]).unwrap();
        let rows = Arc::new(Rows::default());
        let window_rows = Arc::new(Rows::default());
        let query = database.live_query(r#"{"model":"User","relation":"all","params":{}}"#.into(), rows.clone());
        let window = database.live_window(r#"{"model":"User","relation":"all","params":{},"order":[{"field":"name","direction":"asc"}]}"#.into(), 10, None, window_rows.clone());
        let changes = database.changes(r#"{"model":"User","relation":"all","params":{}}"#.into()).unwrap();
        Held { entities, log, query, rows, window, window_rows, changes }
    }

    /// The app cancels its collectors and lists before the data clear. The entities stay held, as CurrentUser does.
    pub(super) fn cancel_streams(&self) {
        self.query.cancel();
        self.window.cancel();
        self.changes.cancel();
    }

    pub(super) fn cancel_all(&self) {
        self.cancel_streams();
        self.entities.shutdown();
    }

    pub(super) async fn shows(&self, account: &str) {
        yield_until(&format!("the held entities show {account}"), || self.log.shows(account)).await;
    }

    pub(super) async fn lists_show(&self, account: &str) {
        for rows in [&self.rows, &self.window_rows] {
            yield_until(&format!("the held list shows {account}"), || {
                let rows = rows.0.lock().unwrap();
                !rows.is_empty() && rows.iter().all(|row| named(row, account))
            })
            .await;
        }
    }

    /// The account never appears in a list delivery, an entity delivery or a change.
    pub(super) fn never_showed(&self, account: &str) -> bool {
        let listed = |rows: &Rows| rows.1.lock().unwrap().iter().any(|change| change.rows.iter().any(|row| named(row, account)));
        !self.log.ever_showed(account) && !listed(&self.rows) && !listed(&self.window_rows)
    }
}

pub(super) fn rows_named(rows: &[WireRow], account: &str) -> bool { rows.iter().any(|row| named(row, account)) }
