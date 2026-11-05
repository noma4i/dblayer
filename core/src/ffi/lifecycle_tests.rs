use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;

use super::*;
use crate::schema::{FieldSpec, ModelSpec, NodeRead, RelationSpec, RemoteShape, VariableSource, NODE_PARAM};
use crate::test_wait::within;
use crate::transport::encrypted::fake_server_tests::{decrypt_request, seal_response};
use crate::value::ScalarKind;

#[derive(Default)]
struct Server {
    state: Mutex<ServerState>,
    pause: Mutex<Option<Arc<Pause>>>,
}

struct Pause {
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

#[derive(Default)]
struct ServerState {
    account: String,
    signed: bool,
    forbidden: bool,
    status: Option<u16>,
    hits: usize,
    calls: BTreeMap<String, usize>,
    violations: Vec<String>,
}

impl Server {
    fn hits(&self) -> usize { self.state.lock().unwrap().hits }
    fn fail_with(&self, status: Option<u16>) { self.state.lock().unwrap().status = status; }
    fn pause_next(&self) -> Arc<Pause> {
        let pause = Arc::new(Pause { entered: tokio::sync::Notify::new(), release: tokio::sync::Semaphore::new(0) });
        *self.pause.lock().unwrap() = Some(pause.clone());
        pause
    }
    fn calls(&self) -> BTreeMap<String, usize> { self.state.lock().unwrap().calls.clone() }
    fn forbid(&self, forbidden: bool) { self.state.lock().unwrap().forbidden = forbidden; }
    fn violations(&self) -> Vec<String> { self.state.lock().unwrap().violations.clone() }
}

#[async_trait]
impl EncryptedHttp for Server {
    async fn post(&self, path: String, body: String, _: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let request = decrypt_request(&body).unwrap();
        let answer = {
            let mut state = self.state.lock().unwrap();
            if state.forbidden {
                state.violations.push(format!("{path} {}", request["body"]["operationName"]));
                return Ok(EncryptedReply { status: 400, body: String::new() });
            }
            state.hits += 1;
            if let Some(status) = state.status {
                return Ok(EncryptedReply { status, body: String::new() });
            }
            if path.ends_with("logout") {
                state.signed = false;
                json!({"revoked":true})
            } else if path == "/api/auth/otp" {
                json!({"sent":true})
            } else if path == "/api/auth/login_link" {
                state.account = request["body"]["account"].as_str().unwrap().into();
                state.signed = true;
                json!({"sessionId":state.account})
            } else if !state.signed {
                json!({"error":"UNAUTHENTICATED"})
            } else {
                let operation = request["body"]["operationName"].as_str().unwrap();
                *state.calls.entry(operation.into()).or_default() += 1;
                let row = json!({"__typename":"User","id":"user","name":state.account});
                match operation {
                    "SessionProfile" => json!({"data":{"sessionProfile":{"id":state.account}}}),
                    "UserNode" => json!({"data":{"node":row}}),
                    "MyProfile" => json!({"data":{"me":row}}),
                    "Users" => json!({"data":{"users":[row]}}),
                    _ => panic!("unexpected operation {operation}"),
                }
            }
        };
        let pause = self.pause.lock().unwrap().take();
        if let Some(pause) = pause {
            pause.entered.notify_one();
            pause.release.acquire().await.unwrap().forget();
        }
        Ok(EncryptedReply { status: 200, body: seal_response(&request, request["sequence"].as_i64(), answer) })
    }
}

pub(super) use super::tape::Rows;

pub(super) fn models() -> String {
    let mut user = ModelSpec::new("User", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str)]);
    for (name, operation, shape) in [("profile", "MyProfile", RemoteShape::Single), ("all", "Users", RemoteShape::List { coverage: crate::schema::Coverage::Complete })] {
        let mut relation = super::load_tests::relation(name, "5.minutes");
        let remote = relation.remote.as_mut().unwrap();
        remote.operation = operation.into();
        remote.shape = shape;
        user.relations.push(relation);
    }
    let mut node = RelationSpec::new("node", &[]);
    node.text_params = vec![NODE_PARAM.into()];
    node.node = Some(NodeRead::One);
    let mut remote = user.relations[0].remote.clone().unwrap();
    remote.operation = "UserNode".into();
    remote.variables.insert("id".into(), VariableSource::Param { name: NODE_PARAM.into() });
    node.remote = Some(remote);
    user.relations.push(node);
    serde_json::to_string(&vec![user]).unwrap()
}

async fn open(server: Arc<Server>) -> Arc<Database> {
    let mut encrypted = crate::transport::encrypted::protocol_tests::config();
    encrypted.endpoints.session_operations.insert("login_link".into(), EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: crate::transport::EncryptedOperationClass::Authenticated });
    encrypted.endpoints.session_operations.insert("otp".into(), EncryptedSessionOperation { path: "/api/auth/otp".into(), class: crate::transport::EncryptedOperationClass::Sessionless });
    let database = OpenDatabase::new().open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, models(), Some(Arc::new(super::encrypted_tests::SubscriptionsOnly)), None, TestClock::new(1_000_000), Some(encrypted), Some(Arc::new(super::session_guards_tests::WatchedHeld::default())), Some(server), None).await.unwrap();
    database.register_operations(json!([
        {"name":"MyProfile","document":"query MyProfile { me { id name } }"},
        {"name":"UserNode","document":"query UserNode($id: ID!) { node(id: $id) { ... on User { id name } } }"},
        {"name":"Users","document":"query Users { users { id name } }"},
        {"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"}
    ]).to_string()).unwrap();
    database.set_online(true).unwrap();
    database
}

async fn sign_in(database: &Database, account: &str) {
    within(database.session_call("login_link".into(), json!({"account":account}).to_string())).await.unwrap();
    within(database.run(database.session_epoch(), "SessionProfile".into(), "{}".into())).await.unwrap();
    within(database.refresh(database.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.unwrap();
}

pub(super) fn named(row: &WireRow, name: &str) -> bool { row.cells.contains(&super::wire::Cell::Text { value: name.into() }) }

#[tokio::test(flavor = "multi_thread")]
async fn session_reset_and_close_reject_answers_started_before_the_transition() {
    for (session_call, end) in [(false, "session_reset"), (false, "close"), (true, "session_reset"), (false, "logout")] {
        let close = end == "close";
        let server = Arc::new(Server::default());
        let database = open(server.clone()).await;
        sign_in(&database, "A").await;
        let pause = Arc::new(Pause { entered: tokio::sync::Notify::new(), release: tokio::sync::Semaphore::new(0) });
        *server.pause.lock().unwrap() = Some(pause.clone());
        let caller = database.clone();
        let work = tokio::spawn(async move {
            if session_call {
                caller.session_call("login_link".into(), json!({"account":"late"}).to_string()).await.map(|_| ())
            } else {
                caller.refresh(caller.session_epoch(), "User".into(), "profile".into(), "{}".into()).await.map(|_| ())
            }
        });
        within(pause.entered.notified()).await;
        match end {
            "close" => database.close_now(),
            "logout" => within(database.session_reset()).await.unwrap(),
            _ => database.session_reset().await.unwrap(),
        }
        pause.release.add_permits(1);
        assert!(within(work).await.unwrap().is_err(), "old response was accepted: session_call={session_call}, end={end}");
        if !close {
            if end == "session_reset" { assert!(database.session.as_ref().unwrap().signed_in().unwrap().is_none()); }
            server.forbid(true);
            within(crate::ffi::settled(&database)).await.unwrap();
            assert!(server.violations().is_empty(), "{end}: {:?}", server.violations());
            database.close_now();
        }
    }
}

async fn end_it(database: &Database, end: &str) {
    match end {
        "close" => database.close_now(),
        _ => database.session_reset().await.unwrap(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_waiting_in_a_retry_or_a_queue_ends_with_the_session_reset_and_the_close() {
    for end in ["session_reset", "close"] {
        let server = Arc::new(Server::default());
        let database = open(server.clone()).await;
        sign_in(&database, "A").await;
        server.fail_with(Some(500));
        let hits = server.hits();
        let caller = database.clone();
        let retrying = tokio::spawn(async move { caller.session_call("login_link".into(), json!({"account":"late"}).to_string()).await.map(|_| ()) });
        crate::test_wait::yield_until("the request failed and the call waits for its retry", || server.hits() > hits).await;
        end_it(&database, end).await;
        assert!(within(retrying).await.unwrap().is_err(), "{end}: a call in a retry wait outlived the transition");
        database.close_now();
    }
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    sign_in(&database, "A").await;
    let pause = server.pause_next();
    let (first, second) = (database.clone(), database.clone());
    let running = tokio::spawn(async move { first.session_call("login_link".into(), json!({"account":"first"}).to_string()).await.map(|_| ()) });
    within(pause.entered.notified()).await;
    let queued = tokio::spawn(async move { second.session_call("login_link".into(), json!({"account":"queued"}).to_string()).await.map(|_| ()) });
    database.session_reset().await.unwrap();
    assert!(within(running).await.unwrap().is_err());
    assert!(within(queued).await.unwrap().is_err(), "a call queued behind another one ran after the session reset");
    assert!(database.session.as_ref().unwrap().signed_in().unwrap().is_none());
    database.close_now();
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn a_confirmed_logout_that_reaches_the_commit_after_the_close_clears_nothing() {
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    sign_in(&database, "A").await;
    let pause = server.pause_next();
    let caller = database.clone();
    let work = tokio::spawn(async move { caller.session_call("logout".into(), "{}".into()).await.map(|_| ()) });
    within(pause.entered.notified()).await;
    database.close_now();
    pause.release.add_permits(1);
    assert!(within(work).await.unwrap().is_err(), "the revoked answer of the closed database was accepted");
    assert!(database.session.as_ref().unwrap().signed_in().unwrap().is_some(), "the answer cleared the session after the database closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_after_a_reset_does_not_join_the_flight_of_the_old_generation() {
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    sign_in(&database, "A").await;
    let pause = server.pause_next();
    let first = database.clone();
    let epoch = first.session_epoch();
    let old = tokio::spawn(async move { first.refresh(epoch, "User".into(), "all".into(), "{}".into()).await });
    within(pause.entered.notified()).await;
    let before = server.calls()["Users"];
    within(database.session_reset()).await.unwrap();
    sign_in(&database, "B").await;
    within(database.refresh(database.session_epoch(), "User".into(), "all".into(), "{}".into())).await.unwrap();
    assert_eq!(server.calls()["Users"], before + 1, "the refresh after the reset waited for the old request");
    assert!(within(old).await.unwrap().is_err(), "the refresh of the old data waited for its request after the reset");
    pause.release.add_permits(1);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reset_ends_every_explicit_call_waiting_on_the_old_flight_without_its_request() {
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    sign_in(&database, "A").await;
    let pause = server.pause_next();
    let first = database.clone();
    let epoch = first.session_epoch();
    let owner = tokio::spawn(async move { first.refresh(epoch, "User".into(), "all".into(), "{}".into()).await });
    within(pause.entered.notified()).await;
    let second = database.clone();
    let epoch = second.session_epoch();
    let waiting = database.life.waiting_calls();
    let joined = tokio::spawn(async move { second.load(epoch, "User".into(), "all".into(), "{}".into()).await });
    crate::test_wait::yield_until("the load joined the flight", || database.life.waiting_calls() > waiting).await;
    within(database.session_reset()).await.unwrap();
    assert!(within(owner).await.unwrap().is_err(), "the owner of the old flight waited for its request");
    assert!(within(joined).await.unwrap().is_err(), "a call joined to the old flight waited for its request");
    pause.release.add_permits(1);
    database.close_now();
}

