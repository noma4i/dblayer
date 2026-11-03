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
    calls: BTreeMap<String, usize>,
    violations: Vec<String>,
}

impl Server {
    fn calls(&self) -> BTreeMap<String, usize> { self.state.lock().unwrap().calls.clone() }
    fn forbid(&self, forbidden: bool) { self.state.lock().unwrap().forbidden = forbidden; }
    fn violations(&self) -> Vec<String> { self.state.lock().unwrap().violations.clone() }
}

#[async_trait]
impl EncryptedHttp for Server {
    async fn post(&self, path: String, body: String, _: bool) -> Result<EncryptedReply, DbError> {
        let request = decrypt_request(&body).unwrap();
        let answer = {
            let mut state = self.state.lock().unwrap();
            if state.forbidden {
                state.violations.push(format!("{path} {}", request["body"]["operationName"]));
                return Ok(EncryptedReply { status: 400, body: String::new() });
            }
            if path == "/api/auth/login_link" {
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

#[derive(Default)]
struct Entities(Mutex<BTreeMap<u64, WireEntitySnapshot>>);

impl EntityListener for Entities {
    fn changed(&self, delivery: EntityDelivery) {
        let mut rows = self.0.lock().unwrap();
        for row in delivery.snapshots { rows.insert(row.lease, row); }
    }
}

#[derive(Default)]
struct Rows(Mutex<Vec<WireRow>>);

impl QueryListener for Rows {
    fn changed(&self, change: LiveChange) {
        let mut rows = self.0.lock().unwrap();
        if change.full { rows.clear(); }
        rows.retain(|row| !change.removed.contains(&row.id));
        for row in change.rows {
            rows.retain(|previous| previous.id != row.id);
            rows.push(row);
        }
    }
}

fn models() -> String {
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
    encrypted.endpoints.session_operations.insert("login_link".into(), EncryptedSessionOperation { path: "/api/auth/login_link".into(), sessionless: false });
    let database = OpenDatabase::new().open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, models(), Some(Arc::new(super::encrypted_tests::SubscriptionsOnly)), None, TestClock::new(1_000_000), false, false, Some(encrypted), Some(Arc::new(super::session_guards_tests::WatchedHeld::default())), Some(server), None).await.unwrap();
    database.register_operations(json!([
        {"name":"MyProfile","document":"query MyProfile { me { id name } }"},
        {"name":"UserNode","document":"query UserNode($id: ID!) { node(id: $id) { ... on User { id name } } }"},
        {"name":"Users","document":"query Users { users { id name } }"},
        {"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"}
    ]).to_string()).unwrap();
    database
}

async fn start_session(database: &Database, account: &str) {
    within(database.session_call("login_link".into(), json!({"account":account}).to_string())).await.unwrap();
    within(database.logout()).await.unwrap();
    within(database.run("SessionProfile".into(), "{}".into())).await.unwrap();
    within(database.refresh("User".into(), "profile".into(), "{}".into())).await.unwrap();
}

async fn delivered(changes: &LiveChanges) -> Vec<Vec<WireRow>> {
    let mut rows: Vec<WireRow> = Vec::new();
    let mut states = Vec::new();
    while let Ok(Some(change)) = tokio::time::timeout(std::time::Duration::from_millis(300), changes.next()).await {
        if change.full { rows.clone_from(&change.rows); } else {
            rows.retain(|row| !change.removed.contains(&row.id));
            for row in change.rows { rows.retain(|kept| kept.id != row.id); rows.push(row); }
        }
        states.push(rows.clone());
    }
    states
}

fn named(row: &WireRow, name: &str) -> bool { row.cells.contains(&super::wire::Cell::Text { value: name.into() }) }

#[tokio::test(flavor = "multi_thread")]
async fn encrypted_app_login_logout_reset_and_account_switch_keep_held_reads_current() {
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    start_session(&database, "initial").await;
    let snapshots = Arc::new(Entities::default());
    let entities = database.live_entities("User".into(), snapshots.clone()).unwrap();
    entities.singleton(r#"{"model":"User","relation":"profile","params":{}}"#.into(), 1).unwrap();
    entities.retain(vec![EntityRequest { id: "user".into(), lease: 2, fetch: EntityFetch::Force }]).unwrap();
    let rows = Arc::new(Rows::default());
    let query = database.live_query(r#"{"model":"User","relation":"all","params":{}}"#.into(), rows.clone());
    within(query.wait_for_initial_load()).await.unwrap();
    let changes = database.changes(r#"{"model":"User","relation":"all","params":{}}"#.into()).unwrap();
    within(database.settle_for_testing()).await.unwrap();
    for account in ["A", "B"] {
        within(database.logout()).await.unwrap();
        server.forbid(true);
        let before = server.calls();
        within(database.reset_for_testing()).await.unwrap();
        within(database.settle_for_testing()).await.unwrap();
        assert!(server.violations().is_empty(), "REL-24: held request after logout: {:?}", server.violations());
        assert_eq!(server.calls(), before, "REL-24: no held request after logout");
        assert!(snapshots.0.lock().unwrap().values().all(|row| row.value.is_none()));
        server.forbid(false);
        start_session(&database, account).await;
        within(database.settle_for_testing()).await.unwrap();
        crate::test_wait::yield_until("both held entities show the new account", || {
            let snapshots = snapshots.0.lock().unwrap();
            snapshots.len() == 2 && snapshots.values().all(|row| row.settled && row.value.as_ref().is_some_and(|row| named(row, account)))
        }).await;
        assert!(rows.0.lock().unwrap().iter().all(|row| named(row, account)));
        let previous = if account == "A" { "initial" } else { "A" };
        let states = delivered(&changes).await;
        assert!(states.first().is_some_and(|first| first.iter().all(|row| !named(row, previous))), "D7: the first change after the logout shows the previous account: {states:?}");
        assert!(states.last().is_some_and(|last| !last.is_empty() && last.iter().all(|row| named(row, account))));
        let before = server.calls();
        within(database.reset_for_testing()).await.unwrap();
        within(database.settle_for_testing()).await.unwrap();
        for operation in ["MyProfile", "UserNode", "Users"] {
            assert_eq!(server.calls()[operation], before[operation] + 1, "reset keeps session: {operation}");
        }
    }
    database.session_reset().unwrap();
    server.state.lock().unwrap().signed = false;
    let refused = within(database.run("SessionProfile".into(), "{}".into())).await.unwrap_err();
    assert!(matches!(refused, DbError::Session { code: SessionCode::Unauthenticated, .. }));
    server.forbid(true);
    within(database.reset_for_testing()).await.unwrap();
    within(database.settle_for_testing()).await.unwrap();
    assert!(server.violations().is_empty(), "a held read reached the server after sessionReset: {:?}", server.violations());
    database.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn session_reset_and_close_reject_answers_started_before_the_transition() {
    for (session_call, end) in [(false, "session_reset"), (false, "close"), (true, "session_reset"), (false, "logout")] {
        let close = end == "close";
        let server = Arc::new(Server::default());
        let database = open(server.clone()).await;
        start_session(&database, "A").await;
        let pause = Arc::new(Pause { entered: tokio::sync::Notify::new(), release: tokio::sync::Semaphore::new(0) });
        *server.pause.lock().unwrap() = Some(pause.clone());
        let caller = database.clone();
        let work = tokio::spawn(async move {
            if session_call {
                caller.session_call("login_link".into(), json!({"account":"late"}).to_string()).await.map(|_| ())
            } else {
                caller.refresh("User".into(), "profile".into(), "{}".into()).await.map(|_| ())
            }
        });
        within(pause.entered.notified()).await;
        match end {
            "close" => database.shutdown(),
            "logout" => within(database.logout()).await.unwrap(),
            _ => database.session_reset().unwrap(),
        }
        pause.release.add_permits(1);
        assert!(within(work).await.unwrap().is_err(), "old response was accepted: session_call={session_call}, end={end}");
        if !close {
            if end == "session_reset" { assert!(database.session.as_ref().unwrap().signed_in().unwrap().is_none()); }
            server.forbid(true);
            within(database.reset_for_testing()).await.unwrap();
            within(database.settle_for_testing()).await.unwrap();
            assert!(server.violations().is_empty(), "{end}: {:?}", server.violations());
            database.shutdown();
        }
    }
}
