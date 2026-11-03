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
    async fn post(&self, path: String, body: String, _: bool) -> Result<EncryptedReply, DbError> {
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

#[derive(Default)]
struct Entities(Mutex<BTreeMap<u64, WireEntitySnapshot>>);

impl EntityListener for Entities {
    fn changed(&self, delivery: EntityDelivery) {
        let mut rows = self.0.lock().unwrap();
        for row in delivery.snapshots { rows.insert(row.lease, row); }
    }
}

#[derive(Default)]
struct Rows(Mutex<Vec<WireRow>>, Mutex<Vec<LiveChange>>);

impl QueryListener for Rows {
    fn changed(&self, change: LiveChange) {
        self.1.lock().unwrap().push(change.clone());
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
    encrypted.endpoints.session_operations.insert("otp".into(), EncryptedSessionOperation { path: "/api/auth/otp".into(), sessionless: true });
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
    let window_rows = Arc::new(Rows::default());
    let query = database.live_query(r#"{"model":"User","relation":"all","params":{}}"#.into(), rows.clone());
    let window = database.live_window(r#"{"model":"User","relation":"all","params":{},"order":[{"field":"name","direction":"asc"}]}"#.into(), 10, None, window_rows.clone());
    within(query.wait_for_initial_load()).await.unwrap();
    within(window.wait_for_initial_load()).await.unwrap();
    crate::test_wait::yield_until("both held entities loaded", || {
        let snapshots = snapshots.0.lock().unwrap();
        snapshots.len() == 2 && snapshots.values().all(|row| row.settled && row.read == EntityRead::Loaded && row.value.is_some())
    }).await;
    let changes = database.changes(r#"{"model":"User","relation":"all","params":{}}"#.into()).unwrap();
    within(database.settle_for_testing()).await.unwrap();
    for account in ["A", "B"] {
        for listener in [&rows, &window_rows] { listener.1.lock().unwrap().clear(); }
        let emptied_generation = database.db.generation().value();
        within(database.logout()).await.unwrap();
        server.forbid(true);
        let before = server.calls();
        within(database.reset_for_testing()).await.unwrap();
        within(database.settle_for_testing()).await.unwrap();
        assert!(server.violations().is_empty(), "REL-24: held request after logout: {:?}", server.violations());
        assert_eq!(server.calls(), before, "REL-24: no held request after logout");
        {
            let snapshots = snapshots.0.lock().unwrap();
            assert_eq!(snapshots.len(), 2, "both held entities publish after the logout");
            assert!(snapshots.values().all(|row| row.value.is_none() && row.generation > emptied_generation && !matches!(row.read, EntityRead::Failed { .. })), "the held entities must be empty, of a newer generation and not failed: {snapshots:?}");
        }
        for listener in [&rows, &window_rows] {
            crate::test_wait::yield_until("the held list publishes the reset", || listener.1.lock().unwrap().iter().any(|change| change.size == 0 && change.rows.is_empty())).await;
            assert!(listener.1.lock().unwrap().iter().all(|change| change.error.is_none()), "a held list shows an error after the logout");
        }
        server.forbid(false);
        start_session(&database, account).await;
        within(database.settle_for_testing()).await.unwrap();
        crate::test_wait::yield_until("both held entities show the new account", || {
            let snapshots = snapshots.0.lock().unwrap();
            snapshots.len() == 2 && snapshots.values().all(|row| row.settled && row.read == EntityRead::Loaded && row.generation > emptied_generation && row.value.as_ref().is_some_and(|row| named(row, account)))
        }).await;
        within(window.wait_for_initial_load()).await.unwrap();
        for listener in [&rows, &window_rows] {
            crate::test_wait::yield_until("the held list shows the new account", || { let rows = listener.0.lock().unwrap(); !rows.is_empty() && rows.iter().all(|row| named(row, account)) }).await;
        }
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
        let after_reset = server.calls();
        within(database.session_call("login_link".into(), json!({"account":account}).to_string())).await.unwrap();
        within(database.settle_for_testing()).await.unwrap();
        assert_eq!(server.calls(), after_reset, "repeating the same session commit rereads nothing");
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

fn end_it(database: &Database, end: &str) {
    match end {
        "close" => database.shutdown(),
        _ => database.session_reset().unwrap(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_waiting_in_a_retry_or_a_queue_ends_with_the_session_reset_and_the_close() {
    for end in ["session_reset", "close"] {
        let server = Arc::new(Server::default());
        let database = open(server.clone()).await;
        start_session(&database, "A").await;
        server.fail_with(Some(500));
        let hits = server.hits();
        let caller = database.clone();
        let retrying = tokio::spawn(async move { caller.session_call("login_link".into(), json!({"account":"late"}).to_string()).await.map(|_| ()) });
        crate::test_wait::yield_until("the request failed and the call waits for its retry", || server.hits() > hits).await;
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        end_it(&database, end);
        assert!(within(retrying).await.unwrap().is_err(), "{end}: a call in a retry wait outlived the transition");
        database.shutdown();
    }
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    start_session(&database, "A").await;
    let pause = server.pause_next();
    let (first, second) = (database.clone(), database.clone());
    let running = tokio::spawn(async move { first.session_call("login_link".into(), json!({"account":"first"}).to_string()).await.map(|_| ()) });
    within(pause.entered.notified()).await;
    let queued = tokio::spawn(async move { second.session_call("login_link".into(), json!({"account":"queued"}).to_string()).await.map(|_| ()) });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    database.session_reset().unwrap();
    assert!(within(running).await.unwrap().is_err());
    assert!(within(queued).await.unwrap().is_err(), "a call queued behind another one ran after the session reset");
    assert!(database.session.as_ref().unwrap().signed_in().unwrap().is_none());
    database.shutdown();
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn an_answer_that_reaches_the_commit_after_the_session_ended_writes_nothing() {
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    start_session(&database, "A").await;
    let sequence = || database.session.as_ref().unwrap().sequence_for_test();
    let before = sequence();
    let pause = server.pause_next();
    let caller = database.clone();
    let work = tokio::spawn(async move { caller.session_call("login_link".into(), json!({"account":"late"}).to_string()).await.map(|_| ()) });
    within(pause.entered.notified()).await;
    let store = &database.db.shared.store;
    let waiting = store.writes_waiting.load(std::sync::atomic::Ordering::SeqCst);
    let writer = store.lock();
    pause.release.add_permits(1);
    crate::test_wait::yield_until("the answer reached the writer", || store.writes_waiting.load(std::sync::atomic::Ordering::SeqCst) > waiting).await;
    database.shutdown();
    drop(writer);
    assert!(within(work).await.unwrap().is_err(), "the answer of the closed database was accepted");
    assert_eq!(sequence(), before, "the answer wrote its sequence after the database closed");
}

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn a_confirmed_logout_that_reaches_the_commit_after_the_close_clears_nothing() {
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    start_session(&database, "A").await;
    let pause = server.pause_next();
    let caller = database.clone();
    let work = tokio::spawn(async move { caller.session_call("logout".into(), "{}".into()).await.map(|_| ()) });
    within(pause.entered.notified()).await;
    let store = &database.db.shared.store;
    let waiting = store.writes_waiting.load(std::sync::atomic::Ordering::SeqCst);
    let writer = store.lock();
    pause.release.add_permits(1);
    crate::test_wait::yield_until("the revoked answer reached the writer", || store.writes_waiting.load(std::sync::atomic::Ordering::SeqCst) > waiting).await;
    database.shutdown();
    drop(writer);
    assert!(within(work).await.unwrap().is_err(), "the revoked answer of the closed database was accepted");
    assert!(database.session.as_ref().unwrap().signed_in().unwrap().is_some(), "the answer cleared the session after the database closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn ending_the_session_of_an_older_epoch_ends_nothing() {
    let database = open(Arc::new(Server::default())).await;
    let store = &database.db.shared.store;
    store.end_session();
    let epoch = store.lifecycle.snapshot().session_epoch;
    assert!(store.end_session_in(Some(epoch - 1), || "refused").is_err());
    assert_eq!(store.lifecycle.snapshot().session_epoch, epoch, "an answer of an older session ended the newer one");
    assert!(store.end_session_in::<&str>(Some(epoch), || "refused").is_ok());
    assert_eq!(store.lifecycle.snapshot().session_epoch, epoch + 1);
    database.shutdown();
    assert!(store.end_session_in(Some(epoch + 1), || "refused").is_err(), "a closed database accepted the end of a session");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_changes_of_an_ended_session_never_return_its_rows() {
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    start_session(&database, "A").await;
    let changes = database.changes(r#"{"model":"User","relation":"all","params":{}}"#.into()).unwrap();
    within(database.settle_for_testing()).await.unwrap();
    crate::test_wait::yield_until("the rows of account A are queued", || changes.queued() > 0).await;
    database.session_reset().unwrap();
    let states = delivered(&changes).await;
    assert!(states.iter().all(|rows| rows.iter().all(|row| !named(row, "A"))), "the rows of the ended session came back: {states:?}");
    database.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_next_change_after_an_ended_session_arrives_without_its_rows() {
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    start_session(&database, "A").await;
    let changes = database.changes(r#"{"model":"User","order":[{"field":"name","direction":"asc"}]}"#.into()).unwrap();
    within(database.settle_for_testing()).await.unwrap();
    crate::test_wait::yield_until("the rows of account A are queued", || changes.queued() > 0).await;
    database.session_reset().unwrap();
    database.create("User".into(), json!({"id":"z","name":"C"}).to_string(), "{}".into()).await.unwrap();
    database.db.await_revision(database.db.revision_seq().unwrap());
    let states = delivered(&changes).await;
    assert!(states.iter().all(|rows| rows.iter().all(|row| !named(row, "A"))), "the rows of the ended session came back: {states:?}");
    assert!(states.last().is_some_and(|last| last.len() == 1 && named(&last[0], "C")), "the new row did not arrive alone: {states:?}");
    database.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_credential_created_while_a_reset_clears_keeps_its_session_state() {
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    start_session(&database, "A").await;
    let park = database.session.as_ref().unwrap().park_next_reset_for_test();
    let resetting = database.clone();
    let reset = std::thread::spawn(move || resetting.session_reset());
    crate::test_wait::yield_until("the reset is parked after the generation moved", || park.entered.load(std::sync::atomic::Ordering::SeqCst)).await;
    let reading = database.clone();
    let mut read = tokio::spawn(async move { reading.refresh("User".into(), "all".into(), "{}".into()).await });
    assert!(tokio::time::timeout(std::time::Duration::from_millis(300), &mut read).await.is_err(), "an explicit read created a credential while the reset was clearing");
    park.release.store(true, std::sync::atomic::Ordering::SeqCst);
    crate::test_wait::join_within(reset).unwrap();
    within(read).await.unwrap().unwrap();
    within(database.refresh("User".into(), "all".into(), "{}".into())).await.unwrap();
    database.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_after_a_reset_does_not_join_the_flight_of_the_old_generation() {
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    start_session(&database, "A").await;
    let pause = server.pause_next();
    let first = database.clone();
    let old = tokio::spawn(async move { first.refresh("User".into(), "all".into(), "{}".into()).await });
    within(pause.entered.notified()).await;
    let before = server.calls()["Users"];
    within(database.reset_for_testing()).await.unwrap();
    within(database.refresh("User".into(), "all".into(), "{}".into())).await.unwrap();
    assert_eq!(server.calls()["Users"], before + 1, "the refresh after the reset waited for the old request");
    assert!(within(old).await.unwrap().is_err(), "the refresh of the old data waited for its request after the reset");
    pause.release.add_permits(1);
    database.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reset_ends_every_explicit_call_waiting_on_the_old_flight_without_its_request() {
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    start_session(&database, "A").await;
    let pause = server.pause_next();
    let first = database.clone();
    let owner = tokio::spawn(async move { first.refresh("User".into(), "all".into(), "{}".into()).await });
    within(pause.entered.notified()).await;
    let second = database.clone();
    let joined = tokio::spawn(async move { second.load("User".into(), "all".into(), "{}".into()).await });
    crate::test_wait::yield_until("the load joined the flight", || database.sync.as_ref().unwrap().joined_loads() > 0).await;
    within(database.reset_for_testing()).await.unwrap();
    assert!(within(owner).await.unwrap().is_err(), "the owner of the old flight waited for its request");
    assert!(within(joined).await.unwrap().is_err(), "a call joined to the old flight waited for its request");
    pause.release.add_permits(1);
    database.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_initial_load_wait_after_a_reset_does_not_return_the_old_ready() {
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    start_session(&database, "A").await;
    let rows = Arc::new(Rows::default());
    let query = database.live_query(r#"{"model":"User","relation":"all","params":{}}"#.into(), rows.clone());
    within(query.wait_for_initial_load()).await.unwrap();
    let pause = server.pause_next();
    within(database.reset_for_testing()).await.unwrap();
    let waiting = tokio::time::timeout(std::time::Duration::from_millis(300), query.wait_for_initial_load()).await;
    assert!(waiting.is_err(), "wait_for_initial_load returned while the new load was still running");
    within(pause.entered.notified()).await;
    pause.release.add_permits(1);
    within(query.wait_for_initial_load()).await.unwrap();
    database.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sessionless_call_that_succeeds_after_a_logout_reopens_the_background() {
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    start_session(&database, "A").await;
    within(database.logout()).await.unwrap();
    assert!(!database.streams.snapshot().signed_in());
    within(database.session_call("otp".into(), "{}".into())).await.unwrap();
    assert!(database.streams.snapshot().signed_in(), "a successful sessionless call with a stored credential did not reopen the background");
    database.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_confirmed_logout_then_a_session_reset_and_a_clear_never_shows_the_account_to_the_next_login() {
    let server = Arc::new(Server::default());
    let database = open(server.clone()).await;
    start_session(&database, "A").await;
    let snapshots = Arc::new(Entities::default());
    let entities = database.live_entities("User".into(), snapshots.clone()).unwrap();
    entities.singleton(r#"{"model":"User","relation":"profile","params":{}}"#.into(), 1).unwrap();
    let rows = Arc::new(Rows::default());
    let query = database.live_query(r#"{"model":"User","relation":"all","params":{}}"#.into(), rows.clone());
    within(query.wait_for_initial_load()).await.unwrap();
    let changes = database.changes(r#"{"model":"User","relation":"all","params":{}}"#.into()).unwrap();
    within(database.settle_for_testing()).await.unwrap();
    within(database.session_call("logout".into(), "{}".into())).await.unwrap();
    assert!(database.session.as_ref().unwrap().signed_in().unwrap().is_none(), "the confirmed logout kept the session");
    database.session_reset().unwrap();
    within(database.logout()).await.unwrap();
    server.forbid(true);
    within(database.settle_for_testing()).await.unwrap();
    assert!(server.violations().is_empty(), "a held read reached the server between the logout and the next login: {:?}", server.violations());
    server.forbid(false);
    start_session(&database, "B").await;
    within(database.settle_for_testing()).await.unwrap();
    crate::test_wait::yield_until("the held entity and list show account B", || {
        let shown = snapshots.0.lock().unwrap();
        let listed = rows.0.lock().unwrap();
        shown.values().all(|row| row.value.as_ref().is_some_and(|row| named(row, "B"))) && !shown.is_empty() && !listed.is_empty() && listed.iter().all(|row| named(row, "B"))
    }).await;
    let states = delivered(&changes).await;
    assert!(states.iter().all(|rows| rows.iter().all(|row| !named(row, "A"))), "account A came back through the changes after the logout: {states:?}");
    assert!(states.last().is_some_and(|last| !last.is_empty() && last.iter().all(|row| named(row, "B"))));
    database.shutdown();
}
