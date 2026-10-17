use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use super::fake_server_tests::{FakeServer, MemoryCredentials};
use super::state;
use super::vectors::*;
use super::*;
use crate::db::Db;
use crate::pacing::ManualClock;

const START: u64 = 1_800_000_000;

fn config() -> EncryptedConfig {
    EncryptedConfig { origin: ORIGIN.to_string(), server_key: PUBLIC_KEY_DER_B64.to_string(), pin: PIN.to_string() }
}

struct Rig {
    server: Arc<FakeServer>,
    credentials: Arc<MemoryCredentials>,
    clock: Arc<ManualClock>,
}

impl Rig {
    fn new() -> Self {
        let clock = Arc::new(ManualClock::new(Duration::from_secs(START)));
        Self { server: FakeServer::new(clock.clone()), credentials: Arc::new(MemoryCredentials::default()), clock }
    }

    fn client(&self, db: &Db) -> EncryptedClient {
        EncryptedClient::new(&config(), self.server.clone(), self.credentials.clone(), db.clone(), self.clock.clone()).unwrap()
    }
}

fn memory_db() -> Db {
    Db::open_in_memory(Vec::new(), None).unwrap()
}

fn file_db(path: &Path) -> Db {
    Db::open(path, Vec::new(), None).unwrap()
}

fn rows(db: &Db) -> (Option<state::Session>, Option<state::Pending>) {
    let inner = db.shared.store.lock();
    (state::session(&inner.connection).unwrap(), state::pending(&inner.connection).unwrap())
}

fn graphql(query: &str) -> Value {
    json!({ "query": query })
}

fn fixed(request: &Value) -> Vec<Value> {
    ["requestId", "responseKey", "credential", "sequence", "path", "body"].iter().map(|field| request[field].clone()).collect()
}

#[tokio::test]
async fn a_mutation_is_stored_as_pending_before_it_is_sent_and_confirmed_by_its_sequence() {
    let rig = Rig::new();
    let db = memory_db();
    let client = rig.client(&db);
    let during = Arc::new(Mutex::new(Vec::new()));
    let (probe, log) = (db.clone(), during.clone());
    rig.server.on_request(move || log.lock().unwrap().push(rows(&probe)));

    let answer = client.mutate("/api/v3/auth/provider", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    client.mutate("/api/v3/graphql", graphql("mutation { a }")).await.unwrap();

    assert_eq!(answer.sequence, Some(1));
    let during = during.lock().unwrap();
    let pending = during[1].1.clone().unwrap();
    assert_eq!((pending.sequence, pending.path.as_str(), during[1].0.as_ref().unwrap().sequence), (2, "/api/v3/graphql", 1));
    let (session, pending) = rows(&db);
    assert_eq!((session.unwrap().sequence, pending), (2, None));
}

#[tokio::test]
async fn a_lost_answer_is_recovered_after_a_restart_by_the_same_request_with_a_fresh_issued_at() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("v3.sqlite");
    let client = rig.client(&file_db(&path));
    client.mutate("/api/v3/auth/provider", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    rig.server.lose_next_answer();
    assert!(matches!(client.mutate("/api/v3/graphql", graphql("mutation { once }")).await, Err(TransportError::Network(_))));
    drop(client);

    rig.clock.advance(Duration::from_secs(5));
    let db = file_db(&path);
    let answer = rig.client(&db).mutate("/api/v3/graphql", graphql("mutation { once }")).await.unwrap();

    let seen = rig.server.seen();
    assert_eq!(fixed(&seen[1]), fixed(&seen[2]));
    assert_eq!(seen[2]["issuedAt"].as_i64().unwrap() - seen[1]["issuedAt"].as_i64().unwrap(), 5);
    assert_eq!(answer.body, json!({ "data": { "done": "mutation { once }" } }));
    assert_eq!(rig.server.executed().len(), 2);
    assert_eq!(rows(&db).0.unwrap().sequence, 2);
    assert_eq!(rows(&db).1, None);
}

#[tokio::test]
async fn resume_sends_the_pending_request_without_the_original_call_and_does_nothing_without_one() {
    let rig = Rig::new();
    let db = memory_db();
    let client = rig.client(&db);
    assert_eq!(client.resume().await.unwrap(), None);
    rig.server.status_next(503);
    assert!(client.mutate("/api/v3/auth/otp", json!({ "phone": "+61400000000" })).await.is_err());

    let answer = client.resume().await.unwrap().unwrap();

    assert_eq!(answer.body, json!({ "nextStep": "authenticated" }));
    assert_eq!(rows(&db).1, None);
}

#[tokio::test]
async fn another_mutation_waits_for_the_pending_one_and_sends_nothing() {
    let rig = Rig::new();
    let db = memory_db();
    let client = rig.client(&db);
    rig.server.drop_next_request();
    assert!(client.mutate("/api/v3/auth/otp", json!({ "phone": "+61400000000" })).await.is_err());

    let refused = client.mutate("/api/v3/auth/otp", json!({ "phone": "+61411111111" })).await;

    assert!(matches!(refused, Err(TransportError::Network(_))));
    assert!(rig.server.seen().is_empty());
    assert_eq!(rows(&db).1.unwrap().body, json!({ "phone": "+61400000000" }).to_string());
}

#[tokio::test]
async fn reads_carry_a_null_sequence_leave_no_pending_and_a_replayed_read_is_retried_once_with_a_new_id() {
    let rig = Rig::new();
    let db = memory_db();
    let client = rig.client(&db);

    client.mutate("/api/v3/auth/provider", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    client.read("/api/v3/graphql", graphql("query { a }")).await.unwrap();
    rig.server.replay_next_read();
    let answer = client.read("/api/v3/graphql", graphql("query { a }")).await.unwrap();

    let seen = rig.server.seen();
    assert_eq!(seen.iter().map(|request| request["sequence"].clone()).collect::<Vec<_>>(), [json!(1), Value::Null, Value::Null, Value::Null]);
    assert_eq!(seen.iter().map(|request| request["requestId"].clone()).collect::<std::collections::HashSet<_>>().len(), 4);
    assert_eq!(answer.body, json!({ "data": { "read": "query { a }" } }));
    assert_eq!(rows(&db).1, None);
}

#[tokio::test]
async fn http_refusals_keep_the_pending_request_and_the_next_attempt_executes_it_once() {
    let rig = Rig::new();
    let db = memory_db();
    let client = rig.client(&db);
    client.mutate("/api/v3/auth/provider", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    let body = graphql("mutation { m }");
    let mut statuses = Vec::new();
    for status in [503, 400] {
        rig.server.status_next(status);
        let Err(TransportError::Http { status, .. }) = client.mutate("/api/v3/graphql", body.clone()).await else { panic!("expected an HTTP error") };
        statuses.push(status);
    }

    assert_eq!(statuses, [503, 400]);
    assert_eq!(rows(&db).1.unwrap().sequence, 2);
    client.mutate("/api/v3/graphql", body).await.unwrap();
    assert_eq!(rig.server.executed().len(), 2);
}

#[tokio::test]
async fn a_sequence_conflict_is_an_error_and_keeps_the_pending_request() {
    let rig = Rig::new();
    let db = memory_db();
    let credential = "c".repeat(43);
    rig.credentials.set(&credential).unwrap();
    let server = rig.server.clone().with_sequence(&credential, 5);
    let client = EncryptedClient::new(&config(), server, rig.credentials.clone(), db.clone(), rig.clock.clone()).unwrap();
    {
        let inner = db.shared.store.lock();
        state::start(&inner.connection, &super::client::digest_of(&credential)).unwrap();
    }

    let outcome = client.mutate("/api/v3/graphql", graphql("mutation { x }")).await;

    assert_eq!(outcome, Err(TransportError::Graphql { messages: vec!["SEQUENCE_CONFLICT".to_string()] }));
    assert_eq!(rows(&db).1.unwrap().sequence, 1);
}

#[tokio::test]
async fn a_revoked_logout_clears_the_credential_and_the_state_even_when_its_answer_was_lost_first() {
    let rig = Rig::new();
    let db = memory_db();
    let client = rig.client(&db);
    client.mutate("/api/v3/auth/provider", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    rig.server.lose_next_answer();
    assert!(client.mutate("/api/v3/logout", json!({})).await.is_err());
    assert!(rig.credentials.value().is_some());

    let answer = client.resume().await.unwrap().unwrap();

    assert_eq!(answer.body, json!({ "revoked": true }));
    assert_eq!((rig.credentials.value(), rows(&db)), (None, (None, None)));
}

#[tokio::test]
async fn the_credential_is_saved_before_the_first_request_and_never_written_to_sqlite() {
    let rig = Rig::new();
    let db = memory_db();
    let (store, saved) = (rig.credentials.clone(), Arc::new(Mutex::new(None)));
    let witness = saved.clone();
    rig.server.on_request(move || *witness.lock().unwrap() = store.value());

    rig.client(&db).mutate("/api/v3/auth/otp", json!({ "phone": "+61400000000" })).await.unwrap();

    let credential = rig.credentials.value().unwrap();
    assert_eq!(saved.lock().unwrap().as_deref(), Some(credential.as_str()));
    assert_eq!(credential.len(), 43);
    let inner = db.shared.store.lock();
    let dump: String = inner
        .connection
        .query_row("SELECT group_concat(credential_digest || sequence, ' ') FROM encrypted_session", [], |row| row.get(0))
        .unwrap();
    assert!(!dump.contains(&credential));
}

#[tokio::test]
async fn a_credential_without_a_matching_session_row_is_replaced_by_a_new_session() {
    let rig = Rig::new();
    rig.credentials.set("from-an-older-install").unwrap();
    let db = memory_db();

    rig.client(&db).read("/api/v3/graphql", graphql("query { a }")).await.unwrap();

    let credential = rig.credentials.value().unwrap();
    assert_ne!(credential, "from-an-older-install");
    assert_eq!(rig.server.seen()[0]["credential"], credential);
}

#[tokio::test]
async fn a_response_for_another_request_is_refused() {
    let rig = Rig::new();
    rig.server.reply_to_other_request();

    let outcome = rig.client(&memory_db()).mutate("/api/v3/auth/otp", json!({ "phone": "+61400000000" })).await;

    assert!(matches!(outcome, Err(TransportError::Decode(_))));
}

#[test]
fn the_client_requires_an_https_origin_without_a_path_and_a_pinned_key() {
    let rig = Rig::new();
    let make = |origin: &str, pin: &str| {
        let config = EncryptedConfig { origin: origin.to_string(), server_key: PUBLIC_KEY_DER_B64.to_string(), pin: pin.to_string() };
        EncryptedClient::new(&config, rig.server.clone(), rig.credentials.clone(), memory_db(), rig.clock.clone()).is_ok()
    };

    assert!(make("https://api.example.com", PIN));
    assert!(make("https://api.example.com/", PIN));
    assert!(!make("https://api.example.com/api", PIN));
    assert!(!make("https://api.example.com", SHORT_KEY_PIN));
}

#[tokio::test]
async fn a_request_over_256_kib_is_refused_without_being_sent() {
    let rig = Rig::new();

    let outcome = rig.client(&memory_db()).read("/api/v3/graphql", graphql(&"q".repeat(300 * 1024))).await;

    assert!(matches!(outcome, Err(TransportError::Http { status: 413, .. })));
    assert!(rig.server.seen().is_empty());
}

#[tokio::test]
async fn a_pending_request_of_a_replaced_credential_is_dropped_and_never_sent() {
    let rig = Rig::new();
    let db = memory_db();
    let client = rig.client(&db);
    rig.server.drop_next_request();
    assert!(client.mutate("/api/v3/auth/otp", json!({ "phone": "+61400000000" })).await.is_err());
    let restored = rig.client(&db);
    rig.credentials.set(&"r".repeat(43)).unwrap();

    assert_eq!(restored.resume().await, Ok(None));

    assert_ne!(rig.credentials.value(), Some("r".repeat(43)));
    assert_eq!((rig.server.seen().len(), rows(&db).1), (0, None));
}

#[tokio::test]
async fn encrypted_paths_never_carry_a_query_or_a_fragment() {
    let rig = Rig::new();
    let client = rig.client(&memory_db());

    for path in ["/api/v3/graphql?platform=ios", "/api/v3/graphql#frag"] {
        assert!(matches!(client.read(path, graphql("{ a }")).await, Err(TransportError::Decode(_))), "{path}");
        assert!(matches!(client.mutate(path, graphql("{ a }")).await, Err(TransportError::Decode(_))), "{path}");
    }
}
