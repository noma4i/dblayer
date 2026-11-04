use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use super::fake_server_tests::{FakeServer, MemoryCredentials};
use super::state;
use super::vectors_tests::*;
use super::*;
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::transport::SessionCode;

const START: u64 = 1_800_000_000;

pub(crate) fn cancel_pending(call: impl std::future::Future) {
    use std::task::{Context, Waker};
    assert!(Box::pin(call).as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
}

fn config() -> EncryptedConfig {
    EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() }
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

#[test]
fn pending_reads_distinguish_missing_corrupt_and_sqlite_failure() {
    for case in ["missing", "valid", "corrupt", "partial_owner", "sqlite"] {
        let db = memory_db();
        let rig = Rig::new();
        let client = rig.client(&db);
        {
            let stored = db.shared.store.lock();
            if case == "sqlite" {
                stored.connection.execute_batch("DROP TABLE encrypted_pending").unwrap();
            } else if case != "missing" {
                state::store_pending(&stored.connection, &state::Pending {
                    credential_digest: "digest".into(), request_id: "request".into(), response_key: "response".into(), sequence: 1,
                    path: "/graphql".into(), body: if case == "corrupt" { "{".into() } else { "{}".into() },
                    owner: Some(crate::transport::RequestOwner { seq: 7, item_position: 0, stage: "put".into() }), upload: true,
                }).unwrap();
                if case == "partial_owner" { stored.connection.execute_batch("UPDATE encrypted_pending SET stage = NULL").unwrap(); }
            }
        }
        let call = client.pending_call();
        let matching = super::graphql::pending_matches(&db, 7, 0, "put");
        match case {
            "missing" => { assert_eq!(call.unwrap(), None); assert!(!matching.unwrap()); }
            "valid" => { assert_eq!(call.unwrap(), Some(("/graphql".into(), json!({})))); assert!(matching.unwrap()); }
            _ => { assert!(call.is_err(), "{case}"); assert!(matching.is_err(), "{case}"); }
        }
    }
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
async fn resume_discards_a_legacy_sessionless_pending_without_sending() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let db = file_db(&path);
    let client = rig.client(&db);
    rig.server.drop_next_request();
    cancel_pending(client.mutate("/api/auth/otp", json!({})));
    drop(client);
    drop(db);
    let db = file_db(&path);
    let mut config = config();
    config.endpoints.session_operations.get_mut("otp").unwrap().sessionless = true;
    let client = EncryptedClient::new(&config, rig.server.clone(), rig.credentials.clone(), db.clone(), rig.clock.clone()).unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = calls.clone();
    rig.server.on_request(move || { counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst); });
    assert_eq!(client.resume().await, Ok(None));
    assert_eq!(rows(&db).1, None);
    assert_eq!(rows(&db).0.unwrap().sequence, 0);
    assert_eq!(client.resume().await, Ok(None));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    client.session_call("social", json!({})).await.unwrap();
    assert_eq!(rows(&db).0.unwrap().sequence, 1);
}

#[tokio::test]
async fn a_sessionless_call_does_not_wait_for_an_inflight_mutation() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    struct Waiting;
    #[async_trait::async_trait]
    impl HttpExchange for Waiting {
        async fn post(&self, path: &str, _: String, _: bool) -> Result<HttpReply, TransportError> {
            if path == "/api/ticket" { return Ok(HttpReply { status: 400, body: String::new() }); }
            std::future::pending().await
        }
    }
    let rig = Rig::new();
    let db = memory_db();
    let mut config = config();
    config.endpoints.session_operations.insert("ticket".into(), EncryptedSessionOperation { path: "/api/ticket".into(), sessionless: true });
    let client = EncryptedClient::new(&config, Arc::new(Waiting), rig.credentials, db.clone(), rig.clock).unwrap();
    let mut mutation = Box::pin(client.mutate("/api/auth/otp", json!({})));
    assert!(mutation.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    let before = rows(&db);
    let mut sessionless = Box::pin(client.session_call("ticket", json!({})));
    assert!(matches!(sessionless.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(Err(TransportError::Http { status: 400, .. }))));
    assert_eq!(rows(&db), before);
}

#[tokio::test]
async fn a_mutation_is_stored_as_pending_before_it_is_sent_and_confirmed_by_its_sequence() {
    let rig = Rig::new();
    let db = memory_db();
    let client = rig.client(&db);
    let during = Arc::new(Mutex::new(Vec::new()));
    let (probe, log) = (db.clone(), during.clone());
    rig.server.on_request(move || log.lock().unwrap().push(rows(&probe)));

    let answer = client.mutate("/api/auth/social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    client.mutate("/api/graphql", graphql("mutation { a }")).await.unwrap();

    assert_eq!(answer.sequence, Some(1));
    let during = during.lock().unwrap();
    let pending = during[1].1.clone().unwrap();
    assert_eq!((pending.sequence, pending.path.as_str(), during[1].0.as_ref().unwrap().sequence), (2, "/api/graphql", 1));
    let (session, pending) = rows(&db);
    assert_eq!((session.unwrap().sequence, pending), (2, None));
}

#[tokio::test]
async fn a_lost_answer_is_recovered_after_a_restart_by_the_same_request_with_a_fresh_issued_at() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let client = rig.client(&file_db(&path));
    client.mutate("/api/auth/social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    rig.server.lose_next_answer();
    cancel_pending(client.mutate("/api/graphql", graphql("mutation { once }")));
    drop(client);

    rig.clock.advance(Duration::from_secs(5));
    let db = file_db(&path);
    let answer = rig.client(&db).mutate("/api/graphql", graphql("mutation { once }")).await.unwrap();

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
    cancel_pending(client.mutate("/api/auth/otp", json!({ "phone": "+61400000000" })));

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
    cancel_pending(client.mutate("/api/auth/otp", json!({ "phone": "+61400000000" })));

    use std::{future::Future, task::{Context, Poll, Waker}};
    let mut refused = Box::pin(client.mutate("/api/auth/otp", json!({ "phone": "+61411111111" })));
    assert!(matches!(refused.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(Err(TransportError::Network(_)))));
    assert!(rig.server.seen().is_empty());
    assert_eq!(rows(&db).1.unwrap().body, json!({ "phone": "+61400000000" }).to_string());
}

#[tokio::test]
async fn reads_carry_a_null_sequence_leave_no_pending_and_a_replayed_read_is_retried_once_with_a_new_id() {
    let rig = Rig::new();
    let db = memory_db();
    let client = rig.client(&db);

    client.mutate("/api/auth/social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    client.read(&db.admit().unwrap(), "/api/graphql", graphql("query { a }")).await.unwrap();
    rig.server.replay_next_read();
    let answer = client.read(&db.admit().unwrap(), "/api/graphql", graphql("query { a }")).await.unwrap();

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
    client.mutate("/api/auth/social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    let body = graphql("mutation { m }");
    for status in [503, 500] {
        rig.server.status_next(status);
        cancel_pending(client.mutate("/api/graphql", body.clone()));
    }
    assert_eq!(rows(&db).1.unwrap().sequence, 2);
    client.mutate("/api/graphql", body).await.unwrap();
    assert_eq!(rig.server.executed().len(), 2);
}

#[tokio::test]
async fn permanent_http_refusals_drop_pending_without_spending_sequence() {
    for (status, permanent) in [(400, true), (404, true), (405, true), (413, true), (415, true), (422, true), (401, false), (403, false), (418, false)] {
        for resume in [false, true] {
            let rig = Rig::new();
            let db = memory_db();
            let client = rig.client(&db);
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter = calls.clone();
            rig.server.on_request(move || { counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst); });
            if resume {
                rig.server.drop_next_request();
                cancel_pending(client.mutate("/api/auth/otp", json!({})));
            }
            rig.server.status_next(status);
            let result = if resume { client.resume().await } else { client.mutate("/api/auth/otp", json!({})).await.map(Some) };
            if resume && permanent { assert_eq!(result, Ok(None), "status {status}"); }
            else { assert!(matches!(result, Err(TransportError::Http { status: actual, .. }) if actual == status), "{result:?}"); }
            assert_eq!(rows(&db).1.is_none(), permanent, "status {status}, resume {resume}");
            assert_eq!(rows(&db).0.unwrap().sequence, 0);
            let before = calls.load(std::sync::atomic::Ordering::SeqCst);
            assert_eq!(client.resume().await.unwrap().is_none(), permanent);
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), before + usize::from(!permanent));
            client.mutate("/api/auth/otp", json!({})).await.unwrap();
            assert_eq!(rows(&db).0.unwrap().sequence, if permanent { 1 } else { 2 });
        }
    }
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

    let outcome = client.mutate("/api/graphql", graphql("mutation { x }")).await;

    assert_eq!(outcome, Err(TransportError::Session { code: SessionCode::SequenceConflict, detail: "SEQUENCE_CONFLICT".to_string() }));
    assert_eq!(rows(&db).1.unwrap().sequence, 1);
}

#[tokio::test]
async fn a_revoked_logout_clears_the_credential_and_the_state_even_when_its_answer_was_lost_first() {
    let rig = Rig::new();
    let db = memory_db();
    let client = rig.client(&db);
    client.mutate("/api/auth/social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    rig.server.lose_next_answer();
    cancel_pending(client.mutate("/api/logout", json!({})));
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

    rig.client(&db).mutate("/api/auth/otp", json!({ "phone": "+61400000000" })).await.unwrap();

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
async fn a_response_for_another_request_is_refused() {
    let rig = Rig::new();
    rig.server.reply_to_other_request();

    let outcome = rig.client(&memory_db()).mutate("/api/auth/otp", json!({ "phone": "+61400000000" })).await;

    assert!(matches!(outcome, Err(TransportError::Decode(_))));
}

#[test]
fn the_client_requires_an_https_origin_without_a_path_and_a_pinned_key() {
    let rig = Rig::new();
    let make = |origin: &str, pin: &str| {
        let config = EncryptedConfig { origin: origin.to_string(), server_key: public_key_der_b64().to_string(), pin: pin.to_string(), ..crate::transport::encrypted::protocol_tests::config() };
        EncryptedClient::new(&config, rig.server.clone(), rig.credentials.clone(), memory_db(), rig.clock.clone()).is_ok()
    };

    assert!(make("https://api.example.com", pin()));
    assert!(make("https://api.example.com/", pin()));
    assert!(!make("https://api.example.com/api", pin()));
    assert!(!make("https://api.example.com", short_key_pin()));
}

#[tokio::test]
async fn a_request_over_256_kib_is_refused_without_being_sent() {
    let rig = Rig::new();

    let db = memory_db();
    let outcome = rig.client(&db).read(&db.admit().unwrap(), "/api/graphql", graphql(&"q".repeat(300 * 1024))).await;

    assert!(matches!(outcome, Err(TransportError::RequestTooLarge { size, limit }) if size > limit && limit == 256 * 1024));
    assert!(rig.server.seen().is_empty());
}

#[tokio::test]
async fn local_413_does_not_poison_the_session() {
    for path in ["/api/graphql", "/api/logout", "/api/auth/social"] {
        let rig = Rig::new();
        let db = memory_db();
        let client = rig.client(&db);
        client.mutate("/api/auth/social", json!({"provider":"google","idToken":"t"})).await.unwrap();
        let before = rows(&db);
        let sent = rig.server.seen().len();
        assert!(matches!(client.mutate(path, graphql(&"x".repeat(260_000))).await, Err(TransportError::RequestTooLarge { size, limit }) if size > limit && limit == 256 * 1024));
        assert_eq!(rig.server.seen().len(), sent);
        assert_eq!(rows(&db), before, "{path}");
        assert_eq!(client.resume().await.unwrap(), None);
        client.mutate("/api/graphql", graphql("mutation Small { x }")).await.unwrap();
        assert_eq!(rows(&db).0.unwrap().sequence, 2);
    }
}

#[tokio::test(start_paused = true)]
async fn a_confirmed_retry_that_exceeds_the_size_limit_does_not_poison_pending() {
    let rig = Rig::new();
    let db = memory_db();
    let client = rig.client(&db);
    let credential = client.credential(&client.admit("test credential").unwrap()).unwrap();
    rig.server.clone().with_sequence(&credential, 8);
    db.shared.store.lock().connection.execute("UPDATE encrypted_session SET sequence = 8", []).unwrap();
    let mut low = 0;
    let mut high: usize = 256 * 1024;
    while low < high {
        let size = (low + high).div_ceil(2);
        rig.server.status_next(400);
        match client.mutate("/api/auth/social", json!({"payload": "x".repeat(size)})).await {
            Err(TransportError::Http { status: 400, .. }) => low = size,
            Err(TransportError::RequestTooLarge { .. }) => high = size - 1,
            result => panic!("unexpected size probe {result:?}"),
        }
    }
    let body = json!({"payload": "x".repeat(low)});
    rig.server.status_next(400);
    assert!(matches!(client.mutate("/api/auth/social", body.clone()).await, Err(TransportError::Http { status: 400, .. })));
    rig.server.refuse_next("TIMEOUT", true);
    assert!(matches!(client.mutate("/api/auth/social", body).await, Err(TransportError::RequestTooLarge { .. })));
    assert_eq!(rows(&db).0.unwrap().sequence, 9);
    assert!(rows(&db).1.is_none(), "oversized retry remained pending");
    client.mutate("/api/auth/social", json!({})).await.unwrap();
    assert_eq!(rows(&db).0.unwrap().sequence, 10);
}

#[tokio::test]
async fn a_pending_request_of_a_replaced_credential_is_reported_and_never_sent() {
    let rig = Rig::new();
    let db = memory_db();
    let client = rig.client(&db);
    rig.server.drop_next_request();
    cancel_pending(client.mutate("/api/auth/otp", json!({ "phone": "+61400000000" })));
    let restored = rig.client(&db);
    rig.credentials.set(&"r".repeat(43)).unwrap();

    let before = rows(&db);
    assert_eq!(restored.resume().await, Err(TransportError::Session { code: SessionCode::StateLost, detail: "lost session sequence state for the stored credential".into() }));

    assert_eq!(rig.credentials.value(), Some("r".repeat(43)));
    assert_eq!((rig.server.seen().len(), rows(&db)), (0, before));
}

#[tokio::test]
async fn encrypted_paths_never_carry_a_query_or_a_fragment() {
    let rig = Rig::new();
    let client = rig.client(&memory_db());

    for path in ["/api/graphql?platform=ios", "/api/graphql#frag"] {
        assert!(matches!(client.read(&client.admit("test read").unwrap(), path, graphql("{ a }")).await, Err(TransportError::Decode(_))), "{path}");
        assert!(matches!(client.mutate(path, graphql("{ a }")).await, Err(TransportError::Decode(_))), "{path}");
    }
}
