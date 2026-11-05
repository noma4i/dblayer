//! The executor of Session against `FakeServer`: the owners on the threaded `Runtime`, the HTTP exchange sealed and opened by `EncryptedClient`.
//! Every wait has a limit of 10 s.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use super::LifeReply;
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::transport::encrypted::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::transport::encrypted::state;
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};
use crate::transport::{EncryptedClient, EncryptedConfig};

const LIMIT: Duration = Duration::from_secs(10);
const OTP: &str = "/api/auth/otp";

fn config() -> EncryptedConfig {
    EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() }
}

struct World {
    server: Arc<FakeServer>,
    credentials: Arc<MemoryCredentials>,
    clock: Arc<ManualClock>,
}

impl World {
    fn new() -> Self {
        let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
        Self { server: FakeServer::new(clock.clone()), credentials: Arc::new(MemoryCredentials::default()), clock }
    }

    async fn open(&self, db: &Db) -> (Arc<EncryptedClient>, Arc<crate::owners::life::Life>) {
        let client = Arc::new(EncryptedClient::new(&config(), self.server.clone(), self.credentials.clone(), db.clone(), self.clock.clone()).unwrap());
        let life = client.start_owners_in_tests(db);
        (client, life)
    }
}

fn rows(db: &Db) -> (Option<state::Session>, Option<state::Pending>) {
    let inner = db.shared.store.lock();
    (state::session(&inner.connection).unwrap(), state::pending(&inner.connection).unwrap())
}

async fn answer(waiter: crate::runtime::Waiter<LifeReply>) -> LifeReply {
    match tokio::time::timeout(LIMIT, waiter.wait()).await.expect("the owner answered within 10 s") {
        crate::runtime::Outcome::Value(reply) => reply,
        other => panic!("the waiter ended without a value: {other:?}"),
    }
}

async fn sign_in(client: &EncryptedClient) {
    client.session_call_in_tests("social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn sign_in_saves_the_credential_before_the_request_and_publishes_its_identity() {
    let world = World::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let (client, life) = world.open(&db).await;
    let before = life.published();
    assert_eq!(before.identity, None);
    sign_in(&client).await;
    let credential = world.credentials.value().expect("the credential was saved");
    let (session, pending) = rows(&db);
    let session = session.expect("the session row");
    assert_eq!(session.credential_digest, crate::transport::encrypted::client::digest_of(&credential));
    assert_eq!((session.sequence, pending.is_none()), (1, true));
    let seen = world.server.seen();
    assert_eq!(seen.len(), 1, "one request in the network");
    assert_eq!(seen[0]["sequence"], json!(1));
    let published = life.published();
    assert_eq!(published.identity, crate::store::identity_of(&session.credential_digest));
    assert!(published.epoch > before.epoch, "the sign-in started a new epoch");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mutate_goes_out_once_with_the_next_sequence_and_its_answer_reaches_the_author() {
    let world = World::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let (client, life) = world.open(&db).await;
    sign_in(&client).await;
    let reply = answer(life.mutate_wait(None, OTP.into(), json!({}).to_string(), 0)).await;
    assert!(matches!(reply, LifeReply::Mutated(crate::owners::session::AuthorOutcome::Response(_))), "{reply:?}");
    let (session, pending) = rows(&db);
    assert_eq!((session.unwrap().sequence, pending.is_none()), (2, true));
    assert_eq!(world.server.seen().iter().map(|request| request["sequence"].clone()).collect::<Vec<Value>>(), [json!(1), json!(2)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_answer_is_repeated_with_the_same_request_id_and_executes_once() {
    let world = World::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let (client, life) = world.open(&db).await;
    sign_in(&client).await;
    world.server.lose_next_answer();
    let reply = answer(life.mutate_wait(None, OTP.into(), json!({}).to_string(), 0)).await;
    assert!(matches!(reply, LifeReply::Mutated(crate::owners::session::AuthorOutcome::Response(_))), "{reply:?}");
    let seen = world.server.seen();
    assert!(seen.len() >= 3, "the sign-in, the request and its repeat");
    assert!(seen[1..].iter().all(|request| request["requestId"] == seen[1]["requestId"]), "every repeat carries the request id of the first send");
    assert_eq!(world.server.executed().len(), 2, "the sign-in and the mutation each executed once");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_over_the_size_limit_is_refused_and_spends_no_sequence() {
    let world = World::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let (client, life) = world.open(&db).await;
    sign_in(&client).await;
    let reply = answer(life.mutate_wait(None, OTP.into(), json!({ "text": "x".repeat(300_000) }).to_string(), 0)).await;
    assert!(matches!(reply, LifeReply::Mutated(crate::owners::session::AuthorOutcome::Error(_))), "{reply:?}");
    let (session, pending) = rows(&db);
    assert_eq!((session.unwrap().sequence, pending.is_none()), (1, true), "the refused request left no pending and spent no sequence");
    let next = answer(life.mutate_wait(None, OTP.into(), json!({}).to_string(), 0)).await;
    assert!(matches!(next, LifeReply::Mutated(crate::owners::session::AuthorOutcome::Response(_))), "{next:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pending_request_stored_before_the_close_is_sent_again_when_the_owners_open() {
    let world = World::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let (request_id, key) = {
        let db = Db::open(&path, Vec::new(), None).unwrap();
        let (client, life) = world.open(&db).await;
        sign_in(&client).await;
        world.server.drop_next_request();
        let waiter = life.mutate_wait(None, OTP.into(), json!({}).to_string(), 0);
        let started = std::time::Instant::now();
        let pending = loop {
            if let (_, Some(pending)) = rows(&db) { break pending; }
            assert!(started.elapsed() < LIMIT, "the pending request was never stored");
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        drop(waiter);
        let closed = life.close();
        let _ = tokio::time::timeout(LIMIT, closed.wait()).await;
        (pending.request_id, pending.response_key)
    };
    assert!(!key.is_empty());
    let db = Db::open(&path, Vec::new(), None).unwrap();
    let (_client, _life) = world.open(&db).await;
    let started = std::time::Instant::now();
    while rows(&db).1.is_some() {
        assert!(started.elapsed() < LIMIT, "the stored request was not sent again");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(world.server.seen().iter().any(|request| request["requestId"] == json!(request_id)), "the request left again with its request id");
    assert_eq!(rows(&db).0.unwrap().sequence, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stored_request_of_an_older_format_is_sent_again_with_the_request_id_and_sequence_it_was_stored_with() {
    let world = World::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let old_id = "3f1c2a52-9d3e-4b7a-8a11-0123456789ab";
    {
        let db = Db::open(&path, Vec::new(), None).unwrap();
        let (client, life) = world.open(&db).await;
        sign_in(&client).await;
        let _ = tokio::time::timeout(LIMIT, life.close().wait()).await;
    }
    {
        let db = Db::open(&path, Vec::new(), None).unwrap();
        let digest = rows(&db).0.unwrap().credential_digest;
        let pending = state::Pending { credential_digest: digest, request_id: old_id.into(), response_key: crate::transport::encrypted::jwe::random_token(), sequence: 2, path: OTP.into(), body: json!({}).to_string(), owner: None, purpose: crate::transport::NetworkPurpose::Standard };
        let inner = db.shared.store.lock();
        state::store_pending(&inner.connection, &pending).unwrap();
    }
    let db = Db::open(&path, Vec::new(), None).unwrap();
    let (_client, _life) = world.open(&db).await;
    let started = std::time::Instant::now();
    while rows(&db).1.is_some() {
        assert!(started.elapsed() < LIMIT, "the stored request of the older format was not sent again");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let again: Vec<Value> = world.server.seen().into_iter().filter(|request| request["requestId"] == json!(old_id)).collect();
    assert_eq!(again.len(), 1, "one request left with the stored request id");
    assert_eq!(again[0]["sequence"], json!(2), "and the stored sequence");
    assert_eq!(rows(&db).0.unwrap().sequence, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_stored_request_fails_the_open_with_a_storage_error() {
    let world = World::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let client = Arc::new(EncryptedClient::new(&config(), world.server.clone(), world.credentials.clone(), db.clone(), world.clock.clone()).unwrap());
    {
        let inner = db.shared.store.lock();
        inner.connection.execute("INSERT INTO encrypted_pending (id, credential_digest, request_id, response_key, sequence, path, body, purpose) VALUES (1, 'd', 'r', 'k', 1, '/p', 'not json', 0)", []).unwrap();
    }
    let credentials: Arc<dyn crate::transport::CredentialSlot> = world.credentials.clone();
    let ports = super::Ports { db: db.clone(), transport: None, executor: tokio::runtime::Handle::current(), operations: Arc::new(|_| None), credentials: Some(credentials), session: Some(client), transfer: None };
    let error = super::Life::start(ports, LIMIT).err().expect("the open fails");
    assert!(matches!(error, super::LifeError::Storage(_)), "the read error reaches the caller: {error:?}");
}
