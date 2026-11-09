//! The executor of Session against `FakeServer`: the owners on the threaded `Runtime`, the HTTP exchange sealed and opened by `EncryptedClient`.
//! Every wait has a limit of 10 s.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use super::LifeReply;
use crate::store::Store;
use crate::test_wait::settled;
use crate::runtime::ManualClock;
use crate::transport::encrypted::tests::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::store::session_state;
use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64};
use crate::transport::EncryptedConfig;
use crate::transport::encrypted::tests::exchange_tests::Client;

const OTP: &str = "/api/auth/otp";

fn config() -> EncryptedConfig {
    let mut config = config_plain();
    config.endpoints.session_operations.insert("first_login".into(), crate::transport::EncryptedSessionOperation { path: "/api/auth/social".into(), class: crate::transport::EncryptedOperationClass::Login });
    config
}

fn config_plain() -> EncryptedConfig {
    EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::tests::protocol_tests::config() }
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

    async fn open(&self, db: &Arc<Store>) -> (Arc<Client>, Arc<crate::owners::life::Life>) {
        let client = Client::new(&config(), self.server.clone(), self.credentials.clone(), db.clone(), self.clock.clone(), Arc::new(crate::transport::MockTransport::new())).unwrap();
        let life = client.start_owners();
        crate::test_wait::within(life.set_online(true).wait()).await;
        (client, life)
    }
}

fn rows(db: &Store) -> (Option<session_state::Session>, Option<session_state::Pending>) {
    let inner = db.lock();
    (session_state::session(&inner.connection).unwrap(), session_state::pending(&inner.connection).unwrap())
}

async fn answer(waiter: crate::runtime::Waiter<LifeReply>) -> LifeReply {
    match crate::test_wait::within(waiter.wait()).await {
        crate::runtime::Outcome::Value(reply) => reply,
        other => panic!("the waiter ended without a value: {other:?}"),
    }
}

static NEXT_REQUEST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1 << 40);

/// One `Mutate` of an author under its own id; the outcome comes through the sink of the author.
async fn attempt(life: &crate::owners::life::Life, req_id: u64, body: String) -> LifeReply {
    match crate::test_wait::within(life.mutate_answered(None, req_id, OTP.into(), body, 0).wait()).await {
        crate::runtime::Outcome::Value(reply) => reply,
        other => panic!("the author has an outcome: {other:?}"),
    }
}

async fn mutate_once(life: &crate::owners::life::Life, body: String) -> LifeReply {
    attempt(life, NEXT_REQUEST.fetch_add(1, std::sync::atomic::Ordering::SeqCst), body).await
}

async fn sign_in(client: &Client) {
    client.session_call("social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn sign_in_saves_the_credential_before_the_request_and_publishes_its_identity() {
    let world = World::new();
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let (client, life) = world.open(&db).await;
    let before = life.published();
    assert_eq!(before.identity, None);
    sign_in(&client).await;
    let credential = world.credentials.value().expect("the credential was saved");
    let (session, pending) = rows(&db);
    let session = session.expect("the session row");
    assert_eq!(session.credential_digest, crate::transport::encrypted::exchange::digest_of(&credential));
    assert_eq!((session.sequence, pending.is_none()), (1, true));
    let seen = world.server.seen();
    assert_eq!(seen.len(), 1, "one request in the network");
    assert_eq!(seen[0]["sequence"], json!(1));
    let published = life.published();
    assert_eq!(published.identity, crate::store::identity_of(&session.credential_digest));
    assert!(published.epoch > before.epoch, "the sign-in started a new epoch");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_login_class_call_with_a_session_fails_without_a_request() {
    let world = World::new();
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let (client, _life) = world.open(&db).await;
    sign_in(&client).await;
    let sent = world.server.seen().len();
    let error = client.session_call("first_login", json!({ "provider": "google", "idToken": "t" })).await.unwrap_err();
    assert!(matches!(error, TransportError::Session { code: crate::transport::SessionCode::AlreadyAuthenticated, .. }), "{error:?}");
    assert_eq!(world.server.seen().len(), sent);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mutate_goes_out_once_with_the_next_sequence_and_its_answer_reaches_the_author() {
    let world = World::new();
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let (client, life) = world.open(&db).await;
    sign_in(&client).await;
    let reply = mutate_once(&life, json!({}).to_string()).await;
    assert!(matches!(reply, LifeReply::Mutated(crate::owners::session::AuthorOutcome::Response(_))), "{reply:?}");
    let (session, pending) = rows(&db);
    assert_eq!((session.unwrap().sequence, pending.is_none()), (2, true));
    assert_eq!(world.server.seen().iter().map(|request| request["sequence"].clone()).collect::<Vec<Value>>(), [json!(1), json!(2)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_answer_of_a_mutate_is_stored_with_the_removal_of_its_pending() {
    let world = World::new();
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let (client, life) = world.open(&db).await;
    sign_in(&client).await;
    let reply = mutate_once(&life, json!({}).to_string()).await;
    let crate::owners::life::LifeReply::Mutated(crate::owners::session::AuthorOutcome::Response(body)) = reply else { panic!("{reply:?}") };
    let (_, pending) = rows(&db);
    let answers = {
        let inner = db.lock();
        session_state::answers(&inner.connection).unwrap()
    };
    assert!(pending.is_none());
    assert_eq!(answers.values().collect::<Vec<_>>(), [&session_state::StoredAnswer::Ok(body)], "the sign-in is a call without an author and leaves no answer");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_answer_is_repeated_with_the_same_request_id_and_executes_once() {
    let world = World::new();
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let (client, life) = world.open(&db).await;
    sign_in(&client).await;
    world.server.lose_next_answer();
    let req_id = NEXT_REQUEST.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let lost = attempt(&life, req_id, json!({}).to_string()).await;
    assert!(matches!(lost, LifeReply::Mutated(crate::owners::session::AuthorOutcome::Dropped | crate::owners::session::AuthorOutcome::Error { class: crate::owners::contract::ErrorClass::Network | crate::owners::contract::ErrorClass::Server, .. })), "{lost:?}");
    let reply = attempt(&life, req_id, json!({}).to_string()).await;
    assert!(matches!(reply, LifeReply::Mutated(crate::owners::session::AuthorOutcome::Response(_))), "{reply:?}");
    let seen = world.server.seen();
    assert_eq!(seen.len(), 3, "the sign-in, the request and its repeat");
    assert_eq!(seen[1]["requestId"], seen[2]["requestId"], "the repeat carries the request id of the first send");
    assert_eq!(world.server.executed().len(), 2, "the sign-in and the mutation each executed once");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_over_the_size_limit_is_refused_and_spends_no_sequence() {
    let world = World::new();
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let (client, life) = world.open(&db).await;
    sign_in(&client).await;
    let reply = mutate_once(&life, json!({ "text": "x".repeat(300_000) }).to_string()).await;
    assert!(matches!(reply, LifeReply::Mutated(crate::owners::session::AuthorOutcome::Error { .. })), "{reply:?}");
    let (session, pending) = rows(&db);
    assert_eq!((session.unwrap().sequence, pending.is_none()), (1, true), "the refused request left no pending and spent no sequence");
    let next = mutate_once(&life, json!({}).to_string()).await;
    assert!(matches!(next, LifeReply::Mutated(crate::owners::session::AuthorOutcome::Response(_))), "{next:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pending_request_stored_before_the_close_is_sent_again_when_the_owners_open() {
    let world = World::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let (request_id, key) = {
        let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
        let (client, life) = world.open(&db).await;
        sign_in(&client).await;
        world.server.drop_next_request();
        let waiter = life.mutate_for_call(None, OTP.into(), json!({}).to_string(), 0);
        crate::test_wait::settled(&life, "the pending request was never stored", || rows(&db).1.is_some()).await;
        let pending = rows(&db).1.unwrap();
        drop(waiter);
        let closed = life.close();
        let _ = crate::test_wait::within(closed.wait()).await;
        (pending.request_id, pending.response_key)
    };
    assert!(!key.is_empty());
    let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let (_client, life) = world.open(&db).await;
    crate::test_wait::settled(&life, "the stored request was not sent again", || rows(&db).1.is_none()).await;
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
        let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
        let (client, life) = world.open(&db).await;
        sign_in(&client).await;
        let _ = crate::test_wait::within(life.close().wait()).await;
    }
    {
        let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
        let digest = rows(&db).0.unwrap().credential_digest;
        let pending = session_state::Pending { credential_digest: digest, request_id: old_id.into(), response_key: crate::transport::encrypted::jwe::random_token(), sequence: 2, path: OTP.into(), document: None, body: json!({}).to_string(), owner: None, purpose: crate::transport::NetworkPurpose::Standard, req_id: 2 };
        let inner = db.lock();
        session_state::store_pending(&inner.connection, &pending).unwrap();
    }
    let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let (_client, life) = world.open(&db).await;
    crate::test_wait::settled(&life, "the stored request of the older format was not sent again", || rows(&db).1.is_none()).await;
    let again: Vec<Value> = world.server.seen().into_iter().filter(|request| request["requestId"] == json!(old_id)).collect();
    assert_eq!(again.len(), 1, "one request left with the stored request id");
    assert_eq!(again[0]["sequence"], json!(2), "and the stored sequence");
    assert_eq!(rows(&db).0.unwrap().sequence, 2);
}

async fn signed_in_file(world: &World, path: &std::path::Path) {
    let db = Store::open_models(Some(path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let (client, life) = world.open(&db).await;
    sign_in(&client).await;
    let _ = crate::test_wait::within(life.close().wait()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pending_dropped_by_a_format_change_blocks_new_requests_until_the_reset() {
    let world = World::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    signed_in_file(&world, &path).await;
    let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
    db.lock().connection.execute("INSERT INTO restore_failed (source_table, key, reason, columns) VALUES ('encrypted_pending', 'k', 'missing_column', '{}')", []).unwrap();
    let sent = world.server.seen().len();
    let (_client, life) = world.open(&db).await;
    let reply = attempt(&life, 1 << 41, json!({}).to_string()).await;
    assert!(matches!(reply, LifeReply::Mutated(AuthorOutcome::Error { error: SessionError::StateLost, .. })), "{reply:?}");
    assert_eq!(world.server.seen().len(), sent, "no request left");
    let _ = answer(life.session_reset()).await;
    let left: i64 = db.lock().connection.query_row("SELECT COUNT(*) FROM restore_failed WHERE source_table = 'encrypted_pending'", [], |row| row.get(0)).unwrap();
    assert_eq!(left, 0, "the reset ends the block");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_negative_stored_sequence_fails_the_open() {
    let world = World::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    signed_in_file(&world, &path).await;
    let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
    db.lock().connection.execute("UPDATE encrypted_session SET sequence = -1", []).unwrap();
    let client = Client::new(&config(), world.server.clone(), world.credentials.clone(), db.clone(), world.clock.clone(), Arc::new(crate::transport::MockTransport::new())).unwrap();
    let seal = Arc::new(crate::owners::session_seal::SealConfig::new(&config(), world.clock.clone()).unwrap());
    let ports = super::Ports { db: db.clone(), transport: None, executor: tokio::runtime::Handle::current(), operations: Arc::new(|_| None), secret: Some(client.secret.clone()), exchange: Some(client.exchange.clone()), seal: Some(seal), transfer: None, cable: None, read_operations: None };
    let error = super::Life::start(ports).err().expect("the open fails");
    assert!(matches!(error, super::LifeError::Storage(_)), "{error:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_pending_of_an_author_is_restored_with_its_request_id_and_owner() {
    let world = World::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let (req_id, owner) = (1 << 41, Some(crate::transport::RequestOwner::Operation { seq: 7 }));
    {
        let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
        let (client, life) = world.open(&db).await;
        sign_in(&client).await;
        world.server.drop_next_request();
        drop(life.mutate_answered(owner.clone(), req_id, OTP.into(), json!({}).to_string(), 0));
        settled(&life, "the pending request was never stored", || rows(&db).1.is_some()).await;
        let _ = crate::test_wait::within(life.close().wait()).await;
    }
    let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let client = Client::new(&config(), world.server.clone(), world.credentials.clone(), db.clone(), world.clock.clone(), Arc::new(crate::transport::MockTransport::new())).unwrap();
    let restored = super::session_runner::SessionRunner::stored(&client.exchange, &client.secret, &client.db).unwrap();
    let pending = restored.pending.expect("the pending is restored");
    assert_eq!((pending.request_id, pending.owner), (req_id, owner));
}

use crate::owners::session::{AuthorOutcome, SessionError};
use crate::transport::encrypted::exchange::digest_of;
use crate::transport::{CredentialSlot, TransportError};

#[derive(Default)]
struct ProbeSlot {
    memory: MemoryCredentials,
    fail_set: std::sync::atomic::AtomicBool,
    panic_set: std::sync::atomic::AtomicBool,
    fail_get: std::sync::atomic::AtomicBool,
    get_count: std::sync::atomic::AtomicUsize,
    fail_at: std::sync::atomic::AtomicUsize,
    db: std::sync::Mutex<Option<Arc<Store>>>,
    session_at_clear: std::sync::Mutex<Vec<bool>>,
    threads: std::sync::Mutex<Vec<(&'static str, Option<String>)>>,
}

impl CredentialSlot for ProbeSlot {
    fn get(&self) -> Result<Option<String>, TransportError> {
        self.threads.lock().unwrap().push(("get", std::thread::current().name().map(str::to_string)));
        let count = self.get_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        if self.fail_at.load(std::sync::atomic::Ordering::SeqCst) == count { self.fail_get.store(true, std::sync::atomic::Ordering::SeqCst); }
        if self.fail_get.load(std::sync::atomic::Ordering::SeqCst) { return Err(TransportError::Network("keychain locked".into())); }
        self.memory.get()
    }

    fn set(&self, credential: &str) -> Result<(), TransportError> {
        self.threads.lock().unwrap().push(("set", std::thread::current().name().map(str::to_string)));
        if self.panic_set.load(std::sync::atomic::Ordering::SeqCst) { panic!("the keychain crashed"); }
        if self.fail_set.load(std::sync::atomic::Ordering::SeqCst) { return Err(TransportError::Network("keychain full".into())); }
        self.memory.set(credential)
    }

    fn clear(&self) -> Result<(), TransportError> {
        self.threads.lock().unwrap().push(("clear", std::thread::current().name().map(str::to_string)));
        if let Some(db) = self.db.lock().unwrap().as_ref() { self.session_at_clear.lock().unwrap().push(rows(db).0.is_some()); }
        self.memory.clear()
    }
}

fn open_with(world: &World, slot: Arc<ProbeSlot>, db: &Arc<Store>) -> (Arc<Client>, Arc<crate::owners::life::Life>) {
    *slot.db.lock().unwrap() = Some(db.clone());
    let client = Client::new(&config(), world.server.clone(), slot, db.clone(), world.clock.clone(), Arc::new(crate::transport::MockTransport::new())).unwrap();
    let life = client.start_owners();
    drop(life.set_online(true));
    (client, life)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_credential_store_is_called_off_the_threads_of_the_owners() {
    let world = World::new();
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let slot = Arc::new(ProbeSlot::default());
    let (client, life) = open_with(&world, slot.clone(), &db);
    sign_in(&client).await;
    drop(life.session_reset());
    settled(&life, "the secret was never cleared", || slot.memory.get().unwrap().is_none()).await;
    let threads = slot.threads.lock().unwrap().clone();
    assert_eq!(threads.iter().map(|(call, _)| *call).collect::<Vec<_>>(), ["get", "set", "get", "clear"], "the open read, the store, the read after it and the clear: {threads:?}");
    let owners = ["lifecycle", "store", "network", "calls", "session", "outbox", "transfer"];
    assert!(threads.iter().all(|(_, name)| name.as_deref().is_none_or(|name| !owners.contains(&name))), "{threads:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn request_ids_are_random_uuid_v4_and_do_not_repeat_after_a_reopen() {
    let world = World::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    for _ in 0..2 {
        let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
        let (client, life) = world.open(&db).await;
        if rows(&db).0.is_none() { sign_in(&client).await; }
        let reply = mutate_once(&life, json!({}).to_string()).await;
        assert!(matches!(reply, LifeReply::Mutated(AuthorOutcome::Response(_))), "{reply:?}");
        let _ = crate::test_wait::within(life.close().wait()).await;
    }
    let ids: Vec<String> = world.server.seen().iter().map(|request| request["requestId"].as_str().unwrap().to_string()).collect();
    assert_eq!(ids.len(), 3);
    for id in &ids {
        assert_eq!(uuid::Uuid::parse_str(id).unwrap().get_version_num(), 4, "{id}");
        assert!(!id.contains("-0000-4000-8000-"), "{id} carries a counter");
    }
    let mut unique = ids.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), ids.len(), "{ids:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_first_sign_in_stored_before_a_crash_lands_its_digest_after_the_reopen() {
    let world = World::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    {
        let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
        let (client, life) = world.open(&db).await;
        world.server.interrupt_next_answer();
        let call = tokio::spawn(crate::runtime::counted(Some(life.launch()), { let client = client.clone(); async move { sign_in(&client).await } }));
        settled(&life, "the server never executed the sign-in", || world.server.executed().len() == 1).await;
        settled(&life, "the pending request was never stored", || rows(&db).1.is_some()).await;
        assert_eq!(rows(&db).0.map(|session| session.sequence), Some(0), "the digest is stored with the pending request, at sequence 0");
        let _ = crate::test_wait::within(life.close().wait()).await;
        call.abort();
    }
    let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let (_client, life) = world.open(&db).await;
    settled(&life, "the stored sign-in did not land", || rows(&db).1.is_none() && rows(&db).0.is_some()).await;
    let secret = world.credentials.value().expect("the secret stays");
    let session = rows(&db).0.unwrap();
    assert_eq!((session.credential_digest.clone(), session.sequence), (digest_of(&secret), 1));
    settled(&life, "the identity was not published", || life.published().identity == crate::store::identity_of(&session.credential_digest)).await;
    assert_eq!(world.server.executed().len(), 1, "the server executed the sign-in once");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_secret_left_by_a_crash_is_erased_and_the_session_rows_go_before_the_secret() {
    let world = World::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    world.credentials.set("left-by-generate").unwrap();
    {
        let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
        let (_client, life) = world.open(&db).await;
        settled(&life, "GenerateCredential crashed before the pending: the secret is not erased", || world.credentials.value().is_none()).await;
        let _ = crate::test_wait::within(life.close().wait()).await;
    }
    {
        let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
        let (client, life) = world.open(&db).await;
        sign_in(&client).await;
        let _ = crate::test_wait::within(life.close().wait()).await;
    }
    let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
    crate::store::fixtures::exec(&db, "INSERT INTO reset_intent (id, mode, started_at) VALUES (1, 'session_end', 1)");
    drop(db);
    let slot = Arc::new(ProbeSlot::default());
    slot.memory.set(&world.credentials.value().unwrap()).unwrap();
    let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
    *slot.db.lock().unwrap() = Some(db.clone());
    let client = Client::new(&config(), world.server.clone(), slot.clone(), db.clone(), world.clock.clone(), Arc::new(crate::transport::MockTransport::new())).unwrap();
    drop(client);
    assert_eq!(*slot.session_at_clear.lock().unwrap(), vec![false], "the session rows were erased before the secret");
    assert_eq!((rows(&db).0, slot.memory.value()), (None, None));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_reset_erases_the_digest_before_the_secret() {
    let world = World::new();
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let slot = Arc::new(ProbeSlot::default());
    let (client, life) = open_with(&world, slot.clone(), &db);
    sign_in(&client).await;
    let reset = answer(life.session_reset()).await;
    assert!(matches!(reset, LifeReply::Life(Ok(()))), "{reset:?}");
    assert_eq!(*slot.session_at_clear.lock().unwrap(), vec![false]);
    assert_eq!((rows(&db).0, slot.memory.value()), (None, None));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_credential_store_that_cannot_save_ends_the_sign_in_with_an_error() {
    let world = World::new();
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let slot = Arc::new(ProbeSlot::default());
    slot.fail_set.store(true, std::sync::atomic::Ordering::SeqCst);
    let (client, _life) = open_with(&world, slot.clone(), &db);
    let outcome = crate::test_wait::within(client.session_call("social", json!({ "provider": "google", "idToken": "t" }))).await;
    assert!(matches!(outcome, Err(crate::transport::TransportError::Storage(_))), "{outcome:?}");
    assert_eq!(world.server.seen().len(), 0, "nothing left the device");
    assert_eq!(rows(&db), (None, None));
}

/// rt_executor_panic_reaches_owner: P-RT-09, the panic of `CredentialSlot.set` ends the sign-in with an error and closes the database through Lifecycle.
#[tokio::test(flavor = "multi_thread")]
async fn a_credential_store_that_panics_fails_the_sign_in_and_closes_the_database() {
    let world = World::new();
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let slot = Arc::new(ProbeSlot::default());
    slot.panic_set.store(true, std::sync::atomic::Ordering::SeqCst);
    let (client, life) = open_with(&world, slot.clone(), &db);
    let outcome = crate::test_wait::within(client.session_call("social", json!({ "provider": "google", "idToken": "t" }))).await;
    assert!(matches!(outcome, Err(crate::transport::TransportError::Interrupted(_) | crate::transport::TransportError::Storage(_))), "{outcome:?}");
    settled(&life, "Lifecycle never heard the executor failure", || life.published().closed).await;
    assert_eq!(world.server.seen().len(), 0, "nothing left the device");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_locked_secret_waits_for_the_foreground_and_a_foreign_secret_ends_the_request_before_the_network() {
    let world = World::new();
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let slot = Arc::new(ProbeSlot::default());
    let (client, life) = open_with(&world, slot.clone(), &db);
    sign_in(&client).await;
    let sent = world.server.seen().len();
    slot.fail_get.store(true, std::sync::atomic::Ordering::SeqCst);
    let refused = mutate_once(&life, json!({}).to_string()).await;
    assert!(matches!(refused, LifeReply::Mutated(AuthorOutcome::Error { error: SessionError::Storage(_), .. })));
    slot.fail_get.store(false, std::sync::atomic::Ordering::SeqCst);
    let target = slot.get_count.load(std::sync::atomic::Ordering::SeqCst) + 2;
    slot.fail_at.store(target, std::sync::atomic::Ordering::SeqCst);
    let waiting = tokio::spawn(crate::runtime::counted(Some(life.launch()), { let life = life.clone(); async move { mutate_once(&life, json!({}).to_string()).await } }));
    crate::test_wait::settled(&life, "the stored request reads the locked secret", || slot.fail_get.load(std::sync::atomic::Ordering::SeqCst)).await;
    life.barrier();
    assert!(!waiting.is_finished(), "a locked store gives the author no answer");
    assert_eq!(world.server.seen().len(), sent, "and no request leaves the device");
    slot.fail_get.store(false, std::sync::atomic::Ordering::SeqCst);
    drop(life.foreground());
    let reply = crate::test_wait::within(waiting).await.unwrap();
    assert!(matches!(reply, LifeReply::Mutated(AuthorOutcome::Response(_))), "{reply:?}");
    assert_eq!(world.server.seen().len(), sent + 1, "one request, with the pair it was stored with");
    slot.memory.set("a-foreign-secret").unwrap();
    let lost = mutate_once(&life, json!({}).to_string()).await;
    assert!(matches!(lost, LifeReply::Mutated(AuthorOutcome::Error { error: SessionError::StateLost, .. })), "{lost:?}");
    assert_eq!(world.server.seen().len(), sent + 1, "no request left the device");
}

#[test]
fn only_a_revoked_logout_answer_is_revoked() {
    use crate::owners::session::SessionInput;
    use crate::transport::encrypted::Answer;
    let answer = |body: Value| Ok(Answer { sequence: Some(2), body });
    let revoked = json!({ "revoked": true });
    assert!(matches!(super::session_runner::session_input(1, 1, true, true, answer(revoked.clone())), SessionInput::Revoked { request_id: 1, attempt_id: 1, .. }));
    assert!(matches!(super::session_runner::session_input(1, 1, false, true, answer(revoked)), SessionInput::Response { .. }));
    assert!(matches!(super::session_runner::session_input(1, 1, true, true, answer(json!({}))), SessionInput::Response { .. }));
}

#[test]
fn session_http_auth_status() {
    use crate::owners::session::SessionInput;
    use crate::transport::TransportError;
    for status in [401, 403] {
        let input = super::session_runner::session_input(1, 1, false, true, Err(TransportError::Http { status, body: "refused".into() }));
        match (status, input) {
            (401, SessionInput::HttpError { auth: true, .. }) => {},
            (403, SessionInput::HttpError { auth: false, status: 403, temporary: false, .. }) => {},
            (_, other) => panic!("unexpected result: {other:?}"),
        }
    }
}

#[test]
fn the_answer_stays_when_the_landing_of_its_batch_fails_and_goes_with_the_landing() {
    use crate::owners::contract::BatchAuthor;
    use crate::owners::store::{StoreBatch, StoreOp};
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    {
        let inner = db.lock();
        session_state::store_answer(&inner.connection, 7, None, &session_state::StoredAnswer::Ok("{}".into())).unwrap();
    }
    let answers = |db: &Store| session_state::answers(&db.lock().connection).unwrap().len();
    let batch = |rows: Vec<(String, String)>| StoreBatch { author: BatchAuthor::Outbox, id: 1, epoch: 1, basis: Default::default(), rows, intents: 0, credential: None, ops: vec![StoreOp::RemoveAnswer { request_id: 7 }] };
    let refused = db.write_batch(&batch(vec![("Task".into(), "not json".into())]), db.files(), db.pointer_guards(), None);
    assert!(matches!(refused, Err(crate::store::StoreError::Json(_))), "{:?}", refused.as_ref().err());
    assert_eq!(answers(&db), 1);
    db.write_batch(&batch(Vec::new()), db.files(), db.pointer_guards(), None).unwrap();
    assert_eq!(answers(&db), 0);
}

/// store_read_intents_queue_failed: an unreadable queue table fails the restore of Outbox only, `open` is not cancelled and the base stays readable.
#[tokio::test(flavor = "multi_thread")]
async fn store_read_intents_queue_failed() {
    let world = World::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    signed_in_file(&world, &path).await;
    let db = Store::open_models(Some(&path), Vec::new(), None).map(std::sync::Arc::new).unwrap();
    db.lock().connection.execute_batch("ALTER TABLE op RENAME TO op_unreadable").unwrap();
    let (_client, life) = world.open(&db).await;
    assert!(life.restore_failures().iter().any(|(owner, _)| *owner == crate::owners::lifecycle::Peer::Outbox), "the queue failure reaches Outbox: {:?}", life.restore_failures());
    assert!(life.published().identity.is_some(), "open went on with the session");
    assert!(db.saved().is_ok(), "the base answers reads after the failure");
}
