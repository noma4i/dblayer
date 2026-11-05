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
        life.set_online(true).wait().await;
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

/// The author of the old stack: it repeats a request with the same `req_id` after `Dropped` or a temporary error, as Outbox does.
async fn mutate_retrying(life: &crate::owners::life::Life, body: String) -> LifeReply {
    use crate::owners::contract::ErrorClass;
    use crate::owners::session::AuthorOutcome;
    let req_id = life.legacy_request();
    for _ in 0..20 {
        match answer(life.mutate_wait(req_id, None, OTP.into(), body.clone(), 0)).await {
            LifeReply::Mutated(AuthorOutcome::Dropped | AuthorOutcome::Error { class: ErrorClass::Network | ErrorClass::Server, .. }) => continue,
            other => return other,
        }
    }
    panic!("the request was still dropped after 20 attempts");
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
    let reply = mutate_retrying(&life, json!({}).to_string()).await;
    assert!(matches!(reply, LifeReply::Mutated(crate::owners::session::AuthorOutcome::Response(_))), "{reply:?}");
    let (session, pending) = rows(&db);
    assert_eq!((session.unwrap().sequence, pending.is_none()), (2, true));
    assert_eq!(world.server.seen().iter().map(|request| request["sequence"].clone()).collect::<Vec<Value>>(), [json!(1), json!(2)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_answer_of_a_mutate_is_stored_with_the_removal_of_its_pending() {
    let world = World::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let (client, life) = world.open(&db).await;
    sign_in(&client).await;
    let reply = mutate_retrying(&life, json!({}).to_string()).await;
    let crate::owners::life::LifeReply::Mutated(crate::owners::session::AuthorOutcome::Response(body)) = reply else { panic!("{reply:?}") };
    let (_, pending) = rows(&db);
    let answers = {
        let inner = db.shared.store.lock();
        state::answers(&inner.connection).unwrap()
    };
    assert!(pending.is_none());
    assert_eq!(answers.values().collect::<Vec<_>>(), [&state::StoredAnswer::Ok(body)], "the sign-in is a call without an author and leaves no answer");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_answer_is_repeated_with_the_same_request_id_and_executes_once() {
    let world = World::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let (client, life) = world.open(&db).await;
    sign_in(&client).await;
    world.server.lose_next_answer();
    let reply = mutate_retrying(&life, json!({}).to_string()).await;
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
    let reply = mutate_retrying(&life, json!({ "text": "x".repeat(300_000) }).to_string()).await;
    assert!(matches!(reply, LifeReply::Mutated(crate::owners::session::AuthorOutcome::Error { .. })), "{reply:?}");
    let (session, pending) = rows(&db);
    assert_eq!((session.unwrap().sequence, pending.is_none()), (1, true), "the refused request left no pending and spent no sequence");
    let next = mutate_retrying(&life, json!({}).to_string()).await;
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
        let waiter = life.mutate_wait(life.legacy_request(), None, OTP.into(), json!({}).to_string(), 0);
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
    let ports = super::Ports { db: db.clone(), transport: None, executor: tokio::runtime::Handle::current(), operations: Arc::new(|_| None), credentials: Some(credentials), session: Some(client), transfer: None, read_operations: None };
    let error = super::Life::start(ports).err().expect("the open fails");
    assert!(matches!(error, super::LifeError::Storage(_)), "the read error reaches the caller: {error:?}");
}

use crate::owners::session::{AuthorOutcome, SessionError};
use crate::transport::encrypted::client::digest_of;
use crate::transport::{CredentialSlot, TransportError};

#[derive(Default)]
struct ProbeSlot {
    memory: MemoryCredentials,
    fail_set: std::sync::atomic::AtomicBool,
    fail_get: std::sync::atomic::AtomicBool,
    db: std::sync::Mutex<Option<Db>>,
    session_at_clear: std::sync::Mutex<Vec<bool>>,
}

impl CredentialSlot for ProbeSlot {
    fn get(&self) -> Result<Option<String>, TransportError> {
        if self.fail_get.load(std::sync::atomic::Ordering::SeqCst) { return Err(TransportError::Network("keychain locked".into())); }
        self.memory.get()
    }

    fn set(&self, credential: &str) -> Result<(), TransportError> {
        if self.fail_set.load(std::sync::atomic::Ordering::SeqCst) { return Err(TransportError::Network("keychain full".into())); }
        self.memory.set(credential)
    }

    fn clear(&self) -> Result<(), TransportError> {
        if let Some(db) = self.db.lock().unwrap().as_ref() { self.session_at_clear.lock().unwrap().push(rows(db).0.is_some()); }
        self.memory.clear()
    }
}

fn open_with(world: &World, slot: Arc<ProbeSlot>, db: &Db) -> (Arc<EncryptedClient>, Arc<crate::owners::life::Life>) {
    *slot.db.lock().unwrap() = Some(db.clone());
    let client = Arc::new(EncryptedClient::new(&config(), world.server.clone(), slot, db.clone(), world.clock.clone()).unwrap());
    let life = client.start_owners_in_tests(db);
    drop(life.set_online(true));
    (client, life)
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let started = std::time::Instant::now();
    while !done() {
        assert!(started.elapsed() < LIMIT, "{what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn request_ids_are_random_uuid_v4_and_do_not_repeat_after_a_reopen() {
    let world = World::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    for _ in 0..2 {
        let db = Db::open(&path, Vec::new(), None).unwrap();
        let (client, life) = world.open(&db).await;
        if rows(&db).0.is_none() { sign_in(&client).await; }
        let reply = mutate_retrying(&life, json!({}).to_string()).await;
        assert!(matches!(reply, LifeReply::Mutated(AuthorOutcome::Response(_))), "{reply:?}");
        let _ = tokio::time::timeout(LIMIT, life.close().wait()).await;
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
        let db = Db::open(&path, Vec::new(), None).unwrap();
        let (client, life) = world.open(&db).await;
        world.server.interrupt_next_answer();
        let call = tokio::spawn({ let client = client.clone(); async move { sign_in(&client).await } });
        until("the server never executed the sign-in", || world.server.executed().len() == 1).await;
        until("the pending request was never stored", || rows(&db).1.is_some()).await;
        assert_eq!(rows(&db).0.map(|session| session.sequence), Some(0), "the digest is stored with the pending request, at sequence 0");
        let _ = tokio::time::timeout(LIMIT, life.close().wait()).await;
        call.abort();
    }
    let db = Db::open(&path, Vec::new(), None).unwrap();
    let (_client, life) = world.open(&db).await;
    until("the stored sign-in did not land", || rows(&db).1.is_none() && rows(&db).0.is_some()).await;
    let secret = world.credentials.value().expect("the secret stays");
    let session = rows(&db).0.unwrap();
    assert_eq!((session.credential_digest.clone(), session.sequence), (digest_of(&secret), 1));
    until("the identity was not published", || life.published().identity == crate::store::identity_of(&session.credential_digest)).await;
    assert_eq!(world.server.executed().len(), 1, "the server executed the sign-in once");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_secret_left_by_a_crash_is_erased_and_the_session_rows_go_before_the_secret() {
    let world = World::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    world.credentials.set("left-by-generate").unwrap();
    {
        let db = Db::open(&path, Vec::new(), None).unwrap();
        let (_client, life) = world.open(&db).await;
        assert_eq!(world.credentials.value(), None, "GenerateCredential crashed before the pending: the secret is erased");
        let _ = tokio::time::timeout(LIMIT, life.close().wait()).await;
    }
    {
        let db = Db::open(&path, Vec::new(), None).unwrap();
        let (client, life) = world.open(&db).await;
        sign_in(&client).await;
        let _ = tokio::time::timeout(LIMIT, life.close().wait()).await;
    }
    let db = Db::open(&path, Vec::new(), None).unwrap();
    db.shared.store.mark_session_end_in_tests().unwrap();
    drop(db);
    let slot = Arc::new(ProbeSlot::default());
    slot.memory.set(&world.credentials.value().unwrap()).unwrap();
    let db = Db::open(&path, Vec::new(), None).unwrap();
    *slot.db.lock().unwrap() = Some(db.clone());
    let client = EncryptedClient::new(&config(), world.server.clone(), slot.clone(), db.clone(), world.clock.clone()).unwrap();
    drop(client);
    assert_eq!(*slot.session_at_clear.lock().unwrap(), vec![false], "the session rows were erased before the secret");
    assert_eq!((rows(&db).0, slot.memory.value()), (None, None));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_reset_erases_the_digest_before_the_secret() {
    let world = World::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
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
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let slot = Arc::new(ProbeSlot::default());
    slot.fail_set.store(true, std::sync::atomic::Ordering::SeqCst);
    let (client, _life) = open_with(&world, slot.clone(), &db);
    let outcome = tokio::time::timeout(LIMIT, client.session_call_in_tests("social", json!({ "provider": "google", "idToken": "t" }))).await.expect("the call has an outcome");
    assert!(outcome.is_err(), "{outcome:?}");
    assert_eq!(world.server.seen().len(), 0, "nothing left the device");
    assert_eq!(rows(&db), (None, None));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_locked_secret_waits_for_the_foreground_and_a_foreign_secret_ends_the_request_before_the_network() {
    let world = World::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let slot = Arc::new(ProbeSlot::default());
    let (client, life) = open_with(&world, slot.clone(), &db);
    sign_in(&client).await;
    let sent = world.server.seen().len();
    slot.fail_get.store(true, std::sync::atomic::Ordering::SeqCst);
    let waiting = tokio::spawn({ let life = life.clone(); async move { mutate_retrying(&life, json!({}).to_string()).await } });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!waiting.is_finished(), "a locked store gives the author no answer");
    assert_eq!(world.server.seen().len(), sent, "and no request leaves the device");
    slot.fail_get.store(false, std::sync::atomic::Ordering::SeqCst);
    drop(life.foreground());
    let reply = tokio::time::timeout(LIMIT, waiting).await.expect("the request went on at the foreground").unwrap();
    assert!(matches!(reply, LifeReply::Mutated(AuthorOutcome::Response(_))), "{reply:?}");
    assert_eq!(world.server.seen().len(), sent + 1, "one request, with the pair it was stored with");
    slot.memory.set("a-foreign-secret").unwrap();
    let lost = mutate_retrying(&life, json!({}).to_string()).await;
    assert!(matches!(lost, LifeReply::Mutated(AuthorOutcome::Error { error: SessionError::StateLost, .. })), "{lost:?}");
    assert_eq!(world.server.seen().len(), sent + 1, "no request left the device");
}

#[test]
fn only_a_revoked_logout_answer_is_revoked() {
    use crate::owners::session::SessionInput;
    use crate::transport::encrypted::Answer;
    let answer = |body: Value| Ok(Answer { sequence: Some(2), body });
    let revoked = json!({ "revoked": true });
    assert!(matches!(super::session_runner::input_of(1, 1, true, answer(revoked.clone())), SessionInput::Revoked { request_id: 1, attempt_id: 1, .. }));
    assert!(matches!(super::session_runner::input_of(1, 1, false, answer(revoked)), SessionInput::Response { .. }));
    assert!(matches!(super::session_runner::input_of(1, 1, true, answer(json!({}))), SessionInput::Response { .. }));
}
