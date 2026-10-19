use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use super::fake_server_tests::{FakeServer, MemoryCredentials};
use super::state;
use super::vectors::*;
use super::*;
use crate::db::Db;
use crate::pacing::ManualClock;

const SOCIAL: &str = "/api/v3/auth/provider";
const GRAPHQL: &str = "/api/v3/graphql";

#[derive(Default)]
struct WatchedCredentials {
    inner: MemoryCredentials,
    sets: AtomicUsize,
    clears: AtomicUsize,
    fail_next_clear: AtomicBool,
}

impl CredentialSlot for WatchedCredentials {
    fn get(&self) -> Result<Option<String>, TransportError> {
        self.inner.get()
    }

    fn set(&self, credential: &str) -> Result<(), TransportError> {
        self.sets.fetch_add(1, Ordering::SeqCst);
        self.inner.set(credential)
    }

    fn clear(&self) -> Result<(), TransportError> {
        self.clears.fetch_add(1, Ordering::SeqCst);
        if self.fail_next_clear.swap(false, Ordering::SeqCst) {
            return Err(TransportError::Network("the app could not clear the credential".into()));
        }
        self.inner.clear()
    }
}

struct Rig {
    server: Arc<FakeServer>,
    credentials: Arc<WatchedCredentials>,
    clock: Arc<ManualClock>,
}

impl Rig {
    fn new() -> Self {
        let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
        Self { server: FakeServer::new(clock.clone()), credentials: Arc::default(), clock }
    }

    fn client(&self, db: &Db) -> EncryptedClient {
        let config = EncryptedConfig { origin: ORIGIN.to_string(), server_key: PUBLIC_KEY_DER_B64.to_string(), pin: PIN.to_string() };
        EncryptedClient::new(&config, self.server.clone(), self.credentials.clone(), db.clone(), self.clock.clone()).unwrap()
    }

    fn touched(&self) -> (usize, usize) {
        (self.credentials.sets.load(Ordering::SeqCst), self.credentials.clears.load(Ordering::SeqCst))
    }
}

fn file_db(path: &Path) -> Db {
    Db::open(path, Vec::new(), None).unwrap()
}

fn rows(db: &Db) -> (Option<state::Session>, Option<state::Pending>) {
    let inner = db.shared.store.lock();
    (state::session(&inner.connection).unwrap(), state::pending(&inner.connection).unwrap())
}

fn signed_in() -> Value {
    json!({ "provider": "google", "idToken": "t" })
}

#[tokio::test]
async fn a_key_that_does_not_match_the_pin_leaves_the_credential_and_the_session_row_alone() {
    let rig = Rig::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let client = rig.client(&db);
    client.mutate(SOCIAL, signed_in()).await.unwrap();
    let (credential, session) = (rig.credentials.inner.value(), rows(&db).0);
    let before = rig.touched();

    let wrong_pin = EncryptedConfig { origin: ORIGIN.to_string(), server_key: PUBLIC_KEY_DER_B64.to_string(), pin: SHORT_KEY_PIN.to_string() };
    assert!(EncryptedClient::new(&wrong_pin, rig.server.clone(), rig.credentials.clone(), db.clone(), rig.clock.clone()).is_err());

    assert_eq!((rig.touched(), rig.credentials.inner.value(), rows(&db).0), (before, credential, session));
}

#[tokio::test]
async fn a_refusal_code_keeps_the_credential_the_session_row_and_the_pending_request() {
    let codes = ["ACCOUNT_UNAVAILABLE", "UNAUTHENTICATED", "PROVIDER_REJECTED", "BLOCKED"];
    for code in codes {
        for confirmed in [false, true] {
            let rig = Rig::new();
            let db = Db::open_in_memory(Vec::new(), None).unwrap();
            let client = rig.client(&db);
            client.mutate(SOCIAL, signed_in()).await.unwrap();
            let (credential, digest) = (rig.credentials.inner.value().unwrap(), rows(&db).0.unwrap().credential_digest);
            rig.server.refuse_next(code, confirmed);

            let outcome = client.mutate(GRAPHQL, json!({ "query": "mutation { m }" })).await;

            let case = format!("{code} confirmed={confirmed}");
            match confirmed {
                true => assert_eq!(outcome.unwrap().body, json!({ "error": code }), "{case}"),
                false => assert_eq!(outcome, Err(TransportError::refusal(code)), "{case}"),
            }
            let (session, pending) = rows(&db);
            assert_eq!((rig.credentials.inner.value(), session.unwrap().credential_digest, pending.is_some(), rig.touched()), (Some(credential), digest, !confirmed, (1, 0)), "{case}");
        }
    }
}

#[tokio::test]
async fn a_request_that_never_left_is_sent_once_by_resume_after_a_restart() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("v3.sqlite");
    let first = rig.client(&file_db(&path));
    rig.server.drop_next_request();
    assert!(first.mutate(SOCIAL, signed_in()).await.is_err());
    assert_eq!(rig.server.seen().len(), 0);
    drop(first);

    let db = file_db(&path);
    let answer = rig.client(&db).resume().await.unwrap().unwrap();

    assert_eq!(answer.body, json!({ "nextStep": "authenticated" }));
    assert_eq!((rig.server.executed().len(), rows(&db).0.unwrap().sequence, rows(&db).1), (1, 1, None));
}

#[tokio::test]
async fn an_answer_that_was_executed_but_never_committed_is_confirmed_once_by_resume_after_a_restart() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("v3.sqlite");
    let first = rig.client(&file_db(&path));
    first.mutate(SOCIAL, signed_in()).await.unwrap();
    rig.server.lose_next_answer();
    assert!(first.mutate(GRAPHQL, json!({ "query": "mutation { m }" })).await.is_err());
    assert_eq!(rig.server.executed().len(), 2);
    drop(first);

    let db = file_db(&path);
    let answer = rig.client(&db).resume().await.unwrap().unwrap();

    assert_eq!(answer.body, json!({ "data": { "done": "mutation { m }" } }));
    assert_eq!((rig.server.executed().len(), rows(&db).0.unwrap().sequence, rows(&db).1), (2, 2, None));
}

#[tokio::test]
async fn a_credential_saved_before_its_session_row_is_replaced_and_the_next_call_runs_once() {
    let rig = Rig::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    rig.credentials.inner.set("saved-before-the-crash").unwrap();

    let client = rig.client(&db);
    client.mutate(SOCIAL, signed_in()).await.unwrap();

    assert_ne!(rig.credentials.inner.value().as_deref(), Some("saved-before-the-crash"));
    assert_eq!((rig.server.executed().len(), rows(&db).0.unwrap().sequence, rows(&db).1), (1, 1, None));
    assert_eq!(client.resume().await, Ok(None));
}

#[tokio::test]
async fn a_session_row_without_its_credential_drops_the_old_pending_request_and_starts_again() {
    let rig = Rig::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    rig.server.drop_next_request();
    assert!(rig.client(&db).mutate(SOCIAL, signed_in()).await.is_err());
    rig.credentials.inner.clear().unwrap();

    let restored = rig.client(&db);

    assert_eq!(restored.resume().await, Ok(None));
    assert_eq!((rig.server.seen().len(), rows(&db).1), (0, None));
    restored.mutate(SOCIAL, signed_in()).await.unwrap();
    assert_eq!((rig.server.executed().len(), rows(&db).0.unwrap().sequence), (1, 1));
}

#[tokio::test]
async fn a_logout_whose_credential_could_not_be_cleared_is_finished_by_resume_without_a_second_effect() {
    let rig = Rig::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let client = rig.client(&db);
    client.mutate(SOCIAL, signed_in()).await.unwrap();
    rig.credentials.fail_next_clear.store(true, Ordering::SeqCst);

    assert!(client.mutate("/api/v3/logout", json!({})).await.is_err());
    let resumed = client.resume().await.unwrap();

    assert_eq!(resumed.map(|answer| answer.body), Some(json!({ "revoked": true })));
    assert_eq!((rig.credentials.inner.value(), rows(&db)), (None, (None, None)));
    assert_eq!(rig.server.executed().len(), 2);
}
