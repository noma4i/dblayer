use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use super::fake_server_tests::{FakeServer, MemoryCredentials};
use super::state;
use super::vectors_tests::*;
use super::*;
use crate::db::Db;
use crate::pacing::ManualClock;

const SOCIAL: &str = "/api/auth/social";
const GRAPHQL: &str = "/api/graphql";

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
        let config = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() };
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

fn session_model() -> crate::schema::ModelSpec {
    crate::schema::ModelSpec::new("messages", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id)])
}

