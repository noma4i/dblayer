use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use super::fake_server_tests::{FakeServer, MemoryCredentials};
use super::state;
use super::vectors_tests::*;
use super::*;
use crate::db::Db;
use crate::pacing::ManualClock;

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
                    owner: Some(crate::transport::RequestOwner::Transfer { operation_id: 7, item_id: "item".into(), step: crate::upload::UploadStage::Put }), purpose: crate::transport::NetworkPurpose::Transfer,
                }).unwrap();
                if case == "partial_owner" { stored.connection.execute_batch("UPDATE encrypted_pending SET owner_step = NULL").unwrap(); }
            }
        }
        let call = client.pending_call();
        let matching = super::graphql::pending_matches(&db, &crate::transport::RequestOwner::Transfer { operation_id: 7, item_id: "item".into(), step: crate::upload::UploadStage::Put });
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

#[test]
fn an_old_subscription_cannot_read_the_next_accounts_cable_credential() {
    let rig = Rig::new();
    let db = memory_db();
    let client = rig.client(&db);
    let old = db.admit().unwrap();
    let life = &db.shared.store.lifecycle;
    let next = life.admit_passive(crate::ffi::legacy::Origin::Explicit, "next session", Some(old.value())).unwrap();
    life.start_session_in_tests(Some("B".into()), next).unwrap();
    assert!(matches!(client.cable_credential(&old), Err(TransportError::Interrupted(_))));
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


impl EncryptedClient {
    /// The owners behind a client that was built without `Database`: Lifecycle, Store, Network, Calls and Session on the threads of the core.
    pub(crate) fn start_owners_in_tests(self: &Arc<Self>, db: &Db) -> Arc<crate::owners::life::Life> {
        let ports = crate::owners::life::Ports { db: db.clone(), transport: None, executor: tokio::runtime::Handle::current(), operations: Arc::new(|_| None), credentials: Some(self.credentials.clone()), session: Some(self.clone()), transfer: None };
        let life = Arc::new(crate::owners::life::Life::start(ports, std::time::Duration::from_secs(10)).expect("the owners started"));
        self.attach(&life);
        crate::db::locked(&self.kept).push(life.clone());
        life
    }

    /// A call of the application: the owner Session signs in when there is no credential.
    pub(crate) async fn session_call_in_tests(&self, operation: &str, body: Value) -> Result<Answer, TransportError> {
        let life = self.life.get().and_then(std::sync::Weak::upgrade).expect("the owners are started");
        match life.session_call(operation.to_string(), body.to_string()).wait().await {
            crate::runtime::Outcome::Value(crate::owners::life::LifeReply::Session(Ok(text))) => Ok(Answer { sequence: None, body: serde_json::from_str(&text).map_err(|error| TransportError::Decode(error.to_string()))? }),
            crate::runtime::Outcome::Value(crate::owners::life::LifeReply::Session(Err(error))) => Err(super::client::session_failure(error)),
            other => Err(TransportError::Interrupted(format!("{other:?}"))),
        }
    }

}
