use std::sync::{Arc, OnceLock};
use std::time::Duration;

use serde_json::{json, Value};

use super::credential::{stored_pending, Secret};
use super::fake_server_tests::FakeServer;
use crate::store::session_state;
use super::vectors_tests::*;
use super::*;
use crate::runtime::{Clock, ManualClock};
use crate::store::Store;
use crate::transport::{NetworkPurpose, Upstream};

const START: u64 = 1_800_000_000;

pub(super) fn config() -> EncryptedConfig {
    EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() }
}

/// An encrypted exchange, its secret and the owners behind them, built without `Database`.
pub(crate) struct Client {
    pub(crate) exchange: Arc<Exchange>,
    pub(crate) secret: Arc<Secret>,
    pub(crate) db: Arc<Store>,
    pub(crate) clock: Arc<dyn Clock>,
    rest: Arc<dyn Upstream>,
    life: OnceLock<Arc<crate::owners::life::Life>>,
}

impl Client {
    pub(crate) fn new(config: &EncryptedConfig, http: Arc<dyn HttpExchange>, credentials: Arc<dyn CredentialSlot>, db: Arc<Store>, clock: Arc<dyn Clock>, rest: Arc<dyn Upstream>) -> Result<Arc<Self>, TransportError> {
        let exchange = Arc::new(Exchange::new(config, http, clock.clone())?);
        let secret = Arc::new(Secret::new(credentials.clone()));
        super::credential::finish_session_end(&db, &secret)?;
        Ok(Arc::new(Self { exchange, secret, db, clock, rest, life: OnceLock::new() }))
    }

    /// Lifecycle, Store, Network, Calls and Session on the threads of the core.
    pub(crate) fn start_owners(&self) -> Arc<crate::owners::life::Life> {
        let seal = Arc::new(crate::owners::session_seal::SealConfig::new(&config(), self.clock.clone()).unwrap());
        let ports = crate::owners::life::Ports { db: self.db.clone(), transport: None, executor: tokio::runtime::Handle::current(), operations: Arc::new(|_| None), secret: Some(self.secret.clone()), exchange: Some(self.exchange.clone()), seal: Some(seal), transfer: None, cable: None, read_operations: None };
        let life = Arc::new(crate::owners::life::Life::start(ports).expect("the owners started"));
        drop(life.set_online(true));
        let _ = self.life.set(life.clone());
        life
    }

    /// The transport of the application on top of this exchange; it joins the owners that `start_owners` started.
    pub(crate) fn transport(&self) -> Arc<EncryptedTransport> {
        let transport = Arc::new(EncryptedTransport::new(self.exchange.clone(), self.secret.clone(), self.db.clone(), self.rest.clone()));
        if let Some(life) = self.life.get() { transport.attach(life); }
        transport
    }

    /// A call of the application: the owner Session signs in when there is no credential.
    pub(crate) async fn session_call(&self, operation: &str, body: Value) -> Result<Answer, TransportError> {
        let life = self.life.get().expect("the owners are started");
        match crate::test_wait::within(life.session_call(operation.to_string(), body.to_string()).wait()).await {
            crate::runtime::Outcome::Value(crate::owners::life::LifeReply::Session(Ok(text))) => Ok(Answer { sequence: None, body: serde_json::from_str(&text).map_err(|error| TransportError::Decode(error.to_string()))? }),
            crate::runtime::Outcome::Value(crate::owners::life::LifeReply::Session(Err(error))) => Err(super::exchange::session_failure(error)),
            other => Err(TransportError::Interrupted(format!("{other:?}"))),
        }
    }

}

pub(crate) fn memory_db() -> Arc<Store> {
    Arc::new(Store::open_models(None, Vec::new(), None).unwrap())
}

pub(crate) fn clock() -> Arc<ManualClock> { Arc::new(ManualClock::new(Duration::from_secs(START))) }

#[test]
fn pending_reads_distinguish_missing_corrupt_and_sqlite_failure() {
    for case in ["missing", "valid", "corrupt", "partial_owner", "sqlite"] {
        let db = memory_db();
        {
            let stored = db.lock();
            if case == "sqlite" {
                stored.connection.execute_batch("DROP TABLE encrypted_pending").unwrap();
            } else if case != "missing" {
                session_state::store_pending(&stored.connection, &session_state::Pending {
                    credential_digest: "digest".into(), request_id: "request".into(), response_key: "response".into(), sequence: 1,
                    path: "/graphql".into(), body: if case == "corrupt" { "{".into() } else { "{}".into() },
                    owner: Some(crate::transport::RequestOwner::Transfer { operation_id: 7, item_id: "item".into(), step: crate::owners::write::upload::UploadStage::Put }), purpose: NetworkPurpose::Transfer, req_id: 1,
                }).unwrap();
                if case == "partial_owner" { stored.connection.execute_batch("UPDATE encrypted_pending SET owner_step = NULL").unwrap(); }
            }
        }
        let call = stored_pending(&db).and_then(|pending| pending.map(|pending| serde_json::from_str::<Value>(&pending.body).map(|body| (pending.path, body)).map_err(|error| TransportError::Decode(error.to_string()))).transpose());
        match case {
            "missing" => { assert_eq!(call.unwrap(), None); }
            "valid" => { assert_eq!(call.unwrap(), Some(("/graphql".into(), json!({})))); }
            _ => { assert!(call.is_err(), "{case}"); }
        }
    }
}

#[test]
fn the_exchange_requires_an_https_origin_without_a_path_and_a_pinned_key() {
    let clock = clock();
    let server = FakeServer::new(clock.clone());
    let make = |origin: &str, pin: &str| {
        let config = EncryptedConfig { origin: origin.to_string(), server_key: public_key_der_b64().to_string(), pin: pin.to_string(), ..crate::transport::encrypted::protocol_tests::config() };
        Exchange::new(&config, server.clone(), clock.clone()).is_ok()
    };

    assert!(make("https://api.example.com", pin()));
    assert!(make("https://api.example.com/", pin()));
    assert!(!make("https://api.example.com/api", pin()));
    assert!(!make("https://api.example.com", short_key_pin()));
}

#[tokio::test]
async fn a_request_over_256_kib_is_refused_without_being_sent() {
    let clock = clock();
    let server = FakeServer::new(clock.clone());
    let exchange = Exchange::new(&config(), server.clone(), clock).unwrap();

    let outcome = exchange.send_once(SendOnce { credential: Some("credential"), request_id: "request", response_key: "key", sequence: None, path: "/api/graphql", body: &json!({ "query": "q".repeat(300 * 1024) }), purpose: NetworkPurpose::Standard, wire: crate::transport::WireIds::bootstrap() }).await;

    assert!(matches!(outcome, Err(TransportError::RequestTooLarge { size, limit }) if size > limit && limit == 256 * 1024));
    assert!(server.seen().is_empty());
}

#[test]
fn cable_url_is_built_by_the_core() {
    let at = |origin: &str| EncryptedConfig { origin: origin.into(), ..config() }.cable_url();
    let path = config().endpoints.cable;
    assert_eq!(at("https://api.example.com/").unwrap(), format!("wss://api.example.com{path}"));
    assert_eq!(at("http://localhost:3000").unwrap(), format!("ws://localhost:3000{path}"));
    assert!(at("ftp://example.com").is_err(), "only an HTTP origin has a cable");
}

