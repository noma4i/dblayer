use crate::ffi::{FakeHttp, FakeTransport};
use std::sync::Arc;

use super::{Config, CredentialStore, Database, DbError, EncryptedConfig, EncryptedHttp, EncryptedReply, Transport, TransportRequest};
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};

pub(crate) struct Plain;

#[async_trait::async_trait]
impl FakeTransport for Plain {
    async fn query(&self, request: TransportRequest) -> Result<String, DbError> {
        assert_eq!(request.document, "query Greeting($name: String!) { greeting(name: $name) }");
        assert_eq!(request.operation_name, "Greeting");
        assert_eq!(request.variables_json, r#"{"name":"Ada"}"#);
        Ok(r#"{"data":{"greeting":"Hello Ada"}}"#.into())
    }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

pub(crate) struct Unused;

impl CredentialStore for Unused {
    fn get(&self) -> Result<Option<String>, DbError> { unreachable!() }
    fn set(&self, _: String) -> Result<(), DbError> { unreachable!() }
    fn clear(&self) -> Result<(), DbError> { unreachable!() }
}

#[async_trait::async_trait]
impl FakeHttp for Unused {
    async fn post(&self, _: String, _: String, _: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> { unreachable!() }
}

fn config() -> Config { Config { path: None, data_version: None } }

#[tokio::test]
async fn life_register_ok() {
    let database = Database::open(config(), crate::ffi::EMPTY_SPECS.into(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
    database.register_operations(r#"[{"name":"Greeting","document":"query Greeting($name: String!) { greeting(name: $name) }"}]"#.into()).await.unwrap();
    database.set_online(true).unwrap();
    assert_eq!(database.run(database.session_epoch(), "Greeting".into(), r#"{"name":"Ada"}"#.into()).await.unwrap(), r#""Hello Ada""#);
    database.session_reset().await.unwrap();
}

/// net_prop_set_online_no_wall_clock: `set_online` returns the outcome of the owner under a clock that never advances, and `Err(Closed)` once the database closed.
#[tokio::test]
async fn net_prop_set_online_no_wall_clock() {
    let database = Database::open_with_clock(config(), crate::ffi::EMPTY_SPECS.into(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, crate::ffi::TestClock::new(1_000)).await.unwrap();
    let sink = database.online_sink().unwrap();
    let live = Arc::clone(&database);
    crate::test_wait::within(async move { crate::task::spawn_blocking(move || live.set_online(true)).await.unwrap() }).await.unwrap();
    database.close_database().await.unwrap();
    let after = Arc::clone(&database);
    let closed = crate::test_wait::within(async move { crate::task::spawn_blocking(move || after.set_online(false)).await.unwrap() }).await;
    assert!(matches!(closed, Err(DbError::Closed { .. })), "{closed:?}");
    crate::test_wait::within(async move { crate::task::spawn_blocking(move || sink.set_online(true)).await.unwrap() }).await;
}

#[tokio::test]
async fn unified_open_rejects_incomplete_or_unencrypted_session_arguments() {
    let mut rejected = Vec::new();
    for (encrypted, credentials, http, transport) in [
        (true, false, true, true),
        (false, true, false, true),
        (false, false, true, true),
        (false, true, true, true),
        (true, true, false, true),
        (true, true, true, false),
    ] {
        let encrypted = encrypted.then(|| EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::protocol_tests::config() });
        let result = Database::open(config(), crate::ffi::EMPTY_SPECS.into(), crate::ffi::OpenPorts { transport: transport.then(|| Arc::new(Plain) as Arc<dyn Transport>), encrypted, credentials: credentials.then(|| Arc::new(Unused) as Arc<dyn CredentialStore>), http: http.then(|| Arc::new(Unused) as Arc<dyn EncryptedHttp>), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await;
        rejected.push(matches!(result, Err(DbError::Invalid { .. })));
    }
    assert_eq!(rejected, [true; 6]);
}

impl Database {
    /// The part of `close` that does not wait: for a caller that cannot await.
    /// The owners of the database, for a test that waits for them to be quiet.
    pub(crate) fn life_handle(&self) -> Arc<crate::owners::life::Life> { self.life.clone() }

    pub(super) fn close_now(&self) {
        let (sender, receiver) = std::sync::mpsc::channel();
        let waiter = self.life.close();
        let handle = super::database::executor().expect("the executor").handle().clone();
        handle.spawn(async move { let _ = sender.send(crate::test_wait::within(waiter.wait()).await); });
        if std::thread::current().name() != Some("lifecycle") {
            crate::test_wait::recv_within(&receiver);
        }
        if let Some(lease) = self.lease.get() { lease.release(); }
    }
}

impl Database {
    /// The digest of the stored session once it signed in: `None` before the first sign-in and after the session ended.
    pub(crate) fn signed_in_digest(&self) -> Option<String> {
        crate::transport::encrypted::credential::stored_session(&self.db).unwrap().filter(|session| session.sequence > 0).map(|session| session.credential_digest)
    }
}
