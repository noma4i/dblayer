use std::sync::Arc;

use super::{Config, CredentialStore, Database, DbError, EncryptedConfig, EncryptedHttp, EncryptedReply, Transport, TransportRequest};
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};

pub(crate) struct Plain;

#[async_trait::async_trait]
impl Transport for Plain {
    async fn open_cable(&self, _: Arc<super::CableLease>, _url: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<super::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) { unreachable!() }
    async fn query(&self, request: TransportRequest) -> Result<String, DbError> {
        assert_eq!(request.document, "query Greeting($name: String!) { greeting(name: $name) }");
        assert_eq!(request.operation_name, "Greeting");
        assert_eq!(request.variables_json, r#"{"name":"Ada"}"#);
        Ok(r#"{"data":{"greeting":"Hello Ada"}}"#.into())
    }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

struct Unused;

impl CredentialStore for Unused {
    fn get(&self) -> Result<Option<String>, DbError> { unreachable!() }
    fn set(&self, _: String) -> Result<(), DbError> { unreachable!() }
    fn clear(&self) -> Result<(), DbError> { unreachable!() }
}

#[async_trait::async_trait]
impl EncryptedHttp for Unused {
    async fn post(&self, _: String, _: String, _: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> { unreachable!() }
}

fn config() -> Config { Config { path: None, data_version: None } }

#[tokio::test]
async fn life_register_ok() {
    let database = Database::open(config(), "[]".into(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), ..Default::default() }).await.unwrap();
    database.register_operations(r#"[{"name":"Greeting","document":"query Greeting($name: String!) { greeting(name: $name) }"}]"#.into()).await.unwrap();
    database.set_online(true).unwrap();
    assert_eq!(database.run(database.session_epoch(), "Greeting".into(), r#"{"name":"Ada"}"#.into()).await.unwrap(), r#""Hello Ada""#);
    database.session_reset().await.unwrap();
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
        let result = Database::open(config(), "[]".into(), crate::ffi::OpenPorts { transport: transport.then(|| Arc::new(Plain) as Arc<dyn Transport>), encrypted, credentials: credentials.then(|| Arc::new(Unused) as Arc<dyn CredentialStore>), http: http.then(|| Arc::new(Unused) as Arc<dyn EncryptedHttp>) }).await;
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
            receiver.recv_timeout(std::time::Duration::from_secs(10)).expect("the owners closed within 10 s");
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
