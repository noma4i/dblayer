use std::sync::Arc;

use async_trait::async_trait;

use super::{Config, CredentialStore, Database, DbError, EncryptedConfig, EncryptedHttp, EncryptedReply, SessionCode, Transport};
use crate::pacing::SystemClock;
use crate::transport::encrypted::fake_server_tests::{config_body, FakeServer, MemoryCredentials};
use crate::transport::encrypted::vectors::{ORIGIN, PIN, PUBLIC_KEY_DER_B64, SHORT_KEY_DER_B64, SHORT_KEY_PIN};
use crate::transport::{CredentialSlot as CoreCredentials, HttpExchange, HttpReply, TransportError};

pub(super) struct ThroughServer(pub(super) Arc<FakeServer>);

pub(super) fn failure(error: TransportError) -> DbError {
    DbError::Network { detail: error.to_string() }
}

#[async_trait]
impl EncryptedHttp for ThroughServer {
    async fn post(&self, path: String, body: String) -> Result<EncryptedReply, DbError> {
        let reply = self.0.post(&path, body).await.map_err(failure)?;
        Ok(EncryptedReply { status: reply.status, body: reply.body })
    }
    async fn get(&self, path: String) -> Result<EncryptedReply, DbError> {
        let reply = self.0.get(&path).await.map_err(failure)?;
        Ok(EncryptedReply { status: reply.status, body: reply.body })
    }
}

struct Held(Arc<MemoryCredentials>);

impl CredentialStore for Held {
    fn get(&self) -> Result<Option<String>, DbError> { self.0.get().map_err(failure) }
    fn set(&self, credential: String) -> Result<(), DbError> { self.0.set(&credential).map_err(failure) }
    fn clear(&self) -> Result<(), DbError> { self.0.clear().map_err(failure) }
}

pub(super) struct SubscriptionsOnly;

#[async_trait]
impl Transport for SubscriptionsOnly {
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, _: Arc<super::SubscriptionSink>) -> Result<String, DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: Vec<u8>, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

fn rig(server_key: &str, pin: &str) -> (Arc<Database>, Arc<FakeServer>, Arc<MemoryCredentials>) {
    let server = FakeServer::new(Arc::new(SystemClock));
    let credentials = Arc::new(MemoryCredentials::default());
    let encrypted = EncryptedConfig { origin: ORIGIN.to_string(), server_key: server_key.to_string(), pin: pin.to_string() };
    let database = Database::open_encrypted(
        Config { path: None, data_version: None, cache_rows: 8 },
        "[]".to_string(),
        encrypted,
        Arc::new(ThroughServer(server.clone())),
        Arc::new(Held(credentials.clone())),
        Arc::new(SubscriptionsOnly),
    )
    .unwrap();
    (database, server, credentials)
}

pub(super) fn code(error: DbError) -> SessionCode {
    let DbError::Session { code, .. } = error else { panic!("expected a session error, got {error:?}") };
    code
}

#[tokio::test(flavor = "multi_thread")]
async fn answers_come_back_as_json_and_error_answers_as_typed_codes_that_keep_the_session() {
    let (database, _, credentials) = rig(PUBLIC_KEY_DER_B64, PIN);

    let started = database.session_call("social".into(), r#"{"provider":"google","idToken":"t"}"#.into()).await.unwrap();
    let refused = database.session_call("confirm".into(), r#"{"code":"000000"}"#.into()).await.unwrap_err();

    assert_eq!(started, r#"{"nextStep":"authenticated"}"#);
    assert_eq!(code(refused), SessionCode::InvalidCode);
    assert!(credentials.value().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sequence_conflict_is_a_typed_code_and_the_credential_stays() {
    let (database, server, credentials) = rig(PUBLIC_KEY_DER_B64, PIN);
    database.session_call("otp".into(), r#"{"phone":"+61400000000"}"#.into()).await.unwrap();
    let credential = credentials.value().unwrap();
    server.rewind_sequence(&credential, 5);

    let conflict = database.session_call("confirm".into(), r#"{"code":"1"}"#.into()).await.unwrap_err();

    assert_eq!(code(conflict), SessionCode::SequenceConflict);
    assert_eq!(credentials.value(), Some(credential));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_config_is_typed_and_a_key_other_than_the_pinned_one_is_refused() {
    let (database, server, _) = rig(PUBLIC_KEY_DER_B64, PIN);
    assert_eq!(database.server_config(None, None).await.unwrap().server_key.kid, PIN);

    server.serve_config(HttpReply { status: 200, body: config_body(SHORT_KEY_PIN, SHORT_KEY_DER_B64, 2048).to_string() });
    assert_eq!(code(database.server_config(None, None).await.unwrap_err()), SessionCode::ServerKeyMismatch);
}
