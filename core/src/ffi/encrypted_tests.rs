use std::sync::Arc;

use async_trait::async_trait;

use super::{Config, CredentialStore, Database, DbError, EncryptedConfig, EncryptedHttp, EncryptedReply, SessionCode, Transport};
use crate::pacing::SystemClock;
use crate::transport::encrypted::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};
use crate::transport::{CredentialSlot as CoreCredentials, HttpExchange, TransportError};

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
    async fn open_cable(&self) -> Result<String, DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, _: Arc<super::SubscriptionSink>) -> Result<String, DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: Vec<u8>, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

fn rig(server_key: &str, pin: &str) -> (Arc<Database>, Arc<FakeServer>, Arc<MemoryCredentials>) {
    let server = FakeServer::new(Arc::new(SystemClock));
    let credentials = Arc::new(MemoryCredentials::default());
    let encrypted = EncryptedConfig { origin: origin().to_string(), server_key: server_key.to_string(), pin: pin.to_string(), ..crate::transport::encrypted::protocol_tests::config() };
    let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, "[]".to_string(), Some(Arc::new(SubscriptionsOnly)), Some(encrypted), Some(Arc::new(Held(credentials.clone()))), Some(Arc::new(ThroughServer(server.clone()))))
    .unwrap();
    (database, server, credentials)
}

pub(super) fn code(error: DbError) -> SessionCode {
    let DbError::Session { code, .. } = error else { panic!("expected a session error, got {error:?}") };
    code
}

#[tokio::test(flavor = "multi_thread")]
async fn answers_come_back_as_json_and_error_answers_as_typed_codes_that_keep_the_session() {
    let (database, _, credentials) = rig(public_key_der_b64(), pin());

    let started = database.session_call("social".into(), r#"{"provider":"google","idToken":"t"}"#.into()).await.unwrap();
    let refused = database.session_call("confirm".into(), r#"{"code":"000000"}"#.into()).await.unwrap_err();

    assert_eq!(started, r#"{"nextStep":"authenticated"}"#);
    assert_eq!(code(refused), SessionCode::InvalidCode);
    assert!(credentials.value().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn support_posts_all_fields_without_a_session() {
    struct SupportPost;

    let body = r#"{"name":"Alex","contact":"alex@example.com","message":"Need assistance","step":"phone","device":"iPhone","os":"iOS","appVersion":"3.0","country":"AU"}"#;

    #[async_trait]
    impl EncryptedHttp for SupportPost {
        async fn post(&self, path: String, body: String) -> Result<EncryptedReply, DbError> {
            use crate::transport::encrypted::fake_server_tests::{open_request, seal_response};
            use crate::pacing::Clock;

            assert_eq!(path, "/api/support");
            let request = open_request(&body, &path, SystemClock.now().as_secs() as i64).unwrap();
            assert_eq!(request["body"], serde_json::json!({"name":"Alex","contact":"alex@example.com","message":"Need assistance","step":"phone","device":"iPhone","os":"iOS","appVersion":"3.0","country":"AU"}));
            Ok(EncryptedReply { status: 200, body: seal_response(&request, Some(1), serde_json::json!({})) })
        }
    }

    let mut encrypted = EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::protocol_tests::config() };
    encrypted.endpoints.session_operations = [("ticket".into(), "/api/support".into())].into();
    let credentials = Arc::new(MemoryCredentials::default());
    let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, "[]".into(), Some(Arc::new(SubscriptionsOnly)), Some(encrypted), Some(Arc::new(Held(credentials.clone()))), Some(Arc::new(SupportPost))).unwrap();
    assert!(credentials.value().is_none());

    database.session_call("ticket".into(), body.into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sequence_conflict_is_a_typed_code_and_the_credential_stays() {
    let (database, server, credentials) = rig(public_key_der_b64(), pin());
    database.session_call("otp".into(), r#"{"phone":"+61400000000"}"#.into()).await.unwrap();
    let credential = credentials.value().unwrap();
    server.rewind_sequence(&credential, 5);

    let conflict = database.session_call("confirm".into(), r#"{"code":"1"}"#.into()).await.unwrap_err();

    assert_eq!(code(conflict), SessionCode::SequenceConflict);
    assert_eq!(credentials.value(), Some(credential));
}
