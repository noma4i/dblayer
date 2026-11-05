use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;

use crate::ffi::{ByteProgress, CableLease, CommandRequest, Config, CredentialStore, Database, DbError, EncryptedConfig, EncryptedHttp, EncryptedReply, EncryptedSocketEvent, PutReply, SubscriptionSink, Transport, TransportRequest};
use crate::pacing::SystemClock;
use crate::schema::{ActionDeclaration, FieldSpec, ModelSpec, OnFailure, RootShape};
use crate::transport::encrypted::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};
use crate::transport::{HttpExchange, PutFile, TransferIdentity, TransportError};
use crate::value::ScalarKind;

pub const SOCIAL: &str = r#"{"provider":"google","idToken":"t"}"#;
const OPERATIONS: &str = r#"[{"name":"MakeMoment","document":"mutation MakeMoment { makeMoment { id text } }","rootPath":"makeMoment"}]"#;

struct ThroughServer(Arc<FakeServer>);

fn failure(error: TransportError) -> DbError {
    match error {
        TransportError::Interrupted(detail) => DbError::Interrupted { detail },
        error => DbError::Network { detail: error.to_string() },
    }
}

#[async_trait]
impl EncryptedHttp for ThroughServer {
    async fn post(&self, path: String, body: String, purpose: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let reply = self.0.post(&path, body, purpose).await.map_err(failure)?;
        Ok(EncryptedReply { status: reply.status, body: reply.body })
    }
}

pub struct Held(pub Arc<MemoryCredentials>);

impl CredentialStore for Held {
    fn get(&self) -> Result<Option<String>, DbError> { crate::transport::CredentialSlot::get(&*self.0).map_err(failure) }
    fn set(&self, credential: String) -> Result<(), DbError> { crate::transport::CredentialSlot::set(&*self.0, &credential).map_err(failure) }
    fn clear(&self) -> Result<(), DbError> { crate::transport::CredentialSlot::clear(&*self.0).map_err(failure) }
}

/// The transport mock of the app side: cable and subscriptions never connect, requests go through the encrypted client.
struct NoCable;

#[async_trait]
impl Transport for NoCable {
    async fn open_cable(&self, _: Arc<CableLease>, _url: String) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { None }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: PutFile, _: TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { Err(DbError::Network { detail: "no cable".into() }) }
}

fn models_json() -> String {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("MakeMoment".to_string());
    create.durable = true;
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    create.on_failure = OnFailure::Keep;
    model.actions.push(create);
    serde_json::to_string(&vec![model]).unwrap_or_default()
}

/// A database behind its public API with the app side mocked: the server, the credential store and the transport.
pub struct Rig {
    pub server: Arc<FakeServer>,
    pub credentials: Arc<MemoryCredentials>,
    pub database: Arc<Database>,
}

impl Rig {
    pub async fn open() -> Self {
        let server = FakeServer::new(Arc::new(SystemClock));
        let credentials = Arc::new(MemoryCredentials::default());
        let encrypted = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() };
        let config = Config { path: None, data_version: None, cache_rows: 32 };
        let database = Database::open(config, models_json(), Some(Arc::new(NoCable)), Some(encrypted), Some(Arc::new(Held(credentials.clone()))), Some(Arc::new(ThroughServer(server.clone()))), None, None).await.expect("the database opens");
        database.register_operations(OPERATIONS.into()).expect("operations register");
        Self { server, credentials, database }
    }

    pub async fn login(&self) -> Result<String, DbError> {
        self.database.set_online(true).ok();
        let answer = bounded(self.database.session_call("social".into(), SOCIAL.into())).await;
        self.database.set_online(false).ok();
        answer.unwrap_or_else(|_| Err(timeout()))
    }

    pub async fn logout(&self) -> Result<String, DbError> {
        self.database.set_online(true).ok();
        let answer = bounded(self.database.session_call("logout".into(), "{}".into())).await;
        self.database.set_online(false).ok();
        answer.unwrap_or_else(|_| Err(timeout()))
    }

    pub async fn create(&self, epoch: u64, text: &str) -> Result<String, DbError> {
        let request = CommandRequest { epoch, model: "Moment".into(), values_json: json!({ "text": text }).to_string(), args_json: "{}".into(), upload: None };
        let receipt = Database::send(&self.database, request).await?;
        Ok(receipt.optimistic_value.map(|row| row.id).unwrap_or_default())
    }

    pub async fn rows(&self, epoch: u64) -> Result<Vec<String>, DbError> {
        Ok(self.database.read_query(epoch, r#"{"model":"Moment"}"#.into()).await?.into_iter().map(|row| row.id).collect())
    }
}

/// Every wait of a spec test has a limit: a core that never answers is a red test, not a hung run.
pub async fn bounded<T>(future: impl Future<Output = T>) -> Result<T, tokio::time::error::Elapsed> {
    tokio::time::timeout(Duration::from_secs(5), future).await
}

pub fn timeout() -> DbError {
    DbError::Invalid { detail: "timeout".into() }
}

/// The error name of the observation vocabulary (`specs/machines/corpus.md`).
pub fn kind(error: &DbError) -> &'static str {
    match error {
        DbError::NoSession { .. } => "no_session",
        DbError::Interrupted { .. } => "interrupted",
        DbError::Invalid { detail } if detail == "timeout" => "timeout",
        DbError::Invalid { .. } => "invalid",
        DbError::Database { .. } => "database",
        DbError::Network { .. } => "network",
        DbError::Session { .. } => "session",
        DbError::Closed { .. } => "closed",
        _ => "other",
    }
}
