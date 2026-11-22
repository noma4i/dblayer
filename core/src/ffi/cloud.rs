use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use async_trait::async_trait;
use super::DbError;

#[derive(uniffi::Record)]
pub struct CloudConfig {
    pub backend: CloudBackend,
    pub application: CloudApplication,
    pub port: Arc<dyn CloudPort>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum CloudBackend { CloudKit, Drive }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum CloudApplication { Standalone { namespace: String }, Session { account_path: String } }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct CloudWireScope { pub epoch: u64, pub generation: u64, pub domain: String }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum CloudAccount { Available { key: String }, SignedOut, Unavailable }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct CloudOutbound { pub key: String, pub version: i64, pub payload: Vec<u8>, pub system_fields: Option<Vec<u8>> }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct CloudInbound { pub key: String, pub payload: Vec<u8>, pub system_fields: Option<Vec<u8>> }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct CloudFetched { pub scope: CloudWireScope, pub records: Vec<CloudInbound>, pub progress: Option<Vec<u8>>, pub initial_complete: bool }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct CloudSendResult { pub key: String, pub version: i64, pub status: CloudSendStatus }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum CloudSendStatus {
    Saved { system_fields: Option<Vec<u8>> },
    Conflict { record: CloudInbound },
    Retryable { detail: String },
    Failed { detail: String },
}

#[uniffi::export(with_foreign)]
#[async_trait]
pub trait CloudPort: Send + Sync {
    async fn observe(&self, signals: Arc<CloudSignals>);
    async fn unobserve(&self);
    async fn start(&self, scope: CloudWireScope, progress: Option<Vec<u8>>, signals: Arc<CloudSignals>) -> Result<(), DbError>;
    async fn stop(&self, generation: u64);
    async fn send(&self, scope: CloudWireScope, records: Vec<CloudOutbound>) -> Result<Vec<CloudSendResult>, DbError>;
}

type FetchFuture = Pin<Box<dyn Future<Output = Result<(), DbError>> + Send>>;

#[derive(uniffi::Object)]
pub struct CloudSignals {
    account: Box<dyn Fn(CloudAccount) + Send + Sync>,
    fetched: Box<dyn Fn(CloudFetched) -> FetchFuture + Send + Sync>,
}

impl CloudSignals {
    pub(crate) fn new(
        account: impl Fn(CloudAccount) + Send + Sync + 'static,
        fetched: impl Fn(CloudFetched) -> FetchFuture + Send + Sync + 'static,
    ) -> Self {
        Self { account: Box::new(account), fetched: Box::new(fetched) }
    }
}

#[uniffi::export]
impl CloudSignals {
    pub fn account(&self, account: CloudAccount) { (self.account)(account); }
    pub async fn fetched(&self, fetched: CloudFetched) -> Result<(), DbError> {
        super::database::on_core(super::database::Scope::root(), (self.fetched)(fetched)).await?
    }
}
