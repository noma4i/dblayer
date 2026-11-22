use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use async_trait::async_trait;
use super::{Database, DbError};

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum CloudPhase { Disabled, WaitingForAccount, Offline, Reading, Ready, Failed, Closed }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum CloudIssueCause { TooLarge, Rejected }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct CloudIssue { pub model: String, pub row_id: String, pub cause: CloudIssueCause }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct CloudStatus { pub configured: bool, pub enabled: bool, pub phase: CloudPhase, pub activity: CloudActivity, pub issues: Vec<CloudIssue> }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum CloudActivity { Idle, ObservingAccount, Reading, Bootstrapping, Sending, WaitingToRetry, Stopping, Erasing }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct CloudModelStatus { pub model: String, pub eligible: bool }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum CloudRecordStatus { NotAttached, Excluded, Held, Waiting, Missing, Queued, QueuedDeletion, Synchronized, Deleted, TooLarge, Rejected }

#[derive(uniffi::Object)]
pub struct CloudSubscription(super::reading::Reader<CloudStatus>, std::sync::atomic::AtomicBool);

#[uniffi::export]
impl CloudSubscription {
    pub async fn next(&self) -> Option<CloudStatus> {
        use std::sync::atomic::Ordering;
        if self.1.load(Ordering::Acquire) { return None; }
        let value = self.0.next().await?;
        if value.phase == CloudPhase::Closed { self.1.store(true, Ordering::Release); }
        Some(value)
    }
    pub fn cancel(&self) { self.0.cancel(); }
}

#[uniffi::export]
impl Database {
    pub fn cloud_status(&self) -> CloudStatus {
        self.life.cloud_status().map(|value| value.borrow().clone()).unwrap_or(CloudStatus { configured: false, enabled: false, phase: CloudPhase::Disabled, activity: CloudActivity::Idle, issues: Vec::new() })
    }
    pub fn cloud_changes(&self) -> Option<Arc<CloudSubscription>> {
        self.life.cloud_status().map(|value| Arc::new(CloudSubscription(super::reading::Reader::of(value), std::sync::atomic::AtomicBool::new(false))))
    }
    pub async fn set_cloud_enabled(&self, enabled: bool) -> Result<bool, DbError> { let this = self.clone(); super::database::on_core(self.scope(), async move {
        this.not_closed()?;
        if this.life.cloud_status().is_none() {
            return if enabled { Err(DbError::Invalid { detail: "cloud is not configured".into() }) } else { Ok(false) };
        }
        match this.await_life(this.life.set_cloud_enabled(enabled)).await? {
            crate::owners::life::LifeReply::CloudEnabled(result) => result.map_err(|detail| DbError::Storage { detail }),
            _ => Err(super::database::foreign_reply()),
        }
    }).await? }
    pub async fn cloud_models(&self) -> Result<Vec<CloudModelStatus>, DbError> { let this = self.clone(); super::database::on_core(self.scope(), async move {
        this.not_closed()?;
        let db = this.db.clone();
        let result = this.offload(move || Ok(db.cloud_models().into_iter().map(|(model, eligible)| CloudModelStatus { model, eligible }).collect())).await?;
        this.not_closed()?;
        Ok(result)
    }).await? }
    pub async fn cloud_record_status(&self, model: String, row_id: String) -> Result<CloudRecordStatus, DbError> { let this = self.clone(); super::database::on_core(self.scope(), async move {
        this.not_closed()?;
        this.wire(&model)?;
        let row_id = super::database::id(&row_id)?;
        let db = this.db.clone();
        let result = this.offload(move || {
            if !db.cloud_model_eligible(&model)? || row_id.is_temp() { return Ok(CloudRecordStatus::Excluded); }
            let Some(scope) = db.cloud_status_scope() else { return Ok(CloudRecordStatus::NotAttached); };
            use crate::store::CloudRecordDisposition as D;
            Ok(match db.cloud_record_disposition(&scope, &model, &row_id)? {
                D::Excluded => CloudRecordStatus::Excluded, D::Held => CloudRecordStatus::Held, D::Waiting => CloudRecordStatus::Waiting, D::Missing => CloudRecordStatus::Missing, D::Queued => CloudRecordStatus::Queued, D::QueuedDeletion => CloudRecordStatus::QueuedDeletion, D::Synchronized => CloudRecordStatus::Synchronized, D::Deleted => CloudRecordStatus::Deleted, D::TooLarge => CloudRecordStatus::TooLarge, D::Rejected => CloudRecordStatus::Rejected,
            })
        }).await?;
        this.not_closed()?;
        Ok(result)
    }).await? }
}

#[derive(uniffi::Record)]
pub struct CloudConfig {
    pub backend: CloudBackend,
    pub application: CloudApplication,
    pub port: Arc<dyn CloudPort>,
    #[uniffi(default = true)]
    pub initially_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum CloudBackend { CloudKit, Drive }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum CloudApplication { Standalone { namespace: String }, Session { account_path: String } }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct CloudWireScope { pub epoch: u64, pub generation: u64, pub domain: String, pub provider_fingerprint: String }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum CloudAccount { Available { key: String }, SignedOut, Unavailable }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum CloudRelayEvent { Lease { milliseconds: u64 }, Ended, Suspended }

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
    relay: Box<dyn Fn(CloudWireScope, CloudRelayEvent) + Send + Sync>,
}

impl CloudSignals {
    pub(crate) fn new(
        account: impl Fn(CloudAccount) + Send + Sync + 'static,
        fetched: impl Fn(CloudFetched) -> FetchFuture + Send + Sync + 'static,
        relay: impl Fn(CloudWireScope, CloudRelayEvent) + Send + Sync + 'static,
    ) -> Self {
        Self { account: Box::new(account), fetched: Box::new(fetched), relay: Box::new(relay) }
    }
}

#[uniffi::export]
impl CloudSignals {
    pub fn account(&self, account: CloudAccount) { (self.account)(account); }
    pub fn relay(&self, scope: CloudWireScope, event: CloudRelayEvent) { (self.relay)(scope, event); }
    pub async fn fetched(&self, fetched: CloudFetched) -> Result<(), DbError> {
        super::database::on_core(super::database::Scope::root(), (self.fetched)(fetched)).await?
    }
}
