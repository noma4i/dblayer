use async_trait::async_trait;

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum CloudValue {
    Null,
    Flag { value: bool },
    Whole { value: i64 },
    Fraction { value: f64 },
    Text { value: String },
    Tree { json: String },
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct CloudChange {
    pub model: String,
    pub row_id: String,
    pub field: String,
    pub clock_ms: i64,
    pub clock_count: u32,
    pub replica: String,
    pub value: CloudValue,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct CloudPage {
    pub changes: Vec<CloudChange>,
    pub cursor: Vec<u8>,
    pub more: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum CloudAccount {
    Available,
    SignedOut,
    NeedsConsent,
    Restricted,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error, uniffi::Error)]
pub enum CloudError {
    #[error("cloud offline: {detail}")]
    Offline { detail: String },
    #[error("cloud throttled, retry after {retry_after_ms} ms")]
    Throttled { retry_after_ms: u64 },
    #[error("cloud unavailable: {detail}")]
    Unavailable { detail: String },
    #[error("cloud needs authorization: {detail}")]
    NeedsAuthorization { detail: String },
    #[error("cloud quota exceeded: {detail}")]
    QuotaExceeded { detail: String },
    #[error("cloud rejected the request: {detail}")]
    Rejected { detail: String },
}

#[uniffi::export(with_foreign)]
#[async_trait]
pub trait CloudIo: Send + Sync {
    async fn account(&self) -> Result<CloudAccount, CloudError>;
    async fn pull(&self, cursor: Option<Vec<u8>>, limit: u32) -> Result<CloudPage, CloudError>;
    async fn push(&self, changes: Vec<CloudChange>) -> Result<(), CloudError>;
}
