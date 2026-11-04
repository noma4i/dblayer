use std::sync::Arc;

use super::cancel::CancelToken;

pub type ByteSink = Arc<dyn Fn(u64) + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum TransferStep { Put, Part { number: u32 } }

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TransferIdentity {
    pub operation_id: i64,
    pub item_id: String,
    pub step: TransferStep,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct PutFile {
    pub path: String,
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone)]
pub struct PutRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub file: PutFile,
    pub identity: TransferIdentity,
    pub cancel: CancelToken,
    pub sent: ByteSink,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutResponse {
    pub status: u16,
    pub etag: Option<String>,
}

impl PutResponse {
    pub fn with_etag(etag: &str) -> Self {
        Self { status: 200, etag: Some(etag.to_string()) }
    }
}
