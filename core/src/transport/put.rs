use std::sync::Arc;

use super::cancel::CancelToken;

pub type ByteSink = Arc<dyn Fn(u64) + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct PutFile {
    pub path: String,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum PutTaskState {
    Running,
    Finished { status: u16, etag: Option<String> },
    Failed { detail: String },
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct PutTask {
    pub tag: String,
    pub state: PutTaskState,
}

#[derive(Clone)]
pub struct PutRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub file: PutFile,
    pub tag: String,
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
