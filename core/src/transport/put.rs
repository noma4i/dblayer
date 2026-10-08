use std::sync::Arc;

use super::cancel::CancelToken;

pub type ByteSink = Arc<dyn Fn(u64) + Send + Sync>;

#[derive(Clone)]
pub struct PutRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
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
