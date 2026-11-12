use serde::{Deserialize, Serialize};

use crate::value::sync::OperationFailure;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadStatus {
    Waiting,
    Preparing,
    Queued,
    Uploading,
    Sending,
    Done,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitReason {
    CreateUnconfirmed,
    SourcePending,
    Offline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadStage {
    Prepare,
    Create,
    Put,
    Init,
    Presign,
    Part,
    Complete,
    Abort,
    Mutation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadScenario {
    Single,
    Multipart,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadPart {
    pub number: u32,
    pub etag: String,
    pub confirmed_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadRetry {
    pub stage: UploadStage,
    pub part_number: u32,
    pub attempts: u32,
    pub retry_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadState {
    pub phase: UploadStatus,
    pub stage: UploadStage,
    pub scenario: Option<UploadScenario>,
    pub total_bytes: u64,
    pub sent_bytes: u64,
    pub retries: Vec<UploadRetry>,
    pub key: Option<String>,
    pub upload_id: Option<String>,
    pub parts: Vec<UploadPart>,
    pub signed_id: Option<String>,
    pub blob_id: Option<String>,
    pub request: Option<UploadRequest>,
    pub transferred: bool,
    pub confirmed_bytes: u64,
    pub checksum: Option<String>,
    pub put_url: Option<String>,
    pub url_issued_at: Option<u64>,
    pub failed_at: Option<u64>,
    pub headers: Vec<(String, String)>,
    pub error: Option<OperationFailure>,
    #[serde(default)]
    pub background_refused: Option<String>,
    #[serde(default)]
    pub offline: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadRequest {
    pub document: String,
    pub name: String,
    pub variables: serde_json::Value,
}

impl UploadState {
    pub fn queued() -> Self {
        Self {
            phase: UploadStatus::Queued,
            stage: UploadStage::Prepare,
            scenario: None,
            total_bytes: 0,
            sent_bytes: 0,
            retries: Vec::new(),
            key: None,
            upload_id: None,
            parts: Vec::new(),
            signed_id: None,
            blob_id: None,
            request: None,
            transferred: false,
            confirmed_bytes: 0,
            checksum: None,
            put_url: None,
            url_issued_at: None,
            failed_at: None,
            headers: Vec::new(),
            error: None,
            background_refused: None,
            offline: false,
        }
    }

    pub fn attempts(&self) -> u32 {
        self.retries.iter().map(|retry| retry.attempts).sum()
    }
}

/// The server works on the file of a finished transfer (`processing` of the action).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UploadProcessing {
    pub progress: Option<f64>,
}

/// The progress is a finite number read from JSON, so equality is total.
impl Eq for UploadProcessing {}

/// The file of a transfer: the copy of the library once it is durable, the file of the caller before (D-376).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferFile {
    Copy(String),
    Caller(String),
}

impl TransferFile {
    /// The one rule of the file of a transfer: a durable copy is `size > 0`, before it the source of the row while the accepting call runs (a failed row has no file of the library).
    pub fn of(size: u64, copy_id: String, source: Option<String>, failed: bool) -> Option<Self> {
        if size > 0 { Some(Self::Copy(copy_id)) } else { source.filter(|_| !failed).map(Self::Caller) }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadInfo {
    pub status: UploadStatus,
    pub stage: UploadStage,
    pub sent_bytes: u64,
    pub total_bytes: u64,
    pub wait_reason: Option<WaitReason>,
    pub attempts: u32,
    pub error: Option<OperationFailure>,
    /// The file URL of the copy of the library from the end of the copy to the removal of the transfer.
    #[serde(default)]
    pub local_url: Option<String>,
    #[serde(default)]
    pub processing: Option<UploadProcessing>,
    /// The file the transfer reads now: `local_url` is made from it at the read.
    #[serde(skip)]
    pub copy_file: Option<TransferFile>,
    /// The share shown last in hundredths.
    #[serde(default)]
    pub share: u32,
}

