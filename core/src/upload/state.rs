use serde::{Deserialize, Serialize};

use crate::commit::OperationStatus;
use crate::syncstate::OperationFailure;

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
    DeliveryUnknown,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitReason {
    CreateUnconfirmed,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadState {
    pub phase: UploadStatus,
    pub stage: UploadStage,
    pub scenario: Option<UploadScenario>,
    pub total_bytes: u64,
    pub sent_bytes: u64,
    pub retries: u32,
    pub key: Option<String>,
    pub upload_id: Option<String>,
    pub parts: Vec<UploadPart>,
    pub signed_id: Option<String>,
    pub blob_id: Option<String>,
}

impl UploadState {
    pub fn queued() -> Self {
        Self {
            phase: UploadStatus::Queued,
            stage: UploadStage::Prepare,
            scenario: None,
            total_bytes: 0,
            sent_bytes: 0,
            retries: 0,
            key: None,
            upload_id: None,
            parts: Vec::new(),
            signed_id: None,
            blob_id: None,
        }
    }

    pub fn is_transferred(&self) -> bool {
        self.signed_id.is_some() && self.blob_id.is_some()
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
}

impl UploadInfo {
    pub fn of(status: OperationStatus, transport_started: bool, state: &UploadState, wait_reason: Option<WaitReason>, error: Option<OperationFailure>) -> Self {
        let shown = match (status, transport_started) {
            (OperationStatus::Failed, _) => UploadStatus::Failed,
            (OperationStatus::DeliveryUnknown, _) => UploadStatus::DeliveryUnknown,
            (_, true) => UploadStatus::Sending,
            _ if wait_reason.is_some() => UploadStatus::Waiting,
            _ => state.phase,
        };
        let wait_reason = (shown == UploadStatus::Waiting).then_some(wait_reason).flatten();
        Self { status: shown, stage: state.stage, sent_bytes: state.sent_bytes, total_bytes: state.total_bytes, wait_reason, attempts: state.retries, error }
    }

    #[cfg(test)]
    pub fn fraction(&self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        (self.sent_bytes as f64 / self.total_bytes as f64).min(1.0)
    }

    #[cfg(test)]
    pub fn is_retryable(&self) -> bool {
        self.error.as_ref().is_some_and(|error| error.kind != crate::syncstate::FailureKind::Rejected)
    }
}
