//! The queue of operations as the platform reads it: one row of the table `op` per operation that has not ended.

use super::wire::{failure, WireFailure, WireUpload, WireUploadStatus};
use crate::owners::write::operation_queue::{QueueStatus, QueuedOperation};

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum OperationQueueStatus {
    Pending,
    Sending,
    Failed,
}

/// An operation of the queue. `model`, `row_id` and `action` are the arguments of `retry` and `discard`.
#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct OperationInfo {
    pub seq: i64,
    pub model: String,
    pub row_id: String,
    pub action: String,
    pub status: OperationQueueStatus,
    pub attempts: u32,
    pub error: Option<WireFailure>,
    pub created_at: i64,
    /// The failed operation of the same row that this one waits behind.
    pub blocked_by: Option<i64>,
    /// The value of `uploads[action]` of an active transfer, the same value the row shows.
    pub upload: Option<super::wire::WireUpload>,
}

impl OperationInfo {
    pub(crate) fn of_failed_transfer(transfer: crate::store::ActiveTransfer) -> Self {
        let error = crate::value::sync::OperationFailure::new(crate::value::sync::FailureKind::Unrecognized, transfer.restore_error.unwrap_or_default());
        Self { seq: transfer.transfer_id, model: transfer.model, row_id: transfer.row_id, action: transfer.action, status: OperationQueueStatus::Failed, attempts: 0, error: Some(failure(&error)), created_at: 0, blocked_by: None, upload: None }
    }
    /// The queue entry of a transfer that has no operation row yet: it shows the value of the transfer.
    pub(crate) fn of_transfer(transfer: crate::store::ActiveTransfer, shown: WireUpload) -> Self {
        let status = match shown.status {
            WireUploadStatus::Failed => OperationQueueStatus::Failed,
            WireUploadStatus::Waiting | WireUploadStatus::Preparing | WireUploadStatus::Queued => OperationQueueStatus::Pending,
            _ => OperationQueueStatus::Sending,
        };
        Self { seq: transfer.transfer_id, model: transfer.model, row_id: transfer.row_id, action: transfer.action, status, attempts: shown.attempts, error: shown.error.clone(), created_at: 0, blocked_by: None, upload: Some(shown) }
    }
}

impl From<QueuedOperation> for OperationInfo {
    fn from(queued: QueuedOperation) -> Self {
        let status = match queued.status {
            QueueStatus::Pending => OperationQueueStatus::Pending,
            QueueStatus::Sending => OperationQueueStatus::Sending,
            QueueStatus::Failed => OperationQueueStatus::Failed,
        };
        Self { seq: queued.seq, model: queued.model, row_id: queued.row_id, action: queued.action, status, attempts: queued.attempts, error: queued.error.as_ref().map(failure), created_at: queued.created_at, blocked_by: queued.blocked_by, upload: None }
    }
}
