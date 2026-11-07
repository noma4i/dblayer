//! The queue of operations as the platform reads it: one row of the table `op` per operation that has not ended.

use super::wire::{failure, WireFailure};
use crate::owners::write::operation_status::{Operation, OperationStatus};

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum OperationQueueStatus {
    Pending,
    Sending,
    Failed,
}

/// An operation of the queue. `model`, `row_id` and `action` are the arguments of `retry` and `discard`.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
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
}

fn status_of(status: OperationStatus) -> Option<OperationQueueStatus> {
    match status {
        OperationStatus::Pending => Some(OperationQueueStatus::Pending),
        OperationStatus::Sending => Some(OperationQueueStatus::Sending),
        OperationStatus::Failed => Some(OperationQueueStatus::Failed),
        OperationStatus::Committed | OperationStatus::RolledBack => None,
    }
}

/// The open operations in `seq` order.
pub(crate) fn queue_of(operations: &[Operation]) -> Vec<OperationInfo> {
    let row_of = |operation: &Operation| operation.row_ids.first().or(operation.temp_id.as_ref()).map(|row| row.as_str().to_string());
    let mut infos = Vec::with_capacity(operations.len());
    for operation in operations {
        let (Some(status), Some((model, action)), Some(row_id)) = (status_of(operation.status), Operation::split_action_key(&operation.action_key), row_of(operation)) else { continue };
        let blocked_by = operations
            .iter()
            .filter(|earlier| earlier.id < operation.id && earlier.status == OperationStatus::Failed)
            .filter(|earlier| Operation::split_action_key(&earlier.action_key).is_some_and(|(earlier_model, _)| earlier_model == model) && row_of(earlier).as_deref() == Some(row_id.as_str()))
            .map(|earlier| earlier.id.0)
            .max();
        infos.push(OperationInfo { seq: operation.id.0, model, row_id, action, status, attempts: operation.attempts, error: operation.error.as_ref().map(failure), created_at: operation.created_at, blocked_by });
    }
    infos
}
