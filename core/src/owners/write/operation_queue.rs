//! The queue of operations the platform reads: the open rows of the table `op` in `seq` order, each with the failed operation of its row that it waits behind.

use crate::owners::write::operation_status::{Operation, OperationStatus};
use crate::value::sync::OperationFailure;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueueStatus {
    Pending,
    Sending,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueuedOperation {
    pub seq: i64,
    pub model: String,
    pub row_id: String,
    pub action: String,
    pub status: QueueStatus,
    pub attempts: u32,
    pub error: Option<OperationFailure>,
    pub created_at: i64,
    pub blocked_by: Option<i64>,
}

fn status_of(status: OperationStatus) -> Option<QueueStatus> {
    match status {
        OperationStatus::Pending => Some(QueueStatus::Pending),
        OperationStatus::Sending => Some(QueueStatus::Sending),
        OperationStatus::Failed => Some(QueueStatus::Failed),
        OperationStatus::Committed | OperationStatus::RolledBack => None,
    }
}

fn opened(operation: &Operation) -> Result<Option<QueuedOperation>, String> {
    let Some(status) = status_of(operation.status) else { return Ok(None) };
    let (model, action) = Operation::split_action_key(&operation.action_key).ok_or_else(|| format!("operation {} has the malformed key {}", operation.id.0, operation.action_key))?;
    let row_id = operation.row_ids.first().or(operation.temp_id.as_ref()).map(|row| row.as_str().to_string()).ok_or_else(|| format!("operation {} names no row", operation.id.0))?;
    Ok(Some(QueuedOperation { seq: operation.id.0, model, row_id, action, status, attempts: operation.attempts, error: operation.error.clone(), created_at: operation.created_at, blocked_by: None }))
}

/// The open operations in `seq` order. A row of the table that the queue cannot show is an error of the read.
pub(crate) fn queue_of(operations: &[Operation]) -> Result<Vec<QueuedOperation>, String> {
    let mut open = operations.iter().filter_map(|operation| opened(operation).transpose()).collect::<Result<Vec<_>, _>>()?;
    let blocked: Vec<Option<i64>> = open.iter().map(|item| open.iter().filter(|earlier| earlier.seq < item.seq && earlier.status == QueueStatus::Failed && earlier.model == item.model && earlier.row_id == item.row_id).map(|earlier| earlier.seq).max()).collect();
    for (item, blocked_by) in open.iter_mut().zip(blocked) {
        item.blocked_by = blocked_by;
    }
    Ok(open)
}
