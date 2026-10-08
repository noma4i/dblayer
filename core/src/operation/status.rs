use serde::{Deserialize, Serialize};

use crate::identity::RowId;
use crate::schema::ActionMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OperationId(pub i64);

pub use crate::commit::OperationStatus;

pub const ALL_STATUSES: [OperationStatus; 5] = [
    OperationStatus::Pending,
    OperationStatus::DeliveryUnknown,
    OperationStatus::Committed,
    OperationStatus::Failed,
    OperationStatus::RolledBack,
];

pub fn is_open(status: OperationStatus) -> bool {
    !removes_row(status)
}

pub fn mode_text(mode: ActionMode) -> &'static str {
    match mode {
        ActionMode::Request => "request",
        ActionMode::Durable => "durable",
    }
}

pub fn parse_mode(text: &str) -> Option<ActionMode> {
    [ActionMode::Request, ActionMode::Durable].into_iter().find(|mode| mode_text(*mode) == text)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    pub id: OperationId,
    pub action_key: String,
    pub mode: ActionMode,
    pub status: OperationStatus,
    pub row_ids: Vec<RowId>,
    pub temp_id: Option<RowId>,
    pub transport_started: bool,
    pub base_revision: i64,
    pub created_at: i64,
    pub dedupe_key: Option<String>,
}

impl Operation {
    pub fn new(action_key: &str, mode: ActionMode, row_ids: Vec<RowId>, base_revision: i64, created_at: i64) -> Self {
        Self {
            id: OperationId(0),
            action_key: action_key.to_string(),
            mode,
            status: OperationStatus::Pending,
            row_ids,
            temp_id: None,
            transport_started: false,
            base_revision,
            created_at,
            dedupe_key: None,
        }
    }

    pub fn action_key(model: &str, action: &str) -> String {
        format!("{model}:{action}")
    }

    pub fn split_action_key(action_key: &str) -> Option<(String, String)> {
        let mut parts = action_key.split(':');
        match (parts.next(), parts.next(), parts.next()) {
            (Some(model), Some(action), None) if !model.is_empty() && !action.is_empty() => {
                Some((model.to_string(), action.to_string()))
            }
            _ => None,
        }
    }
}

pub fn removes_row(status: OperationStatus) -> bool {
    status.is_terminal()
}

pub fn recovered_status(transport_started: bool, status: OperationStatus) -> OperationStatus {
    match status {
        OperationStatus::Pending if transport_started => OperationStatus::DeliveryUnknown,
        OperationStatus::Pending => OperationStatus::Failed,
        other => other,
    }
}

pub fn dedupe_namespace(operation: &Operation) -> Option<String> {
    operation.dedupe_key.as_deref().filter(|key| !key.is_empty()).map(|_| operation.action_key.clone())
}
