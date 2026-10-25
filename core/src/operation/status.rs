use serde::{Deserialize, Serialize};

use crate::identity::RowId;
use crate::syncstate::OperationFailure;
use crate::upload::UploadState;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OperationId(pub i64);

pub use crate::commit::OperationStatus;

pub fn is_open(status: OperationStatus) -> bool {
    !removes_row(status)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    pub id: OperationId,
    pub action_key: String,
    pub status: OperationStatus,
    pub row_ids: Vec<RowId>,
    pub temp_id: Option<RowId>,
    pub transport_started: bool,
    pub base_revision: i64,
    pub created_at: i64,
    pub dedupe_key: Option<String>,
    pub client_mutation_id: String,
    pub attempts: u32,
    pub error: Option<OperationFailure>,
    pub since: i64,
    pub upload: Option<UploadState>,
}

impl Operation {
    #[cfg(test)]
    pub fn new(action_key: &str, row_ids: Vec<RowId>, base_revision: i64, created_at: i64) -> Self {
        Self {
            id: OperationId(0),
            action_key: action_key.to_string(),
            status: OperationStatus::Pending,
            row_ids,
            temp_id: None,
            transport_started: false,
            base_revision,
            created_at,
            dedupe_key: None,
            client_mutation_id: String::new(),
            attempts: 0,
            error: None,
            since: created_at,
            upload: None,
        }
    }

    pub fn action_key(model: &str, action: &str) -> String {
        format!("{model}:{action}")
    }

    pub fn upload_action_key(model: &str, action: &str) -> String {
        format!("{model}:upload:{action}")
    }

    pub fn split_action_key(action_key: &str) -> Option<(String, String)> {
        let mut parts = action_key.split(':');
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(model), Some(action), None, None) if !model.is_empty() && !action.is_empty() => {
                Some((model.to_string(), action.to_string()))
            }
            (Some(model), Some("upload"), Some(action), None) if !model.is_empty() && !action.is_empty() => {
                Some((model.to_string(), format!("upload:{action}")))
            }
            _ => None,
        }
    }

    pub fn upload_action(action_key: &str) -> Option<(String, String)> {
        let (model, rest) = Self::split_action_key(action_key)?;
        rest.strip_prefix("upload:").map(|action| (model, action.to_string()))
    }
}

pub fn removes_row(status: OperationStatus) -> bool {
    status.is_terminal()
}
