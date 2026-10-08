use serde::{Deserialize, Serialize};

use crate::upload::UploadInfo;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncState {
    #[default]
    Synced,
    Pending,
    Sending,
    Failed,
    DeliveryUnknown,
}

impl SyncState {
    pub const ALL: [SyncState; 5] =
        [SyncState::Synced, SyncState::Pending, SyncState::Sending, SyncState::Failed, SyncState::DeliveryUnknown];

    pub fn as_sql(self) -> &'static str {
        match self {
            SyncState::Synced => "synced",
            SyncState::Pending => "pending",
            SyncState::Sending => "sending",
            SyncState::Failed => "failed",
            SyncState::DeliveryUnknown => "delivery_unknown",
        }
    }

    pub fn from_sql(text: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|state| state.as_sql() == text)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    Network,
    Server,
    Rejected,
    Unrecognized,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationFailure {
    pub kind: FailureKind,
    pub message: String,
}

impl OperationFailure {
    pub fn new(kind: FailureKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SyncInfo {
    pub state: SyncState,
    pub actions: Vec<String>,
    pub dirty_fields: Vec<String>,
    pub error: Option<OperationFailure>,
    pub attempts: u32,
    pub since: i64,
    pub upload: Option<UploadInfo>,
}

pub type SyncStamp = (SyncState, u32, Option<UploadInfo>);

impl SyncInfo {
    pub fn is_synced(&self) -> bool {
        self.state == SyncState::Synced
    }

    pub fn is_pending(&self) -> bool {
        self.state == SyncState::Pending
    }

    pub fn is_failed(&self) -> bool {
        self.state == SyncState::Failed
    }

    pub fn stamp(&self) -> SyncStamp {
        (self.state, self.attempts, self.upload.clone())
    }
}
