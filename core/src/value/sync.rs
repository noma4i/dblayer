use serde::{Deserialize, Serialize};

use crate::owners::write::operation_status::OperationStatus;
use crate::transport::SessionCode;
use crate::owners::write::upload::{UploadInfo, UploadState, UploadStatus, WaitReason};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncState {
    #[default]
    Synced,
    Pending,
    Sending,
    Failed,
}

impl SyncState {
    pub const ALL: [SyncState; 4] = [SyncState::Synced, SyncState::Pending, SyncState::Sending, SyncState::Failed];

    pub fn as_sql(self) -> &'static str {
        match self {
            SyncState::Synced => "synced",
            SyncState::Pending => "pending",
            SyncState::Sending => "sending",
            SyncState::Failed => "failed",
        }
    }

    pub fn from_sql(text: &str) -> rusqlite::Result<Self> {
        Self::ALL.into_iter().find(|state| state.as_sql() == text).ok_or_else(|| rusqlite::Error::FromSqlConversionFailure(
            0, rusqlite::types::Type::Text, Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("unknown stored sync state {text}")))
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    Network,
    Server,
    Rejected,
    MissingFile,
    Session(SessionCode),
    /// The sealed request was above the limit of the transport: the size and the limit in bytes.
    RequestTooLarge { size: u64, limit: u64 },
    Unrecognized,
    /// The answer of the server breaks the contract of the action: a batch answer lacks the row of an element.
    Protocol,
}

const SESSION_PREFIX: &str = "session:";
const TOO_LARGE_PREFIX: &str = "request_too_large:";

impl FailureKind {
    fn text(self) -> String {
        match self {
            FailureKind::Network => "network".into(),
            FailureKind::Server => "server".into(),
            FailureKind::Rejected => "rejected".into(),
            FailureKind::MissingFile => "missing_file".into(),
            FailureKind::Session(code) => format!("{SESSION_PREFIX}{}", code.name()),
            FailureKind::RequestTooLarge { size, limit } => format!("{TOO_LARGE_PREFIX}{size}:{limit}"),
            FailureKind::Unrecognized => "unrecognized".into(),
            FailureKind::Protocol => "protocol".into(),
        }
    }

    fn from_text(text: &str) -> Option<Self> {
        match text {
            "network" => Some(FailureKind::Network),
            "server" => Some(FailureKind::Server),
            "rejected" => Some(FailureKind::Rejected),
            "missing_file" => Some(FailureKind::MissingFile),
            "unrecognized" => Some(FailureKind::Unrecognized),
            "protocol" => Some(FailureKind::Protocol),
            _ => text.strip_prefix(TOO_LARGE_PREFIX).and_then(|numbers| numbers.split_once(':')).and_then(|(size, limit)| Some(FailureKind::RequestTooLarge { size: size.parse().ok()?, limit: limit.parse().ok()? })).or_else(|| text.strip_prefix(SESSION_PREFIX).and_then(SessionCode::named).map(FailureKind::Session)),
        }
    }
}

impl Serialize for FailureKind {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.text())
    }
}

impl<'de> Deserialize<'de> for FailureKind {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::from_text(&text).ok_or_else(|| serde::de::Error::custom(format!("unknown failure kind {text}")))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationFailure {
    pub kind: FailureKind,
    pub message: String,
    pub extensions: Option<crate::transport::GraphqlExtensions>,
}

impl OperationFailure {
    pub fn new(kind: FailureKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into(), extensions: None }
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
    pub uploads: std::collections::BTreeMap<String, UploadInfo>,
}

impl SyncInfo {
    #[cfg(test)]
    pub fn is_synced(&self) -> bool {
        self.state == SyncState::Synced
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncEntry {
    pub action: String,
    pub status: OperationStatus,
    pub transport_started: bool,
    pub attempts: u32,
    pub error: Option<OperationFailure>,
    pub since: i64,
    pub fields: Vec<String>,
    pub upload: Option<UploadState>,
    pub batch: bool,
}

impl SyncEntry {
    fn state(&self) -> SyncState {
        match (self.status, self.transport_started) {
            (OperationStatus::Failed, _) => SyncState::Failed,
            (OperationStatus::Pending, true) => SyncState::Sending,
            (OperationStatus::Pending, false) if self.upload.as_ref().is_some_and(|state| state.offline) => SyncState::Pending,
            (OperationStatus::Pending, false) if self.upload.as_ref().is_some_and(|state| state.phase == UploadStatus::Uploading || self.batch) => {
                SyncState::Sending
            }
            _ => SyncState::Pending,
        }
    }
}

fn rank(state: SyncState) -> u8 {
    match state {
        SyncState::Failed => 3,
        SyncState::Sending => 2,
        SyncState::Pending => 1,
        SyncState::Synced => 0,
    }
}

impl SyncInfo {
    pub fn fold_transfers(&mut self, batched: &std::collections::BTreeSet<String>) {
        let behind_create = self.actions.iter().any(|action| action == "create") && self.state != SyncState::Synced;
        let shown = self.uploads.iter_mut().map(|(action, upload)| {
            if behind_create && upload.status == UploadStatus::Queued {
                upload.status = UploadStatus::Waiting;
                upload.wait_reason = Some(WaitReason::CreateUnconfirmed);
            }
            match upload.status {
                UploadStatus::Failed => SyncState::Failed,
                UploadStatus::Uploading | UploadStatus::Sending => SyncState::Sending,
                UploadStatus::Waiting => SyncState::Pending,
                _ if batched.contains(action) => SyncState::Sending,
                _ => SyncState::Pending,
            }
        }).max_by_key(|state| rank(*state));
        let Some(shown) = shown.filter(|shown| rank(*shown) > rank(self.state)) else { return };
        self.state = shown;
        if shown == SyncState::Failed && self.error.is_none() {
            self.error = self.uploads.values().find(|upload| upload.status == UploadStatus::Failed).and_then(|upload| upload.error.clone());
        }
    }
}

const PRECEDENCE: [SyncState; 3] = [SyncState::Failed, SyncState::Sending, SyncState::Pending];

pub fn fold_entries(entries: &[SyncEntry]) -> Option<SyncInfo> {
    let decisive = PRECEDENCE.iter().find_map(|state| entries.iter().find(|entry| entry.state() == *state))?;
    let mut dirty_fields: Vec<String> = Vec::new();
    for field in entries.iter().flat_map(|entry| &entry.fields) {
        if !dirty_fields.contains(field) {
            dirty_fields.push(field.clone());
        }
    }
    let waiting = entries.iter().any(|entry| entry.action == "create").then_some(WaitReason::CreateUnconfirmed);
    let uploads = entries
        .iter()
        .filter_map(|entry| {
            let action = entry.action.strip_prefix("upload:")?;
            let state = entry.upload.as_ref()?;
            Some((action.to_string(), UploadInfo::of(entry.status, entry.transport_started, state, waiting, entry.error.clone().or_else(|| state.background_refused.as_ref().map(|detail| OperationFailure::new(crate::value::sync::FailureKind::Unrecognized, format!("background transfer was not started: {detail}")))))))
        })
        .collect();
    Some(SyncInfo {
        state: decisive.state(),
        actions: entries.iter().filter(|entry| entry.upload.is_none()).map(|entry| entry.action.clone()).collect(),
        dirty_fields,
        error: decisive.error.clone(),
        attempts: decisive.attempts,
        since: decisive.since,
        uploads,
    })
}
