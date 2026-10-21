use crate::commit::OperationStatus;

use crate::upload::{UploadInfo, UploadState, UploadStatus, WaitReason};

use super::info::{OperationFailure, SyncInfo, SyncState};

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
}

impl SyncEntry {
    fn state(&self) -> SyncState {
        match (self.status, self.transport_started) {
            (OperationStatus::Failed, _) => SyncState::Failed,
            (OperationStatus::DeliveryUnknown, _) => SyncState::DeliveryUnknown,
            (OperationStatus::Pending, true) => SyncState::Sending,
            (OperationStatus::Pending, false) if self.upload.as_ref().is_some_and(|state| state.phase == UploadStatus::Uploading || self.action != "upload") => {
                SyncState::Sending
            }
            _ => SyncState::Pending,
        }
    }
}

const PRECEDENCE: [SyncState; 4] = [SyncState::Failed, SyncState::DeliveryUnknown, SyncState::Sending, SyncState::Pending];

pub fn fold_entries(entries: &[SyncEntry]) -> Option<SyncInfo> {
    let decisive = PRECEDENCE.iter().find_map(|state| entries.iter().find(|entry| entry.state() == *state))?;
    let mut dirty_fields: Vec<String> = Vec::new();
    for field in entries.iter().flat_map(|entry| &entry.fields) {
        if !dirty_fields.contains(field) {
            dirty_fields.push(field.clone());
        }
    }
    let waiting = entries.iter().any(|entry| entry.action == "create").then_some(WaitReason::CreateUnconfirmed);
    let upload = entries
        .iter()
        .find_map(|entry| entry.upload.as_ref().map(|state| UploadInfo::of(entry.status, entry.transport_started, state, waiting, entry.error.clone())));
    Some(SyncInfo {
        state: decisive.state(),
        actions: entries.iter().map(|entry| entry.action.clone()).collect(),
        dirty_fields,
        error: decisive.error.clone(),
        attempts: decisive.attempts,
        since: decisive.since,
        upload,
    })
}
