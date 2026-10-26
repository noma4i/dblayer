use super::*;
use crate::operation::OperationStatus;

fn entry(action: &str, status: OperationStatus, started: bool, since: i64) -> SyncEntry {
    SyncEntry { action: action.to_string(), status, transport_started: started, attempts: 0, error: None, since, fields: Vec::new(), upload: None, batch: false }
}

fn failure(text: &str) -> Option<OperationFailure> {
    Some(OperationFailure::new(FailureKind::Rejected, text))
}

#[test]
fn a_single_open_operation_gives_its_state_action_attempts_and_creation_time() {
    assert_eq!(fold_entries(&[]), None, "no open operations");
    let pending = fold_entries(&[entry("create", OperationStatus::Pending, false, 7)]).unwrap();
    assert_eq!(
        (pending.state, pending.actions, pending.attempts, pending.since, pending.error),
        (SyncState::Pending, vec!["create".to_string()], 0, 7, None),
        "pending"
    );
    let sending = fold_entries(&[SyncEntry { attempts: 1, ..entry("update", OperationStatus::Pending, true, 9) }]).unwrap();
    assert_eq!((sending.state, sending.attempts, sending.since), (SyncState::Sending, 1, 9), "started pending");
}

#[test]
fn inv_sync_01_failed_beats_delivery_unknown_beats_sending_beats_pending() {
    let pending = entry("a", OperationStatus::Pending, false, 1);
    let sending = entry("b", OperationStatus::Pending, true, 2);
    let unknown = SyncEntry { error: failure("lost"), ..entry("c", OperationStatus::DeliveryUnknown, true, 3) };
    let failed = SyncEntry { attempts: 2, error: failure("no"), ..entry("d", OperationStatus::Failed, true, 4) };
    let state = |entries: &[SyncEntry]| fold_entries(entries).unwrap().state;
    assert_eq!(state(&[pending.clone(), sending.clone()]), SyncState::Sending);
    assert_eq!(state(&[pending.clone(), sending.clone(), unknown.clone()]), SyncState::DeliveryUnknown);
    let info = fold_entries(&[pending, sending, unknown, failed]).unwrap();
    assert_eq!((info.state, info.attempts, info.since, info.error), (SyncState::Failed, 2, 4, failure("no")));
    assert_eq!(info.actions, ["a", "b", "c", "d"]);
}

#[test]
fn inv_sync_01_dirty_fields_are_the_unique_union_in_queue_order() {
    let first = SyncEntry { fields: vec!["text".into(), "likes".into()], ..entry("update", OperationStatus::Pending, false, 1) };
    let second = SyncEntry { fields: vec!["likes".into(), "media".into()], ..entry("update", OperationStatus::Pending, false, 2) };
    assert_eq!(fold_entries(&[first, second]).unwrap().dirty_fields, ["text", "likes", "media"]);
}

#[test]
fn inv_sync_01_inv_sync_04_random_sets_of_open_operations_fold_to_the_state_attempts_since_and_error_of_the_oracle() {
    let rank = |entry: &SyncEntry| match (entry.status, entry.transport_started) {
        (OperationStatus::Failed, _) => 0,
        (OperationStatus::DeliveryUnknown, _) => 1,
        (OperationStatus::Pending, true) => 2,
        (OperationStatus::Pending, false) => 3,
        (OperationStatus::Committed | OperationStatus::RolledBack, _) => 4,
    };
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = move |bound: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % bound
    };
    for round in 0..2000 {
        let entries: Vec<SyncEntry> = (0..next(6))
            .map(|turn| {
                let status = [OperationStatus::Pending, OperationStatus::Pending, OperationStatus::DeliveryUnknown, OperationStatus::Failed][next(4) as usize];
                let started = next(2) == 1;
                SyncEntry { attempts: next(5) as u32, error: failure(&format!("e{turn}")), ..entry(&format!("a{turn}"), status, started, 10 + turn as i64) }
            })
            .collect();
        let Some(info) = fold_entries(&entries) else {
            assert!(entries.is_empty(), "round {round}: open operations folded to nothing");
            continue;
        };
        let best = entries.iter().map(&rank).min().unwrap();
        let decisive = entries.iter().find(|entry| rank(entry) == best).unwrap();
        let expected = [SyncState::Failed, SyncState::DeliveryUnknown, SyncState::Sending, SyncState::Pending][best];
        assert_eq!((info.state, info.attempts, info.since, info.error.clone()), (expected, decisive.attempts, decisive.since, decisive.error.clone()), "round {round}: {entries:?}");
    }
}

#[test]
fn state_names_round_trip_through_sql() {
    for state in SyncState::ALL {
        assert_eq!(SyncState::from_sql(state.as_sql()), Some(state));
    }
    assert_eq!(SyncState::from_sql("bogus"), None);
}

#[test]
fn inv_upload_14_an_upload_behind_an_open_create_folds_to_waiting_and_stops_waiting_without_it() {
    use crate::upload::{UploadState, UploadStatus, WaitReason};
    let upload = SyncEntry { upload: Some(UploadState::queued()), ..entry("upload:upload", OperationStatus::Pending, false, 2) };
    let behind = fold_entries(&[entry("create", OperationStatus::Pending, false, 1), upload.clone()]).unwrap().uploads.get("upload").cloned().unwrap();
    assert_eq!((behind.status, behind.wait_reason), (UploadStatus::Waiting, Some(WaitReason::CreateUnconfirmed)));
    let alone = fold_entries(&[upload]).unwrap().uploads.get("upload").cloned().unwrap();
    assert_eq!((alone.status, alone.wait_reason), (UploadStatus::Queued, None));
}

#[test]
fn an_offline_upload_folds_to_waiting_offline_and_the_row_stays_pending() {
    use crate::upload::{UploadState, UploadStage, UploadStatus, WaitReason};
    let state = UploadState { phase: UploadStatus::Uploading, stage: UploadStage::Create, offline: true, ..UploadState::queued() };
    let upload = SyncEntry { upload: Some(state.clone()), ..entry("upload:upload", OperationStatus::Pending, false, 1) };
    let info = fold_entries(&[upload]).unwrap();
    let shown = info.uploads.get("upload").cloned().unwrap();
    assert_eq!((info.state, shown.status, shown.wait_reason), (SyncState::Pending, UploadStatus::Waiting, Some(WaitReason::Offline)));
    let online = SyncEntry { upload: Some(UploadState { offline: false, ..state }), ..entry("upload:upload", OperationStatus::Pending, false, 1) };
    let info = fold_entries(&[online]).unwrap();
    assert_eq!((info.state, info.uploads.get("upload").unwrap().status), (SyncState::Sending, UploadStatus::Uploading));
}
