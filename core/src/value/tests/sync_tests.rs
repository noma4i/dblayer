use super::sync::*;
use crate::owners::write::operation_status::OperationStatus;

fn entry(action: &str, status: OperationStatus, since: i64) -> SyncEntry {
    SyncEntry { action: action.to_string(), status, attempts: 0, error: None, since, fields: Vec::new() }
}

fn failure(text: &str) -> Option<OperationFailure> {
    Some(OperationFailure::new(FailureKind::Rejected, text))
}

#[test]
fn a_single_open_operation_gives_its_state_action_attempts_and_creation_time() {
    assert_eq!(fold_entries(&[]), None, "no open operations");
    let pending = fold_entries(&[entry("create", OperationStatus::Pending, 7)]).unwrap();
    assert_eq!(
        (pending.state, pending.actions, pending.attempts, pending.since, pending.error),
        (SyncState::Pending, vec!["create".to_string()], 0, 7, None),
        "pending"
    );
    let sending = fold_entries(&[SyncEntry { attempts: 1, ..entry("update", OperationStatus::Sending, 9) }]).unwrap();
    assert_eq!((sending.state, sending.attempts, sending.since), (SyncState::Sending, 1, 9), "sending status of a plain operation");
}

#[test]
fn inv_sync_01_failed_beats_sending_beats_pending() {
    let pending = entry("a", OperationStatus::Pending, 1);
    let sending = entry("b", OperationStatus::Sending, 2);
    let failed = SyncEntry { attempts: 2, error: failure("no"), ..entry("d", OperationStatus::Failed, 4) };
    let state = |entries: &[SyncEntry]| fold_entries(entries).unwrap().state;
    assert_eq!(state(&[pending.clone(), sending.clone()]), SyncState::Sending);
    let info = fold_entries(&[pending, sending, failed]).unwrap();
    assert_eq!((info.state, info.attempts, info.since, info.error), (SyncState::Failed, 2, 4, failure("no")));
    assert_eq!(info.actions, ["a", "b", "d"]);
}

#[test]
fn inv_sync_01_dirty_fields_are_the_unique_union_in_queue_order() {
    let first = SyncEntry { fields: vec!["text".into(), "likes".into()], ..entry("update", OperationStatus::Pending, 1) };
    let second = SyncEntry { fields: vec!["likes".into(), "media".into()], ..entry("update", OperationStatus::Pending, 2) };
    assert_eq!(fold_entries(&[first, second]).unwrap().dirty_fields, ["text", "likes", "media"]);
}

#[test]
fn inv_sync_01_inv_sync_04_random_sets_of_open_operations_fold_to_the_state_attempts_since_and_error_of_the_oracle() {
    let rank = |entry: &SyncEntry| match entry.status {
        OperationStatus::Failed => 0,
        OperationStatus::Sending => 1,
        OperationStatus::Pending => 2,
        OperationStatus::Committed | OperationStatus::RolledBack => 3,
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
                let status = [OperationStatus::Pending, OperationStatus::Sending, OperationStatus::Failed][next(3) as usize];
                SyncEntry { attempts: next(5) as u32, error: failure(&format!("e{turn}")), ..entry(&format!("a{turn}"), status, 10 + turn as i64) }
            })
            .collect();
        let Some(info) = fold_entries(&entries) else {
            assert!(entries.is_empty(), "round {round}: open operations folded to nothing");
            continue;
        };
        let best = entries.iter().map(&rank).min().unwrap();
        let decisive = entries.iter().find(|entry| rank(entry) == best).unwrap();
        let expected = [SyncState::Failed, SyncState::Sending, SyncState::Pending][best];
        assert_eq!((info.state, info.attempts, info.since, info.error.clone()), (expected, decisive.attempts, decisive.since, decisive.error.clone()), "round {round}: {entries:?}");
    }
}

#[test]
fn state_names_round_trip_through_sql() {
    for state in SyncState::ALL {
        assert_eq!(SyncState::from_sql(state.as_sql()).unwrap(), state);
    }
    assert!(SyncState::from_sql("bogus").is_err());
}

#[test]
fn inv_upload_14_an_upload_behind_an_open_create_folds_to_waiting_and_stops_waiting_without_it() {
    use crate::owners::write::upload::{UploadInfo, UploadStage, UploadStatus, WaitReason};
    let queued = || UploadInfo { status: UploadStatus::Queued, stage: UploadStage::Prepare, sent_bytes: 0, total_bytes: 0, wait_reason: None, attempts: 0, error: None, local_url: None, processing: None, copy_file: None };
    let folded = |actions: &[&str]| {
        let mut info = SyncInfo { state: SyncState::Pending, actions: actions.iter().map(|action| action.to_string()).collect(), uploads: [("upload".to_string(), queued())].into(), ..SyncInfo::default() };
        info.fold_transfers(&Default::default());
        info.uploads["upload"].clone()
    };
    let behind = folded(&["create"]);
    assert_eq!((behind.status, behind.wait_reason), (UploadStatus::Waiting, Some(WaitReason::CreateUnconfirmed)));
    let alone = folded(&[]);
    assert_eq!((alone.status, alone.wait_reason), (UploadStatus::Queued, None));
}
