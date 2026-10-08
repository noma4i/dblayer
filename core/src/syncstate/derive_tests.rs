use super::*;
use crate::commit::OperationStatus;

fn entry(action: &str, status: OperationStatus, started: bool, since: i64) -> SyncEntry {
    SyncEntry { action: action.to_string(), status, transport_started: started, attempts: 0, error: None, since, fields: Vec::new(), upload: None }
}

fn failure(text: &str) -> Option<OperationFailure> {
    Some(OperationFailure::new(FailureKind::Rejected, text))
}

#[test]
fn no_open_operations_means_no_info() {
    assert_eq!(fold_entries(&[]), None);
}

#[test]
fn pending_operation_keeps_its_action_and_creation_time() {
    let info = fold_entries(&[entry("create", OperationStatus::Pending, false, 7)]).unwrap();
    assert_eq!((info.state, info.actions, info.attempts, info.since, info.error), (SyncState::Pending, vec!["create".to_string()], 0, 7, None));
}

#[test]
fn started_pending_operation_is_sending() {
    let info = fold_entries(&[SyncEntry { attempts: 1, ..entry("update", OperationStatus::Pending, true, 9) }]).unwrap();
    assert_eq!((info.state, info.attempts, info.since), (SyncState::Sending, 1, 9));
}

#[test]
fn failed_beats_delivery_unknown_beats_sending_beats_pending() {
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
fn dirty_fields_are_the_unique_union_in_queue_order() {
    let first = SyncEntry { fields: vec!["text".into(), "likes".into()], ..entry("update", OperationStatus::Pending, false, 1) };
    let second = SyncEntry { fields: vec!["likes".into(), "media".into()], ..entry("update", OperationStatus::Pending, false, 2) };
    assert_eq!(fold_entries(&[first, second]).unwrap().dirty_fields, ["text", "likes", "media"]);
}

#[test]
fn state_names_round_trip_through_sql() {
    for state in SyncState::ALL {
        assert_eq!(SyncState::from_sql(state.as_sql()), Some(state));
    }
    assert_eq!(SyncState::from_sql("bogus"), None);
    assert_eq!(SyncInfo::default().state, SyncState::Synced);
}
