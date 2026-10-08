use super::*;
use crate::identity::RowId;
use std::collections::HashSet;
use OperationStatus::*;

fn id(value: &str) -> RowId {
    RowId::new(value).unwrap()
}

fn operation(seq: i64, model: &str, status: OperationStatus, temp: Option<&str>, started: bool) -> Operation {
    let mut operation = Operation::new(&Operation::action_key(model, "create"), vec![], 0, 0);
    operation.id = OperationId(seq);
    operation.status = status;
    operation.temp_id = temp.map(id);
    operation.transport_started = started;
    operation
}

fn plain(seq: i64) -> Operation {
    operation(seq, "message", Pending, None, false)
}

fn known() -> HashSet<String> {
    ["message".to_string()].into()
}

#[test]
fn action_follows_transport_start_and_known_model() {
    let started = operation(1, "message", Pending, None, true);
    let unsent = plain(2);
    let unknown_model = operation(4, "unknown", Pending, None, true);
    assert_eq!(fsck_action(&started, &known()), FsckAction::SetStatus(OperationId(1), DeliveryUnknown));
    assert_eq!(fsck_action(&unsent, &known()), FsckAction::Keep(OperationId(2)));
    assert_eq!(fsck_action(&unknown_model, &known()), FsckAction::Keep(OperationId(4)));
}

#[test]
fn temp_rebuild_needs_missing_row_and_orphans_are_sorted_and_filtered() {
    let temp = id("temp-1");
    let pending = operation(1, "message", Pending, Some("temp-1"), false);
    let terminal = operation(3, "message", Committed, Some("temp-1"), false);
    assert!(!needs_temp_rebuild(&pending, &[temp.clone()].into()));
    assert!(needs_temp_rebuild(&pending, &HashSet::new()));
    assert!(!needs_temp_rebuild(&plain(2), &HashSet::new()));
    assert!(!needs_temp_rebuild(&terminal, &HashSet::new()));
    let orphans = orphan_temp_rows(&[id("temp-z"), id("temp-open"), id("temp-a")].into(), &[id("temp-open")].into());
    assert_eq!(orphans, [id("temp-a"), id("temp-z")]);
}

#[test]
fn plan_combines_actions_in_order() {
    let started = operation(1, "message", Pending, Some("temp-rebuild"), true);
    let actions = fsck_plan(&[started, plain(2)], &known(), &HashSet::new(), &[id("temp-orphan")].into(), &HashSet::new());
    assert_eq!(
        actions,
        [
            FsckAction::SetStatus(OperationId(1), DeliveryUnknown),
            FsckAction::RebuildTempRow(OperationId(1)),
            FsckAction::Keep(OperationId(2)),
            FsckAction::QuarantineOrphan(id("temp-orphan")),
        ]
    );
}
