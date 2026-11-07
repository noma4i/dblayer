use crate::owners::write::operation_status::{Operation, OperationId, OperationStatus};
use super::*;
use crate::identity::{ModelKey, RowId, RowRef};
use std::collections::HashSet;
use OperationStatus::*;

fn id(value: &str) -> RowRef {
    RowRef::new(ModelKey::new("message"), RowId::new(value).unwrap())
}

fn operation(seq: i64, model: &str, status: OperationStatus, temp: Option<&str>) -> Operation {
    let mut operation = Operation::new(&Operation::action_key(model, "create"), vec![], 0, 0);
    operation.id = OperationId(seq);
    operation.status = status;
    operation.temp_id = temp.map(|value| RowId::new(value).unwrap());
    operation
}

fn plain(seq: i64) -> Operation {
    operation(seq, "message", Pending, None)
}

#[test]
fn temp_rebuild_needs_missing_row_and_orphans_are_sorted_and_filtered() {
    let temp = id("temp-1");
    let pending = operation(1, "message", Pending, Some("temp-1"));
    let terminal = operation(3, "message", Committed, Some("temp-1"));
    assert!(!needs_temp_rebuild(&pending, &[temp.clone()].into()));
    assert!(needs_temp_rebuild(&pending, &HashSet::new()));
    assert!(!needs_temp_rebuild(&plain(2), &HashSet::new()));
    assert!(!needs_temp_rebuild(&terminal, &HashSet::new()));
    let orphans = orphan_temp_rows(&[id("temp-z"), id("temp-open"), id("temp-a")].into(), &[id("temp-open")].into());
    assert_eq!(orphans, [id("temp-a"), id("temp-z")]);
}

#[test]
fn plan_combines_actions_in_order() {
    let started = operation(1, "message", Sending, Some("temp-rebuild"));
    let actions = fsck_plan(&[started, plain(2)], &HashSet::new(), &[id("temp-orphan")].into(), &HashSet::new());
    assert_eq!(
        actions,
        [
            FsckAction::Keep(OperationId(1)),
            FsckAction::RebuildTempRow(OperationId(1)),
            FsckAction::Keep(OperationId(2)),
            FsckAction::QuarantineOrphan(id("temp-orphan")),
        ]
    );
}
