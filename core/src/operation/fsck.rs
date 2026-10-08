use std::collections::HashSet;

use super::status::{recovered_status, removes_row, Operation, OperationId, OperationStatus};
use crate::identity::RowId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FsckAction {
    SetStatus(OperationId, OperationStatus),
    RebuildTempRow(OperationId),
    QuarantineOrphan(RowId),
    Keep(OperationId),
}

pub fn fsck_action(operation: &Operation, known_models: &HashSet<String>) -> FsckAction {
    let keep = FsckAction::Keep(operation.id);
    let known = Operation::split_action_key(&operation.action_key).is_some_and(|(model, _)| known_models.contains(&model));
    if !known || operation.status != OperationStatus::Pending {
        return keep;
    }
    if !operation.transport_started {
        return keep;
    }
    FsckAction::SetStatus(operation.id, recovered_status(operation.transport_started, operation.status))
}

pub fn needs_temp_rebuild(operation: &Operation, resident_rows: &HashSet<RowId>) -> bool {
    operation.temp_id.as_ref().is_some_and(|temp| !resident_rows.contains(temp)) && !removes_row(operation.status)
}

pub fn orphan_temp_rows(temp_rows: &HashSet<RowId>, open_row_ids: &HashSet<RowId>) -> Vec<RowId> {
    let mut orphans: Vec<RowId> = temp_rows.difference(open_row_ids).cloned().collect();
    orphans.sort();
    orphans
}

pub fn fsck_plan(
    operations: &[Operation],
    known_models: &HashSet<String>,
    resident_rows: &HashSet<RowId>,
    temp_rows: &HashSet<RowId>,
    open_row_ids: &HashSet<RowId>,
) -> Vec<FsckAction> {
    let mut actions = Vec::new();
    for operation in operations {
        actions.push(fsck_action(operation, known_models));
        if needs_temp_rebuild(operation, resident_rows) {
            actions.push(FsckAction::RebuildTempRow(operation.id));
        }
    }
    actions.extend(orphan_temp_rows(temp_rows, open_row_ids).into_iter().map(FsckAction::QuarantineOrphan));
    actions
}
