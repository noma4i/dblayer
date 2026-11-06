use std::collections::HashSet;

use super::status::{removes_row, Operation, OperationId};
use crate::identity::{ModelKey, RowRef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FsckAction {
    RebuildTempRow(OperationId),
    QuarantineOrphan(RowRef),
    Keep(OperationId),
}

pub fn needs_temp_rebuild(operation: &Operation, resident_rows: &HashSet<RowRef>) -> bool {
    let Some((model, _)) = Operation::split_action_key(&operation.action_key) else { return false };
    operation.temp_id.as_ref().is_some_and(|temp| !resident_rows.contains(&RowRef::new(ModelKey::new(&model), temp.clone()))) && !removes_row(operation.status)
}

pub fn orphan_temp_rows(temp_rows: &HashSet<RowRef>, open_row_ids: &HashSet<RowRef>) -> Vec<RowRef> {
    let mut orphans: Vec<RowRef> = temp_rows.difference(open_row_ids).cloned().collect();
    orphans.sort();
    orphans
}

pub fn fsck_plan(
    operations: &[Operation],
    resident_rows: &HashSet<RowRef>,
    temp_rows: &HashSet<RowRef>,
    open_row_ids: &HashSet<RowRef>,
) -> Vec<FsckAction> {
    let mut actions = Vec::new();
    for operation in operations {
        actions.push(FsckAction::Keep(operation.id));
        if needs_temp_rebuild(operation, resident_rows) {
            actions.push(FsckAction::RebuildTempRow(operation.id));
        }
    }
    actions.extend(orphan_temp_rows(temp_rows, open_row_ids).into_iter().map(FsckAction::QuarantineOrphan));
    actions
}
