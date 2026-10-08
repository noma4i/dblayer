use rusqlite::Connection;

use crate::commit::{DestroyReason, FileOp};
use crate::files::{move_row, put, remove_pair, remove_row};
use crate::identity::{ModelKey, RowId};

use super::StoreError;

pub(crate) fn apply_file_ops(connection: &Connection, operations: &[FileOp]) -> Result<bool, StoreError> {
    let mut changed = false;
    for operation in operations {
        match operation {
            FileOp::Put(record) => {
                put(connection, record)?;
                changed = true;
            }
            FileOp::Remove { model, row_id, field } => {
                changed |= remove_pair(connection, model.as_str(), row_id.as_str(), field)? > 0;
            }
        }
    }
    Ok(changed)
}

pub(crate) fn release_files(
    connection: &Connection,
    model: &ModelKey,
    id: &RowId,
    reason: &DestroyReason,
) -> Result<bool, StoreError> {
    let changed = match reason {
        DestroyReason::Replaced { by } => move_row(connection, model.as_str(), id.as_str(), by.as_str())?,
        DestroyReason::Local => 0,
        DestroyReason::Server | DestroyReason::Expired => remove_row(connection, model.as_str(), id.as_str())?,
    };
    Ok(changed > 0)
}
