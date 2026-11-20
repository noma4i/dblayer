//! Prepare settings of an older layout wait in `restore_failed` (D-32) until the platform library makes a descriptor of them. No outcome drops an element or lets a source go unprepared: an unreadable element fails the read, no `Preparer` or a refusal keeps the elements and the transfer `RestoreFailed`, `Done` writes the descriptor and removes the elements in one batch.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use rusqlite::Connection;
use serde_json::Value;

use super::restore::{read_failed, RestoreFailedRow};
use super::{Store, StoreError};
use crate::ffi::{PrepareImportOutcome, Preparer, SavedPreparation};

fn unreadable(detail: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, detail.into())
}

fn text_of(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        Value::Object(object) if object.len() == 1 => object.get("blob").and_then(Value::as_str).map(str::to_string),
        other => Some(other.to_string()),
    }
}

fn columns_of(row: &RestoreFailedRow) -> rusqlite::Result<HashMap<String, Option<String>>> {
    match serde_json::from_str::<Value>(&row.columns) {
        Ok(Value::Object(object)) => Ok(object.iter().map(|(name, value)| (name.clone(), text_of(value))).collect()),
        _ => Err(unreadable(format!("the saved prepare element {} of {} is unreadable", row.key, row.table))),
    }
}

#[derive(Default)]
struct Saved {
    elements: Vec<RestoreFailedRow>,
    settings: Option<HashMap<String, Option<String>>>,
    children: Vec<HashMap<String, Option<String>>>,
}

fn saved(connection: &Connection) -> rusqlite::Result<BTreeMap<u64, Saved>> {
    let mut found: BTreeMap<u64, Saved> = BTreeMap::new();
    for row in read_failed(connection)? {
        let Some(transfer) = row.saved_prepare_transfer() else { continue };
        let transfer = transfer.map_err(unreadable)?;
        let columns = columns_of(&row)?;
        let entry = found.entry(transfer).or_default();
        if row.table == super::restore::SAVED_PREPARE { entry.settings = Some(columns); } else { entry.children.push(columns); }
        entry.elements.push(row);
    }
    Ok(found)
}

pub(super) fn saved_ids(connection: &Connection) -> rusqlite::Result<BTreeSet<i64>> {
    saved(connection)?.into_keys().map(|id| i64::try_from(id).map_err(|_| unreadable(format!("the saved prepare transfer {id} is too large")))).collect()
}

pub(super) fn remove_saved_of(connection: &Connection, key: [&str; 3]) -> Result<(), StoreError> {
    connection.execute(include_str!("sql/prepare_rows/remove_saved_of_key.sql"), key)?;
    Ok(())
}

/// The library is called outside every lock and transaction.
pub(crate) fn import_saved_preparations(store: &Store, preparer: &dyn Preparer) -> Result<(), StoreError> {
    let found = store.read_locked(saved)?;
    for (transfer_id, Saved { elements, settings, children }) in found {
        let Some(columns) = settings else { continue };
        crate::lock::assert_no_library_lock("Preparer::import_descriptor");
        let outcome = preparer.import_descriptor(SavedPreparation { columns, children });
        let inner = store.lock();
        let transaction = inner.connection.unchecked_transaction()?;
        match outcome {
            PrepareImportOutcome::Done { descriptor } => {
                super::prepare_rows::put_prepare(&transaction, transfer_id, &descriptor)?;
                for element in &elements { transaction.execute(include_str!("sql/prepare_rows/delete_saved.sql"), [&element.table, &element.key])?; }
            }
            PrepareImportOutcome::Failed { message } => {
                for element in &elements { transaction.execute(include_str!("sql/prepare_rows/refuse_saved.sql"), rusqlite::params![format!("import_refused: {message}"), element.table, element.key])?; }
            }
        }
        transaction.commit()?;
    }
    Ok(())
}
