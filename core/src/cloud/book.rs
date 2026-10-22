use std::collections::BTreeMap;

use rusqlite::{Connection, Result};
use serde_json::Value;

use super::{journal, Hlc};

#[derive(Debug, Clone, PartialEq)]
pub enum CloudApplied {
    Field { model: String, row_id: String, field: String, stamp: Hlc, value: Value },
    Tombstone { model: String, row_id: String, stamp: Hlc },
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct CloudBook {
    pub applied: Vec<CloudApplied>,
    pub cursor: Option<Vec<u8>>,
    pub observed: Option<Hlc>,
}

pub(crate) struct LocalEdit {
    pub model: String,
    pub row_id: String,
    pub fields: BTreeMap<String, Value>,
}

pub(crate) fn write_book(connection: &Connection, book: &CloudBook) -> Result<()> {
    for item in &book.applied {
        match item {
            CloudApplied::Field { model, row_id, field, stamp, value } => journal::write_field(connection, model, row_id, field, stamp, value)?,
            CloudApplied::Tombstone { model, row_id, stamp } => journal::write_tombstone(connection, model, row_id, stamp)?,
        }
    }
    if let Some(seen) = &book.observed {
        journal::observe(connection, seen)?;
    }
    if let Some(token) = &book.cursor {
        journal::write_cursor(connection, token)?;
    }
    Ok(())
}

pub(crate) fn record_local(connection: &Connection, edits: &[LocalEdit], deletes: &[(String, String)], now_ms: i64) -> Result<bool> {
    let mut logged = false;
    for edit in edits {
        for (field, value) in &edit.fields {
            let stamp = journal::next_stamp(connection, now_ms)?;
            record_stamped(connection, &edit.model, &edit.row_id, field, &stamp, value)?;
            logged = true;
        }
    }
    for (model, row_id) in deletes {
        let stamp = journal::next_stamp(connection, now_ms)?;
        journal::write_tombstone(connection, model, row_id, &stamp)?;
        journal::log_delete(connection, model, row_id, &stamp)?;
        logged = true;
    }
    Ok(logged)
}

pub(crate) fn record_stamped(connection: &Connection, model: &str, row_id: &str, field: &str, stamp: &Hlc, value: &Value) -> Result<()> {
    journal::write_field(connection, model, row_id, field, stamp, value)?;
    journal::log_field(connection, model, row_id, field, stamp, value)
}
