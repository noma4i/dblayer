use std::collections::HashSet;

use rusqlite::params;
use serde_json::{Map, Value};

use crate::store::plan::{Prior, PriorRow};
use crate::schema::{AssociationSpec, ModelSpec};
use crate::value::read_id;

use super::{admit_upsert, Reader, StoreError, UpsertAdmission};

#[derive(Clone, Copy, Default)]
pub(crate) struct Admission {
    pub base: Option<i64>,
    pub closing: Option<i64>,
}

pub(crate) fn unconfirmed_fields(reader: &Reader<'_>, closing: Option<i64>, model: &str, id: &str) -> Result<HashSet<String>, StoreError> {
    let sql = format!("SELECT DISTINCT e.seq FROM op_effect e JOIN op ON op.seq = e.seq WHERE e.model = ?1 AND e.row_id = ?2 AND e.kind = 'edit' AND op.status IN {}", crate::operation::OPEN_STATUSES);
    let mut statement = reader.connection().prepare_cached(&sql)?;
    let sequences = statement.query_map(params![model, id], |row| row.get::<_, i64>(0))?.collect::<Result<Vec<_>, _>>()?;
    let mut fields = HashSet::new();
    for seq in sequences.into_iter().filter(|seq| Some(*seq) != closing) {
        let prior = crate::store::plan::op_effect::read_prior(reader.connection(), seq)?.map(Prior::decode).transpose()?;
        for row in prior.into_iter().flat_map(|prior| prior.rows) {
            if let PriorRow::Edited { model: owner, id: row_id, owned, .. } = row {
                if owner == model && row_id == id { fields.extend(owned.into_keys()); }
            }
        }
    }
    Ok(fields)
}

/// A server row with its nested members decoded by their declaration: a member that does not decode refuses the row and with it the batch.
fn decode_nested(spec: &ModelSpec, patch: &Map<String, Value>) -> Result<Map<String, Value>, StoreError> {
    let mut row = patch.clone();
    for field in spec.fields.iter().filter(|field| field.is_nested()) {
        if let Some(value) = patch.get(&field.name) {
            row.insert(field.name.clone(), field.decode_nested(Some(value)).map_err(StoreError::Invalid)?);
        }
    }
    Ok(row)
}

/// A row written by the device (`Local`) or one the server sent: a server row is checked against the stored revision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RowKind {
    Local,
    Server,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn admitted(reader: &Reader<'_>, kind: RowKind, admission: Admission, model: &str, patch: &Map<String, Value>, drops: &mut usize, spec: &ModelSpec, guards: &[AssociationSpec]) -> Result<Option<Map<String, Value>>, StoreError> {
    let Some(id) = patch.get("id").and_then(read_id) else { return Ok(None) };
    if kind == RowKind::Local { return Ok(Some(patch.clone())); }
    let decoded = decode_nested(spec, patch)?;
    let patch = &decoded;
    let stored = reader.stored_row(model, id.as_str())?;
    let versioned = spec.server_revision.as_ref().and_then(|field| patch.get(field)).and_then(Value::as_i64);
    let current = spec.server_revision.as_ref().and_then(|field| stored.as_ref()?.row.get(field)).and_then(Value::as_i64);
    if matches!((versioned, current), (Some(incoming), Some(current)) if incoming < current) {
        *drops += 1;
        return Ok(None);
    }
    let tombstone = reader.tombstone_seq(model, id.as_str())?;
    let (mut row, mut dropped) = match admit_upsert(admission.base, stored.as_ref(), tombstone, patch.clone()) {
        UpsertAdmission::Row { row, dropped_fields } => (row, !dropped_fields.is_empty()),
        UpsertAdmission::DropExistence => { *drops += 1; return Ok(stored.map(|stored| stored.row)); }
    };
    for field in unconfirmed_fields(reader, admission.closing, model, id.as_str())? {
        dropped |= row.contains_key(&field);
        match stored.as_ref().and_then(|stored| stored.row.get(&field)) {
            Some(value) => row.insert(field.clone(), value.clone()),
            None => row.remove(&field),
        };
    }
    *drops += usize::from(dropped);
    let Some(mut row) = spec.merged(stored.as_ref().map(|stored| &stored.row), &row) else { return Ok(None) };
    let Some(stored) = stored else { return Ok(Some(row)) };
    for association in guards {
        let moves = association.sets.keys().any(|field| row.get(field) != stored.row.get(field));
        if !moves || association.parent_is_newer(&stored.row, &row) { continue; }
        for field in association.sets.keys() {
            match stored.row.get(field) {
                Some(value) => row.insert(field.clone(), value.clone()),
                None => row.remove(field),
            };
        }
    }
    Ok(Some(row))
}
