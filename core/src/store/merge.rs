use std::collections::{HashMap, HashSet};

use rusqlite::params;
use serde_json::{Map, Value};

use crate::store::plan::PriorRow;
use crate::schema::{AssociationSpec, ModelSpec};
use crate::value::read_id;

use super::{admit_upsert, Reader, StoreError, UpsertAdmission};

#[derive(Clone, Copy, Default)]
pub(crate) struct Admission {
    pub base: Option<i64>,
    pub closing: Option<i64>,
}

pub(crate) fn unconfirmed_fields(reader: &Reader<'_>, closing: Option<i64>, model: &str, ids: &[String]) -> Result<HashMap<String, HashSet<String>>, StoreError> {
    if ids.is_empty() { return Ok(HashMap::new()); }
    let sql = format!(include_str!("sql/merge/unconfirmed_fields.sql"), crate::store::operation::OPEN_STATUSES);
    let ids_json = serde_json::to_string(ids)?;
    let sequences = reader.connection().prepare_cached(&sql)?.query_map(params![model, ids_json], |row| row.get::<_, i64>(0))?.collect::<Result<Vec<_>, _>>()?;
    let sequences: Vec<i64> = sequences.into_iter().filter(|seq| Some(*seq) != closing).collect();
    if sequences.is_empty() { return Ok(HashMap::new()); }
    let sequence_json = serde_json::to_string(&sequences)?;
    let values = crate::store::nodes::read_trees(reader.connection(), "op_effect_value", &["seq", "position"], "seq IN (SELECT value FROM json_each(?1))", &[rusqlite::types::Value::Text(sequence_json)])?;
    let mut fields = HashMap::<String, HashSet<String>>::new();
    for (_, value) in values {
        let prior: PriorRow = serde_json::from_value(value)?;
        crate::identity::RowId::new(prior.id()).map_err(|error| StoreError::Constraint(error.to_string()))?;
        if let PriorRow::Edited { model: owner, id, owned, .. } = prior {
            if owner == model { fields.entry(id).or_default().extend(owned.into_keys()); }
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

pub(crate) fn admitted_with_fields(reader: &Reader<'_>, kind: RowKind, admission: Admission, model: &str, patch: &Map<String, Value>, spec: &ModelSpec, guards: &[AssociationSpec], fields: Option<&HashMap<String, HashSet<String>>>, tombstones: Option<&std::collections::BTreeMap<String, i64>>, stored: Option<&std::collections::BTreeMap<String, super::rows::StoredRow>>) -> Result<Option<Map<String, Value>>, StoreError> {
    let Some(id) = patch.get("id").and_then(read_id) else { return Ok(None) };
    if kind == RowKind::Local { return Ok(Some(patch.clone())); }
    let mut decoded = decode_nested(spec, patch)?;
    for (declared, hidden) in spec.server_values() {
        if let Some(value) = decoded.get(declared).cloned() { decoded.insert(hidden.to_string(), value); }
    }
    let patch = &decoded;
    let stored = match stored { Some(rows) => rows.get(id.as_str()).cloned(), None => reader.stored_row(model, id.as_str())? };
    let versioned = spec.server_revision.as_ref().and_then(|field| patch.get(field)).and_then(Value::as_i64);
    let current = spec.server_revision.as_ref().and_then(|field| stored.as_ref()?.row.get(field)).and_then(Value::as_i64);
    if matches!((versioned, current), (Some(incoming), Some(current)) if incoming < current) {
        return Ok(None);
    }
    let tombstone = match tombstones { Some(tombstones) => tombstones.get(id.as_str()).copied(), None => reader.tombstone_seq(model, id.as_str())? };
    let mut row = match admit_upsert(admission.base, stored.as_ref(), tombstone, patch.clone()) {
        UpsertAdmission::Row { row, .. } => row,
        UpsertAdmission::DropExistence => { return Ok(stored.map(|stored| stored.row)); }
    };
    let loaded;
    let fields = match fields { Some(fields) => fields, None => { loaded = unconfirmed_fields(reader, admission.closing, model, &[id.as_str().to_string()])?; &loaded } };
    for field in fields.get(id.as_str()).into_iter().flatten() {
        match stored.as_ref().and_then(|stored| stored.row.get(field)) {
            Some(value) => row.insert(field.clone(), value.clone()),
            None => row.remove(field),
        };
    }
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
