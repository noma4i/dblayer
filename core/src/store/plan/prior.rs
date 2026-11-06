use std::collections::{BTreeMap, BTreeSet};

use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::store::{EntityOp, ScopeOp, ScopeRef};
use crate::store::files::references_of_row;
use crate::identity::{ModelKey, RowId, ListKey};
use crate::store::{Reader, StoreError};
use crate::value::read_id;
use crate::owners::write::plan_intent::restore_patch;

use super::outbox_entry::OutboxOperation;

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub(crate) struct Prior {
    pub rows: Vec<PriorRow>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum PriorRow {
    Created { model: String, id: String },
    Edited { model: String, id: String, before: Map<String, Value>, owned: BTreeMap<String, i64> },
    Destroyed { model: String, id: String, before: Map<String, Value>, members: Vec<PriorMember>, files: Vec<PriorFile> },
    Join { model: String, id: String, scope_key: String, before: Option<String>, owned: i64 },
    Leave { model: String, id: String, scope_key: String, before: Option<String>, owned: i64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PriorMember {
    pub scope_key: String,
    pub order_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PriorFile {
    pub field: String,
    pub id: String,
}


impl PriorRow {
    pub fn id(&self) -> &str {
        match self {
            PriorRow::Created { id, .. } | PriorRow::Edited { id, .. } | PriorRow::Destroyed { id, .. } | PriorRow::Join { id, .. } | PriorRow::Leave { id, .. } => id,
        }
    }


    pub fn reappearing(&self) -> (Option<EntityOp>, Vec<ScopeOp>) {
        let PriorRow::Destroyed { model, id, before, members, .. } = self else { return (None, Vec::new()) };
        let (Ok(row_id), key) = (RowId::new(id), ModelKey::new(model)) else { return (None, Vec::new()) };
        let mut scopes = Vec::new();
        for member in members {
            scopes.push(ScopeOp::Append { scope: ScopeRef::new(key.clone(), ListKey::from_raw(&member.scope_key)), row: row_id.clone(), order_key: member.order_key.clone() });
        }
        (Some(EntityOp::Upsert { model: key, id: row_id, row: before.clone() }), scopes)
    }


    pub fn destroyed(reader: &Reader<'_>, model: &str, id: &str) -> Result<Option<PriorRow>, StoreError> {
        let Some(before) = reader.point(model, id)? else { return Ok(None) };
        let mut statement = reader.connection().prepare_cached(include_str!("../sql/plan/destroyed.sql"))?;
        let members = statement
            .query_map(params![model, id], |row| Ok(PriorMember { scope_key: row.get(0)?, order_key: row.get(1)? }))?
            .collect::<Result<Vec<_>, _>>()?;
        let files = references_of_row(reader.connection(), model, id)?.into_iter().map(|file| PriorFile { field: file.field, id: file.id }).collect();
        Ok(Some(PriorRow::Destroyed { model: model.to_string(), id: id.to_string(), before, members, files }))
    }

    pub fn edited(reader: &Reader<'_>, model: &str, id: &str, current: &Map<String, Value>, next: &Map<String, Value>) -> Result<PriorRow, StoreError> {
        let changed: Vec<String> = next.keys().filter(|field| current.get(*field) != next.get(*field)).cloned().collect();
        let revision = reader.revision_seq()? + 1;
        let owned = changed.iter().map(|field| (field.clone(), revision)).collect();
        Ok(PriorRow::Edited { model: model.to_string(), id: id.to_string(), before: restore_patch(&changed, current), owned })
    }
}

impl Prior {
    pub(crate) fn decode(value: Value) -> Result<Self, StoreError> {
        let prior: Self = serde_json::from_value(value)?;
        for row in &prior.rows {
            RowId::new(row.id()).map_err(|error| StoreError::Constraint(error.to_string()))?;
        }
        Ok(prior)
    }

    pub(crate) fn related_operations(reader: &Reader<'_>, entry: &OutboxOperation) -> Result<Vec<OutboxOperation>, StoreError> {
        let keys: BTreeSet<_> = Self::of(entry)?.into_iter().flat_map(|prior| prior.rows).map(|row| match row {
            PriorRow::Created { model, id } | PriorRow::Edited { model, id, .. } | PriorRow::Destroyed { model, id, .. } | PriorRow::Join { model, id, .. } | PriorRow::Leave { model, id, .. } => (model, id),
        }).collect();
        let mut sequences = crate::store::operation::row_operation_sequences(reader.connection(), &keys)?;
        let mut statement = reader.connection().prepare_cached(include_str!("../sql/plan/related_operations.sql"))?;
        for (model, id) in keys {
            sequences.extend(statement.query_map(params![model, id], |row| row.get::<_, i64>(0))?.collect::<Result<Vec<_>, _>>()?);
        }
        sequences.into_iter().filter(|seq| *seq != entry.operation.id.0).filter_map(|seq| OutboxOperation::read(reader, seq).transpose()).collect()
    }

    pub fn of(entry: &OutboxOperation) -> Result<Option<Prior>, StoreError> {
        entry.rollback.clone().map(Self::decode).transpose()
    }

    pub fn created_rows(reader: &Reader<'_>, seq: i64) -> Result<Vec<(String, String)>, StoreError> {
        let prior = super::op_effect::read_prior(reader.connection(), seq)?.map(Self::decode).transpose()?;
        Ok(prior.into_iter().flat_map(|prior| prior.rows).filter_map(|row| match row { PriorRow::Created { model, id } => Some((model, id)), _ => None }).collect())
    }



    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    pub fn swap(&mut self, temp: &RowId, real: &RowId, parent: &str, references: &[(String, String)]) -> bool {
        let mut changed = false;
        for row in &mut self.rows {
            let (model, id, before) = match row {
                PriorRow::Created { model, id } | PriorRow::Join { model, id, .. } | PriorRow::Leave { model, id, .. } => (model, id, None),
                PriorRow::Edited { model, id, before, .. } | PriorRow::Destroyed { model, id, before, .. } => (model, id, Some(before)),
            };
            if model == parent && id == temp.as_str() {
                *id = real.to_string();
                changed = true;
            }
            let Some(before) = before else { continue };
            let own_id = (model == parent).then_some("id");
            let fields = references.iter().filter(|(owner, _)| owner == model.as_str()).map(|(_, field)| field.as_str());
            for field in own_id.into_iter().chain(fields) {
                changed |= before.get_mut(field).is_some_and(|value| swap_id(value, temp, real));
            }
        }
        changed
    }
}

pub(crate) fn swap_id(value: &mut Value, temp: &RowId, real: &RowId) -> bool {
    if let Value::Array(items) = value {
        let mut changed = false;
        for item in items.iter_mut() {
            changed |= swap_id(item, temp, real);
        }
        return changed;
    }
    let matches = read_id(value).as_ref() == Some(temp);
    if matches {
        *value = Value::String(real.to_string());
    }
    matches
}
