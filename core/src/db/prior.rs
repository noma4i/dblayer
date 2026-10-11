use std::collections::{BTreeMap, HashMap, HashSet};

use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::commit::{DestroyReason, EntityOp, ScopeOp, ScopeRef};
use crate::files::references_of_row;
use crate::identity::{ModelKey, RowId, ScopeKey};
use crate::store::{Reader, StoreError};
use crate::value::read_id;
use crate::write::restore_patch;

use super::local_write::{read_outbox, OutboxOperation};
use super::parent_pointer::PointerEdit;

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

pub(crate) struct HeldFile {
    pub model: String,
    pub row_id: String,
    pub field: String,
    pub id: String,
}

impl PriorRow {
    pub fn id(&self) -> &str {
        match self {
            PriorRow::Created { id, .. } | PriorRow::Edited { id, .. } | PriorRow::Destroyed { id, .. } => id,
        }
    }

    pub fn reappearing(&self, reader: &Reader<'_>, taken: &mut HashSet<(String, String, String)>) -> Result<(Option<EntityOp>, Vec<ScopeOp>), StoreError> {
        let PriorRow::Destroyed { model, id, before, members, .. } = self else { return Ok((None, Vec::new())) };
        let (Ok(row_id), key) = (RowId::new(id), ModelKey::new(model)) else { return Ok((None, Vec::new())) };
        let mut scopes = Vec::new();
        for member in members {
            let claimed: HashSet<String> = taken.iter().filter(|(owner, scope, _)| owner == model && scope == &member.scope_key).map(|(_, _, key)| key.clone()).collect();
            let order_key = reader.free_order_key(model, &member.scope_key, id, &member.order_key, &claimed)?;
            taken.insert((model.clone(), member.scope_key.clone(), order_key.clone()));
            scopes.push(ScopeOp::Append { scope: ScopeRef::new(key.clone(), ScopeKey::from_raw(&member.scope_key)), row: row_id.clone(), order_key });
        }
        Ok((Some(EntityOp::Upsert { model: key, id: row_id, row: before.clone() }), scopes))
    }

    pub fn destroyed(reader: &Reader<'_>, model: &str, id: &str) -> Result<Option<PriorRow>, StoreError> {
        let Some(before) = reader.point(model, id)? else { return Ok(None) };
        let mut statement = reader.connection().prepare_cached("SELECT scope_key, order_key FROM scope_member WHERE model = ?1 AND row_id = ?2")?;
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
    pub fn of(entry: &OutboxOperation) -> Option<Prior> {
        serde_json::from_value(entry.rollback.clone()?).ok()
    }

    pub fn unconfirmed_fields(reader: &Reader<'_>, closing: Option<i64>) -> Result<HashMap<(String, String), HashSet<String>>, StoreError> {
        let mut owned: HashMap<(String, String), HashSet<String>> = HashMap::new();
        for entry in read_outbox(reader)?.iter().filter(|entry| Some(entry.operation.id.0) != closing) {
            for row in Prior::of(entry).into_iter().flat_map(|prior| prior.rows) {
                if let PriorRow::Edited { model, id, owned: fields, .. } = row {
                    owned.entry((model, id)).or_default().extend(fields.into_keys());
                }
            }
        }
        Ok(owned)
    }

    pub(crate) fn with_edits(rollback: Value, reader: &Reader<'_>, edits: &[PointerEdit]) -> Result<Value, StoreError> {
        let Ok(mut prior) = serde_json::from_value::<Prior>(rollback.clone()) else { return Ok(rollback) };
        for edit in edits {
            prior.rows.push(PriorRow::edited(reader, edit.parent.model.as_str(), edit.parent.id.as_str(), &edit.before, &edit.after)?);
        }
        Ok(prior.to_value())
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    pub fn reappearing(&self, reader: &Reader<'_>) -> Result<(Vec<EntityOp>, Vec<ScopeOp>), StoreError> {
        let (mut entity_ops, mut scope_ops, mut taken) = (Vec::new(), Vec::new(), HashSet::new());
        for row in &self.rows {
            let (entity, scopes) = row.reappearing(reader, &mut taken)?;
            entity_ops.extend(entity);
            scope_ops.extend(scopes);
        }
        Ok((entity_ops, scope_ops))
    }

    pub fn hiding(&self) -> Vec<EntityOp> {
        self.rows
            .iter()
            .filter_map(|row| match row {
                PriorRow::Destroyed { model, id, .. } => Some(EntityOp::Destroy { model: ModelKey::new(model), id: RowId::new(id).ok()?, reason: DestroyReason::Local }),
                _ => None,
            })
            .collect()
    }

    pub fn held_files(&self) -> Vec<HeldFile> {
        let mut held = Vec::new();
        for row in &self.rows {
            if let PriorRow::Destroyed { model, id, files, .. } = row {
                held.extend(files.iter().map(|file| HeldFile { model: model.clone(), row_id: id.clone(), field: file.field.clone(), id: file.id.clone() }));
            }
        }
        held
    }

    pub fn swap(&mut self, temp: &RowId, real: &RowId, parent: &str, references: &[(String, String)]) -> bool {
        let mut changed = false;
        for row in &mut self.rows {
            let (model, id, before) = match row {
                PriorRow::Created { model, id } => (model, id, None),
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
    let matches = read_id(value).as_ref() == Some(temp);
    if matches {
        *value = Value::String(real.to_string());
    }
    matches
}
