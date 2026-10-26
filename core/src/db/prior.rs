use std::collections::{BTreeMap, BTreeSet, HashSet};

use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::commit::{DestroyReason, EntityOp, ScopeOp, ScopeRef};
use crate::files::references_of_row;
use crate::identity::{ModelKey, RowId, ListKey};
use crate::store::{PendingMembers, PendingPriors, Reader, StoreError};
use crate::value::read_id;
use crate::write::restore_patch;

use super::local_write::OutboxOperation;
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
    Member { model: String, id: String, scope_key: String, before: Option<String> },
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
            PriorRow::Created { id, .. } | PriorRow::Edited { id, .. } | PriorRow::Destroyed { id, .. } | PriorRow::Member { id, .. } => id,
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
    pub(crate) fn related_operations(reader: &Reader<'_>, entry: &OutboxOperation) -> Result<Vec<OutboxOperation>, StoreError> {
        let keys: BTreeSet<_> = Self::of(entry).into_iter().flat_map(|prior| prior.rows).map(|row| match row {
            PriorRow::Created { model, id } | PriorRow::Edited { model, id, .. } | PriorRow::Destroyed { model, id, .. } | PriorRow::Member { model, id, .. } => (model, id),
        }).collect();
        let mut sequences = crate::operation::row_operation_sequences(reader.connection(), &keys)?;
        let mut statement = reader.connection().prepare_cached("SELECT ids.seq FROM op_value AS ids JOIN op_value AS models ON models.seq = ids.seq AND models.item_position = -1 AND models.part = 'prior' AND models.parent = ids.parent AND models.name = 'model' AND models.text = ?1 WHERE ids.item_position = -1 AND ids.part = 'prior' AND ids.name = 'id' AND ids.text = ?2")?;
        for (model, id) in keys {
            sequences.extend(statement.query_map(params![model, id], |row| row.get::<_, i64>(0))?.collect::<Result<Vec<_>, _>>()?);
        }
        sequences.into_iter().filter(|seq| *seq != entry.operation.id.0).filter_map(|seq| OutboxOperation::read(reader, seq).transpose()).collect()
    }

    pub fn of(entry: &OutboxOperation) -> Option<Prior> {
        serde_json::from_value(entry.rollback.clone()?).ok()
    }

    fn pending<'a>(reader: &'a Reader<'_>) -> Result<&'a PendingPriors, StoreError> {
        if reader.pending.get().is_none() {
            let mut pending = PendingPriors::default();
            let mut statement = reader.connection().prepare_cached(&format!("SELECT seq FROM op WHERE status IN {}", crate::operation::OPEN_STATUSES))?;
            let sequences = statement.query_map([], |row| row.get::<_, i64>(0))?.collect::<Result<Vec<_>, _>>()?;
            for seq in sequences {
                let prior = crate::operation::read_value(reader.connection(), seq, "prior")?.and_then(|value| serde_json::from_value::<Prior>(value).ok());
                for row in prior.into_iter().flat_map(|prior| prior.rows) {
                    match row {
                        PriorRow::Edited { model, id, owned: fields, .. } => {
                            let owner = pending.fields.entry((model, id)).or_default();
                            for field in fields.into_keys() { owner.entry(field).or_default().insert(seq); }
                        }
                        PriorRow::Member { model, id, scope_key, before } => {
                            let members = pending.members.entry((model, scope_key)).or_default();
                            if before.is_some() { members.left.insert(id); } else { members.joined.insert(id); }
                        }
                        _ => {}
                    }
                }
            }
            let _ = reader.pending.set(pending);
        }
        reader.pending.get().ok_or_else(|| StoreError::Constraint("pending priors are not loaded".into()))
    }

    pub fn unconfirmed_fields(reader: &Reader<'_>, closing: Option<i64>, model: &str, id: &str) -> Result<HashSet<String>, StoreError> {
        Ok(Self::pending(reader)?.fields.get(&(model.to_string(), id.to_string())).into_iter().flatten()
            .filter(|(_, owners)| owners.iter().any(|seq| Some(*seq) != closing)).map(|(field, _)| field.clone()).collect())
    }

    pub fn pending_members<'a>(reader: &'a Reader<'_>, model: &str, scope_key: &str) -> Result<Option<&'a PendingMembers>, StoreError> {
        Ok(Self::pending(reader)?.members.get(&(model.to_string(), scope_key.to_string())))
    }

    pub fn created_rows(reader: &Reader<'_>, seq: i64) -> Result<Vec<(String, String)>, StoreError> {
        let prior = crate::operation::read_value(reader.connection(), seq, "prior")?.and_then(|value| serde_json::from_value::<Prior>(value).ok());
        Ok(prior.into_iter().flat_map(|prior| prior.rows).filter_map(|row| match row { PriorRow::Created { model, id } => Some((model, id)), _ => None }).collect())
    }

    pub(crate) fn with_edits(rollback: Value, reader: &Reader<'_>, edits: &[PointerEdit]) -> Result<Value, StoreError> {
        let Ok(mut prior) = serde_json::from_value::<Prior>(rollback.clone()) else { return Ok(rollback) };
        for edit in edits {
            let model = edit.parent.model.as_str();
            let id = edit.parent.id.as_str();
            match prior.rows.iter_mut().find(|row| matches!(row, PriorRow::Edited { model: owner, id: row_id, .. } if owner == model && row_id == id)) {
                Some(PriorRow::Edited { before, owned, .. }) => {
                    let stored = reader.stored_row(model, id)?;
                    let revision = reader.revision_seq()? + 1;
                    for field in edit.after.keys().filter(|field| edit.before.get(*field) != edit.after.get(*field)) {
                        if !owned.contains_key(field) || stored.as_ref().and_then(|row| row.field_rev.get(field)) != owned.get(field) {
                            before.insert(field.clone(), edit.before.get(field).cloned().unwrap_or(Value::Null));
                        }
                        owned.insert(field.clone(), revision);
                    }
                }
                _ => prior.rows.push(PriorRow::edited(reader, model, id, &edit.before, &edit.after)?),
            }
        }
        Ok(prior.to_value())
    }

    pub fn created_beyond(&self, owned: &[RowId]) -> Vec<(String, RowId)> {
        self.rows.iter().filter_map(|row| match row {
            PriorRow::Created { model, id } => RowId::new(id).ok().filter(|id| !owned.contains(id)).map(|id| (model.clone(), id)),
            _ => None,
        }).collect()
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    pub fn reappearing(&self) -> (Vec<EntityOp>, Vec<ScopeOp>) {
        let (mut entity_ops, mut scope_ops) = (Vec::new(), Vec::new());
        for row in &self.rows {
            let (entity, scopes) = row.reappearing();
            entity_ops.extend(entity);
            scope_ops.extend(scopes);
        }
        (entity_ops, scope_ops)
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
                PriorRow::Created { model, id } | PriorRow::Member { model, id, .. } => (model, id, None),
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
