use std::collections::{BTreeMap, BTreeSet, HashSet};

use rusqlite::OptionalExtension;
use serde_json::{Map, Value};

use crate::store::{BatchPlan, CommitResult, DestroyReason, EntityOp, ScopeOp, ScopeRef};
use crate::identity::{ModelKey, RowId, RowRef};
use crate::schema::{AssociationKind, PageDirection, RemoteShape};
use crate::value::{fractional_between, read_id};

use super::merge::{unconfirmed_fields, Admission, RowKind};
use super::row_write::RowWrite;
use super::{Reader, StoreError};

type EventRows = BTreeMap<(String, String), Option<Map<String, Value>>>;
pub(super) type OverlayWrites = Vec<(String, String, Map<String, Value>)>;

fn fault(error: impl std::fmt::Display) -> StoreError { StoreError::Constraint(error.to_string()) }

fn touch_parents(writer: &mut RowWrite<'_>, owner: &RowRef, rows: &[&Map<String, Value>]) -> Result<Vec<EntityOp>, StoreError> {
    let spec = &writer.models.get(owner.model.as_str()).ok_or_else(|| StoreError::UnknownModel(owner.model.to_string()))?.spec;
    let associations: Vec<_> = spec.associations.iter().filter(|association| association.kind == AssociationKind::BelongsTo && association.touches_parent).cloned().collect();
    let mut seen = HashSet::from([owner.clone()]);
    let mut parents = Vec::new();
    let mut wanted = BTreeMap::<String, Vec<String>>::new();
    for association in associations {
        for row in rows {
            let Some(id) = row.get(&association.foreign_key).and_then(read_id) else { continue };
            let parent = RowRef::new(association.target.clone(), id);
            if seen.insert(parent.clone()) {
                wanted.entry(parent.model.to_string()).or_default().push(parent.id.to_string());
                parents.push(parent);
            }
        }
    }
    for (model, ids) in wanted { writer.prefetch(&model, &ids, None)?; }
    let mut touches = Vec::new();
    for parent in parents {
        if writer.stored(parent.model.as_str(), parent.id.as_str())?.is_some() { touches.push(EntityOp::Touch { model: parent.model, id: parent.id }); }
    }
    Ok(touches)
}

fn joins(writer: &RowWrite<'_>, reader: &Reader<'_>, model: &str, id: &RowId, joins: &[Value]) -> Result<Vec<ScopeOp>, StoreError> {
    let spec = &writer.models.get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
    let mut result = Vec::new();
    for join in joins {
        let Some(relation) = join.get("relation").and_then(Value::as_str) else { continue };
        let Some(params) = join.get("params").and_then(Value::as_object) else { continue };
        let Some(declared) = spec.relations.iter().find(|candidate| candidate.name == relation) else { continue };
        let Some(remote) = declared.remote.as_ref() else { continue };
        let Ok(scope_key) = spec.selection_key(relation, params) else { continue };
        let direction = match remote.shape { RemoteShape::Connection { direction, .. } => direction, _ => PageDirection::Forward };
        let key = super::window_key(scope_key.as_str(), direction);
        let loaded = reader.connection().query_row(include_str!("sql/feed_write/joins.sql"), [&key], |row| row.get::<_, i64>(0)).optional()?;
        if loaded.is_none() || reader.scope_has_member(model, scope_key.as_str(), id.as_str())? { continue; }
        let last = reader.scope_last_key(model, scope_key.as_str())?;
        let order_key = fractional_between(last.as_deref(), None).map_err(fault)?;
        result.push(ScopeOp::Append { scope: ScopeRef::new(ModelKey::new(model), scope_key), row: id.clone(), order_key });
    }
    Ok(result)
}

fn landing(writer: &mut RowWrite<'_>, model: &str, payload: &Value, overlay: &mut OverlayWrites) -> Result<Option<CommitResult>, StoreError> {
    let op = payload.get("op").and_then(Value::as_str).ok_or_else(|| StoreError::Constraint("event has no operation".into()))?;
    if op == "reread" { return Ok(None); }
    if op == "invalid" { return Err(StoreError::Constraint(payload.get("reason").and_then(Value::as_str).unwrap_or("invalid event").to_string())); }
    let id = payload.get("id").and_then(Value::as_str).ok_or_else(|| StoreError::Constraint("event has no id".into()))?;
    let id = RowId::new(id).map_err(fault)?;
    if op == "ephemeral" {
        let values = payload.get("values").and_then(Value::as_object).cloned().unwrap_or_default();
        overlay.push((model.to_string(), id.to_string(), values));
        return Ok(None);
    }
    let connection = writer.connection;
    let models = writer.models;
    let reader = Reader::new(connection, models);
    let mut envelope = BatchPlan::from_server(writer.generation);
    if op == "delete" {
        let owner = RowRef::new(ModelKey::new(model), id.clone());
        let mut children = Vec::new();
        super::follow_cascade::descendants(writer.models, &reader, &owner, &mut HashSet::from([owner.clone()]), &mut children)?;
        envelope.entity_ops = crate::owners::write::plan_cascade::cascade_destroys(&owner, &children, true).into_iter().map(|row| EntityOp::Destroy { model: row.model, id: row.id, reason: DestroyReason::Server }).collect();
        let keys: BTreeSet<(String, String)> = envelope.entity_ops.iter().map(|op| (op.row_ref().model.to_string(), op.row_ref().id.to_string())).collect();
        for seq in crate::store::operation::row_operation_sequences(writer.connection, &keys)? {
            envelope.operation_transitions.push(crate::store::OperationTransition::Remove { seq, expected: None });
        }
    } else if op == "upsert" {
        let patch = payload.get("row").and_then(Value::as_object).ok_or_else(|| StoreError::Constraint("event has no row".into()))?;
        let key = (model.to_string(), id.to_string());
        let Some(row) = writer.admit(model, patch, RowKind::Server, Admission { base: Some(writer.revision), closing: None })? else { return Ok(None) };
        let old = writer.current.get(&key).and_then(|row| row.as_ref()).map(|row| row.row.clone());
        let owner = RowRef::new(ModelKey::new(model), id.clone());
        let rows: Vec<_> = std::iter::once(&row).chain(old.as_ref()).collect();
        envelope.entity_ops = touch_parents(writer, &owner, &rows)?;
        for related in payload.get("related").and_then(Value::as_array).into_iter().flatten() {
            let Some(related_model) = related.get("model").and_then(Value::as_str) else { continue };
            let Some(patch) = related.get("row").and_then(Value::as_object) else { continue };
            let Some(row) = writer.admit(related_model, patch, RowKind::Server, Admission { base: Some(writer.revision), closing: None })? else { continue };
            if let Some(id) = row.get("id").and_then(read_id) { envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(related_model), id, row }); }
        }
        let wanted = payload.get("joins").and_then(Value::as_array).map(Vec::as_slice).unwrap_or_default();
        envelope.scope_ops = joins(writer, &reader, model, &id, wanted)?;
        let arrives_new = old.is_none();
        let (correlated, looked_at) = if arrives_new { super::correlate::correlated_temp(&reader, model, &row)? } else { (None, 0) };
        writer.candidates += looked_at;
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id: id.clone(), row });
        if let Some(found) = correlated {
            let held = writer.stored(model, found.temp.as_str())?.map(|stored| stored.row);
            let outbox = crate::store::plan::read_outbox(&reader)?;
            super::swap::plan(&reader, &mut envelope, &outbox, super::swap::TempReplacement { model, temp: &found.temp, real: &id }, found.operation, held)?;
        }
        if let Some(values) = payload.get("set").and_then(Value::as_object).filter(|values| !values.is_empty()) { overlay.push((model.to_string(), id.to_string(), values.clone())); }
    } else {
        return Err(StoreError::Constraint(format!("unknown event operation: {op}")));
    }
    let mut seen = BTreeSet::new();
    envelope.entity_ops.reverse();
    envelope.entity_ops.retain(|op| seen.insert(op.row_ref()));
    envelope.entity_ops.reverse();
    envelope.validate(writer.generation)?;
    let hidden = writer.feed_hidden.as_ref().cloned().unwrap_or_default();
    let result = super::apply::apply_envelope_at(writer.connection, writer.models, &envelope, writer.now, writer.revision, Some(writer.originals), Some((&mut writer.current, &hidden)))?;
    if op == "delete" { writer.feed_tombstones.entry(model.to_string()).or_default().insert(id.to_string(), writer.revision); }
    if !envelope.operation_transitions.is_empty() {
        writer.feed_hidden = Some(super::apply::hidden_rows(writer.connection, &[])?);
        let reader = Reader::new(writer.connection, writer.models);
        let mut ids = BTreeMap::<String, Vec<String>>::new();
        for (model, id) in writer.current.keys() { ids.entry(model.clone()).or_default().push(id.clone()); }
        for (model, ids) in ids { writer.feed_fields.insert((model.clone(), None), unconfirmed_fields(&reader, None, &model, &ids)?); }
    }
    Ok(Some(result))
}

pub(super) fn land(writer: &mut RowWrite<'_>, events: &[(String, String)], overlay: &mut OverlayWrites, results: &mut Vec<CommitResult>) -> Result<Vec<String>, StoreError> {
    let parsed = events.iter().map(|(model, text)| Ok((model, serde_json::from_str::<Value>(text)?))).collect::<Result<Vec<_>, StoreError>>()?;
    let mut wanted = BTreeMap::<String, Vec<String>>::new();
    for (model, payload) in &parsed {
        if let Some(id) = payload.get("id").and_then(Value::as_str) { wanted.entry((*model).clone()).or_default().push(id.to_string()); }
        for related in payload.get("related").and_then(Value::as_array).into_iter().flatten() {
            if let (Some(model), Some(id)) = (related.get("model").and_then(Value::as_str), related.get("row").and_then(|row| row.get("id")).and_then(Value::as_str)) {
                wanted.entry(model.to_string()).or_default().push(id.to_string());
            }
        }
    }
    let mut before = EventRows::new();
    for (model, ids) in wanted {
        writer.prefetch(&model, &ids, None)?;
        for id in ids {
            let key = (model.clone(), id.clone());
            let row = writer.current.get(&key).and_then(|row| row.as_ref());
            before.entry(key).or_insert_with(|| row.map(|row| row.row.clone()));
            if let Ok(row_id) = RowId::new(&id) { writer.originals.entry(RowRef::new(ModelKey::new(&model), row_id)).or_insert_with(|| row.map(|row| row.row.clone())); }
        }
    }
    writer.feed_hidden = Some(super::apply::hidden_rows(writer.connection, &[])?);
    for (model, payload) in &parsed {
        if let Some(result) = landing(writer, model, payload, overlay)? { results.push(result); }
    }
    let mut observed = Vec::with_capacity(parsed.len());
    for (model, mut payload) in parsed {
        if let Some(object) = payload.as_object_mut() {
            if object.get("observe") == Some(&Value::Bool(true)) {
                if let Some(id) = object.get("id").and_then(Value::as_str).map(str::to_string) {
                    object.insert("before".into(), before.get(&(model.clone(), id.clone())).cloned().flatten().map_or(Value::Null, Value::Object));
                    object.insert("after".into(), writer.current.get(&(model.clone(), id.clone())).and_then(|row| row.as_ref()).map_or(Value::Null, |row| Value::Object(row.row.clone())));
                }
            }
        }
        observed.push(payload.to_string());
    }
    Ok(observed)
}
