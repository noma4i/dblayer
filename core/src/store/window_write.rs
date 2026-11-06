use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::store::{BatchPlan, CommitResult, EntityOp, ScopeOp, ScopeRef, WindowMarker, WindowSave, WindowTail};
use crate::identity::{ModelKey, RowId};
use crate::owners::read::windows::WindowKind;
use crate::read::{incoming_ids, plan_landing, StoredScope, EDGE_KEY};
use crate::schema::{Coverage, RemoteShape};
use crate::value::read_id;

use super::merge::{admitted, Admission, RowKind};
use super::replace::{replace_plan, scope_membership, ReplaceRequest};
use super::row_write::RowWrite;
use super::{Reader, StoreError};
use super::window_values::{edge_writes, row_values, WindowPage};

fn fault(error: impl std::fmt::Display) -> StoreError { StoreError::Constraint(error.to_string()) }

fn admitted_rows(writer: &RowWrite<'_>, model: &str, rows: &[Map<String, Value>], base_revision: i64) -> Result<Vec<Map<String, Value>>, StoreError> {
    let spec = &writer.models.get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
    let reader = Reader::new(writer.connection, writer.models);
    let guards = writer.guards.get(model).map(Vec::as_slice).unwrap_or_default();
    let mut accepted = Vec::new();
    for row in rows {
        if let Some(row) = admitted(&reader, RowKind::Server, Admission { base: Some(base_revision), closing: None }, model, row, spec, guards)? {
            accepted.push(row);
        }
    }
    Ok(accepted)
}

fn related_rows(writer: &RowWrite<'_>, related: &[(String, Map<String, Value>)], planned: &BTreeSet<(ModelKey, RowId)>, base_revision: i64) -> Result<Vec<EntityOp>, StoreError> {
    let mut ops = Vec::new();
    for (model, row) in related {
        for row in admitted_rows(writer, model, std::slice::from_ref(row), base_revision)? {
            let Some(id) = row.get("id").and_then(read_id) else { continue };
            let model = ModelKey::new(model);
            if !planned.contains(&(model.clone(), id.clone())) { ops.push(EntityOp::Upsert { model, id, row }); }
        }
    }
    Ok(ops)
}

pub(super) fn land(writer: &mut RowWrite<'_>, page: &WindowPage<'_>) -> Result<CommitResult, StoreError> {
    let model = page.key.model.as_str();
    let mut roots = Vec::with_capacity(page.roots.len());
    let mut edges = BTreeMap::new();
    for root in page.roots {
        let mut values = row_values(root).map_err(fault)?;
        if let Some(Value::Object(edge)) = values.remove(EDGE_KEY) { edges.insert(root.row_id.clone(), edge); }
        roots.push(values);
    }
    let related = page.related.iter().map(|(model, row)| Ok((model.clone(), row_values(row).map_err(fault)?))).collect::<Result<Vec<_>, StoreError>>()?;
    let accepted = admitted_rows(writer, model, &roots, page.base_revision)?;
    let mut envelope = BatchPlan::new(writer.generation);
    let WindowKind::Relation { relation, params } = page.key.kind() else {
        envelope.entity_ops = related_rows(writer, &related, &BTreeSet::new(), page.base_revision)?;
        for row in accepted {
            if let Some(id) = row.get("id").and_then(read_id) { envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id, row }); }
        }
        envelope.scope_ops = answered_scope_ops(writer, page.key, &roots)?;
        return super::apply::apply_envelope_at(writer.connection, writer.models, &envelope, writer.now, writer.revision, Some(writer.originals));
    };
    let spec = &writer.models.get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
    let declared = spec.relations.iter().find(|item| item.name == relation).ok_or_else(|| StoreError::Constraint(format!("{model} has no relation {relation}")))?;
    let remote = declared.remote.as_ref().ok_or_else(|| StoreError::Constraint(format!("{model}.{relation} is not remote")))?;
    let (scope, key) = super::windows::window_identity(spec, model, &relation, remote, &params).map_err(fault)?;
    let stored = super::windows::read_window(writer.connection, &key)?;
    let issued = super::windows::max_sequence(writer.connection)? + 1;
    let first = page.page <= 1;
    let plans_first = first && (matches!(remote.shape, RemoteShape::Connection { .. }) || remote.coverage() == Coverage::Complete);
    let applied = if first { issued } else { stored.as_ref().map_or(issued, |window| window.applied) };
    let reader = Reader::new(writer.connection, writer.models);
    let last = if plans_first && !page.replace_all && remote.coverage() == Coverage::Page && stored.is_some() { reader.scope_last_id(model, scope.key.as_str())? } else { None };
    let tail = stored.as_ref().zip(last).and_then(|(window, last)| {
        let listed = roots.iter().any(|root| root.get("id").and_then(read_id).is_some_and(|id| id.as_str() == last));
        (!listed).then(|| WindowTail { anchor: last, cursor: window.cursor.clone(), has_next: window.has_next })
    });
    let model_key = ModelKey::new(model);
    let plan = if plans_first {
        let mut request = ReplaceRequest::new(scope.clone(), remote.coverage());
        request.replace_all = page.replace_all;
        replace_plan(&reader, &request, &accepted, page.base_revision).map_err(fault)?
    } else {
        let ids: Vec<String> = incoming_ids(&accepted).iter().map(ToString::to_string).collect();
        let wanted: Vec<&str> = ids.iter().map(String::as_str).collect();
        let stored_scope = StoredScope {
            last_key: reader.scope_last_key(model, scope.key.as_str())?,
            incoming: reader.scope_members_among(model, scope.key.as_str(), if spec.stable_order(&scope.key).is_some() { &[] } else { &wanted })?,
        };
        let membership = scope_membership(&reader, model, scope.key.as_str(), page.base_revision).map_err(fault)?;
        let (placed, left): (Vec<_>, Vec<_>) = accepted.iter().cloned().partition(|row| row.get("id").and_then(read_id).is_none_or(|id| membership.get(id.as_str()) != Some(&false)));
        let mut plan = plan_landing(&scope, &model_key, &stored_scope, &placed).map_err(fault)?;
        plan.entity_ops.extend(left.into_iter().filter_map(|row| row.get("id").and_then(read_id).map(|id| EntityOp::Upsert { model: model_key.clone(), id, row })));
        plan
    };
    let planned = plan.entity_ops.iter().map(|op| { let row = op.row_ref(); (row.model, row.id) }).collect();
    envelope.entity_ops = related_rows(writer, &related, &planned, page.base_revision)?;
    envelope.entity_ops.extend(plan.entity_ops);
    envelope.scope_ops = plan.scope_ops;
    envelope.edge_writes = edge_writes(&scope, &declared.edge_fields, &accepted, &edges);
    envelope.window = Some(WindowSave {
        marker: None, stale_since: None, key, scope, cursor: page.cursor.map(str::to_string), has_next: page.cursor.is_some(),
        total: page.total.or(stored.as_ref().filter(|_| remote.total_from.is_some()).and_then(|window| window.total)),
        pages: if first { 1 } else { stored.as_ref().map_or(1, |window| window.pages + 1) },
        tail, issued, applied, cleared_at: first.then_some(writer.now),
    });
    envelope.validate(writer.generation)?;
    super::apply::apply_envelope_at(writer.connection, writer.models, &envelope, writer.now, writer.revision, Some(writer.originals))
}

/// The marker of a media window replaces the stored one in the same transaction as the rows of its chain; the window lands now and no debt stays.
pub(super) fn mark(writer: &mut RowWrite<'_>, key: &crate::owners::read::windows::WindowKey, marker: &WindowMarker) -> Result<CommitResult, StoreError> {
    let model = key.model.as_str();
    let WindowKind::Relation { relation, params } = key.kind() else { return Err(fault("a marker belongs to a relation window")) };
    let spec = &writer.models.get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
    let declared = spec.relations.iter().find(|item| item.name == relation).ok_or_else(|| fault(format!("{model} has no relation {relation}")))?;
    let remote = declared.remote.as_ref().ok_or_else(|| fault(format!("{model}.{relation} is not remote")))?;
    let (_, window_key) = super::windows::window_identity(spec, model, &relation, remote, &params).map_err(fault)?;
    let generation = writer.generation.value() as i64;
    writer.connection.execute("DELETE FROM window_marker WHERE window_key = ?1", [&window_key])?;
    let mut insert = writer.connection.prepare_cached("INSERT INTO window_marker (window_key, generation, sequence, field, value) VALUES (?1, ?2, ?3, ?4, ?5)")?;
    for (field, value) in &marker.values {
        insert.execute(rusqlite::params![window_key, generation, marker.sequence, field, value])?;
    }
    writer.connection.execute("UPDATE query_window SET stale_since = NULL, landed_at = ?1 WHERE key = ?2", rusqlite::params![writer.now, window_key])?;
    Ok(CommitResult::noop(uuid::Uuid::new_v4()))
}

/// The ids that the server returned as rows for the ids of a `nodes` window, in the order of the answer: what `nodes` gives back.
fn answered_scope_ops(writer: &RowWrite<'_>, key: &crate::owners::read::windows::WindowKey, roots: &[Map<String, Value>]) -> Result<Vec<ScopeOp>, StoreError> {
    let scope = ScopeRef::new(ModelKey::new(&key.model), crate::identity::ListKey::from_raw(&key.name));
    let reader = Reader::new(writer.connection, writer.models);
    let previous: BTreeSet<String> = reader.scope_members(&key.model, &key.name)?.into_iter().map(|(id, _)| id).collect();
    let mut seen = BTreeSet::new();
    let answered: Vec<RowId> = roots.iter().filter_map(|root| root.get("id").and_then(read_id)).filter(|id| seen.insert(id.as_str().to_string())).collect();
    let mut ops: Vec<ScopeOp> = previous.iter().filter(|id| !seen.contains(*id)).filter_map(|id| RowId::new(id).ok()).map(|row| ScopeOp::Detach { scope: scope.clone(), row }).collect();
    for (index, row) in answered.into_iter().enumerate() {
        let order_key = format!("{index:010}");
        ops.push(if previous.contains(row.as_str()) { ScopeOp::Move { scope: scope.clone(), row, order_key } } else { ScopeOp::Append { scope: scope.clone(), row, order_key } });
    }
    Ok(ops)
}
