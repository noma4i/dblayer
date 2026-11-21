use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::store::{BatchPlan, CommitResult, EntityOp, ScopeOp, ScopeRef, WindowMarker, WindowSave};
use crate::identity::{ModelKey, RowId};
use crate::owners::read::windows::WindowKind;
use crate::owners::read::codec::{incoming_ids, plan_landing, StoredScope, EDGE_KEY};
use crate::schema::{Coverage, RemoteShape};
use crate::value::read_id;

use super::merge::{Admission, RowKind};
use super::replace::{replace_plan, scope_membership, ReplaceRequest};
use super::row_write::RowWrite;
use super::{Reader, StoreError};
use super::window_values::{edge_writes, row_values, WindowPage};

pub(super) fn fault(error: impl std::fmt::Display) -> StoreError { StoreError::Constraint(error.to_string()) }

fn admitted_rows(writer: &mut RowWrite<'_>, model: &str, rows: &[Map<String, Value>], base_revision: i64) -> Result<Vec<Map<String, Value>>, StoreError> {
    let ids: Vec<String> = rows.iter().filter_map(|row| row.get("id").and_then(read_id).map(|id| id.to_string())).collect();
    writer.prefetch(model, &ids, &[])?;
    let mut accepted = Vec::new();
    for row in rows {
        if let Some(row) = writer.admit(model, row, RowKind::Server, Admission { base: Some(base_revision), closing: &[] })? { accepted.push(row); }
    }
    Ok(accepted)
}

fn related_rows(writer: &mut RowWrite<'_>, related: &[(String, Map<String, Value>)], planned: &BTreeSet<(ModelKey, RowId)>, base_revision: i64) -> Result<Vec<EntityOp>, StoreError> {
    let mut grouped = BTreeMap::<&str, Vec<String>>::new();
    for (model, row) in related {
        if let Some(id) = row.get("id").and_then(read_id) { grouped.entry(model).or_default().push(id.to_string()); }
    }
    for (model, ids) in grouped { writer.prefetch(model, &ids, &[])?; }
    let mut ops = Vec::new();
    for (model, row) in related {
        let Some(row) = writer.admit(model, row, RowKind::Server, Admission { base: Some(base_revision), closing: &[] })? else { continue };
        let Some(id) = row.get("id").and_then(read_id) else { continue };
        let model = ModelKey::new(model);
        if !planned.contains(&(model.clone(), id.clone())) { ops.push(EntityOp::Upsert { model, id, row }); }
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
    let related: Vec<_> = crate::store::coalesce_rows(related.into_iter().map(|(model, row)| (ModelKey::new(&model), row))).into_iter().map(|(model, row)| (model.to_string(), row)).collect();
    let accepted = admitted_rows(writer, model, &roots, page.base_revision)?;
    let mut envelope = BatchPlan::new();
    envelope.request_base = Some(page.base_revision);
    let WindowKind::Relation { relation, params } = page.key.kind().map_err(StoreError::Constraint)? else {
        envelope.entity_ops = related_rows(writer, &related, &BTreeSet::new(), page.base_revision)?;
        for row in accepted {
            if let Some(id) = row.get("id").and_then(read_id) { envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id, row }); }
        }
        envelope.scope_ops = answered_scope_ops(writer, page.key, &roots)?;
        super::correlate::replace_temp_rows(writer, &mut envelope)?;
        return writer.apply(&envelope);
    };
    let spec = &writer.models.get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
    let declared = spec.relations.iter().find(|item| item.name == relation).ok_or_else(|| StoreError::Constraint(format!("{model} has no relation {relation}")))?;
    let remote = declared.remote.as_ref().ok_or_else(|| StoreError::Constraint(format!("{model}.{relation} is not remote")))?;
    envelope.source = page.source;
    let mut paid = None;
    if declared.node == Some(crate::schema::NodeRead::One) {
        envelope.landed_read = Some(relation.clone());
        if let Some(id) = params.get(crate::schema::NODE_PARAM).and_then(Value::as_str) {
            if super::reread_owed::release(writer.connection, model, id, super::reread_owed::Release::Node { relation: &relation, base: page.base_revision })? { paid = Some(id.to_string()); }
        }
    }
    let (scope, key) = super::windows::window_identity(spec, model, &relation, remote, &params).map_err(fault)?;
    let stored = super::windows::read_window(writer.connection, &key)?;
    let issued = super::windows::max_sequence(writer.connection)? + 1;
    let first = page.page <= 1;
    let plans_first = first && (matches!(remote.shape, RemoteShape::Connection { .. }) || remote.coverage() == Coverage::Complete);
    let applied = if first { issued } else { stored.as_ref().map_or(issued, |window| window.applied) };
    let reader = Reader::new(writer.connection, writer.models);
    let to_end = page.replace_all || page.cursor.is_none();
    let covers_next = !first && matches!(remote.shape, RemoteShape::Connection { .. }) && remote.coverage() == Coverage::Page && !(accepted.is_empty() && page.cursor.is_some());
    let start_after = match stored.as_ref().and_then(|window| window.anchor.clone()) {
        Some(anchor) if covers_next && reader.scope_members_among(model, scope.key.as_str(), &[anchor.as_str()])?.iter().any(|id| id == &anchor) => Some(anchor),
        _ => None,
    };
    let model_key = ModelKey::new(model);
    let local_order = match &declared.sort { crate::schema::Sort::Fields(fields) => Some(fields.clone()), _ => None };
    let covers_nothing = accepted.is_empty() && page.cursor.is_some() && !page.replace_all;
    let plan = if covers_nothing {
        crate::owners::read::codec::LandingPlan { entity_ops: Vec::new(), scope_ops: Vec::new() }
    } else if plans_first || start_after.is_some() || local_order.is_some() {
        let mut request = ReplaceRequest::new(scope.clone(), remote.coverage());
        request.replace_all = to_end;
        request.ordered_complete = first && page.cursor.is_none();
        request.first_page = first && !page.replace_all;
        request.start_after = start_after;
        request.local_order = if page.replace_all { None } else { local_order.clone() };
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
    let (pages, keep_tail) = crate::owners::read::windows::landed_tail(stored.as_ref().and_then(|window| u32::try_from(window.pages).ok()).unwrap_or_default(), page.page, page.replace_all, local_order.is_some() || declared.media.is_some());
    let cursor = if keep_tail { stored.as_ref().and_then(|window| window.cursor.clone()) } else { page.cursor.map(str::to_string) };
    envelope.window = Some(WindowSave {
        marker: None, stale_since: None, key, scope, has_next: cursor.is_some(), cursor,
        total: page.total.or(stored.as_ref().filter(|_| remote.total_from.is_some()).and_then(|window| window.total)),
        pages: i64::from(pages),
        anchor: if keep_tail { stored.as_ref().and_then(|window| window.anchor.clone()) } else { incoming_ids(&accepted).last().map(ToString::to_string).or_else(|| stored.as_ref().filter(|_| !first).and_then(|window| window.anchor.clone())) }, issued, applied, cleared_at: first.then_some(page.landed_at),
    });
    super::correlate::replace_temp_rows(writer, &mut envelope)?;
    let mut result = writer.apply(&envelope)?;
    if let Some(id) = paid { super::reread_owed::announce(&mut result, model, &id)?; }
    Ok(result)
}

/// An authoritative "no such row" answer deletes each stored row that nothing wrote after the request, with the plan of a server delete event.
pub(super) fn forget(writer: &mut RowWrite<'_>, model: &str, ids: &[String], base_revision: i64) -> Result<CommitResult, StoreError> {
    writer.prefetch(model, ids, &[])?;
    let mut envelope = BatchPlan::from_source(crate::schema::RowSource::Event);
    for id in ids {
        let Some(stored) = writer.stored(model, id)? else { continue };
        if stored.revision > base_revision { continue; }
        let (entity_ops, transitions) = super::feed_write::server_delete_plan(writer.connection, writer.models, &crate::identity::RowRef::new(ModelKey::new(model), stored.id))?;
        envelope.entity_ops.extend(entity_ops);
        envelope.operation_transitions.extend(transitions);
    }
    writer.apply(&envelope)
}

/// The marker of a media window replaces the stored one in the same transaction as the rows of its chain; the window lands now and no debt stays.
pub(super) fn mark(writer: &mut RowWrite<'_>, key: &crate::owners::read::windows::WindowKey, marker: &WindowMarker, landed_at: Option<i64>) -> Result<CommitResult, StoreError> {
    let model = key.model.as_str();
    let WindowKind::Relation { relation, params } = key.kind().map_err(StoreError::Constraint)? else { return Err(fault("a marker belongs to a relation window")) };
    let spec = &writer.models.get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
    let declared = spec.relations.iter().find(|item| item.name == relation).ok_or_else(|| fault(format!("{model} has no relation {relation}")))?;
    let remote = declared.remote.as_ref().ok_or_else(|| fault(format!("{model}.{relation} is not remote")))?;
    let (_, window_key) = super::windows::window_identity(spec, model, &relation, remote, &params).map_err(fault)?;
    let generation = writer.generation.value() as i64;
    let same = super::windows::read_marker(writer.connection, &window_key)?.is_some_and(|stored| stored == *marker) && writer.connection.query_row(include_str!("sql/window_write/mark_marker_generation.sql"), [&window_key], |row| row.get::<_, i64>(0))? == generation;
    if !same {
        writer.connection.execute(include_str!("sql/window_write/mark_delete_window_marker.sql"), [&window_key])?;
        let mut insert = writer.connection.prepare_cached(include_str!("sql/window_write/mark_insert_window_marker.sql"))?;
        for (field, value) in &marker.values {
            insert.execute(rusqlite::params![window_key, generation, marker.sequence, field, value])?;
        }
    }
    writer.connection.execute(include_str!("sql/window_write/mark_update_query_window.sql"), rusqlite::params![landed_at, window_key])?;
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
