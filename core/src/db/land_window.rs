//! The executor `Write` for `StoreOp::Window`: the rows of one page of a window, the membership of its scope and the window record, in one commit.
//! Windows decides which page lands and when; this code turns the page into SQL through the commit path of `Db`.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, EdgeValue, EdgeWrite, EntityOp, ScopeRef, WindowSave, WindowTail, WriteOrigin};
use crate::identity::{ModelKey, RowId};
use crate::lifecycle::authority::Authority;
use crate::owners::read::windows::{WindowKind, WindowKey};
use crate::owners::store::RowValue;
use crate::read::{incoming_ids, plan_landing, StoredScope, EDGE_KEY};
use crate::schema::{Coverage, FieldKind, FieldSpec, RemoteShape};
use crate::store::window_key;
use crate::value::{read_id, ScalarKind};

use super::error::DbError;
use super::local_write::plan_fault;
use super::open::Db;
use super::replace::ReplaceRequest;

/// One page of a window as `StoreOp::Window` carries it.
pub(crate) struct WindowPage<'a> {
    pub key: &'a WindowKey,
    pub page: u32,
    pub replace_all: bool,
    pub cursor: Option<&'a str>,
    pub roots: &'a [RowValue],
    pub related: &'a [(String, RowValue)],
    pub total: Option<i64>,
    pub base_revision: i64,
}

fn json_fault(error: impl std::fmt::Display) -> DbError { DbError::Internal(error.to_string()) }

fn row_values(row: &RowValue) -> Result<Map<String, Value>, DbError> {
    match serde_json::from_str(&row.values).map_err(json_fault)? {
        Value::Object(map) => Ok(map),
        _ => Err(DbError::Internal("the values of a row are not an object".into())),
    }
}

fn edge_value(kind: &FieldKind, value: &Value) -> Option<EdgeValue> {
    match kind {
        FieldKind::Scalar { kind: ScalarKind::Int } => value.as_i64().map(EdgeValue::Whole),
        FieldKind::Scalar { kind: ScalarKind::Num } => value.as_f64().map(EdgeValue::Fraction),
        FieldKind::Scalar { kind: ScalarKind::Bool } => value.as_bool().map(|flag| EdgeValue::Whole(i64::from(flag))),
        FieldKind::Scalar { .. } | FieldKind::Enum { .. } => value.as_str().map(|text| EdgeValue::Text(text.to_string())),
        _ => None,
    }
}

fn edge_writes(scope: &ScopeRef, fields: &[FieldSpec], roots: &[Map<String, Value>], edges: &BTreeMap<String, Map<String, Value>>) -> Vec<EdgeWrite> {
    if fields.is_empty() {
        return Vec::new();
    }
    roots.iter().filter_map(|root| root.get("id").and_then(read_id)).map(|row| {
        let edge = edges.get(row.as_str());
        let values = fields.iter().filter_map(|field| {
            let value = field.decode(&field.read(edge?)?)?;
            Some((field.name.clone(), edge_value(&field.kind, &value)?))
        }).collect();
        EdgeWrite { scope: scope.clone(), row, values }
    }).collect()
}

/// The scope of a relation with params and the key of its window record.
pub(super) fn window_identity(spec: &crate::schema::ModelSpec, model: &str, relation: &str, remote: &crate::schema::RemoteSpec, params: &Map<String, Value>) -> Result<(ScopeRef, String), DbError> {
    let scope_key = spec.selection_key(relation, params).map_err(json_fault)?;
    let direction = match remote.shape {
        RemoteShape::Connection { direction, .. } => direction,
        _ => crate::schema::PageDirection::Forward,
    };
    let key = window_key(scope_key.as_str(), direction);
    Ok((ScopeRef::new(ModelKey::new(model), scope_key), key))
}

impl Db {
    /// The stored record of the window of a relation, `None` before its first page.
    pub(crate) fn window_record(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<Option<crate::store::WindowRecord>, DbError> {
        let spec = self.spec(model)?;
        let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| DbError::Internal(format!("{model} has no relation {relation}")))?;
        let remote = declared.remote.as_ref().ok_or_else(|| DbError::Internal(format!("{model}.{relation} is no remote relation")))?;
        let (_, key) = window_identity(spec, model, relation, remote, params)?;
        Ok(self.shared.store.window(&key)?)
    }

    /// Whether the window landed within the stale time of its relation and nothing marked it stale since (`now_ms` is the clock of the database).
    pub(crate) fn window_is_fresh(&self, model: &str, relation: &str, params: &Map<String, Value>, now_ms: i64) -> Result<bool, DbError> {
        Ok(self.window_age(model, relation, params, now_ms)?.unwrap_or(false))
    }

    /// Whether a window that landed once outlived its stale time or was marked stale; a window that never landed has no answer.
    pub(crate) fn window_expired(&self, model: &str, relation: &str, params: &Map<String, Value>, now_ms: i64) -> Result<bool, DbError> {
        Ok(self.window_age(model, relation, params, now_ms)? == Some(false))
    }

    fn window_age(&self, model: &str, relation: &str, params: &Map<String, Value>, now_ms: i64) -> Result<Option<bool>, DbError> {
        let spec = self.spec(model)?;
        let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| DbError::Internal(format!("{model} has no relation {relation}")))?;
        let remote = declared.remote.as_ref().ok_or_else(|| DbError::Internal(format!("{model}.{relation} is no remote relation")))?;
        let (scope, key) = window_identity(spec, model, relation, remote, params)?;
        let Some(freshness) = self.shared.store.window_freshness(&key)? else { return Ok(None) };
        let is_empty = self.shared.store.read(&[model], |reader| reader.scope_last_id(model, scope.key.as_str()))?.is_none();
        let policy = crate::pacing::selected_policy(is_empty, &remote.stale_time, remote.empty_stale_time.as_deref());
        let window = crate::pacing::parse_duration(policy).ok_or_else(|| DbError::Internal(format!("unknown freshness {policy}")))?;
        Ok(Some(freshness.is_fresh(now_ms, i64::try_from(window.as_millis()).unwrap_or(i64::MAX))))
    }

    /// The edge values of the rows of a relation window, by row id.
    pub(crate) fn edge_values(&self, model: &str, relation: &str, params: &Map<String, Value>, ids: &[String]) -> Result<BTreeMap<String, Map<String, Value>>, DbError> {
        let spec = self.spec(model)?;
        let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| DbError::UnknownAssociation { model: model.to_string(), name: relation.to_string() })?;
        let scope = spec.selection_key(relation, params)?;
        let wanted: Vec<&str> = ids.iter().map(String::as_str).collect();
        let read = self.shared.store.read(&[model], |reader| reader.scope_edges(model, scope.as_str(), &wanted))?;
        self.shared.counters.lock().unwrap_or_else(std::sync::PoisonError::into_inner).edge_read_steps += read.steps;
        self.shared.counters.lock().unwrap_or_else(std::sync::PoisonError::into_inner).edge_read_scans += read.scans;
        Ok(read.members.into_iter().map(|(id, stored)| {
            let mut values: Map<String, Value> = declared.edge_fields.iter().map(|field| (field.name.clone(), Value::Null)).collect();
            for (name, value) in stored {
                if let Some(field) = declared.edge_fields.iter().find(|field| field.name == name) {
                    values.insert(name, edge_json(&field.kind, value));
                }
            }
            (id, values)
        }).collect())
    }

    /// Whether the row is in the membership of the scope of a relation with params.
    pub(crate) fn window_member(&self, model: &str, relation: &str, params: &Map<String, Value>, id: &str) -> Result<bool, DbError> {
        let scope = self.spec(model)?.selection_key(relation, params).map_err(json_fault)?;
        Ok(self.shared.store.read(&[model], |reader| reader.scope_has_member(model, scope.as_str(), id))?)
    }

    /// `StoreOp::Window`: the rows of a node or nodes key land as plain rows, the page of a relation key also plans the membership of its scope and saves the window record.
    pub(crate) fn land_window_page(&self, page: &WindowPage<'_>, min_pin_revision: Option<u64>) -> Result<(), DbError> {
        let model = page.key.model.as_str();
        let generation = self.generation();
        let mut roots = Vec::with_capacity(page.roots.len());
        let mut edges: BTreeMap<String, Map<String, Value>> = BTreeMap::new();
        for root in page.roots {
            let mut values = row_values(root)?;
            if let Some(Value::Object(edge)) = values.remove(EDGE_KEY) {
                edges.insert(root.row_id.clone(), edge);
            }
            roots.push(values);
        }
        let mut related = Vec::with_capacity(page.related.len());
        for (related_model, row) in page.related {
            related.push((ModelKey::new(related_model), row_values(row)?));
        }
        let (relation, params) = match page.key.kind() {
            WindowKind::Relation { relation, params } => (relation, params),
            WindowKind::Nodes { .. } => return self.land_loose_rows(model, &roots, &related, page.base_revision, min_pin_revision),
        };
        let spec = self.spec(model)?;
        let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| DbError::Internal(format!("{model} has no relation {relation}")))?;
        let remote = declared.remote.as_ref().ok_or_else(|| DbError::Internal(format!("{model}.{relation} is no remote relation")))?;
        let (scope, key) = window_identity(spec, model, &relation, remote, &params)?;
        let is_connection = matches!(remote.shape, RemoteShape::Connection { .. });
        let coverage = remote.coverage();
        let edge_fields = declared.edge_fields.clone();
        let keeps_total = remote.total_from.is_some();
        let is_first_page = page.page <= 1;
        let plans_as_first = is_first_page && (is_connection || coverage == Coverage::Complete);
        let store = &self.shared.store;
        let stored = store.window(&key)?;
        let issued = store.max_window_sequence().map_err(json_fault)? + 1;
        let applied = if is_first_page { issued } else { stored.as_ref().map_or(issued, |window| window.applied) };
        let last = if plans_as_first && !page.replace_all && coverage == Coverage::Page && stored.is_some() {
            store.read(&[model], |reader| reader.scope_last_id(model, scope.key.as_str()))?
        } else {
            None
        };
        let tail = stored.as_ref().zip(last).and_then(|(window, last)| {
            let listed = roots.iter().any(|root| root.get("id").and_then(read_id).is_some_and(|id| id.as_str() == last));
            (!listed).then(|| WindowTail { anchor: last, cursor: window.cursor.clone(), has_next: window.has_next })
        });
        let now = store.lock().now_ms();
        let keeps_member_order = spec.stable_order(&scope.key).is_some();
        let base_revision = page.base_revision;
        let model_key = ModelKey::new(model);
        let mut drops = 0;
        let (committed, _) = self
            .commit_following_with_pin(&Authority::Owner, generation, crate::store::TombstonePrune::WithPins(min_pin_revision), |reader| {
                drops = 0;
                let admitted = self.admit_rows(reader, base_revision, None, model, &roots, &mut drops)?.rows;
                let plan = if plans_as_first {
                    let mut request = ReplaceRequest::new(scope.clone(), coverage);
                    request.replace_all = page.replace_all;
                    self.replace_plan(reader, &request, &admitted, base_revision)?
                } else {
                    let ids: Vec<String> = incoming_ids(&admitted).iter().map(ToString::to_string).collect();
                    let wanted: Vec<&str> = ids.iter().map(String::as_str).collect();
                    let stored_scope = StoredScope {
                        last_key: reader.scope_last_key(model, scope.key.as_str())?,
                        incoming: reader.scope_members_among(model, scope.key.as_str(), if keeps_member_order { &[] } else { &wanted })?,
                    };
                    let membership = Db::scope_membership(reader, model, scope.key.as_str(), base_revision)?;
                    let (placed, left): (Vec<_>, Vec<_>) = admitted.iter().cloned().partition(|row| row.get("id").and_then(read_id).is_none_or(|id| membership.get(id.as_str()) != Some(&false)));
                    let mut plan = plan_landing(&scope, &model_key, &stored_scope, &placed).map_err(json_fault)?;
                    plan.entity_ops.extend(left.into_iter().filter_map(|row| row.get("id").and_then(read_id).map(|id| EntityOp::Upsert { model: model_key.clone(), id, row })));
                    plan
                };
                let planned: BTreeSet<(ModelKey, RowId)> = plan.entity_ops.iter().map(|op| {
                    let row = op.row_ref();
                    (row.model, row.id)
                }).collect();
                let mut envelope = CommitEnvelope::new(WriteOrigin::Snapshot, generation);
                envelope.entity_ops = self.related_upserts(reader, base_revision, &related, &planned, &mut drops)?;
                envelope.entity_ops.extend(plan.entity_ops);
                envelope.scope_ops = plan.scope_ops;
                envelope.edge_writes = edge_writes(&scope, &edge_fields, &admitted, &edges);
                envelope.window = Some(WindowSave {
                    marker: None,
                    stale_since: None,
                    key: key.clone(),
                    scope: scope.clone(),
                    cursor: page.cursor.map(str::to_string),
                    has_next: page.cursor.is_some(),
                    total: page.total.or(stored.as_ref().filter(|_| keeps_total).and_then(|window| window.total)),
                    pages: if is_first_page { 1 } else { stored.as_ref().map_or(1, |window| window.pages + 1) },
                    tail: tail.clone(),
                    issued,
                    applied,
                    cleared_at: is_first_page.then_some(now),
                });
                Ok::<_, DbError>(Some(envelope))
            })
            .map_err(plan_fault)?;
        committed.publish();
        Ok(())
    }

    /// The rows of `node` and `nodes` keys: no scope, no window record.
    fn land_loose_rows(&self, model: &str, roots: &[Map<String, Value>], related: &[(ModelKey, Map<String, Value>)], base_revision: i64, min_pin_revision: Option<u64>) -> Result<(), DbError> {
        let generation = self.generation();
        let model_key = ModelKey::new(model);
        let mut drops = 0;
        let (committed, _) = self
            .commit_following_with_pin(&Authority::Owner, generation, crate::store::TombstonePrune::WithPins(min_pin_revision), |reader| {
                drops = 0;
                let admitted = self.admit_rows(reader, base_revision, None, model, roots, &mut drops)?.rows;
                let mut envelope = CommitEnvelope::new(WriteOrigin::Snapshot, generation);
                envelope.entity_ops = self.related_upserts(reader, base_revision, related, &BTreeSet::new(), &mut drops)?;
                for row in admitted {
                    let Some(id) = row.get("id").and_then(read_id) else { continue };
                    envelope.entity_ops.push(EntityOp::Upsert { model: model_key.clone(), id, row });
                }
                Ok::<_, DbError>(Some(envelope))
            })
            .map_err(plan_fault)?;
        committed.publish();
        Ok(())
    }
}

fn edge_json(kind: &FieldKind, value: EdgeValue) -> Value {
    match (kind, value) {
        (FieldKind::Scalar { kind: ScalarKind::Bool }, EdgeValue::Whole(whole)) => Value::Bool(whole != 0),
        (_, EdgeValue::Whole(whole)) => Value::from(whole),
        (_, EdgeValue::Fraction(fraction)) => Value::from(fraction),
        (_, EdgeValue::Text(text)) => Value::String(text),
    }
}
