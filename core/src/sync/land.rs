use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, EdgeValue, EdgeWrite, EntityOp, EnvelopeFault, ScopeRef, WindowSave, WindowTail, WriteOrigin};
use crate::identity::{ModelKey, RowId};
use crate::db::{Db, Prior, ReplaceRequest};
use crate::schema::{Coverage, FieldKind, FieldSpec};
use crate::value::ScalarKind;
use crate::read::{incoming_ids, plan_landing, StoredMember, StoredScope};
use crate::store::{admit_upsert, PageInfo, PlanCommitError, Reader, StoreError, UpsertAdmission};
use crate::transport::FetchAttempt;
use crate::value::read_id;

use super::error::SyncError;
use super::load::DiscardReason;
use super::normalize::Normalized;
use super::{locked, Inner};

pub(crate) struct Landing {
    pub model: String,
    pub scope: ScopeRef,
    pub window_key: String,
    pub normalized: Normalized,
    pub is_first_page: bool,
    pub plans_as_first: bool,
    pub coverage: Coverage,
    pub edge_fields: Vec<FieldSpec>,
    pub requested_cursor: Option<String>,
    pub page_info: Option<PageInfo>,
    pub keeps_total: bool,
    pub attempt: FetchAttempt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Landed {
    Applied { rows: usize, invalid: usize },
    Discarded(DiscardReason),
}

pub(crate) struct Admitted {
    pub rows: Vec<Map<String, Value>>,
    pub invalid: usize,
}

pub(crate) fn admit(db: &Db, reader: &Reader<'_>, base: i64, closing: Option<i64>, model: &str, rows: &[Map<String, Value>], drops: &mut usize) -> Result<Admitted, SyncError> {
    db.spec(model).map_err(|_| SyncError::UnknownModel(model.to_string()))?;
    let unconfirmed = Prior::unconfirmed_fields(reader, closing)?;
    let mut admitted = Admitted { rows: Vec::with_capacity(rows.len()), invalid: 0 };
    for row in rows {
        let Some(id) = row.get("id").and_then(read_id) else { continue };
        let stored = reader.stored_row(model, id.as_str())?;
        let tombstone = reader.tombstone_seq(model, id.as_str())?;
        match admit_upsert(Some(base), stored.as_ref(), tombstone, row.clone()) {
            UpsertAdmission::Row { mut row, dropped_fields } => {
                let mut dropped = !dropped_fields.is_empty();
                for field in unconfirmed.get(&(model.to_string(), id.to_string())).into_iter().flatten() {
                    dropped |= row.contains_key(field);
                    match stored.as_ref().and_then(|stored| stored.row.get(field)) {
                        Some(value) => row.insert(field.clone(), value.clone()),
                        None => row.remove(field),
                    };
                }
                *drops += usize::from(dropped);
                match db.merged(model, stored.as_ref().map(|stored| &stored.row), &row)? {
                    Some(merged) => admitted.rows.push(merged),
                    None => admitted.invalid += 1,
                }
            }
            UpsertAdmission::DropExistence => {
                *drops += 1;
                admitted.rows.extend(stored.map(|stored| stored.row));
            }
        }
    }
    Ok(admitted)
}

fn edge_writes(scope: &ScopeRef, fields: &[FieldSpec], roots: &[Map<String, Value>], edges: &std::collections::BTreeMap<String, Map<String, Value>>) -> Vec<EdgeWrite> {
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

fn edge_value(kind: &FieldKind, value: &Value) -> Option<EdgeValue> {
    match kind {
        FieldKind::Scalar { kind: ScalarKind::Int } => value.as_i64().map(EdgeValue::Whole),
        FieldKind::Scalar { kind: ScalarKind::Num } => value.as_f64().map(EdgeValue::Fraction),
        FieldKind::Scalar { kind: ScalarKind::Bool } => value.as_bool().map(|flag| EdgeValue::Whole(i64::from(flag))),
        FieldKind::Scalar { .. } | FieldKind::Enum { .. } => value.as_str().map(|text| EdgeValue::Text(text.to_string())),
        _ => None,
    }
}

impl Inner {
    fn related_upserts(&self, reader: &Reader<'_>, base_revision: i64, related: &[(ModelKey, Map<String, Value>)], planned: &BTreeSet<(ModelKey, RowId)>, drops: &mut usize) -> Result<Vec<EntityOp>, SyncError> {
        let mut ops = Vec::new();
        for (related_model, row) in related {
            for row in admit(&self.db, reader, base_revision, None, related_model.as_str(), std::slice::from_ref(row), drops)?.rows {
                let Some(id) = row.get("id").and_then(read_id) else { continue };
                if !planned.contains(&(related_model.clone(), id.clone())) {
                    ops.push(EntityOp::Upsert { model: related_model.clone(), id, row });
                }
            }
        }
        Ok(ops)
    }

    pub(crate) fn land_rows(&self, model: &str, normalized: &Normalized, attempt: &FetchAttempt) -> Result<Landed, SyncError> {
        let _serial = locked(&self.landing);
        let mut drops = 0;
        let (mut admitted_roots, mut invalid_roots) = (0, 0);
        let committed = self.db.commit_following(attempt.generation, |reader| {
            let Admitted { rows: roots, invalid } = admit(&self.db, reader, attempt.base_revision, None, model, &normalized.roots, &mut drops)?;
            admitted_roots = roots.len();
            invalid_roots = invalid;
            let mut envelope = CommitEnvelope::new(WriteOrigin::Snapshot, attempt.generation);
            envelope.entity_ops = self.related_upserts(reader, attempt.base_revision, &normalized.related, &BTreeSet::new(), &mut drops)?;
            for row in roots {
                let Some(id) = row.get("id").and_then(read_id) else { continue };
                envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id, row });
            }
            Ok::<_, SyncError>(Some(envelope))
        });
        match committed {
            Ok(_) => {}
            Err(PlanCommitError::Plan(error)) => return Err(error),
            Err(PlanCommitError::Store(StoreError::Envelope(EnvelopeFault::StaleGeneration))) => return Ok(Landed::Discarded(DiscardReason::StaleGeneration)),
            Err(PlanCommitError::Store(error)) => return Err(error.into()),
        }
        locked(&self.counters).record_stale_version_drops(drops);
        Ok(Landed::Applied { rows: admitted_roots, invalid: invalid_roots })
    }

    pub(crate) fn land(&self, landing: Landing) -> Result<Landed, SyncError> {
        let _serial = locked(&self.landing);
        let Landing { model, scope, window_key, normalized, is_first_page, plans_as_first, coverage, edge_fields, requested_cursor, page_info, keeps_total, attempt } = landing;
        let generation = attempt.generation;
        if !attempt.applies_to(self.db.generation()) {
            return Ok(Landed::Discarded(DiscardReason::StaleGeneration));
        }
        let store = &self.db.shared.store;
        let window = store.window(&window_key)?;
        let applied_before = window.as_ref().map(|window| window.applied);
        if is_first_page {
            let newer = applied_before.map(|applied| FetchAttempt::new(&window_key, applied, generation, 0));
            if newer.is_some_and(|newer| attempt.is_superseded_by(&newer)) {
                return Ok(Landed::Discarded(DiscardReason::Superseded));
            }
        } else if window.as_ref().map(|window| window.cursor.as_deref()) != Some(requested_cursor.as_deref()) {
            return Ok(Landed::Discarded(DiscardReason::ChainMoved));
        }
        let model_key = ModelKey::new(&model);
        let applied = if is_first_page { attempt.issued } else { applied_before.unwrap_or(attempt.issued) };
        let tail = window.as_ref().filter(|_| plans_as_first && coverage == Coverage::Page).and_then(|window| {
            let last = window.ids.last()?;
            let listed = normalized.roots.iter().any(|root| root.get("id").and_then(read_id).is_some_and(|id| id.as_str() == last));
            (!listed).then(|| WindowTail { anchor: last.clone(), cursor: window.cursor.clone(), has_next: window.has_next })
        });
        let mut drops = 0;
        let (mut admitted_roots, mut invalid_roots) = (0, 0);
        let committed = self.db.commit_following(generation, |reader| {
            let Admitted { rows: roots, invalid } = admit(&self.db, reader, attempt.base_revision, None, &model, &normalized.roots, &mut drops)?;
            admitted_roots = roots.len();
            invalid_roots = invalid;
            let plan = if plans_as_first {
                let request = ReplaceRequest::new(scope.clone(), coverage);
                self.db.replace_plan(reader, &request, &roots, Some(attempt.base_revision)).map_err(|error| SyncError::Malformed(error.to_string()))?
            } else {
                let ids: Vec<String> = incoming_ids(&roots).iter().map(ToString::to_string).collect();
                let wanted: Vec<&str> = ids.iter().map(String::as_str).collect();
                let stored = StoredScope {
                    last_key: reader.scope_last_key(&model, scope.key.as_str())?,
                    incoming: reader
                        .scope_member_positions(&model, scope.key.as_str(), &wanted)?
                        .into_iter()
                        .map(|(id, order_key, position)| StoredMember { id, order_key, position })
                        .collect(),
                };
                plan_landing(&scope, &model_key, &stored, &roots).map_err(|error| SyncError::Malformed(error.to_string()))?
            };
            let planned: BTreeSet<(ModelKey, RowId)> = plan.entity_ops.iter().map(|op| {
                let row = op.row_ref();
                (row.model, row.id)
            }).collect();
            let mut envelope = CommitEnvelope::new(WriteOrigin::Snapshot, generation);
            envelope.entity_ops = self.related_upserts(reader, attempt.base_revision, &normalized.related, &planned, &mut drops)?;
            envelope.entity_ops.extend(plan.entity_ops);
            envelope.scope_ops = plan.scope_ops;
            envelope.edge_writes = edge_writes(&scope, &edge_fields, &roots, &normalized.edges);
            envelope.window = Some(WindowSave {
                key: window_key.clone(),
                scope: scope.clone(),
                cursor: page_info.as_ref().and_then(|info| info.end_cursor.clone()),
                has_next: page_info.as_ref().is_some_and(|info| info.has_next_page),
                total: page_info.as_ref().and_then(|info| info.total).or(window.as_ref().filter(|_| keeps_total).and_then(|window| window.total)),
                tail: tail.clone(),
                issued: attempt.issued,
                applied,
                cleared_at: is_first_page.then(|| self.now_ms()),
            });
            Ok::<_, SyncError>(Some(envelope))
        });
        match committed {
            Ok(_) => {}
            Err(PlanCommitError::Plan(error)) => return Err(error),
            Err(PlanCommitError::Store(StoreError::Envelope(EnvelopeFault::StaleGeneration))) => return Ok(Landed::Discarded(DiscardReason::StaleGeneration)),
            Err(PlanCommitError::Store(error)) => return Err(error.into()),
        }
        locked(&self.counters).record_stale_version_drops(drops);
        Ok(Landed::Applied { rows: admitted_roots, invalid: invalid_roots })
    }
}
