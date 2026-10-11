use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, EntityOp, EnvelopeFault, ScopeRef, WindowSave, WindowTail, WriteOrigin};
use crate::identity::{ModelKey, RowId};
use crate::db::{Db, Prior, ReplaceRequest};
use crate::schema::Coverage;
use crate::read::{plan_landing, StoredMember};
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
    pub requested_cursor: Option<String>,
    pub page_info: Option<PageInfo>,
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
    let spec = db.spec(model).map_err(|_| SyncError::UnknownModel(model.to_string()))?;
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
                    match stored.as_ref().and_then(|stored| stored.row.as_ref()).and_then(|stored| stored.get(field)) {
                        Some(value) => row.insert(field.clone(), value.clone()),
                        None => row.remove(field),
                    };
                }
                *drops += usize::from(dropped);
                match spec.merged(stored.as_ref().and_then(|stored| stored.row.as_ref()), &row) {
                    Some(merged) => admitted.rows.push(merged),
                    None => admitted.invalid += 1,
                }
            }
            UpsertAdmission::DropExistence => {
                *drops += 1;
                admitted.rows.extend(stored.and_then(|stored| stored.row));
            }
        }
    }
    Ok(admitted)
}

impl Inner {
    pub(crate) fn land(&self, landing: Landing) -> Result<Landed, SyncError> {
        let _serial = locked(&self.landing);
        let Landing { model, scope, window_key, normalized, is_first_page, plans_as_first, coverage, requested_cursor, page_info, attempt } = landing;
        let generation = self.db.generation();
        if !attempt.applies_to(generation) {
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
        let committed = store.commit_with(generation, |reader| {
            let Admitted { rows: roots, invalid } = admit(&self.db, reader, attempt.base_revision, None, &model, &normalized.roots, &mut drops)?;
            admitted_roots = roots.len();
            invalid_roots = invalid;
            let plan = if plans_as_first {
                let request = ReplaceRequest::new(scope.clone(), coverage);
                self.db.replace_plan(reader, &request, &roots, Some(attempt.base_revision)).map_err(|error| SyncError::Malformed(error.to_string()))?
            } else {
                let current: Vec<StoredMember> = reader
                    .scope_members(&model, scope.key.as_str())?
                    .into_iter()
                    .map(|(id, order_key)| StoredMember { id, order_key })
                    .collect();
                plan_landing(&scope, &model_key, &current, &roots, false).map_err(|error| SyncError::Malformed(error.to_string()))?
            };
            let planned: BTreeSet<(ModelKey, RowId)> = plan.entity_ops.iter().map(|op| {
                let row = op.row_ref();
                (row.model, row.id)
            }).collect();
            let mut envelope = CommitEnvelope::new(WriteOrigin::Snapshot, generation);
            for (related_model, row) in &normalized.related {
                for row in admit(&self.db, reader, attempt.base_revision, None, related_model.as_str(), std::slice::from_ref(row), &mut drops)?.rows {
                    let Some(id) = row.get("id").and_then(read_id) else { continue };
                    if !planned.contains(&(related_model.clone(), id.clone())) {
                        envelope.entity_ops.push(EntityOp::Upsert { model: related_model.clone(), id, row });
                    }
                }
            }
            envelope.entity_ops.extend(plan.entity_ops);
            envelope.scope_ops = plan.scope_ops;
            envelope.window = Some(WindowSave {
                key: window_key.clone(),
                scope: scope.clone(),
                cursor: page_info.as_ref().and_then(|info| info.end_cursor.clone()),
                has_next: page_info.as_ref().is_some_and(|info| info.has_next_page),
                total: page_info.as_ref().and_then(|info| info.total),
                tail: tail.clone(),
                issued: attempt.issued,
                applied,
                cleared_at: is_first_page.then(|| self.now_ms()),
            });
            self.db.follow_parent_pointers(reader, &mut envelope)?;
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
