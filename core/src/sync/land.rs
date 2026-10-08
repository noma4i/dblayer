use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, EntityOp, EnvelopeFault, ScopeRef, WriteOrigin};
use crate::identity::{ModelKey, RowId};
use crate::read::{plan_landing, StoredMember};
use crate::store::{admit_upsert, PageInfo, PlanCommitError, Reader, StoreError, UpsertAdmission, WindowRecord};
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
    pub requested_cursor: Option<String>,
    pub page_info: Option<PageInfo>,
    pub attempt: FetchAttempt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Landed {
    Applied { rows: usize },
    Discarded(DiscardReason),
}

fn admit(reader: &Reader<'_>, base: i64, model: &str, rows: &[Map<String, Value>], drops: &mut usize) -> Result<Vec<Map<String, Value>>, StoreError> {
    let mut admitted = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(id) = row.get("id").and_then(read_id) else { continue };
        let stored = reader.stored_row(model, id.as_str())?;
        let tombstone = reader.tombstone_seq(model, id.as_str())?;
        match admit_upsert(Some(base), stored.as_ref(), tombstone, row.clone()) {
            UpsertAdmission::Row { row, dropped_fields } => {
                *drops += usize::from(!dropped_fields.is_empty());
                admitted.push(row);
            }
            UpsertAdmission::DropExistence => *drops += 1,
        }
    }
    Ok(admitted)
}

impl Inner {
    pub(crate) fn land(&self, landing: Landing) -> Result<Landed, SyncError> {
        let _serial = locked(&self.landing);
        let Landing { model, scope, window_key, normalized, is_first_page, plans_as_first, requested_cursor, page_info, attempt } = landing;
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
        let mut drops = 0;
        let mut admitted_roots = 0;
        let committed = store.commit_with(generation, |reader| {
            let roots = admit(reader, attempt.base_revision, &model, &normalized.roots, &mut drops)?;
            admitted_roots = roots.len();
            let current: Vec<StoredMember> = reader
                .scope_members(&model, scope.key.as_str())?
                .into_iter()
                .map(|(id, order_key)| StoredMember { id, order_key })
                .collect();
            let plan = plan_landing(&scope, &model_key, &current, &roots, plans_as_first).map_err(|error| SyncError::Malformed(error.to_string()))?;
            let planned: BTreeSet<(ModelKey, RowId)> = plan.entity_ops.iter().map(|op| {
                let row = op.row_ref();
                (row.model, row.id)
            }).collect();
            let mut envelope = CommitEnvelope::new(WriteOrigin::Snapshot, generation);
            for (related_model, row) in &normalized.related {
                for row in admit(reader, attempt.base_revision, related_model.as_str(), std::slice::from_ref(row), &mut drops)? {
                    let Some(id) = row.get("id").and_then(read_id) else { continue };
                    if !planned.contains(&(related_model.clone(), id.clone())) {
                        envelope.entity_ops.push(EntityOp::Upsert { model: related_model.clone(), id, row });
                    }
                }
            }
            envelope.entity_ops.extend(plan.entity_ops);
            envelope.scope_ops = plan.scope_ops;
            Ok::<_, SyncError>(Some(envelope))
        });
        let result = match committed {
            Ok(result) => result,
            Err(PlanCommitError::Plan(error)) => return Err(error),
            Err(PlanCommitError::Store(StoreError::Envelope(EnvelopeFault::StaleGeneration))) => return Ok(Landed::Discarded(DiscardReason::StaleGeneration)),
            Err(PlanCommitError::Store(error)) => return Err(error.into()),
        };
        self.db.publish(&result);
        locked(&self.counters).record_stale_version_drops(drops);
        let ids = store.scope_members(&model, scope.key.as_str())?.into_iter().map(|(id, _)| id).collect();
        let (cursor, has_next) = page_info.map_or((None, false), |info| (info.end_cursor, info.has_next_page));
        let applied = if is_first_page { attempt.issued } else { applied_before.unwrap_or(attempt.issued) };
        let record = WindowRecord { key: window_key.clone(), cursor, has_next, ids, issued: attempt.issued, applied, generation: generation.value() as i64 };
        store.save_window(&record)?;
        if is_first_page {
            store.clear_invalidation(&window_key, self.now_ms())?;
        }
        Ok(Landed::Applied { rows: admitted_roots })
    }
}
