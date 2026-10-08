use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde_json::{Map, Value};

use crate::commit::{
    CommitEnvelope, CommitResult, DestroyReason, EntityOp, OperationRecord, OperationStatus, OperationTransition,
    RowChange, RowChangeKind, ScopeOp, ScopeRef,
};
use crate::ddl::{manifest_write, RECORD_VERSION, REVISION_SEQ_KEY};
use crate::identity::{Generation, ModelKey, RowRef};
use crate::syncstate::{operation_keys, recompute_sync, SyncKey};

use super::file_ops::{apply_file_ops, release_files};
use super::rows::{column_value, encode_row, hydrate, placeholders, Reader, CHUNK};
use super::tombstones::{prune_tombstones, write_tombstone, TombstoneRetention};
use super::{Inner, ModelInfo, Store, StoreError};

#[derive(Debug, thiserror::Error)]
pub enum PlanCommitError<E> {
    #[error("planning failed")]
    Plan(E),
    #[error(transparent)]
    Store(StoreError),
}

impl<E> From<StoreError> for PlanCommitError<E> {
    fn from(error: StoreError) -> Self {
        PlanCommitError::Store(error)
    }
}

impl Store {
    pub fn commit(&self, envelope: CommitEnvelope) -> Result<CommitResult, StoreError> {
        let generation = envelope.generation;
        match self.commit_with(generation, |_| Ok::<_, std::convert::Infallible>(Some(envelope))) {
            Ok(result) => Ok(result),
            Err(PlanCommitError::Store(error)) => Err(error),
            Err(PlanCommitError::Plan(never)) => match never {},
        }
    }

    pub fn commit_with<E>(
        &self,
        generation: Generation,
        plan: impl FnOnce(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<CommitResult, PlanCommitError<E>> {
        let mut inner = self.lock();
        inner.ensure_healthy()?;
        if generation != inner.generation {
            return Err(StoreError::Envelope(crate::commit::EnvelopeFault::StaleGeneration).into());
        }
        let now_ms = inner.now_ms();
        let envelope = {
            let reader = Reader::new(&inner.connection, &inner.models);
            plan(&reader).map_err(PlanCommitError::Plan)?
        };
        let Some(envelope) = envelope else {
            return Ok(CommitResult::noop(uuid::Uuid::new_v4()));
        };
        envelope.validate(inner.generation).map_err(StoreError::Envelope)?;
        if envelope.is_empty() {
            return Ok(CommitResult::noop(envelope.tx_id));
        }
        match attempt(&inner, &envelope, now_ms) {
            Ok(result) => Ok(self.finish(inner, result)),
            Err(first) if first.is_apply_failure() => match attempt(&inner, &envelope, now_ms) {
                Ok(result) => Ok(self.finish(inner, result)),
                Err(second) => {
                    if second.is_apply_failure() {
                        inner.poison = Some(second.to_string());
                    }
                    Err(second.into())
                }
            },
            Err(other) => Err(other.into()),
        }
    }

    fn finish(&self, mut inner: std::sync::MutexGuard<'_, Inner>, result: (CommitResult, u32)) -> CommitResult {
        inner.counters.unknown_operation_acks += result.1 as usize;
        if !result.0.is_noop() {
            self.announce(super::StoreEvent::Committed(&result.0));
        }
        result.0
    }
}

fn attempt(inner: &Inner, envelope: &CommitEnvelope, now_ms: i64) -> Result<(CommitResult, u32), StoreError> {
    let transaction = inner.connection.unchecked_transaction()?;
    let outcome = apply_envelope(&transaction, &inner.models, envelope, now_ms)?;
    if outcome.is_noop() {
        transaction.rollback()?;
    } else {
        transaction.commit()?;
    }
    let acks = outcome.unknown_operation_acks;
    Ok((outcome, acks))
}

fn json_bytes<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, StoreError> {
    Ok(serde_json::to_vec(value)?)
}

fn diff_fields(old: &Map<String, Value>, new: &Map<String, Value>) -> BTreeSet<String> {
    let mut changed = BTreeSet::new();
    for (key, value) in new {
        if old.get(key) != Some(value) {
            changed.insert(key.clone());
        }
    }
    for key in old.keys() {
        if !new.contains_key(key) {
            changed.insert(key.clone());
        }
    }
    changed
}

type HiddenRows = HashSet<(String, String)>;
type OpenDestroy = (i64, String, Vec<u8>, Option<Vec<u8>>);

fn hidden_rows(connection: &Connection, transitions: &[OperationTransition]) -> Result<HiddenRows, StoreError> {
    let mut released: HashSet<i64> = HashSet::new();
    let mut rewritten: HashMap<i64, &OperationRecord> = HashMap::new();
    for transition in transitions {
        match transition {
            OperationTransition::Close { seq, status, .. } if status.is_terminal() => {
                released.insert(*seq);
            }
            OperationTransition::Fail { seq, status: OperationStatus::Failed, .. } | OperationTransition::Remove { seq, .. } => {
                released.insert(*seq);
            }
            OperationTransition::Begin(record) => {
                if let Some(seq) = record.seq {
                    rewritten.insert(seq, record);
                }
            }
            _ => {}
        }
    }
    let mut statement =
        connection.prepare_cached("SELECT seq, action_key, row_ids, rollback FROM op WHERE status IN ('pending', 'delivery_unknown') AND action_key LIKE '%:destroy'")?;
    let open: Vec<OpenDestroy> =
        statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?.collect::<Result<_, _>>()?;
    let mut hidden = HiddenRows::new();
    for (seq, action_key, row_ids, rollback) in open {
        if released.contains(&seq) {
            continue;
        }
        let model = action_key.split(':').next().unwrap_or_default().to_string();
        let ids: Vec<String> = match rewritten.get(&seq) {
            Some(record) => record.row_ids.iter().map(|id| id.to_string()).collect(),
            None => serde_json::from_slice(&row_ids)?,
        };
        hidden.extend(ids.into_iter().map(|id| (model.clone(), id)));
        let snapshot: Option<Value> = rollback.map(|bytes| serde_json::from_slice(&bytes)).transpose()?;
        let rows = snapshot.as_ref().and_then(|snapshot| snapshot.get("rows")).and_then(Value::as_array);
        for item in rows.into_iter().flatten() {
            let owner = item.get("model").and_then(Value::as_str);
            let id = item.get("id").and_then(Value::as_str);
            if let (Some(owner), Some(id)) = (owner, id) {
                hidden.insert((owner.to_string(), id.to_string()));
            }
        }
    }
    Ok(hidden)
}

fn visible_scope_ops(operations: &[ScopeOp], hidden: &HiddenRows) -> Vec<ScopeOp> {
    let is_hidden = |scope: &ScopeRef, row: &crate::identity::RowId| hidden.contains(&(scope.model.to_string(), row.to_string()));
    operations
        .iter()
        .filter_map(|operation| match operation {
            ScopeOp::Append { scope, row, .. } | ScopeOp::Move { scope, row, .. } if is_hidden(scope, row) => None,
            ScopeOp::ReplaceAll { scope, members } => {
                Some(ScopeOp::ReplaceAll { scope: scope.clone(), members: members.iter().filter(|(row, _)| !is_hidden(scope, row)).cloned().collect() })
            }
            other => Some(other.clone()),
        })
        .collect()
}

fn apply_upsert(
    connection: &Connection,
    info: &ModelInfo,
    model: &ModelKey,
    row: &Map<String, Value>,
    revision: i64,
    hidden: &HiddenRows,
    result: &mut RowChanges,
) -> Result<(), StoreError> {
    let Some((id, payload)) = encode_row(row)? else { return Ok(()) };
    if hidden.contains(&(model.to_string(), id.clone())) {
        return Ok(());
    }
    let existing: Option<(i64, Vec<u8>, i64, Vec<u8>)> = connection
        .query_row(
            &format!("SELECT born_rev, field_rev, record_version, payload FROM {} WHERE id = ?1", info.table),
            params![id],
            |record| Ok((record.get(0)?, record.get(1)?, record.get(2)?, record.get(3)?)),
        )
        .optional()?;
    let old_row = existing.as_ref().and_then(|(_, _, version, stored)| match hydrate(*version, stored, &id) {
        (_, Some(old)) => Some(old),
        _ => None,
    });
    let (born_rev, mut field_rev, changed, kind) = match (&existing, &old_row) {
        (Some((born, field_rev, _, _)), Some(old)) => {
            let changed = diff_fields(old, row);
            if changed.is_empty() {
                return Ok(());
            }
            let field_rev: BTreeMap<String, i64> = serde_json::from_slice(field_rev).unwrap_or_default();
            (*born, field_rev, changed, RowChangeKind::Updated)
        }
        (Some(_), None) => (revision, BTreeMap::new(), row.keys().cloned().collect(), RowChangeKind::Updated),
        (None, _) => (revision, BTreeMap::new(), row.keys().cloned().collect(), RowChangeKind::Inserted),
    };
    for field in &changed {
        field_rev.insert(field.clone(), revision);
    }
    field_rev.retain(|field, _| row.contains_key(field));
    let mut values: Vec<rusqlite::types::Value> = vec![
        id.clone().into(),
        revision.into(),
        born_rev.into(),
        json_bytes(&field_rev)?.into(),
        RECORD_VERSION.into(),
        payload.into(),
    ];
    values.extend(info.promoted.iter().map(|column| column_value(column.kind, row.get(&column.field))));
    connection.execute(&info.upsert_sql, params_from_iter(values))?;
    result.push(model, &id, kind, changed);
    Ok(())
}

#[derive(Default)]
struct RowChanges {
    changes: Vec<RowChange>,
}

impl RowChanges {
    fn push(&mut self, model: &ModelKey, id: &str, kind: RowChangeKind, fields: BTreeSet<String>) {
        if let Ok(row_id) = crate::identity::RowId::new(id) {
            self.changes.push(RowChange { row: RowRef::new(model.clone(), row_id), kind, fields });
        }
    }
}

struct Destroyed {
    model: ModelKey,
    id: String,
    reason: DestroyReason,
}

pub(crate) fn apply_envelope(
    connection: &Connection,
    models: &BTreeMap<String, ModelInfo>,
    envelope: &CommitEnvelope,
    now_ms: i64,
) -> Result<CommitResult, StoreError> {
    let reader = Reader::new(connection, models);
    let revision = reader.revision_seq()? + 1;
    let mut wrote = false;
    let mut tombstoned = false;
    let mut files_changed = false;
    let mut changes = RowChanges::default();
    let mut destroyed: Vec<Destroyed> = Vec::new();
    let mut replaced: Vec<(RowRef, crate::identity::RowId)> = Vec::new();
    let mut result = CommitResult::noop(envelope.tx_id);
    let lands_rows = envelope.entity_ops.iter().any(|operation| matches!(operation, EntityOp::Upsert { .. })) || !envelope.scope_ops.is_empty();
    let hidden = if lands_rows { hidden_rows(connection, &envelope.operation_transitions)? } else { HiddenRows::new() };

    for operation in &envelope.entity_ops {
        match operation {
            EntityOp::Upsert { model, row, .. } => {
                let info = models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?;
                apply_upsert(connection, info, model, row, revision, &hidden, &mut changes)?;
            }
            EntityOp::Touch { model, id } => {
                let info = models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?;
                let touched = connection
                    .execute(&format!("UPDATE {} SET revision = ?1 WHERE id = ?2", info.table), params![revision, id.as_str()])?;
                if touched > 0 {
                    changes.push(model, id.as_str(), RowChangeKind::Touched, BTreeSet::new());
                }
            }
            EntityOp::Destroy { model, id, reason } => {
                let info = models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?;
                let removed =
                    connection.execute(&format!("DELETE FROM {} WHERE id = ?1", info.table), params![id.as_str()])?;
                if reason.writes_tombstone() {
                    write_tombstone(connection, model.as_str(), id.as_str(), revision, now_ms)?;
                    tombstoned = true;
                }
                if removed > 0 {
                    changes.push(model, id.as_str(), RowChangeKind::Destroyed, BTreeSet::new());
                    if let DestroyReason::Replaced { by } = reason {
                        replaced.push((RowRef::new(model.clone(), id.clone()), by.clone()));
                    }
                }
                files_changed |= release_files(connection, model, id, reason)?;
                destroyed.push(Destroyed { model: model.clone(), id: id.as_str().to_string(), reason: reason.clone() });
            }
        }
    }

    stamp_confirmed_rows(connection, models, &envelope.operation_transitions, revision)?;
    let mut synced_keys: BTreeSet<SyncKey> = BTreeSet::new();
    wrote |= apply_transitions(connection, &envelope.operation_transitions, now_ms, &mut synced_keys, &mut result)?;
    for (model, id) in recompute_sync(connection, &synced_keys)? {
        let touched = changes.changes.iter().any(|change| change.row.model.as_str() == model && change.row.id.as_str() == id);
        if !touched {
            changes.push(&ModelKey::new(&model), &id, RowChangeKind::Synced, BTreeSet::new());
        }
        wrote = true;
    }

    collect_scopes(connection, &changes.changes, &destroyed, &mut result.scopes)?;
    for item in &destroyed {
        let removed = match &item.reason {
            DestroyReason::Replaced { by } => connection.execute(
                "UPDATE OR REPLACE scope_member SET row_id = ?1 WHERE model = ?2 AND row_id = ?3",
                params![by.as_str(), item.model.as_str(), item.id],
            )?,
            _ => connection.execute(
                "DELETE FROM scope_member WHERE model = ?1 AND row_id = ?2",
                params![item.model.as_str(), item.id],
            )?,
        };
        if removed > 0 {
            wrote = true;
        }
    }
    wrote |= !changes.changes.is_empty() || tombstoned;

    files_changed |= apply_file_ops(connection, &envelope.file_ops)?;
    wrote |= files_changed;

    let visible;
    let scope_ops: &[ScopeOp] = if hidden.is_empty() {
        &envelope.scope_ops
    } else {
        visible = visible_scope_ops(&envelope.scope_ops, &hidden);
        &visible
    };
    wrote |= apply_scope_ops(connection, scope_ops, &mut result.scopes)?;

    if wrote {
        manifest_write(connection, REVISION_SEQ_KEY, &revision.to_string())?;
        result.revision = Some(revision);
        result.rows = changes.changes;
        result.replaced = replaced;
        if tombstoned {
            prune_tombstones(connection, now_ms, &TombstoneRetention::DEFAULT)?;
        }
    } else {
        result.scopes.clear();
    }
    Ok(result)
}

fn collect_scopes(
    connection: &Connection,
    changes: &[RowChange],
    destroyed: &[Destroyed],
    scopes: &mut BTreeSet<ScopeRef>,
) -> Result<(), StoreError> {
    let mut by_model: BTreeMap<&ModelKey, BTreeSet<&str>> = BTreeMap::new();
    for change in changes {
        if change.kind != RowChangeKind::Inserted {
            by_model.entry(&change.row.model).or_default().insert(change.row.id.as_str());
        }
    }
    for item in destroyed {
        by_model.entry(&item.model).or_default().insert(item.id.as_str());
    }
    for (model, ids) in by_model {
        let ids: Vec<&str> = ids.into_iter().collect();
        for chunk in ids.chunks(CHUNK) {
            let sql = format!(
                "SELECT DISTINCT scope_key FROM scope_member WHERE model = ? AND row_id IN ({})",
                placeholders(chunk.len())
            );
            let mut statement = connection.prepare_cached(&sql)?;
            let arguments = std::iter::once(model.as_str()).chain(chunk.iter().copied());
            let keys = statement.query_map(params_from_iter(arguments), |row| row.get::<_, String>(0))?;
            for key in keys {
                scopes.insert(ScopeRef::new(model.clone(), crate::identity::ScopeKey::from_raw(&key?)));
            }
        }
    }
    Ok(())
}

fn apply_scope_ops(
    connection: &Connection,
    operations: &[ScopeOp],
    scopes: &mut BTreeSet<ScopeRef>,
) -> Result<bool, StoreError> {
    let mut wrote = false;
    for operation in operations {
        if let ScopeOp::Move { scope, row, .. } = operation {
            connection.execute(
                "UPDATE scope_member SET order_key = ?1 WHERE model = ?2 AND scope_key = ?3 AND row_id = ?4",
                params![format!("~parked~{}", row.as_str()), scope.model.as_str(), scope.key.as_str(), row.as_str()],
            )?;
        }
    }
    for operation in operations {
        let changed = match operation {
            ScopeOp::Append { scope, row, order_key } => connection.execute(
                "INSERT INTO scope_member (model, scope_key, row_id, order_key) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT (model, scope_key, row_id) DO UPDATE SET order_key = excluded.order_key \
                 WHERE order_key IS NOT excluded.order_key",
                params![scope.model.as_str(), scope.key.as_str(), row.as_str(), order_key],
            )?,
            ScopeOp::Move { scope, row, order_key } => connection.execute(
                "UPDATE scope_member SET order_key = ?1 WHERE model = ?2 AND scope_key = ?3 AND row_id = ?4",
                params![order_key, scope.model.as_str(), scope.key.as_str(), row.as_str()],
            )?,
            ScopeOp::Detach { scope, row } => connection.execute(
                "DELETE FROM scope_member WHERE model = ?1 AND scope_key = ?2 AND row_id = ?3",
                params![scope.model.as_str(), scope.key.as_str(), row.as_str()],
            )?,
            ScopeOp::ReplaceAll { scope, members } => {
                let mut changed = connection.execute(
                    "DELETE FROM scope_member WHERE model = ?1 AND scope_key = ?2",
                    params![scope.model.as_str(), scope.key.as_str()],
                )?;
                for (row, order_key) in members {
                    changed += connection.execute(
                        "INSERT INTO scope_member (model, scope_key, row_id, order_key) VALUES (?1, ?2, ?3, ?4)",
                        params![scope.model.as_str(), scope.key.as_str(), row.as_str(), order_key],
                    )?;
                }
                changed
            }
        };
        if changed > 0 {
            wrote = true;
            scopes.insert(operation.scope().clone());
        }
    }
    Ok(wrote)
}

fn op_keys_of(connection: &Connection, seq: i64) -> Result<Vec<SyncKey>, StoreError> {
    let found: Option<(String, Vec<u8>, Option<String>)> = connection
        .query_row("SELECT action_key, row_ids, temp_id FROM op WHERE seq = ?1", params![seq], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .optional()?;
    let Some((action_key, row_ids, temp_id)) = found else { return Ok(Vec::new()) };
    let row_ids: Vec<crate::identity::RowId> = serde_json::from_slice(&row_ids)?;
    let temp_id = temp_id.and_then(|text| crate::identity::RowId::new(&text).ok());
    Ok(operation_keys(&action_key, &row_ids, temp_id.as_ref()))
}

fn stamp_confirmed_rows(
    connection: &Connection,
    models: &BTreeMap<String, ModelInfo>,
    transitions: &[OperationTransition],
    revision: i64,
) -> Result<(), StoreError> {
    for transition in transitions {
        let OperationTransition::Close { seq, status: OperationStatus::Committed, .. } = transition else { continue };
        let rollback: Option<Vec<u8>> = connection.query_row("SELECT rollback FROM op WHERE seq = ?1", params![seq], |row| row.get(0)).optional()?.flatten();
        let Some(snapshot) = rollback.and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok()) else { continue };
        for item in snapshot.get("rows").and_then(Value::as_array).into_iter().flatten() {
            let (Some(kind), Some(model), Some(id)) =
                (item.get("kind").and_then(Value::as_str), item.get("model").and_then(Value::as_str), item.get("id").and_then(Value::as_str))
            else {
                continue;
            };
            match (kind, item.get("owned").and_then(Value::as_object), models.get(model)) {
                ("destroyed", _, _) => {
                    connection.execute("UPDATE tombstone SET seq = ?1 WHERE model = ?2 AND id = ?3", params![revision, model, id])?;
                }
                ("edited", Some(owned), Some(info)) => stamp_owned_fields(connection, info, id, owned, revision)?,
                _ => {}
            }
        }
    }
    Ok(())
}

fn stamp_owned_fields(connection: &Connection, info: &ModelInfo, id: &str, owned: &Map<String, Value>, revision: i64) -> Result<(), StoreError> {
    let stored: Option<Vec<u8>> = connection.query_row(&format!("SELECT field_rev FROM {} WHERE id = ?1", info.table), params![id], |row| row.get(0)).optional()?;
    let Some(mut field_rev) = stored.and_then(|bytes| serde_json::from_slice::<BTreeMap<String, i64>>(&bytes).ok()) else { return Ok(()) };
    field_rev.iter_mut().filter(|(field, _)| owned.contains_key(*field)).for_each(|(_, stamp)| *stamp = revision);
    connection.execute(&format!("UPDATE {} SET field_rev = ?1 WHERE id = ?2", info.table), params![json_bytes(&field_rev)?, id])?;
    Ok(())
}

fn apply_transitions(
    connection: &Connection,
    transitions: &[OperationTransition],
    now_ms: i64,
    synced: &mut BTreeSet<SyncKey>,
    result: &mut CommitResult,
) -> Result<bool, StoreError> {
    let mut wrote = false;
    for transition in transitions {
        match transition {
            OperationTransition::Begin(record) => {
                if let Some(seq) = record.seq {
                    synced.extend(op_keys_of(connection, seq)?);
                }
                let seq = begin_operation(connection, record)?;
                synced.extend(operation_keys(&record.action_key, &record.row_ids, record.temp_id.as_ref()));
                result.operations.insert(seq);
                wrote = true;
            }
            OperationTransition::Close { seq, status, once } => {
                let found: Option<(String, Option<String>)> = connection
                    .query_row("SELECT action_key, dedupe_key FROM op WHERE seq = ?1", params![seq], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })
                    .optional()?;
                let Some((action_key, dedupe_key)) = found else {
                    result.unknown_operation_acks += 1;
                    continue;
                };
                synced.extend(op_keys_of(connection, *seq)?);
                if status.is_terminal() {
                    if let (true, OperationStatus::Committed, Some(key)) = (*once, status, dedupe_key) {
                        connection.execute(
                            "INSERT OR IGNORE INTO op_once (namespace, dedupe_key) VALUES (?1, ?2)",
                            params![action_key, key],
                        )?;
                    }
                    connection.execute("DELETE FROM op WHERE seq = ?1", params![seq])?;
                } else {
                    set_open_status(connection, *seq, *status, None, now_ms)?;
                }
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Fail { seq, status, failure } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let error = json_bytes(failure)?;
                if set_open_status(connection, *seq, *status, Some(error), now_ms)? == 0 {
                    result.unknown_operation_acks += 1;
                    continue;
                }
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Start { seq } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let started = connection.execute(
                    "UPDATE op SET transport_started = 1, attempts = attempts + 1, since = ?2 WHERE seq = ?1 AND status = 'pending' AND transport_started = 0",
                    params![seq, now_ms],
                )?;
                if started == 0 {
                    result.unknown_operation_acks += 1;
                    continue;
                }
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Postpone { seq, failure } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let postponed = connection.execute(
                    "UPDATE op SET transport_started = 0, error = ?2, since = ?3 WHERE seq = ?1 AND status = 'pending' AND transport_started = 1",
                    params![seq, json_bytes(failure)?, now_ms],
                )?;
                if postponed == 0 {
                    result.unknown_operation_acks += 1;
                    continue;
                }
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Progress { seq, state } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let saved = connection.execute("UPDATE op SET upload = ?2 WHERE seq = ?1", params![seq, json_bytes(state)?])?;
                if saved == 0 {
                    result.unknown_operation_acks += 1;
                    continue;
                }
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Retry { seq } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let retried = connection.execute(
                    "UPDATE op SET status = 'pending', transport_started = 0, attempts = 0, error = NULL, since = ?2 \
                     WHERE seq = ?1 AND status IN ('failed', 'delivery_unknown')",
                    params![seq, now_ms],
                )?;
                if retried == 0 {
                    result.unknown_operation_acks += 1;
                    continue;
                }
                result.operations.insert(*seq);
                wrote = true;
            }
            OperationTransition::Remove { seq, expected } => {
                synced.extend(op_keys_of(connection, *seq)?);
                let removed = match expected {
                    Some(status) => connection
                        .execute("DELETE FROM op WHERE seq = ?1 AND status = ?2", params![seq, status.as_sql()])?,
                    None => connection.execute("DELETE FROM op WHERE seq = ?1", params![seq])?,
                };
                if removed == 0 {
                    result.unknown_operation_acks += 1;
                } else {
                    result.operations.insert(*seq);
                    wrote = true;
                }
            }
        }
    }
    Ok(wrote)
}

fn set_open_status(connection: &Connection, seq: i64, status: OperationStatus, error: Option<Vec<u8>>, now_ms: i64) -> Result<usize, StoreError> {
    let dedupe = if status == OperationStatus::Failed { ", dedupe_key = NULL" } else { "" };
    Ok(connection.execute(
        &format!("UPDATE op SET status = ?1, error = ?2, since = ?3{dedupe} WHERE seq = ?4"),
        params![status.as_sql(), error, now_ms, seq],
    )?)
}

fn begin_operation(connection: &Connection, record: &OperationRecord) -> Result<i64, StoreError> {
    let row_ids: Vec<&str> = record.row_ids.iter().map(|id| id.as_str()).collect();
    let dedupe_key = if record.status == OperationStatus::Failed { None } else { record.dedupe_key.clone() };
    let rollback = record.rollback.as_ref().map(json_bytes).transpose()?;
    connection.execute(
        "INSERT INTO op (seq, action_key, status, row_ids, temp_id, input, transport_started, \
         base_revision, created_at, dedupe_key, rollback, since, upload) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?9, ?12) \
         ON CONFLICT (seq) DO UPDATE SET action_key = excluded.action_key, \
         status = excluded.status, row_ids = excluded.row_ids, temp_id = excluded.temp_id, input = excluded.input, \
         transport_started = excluded.transport_started, base_revision = excluded.base_revision, \
         created_at = excluded.created_at, dedupe_key = excluded.dedupe_key, rollback = excluded.rollback",
        params![
            record.seq,
            record.action_key,
            record.status.as_sql(),
            json_bytes(&row_ids)?,
            record.temp_id.as_ref().map(|id| id.as_str().to_string()),
            json_bytes(&record.input)?,
            i64::from(record.transport_started),
            record.base_revision,
            record.created_at,
            dedupe_key,
            rollback,
            record.upload.as_ref().map(json_bytes).transpose()?,
        ],
    )?;
    Ok(record.seq.unwrap_or_else(|| connection.last_insert_rowid()))
}
