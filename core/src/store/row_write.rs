use std::collections::BTreeMap;

use rusqlite::Connection;
use serde_json::{Map, Value};

use crate::store::{BatchPlan, CommitResult, DestroyReason, EntityOp, OperationTransition, PendingLocal, WindowDebt};
use crate::identity::{Generation, ModelKey, RowId, RowRef};
use crate::owners::store::RowValue;
use crate::schema::AssociationSpec;

use super::merge::{admitted_with_fields, unconfirmed_fields, Admission, RowKind};
use super::{ModelInfo, Reader, StoreError};

fn number_at(row: &mut Map<String, Value>, path: &[&str], number: i64) {
    let Some((key, rest)) = path.split_first() else { return };
    if rest.is_empty() { row.insert((*key).to_string(), Value::from(number)); return; }
    let value = row.entry((*key).to_string()).or_insert_with(|| Value::Object(Map::new()));
    if !value.is_object() { *value = Value::Object(Map::new()); }
    if let Value::Object(child) = value { number_at(child, rest, number); }
}

pub(super) struct RowWrite<'a> {
    pub connection: &'a Connection,
    pub models: &'a BTreeMap<String, ModelInfo>,
    pub guards: &'a BTreeMap<String, Vec<AssociationSpec>>,
    pub generation: Generation,
    pub revision: i64,
    pub now: i64,
    pub originals: &'a mut BTreeMap<RowRef, Option<Map<String, Value>>>,
    pub closing: Option<i64>,
    pub candidates: usize,
    pub current: super::apply::Prefetched,
    pub feed_fields: BTreeMap<(String, Option<i64>), std::collections::HashMap<String, std::collections::HashSet<String>>>,
    pub feed_tombstones: BTreeMap<String, BTreeMap<String, i64>>,
    pub feed_hidden: Option<super::apply::HiddenRows>,
}

impl RowWrite<'_> {
    pub fn prefetch(&mut self, model: &str, ids: &[String], closing: Option<i64>) -> Result<(), StoreError> {
        let reader = Reader::new(self.connection, self.models);
        let missing: Vec<String> = ids.iter().filter(|id| !self.current.contains_key(&(model.to_string(), (*id).clone()))).cloned().collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        if !missing.is_empty() {
            let mut rows = reader.stored_rows_with_revisions(model, &missing)?;
            for id in missing { self.current.insert((model.to_string(), id.clone()), rows.remove(&id)); }
        }
        let needed: Vec<String> = ids.iter().filter(|id| !self.feed_fields.get(&(model.to_string(), closing)).is_some_and(|fields| fields.contains_key(*id))).cloned().collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        if !needed.is_empty() {
            self.feed_tombstones.entry(model.to_string()).or_default();
            let mut fields = unconfirmed_fields(&reader, closing, model, &needed)?;
            let mut tombstones = reader.tombstones(model, &needed)?;
            for id in needed {
                self.feed_fields.entry((model.to_string(), closing)).or_default().insert(id.clone(), fields.remove(&id).unwrap_or_default());
                if let Some(value) = tombstones.remove(&id) { self.feed_tombstones.entry(model.to_string()).or_default().insert(id, value); }
            }
        }
        Ok(())
    }

    pub fn stored(&mut self, model: &str, id: &str) -> Result<Option<super::rows::StoredRow>, StoreError> {
        let key = (model.to_string(), id.to_string());
        if !self.current.contains_key(&key) {
            let reader = Reader::new(self.connection, self.models);
            self.current.insert(key.clone(), reader.stored_row(model, id)?);
        }
        Ok(self.current.get(&key).cloned().flatten())
    }

    pub fn apply(&mut self, envelope: &BatchPlan) -> Result<CommitResult, StoreError> {
        let lands_rows = envelope.entity_ops.iter().any(|op| matches!(op, EntityOp::Upsert { .. })) || !envelope.scope_ops.is_empty();
        let hidden = if lands_rows { super::apply::hidden_rows(self.connection, &envelope.operation_transitions)? } else { Default::default() };
        let result = super::apply::apply_envelope_at(self.connection, self.models, envelope, self.now, self.revision, Some(self.originals), Some((&mut self.current, &hidden)))?;
        for operation in &envelope.entity_ops {
            if let EntityOp::Destroy { model, id, reason } = operation {
                if reason.writes_tombstone() { self.feed_tombstones.entry(model.to_string()).or_default().insert(id.to_string(), self.revision); }
            }
        }
        if !envelope.operation_transitions.is_empty() { self.feed_fields.clear(); self.feed_hidden = None; }
        Ok(result)
    }

    pub fn admit(&mut self, model: &str, patch: &Map<String, Value>, kind: RowKind, admission: Admission) -> Result<Option<Map<String, Value>>, StoreError> {
        let Some(id) = patch.get("id").and_then(crate::value::read_id) else { return Ok(None) };
        if kind == RowKind::Local { return Ok(Some(patch.clone())); }
        self.prefetch(model, &[id.to_string()], admission.closing)?;
        let mut stored = BTreeMap::new();
        if let Some(row) = self.stored(model, id.as_str())? { stored.insert(id.to_string(), row); }
        let spec = &self.models.get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
        let reader = Reader::new(self.connection, self.models);
        let guards = self.guards.get(model).map(Vec::as_slice).unwrap_or_default();
        admitted_with_fields(&reader, kind, admission, model, patch, spec, guards, self.feed_fields.get(&(model.to_string(), admission.closing)), self.feed_tombstones.get(model), Some(&stored))
    }

    pub fn upsert(&mut self, model: &str, row: &RowValue, local: bool, response: bool) -> Result<Option<CommitResult>, StoreError> {
        let id = RowId::new(&row.row_id).map_err(|error| StoreError::Constraint(error.to_string()))?;
        let value: Value = serde_json::from_str(&row.values)?;
        let patch: &Map<String, Value> = value.as_object().ok_or_else(|| StoreError::Constraint("row values must be an object".into()))?;
        let kind = if local { RowKind::Local } else { RowKind::Server };
        let Some(row) = self.admit(model, patch, kind, Admission { base: None, closing: self.closing })? else { return Ok(None) };
        let mut envelope = BatchPlan::new(self.generation);
        envelope.response_rows = response;
        envelope.writes_local = local;
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id, row });
        envelope.validate(self.generation)?;
        self.apply(&envelope).map(Some)
    }

    pub fn remove(&mut self, model: &str, id: &str) -> Result<CommitResult, StoreError> {
        self.remove_with_reason(model, id, DestroyReason::Local)
    }

    pub fn expire(&mut self, model: &str, id: &str) -> Result<CommitResult, StoreError> {
        self.remove_with_reason(model, id, DestroyReason::Expired)
    }

    fn remove_with_reason(&mut self, model: &str, id: &str, reason: DestroyReason) -> Result<CommitResult, StoreError> {
        let id = RowId::new(id).map_err(|error| StoreError::Constraint(error.to_string()))?;
        let mut envelope = BatchPlan::new(self.generation);
        envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(model), id, reason });
        self.apply(&envelope)
    }

    pub fn touch_upload(&mut self, key: &str) -> Result<CommitResult, StoreError> {
        let mut parts = key.splitn(3, ':');
        let (Some(model), Some(id), Some(_)) = (parts.next(), parts.next(), parts.next()) else { return Err(StoreError::Constraint(format!("invalid transfer key: {key}"))) };
        let id = RowId::new(id).map_err(|error| StoreError::Constraint(error.to_string()))?;
        let mut envelope = BatchPlan::new(self.generation);
        envelope.entity_ops.push(EntityOp::Touch { model: ModelKey::new(model), id });
        let mut result = self.apply(&envelope)?;
        result.uploads_changed = true;
        Ok(result)
    }

    pub fn attach(&mut self, record: &crate::store::files::FileRecord) -> Result<CommitResult, StoreError> {
        let reader = Reader::new(self.connection, self.models);
        let mut envelope = BatchPlan::new(self.generation);
        super::file_write::attach(&reader, self.models, &mut envelope, record)?;
        self.apply(&envelope)
    }

    pub fn detach(&mut self, model: &str, id: &str, field: &str) -> Result<CommitResult, StoreError> {
        let reader = Reader::new(self.connection, self.models);
        let mut envelope = BatchPlan::new(self.generation);
        super::file_write::detach(&reader, self.models, &mut envelope, model, id, field)?;
        self.apply(&envelope)
    }

    pub fn local_pending(&mut self, model: &str, rows: &[RowValue]) -> Result<CommitResult, StoreError> {
        let mut envelope = BatchPlan::new(self.generation);
        for row in rows {
            let value: Value = serde_json::from_str(&row.values)?;
            let path = value.get("path").and_then(Value::as_str).ok_or_else(|| StoreError::Constraint("pending value has no path".into()))?;
            let value = value.get("value").cloned().ok_or_else(|| StoreError::Constraint("pending value has no value".into()))?;
            envelope.pending_locals.push(PendingLocal { model: model.to_string(), row_id: row.row_id.clone(), path: path.to_string(), value });
        }
        self.apply(&envelope)
    }

    pub fn transitions(&mut self, transitions: Vec<OperationTransition>) -> Result<CommitResult, StoreError> {
        let mut envelope = BatchPlan::new(self.generation);
        envelope.operation_transitions = transitions;
        self.apply(&envelope)
    }

    pub fn stale_families(&mut self, families: &[(String, String)]) -> Result<CommitResult, StoreError> {
        let since = super::windows::max_sequence(self.connection)? + 1;
        let mut envelope = BatchPlan::new(self.generation);
        envelope.window_debts.extend(families.iter().map(|(model, relation)| WindowDebt { model: model.clone(), relation: relation.clone(), since }));
        self.apply(&envelope)
    }

    pub fn numbers(&mut self, model: &str, row_id: &str, writes: &[(String, i64)]) -> Result<Option<CommitResult>, StoreError> {
        let Some(mut row) = self.stored(model, row_id)?.map(|stored| stored.row) else { return Ok(None) };
        for (path, number) in writes { number_at(&mut row, &path.split('.').collect::<Vec<_>>(), *number); }
        let id = RowId::new(row_id).map_err(|error| StoreError::Constraint(error.to_string()))?;
        let mut envelope = BatchPlan::new(self.generation);
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id, row });
        self.apply(&envelope).map(Some)
    }
}
