use std::collections::BTreeMap;

use rusqlite::Connection;
use serde_json::{Map, Value};

use crate::store::{BatchPlan, CommitResult, DestroyReason, EntityOp, OperationTransition, PendingLocal, WindowDebt};
use crate::identity::{Generation, ModelKey, RowId, RowRef};
use crate::owners::store::RowValue;
use crate::schema::AssociationSpec;

use super::merge::{admitted, Admission, RowKind};
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
}

impl RowWrite<'_> {
    pub fn upsert(&mut self, model: &str, row: &RowValue, local: bool) -> Result<Option<CommitResult>, StoreError> {
        let spec = &self.models.get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
        let id = RowId::new(&row.row_id).map_err(|error| StoreError::Constraint(error.to_string()))?;
        let value: Value = serde_json::from_str(&row.values)?;
        let patch: &Map<String, Value> = value.as_object().ok_or_else(|| StoreError::Constraint("row values must be an object".into()))?;
        let kind = if local { RowKind::Local } else { RowKind::Server };
        let reader = Reader::new(self.connection, self.models);
        let guards = self.guards.get(model).map(Vec::as_slice).unwrap_or_default();
        let Some(row) = admitted(&reader, kind, Admission { base: None, closing: self.closing }, model, patch, spec, guards)? else { return Ok(None) };
        let mut envelope = if local { BatchPlan::new(self.generation) } else { BatchPlan::from_server(self.generation) };
        envelope.writes_local = local;
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id, row });
        envelope.validate(self.generation)?;
        super::apply::apply_envelope_at(self.connection, self.models, &envelope, self.now, self.revision, Some(self.originals)).map(Some)
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
        super::apply::apply_envelope_at(self.connection, self.models, &envelope, self.now, self.revision, Some(self.originals))
    }

    pub fn touch_upload(&mut self, key: &str) -> Result<CommitResult, StoreError> {
        let mut parts = key.splitn(3, ':');
        let (Some(model), Some(id), Some(_)) = (parts.next(), parts.next(), parts.next()) else { return Err(StoreError::Constraint(format!("invalid transfer key: {key}"))) };
        let id = RowId::new(id).map_err(|error| StoreError::Constraint(error.to_string()))?;
        let mut envelope = BatchPlan::new(self.generation);
        envelope.entity_ops.push(EntityOp::Touch { model: ModelKey::new(model), id });
        super::apply::apply_envelope_at(self.connection, self.models, &envelope, self.now, self.revision, Some(self.originals))
    }

    pub fn attach(&mut self, record: &crate::files::FileRecord) -> Result<CommitResult, StoreError> {
        let reader = Reader::new(self.connection, self.models);
        let mut envelope = BatchPlan::new(self.generation);
        super::file_write::attach(&reader, self.models, &mut envelope, record)?;
        super::apply::apply_envelope_at(self.connection, self.models, &envelope, self.now, self.revision, Some(self.originals))
    }

    pub fn detach(&mut self, model: &str, id: &str, field: &str) -> Result<CommitResult, StoreError> {
        let reader = Reader::new(self.connection, self.models);
        let mut envelope = BatchPlan::new(self.generation);
        super::file_write::detach(&reader, self.models, &mut envelope, model, id, field)?;
        super::apply::apply_envelope_at(self.connection, self.models, &envelope, self.now, self.revision, Some(self.originals))
    }

    pub fn local_pending(&mut self, model: &str, rows: &[RowValue]) -> Result<CommitResult, StoreError> {
        let mut envelope = BatchPlan::new(self.generation);
        for row in rows {
            let value: Value = serde_json::from_str(&row.values)?;
            let path = value.get("path").and_then(Value::as_str).ok_or_else(|| StoreError::Constraint("pending value has no path".into()))?;
            let value = value.get("value").cloned().ok_or_else(|| StoreError::Constraint("pending value has no value".into()))?;
            envelope.pending_locals.push(PendingLocal { model: model.to_string(), row_id: row.row_id.clone(), path: path.to_string(), value });
        }
        super::apply::apply_envelope_at(self.connection, self.models, &envelope, self.now, self.revision, Some(self.originals))
    }

    pub fn transitions(&mut self, transitions: Vec<OperationTransition>) -> Result<CommitResult, StoreError> {
        let mut envelope = BatchPlan::new(self.generation);
        envelope.operation_transitions = transitions;
        super::apply::apply_envelope_at(self.connection, self.models, &envelope, self.now, self.revision, Some(self.originals))
    }

    pub fn stale_families(&mut self, families: &[(String, String)]) -> Result<CommitResult, StoreError> {
        let since = super::windows::max_sequence(self.connection)? + 1;
        let mut envelope = BatchPlan::new(self.generation);
        envelope.window_debts.extend(families.iter().map(|(model, relation)| WindowDebt { model: model.clone(), relation: relation.clone(), since }));
        super::apply::apply_envelope_at(self.connection, self.models, &envelope, self.now, self.revision, Some(self.originals))
    }

    pub fn numbers(&mut self, model: &str, row_id: &str, writes: &[(String, i64)]) -> Result<Option<CommitResult>, StoreError> {
        let reader = Reader::new(self.connection, self.models);
        let Some(mut row) = reader.point(model, row_id)? else { return Ok(None) };
        for (path, number) in writes { number_at(&mut row, &path.split('.').collect::<Vec<_>>(), *number); }
        let id = RowId::new(row_id).map_err(|error| StoreError::Constraint(error.to_string()))?;
        let mut envelope = BatchPlan::new(self.generation);
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id, row });
        super::apply::apply_envelope_at(self.connection, self.models, &envelope, self.now, self.revision, Some(self.originals)).map(Some)
    }
}
