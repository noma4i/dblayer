use std::collections::BTreeMap;

use rusqlite::Connection;
use serde_json::{Map, Value};

use crate::store::{BatchPlan, CommitResult, DestroyReason, EntityOp, OperationTransition, PendingLocal, WindowDebt};
use crate::identity::{Generation, ModelKey, RowId, RowRef};
use crate::owners::store::RowValue;

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
    /// The memory the feed event in its savepoint changed, while the event lands.
    pub event: Option<EventUndo>,
}

type FieldKey = (String, Option<i64>);

/// What one feed event changed in the memory of the writer, given back when the Store refuses the event (P-DATA-99).
#[derive(Default)]
pub(super) struct EventUndo {
    pub rows: Vec<((String, String), Option<Option<super::rows::StoredRow>>)>,
    pub originals: Vec<RowRef>,
    field_ids: Vec<(FieldKey, String)>,
    fields: Vec<(FieldKey, Option<std::collections::HashMap<String, std::collections::HashSet<String>>>)>,
    tombstones: Vec<(String, String, Option<i64>)>,
    hidden: Option<Option<super::apply::HiddenRows>>,
    candidates: usize,
    overlay: usize,
}

impl RowWrite<'_> {
    /// A feed event starts in its savepoint: the writer notes what the event changes from here on.
    pub fn begin_event(&mut self, overlay: usize) { self.event = Some(EventUndo { candidates: self.candidates, overlay, ..EventUndo::default() }); }

    /// The Store refused the feed event: its savepoint rolled back, the memory returns to the state before it.
    pub fn refuse_event(&mut self, overlay: &mut super::feed_write::OverlayWrites) {
        let Some(undo) = self.event.take() else { return };
        for (key, before) in undo.rows.into_iter().rev() {
            match before { Some(row) => { self.current.insert(key, row); } None => { self.current.remove(&key); } }
        }
        for row in undo.originals { self.originals.remove(&row); }
        for (key, before) in undo.fields.into_iter().rev() {
            match before { Some(fields) => { self.feed_fields.insert(key, fields); } None => { self.feed_fields.remove(&key); } }
        }
        for (key, id) in undo.field_ids { if let Some(fields) = self.feed_fields.get_mut(&key) { fields.remove(&id); } }
        for (model, id, before) in undo.tombstones.into_iter().rev() {
            let Some(rows) = self.feed_tombstones.get_mut(&model) else { continue };
            match before { Some(revision) => { rows.insert(id, revision); } None => { rows.remove(&id); } }
        }
        if let Some(hidden) = undo.hidden { self.feed_hidden = hidden; }
        self.candidates = undo.candidates;
        overlay.truncate(undo.overlay);
    }

    /// The feed event replaces the hidden rows and the unconfirmed fields it read after its write.
    pub fn replace_feed_reads(&mut self, hidden: super::apply::HiddenRows, fields: Vec<(FieldKey, std::collections::HashMap<String, std::collections::HashSet<String>>)>) {
        let before = std::mem::replace(&mut self.feed_hidden, Some(hidden));
        for (key, value) in fields {
            let old = self.feed_fields.insert(key.clone(), value);
            if let Some(undo) = self.event.as_mut() { undo.fields.push((key, old)); }
        }
        if let Some(undo) = self.event.as_mut() { undo.hidden.get_or_insert(before); }
    }

    /// A server delete of the feed event leaves a tombstone at the revision of the batch.
    pub fn feed_tombstone(&mut self, model: &str, id: &str) {
        let before = self.feed_tombstones.entry(model.to_string()).or_default().insert(id.to_string(), self.revision);
        if let Some(undo) = self.event.as_mut() { undo.tombstones.push((model.to_string(), id.to_string(), before)); }
    }

    pub fn prefetch(&mut self, model: &str, ids: &[String], closing: Option<i64>) -> Result<(), StoreError> {
        let reader = Reader::new(self.connection, self.models);
        let missing: Vec<String> = ids.iter().filter(|id| !self.current.contains_key(&(model.to_string(), (*id).clone()))).cloned().collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        if !missing.is_empty() {
            let mut rows = reader.stored_rows_with_revisions(model, &missing)?;
            for id in missing {
                if let Some(undo) = self.event.as_mut() { undo.rows.push(((model.to_string(), id.clone()), None)); }
                self.current.insert((model.to_string(), id.clone()), rows.remove(&id));
            }
        }
        let needed: Vec<String> = ids.iter().filter(|id| !self.feed_fields.get(&(model.to_string(), closing)).is_some_and(|fields| fields.contains_key(*id))).cloned().collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        if !needed.is_empty() {
            self.feed_tombstones.entry(model.to_string()).or_default();
            let mut fields = unconfirmed_fields(&reader, closing, model, &needed)?;
            let mut tombstones = reader.tombstones(model, &needed)?;
            for id in needed {
                if let Some(undo) = self.event.as_mut() { undo.field_ids.push(((model.to_string(), closing), id.clone())); }
                self.feed_fields.entry((model.to_string(), closing)).or_default().insert(id.clone(), fields.remove(&id).unwrap_or_default());
                if let Some(value) = tombstones.remove(&id) {
                    let before = self.feed_tombstones.entry(model.to_string()).or_default().insert(id.clone(), value);
                    if let Some(undo) = self.event.as_mut() { undo.tombstones.push((model.to_string(), id, before)); }
                }
            }
        }
        Ok(())
    }

    pub fn stored(&mut self, model: &str, id: &str) -> Result<Option<super::rows::StoredRow>, StoreError> {
        let key = (model.to_string(), id.to_string());
        if !self.current.contains_key(&key) {
            let reader = Reader::new(self.connection, self.models);
            let row = reader.stored_row(model, id)?;
            if let Some(undo) = self.event.as_mut() { undo.rows.push((key.clone(), None)); }
            self.current.insert(key.clone(), row);
        }
        Ok(self.current.get(&key).cloned().flatten())
    }

    pub fn apply(&mut self, envelope: &BatchPlan) -> Result<CommitResult, StoreError> {
        if self.event.is_some() { return Err(StoreError::Poisoned("a batch plan applies while a feed event lands".into())); }
        let lands_rows = envelope.entity_ops.iter().any(|op| matches!(op, EntityOp::Upsert { .. })) || !envelope.scope_ops.is_empty();
        let hidden = if lands_rows { super::apply::hidden_rows(self.connection, &envelope.operation_transitions)? } else { Default::default() };
        let result = super::apply::apply_envelope_at(self.connection, self.models, envelope, (self.now, self.revision, self.generation), Some(self.originals), Some((&mut self.current, &hidden, &mut EventUndo::default())))?;
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
        self.prefetch(model, &[id.to_string()], admission.closing)?;
        let mut stored = BTreeMap::new();
        if let Some(row) = self.stored(model, id.as_str())? { stored.insert(id.to_string(), row); }
        let spec = &self.models.get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
        let reader = Reader::new(self.connection, self.models);
        admitted_with_fields(&reader, kind, admission, model, patch, spec, self.feed_fields.get(&(model.to_string(), admission.closing)), self.feed_tombstones.get(model), Some(&stored))
    }

    pub fn upsert(&mut self, model: &str, row: &RowValue, source: crate::schema::RowSource) -> Result<Option<CommitResult>, StoreError> {
        let id = RowId::new(&row.row_id).map_err(|error| StoreError::Constraint(error.to_string()))?;
        let value: Value = serde_json::from_str(&row.values)?;
        let patch: &Map<String, Value> = value.as_object().ok_or_else(|| StoreError::Constraint("row values must be an object".into()))?;
        let kind = if source == crate::schema::RowSource::Local { RowKind::Local } else { RowKind::Server };
        let Some(row) = self.admit(model, patch, kind, Admission { base: None, closing: self.closing })? else { return Ok(None) };
        let mut envelope = BatchPlan::from_source(self.generation, source);
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id, row });
        self.apply(&envelope).map(Some)
    }

    pub fn remove(&mut self, model: &str, id: &str) -> Result<CommitResult, StoreError> {
        self.remove_with_reason(model, id, DestroyReason::Local)
    }

    pub fn remove_server(&mut self, model: &str, id: &str) -> Result<CommitResult, StoreError> {
        self.remove_with_reason(model, id, DestroyReason::Server)
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
        let current = self.stored(&record.model, &record.row_id)?.map(|stored| stored.row);
        let mut envelope = BatchPlan::new(self.generation);
        super::file_write::attach(current, self.models, &mut envelope, record)?;
        self.apply(&envelope)
    }

    pub fn detach(&mut self, model: &str, id: &str, field: &str) -> Result<CommitResult, StoreError> {
        let current = self.stored(model, id)?.map(|stored| stored.row);
        let mut envelope = BatchPlan::new(self.generation);
        super::file_write::detach(current, self.models, &mut envelope, model, id, field)?;
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
