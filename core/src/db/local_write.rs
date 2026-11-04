use std::collections::{BTreeSet, HashSet};

use rusqlite::OptionalExtension;
use serde_json::{json, Map, Value};

use crate::commit::{
    CommitEnvelope, CommitResult, DestroyReason, EntityOp, OperationRecord, OperationTransition, ScopeOp, ScopeRef, WriteOrigin,
};
use crate::operation::OperationStatus;
use crate::ddl::table_name;
use crate::files::references_of_row;
use crate::identity::{ModelKey, RowId, RowRef};
use crate::value::{fractional_between, same_fields};
use crate::operation::Operation;
use crate::query::{Query, Where};
use crate::schema::{AssociationKind, Dependent, ModelSpec, SchemaError, ValueFault};
use crate::store::{PlanCommitError, Reader, StoreError};
use crate::syncstate::read_sync;
use crate::upload::UploadState;
use crate::value::read_id;
use crate::write::{cascade_destroys, cascade_envelope, WritePlan};

use super::error::DbError;
use super::open::Db;
use super::effects::{Call, Edits};
use super::prior::{Prior, PriorRow};
use super::row::Row;

#[derive(Debug, Clone)]
pub struct Acted {
    pub row: Row,
    pub operation: i64,
    pub revision: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutboxOperation {
    pub operation: Operation,
    pub input: Value,
    pub rollback: Option<Value>,
}

impl OutboxOperation {
    pub(crate) fn for_rows(reader: &Reader<'_>, keys: &BTreeSet<(String, String)>) -> Result<Vec<Self>, StoreError> {
        crate::operation::row_operation_sequences(reader.connection(), keys)?.into_iter().filter_map(|seq| Self::read(reader, seq).transpose()).collect()
    }

    pub(crate) fn read(reader: &Reader<'_>, seq: i64) -> Result<Option<Self>, StoreError> {
        let Some(mut operation) = crate::operation::read_operation(reader.connection(), seq)? else { return Ok(None) };
        operation.upload = crate::operation::read_operation_uploads(reader.connection(), seq)?.into_iter().min_by_key(|(key, _)| *key).map(|(_, state)| state);
        Self::from_operation(reader, operation).map(Some)
    }

    fn from_operation(reader: &Reader<'_>, operation: Operation) -> Result<Self, StoreError> {
        let input = crate::operation::read_value(reader.connection(), operation.id.0, "input")?.unwrap_or(Value::Null);
        let rollback = super::op_effect::read_prior(reader.connection(), operation.id.0)?;
        Ok(Self { operation, input, rollback })
    }

    pub(crate) fn rewritten(&self, row_ids: Vec<RowId>, input: Value, rollback: Option<Value>) -> OperationTransition {
        let operation = &self.operation;
        OperationTransition::Begin(Box::new(OperationRecord {
            seq: Some(operation.id.0),
            action_key: operation.action_key.clone(),
            status: operation.status,
            row_ids,
            temp_id: operation.temp_id.clone(),
            input,
            transport_started: operation.transport_started,
            base_revision: Some(operation.base_revision),
            created_at: operation.created_at,
            dedupe_key: operation.dedupe_key.clone(),
            rollback,
            upload: None,
        }))
    }
}

pub(crate) fn read_outbox(reader: &Reader<'_>) -> Result<Vec<OutboxOperation>, StoreError> {
    let operations = crate::operation::open_operations(reader.connection())?;
    let mut entries = Vec::with_capacity(operations.len());
    for operation in operations {
        entries.push(OutboxOperation::from_operation(reader, operation)?);
    }
    Ok(entries)
}

enum Action {
    Create,
    Update,
    Destroy,
    Named(String),
    Upload(String),
}

impl Action {
    fn name(&self) -> &str {
        match self {
            Action::Create => "create",
            Action::Update => "update",
            Action::Destroy => "destroy",
            Action::Named(name) | Action::Upload(name) => name,
        }
    }
}

/// The lists a scope effect changes (scope and key text) and the ids of the rows it moves.
type EffectLists = (Vec<(ScopeRef, String)>, Vec<String>);

struct Queued {
    action: Action,
    row_id: RowId,
    temp_id: Option<RowId>,
    input: Value,
    rollback: Option<Value>,
    scope_ops: Vec<ScopeOp>,
    upload: Option<crate::commit::UploadAdmission>,
}

pub(super) fn local_scope_ops(reader: &Reader<'_>, spec: &ModelSpec, id: &RowId, row: &Map<String, Value>) -> Result<Vec<ScopeOp>, DbError> {
    let mut operations = Vec::new();
    for key in spec.list_scopes(row) {
        let first = reader.scope_first_key(spec.key.as_str(), key.as_str())?;
        let order_key = fractional_between(None, first.as_deref()).map_err(|_| crate::read::LandingError::InvalidOrderBounds)?;
        operations.push(ScopeOp::Append { scope: ScopeRef::new(spec.key.clone(), key), row: id.clone(), order_key });
    }
    Ok(operations)
}

fn created_members(operations: &[ScopeOp], owned: i64) -> Vec<PriorRow> {
    operations.iter().filter_map(|operation| match operation {
        ScopeOp::Append { scope, row, .. } => Some(PriorRow::Join { model: scope.model.to_string(), id: row.to_string(), scope_key: scope.key.to_string(), before: None, owned }),
        _ => None,
    }).collect()
}

pub(crate) fn plan_fault(error: PlanCommitError<DbError>) -> DbError {
    match error {
        PlanCommitError::Plan(error) => error,
        PlanCommitError::Store(error) => error.into(),
    }
}

fn unsent_operations(reader: &Reader<'_>, id: &RowId, destroyed: &[RowRef]) -> Result<Option<Vec<OutboxOperation>>, StoreError> {
    if !id.is_temp() {
        return Ok(None);
    }
    let doomed = destroyed.iter().map(|target| (target.model.to_string(), target.id.to_string())).collect();
    let touching = OutboxOperation::for_rows(reader, &doomed)?;
    let unsent = |entry: &OutboxOperation| entry.operation.status == OperationStatus::Pending && !entry.operation.transport_started;
    Ok((!touching.is_empty() && touching.iter().all(unsent)).then_some(touching))
}

pub(super) fn is_resident(reader: &Reader<'_>, model: &ModelKey, id: &RowId) -> Result<bool, DbError> {
    let sql = format!("SELECT 1 FROM {} WHERE id = ?1", table_name(model.as_str()));
    let found: Option<i64> = reader.connection().query_row(&sql, [id.as_str()], |row| row.get(0)).optional()?;
    Ok(found.is_some())
}

impl Db {
    pub fn create(&self, admitted: &crate::lifecycle::Admitted, model: &str, values: Map<String, Value>) -> Result<Row, DbError> {
        self.create_with(admitted, model, values, Map::new())
    }

    pub fn create_with(&self, admitted: &crate::lifecycle::Admitted, model: &str, values: Map<String, Value>, args: Map<String, Value>) -> Result<Row, DbError> {
        self.create_in(admitted, None, model, values, args)
    }

    /// Creates the row in the data generation the caller was admitted in: a commit after that generation ended is refused as stale.
    pub(crate) fn create_in(&self, admitted: &crate::lifecycle::Admitted, pinned: Option<crate::identity::Generation>, model: &str, values: Map<String, Value>, args: Map<String, Value>) -> Result<Row, DbError> {
        self.create_queued(admitted, pinned, model, values, args).map(|(row, _, _)| row)
    }

    pub(crate) fn create_queued(&self, admitted: &crate::lifecycle::Admitted, pinned: Option<crate::identity::Generation>, model: &str, values: Map<String, Value>, args: Map<String, Value>) -> Result<(Row, Option<i64>, i64), DbError> {
        self.create_claiming(admitted, pinned, model, values, args, |_| ()).map(|(row, operation, revision, ())| (row, operation, revision))
    }

    pub(crate) fn create_claiming<T>(&self, admitted: &crate::lifecycle::Admitted, pinned: Option<crate::identity::Generation>, model: &str, mut values: Map<String, Value>, args: Map<String, Value>, claim: impl FnOnce(Option<i64>) -> T) -> Result<(Row, Option<i64>, i64, T), DbError> {
        if !args.is_empty() { self.expire_blobs_before_use(admitted)?; }
        self.fill_local_sources(model, &mut values)?;
        let mut unannounced = None;
        let (id, operation, revision) = self.insert_local(admitted, model, values, args, Action::Create, (None, pinned), Some(&mut unannounced))?;
        let claimed = claim(operation);
        let row = unannounced.as_ref().and_then(|commit| commit.result().snapshot.clone()).ok_or(DbError::NotFound { model: model.to_string(), id: id.to_string() })?;
        let row = self.shared.overlay_snapshot(model, row)?;
        if let Some(unannounced) = unannounced { unannounced.publish(); }
        Ok((row, operation, revision, claimed))
    }

    pub(crate) fn create_held(&self, admitted: &crate::lifecycle::Admitted, generation: crate::identity::Generation, model: &str, action: &str, values: Map<String, Value>, input: crate::upload::UploadInput, state: UploadState) -> Result<RowId, DbError> {
        self.create_held_claiming(admitted, generation, model, action, values, input, state, |_| ()).map(|(row, _, _, ())| row.id.clone())
    }

    pub(crate) fn create_held_claiming<T>(&self, admitted: &crate::lifecycle::Admitted, generation: crate::identity::Generation, model: &str, action: &str, mut values: Map<String, Value>, input: crate::upload::UploadInput, state: UploadState, claim: impl FnOnce(i64) -> T) -> Result<(Row, i64, i64, T), DbError> {
        let id = values.get("id").and_then(read_id).unwrap_or_else(|| RowId::generate_temp(None));
        values.insert("id".into(), Value::String(id.to_string()));
        let (row, operation, revision, ticket) = self.preparing_uploads(admitted, model, generation, |files, generation, committed| {
            let item = files.prepare(model, id, uuid::Uuid::new_v4().to_string(), input, state)?;
            let (id, operation, revision) = self.insert_local(admitted, model, values, Map::new(), Action::Upload(action.to_string()), (Some(item), Some(generation)), Some(committed))?;
            let operation = operation.ok_or_else(|| DbError::Internal("upload has no operation".into()))?;
            let row = committed.as_ref().and_then(|commit| commit.result().snapshot.clone()).ok_or(DbError::NotFound { model: model.to_string(), id: id.to_string() })?;
            Ok::<_, DbError>((row, operation, revision, claim(operation)))
        })?;
        Ok((self.shared.overlay_snapshot(model, row)?, operation, revision, ticket))
    }

    pub(crate) fn declared_upload(&self, model: &str, action: &str) -> Result<String, DbError> {
        let spec = self.shared.spec(model)?;
        let declared = spec.upload_action(action).filter(|declared| declared.batch.is_none()).ok_or_else(|| DbError::InvalidUpload(format!("{model} declares no single upload action {action}")))?;
        declared.mutation.clone().ok_or_else(|| DbError::InvalidUpload(format!("{model} {action} declares no mutation")))
    }

    pub(crate) fn create_upload_batch<T>(&self, admitted: &crate::lifecycle::Admitted, generation: crate::identity::Generation, model: &str, items: Vec<crate::upload::UploadItem>, request: crate::upload::UploadBatchInput, uploads: &crate::upload::UploadOperations, claim: impl FnOnce(i64) -> T) -> Result<(Vec<(String, Row)>, i64, i64, T), DbError> {
        let spec = self.shared.spec(model)?;
        let action = spec.actions.iter().find(|action| action.name == request.operation).ok_or_else(|| DbError::InvalidUpload("unknown upload operation".into()))?;
        let batch = action.batch.as_ref().ok_or_else(|| DbError::InvalidUpload("operation has no upload batch declaration".into()))?;
        if items.len() < batch.min || items.len() > batch.max {
            return Err(DbError::InvalidUpload(format!("upload batch requires {}..{} items", batch.min, batch.max)));
        }
        let steps = uploads.steps(action.mutation.as_deref().ok_or_else(|| DbError::InvalidUpload("upload mutation is missing".into()))?);
        let field = &action.correlate[0];
        let mut keys = HashSet::new();
        let now = self.shared.store.lock().now_ms();
        let mut rows = Vec::new();
        let mut files = Vec::new();
        for item in items {
            let key = item.values.get(field).and_then(Value::as_str).filter(|key| !key.trim().is_empty()).ok_or_else(|| DbError::InvalidUpload("upload correlation key is empty".into()))?.to_owned();
            if !keys.insert(key.clone()) { return Err(DbError::InvalidUpload("upload correlation keys must be unique".into())); }
            let id = RowId::generate_temp(None);
            let mut values = item.values;
            values.insert("id".into(), Value::String(id.to_string()));
            reject_unknown_fields(spec, model, &values)?;
            reject_invalid_values(spec, model, &values)?;
            self.shared.store.decode_values(model, &mut values, |source, key| source.get(key))?;
            let data = spec.normalize_local(&values, now)?;
            let mut variables = item.variables;
            for (target, source) in &batch.input {
                variables.insert(target.clone(), data.get(source).cloned().unwrap_or(Value::Null));
            }
            files.push((id.clone(), key, crate::upload::UploadInput { path: item.path, name: item.name, mime: item.mime, variables, steps: steps.clone() }));
            rows.push((id, data));
        }
        let ids: Vec<_> = rows.iter().map(|(id, _)| id.clone()).collect();
        let record = OperationRecord {
            seq: None, action_key: Operation::upload_action_key(model, &action.name), status: OperationStatus::Pending,
            row_ids: ids.clone(), temp_id: None, input: Value::Object(request.variables), transport_started: false,
            base_revision: Some(self.revision_seq()?), created_at: now, dedupe_key: None, upload: None,
            rollback: Some(Prior { rows: ids.iter().map(|id| PriorRow::Created { model: model.into(), id: id.to_string() }).collect() }.to_value()),
        };
        let (snapshots, operation, revision, ticket) = self.preparing_uploads(admitted, model, generation, |prepared, generation, pending| {
            let files = files.into_iter().map(|(row, key, input)| prepared.prepare(model, row, key, input, UploadState::queued())).collect::<Result<Vec<_>, _>>()?;
            let (committed, _) = self.commit_following_deferred(&admitted.authority(), generation, |reader| {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                for (id, data) in &rows {
                    envelope.scope_ops.extend(local_scope_ops(reader, spec, id, data)?);
                    envelope.entity_ops.push(EntityOp::Upsert { model: spec.key.clone(), id: id.clone(), row: data.clone() });
                }
                envelope.item_snapshots = files.iter().map(|item| (item.client_key.clone(), RowRef::new(spec.key.clone(), item.row.clone()))).collect();
                let mut record = record.clone();
                let mut prior: Prior = serde_json::from_value(record.rollback.take().unwrap_or(Value::Null)).map_err(StoreError::from)?;
                prior.rows.extend(created_members(&envelope.scope_ops, reader.revision_seq()? + 1));
                record.rollback = Some(prior.to_value());
                envelope.operation_transitions.push(OperationTransition::BeginUpload { record: Box::new(record), items: files.clone() });
                Ok(Some(envelope))
            }).map_err(plan_fault)?;
            let result = committed.result();
            let operation = result.operations.first().copied().ok_or_else(|| DbError::Internal("upload batch has no operation".into()))?;
            let revision = result.revision.ok_or_else(|| DbError::Internal("upload batch has no revision".into()))?;
            let snapshots = result.item_snapshots.iter().map(|(item, row)| row.clone().map(|row| (item.clone(), row)).ok_or_else(|| DbError::Internal("upload batch item is missing".into()))).collect::<Result<Vec<_>, _>>()?;
            let ticket = claim(operation);
            *pending = Some(committed);
            Ok::<_, DbError>((snapshots, operation, revision, ticket))
        })?;
        let snapshots = snapshots.into_iter().map(|(item, row)| self.shared.overlay_snapshot(model, row).map(|row| (item, row))).collect::<Result<Vec<_>, _>>()?;
        Ok((snapshots, operation, revision, ticket))
    }

    fn insert_local<'a>(&'a self, admitted: &crate::lifecycle::Admitted, model: &str, values: Map<String, Value>, args: Map<String, Value>, action: Action, held: (Option<crate::commit::UploadAdmission>, Option<crate::identity::Generation>), pending: Option<&mut Option<crate::store::Committed<'a>>>) -> Result<(RowId, Option<i64>, i64), DbError> {
        let spec = self.shared.spec(model)?;
        reject_unknown_fields(spec, model, &values)?;
        let mut input = values;
        let id = match input.get("id").and_then(read_id) {
            Some(id) => id,
            None if spec.is_local() => RowId::generate(),
            None => RowId::generate_temp(None),
        };
        input.insert("id".to_string(), Value::String(id.to_string()));
        reject_invalid_values(spec, model, &input)?;
        self.shared.store.decode_values(model, &mut input, |source, key| source.get(key))?;
        let now_ms = self.shared.store.lock().now_ms();
        let data = spec.normalize_local(&input, now_ms)?;
        let key = ModelKey::new(model);
        let (item, generation) = held;
        let queued_input = match &item {
            Some(item) => Value::Object(item.input.variables.clone()),
            None => Value::Object(data.iter().chain(&args).map(|(name, value)| (name.clone(), value.clone())).collect()),
        };
        let upload = item;
        let mut queued = Queued {
            action,
            row_id: id.clone(),
            temp_id: id.is_temp().then(|| id.clone()),
            input: queued_input,
            rollback: None,
            scope_ops: Vec::new(),
            upload,
        };
        let committed = self.commit_local_optional(admitted, spec, &mut queued, generation, pending, |reader, plan, queued| {
            if reader.point(model, id.as_str())?.is_some() || !read_sync(reader.connection(), model, &[id.to_string()])?.is_empty() {
                return Err(DbError::AlreadyExists { model: model.to_string(), id: id.to_string() });
            }
            queued.scope_ops = local_scope_ops(reader, spec, &id, &data)?;
            plan.upsert(key.clone(), vec![data.clone()]);
            let mut rows = vec![PriorRow::Created { model: model.to_string(), id: id.to_string() }];
            rows.extend(created_members(&queued.scope_ops, reader.revision_seq()? + 1));
            queued.rollback = Some(Prior { rows }.to_value());
            Ok(Some(self.touch_ops(reader, spec, &[&data], &RowRef::new(key.clone(), id.clone()))?))
        })?;
        let operation = committed.operations.first().copied();
        Ok((id, operation, committed.revision.ok_or_else(|| DbError::Internal("created row has no revision".into()))?))
    }

    pub fn update(&self, admitted: &crate::lifecycle::Admitted, model: &str, id: &RowId, patch: Map<String, Value>) -> Result<Row, DbError> {
        self.update_with(admitted, model, id, patch, Map::new())
    }

    pub fn update_with(&self, admitted: &crate::lifecycle::Admitted, model: &str, id: &RowId, patch: Map<String, Value>, args: Map<String, Value>) -> Result<Row, DbError> {
        self.update_queued(admitted, model, id, patch, args).map(|(row, _, _)| row)
    }

    pub(crate) fn update_queued(&self, admitted: &crate::lifecycle::Admitted, model: &str, id: &RowId, patch: Map<String, Value>, args: Map<String, Value>) -> Result<(Row, Option<i64>, i64), DbError> {
        self.update_claiming(admitted, model, id, patch, args, |_| ()).map(|(row, operation, revision, ())| (row, operation, revision))
    }

    pub(crate) fn update_claiming<T>(&self, admitted: &crate::lifecycle::Admitted, model: &str, id: &RowId, patch: Map<String, Value>, args: Map<String, Value>, claim: impl FnOnce(Option<i64>) -> T) -> Result<(Row, Option<i64>, i64, T), DbError> {
        if !args.is_empty() { self.expire_blobs_before_use(admitted)?; }
        let spec = self.shared.spec(model)?;
        reject_unknown_fields(spec, model, &patch)?;
        reject_invalid_values(spec, model, &patch)?;
        let mut patch = spec.patch(&patch);
        self.shared.store.decode_values(model, &mut patch, |source, key| source.get(key))?;
        patch.remove("id");
        let key = ModelKey::new(model);
        let own = RowRef::new(key.clone(), id.clone());
        let mut input = patch.clone();
        input.insert("id".to_string(), Value::String(id.to_string()));
        let sends_args = !args.is_empty();
        input.extend(args);
        let mut queued = Queued { action: Action::Update, row_id: id.clone(), temp_id: None, input: Value::Object(input), rollback: None, scope_ops: Vec::new(), upload: None };
        let mut unannounced = None;
        let committed = self.commit_local_optional(admitted, spec, &mut queued, None, Some(&mut unannounced), |reader, plan, queued| {
            let current = reader.point(model, id.as_str())?.ok_or_else(|| DbError::NotFound { model: model.to_string(), id: id.to_string() })?;
            let mut merged = current.clone();
            merged.extend(patch.clone());
            if same_fields(&merged, &current) && !sends_args {
                return Ok(None);
            }
            queued.rollback = Some(Prior { rows: vec![PriorRow::edited(reader, model, id.as_str(), &current, &merged)?] }.to_value());
            plan.update(key.clone(), id.clone(), merged.clone());
            Ok(Some(self.touch_ops(reader, spec, &[&merged, &current], &own)?))
        })?;
        if committed.is_noop() {
            {
                let life = &self.shared.store.lifecycle;
                let _line = life.commit_line();
                if !std::sync::Arc::ptr_eq(life, admitted.life()) || !admitted.is_current() { return Err(DbError::NoSession { detail: "the session of this write ended".into() }); }
                self.shared.ephemeral.lock().track(model, id.as_str(), i64::MAX);
            }
        }
        let operation = committed.operations.first().copied();
        let claimed = claim(operation);
        let row = match committed.snapshot.clone() {
            Some(row) => self.shared.overlay_snapshot(model, row)?,
            None => self.shared.read(&[model], |reader| self.shared.load(reader, model, &[id.to_string()]))?.remove(id.as_str()).ok_or(DbError::NotFound { model: model.to_string(), id: id.to_string() })?,
        };
        if let Some(unannounced) = unannounced { unannounced.publish(); }
        let revision = match committed.revision { Some(revision) => revision, None => self.revision_seq()? };
        Ok((row, operation, revision, claimed))
    }

    pub fn act(&self, admitted: &crate::lifecycle::Admitted, model: &str, action: &str, id: &RowId, patch: Map<String, Value>, args: Map<String, Value>) -> Result<Acted, DbError> {
        self.act_claiming(admitted, model, action, id, patch, args, |_| ()).map(|(acted, ())| acted)
    }

    pub fn act_claiming<T>(&self, admitted: &crate::lifecycle::Admitted, model: &str, action: &str, id: &RowId, patch: Map<String, Value>, args: Map<String, Value>, claim: impl FnOnce(i64) -> T) -> Result<(Acted, T), DbError> {
        self.expire_blobs_before_use(admitted)?;
        let spec = self.shared.spec(model)?;
        let mut patch = patch;
        crate::schema::keyed::resolve_maps(&mut patch, &args);
        let keyed = crate::schema::keyed::take(&mut patch);
        let flagged = crate::schema::keyed::take_flags(&mut patch);
        reject_unknown_fields(spec, model, &patch)?;
        reject_invalid_values(spec, model, &patch)?;
        let mut patch = spec.patch(&patch);
        self.shared.store.decode_values(model, &mut patch, |source, key| source.get(key))?;
        let key = ModelKey::new(model);
        let own = RowRef::new(key.clone(), id.clone());
        let input_args = args.clone();
        let mut input = args;
        input.insert("id".to_string(), Value::String(id.to_string()));
        let declared = spec.actions.iter().find(|declared| declared.name == action).cloned();
        let now_ms = self.shared.store.lock().now_ms();
        let receiver_row = self.find(model, id)?;
        let mut queued = Queued { action: Action::Named(action.to_string()), row_id: id.clone(), temp_id: None, input: Value::Object(input), rollback: None, scope_ops: Vec::new(), upload: None };
        let mut unannounced = None;
        let committed = self.commit_local_optional(admitted, spec, &mut queued, None, Some(&mut unannounced), |reader, plan, queued| {
            let current = reader.point(model, id.as_str())?.ok_or_else(|| DbError::NotFound { model: model.to_string(), id: id.to_string() })?;
            let mut merged = current.clone();
            merged.extend(patch.clone());
            for (field, op) in &keyed {
                let Some(declared) = spec.field(field) else { continue };
                let target_row = |model: &str, id: &str| Ok::<_, DbError>(reader.point(model, id)?);
                if let Some(next) = crate::schema::keyed::apply(declared, current.get(field), op, &input_args, target_row)? {
                    merged.insert(field.clone(), next);
                }
            }
            let mut edits = Edits::of_owner(model, id, current, merged);
            self.apply_flagged(reader, &mut edits, &Call { owner: &own, args: &input_args }, &flagged)?;
            let mut rows = Vec::new();
            let mut touches = Vec::new();
            let removes = declared.as_ref().is_some_and(|declared| declared.removes_receiver);
            for ((edited_model, edited_id), (before, after)) in edits.into_rows() {
                let is_receiver = edited_model == model && edited_id == id.as_str();
                if (!is_receiver && before == after) || (is_receiver && removes) {
                    continue;
                }
                let edited_key = ModelKey::new(&edited_model);
                let edited_row = RowId::new(&edited_id)?;
                rows.push(PriorRow::edited(reader, &edited_model, &edited_id, &before, &after)?);
                touches.extend(self.touch_ops(reader, self.shared.spec(&edited_model)?, &[&after, &before], &RowRef::new(edited_key.clone(), edited_row.clone()))?);
                plan.update(edited_key, edited_row, after);
            }
            if let Some(declared) = &declared {
                self.apply_closed_effect(reader, spec, declared, (&own, &input_args), now_ms, plan, &mut rows, &mut touches, queued)?;
            }
            queued.rollback = Some(Prior { rows }.to_value());
            Ok(Some(touches))
        })?;
        let operation = committed.operations.first().copied().ok_or_else(|| DbError::NotFound { model: model.to_string(), id: id.to_string() })?;
        let claimed = claim(operation);
        let row = match committed.snapshot.clone() {
            Some(row) => self.shared.overlay_snapshot(model, row)?,
            None if declared.as_ref().is_some_and(|declared| declared.removes_receiver) => receiver_row.ok_or(DbError::NotFound { model: model.to_string(), id: id.to_string() })?,
            None => return Err(DbError::NotFound { model: model.to_string(), id: id.to_string() }),
        };
        if let Some(unannounced) = unannounced { unannounced.publish(); }
        let revision = committed.revision.ok_or_else(|| DbError::Internal("action has no revision".into()))?;
        Ok((Acted { row, operation, revision }, claimed))
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_closed_effect(
        &self,
        reader: &Reader<'_>,
        spec: &ModelSpec,
        declared: &crate::schema::ActionDeclaration,
        (own, args): (&RowRef, &Map<String, Value>),
        now_ms: i64,
        plan: &mut WritePlan,
        rows: &mut Vec<PriorRow>,
        touches: &mut Vec<EntityOp>,
        queued: &mut Queued,
    ) -> Result<(), DbError> {
        if declared.removes_receiver {
            self.remove_receiver(reader, own, rows, touches)?;
        }
        if let Some(copy) = &declared.copies_receiver_to {
            self.copy_receiver(reader, spec, copy, (own, args), now_ms, plan, rows, queued)?;
        }
        for effect in &declared.scope_effects {
            plan_scope_effect(reader, self.shared.spec(&effect.model)?, effect, (own, args), rows, &mut queued.scope_ops)?;
        }
        Ok(())
    }

    fn remove_receiver(&self, reader: &Reader<'_>, own: &RowRef, rows: &mut Vec<PriorRow>, touches: &mut Vec<EntityOp>) -> Result<(), DbError> {
        let destroyed = self.destroy_targets(reader, own)?;
        for target in &destroyed {
            rows.extend(PriorRow::destroyed(reader, target.model.as_str(), target.id.as_str())?);
            touches.push(EntityOp::Destroy { model: target.model.clone(), id: target.id.clone(), reason: DestroyReason::Local });
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn copy_receiver(
        &self,
        reader: &Reader<'_>,
        spec: &ModelSpec,
        copy: &crate::schema::ReceiverCopy,
        (own, args): (&RowRef, &Map<String, Value>),
        now_ms: i64,
        plan: &mut WritePlan,
        rows: &mut Vec<PriorRow>,
        queued: &mut Queued,
    ) -> Result<(), DbError> {
        let target = args.get(&copy.arg).cloned().ok_or_else(|| DbError::Internal(format!("the argument {} of the copy is missing", copy.arg)))?;
        let mut values = reader.point(own.model.as_str(), own.id.as_str())?.ok_or_else(|| DbError::NotFound { model: own.model.to_string(), id: own.id.to_string() })?;
        let copy_id = RowId::generate_temp(None);
        values.insert("id".to_string(), Value::String(copy_id.to_string()));
        values.insert(copy.field.clone(), target);
        let data = spec.normalize_local(&values, now_ms)?;
        queued.scope_ops.extend(local_scope_ops(reader, spec, &copy_id, &data)?);
        plan.upsert(own.model.clone(), vec![data]);
        rows.push(PriorRow::Created { model: own.model.to_string(), id: copy_id.to_string() });
        Ok(())
    }

}

fn effect_lists(target: &ModelSpec, effect: &crate::schema::ScopeEffect, (own, args): (&RowRef, &Map<String, Value>)) -> Result<EffectLists, DbError> {
        let mut ids: Vec<String> = match args.get(&effect.rows) {
            Some(Value::Array(items)) => items.iter().filter_map(|item| item.as_str().map(str::to_string)).collect(),
            Some(Value::String(one)) => vec![one.clone()],
            _ => return Err(DbError::Internal(format!("the argument {} of the scope effect is missing", effect.rows))),
        };
        let mut seen = HashSet::new();
        ids.retain(|id| seen.insert(id.clone()));
        let mut lists = Vec::new();
        for order in &effect.orders {
            let mut params = Map::new();
            for (name, source) in &effect.with {
                let value = if source == "id" { Value::String(own.id.to_string()) } else { args.get(source).cloned().ok_or_else(|| DbError::Internal(format!("the argument {source} of the scope effect is missing")))? };
                params.insert(name.clone(), value);
            }
            params.insert(effect.order_param.clone(), Value::String(order.clone()));
            let key = target.selection_key(&effect.scope, &params)?;
            lists.push((ScopeRef::new(ModelKey::new(&effect.model), key.clone()), key.to_string()));
        }
        Ok((lists, ids))
    }

pub(super) fn plan_scope_effect(reader: &Reader<'_>, target: &ModelSpec, effect: &crate::schema::ScopeEffect, call: (&RowRef, &Map<String, Value>), rows: &mut Vec<PriorRow>, scope_ops: &mut Vec<ScopeOp>) -> Result<(), DbError> {
        let (lists, ids) = effect_lists(target, effect, call)?;
        let owned = reader.revision_seq()? + 1;
        for (scope, key) in lists {
            let mut last = reader.scope_last_key(&effect.model, &key)?;
            for id in &ids {
                let before = reader.scope_member_key(&effect.model, &key, id)?;
                match effect.kind {
                    crate::schema::ScopeEffectKind::Join => {
                        if before.is_none() && reader.point(&effect.model, id)?.is_some() {
                            let order_key = fractional_between(last.as_deref(), None).map_err(|_| crate::read::LandingError::InvalidOrderBounds)?;
                            last = Some(order_key.clone());
                            scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: RowId::new(id)?, order_key });
                        }
                        rows.push(PriorRow::Join { model: effect.model.clone(), id: id.clone(), scope_key: key.clone(), before, owned });
                    }
                    crate::schema::ScopeEffectKind::Leave => {
                        if before.is_some() { scope_ops.push(ScopeOp::Detach { scope: scope.clone(), row: RowId::new(id)? }); }
                        rows.push(PriorRow::Leave { model: effect.model.clone(), id: id.clone(), scope_key: key.clone(), before, owned });
                    }
                }
            }
        }
        Ok(())
}

impl Db {
    pub fn destroy(&self, admitted: &crate::lifecycle::Admitted, model: &str, id: &RowId) -> Result<Vec<RowRef>, DbError> {
        self.destroy_claiming(admitted, model, id, |_| ()).map(|(destroyed, _, _, ())| destroyed)
    }

    pub(crate) fn destroy_claiming<T>(&self, admitted: &crate::lifecycle::Admitted, model: &str, id: &RowId, claim: impl FnOnce(Option<i64>) -> T) -> Result<(Vec<RowRef>, Option<i64>, i64, T), DbError> {
        let spec = self.shared.spec(model)?;
        let own = RowRef::new(ModelKey::new(model), id.clone());
        let generation = self.generation();
        let created_at = self.shared.store.lock().now_ms();
        let mut destroyed: Vec<RowRef> = Vec::new();
        let mut discarded: Vec<String> = Vec::new();
        let mut will_send = false;
        let (committed, _) = self.commit_following_deferred(&admitted.authority(), generation, |reader| {
                will_send = false;
                if reader.point(model, id.as_str())?.is_none() {
                    return Err(DbError::NotFound { model: model.to_string(), id: id.to_string() });
                }
                destroyed = self.destroy_targets(reader, &own)?;
                if let Some(unsent) = unsent_operations(reader, id, &destroyed)? {
                    let mut envelope = cascade_envelope(&destroyed, DestroyReason::Expired, WriteOrigin::Optimistic, generation);
                    envelope.operation_transitions.extend(unsent.iter().map(|entry| OperationTransition::Remove { seq: entry.operation.id.0, expected: None }));
                    for target in &destroyed {
                        discarded.extend(references_of_row(reader.connection(), target.model.as_str(), target.id.as_str())?.into_iter().map(|file| file.id));
                    }
                    return Ok(Some(envelope));
                }
                let mut rows = Vec::new();
                for target in &destroyed {
                    rows.extend(PriorRow::destroyed(reader, target.model.as_str(), target.id.as_str())?);
                }
                let mut envelope = cascade_envelope(&destroyed, DestroyReason::Local, WriteOrigin::Optimistic, generation);
                let queued = Queued {
                    action: Action::Destroy,
                    row_id: id.clone(),
                    temp_id: None,
                    input: json!({"id": id.as_str()}),
                    rollback: Some(Prior { rows }.to_value()),
                    scope_ops: Vec::new(),
                    upload: None,
                };
                if !spec.is_local() {
                    envelope.operation_transitions.push(self.begin(reader, spec, &queued, created_at)?);
                    will_send = true;
                }
                Ok(Some(envelope))
            })
            .map_err(plan_fault)?;
        let result = committed.result();
        let operation = if will_send { result.operations.first().copied() } else { None };
        let revision = result.revision.ok_or_else(|| DbError::Internal("destroy has no revision".into()))?;
        let claimed = claim(operation);
        committed.publish();
        self.shared.files.discard(&discarded).map_err(super::files::io_fault)?;
        Ok((destroyed, operation, revision, claimed))
    }

    /// The version of the outbox: it grows on every commit that touches an operation and on every reset.
    pub fn outbox_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.shared.outbox_changes.subscribe()
    }

    pub fn outbox(&self) -> Result<Vec<OutboxOperation>, DbError> {
        let entries = self.shared.read(&[], |reader| Ok(read_outbox(reader)?))?;
        let mut counters = super::open::locked(&self.shared.counters);
        counters.outbox_reads += 1;
        counters.outbox_rows_loaded += entries.len();
        Ok(entries)
    }

    pub(crate) fn operation_queue(&self) -> Result<std::sync::Arc<std::sync::Mutex<crate::operation::OperationQueue>>, DbError> {
        let stored = self.shared.store.lock();
        let entries = crate::operation::read_queue(&stored.connection).map_err(StoreError::from)?;
        let queue = self.shared.operation_queues.subscribe(&entries);
        let mut counters = super::open::locked(&self.shared.counters);
        counters.outbox_reads += 1;
        counters.outbox_rows_loaded += entries.len();
        Ok(queue)
    }

    fn commit_local_optional<'a>(
        &'a self,
        admitted: &crate::lifecycle::Admitted,
        spec: &ModelSpec,
        queued: &mut Queued,
        generation: Option<crate::identity::Generation>,
        pending: Option<&mut Option<crate::store::Committed<'a>>>,
        mut build: impl FnMut(&Reader<'_>, &mut WritePlan, &mut Queued) -> Result<Option<Vec<EntityOp>>, DbError>,
    ) -> Result<CommitResult, DbError> {
        let generation = generation.unwrap_or_else(|| self.generation());
        let created_at = self.shared.store.lock().now_ms();
        let (result, _) = self.commit_following_deferred(&admitted.authority(), generation, |reader| {
                queued.scope_ops.clear();
                let mut plan = WritePlan::new();
                let Some(touches) = build(reader, &mut plan, queued)? else { return Ok(None) };
                let mut envelope: CommitEnvelope = plan.envelope(WriteOrigin::Optimistic, generation);
                envelope.entity_ops.extend(touches);
                envelope.scope_ops.extend(std::mem::take(&mut queued.scope_ops));
                let snapshot_id = queued.rollback.as_ref().and_then(|value| serde_json::from_value::<Prior>(value.clone()).ok()).and_then(|prior| prior.rows.into_iter().find_map(|row| match row {
                    PriorRow::Created { model, id } if model == spec.key.as_str() => RowId::new(&id).ok(),
                    _ => None,
                })).unwrap_or_else(|| queued.row_id.clone());
                envelope.snapshot = Some(RowRef::new(spec.key.clone(), snapshot_id));
                if !spec.is_local() {
                    envelope.operation_transitions.push(self.begin(reader, spec, queued, created_at)?);
                }
                Ok(Some(envelope))
            })
            .map_err(plan_fault)?;
        if let Some(pending) = pending {
            let snapshot = result.result().clone();
            *pending = Some(result);
            Ok(snapshot)
        } else {
            Ok(result.publish())
        }
    }

    fn begin(&self, reader: &Reader<'_>, spec: &ModelSpec, queued: &Queued, created_at: i64) -> Result<OperationTransition, DbError> {
        let record = OperationRecord {
            seq: None,
            action_key: match &queued.action {
                Action::Upload(action) => Operation::upload_action_key(spec.key.as_str(), action),
                other => Operation::action_key(spec.key.as_str(), other.name()),
            },
            status: OperationStatus::Pending,
            row_ids: vec![queued.row_id.clone()],
            temp_id: queued.temp_id.clone(),
            input: queued.input.clone(),
            transport_started: false,
            base_revision: Some(reader.revision_seq()?),
            created_at,
            dedupe_key: queued.upload.as_ref().map(|_| format!("{}:{}:upload", spec.key, queued.row_id)),
            rollback: queued.rollback.clone(),
            upload: queued.upload.as_ref().map(|item| item.state.clone()),
        };
        Ok(match &queued.upload {
            Some(item) => OperationTransition::BeginUpload { record: Box::new(record), items: vec![item.clone()] },
            None => OperationTransition::Begin(Box::new(record)),
        })
    }

    pub(crate) fn touch_ops(&self, reader: &Reader<'_>, spec: &ModelSpec, rows: &[&Map<String, Value>], own: &RowRef) -> Result<Vec<EntityOp>, DbError> {
        let mut seen = HashSet::from([own.clone()]);
        let mut ops = Vec::new();
        for association in spec.associations.iter().filter(|association| association.kind == AssociationKind::BelongsTo && association.touches_parent) {
            for row in rows {
                let Some(parent_id) = row.get(&association.foreign_key).and_then(read_id) else { continue };
                let parent = RowRef::new(association.target.clone(), parent_id);
                if seen.contains(&parent) || !is_resident(reader, &parent.model, &parent.id)? {
                    continue;
                }
                seen.insert(parent.clone());
                ops.push(EntityOp::Touch { model: parent.model, id: parent.id });
            }
        }
        Ok(ops)
    }

    pub(crate) fn destroy_targets(&self, reader: &Reader<'_>, own: &RowRef) -> Result<Vec<RowRef>, DbError> {
        let mut descendants = Vec::new();
        self.descendants(reader, own, &mut HashSet::from([own.clone()]), &mut descendants)?;
        Ok(cascade_destroys(own, &descendants, true))
    }

    fn descendants(&self, reader: &Reader<'_>, parent: &RowRef, seen: &mut HashSet<RowRef>, found: &mut Vec<RowRef>) -> Result<(), DbError> {
        let spec = self.shared.spec(parent.model.as_str())?;
        for association in spec.associations.iter().filter(|association| association.owns_children() && association.dependent == Some(Dependent::Destroy)) {
            let query = Query::new(association.target.as_str()).filtered(Where::equal(&association.foreign_key, json!(parent.id.as_str())));
            for (child_id, _) in self.shared.query_keys(reader, &query)? {
                let child = RowRef::new(association.target.clone(), RowId::new(&child_id)?);
                if seen.insert(child.clone()) {
                    self.descendants(reader, &child, seen, found)?;
                    found.push(child);
                }
            }
        }
        Ok(())
    }
}

pub(super) fn reject_unknown_fields(spec: &ModelSpec, model: &str, values: &Map<String, Value>) -> Result<(), DbError> {
    match spec.fault(values, false) {
        Some(fault) => Err(fault_error(model, fault)),
        None => Ok(()),
    }
}

pub(super) fn reject_invalid_values(spec: &ModelSpec, model: &str, values: &Map<String, Value>) -> Result<(), DbError> {
    match spec.fault(values, true) {
        Some(fault) => Err(fault_error(model, fault)),
        None => Ok(()),
    }
}

pub(crate) fn fault_error(model: &str, fault: ValueFault) -> DbError {
    match fault {
        ValueFault::Unknown(field) => DbError::UnknownField { model: model.to_string(), field },
        ValueFault::Invalid(field) => SchemaError::InvalidValue(field).into(),
    }
}
