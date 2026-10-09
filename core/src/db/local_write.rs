use std::collections::HashSet;

use rusqlite::OptionalExtension;
use serde_json::{json, Map, Value};

use crate::commit::{
    CommitEnvelope, DestroyReason, EntityOp, OperationRecord, OperationStatus, OperationTransition, ScopeOp, ScopeRef, WriteOrigin,
};
use crate::ddl::table_name;
use crate::files::references_of_row;
use crate::identity::{ModelKey, RowId, RowRef};
use crate::schema::{FieldKind, RelationSpec, VariableSource};
use crate::value::{fractional_between, normalize_relation_params, relation_scope_key, ScalarKind};
use crate::operation::Operation;
use crate::query::{Query, Where};
use crate::schema::{AssociationKind, Dependent, ModelSpec, RootShape};
use crate::store::{PlanCommitError, Reader, StoreError};
use crate::syncstate::{operation_keys, read_sync};
use crate::value::read_id;
use crate::write::{
    cascade_destroys, cascade_envelope, cascades, touches_parent, WritePlan,
};

use super::error::DbError;
use super::open::Db;
use super::prior::{Prior, PriorRow};
use super::row::Row;

#[derive(Debug, Clone, PartialEq)]
pub struct OutboxOperation {
    pub operation: Operation,
    pub input: Value,
    pub rollback: Option<Value>,
}

impl OutboxOperation {
    pub(crate) fn rewritten(&self, row_ids: Vec<RowId>, input: Value, rollback: Option<Value>) -> OperationTransition {
        let operation = &self.operation;
        OperationTransition::Begin(OperationRecord {
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
        })
    }
}

pub(crate) fn read_outbox(reader: &Reader<'_>) -> Result<Vec<OutboxOperation>, StoreError> {
    let operations = crate::operation::open_operations(reader.connection())?;
    let mut statement = reader.connection().prepare("SELECT input, rollback FROM op WHERE seq = ?1")?;
    let mut entries = Vec::with_capacity(operations.len());
    for operation in operations {
        let (input, rollback): (Vec<u8>, Option<Vec<u8>>) = statement.query_row([operation.id.0], |row| Ok((row.get(0)?, row.get(1)?)))?;
        entries.push(OutboxOperation {
            operation,
            input: serde_json::from_slice(&input)?,
            rollback: rollback.map(|bytes| serde_json::from_slice(&bytes)).transpose()?,
        });
    }
    Ok(entries)
}

enum Action {
    Create,
    Update,
    Destroy,
    Named(String),
}

impl Action {
    fn name(&self) -> &str {
        match self {
            Action::Create => "create",
            Action::Update => "update",
            Action::Destroy => "destroy",
            Action::Named(name) => name,
        }
    }
}

struct Queued {
    action: Action,
    row_id: RowId,
    temp_id: Option<RowId>,
    input: Value,
    rollback: Option<Value>,
    scope_ops: Vec<ScopeOp>,
}

fn lists_every_row_of_its_params(relation: &RelationSpec) -> bool {
    let Some(remote) = &relation.remote else { return false };
    !relation.by_columns.is_empty()
        && relation.text_params.is_empty()
        && !remote.variables.values().any(|source| matches!(source, VariableSource::Constant { .. }))
}

fn local_scope_ops(reader: &Reader<'_>, spec: &ModelSpec, id: &RowId, row: &Map<String, Value>) -> Result<Vec<ScopeOp>, DbError> {
    let mut operations = Vec::new();
    for relation in spec.relations.iter().filter(|relation| lists_every_row_of_its_params(relation)) {
        let mut params = Map::new();
        let mut kinds = std::collections::BTreeMap::new();
        for column in &relation.by_columns {
            let kind = match spec.field(column).map(|field| &field.kind) {
                Some(FieldKind::Scalar { kind }) => *kind,
                Some(FieldKind::Enum { .. }) => ScalarKind::Str,
                _ => break,
            };
            let Some(value) = row.get(column) else { break };
            params.insert(column.clone(), value.clone());
            kinds.insert(column.clone(), kind);
        }
        if params.len() != relation.by_columns.len() {
            continue;
        }
        let Ok(normalized) = normalize_relation_params(&params, &kinds) else { continue };
        let Ok(key) = relation_scope_key(spec.key.as_str(), &relation.name, &normalized) else { continue };
        let first = reader.scope_members(spec.key.as_str(), key.as_str())?.into_iter().map(|(_, order_key)| order_key).min();
        let order_key = fractional_between(None, first.as_deref()).map_err(|_| crate::read::LandingError::InvalidOrderBounds)?;
        operations.push(ScopeOp::Append { scope: ScopeRef::new(spec.key.clone(), key), row: id.clone(), order_key });
    }
    Ok(operations)
}

pub(super) fn plan_fault(error: PlanCommitError<DbError>) -> DbError {
    match error {
        PlanCommitError::Plan(error) => error,
        PlanCommitError::Store(error) => error.into(),
    }
}

fn unsent_operations(reader: &Reader<'_>, id: &RowId, destroyed: &[RowRef]) -> Result<Option<Vec<OutboxOperation>>, StoreError> {
    if !id.is_temp() {
        return Ok(None);
    }
    let doomed: HashSet<(String, String)> = destroyed.iter().map(|target| (target.model.to_string(), target.id.to_string())).collect();
    let touching: Vec<OutboxOperation> = read_outbox(reader)?
        .into_iter()
        .filter(|entry| operation_keys(&entry.operation.action_key, &entry.operation.row_ids, entry.operation.temp_id.as_ref()).iter().any(|key| doomed.contains(key)))
        .collect();
    let unsent = |entry: &OutboxOperation| entry.operation.status == OperationStatus::Pending && !entry.operation.transport_started;
    Ok((!touching.is_empty() && touching.iter().all(unsent)).then_some(touching))
}

fn is_resident(reader: &Reader<'_>, model: &ModelKey, id: &RowId) -> Result<bool, DbError> {
    let sql = format!("SELECT 1 FROM {} WHERE id = ?1", table_name(model.as_str()));
    let found: Option<i64> = reader.connection().query_row(&sql, [id.as_str()], |row| row.get(0)).optional()?;
    Ok(found.is_some())
}

impl Db {
    pub fn create(&self, model: &str, values: Map<String, Value>) -> Result<Row, DbError> {
        let spec = self.shared.spec(model)?;
        let mut input = values;
        let id = match input.get("id").and_then(read_id) {
            Some(id) => id,
            None => RowId::generate_temp(None),
        };
        input.insert("id".to_string(), Value::String(id.to_string()));
        let now_ms = self.shared.store.lock().now_ms();
        let data = spec.normalize_local(&input, now_ms)?;
        let key = ModelKey::new(model);
        let mut queued = Queued {
            action: Action::Create,
            row_id: id.clone(),
            temp_id: id.is_temp().then(|| id.clone()),
            input: Value::Object(data.clone()),
            rollback: None,
            scope_ops: Vec::new(),
        };
        self.commit_local_optional(spec, &mut queued, |reader, plan, queued| {
            if reader.point(model, id.as_str())?.is_some() || !read_sync(reader.connection(), model, &[id.to_string()])?.is_empty() {
                return Err(DbError::AlreadyExists { model: model.to_string(), id: id.to_string() });
            }
            queued.rollback = Some(Prior { rows: vec![PriorRow::Created { model: model.to_string(), id: id.to_string() }] }.to_value());
            queued.scope_ops = local_scope_ops(reader, spec, &id, &data)?;
            plan.upsert(key.clone(), vec![data.clone()]);
            Ok(Some(self.touch_ops(reader, spec, &[&data], &RowRef::new(key.clone(), id.clone()))?))
        })?;
        self.find(model, &id)?.ok_or(DbError::NotFound { model: model.to_string(), id: id.to_string() })
    }

    pub fn update(&self, model: &str, id: &RowId, patch: Map<String, Value>) -> Result<Row, DbError> {
        let spec = self.shared.spec(model)?;
        let mut patch = spec.patch(&patch);
        patch.remove("id");
        let key = ModelKey::new(model);
        let own = RowRef::new(key.clone(), id.clone());
        let mut input = patch.clone();
        input.insert("id".to_string(), Value::String(id.to_string()));
        let mut queued = Queued { action: Action::Update, row_id: id.clone(), temp_id: None, input: Value::Object(input), rollback: None, scope_ops: Vec::new() };
        let committed = self.commit_local_optional(spec, &mut queued, |reader, plan, queued| {
            let current = reader.point(model, id.as_str())?.ok_or_else(|| DbError::NotFound { model: model.to_string(), id: id.to_string() })?;
            let mut merged = current.clone();
            merged.extend(patch.clone());
            if merged == current {
                return Ok(None);
            }
            queued.rollback = Some(Prior { rows: vec![PriorRow::edited(reader, model, id.as_str(), &current, &merged)?] }.to_value());
            plan.update(key.clone(), id.clone(), merged.clone());
            Ok(Some(self.touch_ops(reader, spec, &[&merged, &current], &own)?))
        })?;
        if !committed {
            let current = self.find(model, id)?;
            return current.ok_or(DbError::NotFound { model: model.to_string(), id: id.to_string() });
        }
        self.find(model, id)?.ok_or(DbError::NotFound { model: model.to_string(), id: id.to_string() })
    }

    pub fn act(&self, model: &str, action: &str, id: &RowId, patch: Map<String, Value>, args: Map<String, Value>) -> Result<Row, DbError> {
        let spec = self.shared.spec(model)?;
        let patch = spec.patch(&patch);
        let key = ModelKey::new(model);
        let own = RowRef::new(key.clone(), id.clone());
        let mut input = args;
        input.insert("id".to_string(), Value::String(id.to_string()));
        let mut queued = Queued { action: Action::Named(action.to_string()), row_id: id.clone(), temp_id: None, input: Value::Object(input), rollback: None, scope_ops: Vec::new() };
        self.commit_local_optional(spec, &mut queued, |reader, plan, queued| {
            let current = reader.point(model, id.as_str())?.ok_or_else(|| DbError::NotFound { model: model.to_string(), id: id.to_string() })?;
            let mut merged = current.clone();
            merged.extend(patch.clone());
            queued.rollback = Some(Prior { rows: vec![PriorRow::edited(reader, model, id.as_str(), &current, &merged)?] }.to_value());
            plan.update(key.clone(), id.clone(), merged.clone());
            Ok(Some(self.touch_ops(reader, spec, &[&merged, &current], &own)?))
        })?;
        self.find(model, id)?.ok_or(DbError::NotFound { model: model.to_string(), id: id.to_string() })
    }

    pub fn destroy(&self, model: &str, id: &RowId) -> Result<Vec<RowRef>, DbError> {
        let spec = self.shared.spec(model)?;
        let own = RowRef::new(ModelKey::new(model), id.clone());
        let generation = self.generation();
        let created_at = self.shared.store.lock().now_ms();
        let mut destroyed: Vec<RowRef> = Vec::new();
        let mut discarded: Vec<String> = Vec::new();
        self.shared
            .store
            .commit_with(generation, |reader| {
                if reader.point(model, id.as_str())?.is_none() {
                    return Err(DbError::NotFound { model: model.to_string(), id: id.to_string() });
                }
                destroyed = self.destroy_targets(reader, &own, WriteOrigin::Optimistic)?;
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
                };
                envelope.operation_transitions.push(self.begin(reader, spec, &queued, created_at)?);
                Ok(Some(envelope))
            })
            .map_err(plan_fault)?;
        self.shared.files.discard(&discarded);
        Ok(destroyed)
    }

    pub fn outbox(&self) -> Result<Vec<OutboxOperation>, DbError> {
        self.shared.read(|reader| Ok(read_outbox(reader)?))
    }

    fn commit_local_optional(
        &self,
        spec: &ModelSpec,
        queued: &mut Queued,
        build: impl FnOnce(&Reader<'_>, &mut WritePlan, &mut Queued) -> Result<Option<Vec<EntityOp>>, DbError>,
    ) -> Result<bool, DbError> {
        let generation = self.generation();
        let created_at = self.shared.store.lock().now_ms();
        let result = self
            .shared
            .store
            .commit_with(generation, |reader| {
                let mut plan = WritePlan::new();
                let Some(touches) = build(reader, &mut plan, queued)? else { return Ok(None) };
                let mut envelope: CommitEnvelope = plan.envelope(WriteOrigin::Optimistic, generation);
                envelope.entity_ops.extend(touches);
                envelope.scope_ops.extend(std::mem::take(&mut queued.scope_ops));
                envelope.operation_transitions.push(self.begin(reader, spec, queued, created_at)?);
                Ok(Some(envelope))
            })
            .map_err(plan_fault)?;
        Ok(!result.is_noop())
    }

    fn begin(&self, reader: &Reader<'_>, spec: &ModelSpec, queued: &Queued, created_at: i64) -> Result<OperationTransition, DbError> {
        Ok(OperationTransition::Begin(OperationRecord {
            seq: None,
            action_key: Operation::action_key(spec.key.as_str(), queued.action.name()),
            status: OperationStatus::Pending,
            row_ids: vec![queued.row_id.clone()],
            temp_id: queued.temp_id.clone(),
            input: queued.input.clone(),
            transport_started: false,
            base_revision: Some(reader.revision_seq()?),
            created_at,
            dedupe_key: None,
            rollback: queued.rollback.clone(),
            upload: None,
        }))
    }

    pub(crate) fn touch_ops(&self, reader: &Reader<'_>, spec: &ModelSpec, rows: &[&Map<String, Value>], own: &RowRef) -> Result<Vec<EntityOp>, DbError> {
        if !touches_parent(RootShape::Upsert, WriteOrigin::Optimistic, false) {
            return Ok(Vec::new());
        }
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

    pub(crate) fn destroy_targets(&self, reader: &Reader<'_>, own: &RowRef, origin: WriteOrigin) -> Result<Vec<RowRef>, DbError> {
        if !cascades(RootShape::Destroy, origin, true) {
            return Ok(vec![own.clone()]);
        }
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
