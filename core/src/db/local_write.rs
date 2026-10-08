use std::collections::HashSet;

use rusqlite::OptionalExtension;
use serde_json::{json, Map, Value};

use crate::commit::{
    CommitEnvelope, DestroyReason, EntityOp, OperationRecord, OperationStatus, OperationTransition, WriteOrigin,
};
use crate::ddl::table_name;
use crate::identity::{ModelKey, RowId, RowRef};
use crate::operation::Operation;
use crate::query::{Query, Where};
use crate::schema::{ActionMode, AssociationKind, Dependent, ModelSpec, RootShape};
use crate::store::{PlanCommitError, Reader};
use crate::value::read_id;
use crate::write::{
    cascade_destroys, cascade_envelope, cascades, restore_patch, touches_parent, WritePlan,
};

use super::error::DbError;
use super::open::Db;
use super::row::Row;

#[derive(Debug, Clone, PartialEq)]
pub struct OutboxEntry {
    pub operation: Operation,
    pub input: Value,
    pub rollback: Option<Value>,
}

#[derive(Clone, Copy)]
enum Action {
    Create,
    Update,
    Destroy,
}

impl Action {
    fn name(self) -> &'static str {
        match self {
            Action::Create => "create",
            Action::Update => "update",
            Action::Destroy => "destroy",
        }
    }

    fn default_mode(self) -> ActionMode {
        match self {
            Action::Create | Action::Update => ActionMode::Durable,
            Action::Destroy => ActionMode::Request,
        }
    }
}

struct Queued {
    action: Action,
    row_id: RowId,
    temp_id: Option<RowId>,
    input: Value,
    rollback: Option<Value>,
}

fn plan_fault(error: PlanCommitError<DbError>) -> DbError {
    match error {
        PlanCommitError::Plan(error) => error,
        PlanCommitError::Store(error) => error.into(),
    }
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
        let data = spec.normalize(&input)?;
        let key = ModelKey::new(model);
        let queued = Queued {
            action: Action::Create,
            row_id: id.clone(),
            temp_id: id.is_temp().then(|| id.clone()),
            input: Value::Object(data.clone()),
            rollback: None,
        };
        self.commit_local(spec, queued, |reader, plan| {
            plan.upsert(key.clone(), vec![data.clone()]);
            self.touch_ops(reader, spec, &[&data], &RowRef::new(key.clone(), id.clone()))
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
        let mut queued = Queued { action: Action::Update, row_id: id.clone(), temp_id: None, input: Value::Object(input), rollback: None };
        let committed = self.commit_local_optional(spec, &mut queued, |reader, plan, queued| {
            let current = reader.point(model, id.as_str())?.ok_or_else(|| DbError::NotFound { model: model.to_string(), id: id.to_string() })?;
            let mut merged = current.clone();
            merged.extend(patch.clone());
            if merged == current {
                return Ok(None);
            }
            let fields: Vec<String> = patch.keys().cloned().collect();
            queued.rollback = Some(Value::Object(restore_patch(&fields, &current)));
            plan.update(key.clone(), id.clone(), merged.clone());
            Ok(Some(self.touch_ops(reader, spec, &[&merged, &current], &own)?))
        })?;
        if !committed {
            let current = self.find(model, id)?;
            return current.ok_or(DbError::NotFound { model: model.to_string(), id: id.to_string() });
        }
        self.find(model, id)?.ok_or(DbError::NotFound { model: model.to_string(), id: id.to_string() })
    }

    pub fn destroy(&self, model: &str, id: &RowId) -> Result<Vec<RowRef>, DbError> {
        let spec = self.shared.spec(model)?;
        let own = RowRef::new(ModelKey::new(model), id.clone());
        let generation = self.generation();
        let created_at = self.shared.store.lock().now_ms();
        let mut destroyed: Vec<RowRef> = Vec::new();
        let result = self
            .shared
            .store
            .commit_with(generation, |reader| {
                if reader.point(model, id.as_str())?.is_none() {
                    return Err(DbError::NotFound { model: model.to_string(), id: id.to_string() });
                }
                let mut descendants = Vec::new();
                let mut seen = HashSet::from([own.clone()]);
                self.descendants(reader, &own, &mut seen, &mut descendants)?;
                destroyed = if cascades(RootShape::Destroy, WriteOrigin::Optimistic, true) {
                    cascade_destroys(&own, &descendants, true)
                } else {
                    vec![own.clone()]
                };
                let mut rows = Vec::new();
                for target in &destroyed {
                    if let Some(data) = reader.point(target.model.as_str(), target.id.as_str())? {
                        rows.push(json!({"model": target.model.as_str(), "row": data}));
                    }
                }
                let mut envelope = cascade_envelope(&destroyed, DestroyReason::Local, WriteOrigin::Optimistic, generation);
                let queued = Queued {
                    action: Action::Destroy,
                    row_id: id.clone(),
                    temp_id: None,
                    input: json!({"id": id.as_str()}),
                    rollback: Some(json!({"rows": rows})),
                };
                envelope.operation_transitions.push(self.begin(reader, spec, &queued, created_at)?);
                Ok(Some(envelope))
            })
            .map_err(plan_fault)?;
        self.publish(&result);
        Ok(destroyed)
    }

    pub fn outbox(&self) -> Result<Vec<OutboxEntry>, DbError> {
        self.shared.read(|reader| {
            let operations = crate::operation::open_operations(reader.connection())?;
            let mut statement = reader.connection().prepare("SELECT input, rollback FROM op WHERE seq = ?1")?;
            let mut entries = Vec::with_capacity(operations.len());
            for operation in operations {
                let (input, rollback): (Vec<u8>, Option<Vec<u8>>) =
                    statement.query_row([operation.id.0], |row| Ok((row.get(0)?, row.get(1)?)))?;
                entries.push(OutboxEntry {
                    operation,
                    input: serde_json::from_slice(&input).map_err(crate::store::StoreError::from)?,
                    rollback: rollback
                        .map(|bytes| serde_json::from_slice(&bytes))
                        .transpose()
                        .map_err(crate::store::StoreError::from)?,
                });
            }
            Ok(entries)
        })
    }

    fn commit_local(
        &self,
        spec: &ModelSpec,
        queued: Queued,
        build: impl FnOnce(&Reader<'_>, &mut WritePlan) -> Result<Vec<EntityOp>, DbError>,
    ) -> Result<(), DbError> {
        let mut queued = queued;
        self.commit_local_optional(spec, &mut queued, |reader, plan, _| build(reader, plan).map(Some))?;
        Ok(())
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
                envelope.operation_transitions.push(self.begin(reader, spec, queued, created_at)?);
                Ok(Some(envelope))
            })
            .map_err(plan_fault)?;
        self.publish(&result);
        Ok(!result.is_noop())
    }

    fn begin(&self, reader: &Reader<'_>, spec: &ModelSpec, queued: &Queued, created_at: i64) -> Result<OperationTransition, DbError> {
        let name = queued.action.name();
        let mode = spec.actions.iter().find(|declaration| declaration.name == name).map_or(queued.action.default_mode(), |declaration| declaration.mode);
        Ok(OperationTransition::Begin(OperationRecord {
            seq: None,
            action_key: Operation::action_key(spec.key.as_str(), name),
            action_mode: mode,
            status: OperationStatus::Pending,
            row_ids: vec![queued.row_id.clone()],
            temp_id: queued.temp_id.clone(),
            input: queued.input.clone(),
            transport_started: false,
            base_revision: Some(reader.revision_seq()?),
            created_at,
            dedupe_key: None,
            rollback: queued.rollback.clone(),
        }))
    }

    fn touch_ops(&self, reader: &Reader<'_>, spec: &ModelSpec, rows: &[&Map<String, Value>], own: &RowRef) -> Result<Vec<EntityOp>, DbError> {
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
