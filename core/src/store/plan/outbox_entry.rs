use std::collections::HashSet;

use serde_json::{json, Map, Value};

use crate::store::{OperationRecord, OperationTransition, ScopeOp, ScopeRef};
use crate::identity::{ModelKey, RowId, RowRef};
use crate::owners::write::operation_status::Operation;
use crate::store::query::{Query, Where};
use crate::schema::{Dependent, ModelSpec, SchemaError, ValueFault};
use crate::store::{Reader, Store, StoreError, CoreError};
use crate::value::fractional_between;
use crate::owners::write::plan_cascade::cascade_destroys;

use super::prior::PriorRow;

#[derive(Debug, Clone, PartialEq)]
pub struct OutboxOperation {
    pub operation: Operation,
    pub input: Value,
    pub rollback: Option<Value>,
}

impl OutboxOperation {
    pub(crate) fn read(reader: &Reader<'_>, seq: i64) -> Result<Option<Self>, StoreError> {
        let Some(operation) = crate::store::operation::read_operation(reader.connection(), seq)? else { return Ok(None) };
        Self::from_operation(reader, operation).map(Some)
    }

    fn from_operation(reader: &Reader<'_>, operation: Operation) -> Result<Self, StoreError> {
        let input = crate::store::operation::read_value(reader.connection(), operation.id.0, "input")?.unwrap_or(Value::Null);
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
            base_revision: Some(operation.base_revision),
            created_at: operation.created_at,
            dedupe_key: operation.dedupe_key.clone(),
            rollback,
        }))
    }
}

pub(crate) fn read_outbox(reader: &Reader<'_>) -> Result<Vec<OutboxOperation>, StoreError> {
    let operations = crate::store::operation::open_operations(reader.connection())?;
    let filter = format!("seq IN (SELECT seq FROM op WHERE status IN {})", crate::store::operation::OPEN_STATUSES);
    let inputs = crate::store::nodes::read_trees(reader.connection(), "op_value", &["seq", "part"], &format!("{filter} AND part = 'input'"), &[])?;
    let mut inputs: std::collections::HashMap<i64, Value> = inputs.into_iter().filter_map(|(owner, value)| match owner.first() { Some(rusqlite::types::Value::Integer(seq)) => Some((*seq, value)), _ => None }).collect();
    let rows = crate::store::nodes::read_trees(reader.connection(), "op_effect_value", &["seq", "position"], &filter, &[])?;
    let mut priors: std::collections::HashMap<i64, Vec<Value>> = std::collections::HashMap::new();
    for (owner, value) in rows { if let Some(rusqlite::types::Value::Integer(seq)) = owner.first() { priors.entry(*seq).or_default().push(value); } }
    let mut entries = Vec::with_capacity(operations.len());
    for operation in operations {
        let seq = operation.id.0;
        let input = inputs.remove(&seq).unwrap_or(Value::Null);
        let rollback = priors.remove(&seq).and_then(super::op_effect::prior_rows);
        entries.push(OutboxOperation { operation, input, rollback });
    }
    Ok(entries)
}

pub(super) fn local_scope_ops(reader: &Reader<'_>, spec: &ModelSpec, id: &RowId, row: &Map<String, Value>) -> Result<Vec<ScopeOp>, CoreError> {
    let mut operations = Vec::new();
    for key in spec.list_scopes(row) {
        let first = reader.scope_first_key(spec.key.as_str(), key.as_str())?;
        let order_key = fractional_between(None, first.as_deref()).map_err(|_| crate::owners::read::codec::LandingError::InvalidOrderBounds)?;
        operations.push(ScopeOp::Append { scope: ScopeRef::new(spec.key.clone(), key), row: id.clone(), order_key });
    }
    Ok(operations)
}

pub(super) fn plan_scope_effect<'a>(reader: &'a Reader<'_>, specs: &'a dyn Fn(&str) -> Option<&'a ModelSpec>, target: &ModelSpec, effect: &crate::schema::ScopeEffect, call: (&RowRef, &Map<String, Value>), rows: &mut Vec<PriorRow>, scope_ops: &mut Vec<ScopeOp>) -> Result<(), CoreError> {
    use crate::owners::write::outbox_plan::{scope_changes, ListChange};
    let view = super::view::ReaderView { reader, specs, db: None };
    let owned = reader.revision_seq()? + 1;
    for change in scope_changes(&view, target, effect, call.0.id.as_str(), call.1).map_err(|error| CoreError::Internal(error.0))? {
        match change {
            ListChange::Join { model, list, id, before, order_key } => {
                if let Some(order_key) = order_key {
                    scope_ops.push(ScopeOp::Append { scope: ScopeRef::new(ModelKey::new(&model), list.clone()), row: RowId::new(&id)?, order_key });
                }
                rows.push(PriorRow::Join { model, id, scope_key: list.to_string(), before, owned });
            }
            ListChange::Leave { model, list, id, before } => {
                if before.is_some() { scope_ops.push(ScopeOp::Detach { scope: ScopeRef::new(ModelKey::new(&model), list.clone()), row: RowId::new(&id)? }); }
                rows.push(PriorRow::Leave { model, id, scope_key: list.to_string(), before, owned });
            }
        }
    }
    Ok(())
}

impl Store {

    pub(crate) fn destroy_targets(&self, reader: &Reader<'_>, own: &RowRef) -> Result<Vec<RowRef>, CoreError> {
        let mut descendants = Vec::new();
        self.descendants(reader, own, &mut HashSet::from([own.clone()]), &mut descendants)?;
        Ok(cascade_destroys(own, &descendants, true))
    }

    fn descendants(&self, reader: &Reader<'_>, parent: &RowRef, seen: &mut HashSet<RowRef>, found: &mut Vec<RowRef>) -> Result<(), CoreError> {
        let spec = self.spec(parent.model.as_str())?;
        for association in spec.associations.iter().filter(|association| association.owns_children() && association.dependent == Some(Dependent::Destroy)) {
            let query = Query::new(association.target.as_str()).filtered(Where::equal(&association.foreign_key, json!(parent.id.as_str())));
            for (child_id, _) in self.query_keys(reader, &query)? {
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



pub(super) fn reject_unknown_fields(spec: &ModelSpec, model: &str, values: &Map<String, Value>) -> Result<(), CoreError> {
    match spec.fault(values, false) {
        Some(fault) => Err(fault_error(model, fault)),
        None => Ok(()),
    }
}

pub(super) fn reject_invalid_values(spec: &ModelSpec, model: &str, values: &Map<String, Value>) -> Result<(), CoreError> {
    match spec.fault(values, true) {
        Some(fault) => Err(fault_error(model, fault)),
        None => Ok(()),
    }
}

pub(crate) fn fault_error(model: &str, fault: ValueFault) -> CoreError {
    match fault {
        ValueFault::Unknown(field) => CoreError::UnknownField { model: model.to_string(), field },
        ValueFault::Invalid(field) => SchemaError::InvalidValue(field).into(),
    }
}
