use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, WriteOrigin};
use crate::identity::{Generation, ModelKey, RowId};
use crate::value::read_id;

#[derive(Debug, Clone, PartialEq)]
pub enum WriteIntent {
    Upsert { model: ModelKey, rows: Vec<Map<String, Value>> },
    Update { model: ModelKey, id: RowId, patch: Map<String, Value> },
    Destroy { model: ModelKey, ids: Vec<RowId> },
    Invalidate(RelationInvalidation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationInvalidation {
    pub model: ModelKey,
    pub relation_name: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct WritePlan {
    intents: Vec<WriteIntent>,
}

impl WritePlan {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&mut self, model: ModelKey, rows: Vec<Map<String, Value>>) {
        if !rows.is_empty() {
            self.intents.push(WriteIntent::Upsert { model, rows });
        }
    }

    pub fn update(&mut self, model: ModelKey, id: RowId, patch: Map<String, Value>) {
        if !patch.is_empty() {
            self.intents.push(WriteIntent::Update { model, id, patch });
        }
    }

    pub fn destroy(&mut self, model: ModelKey, ids: Vec<RowId>) {
        if !ids.is_empty() {
            self.intents.push(WriteIntent::Destroy { model, ids });
        }
    }

    pub fn invalidate(&mut self, model: ModelKey, relation_name: &str) {
        self.intents.push(WriteIntent::Invalidate(RelationInvalidation { model, relation_name: relation_name.to_string() }));
    }

    pub fn is_empty(&self) -> bool {
        self.intents.is_empty()
    }

    pub fn intents(&self) -> &[WriteIntent] {
        &self.intents
    }

    pub fn models(&self) -> Vec<ModelKey> {
        let mut seen = HashSet::new();
        let models = self.intents.iter().map(|intent| match intent {
            WriteIntent::Upsert { model, .. } | WriteIntent::Update { model, .. } | WriteIntent::Destroy { model, .. } => model,
            WriteIntent::Invalidate(request) => &request.model,
        });
        models.filter(|model| seen.insert((*model).clone())).cloned().collect()
    }

    pub fn invalidations(&self) -> Vec<RelationInvalidation> {
        self.intents
            .iter()
            .filter_map(|intent| match intent {
                WriteIntent::Invalidate(request) => Some(request.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn envelope(&self, origin: WriteOrigin, generation: Generation) -> CommitEnvelope {
        let mut envelope = CommitEnvelope::new(origin, generation);
        envelope.entity_ops = merged_ops(entity_ops(self));
        envelope
    }
}

pub fn entity_ops(plan: &WritePlan) -> Vec<EntityOp> {
    let mut ops = Vec::new();
    for intent in plan.intents() {
        match intent {
            WriteIntent::Upsert { model, rows } => {
                for row in rows {
                    if let Some(id) = row.get("id").and_then(read_id) {
                        let mut row = row.clone();
                        row.insert("id".to_string(), Value::String(id.to_string()));
                        ops.push(EntityOp::Upsert { model: model.clone(), id, row });
                    }
                }
            }
            WriteIntent::Update { model, id, patch } => {
                let mut row = patch.clone();
                row.insert("id".to_string(), Value::String(id.to_string()));
                ops.push(EntityOp::Upsert { model: model.clone(), id: id.clone(), row });
            }
            WriteIntent::Destroy { model, ids } => {
                ops.extend(ids.iter().map(|id| EntityOp::Destroy { model: model.clone(), id: id.clone(), reason: DestroyReason::Local }));
            }
            WriteIntent::Invalidate(_) => {}
        }
    }
    ops
}

fn merged_ops(ops: Vec<EntityOp>) -> Vec<EntityOp> {
    let mut merged: Vec<EntityOp> = Vec::new();
    let mut positions: HashMap<(ModelKey, RowId), usize> = HashMap::new();
    for op in ops {
        let row = op.row_ref();
        let key = (row.model, row.id);
        match positions.get(&key) {
            Some(&index) => match (&mut merged[index], op) {
                (EntityOp::Upsert { row: existing, .. }, EntityOp::Upsert { row: incoming, .. }) => existing.extend(incoming),
                (slot, replacement) => *slot = replacement,
            },
            None => {
                positions.insert(key, merged.len());
                merged.push(op);
            }
        }
    }
    merged
}

pub fn restorable_fields(patched_fields: &[String], current: &Map<String, Value>, patched: &Map<String, Value>) -> Vec<String> {
    patched_fields.iter().filter(|field| current.get(*field) == patched.get(*field)).cloned().collect()
}

pub fn restore_patch(fields: &[String], previous: &Map<String, Value>) -> Map<String, Value> {
    fields.iter().map(|field| (field.clone(), previous.get(field).cloned().unwrap_or(Value::Null))).collect()
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    #[error("intent targets the owner model {0}")]
    SameOwnerIntent(ModelKey),
}

pub fn rejects_same_owner(owner: &ModelKey, intent_models: &[ModelKey]) -> Option<PlanError> {
    intent_models.iter().find(|model| *model == owner).map(|model| PlanError::SameOwnerIntent(model.clone()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootSelection<T> {
    None,
    One(T),
    Many(Vec<T>),
}

impl<T: Clone> RootSelection<T> {
    pub fn values(&self) -> Vec<T> {
        match self {
            RootSelection::None => Vec::new(),
            RootSelection::One(value) => vec![value.clone()],
            RootSelection::Many(values) => values.clone(),
        }
    }

    pub fn is_noop(&self) -> bool {
        match self {
            RootSelection::None => true,
            RootSelection::One(_) => false,
            RootSelection::Many(values) => values.is_empty(),
        }
    }

    pub fn planned_count(&self) -> usize {
        self.values().len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingRowOutcome {
    Applied,
    InvalidateAuthoritative,
    NoOp,
    StaleGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingRowInput {
    pub row_exists: bool,
    pub authoritative_relation: Option<String>,
    pub is_stale_generation: bool,
}

pub fn missing_row_outcome(input: &MissingRowInput) -> MissingRowOutcome {
    if input.is_stale_generation {
        MissingRowOutcome::StaleGeneration
    } else if input.row_exists {
        MissingRowOutcome::Applied
    } else if input.authoritative_relation.as_deref().is_some_and(|relation| !relation.is_empty()) {
        MissingRowOutcome::InvalidateAuthoritative
    } else {
        MissingRowOutcome::NoOp
    }
}

pub fn publishes_event(outcome: MissingRowOutcome) -> bool {
    matches!(outcome, MissingRowOutcome::Applied | MissingRowOutcome::InvalidateAuthoritative)
}

pub fn invalidated_relations(input: &MissingRowInput) -> Vec<String> {
    match (missing_row_outcome(input), &input.authoritative_relation) {
        (MissingRowOutcome::InvalidateAuthoritative, Some(relation)) => vec![relation.clone()],
        _ => Vec::new(),
    }
}

pub fn publishes(envelope: &CommitEnvelope, current: Generation, apply_failed: bool) -> bool {
    !apply_failed && !envelope.is_empty() && envelope.generation == current
}

pub fn affected_row_keys(envelope: &CommitEnvelope, subscribed_rows: &HashSet<String>) -> Vec<String> {
    envelope.touched_rows().into_iter().map(|row| row.id.to_string()).filter(|id| subscribed_rows.contains(id)).collect()
}

pub fn affected_scope_keys(envelope: &CommitEnvelope, subscribed_scopes: &HashSet<String>) -> Vec<String> {
    envelope.touched_scopes().into_iter().map(|scope| scope.key.as_str().to_string()).filter(|key| subscribed_scopes.contains(key)).collect()
}

pub fn tick_count(envelope: &CommitEnvelope, current: Generation, subscribed_rows: &HashSet<String>, subscribed_scopes: &HashSet<String>) -> usize {
    let affected = !affected_row_keys(envelope, subscribed_rows).is_empty() || !affected_scope_keys(envelope, subscribed_scopes).is_empty();
    usize::from(publishes(envelope, current, false) && affected)
}
