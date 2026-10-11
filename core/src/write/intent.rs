use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use crate::identity::{Generation, ModelKey, RowId};
use crate::value::read_id;

#[derive(Debug, Clone, PartialEq)]
pub enum WriteIntent {
    Upsert { model: ModelKey, rows: Vec<Map<String, Value>> },
    Update { model: ModelKey, id: RowId, patch: Map<String, Value> },
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

    pub fn envelope(&self, origin: WriteOrigin, generation: Generation) -> CommitEnvelope {
        let mut envelope = CommitEnvelope::new(origin, generation);
        envelope.entity_ops = merged_ops(entity_ops(self));
        envelope
    }
}

pub fn entity_ops(plan: &WritePlan) -> Vec<EntityOp> {
    let mut ops = Vec::new();
    for intent in &plan.intents {
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

pub fn restore_patch(fields: &[String], previous: &Map<String, Value>) -> Map<String, Value> {
    fields.iter().map(|field| (field.clone(), previous.get(field).cloned().unwrap_or(Value::Null))).collect()
}
