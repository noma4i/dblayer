use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::commit::{EntityOp, ScopeOp, ScopeRef};
use crate::identity::{ModelKey, RowId};
use crate::value::{fractional_between, read_id};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LandingError {
    #[error("scope belongs to another model")]
    ScopeModelMismatch,
    #[error("fractional order bounds are invalid")]
    InvalidOrderBounds,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMember {
    pub id: String,
    pub order_key: String,
    pub position: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoredScope {
    pub last_key: Option<String>,
    pub incoming: Vec<StoredMember>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LandingPlan {
    pub entity_ops: Vec<EntityOp>,
    pub scope_ops: Vec<ScopeOp>,
}

pub fn incoming_ids(incoming: &[Map<String, Value>]) -> Vec<RowId> {
    identified_rows(incoming).into_iter().map(|(id, _)| id).collect()
}

pub fn plan_landing(scope: &ScopeRef, model: &ModelKey, stored: &StoredScope, incoming: &[Map<String, Value>]) -> Result<LandingPlan, LandingError> {
    if &scope.model != model {
        return Err(LandingError::ScopeModelMismatch);
    }
    let rows = identified_rows(incoming);
    let existing: BTreeMap<&str, &StoredMember> = stored.incoming.iter().map(|member| (member.id.as_str(), member)).collect();
    let mut previous_key = stored.last_key.clone();
    let mut keyed = Vec::with_capacity(rows.len());
    let (mut inserted, mut moved) = (Vec::new(), Vec::new());
    for (index, (id, _)) in rows.iter().enumerate() {
        let (order_key, placed) = match existing.get(id.as_str()) {
            Some(member) => (member.order_key.clone(), Some(member.position)),
            None => {
                let order_key = fractional_between(previous_key.as_deref(), None).map_err(|_| LandingError::InvalidOrderBounds)?;
                previous_key = Some(order_key.clone());
                (order_key, None)
            }
        };
        match placed {
            None => inserted.push((id.clone(), order_key.clone())),
            Some(position) if position != index => moved.push((id.clone(), order_key.clone())),
            Some(_) => {}
        }
        keyed.push(order_key);
    }
    let entity_ops = rows.into_iter().map(|(id, row)| EntityOp::Upsert { model: model.clone(), id, row }).collect();
    let appended = inserted.into_iter().map(|(row, order_key)| ScopeOp::Append { scope: scope.clone(), row, order_key });
    let shifted = moved.into_iter().map(|(row, order_key)| ScopeOp::Move { scope: scope.clone(), row, order_key });
    Ok(LandingPlan { entity_ops, scope_ops: appended.chain(shifted).collect() })
}

fn identified_rows(incoming: &[Map<String, Value>]) -> Vec<(RowId, Map<String, Value>)> {
    let mut seen = BTreeSet::new();
    let mut rows = Vec::new();
    for row in incoming {
        let Some(id) = row.get("id").and_then(read_id) else { continue };
        if !seen.insert(id.clone()) {
            continue;
        }
        let mut normalized = row.clone();
        normalized.insert("id".to_string(), Value::String(id.as_str().to_string()));
        rows.push((id, normalized));
    }
    rows
}
