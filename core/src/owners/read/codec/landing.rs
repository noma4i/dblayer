use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::store::{EntityOp, ScopeOp, ScopeRef};
use crate::identity::{ModelKey, RowId};
use crate::value::{fractional_between, read_id};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LandingError {
    #[error("scope belongs to another model")]
    ScopeModelMismatch,
    #[error("fractional order bounds are invalid")]
    InvalidOrderBounds,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoredScope {
    pub last_key: Option<String>,
    pub incoming: Vec<String>,
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
    let existing: BTreeSet<&str> = stored.incoming.iter().map(String::as_str).collect();
    let mut previous_key = stored.last_key.clone();
    let mut inserted = Vec::new();
    for (id, _) in &rows {
        if existing.contains(id.as_str()) {
            continue;
        }
        let order_key = fractional_between(previous_key.as_deref(), None).map_err(|_| LandingError::InvalidOrderBounds)?;
        previous_key = Some(order_key.clone());
        inserted.push((id.clone(), order_key));
    }
    let entity_ops = rows.into_iter().map(|(id, row)| EntityOp::Upsert { model: model.clone(), id, row }).collect();
    let appended = inserted.into_iter().map(|(row, order_key)| ScopeOp::Append { scope: scope.clone(), row, order_key });
    Ok(LandingPlan { entity_ops, scope_ops: appended.collect() })
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
