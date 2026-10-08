use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::db::Db;
use crate::identity::ModelKey;
use crate::schema::AssociationKind;
use crate::value::read_id;

use super::error::SyncError;

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct Normalized {
    pub roots: Vec<Map<String, Value>>,
    pub related: Vec<(ModelKey, Map<String, Value>)>,
    pub dropped: usize,
}

pub(crate) type Related = BTreeMap<(String, String), Map<String, Value>>;

pub(crate) fn normalize_nodes(db: &Db, model: &str, nodes: &[Map<String, Value>]) -> Result<Normalized, SyncError> {
    db.spec(model).map_err(|_| SyncError::UnknownModel(model.to_string()))?;
    let mut related = Related::new();
    let mut normalized = Normalized::default();
    for node in nodes {
        match normalize_node(db, model, node, &mut related)? {
            Some(row) => normalized.roots.push(row),
            None => normalized.dropped += 1,
        }
    }
    normalized.related = related.into_iter().map(|((model, _), row)| (ModelKey::new(&model), row)).collect();
    Ok(normalized)
}

pub(crate) fn normalize_node(db: &Db, model: &str, node: &Map<String, Value>, related: &mut Related) -> Result<Option<Map<String, Value>>, SyncError> {
    let spec = db.spec(model).map_err(|_| SyncError::UnknownModel(model.to_string()))?;
    let mut input = node.clone();
    for association in spec.associations.iter().filter(|association| association.kind == AssociationKind::BelongsTo) {
        let Some(nested) = node.get(&association.name).and_then(Value::as_object) else { continue };
        let target = association.target.as_str();
        if db.spec(target).is_err() {
            continue;
        }
        let Some(parent) = normalize_node(db, target, nested, related)? else { continue };
        let Some(id) = parent.get("id").and_then(read_id) else { continue };
        input.insert(association.foreign_key.clone(), Value::String(id.as_str().to_string()));
        related.insert((target.to_string(), id.as_str().to_string()), parent);
    }
    let Ok(row) = spec.normalize(&input) else { return Ok(None) };
    Ok(row.get("id").is_some().then_some(row))
}
