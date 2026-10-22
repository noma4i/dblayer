use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::db::Db;
use crate::identity::{ModelKey, RowId};
use crate::read::EDGE_KEY;
use crate::schema::AssociationKind;
use crate::value::read_id;

use super::error::SyncError;

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct Normalized {
    pub roots: Vec<Map<String, Value>>,
    pub order: Vec<RowId>,
    pub related: Vec<(ModelKey, Map<String, Value>)>,
    pub dropped: usize,
    pub edges: BTreeMap<String, Map<String, Value>>,
}

pub(crate) type Related = Vec<(ModelKey, Map<String, Value>)>;

impl Normalized {
    pub(crate) fn coalesce(&mut self, model: &str) {
        let key = ModelKey::new(model);
        let positions: BTreeMap<_, _> = self.roots.iter().enumerate().rev()
            .filter_map(|(index, row)| Some((row.get("id").and_then(read_id)?, index))).collect();
        let input = std::mem::take(&mut self.related).into_iter()
            .chain(std::mem::take(&mut self.roots).into_iter().map(|row| (key.clone(), row)));
        let mut roots = Vec::new();
        for (model, row) in crate::store::coalesce_rows(input) {
            let position = row.get("id").and_then(read_id).and_then(|id| positions.get(&id));
            match position.filter(|_| model == key) {
                Some(position) => roots.push((*position, row)),
                None => self.related.push((model, row)),
            }
        }
        roots.sort_by_key(|(position, _)| *position);
        self.roots = roots.into_iter().map(|(_, row)| row).collect();
    }
}

pub(crate) fn normalize_nodes(db: &Db, model: &str, nodes: &[Map<String, Value>]) -> Result<Normalized, SyncError> {
    db.spec(model).map_err(|_| SyncError::UnknownModel(model.to_string()))?;
    let mut related = Related::new();
    let mut normalized = Normalized::default();
    for node in nodes {
        match normalize_node(db, model, node, &mut related)? {
            Some(row) => {
                normalized.order.extend(row.get("id").and_then(read_id));
                if let (Some(id), Some(edge)) = (row.get("id").and_then(read_id), node.get(EDGE_KEY).and_then(Value::as_object)) {
                    normalized.edges.insert(id.as_str().to_string(), edge.clone());
                }
                normalized.roots.push(row);
            }
            None => normalized.dropped += 1,
        }
    }
    normalized.related = related;
    normalized.coalesce(model);
    Ok(normalized)
}

pub(crate) fn nested_input(db: &Db, model: &str, node: &Map<String, Value>, related: &mut Related) -> Result<Map<String, Value>, SyncError> {
    let spec = db.spec(model).map_err(|_| SyncError::UnknownModel(model.to_string()))?;
    let parent_id = node.get("id").and_then(read_id);
    let mut input = node.clone();
    for association in &spec.associations {
        let target = association.target.as_str();
        if db.spec(target).is_err() {
            continue;
        }
        let path = association.from.as_deref().unwrap_or(&association.name);
        let Some(found) = nested_values(node, path) else { continue };
        let mut ids = Vec::new();
        let mut bare_id = None;
        for nested in found.iter().filter_map(|value| value.as_object()) {
            bare_id = bare_id.or_else(|| nested.get("id").and_then(read_id));
            let mut child = nested.clone();
            if matches!(association.kind, AssociationKind::HasOne | AssociationKind::HasMany) {
                let Some(parent) = &parent_id else { continue };
                child.insert(association.foreign_key.clone(), Value::String(parent.as_str().to_string()));
            }
            let Some(row) = normalize_node(db, target, &child, related)? else { continue };
            let Some(id) = row.get("id").and_then(read_id) else { continue };
            related.push((ModelKey::new(target), row));
            ids.push(Value::String(id.as_str().to_string()));
        }
        match association.kind {
            AssociationKind::BelongsTo => {
                let key = ids.first().and_then(Value::as_str).map(str::to_string).or_else(|| bare_id.map(|id| id.as_str().to_string()));
                match key {
                    Some(id) => {
                        input.insert(association.foreign_key.clone(), Value::String(id));
                    }
                    None if crate::schema::read_path(node, &format!("{path}.id")).is_some_and(Value::is_null)
                        && spec.field(&association.foreign_key).is_some_and(|field| field.nullable || field.default.is_some()) => {
                        input.insert(association.foreign_key.clone(), Value::Null);
                    }
                    None => {}
                }
            }
            AssociationKind::References => {
                input.insert(association.foreign_key.clone(), Value::Array(ids));
            }
            AssociationKind::HasOne | AssociationKind::HasMany => {}
        }
        let feeds_an_id = spec.fields.iter().any(|field| field.id_from_parent.as_deref() == Some(association.name.as_str()));
        if path == association.name && !feeds_an_id {
            input.remove(&association.name);
        }
    }
    Ok(input)
}

/// The nested values at a dotted path. Lists are walked element by element, and a connection (`nodes` or `edges`) yields its nodes.
fn nested_values<'a>(node: &'a Map<String, Value>, path: &str) -> Option<Vec<&'a Value>> {
    let mut segments = path.split('.');
    let mut current: Vec<&Value> = vec![node.get(segments.next()?)?];
    for segment in segments {
        current = flatten(current).into_iter().filter_map(|value| value.as_object()?.get(segment)).collect();
    }
    let found: Vec<&Value> = flatten(current).into_iter().flat_map(connection_items).filter(|value| !value.is_null()).collect();
    Some(found)
}

fn flatten(values: Vec<&Value>) -> Vec<&Value> {
    values.into_iter().flat_map(|value| match value {
        Value::Array(items) => items.iter().collect(),
        other => vec![other],
    }).collect()
}

fn connection_items(value: &Value) -> Vec<&Value> {
    let Some(fields) = value.as_object() else { return vec![value] };
    if let Some(nodes) = fields.get("nodes").and_then(Value::as_array) {
        return nodes.iter().collect();
    }
    if let Some(edges) = fields.get("edges").and_then(Value::as_array) {
        return edges.iter().filter_map(|edge| edge.get("node")).collect();
    }
    vec![value]
}

pub(crate) fn normalize_node(db: &Db, model: &str, node: &Map<String, Value>, related: &mut Related) -> Result<Option<Map<String, Value>>, SyncError> {
    let input = nested_input(db, model, node, related)?;
    let mut patch = db.spec(model).map_err(|_| SyncError::UnknownModel(model.to_string()))?.patch(&input);
    db.shared.store.decode_values(model, &mut patch, crate::schema::read_path)?;
    Ok(patch.get("id").and_then(read_id).is_some().then_some(patch))
}
