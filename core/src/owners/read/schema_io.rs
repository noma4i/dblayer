//! The wire form of the windows from the schema of the models: how a page is asked, how its answer reads as rows, which keys the `.dbl` declares `preload`.
//! The codec has no state: the same key and page give the same request.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::{json, Map, Value};

use super::windows::{AnswerPage, MaxPages, OnForeground, PageRows, WindowKey, WindowKind};
use super::WindowCodec;
use crate::db::Db;
use crate::identity::RowId;
use crate::owners::store::RowValue;
use crate::read::EDGE_KEY;
use crate::schema::{Coverage, NodeRead, PreloadChoice, RemoteShape};
use crate::sync::{extract, limited_combinations, load_variables, normalize_nodes, stamp_scope_values, OperationDef};
use crate::transport::{GraphQLRequest, Response, ResponseOutcome};
use crate::value::read_id;

/// The operation of a name, with its document and root path.
pub type Operations = Arc<dyn Fn(&str) -> Option<Arc<OperationDef>> + Send + Sync>;

pub struct SchemaWindowCodec {
    db: Db,
    operations: Operations,
}

impl SchemaWindowCodec {
    pub fn new(db: Db, operations: Operations) -> Self { Self { db, operations } }

    fn operation(&self, name: &str) -> Result<Arc<OperationDef>, String> { (self.operations)(name).ok_or_else(|| format!("operation {name} is not registered")) }

    fn nodes_relation(&self, model: &str) -> Result<&crate::schema::RelationSpec, String> {
        let spec = self.db.spec(model).map_err(|error| error.to_string())?;
        spec.relations.iter().find(|relation| relation.node == Some(NodeRead::Many)).ok_or_else(|| format!("{model} declares no nodes read"))
    }
}

fn row_value(row: &Map<String, Value>) -> Option<(RowId, Map<String, Value>)> { Some((row.get("id").and_then(read_id)?, row.clone())) }

fn page_rows(normalized: crate::sync::Normalized, total: Option<i64>) -> (Vec<String>, PageRows) {
    let mut ids = Vec::with_capacity(normalized.roots.len());
    let mut roots = Vec::with_capacity(normalized.roots.len());
    for root in &normalized.roots {
        let Some((id, mut values)) = row_value(root) else { continue };
        if let Some(edge) = normalized.edges.get(id.as_str()) {
            values.insert(EDGE_KEY.to_string(), Value::Object(edge.clone()));
        }
        ids.push(id.as_str().to_string());
        roots.push(RowValue { row_id: id.as_str().to_string(), values: Value::Object(values).to_string() });
    }
    let related = normalized.related.iter().filter_map(|(model, row)| row_value(row).map(|(id, values)| (model.as_str().to_string(), RowValue { row_id: id.as_str().to_string(), values: Value::Object(values).to_string() }))).collect();
    (ids, PageRows { roots, related, total })
}

impl WindowCodec for SchemaWindowCodec {
    fn request(&self, key: &WindowKey, _page: u32, cursor: Option<&str>) -> Result<GraphQLRequest, String> {
        match key.kind() {
            WindowKind::Relation { relation, params } => {
                let spec = self.db.spec(&key.model).map_err(|error| error.to_string())?;
                let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| format!("{} has no relation {relation}", key.model))?;
                let remote = declared.remote.as_ref().ok_or_else(|| format!("{}.{relation} is no remote relation", key.model))?;
                let operation = self.operation(&remote.operation)?;
                let page_size = remote.page_size.unwrap_or(operation.page_size);
                Ok(GraphQLRequest::new(&operation.document, &operation.name, load_variables(&params, &remote.shape, page_size, cursor, &remote.variables)))
            }
            WindowKind::Nodes { ids } => {
                let remote = self.nodes_relation(&key.model)?.remote.as_ref().ok_or_else(|| format!("the nodes read of {} is no remote relation", key.model))?;
                let operation = self.operation(&remote.operation)?;
                Ok(GraphQLRequest::new(&operation.document, &operation.name, json!({ "ids": ids })))
            }
        }
    }

    fn answer(&self, key: &WindowKey, page: u32, response: Response) -> Result<AnswerPage, String> {
        if response.shape().outcome() != ResponseOutcome::Commit {
            let messages: Vec<String> = response.errors.iter().map(|error| error.message.clone()).collect();
            return Err(if messages.is_empty() { "empty response".to_string() } else { messages.join("; ") });
        }
        let data = response.data.ok_or("response has no data")?;
        let model = key.model.as_str();
        let spec = self.db.spec(model).map_err(|error| error.to_string())?;
        let (declared, params, shape, root, total_from) = match key.kind() {
            WindowKind::Relation { relation, params } => {
                let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| format!("{model} has no relation {relation}"))?;
                let remote = declared.remote.as_ref().ok_or_else(|| format!("{model}.{relation} is no remote relation"))?;
                let operation = self.operation(&remote.operation)?;
                (declared, params, remote.shape.clone(), remote.root.clone().unwrap_or_else(|| operation.root_path.clone()), remote.total_from.clone())
            }
            WindowKind::Nodes { .. } => {
                let declared = self.nodes_relation(model)?;
                let remote = declared.remote.as_ref().ok_or_else(|| format!("the nodes read of {model} is no remote relation"))?;
                let operation = self.operation(&remote.operation)?;
                (declared, Map::new(), RemoteShape::List { coverage: Coverage::Complete }, remote.root.clone().unwrap_or_else(|| operation.root_path.clone()), None)
            }
        };
        let extracted = extract(&data, &root, &shape, total_from.as_deref()).map_err(|error| error.to_string())?;
        let scope_values: Map<String, Value> = declared.by_columns.iter().filter_map(|column| params.get(column).map(|value| (column.clone(), value.clone()))).collect();
        let stamped = stamp_scope_values(extracted.nodes, &scope_values);
        let normalized = normalize_nodes(&self.db, model, &stamped).map_err(|error| error.to_string())?;
        let mut dependency_models: BTreeSet<String> = normalized.related.iter().map(|(related, _)| related.as_str().to_string()).collect();
        dependency_models.insert(model.to_string());
        let cursor = extracted.page_info.as_ref().filter(|info| info.has_next_page).and_then(|info| info.end_cursor.clone());
        let total = extracted.page_info.as_ref().and_then(|info| info.total);
        let (rows, payload) = page_rows(normalized, total);
        Ok(AnswerPage { page, rows, cursor, dependency_models, payload })
    }
}

/// How many pages a key may land at most: `max_pages` of the connection in the `.dbl`.
pub fn max_pages(db: Db) -> MaxPages {
    Arc::new(move |key| match key.kind() {
        WindowKind::Relation { relation, .. } => db
            .spec(&key.model)
            .ok()
            .and_then(|spec| spec.relations.iter().find(|candidate| candidate.name == relation))
            .and_then(|declared| declared.remote.as_ref())
            .and_then(|remote| match remote.shape {
                RemoteShape::Connection { max_pages, .. } => max_pages,
                _ => None,
            })
            .unwrap_or(u32::MAX),
        WindowKind::Nodes { .. } => 1,
    })
}

/// The keys of the relations the `.dbl` declares `refresh: foreground`.
pub fn refreshes_on_foreground(db: Db) -> OnForeground {
    Arc::new(move |key| match key.kind() {
        WindowKind::Relation { relation, .. } => db.spec(&key.model).ok().and_then(|spec| spec.relations.iter().find(|candidate| candidate.name == relation)).is_some_and(|declared| declared.refresh_on_foreground),
        WindowKind::Nodes { .. } => false,
    })
}

/// The keys of the relations declared `preload`. `derived = false`: only the relations whose every choice is listed (known at `EpochStarted`).
/// `derived = true`: every relation, the choices `RowsOf` read the first ids of their source scope.
pub fn preload_keys(db: &Db, derived: bool) -> Result<Vec<WindowKey>, String> {
    let mut keys = Vec::new();
    for (model, spec) in &db.shared.specs {
        for relation in &spec.relations {
            let Some(values) = &relation.preload else { continue };
            let has_source = values.values().any(|choice| matches!(choice, PreloadChoice::RowsOf { .. }));
            if has_source != derived {
                continue;
            }
            let limit = relation.preload_limit.unwrap_or(usize::MAX);
            if limit == 0 {
                return Err(format!("the preload limit of {model}.{} must be positive", relation.name));
            }
            let names: Vec<&String> = if relation.preload_order.is_empty() { values.keys().collect() } else { relation.preload_order.iter().collect() };
            let mut parameters = Vec::new();
            for name in names {
                let choices = match &values[name] {
                    PreloadChoice::Listed(listed) => listed.clone(),
                    PreloadChoice::RowsOf { from } => {
                        if relation.preload_limit.is_none() {
                            return Err(format!("the derived preload of {model}.{} needs a limit", relation.name));
                        }
                        let source = db.spec(&from.model).map_err(|error| error.to_string())?;
                        let scope = source.selection_key(&from.relation, &Map::new()).map_err(|error| error.to_string())?;
                        let ids = db.shared.store.read(&[from.model.as_str()], |reader| reader.scope_first_ids(&from.model, scope.as_str(), limit)).map_err(|error| error.to_string())?;
                        ids.into_iter().map(Value::String).collect()
                    }
                };
                parameters.push((name, choices));
            }
            let mut combinations = Vec::new();
            limited_combinations(&parameters, 0, &mut Map::new(), limit, &mut combinations);
            keys.extend(combinations.into_iter().map(|params| WindowKey::relation(model, &relation.name, &params)));
        }
    }
    Ok(keys)
}

/// The keys whose rows name other keys to preload: the sources of the choices `RowsOf`.
pub fn preload_sources(db: &Db) -> Vec<WindowKey> {
    let sources: BTreeSet<(String, String)> = db
        .shared
        .specs
        .values()
        .flat_map(|spec| &spec.relations)
        .filter_map(|relation| relation.preload.as_ref())
        .flat_map(BTreeMap::values)
        .filter_map(|choice| match choice {
            PreloadChoice::RowsOf { from } => Some((from.model.clone(), from.relation.clone())),
            PreloadChoice::Listed(_) => None,
        })
        .collect();
    sources.into_iter().map(|(model, relation)| WindowKey::relation(&model, &relation, &Map::new())).collect()
}
