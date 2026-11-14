//! The wire form of the windows from the schema of the models: how a page is asked, how its answer reads as rows, which keys the `.dbl` declares `preload`.
//! The codec has no state: the same key and page give the same request.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::{json, Map, Value};

use super::windows::{AnswerPage, HasLocalOrder, HasMedia, Lifetimes, Marker, MaxPages, MembersComplete, OnForeground, PageRows, RequestKind, WindowKey, WindowKind};
use super::WindowCodec;
use crate::store::Store;
use crate::identity::RowId;
use crate::owners::store::RowValue;
use crate::owners::read::codec::EDGE_KEY;
use crate::schema::{Coverage, NodeRead, PreloadChoice, RemoteShape};
use super::codec::{declared_variables, extract, limited_combinations, load_variables, normalize_nodes, root_value, stamp_scope_values, Normalized};
use crate::transport::{GraphQLRequest, Response, ResponseOutcome};
use crate::value::read_id;

pub const DEFAULT_PAGE_SIZE: u32 = 20;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadOperation {
    pub name: String,
    pub document: String,
    pub root_path: String,
    pub page_size: u32,
}

impl ReadOperation {
    pub fn new(name: &str, document: &str, root_path: &str) -> Self { Self { name: name.into(), document: document.into(), root_path: root_path.into(), page_size: DEFAULT_PAGE_SIZE } }

    /// The root path is the response key of the first field of the document.
    pub fn from_document(name: &str, document: &str) -> Option<Self> { Some(Self::new(name, document, &first_field(document)?)) }

    pub fn paged(self, page_size: u32) -> Self { Self { page_size, ..self } }
}

fn first_field(document: &str) -> Option<String> {
    let mut depth = 0usize;
    let mut body = None;
    for (index, character) in document.char_indices() {
        match character {
            '(' | '[' => depth += 1,
            ')' | ']' => depth = depth.saturating_sub(1),
            '{' if depth == 0 => {
                body = Some(&document[index + 1..]);
                break;
            }
            _ => {}
        }
    }
    let body = body?.trim_start();
    let end = body.find(|character: char| !(character.is_alphanumeric() || character == '_')).unwrap_or(body.len());
    (end > 0).then(|| body[..end].to_string())
}

#[derive(Clone, Default)]
pub struct ReadOperations(Arc<arc_swap::ArcSwap<std::collections::HashMap<String, Arc<ReadOperation>>>>);

impl ReadOperations {
    pub fn declare(&self, operations: impl IntoIterator<Item = ReadOperation>) {
        let operations: Vec<Arc<ReadOperation>> = operations.into_iter().map(Arc::new).collect();
        self.0.rcu(|known| {
            let mut next = std::collections::HashMap::clone(known);
            next.extend(operations.iter().map(|operation| (operation.name.clone(), operation.clone())));
            next
        });
    }

    pub fn is_empty(&self) -> bool { self.0.load().is_empty() }

    pub fn get(&self, name: &str) -> Option<Arc<ReadOperation>> { self.0.load().get(name).cloned() }
}

pub struct SchemaWindowCodec {
    db: std::sync::Arc<Store>,
    operations: ReadOperations,
}

impl SchemaWindowCodec {
    pub fn new(db: std::sync::Arc<Store>, operations: ReadOperations) -> Self { Self { db, operations } }

    fn operation(&self, name: &str) -> Result<Arc<ReadOperation>, String> { self.operations.get(name).ok_or_else(|| format!("operation {name} is not registered")) }

    fn nodes_relation(&self, model: &str) -> Result<&crate::schema::RelationSpec, String> {
        let spec = self.db.spec(model).map_err(|error| error.to_string())?;
        spec.relations.iter().find(|relation| relation.node == Some(NodeRead::Many)).ok_or_else(|| format!("{model} declares no nodes read"))
    }
}

fn row_value(row: &Map<String, Value>) -> Option<(RowId, Map<String, Value>)> { Some((row.get("id").and_then(read_id)?, row.clone())) }

fn page_rows(normalized: Normalized, total: Option<i64>) -> (Vec<String>, PageRows) {
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
    fn request(&self, key: &WindowKey, _page: u32, cursor: Option<&str>, kind: RequestKind) -> Result<GraphQLRequest, String> {
        match key.kind()? {
            WindowKind::Relation { relation, params } => {
                let spec = self.db.spec(&key.model).map_err(|error| error.to_string())?;
                let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| format!("{} has no relation {relation}", key.model))?;
                let remote = declared.remote.as_ref().ok_or_else(|| format!("{}.{relation} is no remote relation", key.model))?;
                let media = || declared.media.as_ref().ok_or_else(|| format!("{}.{relation} declares no media", key.model));
                let operation = self.operation(match kind {
                    RequestKind::Page => &remote.operation,
                    RequestKind::Marker { .. } => &media()?.marker,
                    RequestKind::Tail { .. } => &media()?.tail,
                })?;
                let page_size = remote.page_size.unwrap_or(operation.page_size);
                match kind {
                    RequestKind::Page => Ok(GraphQLRequest::new(&operation.document, &operation.name, load_variables(&params, &remote.shape, page_size, cursor, &remote.variables))),
                    RequestKind::Marker { up_to } => {
                        let mut variables = declared_variables(&params, &media()?.marker_variables);
                        variables.insert("upToSequence".into(), json!(up_to));
                        Ok(GraphQLRequest::new(&operation.document, &operation.name, Value::Object(variables)))
                    }
                    RequestKind::Tail { after } => {
                        let media = media()?;
                        let mut tail_params = declared_variables(&params, &media.tail_variables);
                        tail_params.insert(media.after.clone(), json!(after));
                        Ok(GraphQLRequest::new(&operation.document, &operation.name, load_variables(&tail_params, &remote.shape, page_size, cursor, &BTreeMap::new())))
                    }
                }
            }
            WindowKind::Nodes { ids } => {
                let remote = self.nodes_relation(&key.model)?.remote.as_ref().ok_or_else(|| format!("the nodes read of {} is no remote relation", key.model))?;
                let operation = self.operation(&remote.operation)?;
                Ok(GraphQLRequest::new(&operation.document, &operation.name, json!({ "ids": ids })))
            }
        }
    }

    fn answer(&self, key: &WindowKey, page: u32, kind: RequestKind, response: Response) -> Result<AnswerPage, String> {
        if response.shape().outcome() != ResponseOutcome::Commit {
            let messages: Vec<String> = response.errors.iter().map(|error| error.message.clone()).collect();
            return Err(if messages.is_empty() { "empty response".to_string() } else { messages.join("; ") });
        }
        let data = response.data.ok_or("response has no data")?;
        let model = key.model.as_str();
        let spec = self.db.spec(model).map_err(|error| error.to_string())?;
        if matches!(kind, RequestKind::Marker { .. }) {
            return marker_answer(spec, key, &data).map(AnswerPage::marked);
        }
        let (declared, params, shape, root, total_from) = match key.kind()? {
            WindowKind::Relation { relation, params } => {
                let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| format!("{model} has no relation {relation}"))?;
                let remote = declared.remote.as_ref().ok_or_else(|| format!("{model}.{relation} is no remote relation"))?;
                let operation = self.operation(match (&kind, &declared.media) {
                    (RequestKind::Tail { .. }, Some(media)) => &media.tail,
                    _ => &remote.operation,
                })?;
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
        let more = extracted.page_info.as_ref().is_some_and(|info| info.has_next_page);
        let cursor = extracted.page_info.as_ref().filter(|info| info.has_next_page).and_then(|info| info.end_cursor.clone());
        let total = extracted.page_info.as_ref().and_then(|info| info.total);
        let (rows, payload) = page_rows(normalized, total);
        Ok(AnswerPage { page, rows, cursor, dependency_models, payload, marker: None, more })
    }
}

fn media_integer(value: &Value, field: &str) -> Result<i64, String> {
    value.as_i64().or_else(|| value.as_str().and_then(|text| text.parse().ok())).ok_or_else(|| format!("marker {field} must be an integer"))
}

/// The marker of a media window from the answer of its marker operation: the newest sequence and the counts the fields name.
fn marker_answer(spec: &crate::schema::ModelSpec, key: &WindowKey, data: &Value) -> Result<Marker, String> {
    let WindowKind::Relation { relation, .. } = key.kind()? else { return Err("a marker belongs to a relation window".into()) };
    let media = spec.relations.iter().find(|candidate| candidate.name == relation).and_then(|declared| declared.media.as_ref()).ok_or("the relation declares no media")?;
    let row = root_value(data, &media.from).and_then(Value::as_object).ok_or_else(|| format!("marker {} is not an object", media.from))?;
    let get = |name: &str| media_integer(row.get(name).ok_or_else(|| format!("marker has no {name}"))?, name);
    let sequence = get(&media.sequence)?;
    if sequence < 0 { return Err("marker sequence must not be negative".into()); }
    let values = media.fields.iter().map(|field| Ok((field.clone(), get(field)?))).collect::<Result<_, String>>()?;
    Ok(Marker { sequence, values })
}

/// The keys of the relations the `.dbl` declares with `media`.
pub fn has_media(db: std::sync::Arc<Store>) -> HasMedia {
    Arc::new(move |key| match key.kind() {
        Ok(WindowKind::Relation { relation, .. }) => db.spec(&key.model).ok().and_then(|spec| spec.relations.iter().find(|candidate| candidate.name == relation)).is_some_and(|declared| declared.media.is_some()),
        Ok(WindowKind::Nodes { .. }) | Err(_) => false,
    })
}

/// How many pages a key may land at most: `max_pages` of the connection in the `.dbl`.
pub fn max_pages(db: std::sync::Arc<Store>) -> MaxPages {
    Arc::new(move |key| match key.kind() {
        Ok(WindowKind::Relation { relation, .. }) => db
            .spec(&key.model)
            .ok()
            .and_then(|spec| spec.relations.iter().find(|candidate| candidate.name == relation))
            .and_then(|declared| declared.remote.as_ref())
            .and_then(|remote| match remote.shape {
                RemoteShape::Connection { max_pages, .. } => max_pages,
                _ => None,
            })
            .unwrap_or(u32::MAX),
        Ok(WindowKind::Nodes { .. }) | Err(_) => 1,
    })
}

/// `expires_in` and `empty_in` of the relation of a key in milliseconds; a policy that is no duration (`paged_session`) never expires.
pub fn lifetimes(db: std::sync::Arc<Store>) -> Lifetimes {
    Arc::new(move |key| {
        let never = i64::MAX / 4;
        let millis = |text: &str| crate::runtime::parse_duration(text).map_or(never, |window| i64::try_from(window.as_millis()).unwrap_or(never));
        let Ok(WindowKind::Relation { relation, .. }) = key.kind() else { return (never, never) };
        let Some(remote) = db.spec(&key.model).ok().and_then(|spec| spec.relations.iter().find(|candidate| candidate.name == relation)).and_then(|declared| declared.remote.as_ref()) else { return (never, never) };
        let expires = millis(&remote.stale_time);
        (expires, remote.empty_stale_time.as_deref().map_or(expires, millis))
    })
}

/// The keys of the relations the `.dbl` declares `members_complete`.
pub fn members_complete(db: std::sync::Arc<Store>) -> MembersComplete {
    Arc::new(move |key| match key.kind() {
        Ok(WindowKind::Relation { relation, .. }) => db.spec(&key.model).ok().and_then(|spec| spec.relations.iter().find(|candidate| candidate.name == relation)).is_some_and(|declared| declared.members_complete),
        Ok(WindowKind::Nodes { .. }) | Err(_) => false,
    })
}

pub fn has_local_order(db: std::sync::Arc<Store>) -> HasLocalOrder {
    Arc::new(move |key| match key.kind() {
        Ok(WindowKind::Relation { relation, .. }) => db.spec(&key.model).ok().and_then(|spec| spec.relations.iter().find(|candidate| candidate.name == relation)).is_some_and(|declared| matches!(declared.sort, crate::schema::Sort::Fields(_))),
        Ok(WindowKind::Nodes { .. }) | Err(_) => false,
    })
}

/// The keys of the relations the `.dbl` declares `refresh: foreground`.
pub fn refreshes_on_foreground(db: std::sync::Arc<Store>) -> OnForeground {
    Arc::new(move |key| match key.kind() {
        Ok(WindowKind::Relation { relation, .. }) => db.spec(&key.model).ok().and_then(|spec| spec.relations.iter().find(|candidate| candidate.name == relation)).is_some_and(|declared| declared.refresh_on_foreground),
        Ok(WindowKind::Nodes { .. }) | Err(_) => false,
    })
}

/// The keys of the relations declared `preload`. `derived = false`: only the relations whose every choice is listed (known at `EpochStarted`).
/// `derived = true`: every relation, the choices `RowsOf` read the first ids of their source scope.
pub fn preload_keys(db: &Store, derived: bool) -> Result<Vec<WindowKey>, String> {
    let mut keys = Vec::new();
    for (model, spec) in db.specs() {
        for relation in &spec.relations {
            let Some(values) = &relation.preload else { continue };
            let declared = spec.selection_params(&relation.name);
            for (name, choice) in values {
                if !declared.contains(name) { return Err(format!("unknown preload parameter {model}.{}.{name}", relation.name)); }
                if let PreloadChoice::RowsOf { from } = choice {
                    let source = db.spec(&from.model).map_err(|error| error.to_string())?;
                    if !source.relations.iter().any(|candidate| candidate.name == from.relation) {
                        return Err(format!("unknown preload selection {}.{}", from.model, from.relation));
                    }
                }
            }
            if relation.preload_order.iter().any(|name| !values.contains_key(name)) {
                return Err(format!("unknown preload parameter in {model}.{}", relation.name));
            }
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
                        let ids = db.read(&[from.model.as_str()], |reader| reader.scope_first_ids(&from.model, scope.as_str(), limit)).map_err(|error| error.to_string())?;
                        let mut resolved = Vec::with_capacity(ids.len());
                        for id in ids {
                            let row = crate::identity::RowId::new(&id).map_err(|error| error.to_string())?;
                            let row = db.resolve(&from.model, &row).map_err(|error| error.to_string())?;
                            if !row.is_temp() { resolved.push(Value::String(row.as_str().to_string())); }
                        }
                        resolved
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
pub fn preload_sources(db: &Store) -> Vec<WindowKey> {
    let sources: BTreeSet<(String, String)> = db
        .specs()
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
