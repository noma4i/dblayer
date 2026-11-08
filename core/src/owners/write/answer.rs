//! The answer of the server to the request of an operation, read into the rows Outbox lands.
//! A pure function of the base (the declaration of the model) and the text of the answer.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::store::Store;
use crate::identity::ModelKey;
use crate::owners::read::codec::{extract, normalize_nodes, root_value};
use crate::owners::store::RowValue;
use crate::schema::{Coverage, RemoteShape};
use crate::value::{read_id, read_scalar, ScalarKind};

/// What the executor needs to read the answer of one operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodeJob {
    pub op_id: u64,
    pub model: String,
    pub action: String,
    /// The temporary id of a `create`: the answer must name the real row.
    pub temp: Option<String>,
    /// The group of a batch mutation: the answer lists its elements, each one replaces the temporary row with the same correlation key.
    pub batch: Option<u64>,
    pub body: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decoded {
    /// The rows of the answer by model and the pair of a swap.
    Land { rows: Vec<(String, Vec<RowValue>)>, swaps: Vec<(String, String)>, stale: Vec<(String, String)>, result: Option<String>, missing: Vec<String> },
    /// The server refused or the answer cannot be read. A temporary refusal is sent again.
    Refused { temporary: bool, detail: String },
}

fn refused(detail: impl Into<String>) -> Decoded { Decoded::Refused { temporary: false, detail: super::failure::of_graphql(detail.into(), None) } }

/// `root_of` gives the root field of a registered mutation.
pub fn decode_answer(db: &Store, root_of: &dyn Fn(&str) -> Option<String>, job: &DecodeJob) -> Decoded {
    if let Some(code) = serde_json::from_str::<Value>(&job.body).ok().and_then(|body| body.get("error").and_then(Value::as_str).map(str::to_string)) {
        return Decoded::Refused { temporary: crate::transport::encrypted::exchange::temporary_refusal(&job.body), detail: super::failure::of_graphql(code, None) };
    }
    let Ok(answer) = serde_json::from_str::<crate::transport::Response>(&job.body) else { return refused("the answer is not a GraphQL response") };
    if !answer.errors.is_empty() && job.action == "destroy" && answer.errors.iter().all(|error| error.code() == Some("NOT_FOUND")) {
        let stale = db.spec(&job.model).map(|spec| spec.relations.iter().filter(|relation| relation.remote.is_some()).map(|relation| (job.model.clone(), relation.name.clone())).collect()).unwrap_or_default();
        return Decoded::Land { rows: Vec::new(), swaps: Vec::new(), stale, result: None, missing: Vec::new() };
    }
    if !answer.errors.is_empty() {
        let messages: Vec<&str> = answer.errors.iter().map(|error| error.message.as_str()).collect();
        return Decoded::Refused { temporary: crate::transport::encrypted::exchange::temporary_refusal(&job.body), detail: super::failure::of_graphql(messages.join("; "), answer.extensions()) };
    }
    let data = answer.data.unwrap_or(Value::Null);
    let Ok(spec) = db.spec(&job.model) else { return refused(format!("unknown model {}", job.model)) };
    let declared = spec.actions.iter().find(|declared| declared.name == job.action);
    let stale: Vec<(String, String)> = declared.iter().flat_map(|declared| &declared.refetch).map(|name| name.split_once('.').map_or_else(|| (job.model.clone(), name.clone()), |(model, relation)| (model.to_string(), relation.to_string()))).collect();
    if job.action == "destroy" || declared.is_some_and(|declared| declared.ignore_response) {
        return Decoded::Land { rows: Vec::new(), swaps: Vec::new(), stale, result: None, missing: Vec::new() };
    }
    let default_root = declared.and_then(|declared| declared.mutation.as_deref()).and_then(root_of);
    let Some(root) = declared.and_then(|declared| declared.root.clone()).or(default_root) else { return refused(format!("{}.{} has no answer root", job.model, job.action)) };
    let root = root.as_str();
    if let Some(declared) = declared.filter(|declared| !declared.result.is_empty()) {
        if root_value(&data, root).is_none_or(Value::is_null) {
            let result = match returns_of(&data, default_root_of(declared.mutation.as_deref().and_then(root_of), root), &declared.result) {
                Ok(result) => result,
                Err(detail) => return refused(detail),
            };
            return Decoded::Land { rows: Vec::new(), swaps: Vec::new(), stale, result: Some(result), missing: Vec::new() };
        }
    }
    if !root_value(&data, root).is_some_and(|value| value.is_object() || value.is_array()) {
        return refused(format!("{root}: the answer has no row"));
    }
    let read = |model: &str, root: &str, shape: &RemoteShape| {
        let extracted = extract(&data, root, shape, None).map_err(|error| format!("{error:?}"))?;
        normalize_nodes(db, model, &extracted.nodes).map_err(|error| format!("{error:?}"))
    };
    let batch = declared.is_some_and(|declared| declared.batch.is_some());
    let shape = if batch { RemoteShape::List { coverage: Coverage::Complete } } else { RemoteShape::Single };
    let mut normalized = match read(&job.model, root, &shape) {
        Ok(normalized) => normalized,
        Err(detail) => return refused(detail),
    };
    for landing in declared.iter().flat_map(|declared| &declared.lands) {
        let shape = if root_value(&data, &landing.root).is_some_and(Value::is_array) { RemoteShape::List { coverage: Coverage::Complete } } else { RemoteShape::Single };
        match read(&landing.model, &landing.root, &shape) {
            Ok(landed) => {
                let model = ModelKey::new(&landing.model);
                normalized.related.extend(landed.roots.into_iter().map(|row| (model.clone(), row)));
                normalized.related.extend(landed.related);
            }
            Err(detail) => return refused(detail),
        }
    }
    normalized.coalesce(&job.model);
    let mut missing = Vec::new();
    let swaps = if batch {
        match batch_swaps(db, &job.model, declared, job, &normalized.roots) {
            Ok((swaps, absent)) => { missing = absent; swaps }
            Err(detail) => return refused(detail),
        }
    } else {
        let real = normalized.roots.first().and_then(|row| row.get("id")).and_then(read_id);
        match (&job.temp, real) {
            (Some(temp), Some(real)) if normalized.roots.len() == 1 && real.as_str() != temp => vec![(temp.clone(), real.to_string())],
            (Some(_), _) => return refused("the answer does not name the created row"),
            (None, _) => Vec::new(),
        }
    };
    let mut by_model: BTreeMap<String, Vec<RowValue>> = BTreeMap::new();
    let related = normalized.related.iter().map(|(model, row)| (model.to_string(), row));
    let roots = normalized.roots.iter().map(|row| (job.model.clone(), row));
    for (model, row) in related.chain(roots) {
        let Some(id) = row.get("id").and_then(read_id) else { continue };
        by_model.entry(model).or_default().push(RowValue { row_id: id.to_string(), values: Value::Object(row.clone()).to_string() });
    }
    let result = match declared.filter(|declared| !declared.result.is_empty()).map(|declared| returns_of(&data, default_root_of(declared.mutation.as_deref().and_then(root_of), root), &declared.result)).transpose() {
        Ok(result) => result,
        Err(detail) => return refused(detail),
    };
    Decoded::Land { rows: by_model.into_iter().collect(), swaps, stale, result, missing }
}

/// The object that carries the `returns` fields: the root field of the mutation, else the parent of the row root.
fn default_root_of(mutation_root: Option<String>, row_root: &str) -> String {
    mutation_root.unwrap_or_else(|| row_root.rsplit_once('.').map_or_else(|| row_root.to_string(), |(parent, _)| parent.to_string()))
}

/// The fields of the `returns` block read from the root of the answer: a field the answer lacks is null, an `id` is text.
fn returns_of(data: &Value, container: String, fields: &BTreeMap<String, String>) -> Result<String, String> {
    let source = root_value(data, &container);
    let mut values = serde_json::Map::new();
    for (name, kind) in fields {
        let kind = match kind.as_str() {
            "id" => ScalarKind::Id,
            "int" => ScalarKind::Int,
            "num" => ScalarKind::Num,
            "bool" => ScalarKind::Bool,
            _ => ScalarKind::Str,
        };
        let value = match source.and_then(|root| root.get(name)).filter(|value| !value.is_null()) {
            Some(value) => read_scalar(value, kind).ok_or_else(|| format!("returns {name}: the value does not match the declaration"))?,
            None => Value::Null,
        };
        values.insert(name.clone(), value);
    }
    Ok(Value::Object(values).to_string())
}

fn at<'a>(row: &'a serde_json::Map<String, Value>, path: &str) -> Option<&'a Value> {
    let mut segments = path.split('.');
    let mut value = row.get(segments.next()?)?;
    for segment in segments {
        value = value.as_object()?.get(segment)?;
    }
    let empty = match value {
        Value::Null => true,
        Value::String(text) => text.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(fields) => fields.is_empty(),
        _ => false,
    };
    (!empty).then_some(value)
}

fn correlation_key(paths: &[String], row: &serde_json::Map<String, Value>) -> Option<String> {
    let values: Option<Vec<Value>> = paths.iter().map(|path| at(row, path).cloned()).collect();
    values.map(|values| Value::Array(values).to_string())
}

type BatchSwaps = (Vec<(String, String)>, Vec<String>);

/// The pairs (temporary id, real id) of a batch answer: the waiting elements of the group against the rows of the answer.
fn batch_swaps(db: &Store, model: &str, declared: Option<&crate::schema::ActionDeclaration>, job: &DecodeJob, roots: &[serde_json::Map<String, Value>]) -> Result<BatchSwaps, String> {
    let Some(group) = job.batch else { return Err("the answer of a batch has no group".into()) };
    let (Some(declared), [path]) = (declared, declared.map_or(&[][..], |declared| &declared.correlate[..])) else { return Err("the batch has no single correlation field".into()) };
    let target = declared.batch.as_ref().and_then(|batch| batch.input.iter().find(|(_, source)| *source == path).map(|(target, _)| target.clone())).unwrap_or_else(|| path.clone());
    let waiting = db.batch_rows(model, &job.action, group).map_err(|error| format!("{error:?}"))?;
    let keyed: Vec<(String, serde_json::Map<String, Value>)> = waiting.into_iter().map(|(id, fields)| (id.to_string(), fields.get(&target).map(|value| serde_json::Map::from_iter([(path.clone(), value.clone())])).unwrap_or_default())).collect();
    let answered: Vec<String> = roots.iter().filter_map(|root| correlation_key(&declared.correlate, root)).collect();
    let absent = keyed.iter().filter(|(_, data)| correlation_key(&declared.correlate, data).is_none_or(|key| !answered.contains(&key))).map(|(id, _)| id.clone()).collect();
    Ok((pair_by_key(&declared.correlate, &keyed, roots), absent))
}

/// Each row of the answer replaces the single waiting temporary row with its correlation key (P-DATA-23), in the order of the answer. An element without a row of the answer keeps its temporary row.
pub(super) fn pair_by_key(correlate: &[String], waiting: &[(String, serde_json::Map<String, Value>)], roots: &[serde_json::Map<String, Value>]) -> Vec<(String, String)> {
    let keyed: Vec<(&str, Option<String>)> = waiting.iter().map(|(id, data)| (id.as_str(), correlation_key(correlate, data))).collect();
    let mut taken: Vec<&str> = Vec::new();
    let mut swaps = Vec::new();
    for root in roots {
        let (Some(real), Some(incoming)) = (root.get("id").and_then(read_id), correlation_key(correlate, root)) else { continue };
        let mut found = keyed.iter().filter(|(temp, key)| key.as_deref() == Some(incoming.as_str()) && !taken.contains(temp));
        if let (Some((temp, _)), None) = (found.next(), found.next()) {
            if *temp != real.as_str() {
                taken.push(temp);
                swaps.push(((*temp).to_string(), real.to_string()));
            }
        }
    }
    swaps
}
