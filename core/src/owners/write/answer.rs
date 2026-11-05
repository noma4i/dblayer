//! The answer of the server to the request of an operation, read into the rows Outbox lands.
//! A pure function of the base (the declaration of the model) and the text of the answer.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::db::Db;
use crate::identity::ModelKey;
use crate::owners::read::codec::{extract, normalize_nodes, root_value};
use crate::owners::store::RowValue;
use crate::schema::{Coverage, RemoteShape};
use crate::value::read_id;

/// What the executor needs to read the answer of one operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodeJob {
    pub op_id: u64,
    pub model: String,
    pub action: String,
    /// The temporary id of a `create`: the answer must name the real row.
    pub temp: Option<String>,
    pub body: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decoded {
    /// The rows of the answer by model and the pair of a swap.
    Land { rows: Vec<(String, Vec<RowValue>)>, swap: Option<(String, String)> },
    /// The server refused or the answer cannot be read. A temporary refusal is sent again.
    Refused { temporary: bool, detail: String },
}

fn refused(detail: impl Into<String>) -> Decoded { Decoded::Refused { temporary: false, detail: super::failure::of_graphql(detail.into(), None) } }

/// `root_of` gives the root field of a registered mutation.
pub fn decode_answer(db: &Db, root_of: &dyn Fn(&str) -> Option<String>, job: &DecodeJob) -> Decoded {
    let Ok(answer) = serde_json::from_str::<crate::transport::Response>(&job.body) else { return refused("the answer is not a GraphQL response") };
    if !answer.errors.is_empty() {
        let messages: Vec<&str> = answer.errors.iter().map(|error| error.message.as_str()).collect();
        return Decoded::Refused { temporary: false, detail: super::failure::of_graphql(messages.join("; "), answer.extensions()) };
    }
    let data = answer.data.unwrap_or(Value::Null);
    let Ok(spec) = db.spec(&job.model) else { return refused(format!("unknown model {}", job.model)) };
    let declared = spec.actions.iter().find(|declared| declared.name == job.action);
    if job.action == "destroy" || declared.is_some_and(|declared| declared.ignore_response) {
        return Decoded::Land { rows: Vec::new(), swap: None };
    }
    let default_root = declared.and_then(|declared| declared.mutation.as_deref()).and_then(root_of);
    let Some(root) = declared.and_then(|declared| declared.root.clone()).or(default_root) else { return refused(format!("{}.{} has no answer root", job.model, job.action)) };
    let root = root.as_str();
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
    let real = normalized.roots.first().and_then(|row| row.get("id")).and_then(read_id);
    let swap = match (&job.temp, real) {
        (Some(temp), Some(real)) if normalized.roots.len() == 1 && real.as_str() != temp => Some((temp.clone(), real.to_string())),
        (Some(_), _) => return refused("the answer does not name the created row"),
        (None, _) => None,
    };
    let mut by_model: BTreeMap<String, Vec<RowValue>> = BTreeMap::new();
    let related = normalized.related.iter().map(|(model, row)| (model.to_string(), row));
    let roots = normalized.roots.iter().map(|row| (job.model.clone(), row));
    for (model, row) in related.chain(roots) {
        let Some(id) = row.get("id").and_then(read_id) else { continue };
        by_model.entry(model).or_default().push(RowValue { row_id: id.to_string(), values: Value::Object(row.clone()).to_string() });
    }
    Decoded::Land { rows: by_model.into_iter().collect(), swap }
}
