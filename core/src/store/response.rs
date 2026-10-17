use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::identity::ModelKey;
use crate::value::read_id;

pub(crate) fn coalesce_rows(rows: impl IntoIterator<Item = (ModelKey, Map<String, Value>)>) -> Vec<(ModelKey, Map<String, Value>)> {
    let mut positions = BTreeMap::new();
    let mut merged: Vec<(ModelKey, Map<String, Value>)> = Vec::new();
    for (model, row) in rows {
        let Some(id) = row.get("id").and_then(read_id) else { continue };
        let index = *positions.entry((model.clone(), id)).or_insert_with(|| {
            merged.push((model, Map::new()));
            merged.len() - 1
        });
        merged[index].1.extend(row);
    }
    merged
}
