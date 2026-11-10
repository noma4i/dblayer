//! A nested object that declares `ref: Model, id: :field` is read from the row of `Model` at the revision of the read (D-76).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::schema::{FieldKind, FieldSpec, ModelSpec};
use crate::store::Reader;

use crate::store::CoreError;
use crate::store::Store;

type FetchedRow = Option<(Map<String, Value>, i64)>;

fn stored_name(declared: &[&FieldSpec], name: &str) -> String { declared.iter().find(|field| field.name == name).and_then(|field| field.from.clone()).unwrap_or_else(|| name.to_string()) }

fn targets_of(fields: &[FieldSpec], found: &mut BTreeSet<String>) {
    for field in fields {
        if let Some(link) = &field.link {
            found.insert(link.model.clone());
        }
        targets_of(&field.fields, found);
        targets_of(&field.shared, found);
    }
}

/// The models whose rows the nested references of `spec` read.
pub(crate) fn link_targets(spec: &ModelSpec) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    targets_of(&spec.fields, &mut found);
    found
}

fn snapshot(nested: &FieldSpec, id: &str, row: &Map<String, Value>) -> Map<String, Value> {
    nested
        .fields
        .iter()
        .filter_map(|child| {
            let column = child.from.clone().unwrap_or_else(|| child.name.clone());
            let value = if column == "id" { Value::String(id.to_string()) } else { row.get(&column).filter(|value| !value.is_null())?.clone() };
            Some((column, value))
        })
        .collect()
}

type Fetched = BTreeMap<(String, String), Option<(Map<String, Value>, i64)>>;
type Fetch<'a> = dyn FnMut(&str, &str) -> Result<Option<(Map<String, Value>, i64)>, CoreError> + 'a;

fn walk(fields: &[FieldSpec], shared: &[FieldSpec], value: &mut Value, fetch: &mut Fetch<'_>) -> Result<(), CoreError> {
    match value {
        Value::Array(items) => items.iter_mut().try_for_each(|item| walk(fields, shared, item, fetch)),
        Value::Object(map) => {
            let declared: Vec<&FieldSpec> = shared.iter().chain(fields).collect();
            for nested in declared.iter().filter(|field| matches!(field.kind, FieldKind::Object | FieldKind::Array)) {
                let stored = stored_name(&declared, &nested.name);
                if let Some(link) = &nested.link {
                    let id = map.get(&stored_name(&declared, &link.id)).and_then(Value::as_str).filter(|id| !id.is_empty()).map(str::to_string);
                    if let Some((row, _)) = id.as_deref().map(|id| fetch(&link.model, id)).transpose()?.flatten() {
                        map.insert(stored, Value::Object(snapshot(nested, id.as_deref().unwrap_or_default(), &row)));
                    }
                } else if let Some(child) = map.get_mut(&stored) {
                    walk(&nested.fields, &nested.shared, child, fetch)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

impl Store {
    /// Replaces the nested records that reference a row by the current row at the revision of `reader`. A row that is not there keeps the record as written.
    /// Gives the newest revision among the rows read: the stamp of the row changes with them.
    pub(crate) fn resolve_nested_refs(&self, reader: &Reader<'_>, spec: &ModelSpec, rows: &mut HashMap<String, crate::store::record::Row>) -> Result<(), CoreError> {
        let mut keys: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut collect = |model: &str, id: &str| -> Result<FetchedRow, CoreError> {
            keys.entry(model.to_string()).or_default().insert(id.to_string());
            Ok(None)
        };
        for row in rows.values_mut() {
            let data = &mut Arc::make_mut(row).data;
            for field in spec.fields.iter().filter(|field| matches!(field.kind, FieldKind::Object | FieldKind::Array)) {
                let name = field.from.clone().unwrap_or_else(|| field.name.clone());
                if let Some(value) = data.get_mut(&name) { walk(&field.fields, &field.shared, value, &mut collect)?; }
            }
        }
        let snapshot_revision = reader.revision_seq()?;
        let mut fetched = Fetched::new();
        for (model, ids) in keys {
            let ids: Vec<String> = ids.into_iter().collect();
            let mut stored = reader.stored_rows(&model, &ids)?;
            for id in ids {
                let row = stored.remove(&id).map(|row| (row.row, row.revision));
                fetched.insert((model.clone(), id), row);
            }
        }
        for row in rows.values_mut() {
            let row = Arc::make_mut(row);
            let mut newest = 0;
            let mut fetch = |model: &str, id: &str| -> Result<FetchedRow, CoreError> {
                let value = fetched.get(&(model.to_string(), id.to_string())).cloned().flatten();
                newest = newest.max(value.as_ref().map_or(snapshot_revision, |(_, revision)| *revision));
                Ok(value)
            };
            for field in spec.fields.iter().filter(|field| matches!(field.kind, FieldKind::Object | FieldKind::Array)) {
                let name = field.from.clone().unwrap_or_else(|| field.name.clone());
                if let Some(value) = row.data.get_mut(&name) { walk(&field.fields, &field.shared, value, &mut fetch)?; }
            }
            row.revision = row.revision.max(newest);
        }
        Ok(())
    }
}
