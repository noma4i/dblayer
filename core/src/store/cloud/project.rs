use crate::schema::{FieldKind, FieldSpec, MergeRule, ModelSpec};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

pub(in crate::store) fn project(spec: &ModelSpec, row: &Map<String, Value>) -> BTreeMap<Vec<String>, Value> {
    let mut result = BTreeMap::new();
    for field in &spec.fields {
        if field.name == "id" || field.derived || matches!(field.kind, FieldKind::File) || spec.ephemeral_field(&field.name).is_some() { continue; }
        if let Some(value) = row.get(&field.name) { visit(field, value, vec![field.name.clone()], &mut result); }
    }
    result
}

pub(in crate::store) fn field<'a>(spec: &'a ModelSpec, path: &[String]) -> Option<(&'a FieldSpec, Option<&'a MergeRule>, bool)> {
    let (first, rest) = path.split_first()?;
    let mut field = spec.fields.iter().find(|field| field.name == *first)?;
    if field.name == "id" || spec.ephemeral_field(&field.name).is_some() { return None; }
    let mut rule = field.merge.as_ref();
    let mut local = field.is_local();
    for segment in rest {
        if field.derived || !matches!(field.kind, FieldKind::Object | FieldKind::Raw) { return None; }
        let keyed = match rule { Some(MergeRule::Keys { keys }) => keys.get(segment), _ => None };
        if !field.fields.is_empty() {
            field = field.fields.iter().find(|child| child.from.as_ref().unwrap_or(&child.name) == segment)?;
            rule = keyed.or(field.merge.as_ref());
        } else {
            rule = keyed;
        }
        local |= matches!(rule, Some(MergeRule::Local));
    }
    if field.derived || matches!(field.kind, FieldKind::File) { return None; }
    Some((field, rule, local))
}

pub(in crate::store) fn set(row: &mut Map<String, Value>, path: &[String], value: Value) -> Result<(), crate::store::StoreError> {
    let (last, parents) = path.split_last().ok_or_else(|| crate::store::StoreError::Invalid("empty cloud path".into()))?;
    let mut current = row;
    for segment in parents {
        let item = current.entry(segment.clone()).or_insert_with(|| Value::Object(Map::new()));
        if !item.is_object() { *item = Value::Object(Map::new()); }
        let Value::Object(object) = item else { return Err(crate::store::StoreError::Invalid("invalid cloud path".into())); };
        current = object;
    }
    current.insert(last.clone(), value);
    Ok(())
}

fn visit(field: &FieldSpec, value: &Value, path: Vec<String>, result: &mut BTreeMap<Vec<String>, Value>) {
    if field.derived || matches!(field.kind, FieldKind::File) { return; }
    if matches!(field.merge, Some(MergeRule::Keys { .. })) && field.fields.is_empty() {
        if let Some(object) = value.as_object().filter(|object| !object.is_empty()) {
            for (key, value) in object {
                let mut child_path = path.clone();
                child_path.push(key.clone());
                result.insert(child_path, value.clone());
            }
            return;
        }
    }
    if matches!(field.kind, FieldKind::Object) && !field.fields.is_empty() {
        if let Some(object) = value.as_object().filter(|object| !object.is_empty()) {
            for child in &field.fields {
                let key = child.from.as_ref().unwrap_or(&child.name);
                if let Some(value) = object.get(key) {
                    let mut path = path.clone();
                    path.push(key.clone());
                    visit(child, value, path, result);
                }
            }
            return;
        }
    }
    result.insert(path, value.clone());
}

pub(super) fn changed(before: &BTreeMap<Vec<String>, Value>, after: &BTreeMap<Vec<String>, Value>) -> BTreeSet<Vec<String>> {
    let mut paths: BTreeSet<_> = after.iter().filter_map(|(path, value)| (before.get(path) != Some(value)).then(|| path.clone())).collect();
    for old in before.keys() {
        if (1..=old.len()).any(|length| after.contains_key(&old[..length])) { continue; }
        for length in 1..=old.len() {
            let path = old[..length].to_vec();
            if after.range(path.clone()..).next().is_some_and(|(next, _)| next.starts_with(&path)) { continue; }
            paths.insert(path);
            break;
        }
    }
    paths
}
