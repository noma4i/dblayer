use serde_json::{Map, Value};

use super::{FieldKind, FieldSpec, ModelSpec};
use crate::value::ScalarKind;

pub type SearchPath = Vec<String>;

fn is_text(field: &FieldSpec) -> bool {
    matches!(field.kind, FieldKind::Scalar { kind: ScalarKind::Str } | FieldKind::List { element: ScalarKind::Str })
}

impl FieldSpec {
    pub(crate) fn check_searchable(&self) -> Result<(), super::DeclarationError> {
        if self.searchable && !is_text(self) {
            return Err(super::DeclarationError::SearchableNotText(self.name.clone()));
        }
        self.fields.iter().try_for_each(FieldSpec::check_searchable)
    }

    fn collect_search(&self, key: &str, prefix: &[String], out: &mut Vec<SearchPath>) {
        let mut path = prefix.to_vec();
        path.push(key.to_string());
        if self.searchable {
            out.push(path.clone());
        }
        for child in &self.fields {
            child.collect_search(child.from.as_deref().unwrap_or(&child.name), &path, out);
        }
    }
}

impl ModelSpec {
    /// The key paths of the text that a search reads, in declaration order. A path runs through objects and arrays.
    pub fn search_paths(&self) -> Vec<SearchPath> {
        let mut paths = Vec::new();
        self.fields.iter().for_each(|field| field.collect_search(&field.name, &[], &mut paths));
        paths
    }
}

fn gather(value: &Value, path: &[String], out: &mut Vec<String>) {
    match (value, path.split_first()) {
        (Value::String(text), None) => out.push(text.clone()),
        (Value::Array(items), _) => items.iter().for_each(|item| gather(item, path, out)),
        (Value::Object(fields), Some((key, rest))) => {
            if let Some(next) = fields.get(key) {
                gather(next, rest, out);
            }
        }
        _ => {}
    }
}

pub fn search_text(row: &Map<String, Value>, path: &[String]) -> Option<String> {
    let mut found = Vec::new();
    let (first, rest) = path.split_first()?;
    if let Some(value) = row.get(first) {
        gather(value, rest, &mut found);
    }
    (!found.is_empty()).then(|| found.join(" "))
}
