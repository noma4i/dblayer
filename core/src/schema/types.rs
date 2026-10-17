use std::collections::HashMap;

use super::{FieldKind, FieldSpec, ModelSpec};

fn collect(field: &FieldSpec, registry: &mut HashMap<String, Vec<FieldSpec>>) {
    if let (true, Some(name)) = (!field.fields.is_empty(), &field.app_type) {
        registry.entry(name.clone()).or_insert_with(|| field.fields.clone());
    }
    field.fields.iter().for_each(|child| collect(child, registry));
}

fn resolve(field: &mut FieldSpec, arrays: &mut Vec<String>, registry: &HashMap<String, Vec<FieldSpec>>) {
    if !matches!(field.kind, FieldKind::Object | FieldKind::Array | FieldKind::Union) {
        return;
    }
    let Some(name) = field.app_type.clone() else { return };
    let is_array = field.kind == FieldKind::Array;
    if field.fields.is_empty() {
        if is_array && arrays.contains(&name) {
            return;
        }
        match registry.get(&name) {
            Some(declared) => field.fields = declared.clone(),
            None => {
                field.app_type = None;
                return;
            }
        }
    }
    if is_array {
        arrays.push(name);
    }
    field.fields.iter_mut().for_each(|child| resolve(child, arrays, registry));
    if is_array {
        arrays.pop();
    }
}

/// Fills the members of every object and array that names a declared type without declaring it, in any model. A recursive array keeps no members: its name points at the enclosing array of the same type.
pub fn resolve_types(models: &mut [ModelSpec]) {
    let mut registry = HashMap::new();
    models.iter().flat_map(|model| model.fields.iter()).for_each(|field| collect(field, &mut registry));
    for model in models {
        model.fields.iter_mut().for_each(|field| resolve(field, &mut Vec::new(), &registry));
    }
}

impl FieldSpec {
    /// A value with a declared shape: a list, or an object or array of a named type.
    pub fn is_structured(&self) -> bool {
        match self.kind {
            FieldKind::List { .. } => true,
            FieldKind::Object | FieldKind::Array | FieldKind::Union => self.app_type.is_some(),
            _ => false,
        }
    }

}
