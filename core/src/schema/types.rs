use std::collections::HashMap;

use serde_json::{Map, Value};

use super::{FieldKind, FieldSpec, ModelSpec, TYPENAME};

type Registry = HashMap<String, (Vec<FieldSpec>, Vec<FieldSpec>)>;

fn collect(field: &FieldSpec, registry: &mut Registry) {
    if let (true, Some(name)) = (!field.fields.is_empty(), &field.app_type) {
        registry.entry(name.clone()).or_insert_with(|| (field.fields.clone(), field.shared.clone()));
    }
    field.fields.iter().chain(field.shared.iter()).for_each(|child| collect(child, registry));
}

fn resolve(field: &mut FieldSpec, arrays: &mut Vec<String>, registry: &Registry) {
    if !matches!(field.kind, FieldKind::Object | FieldKind::Array | FieldKind::Union | FieldKind::UnionArray) {
        return;
    }
    let Some(name) = field.app_type.clone() else { return };
    let is_array = field.kind == FieldKind::Array;
    if field.fields.is_empty() {
        if is_array && arrays.contains(&name) {
            return;
        }
        match registry.get(&name) {
            Some((declared, shared)) => {
                field.fields = declared.clone();
                field.shared = shared.clone();
            }
            None => {
                field.app_type = None;
                return;
            }
        }
    }
    if is_array {
        arrays.push(name);
    }
    let variants = matches!(field.kind, FieldKind::Union | FieldKind::UnionArray);
    for child in field.fields.iter_mut() {
        if variants {
            if let (true, Some((declared, _))) = (child.fields.is_empty(), child.app_type.as_ref().and_then(|name| registry.get(name))) {
                child.fields = declared.clone();
            }
            child.fields.iter_mut().for_each(|member| resolve(member, arrays, registry));
        } else {
            resolve(child, arrays, registry);
        }
    }
    field.shared.iter_mut().for_each(|child| resolve(child, arrays, registry));
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
    pub fn is_structured(&self) -> bool {
        match self.kind {
            FieldKind::List { .. } => true,
            FieldKind::Object | FieldKind::Array | FieldKind::Union | FieldKind::UnionArray => self.app_type.is_some(),
            _ => false,
        }
    }

}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueFault {
    Unknown(String),
    Invalid(String),
}

type Declared<'a> = Vec<(&'a str, &'a [FieldSpec])>;

fn members_fault<'a>(groups: &[&'a [FieldSpec]], object: &Map<String, Value>, path: &str, typed: bool, declared: &mut Declared<'a>) -> Option<ValueFault> {
    for (key, value) in object {
        if key == TYPENAME {
            continue;
        }
        let at = format!("{path}.{key}");
        let Some(member) = groups.iter().flat_map(|group| group.iter()).find(|member| member.name == *key || member.from.as_deref() == Some(key)) else {
            return Some(ValueFault::Unknown(at));
        };
        if let Some(fault) = member.fault(value, &at, typed, declared) {
            return Some(fault);
        }
    }
    None
}

impl FieldSpec {
    fn union_fault<'a>(&'a self, object: &Map<String, Value>, path: &str, typed: bool, declared: &mut Declared<'a>) -> Option<ValueFault> {
        let typename = object.get(TYPENAME).and_then(Value::as_str)?;
        let variant = self.fields.iter().find(|variant| variant.app_type.as_deref() == Some(typename))?;
        members_fault(&[&self.shared, &variant.fields], object, path, typed, declared)
    }

    pub(crate) fn fault<'a>(&'a self, value: &Value, path: &str, typed: bool, declared: &mut Declared<'a>) -> Option<ValueFault> {
        if value.is_null() {
            return None;
        }
        if typed && self.decode_written(value).is_none() {
            return Some(ValueFault::Invalid(path.to_string()));
        }
        match &self.kind {
            FieldKind::Object if !self.fields.is_empty() => value.as_object().and_then(|object| members_fault(&[&self.fields], object, path, typed, declared)),
            FieldKind::Union => value.as_object().and_then(|object| self.union_fault(object, path, typed, declared)),
            FieldKind::UnionArray => value.as_array()?.iter().enumerate().find_map(|(index, item)| item.as_object().and_then(|object| self.union_fault(object, &format!("{path}[{index}]"), typed, declared))),
            FieldKind::List { element } => value.as_array()?.iter().enumerate().find_map(|(index, item)| (typed && crate::value::read_written_scalar(item, *element).is_none()).then(|| ValueFault::Invalid(format!("{path}[{index}]")))),
            FieldKind::Array => {
                let name = self.app_type.as_deref();
                let members: &'a [FieldSpec] = if self.fields.is_empty() {
                    name.and_then(|name| declared.iter().rev().find(|(known, _)| *known == name)).map_or(&[][..], |(_, members)| *members)
                } else {
                    &self.fields
                };
                if members.is_empty() {
                    return None;
                }
                declared.push((name.unwrap_or_default(), members));
                let found = value.as_array()?.iter().enumerate().find_map(|(index, item)| {
                    let at = format!("{path}[{index}]");
                    match item.as_object() {
                        Some(object) => members_fault(&[members], object, &at, typed, declared),
                        None => typed.then_some(ValueFault::Invalid(at)),
                    }
                });
                declared.pop();
                found
            }
            _ => None,
        }
    }
}

impl ModelSpec {
    /// First unknown key (any depth) or, when `typed`, first value that does not fit its declared type. Paths are dotted, list items carry `[index]`.
    pub fn fault(&self, input: &Map<String, Value>, typed: bool) -> Option<ValueFault> {
        let mut declared = Vec::new();
        if let Some(key) = input.keys().find(|key| self.field(key).is_none()) {
            return Some(ValueFault::Unknown(key.clone()));
        }
        self.fields.iter().filter(|field| !field.is_local()).find_map(|field| field.read(input).and_then(|value| field.fault(&value, &field.name, typed, &mut declared)))
    }
}
