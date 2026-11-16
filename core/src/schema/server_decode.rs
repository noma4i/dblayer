//! The decoding of a value that comes from the server: one function for every path that lands a row, at any depth (data.md P-DATA-63).

use serde_json::{Map, Value};

use super::{read_path, FieldKind, FieldSpec, ModelSpec, TYPENAME};
use crate::value::{read_array, read_enum, read_json_text, read_raw, read_scalar, ScalarKind};

type Enclosing<'a> = Vec<&'a FieldSpec>;

impl ModelSpec {
    /// The stored values of a node the server sent: a field the node lacks stays out of the row, a value that does not decode refuses the row. A node whose `id` is absent or null has no identity: the row has no `id`.
    pub fn decode_server(&self, input: &Map<String, Value>) -> Result<Map<String, Value>, String> {
        let mut row = Map::new();
        for field in self.fields.iter().filter(|field| !field.is_local()) {
            let Some(value) = field.read(input) else { continue };
            if field.name == "id" && value.is_null() { continue; }
            row.insert(field.name.clone(), field.decode_server(Some(&value), true, &mut Vec::new())?);
        }
        Ok(row)
    }
}

impl ModelSpec {
    /// The first field a full payload lacks: a field of the server that is neither optional, nullable nor defaulted must be in the decoded row.
    pub fn missing_field(&self, row: &Map<String, Value>) -> Option<&str> {
        self.fields.iter().filter(|field| !field.is_local() && !field.optional && !field.nullable && field.default.is_none()).find(|field| !row.contains_key(&field.name)).map(|field| field.name.as_str())
    }
}

impl FieldSpec {
    /// The stored value of a field for what the server sent, or why the row is refused. `top` is a field of the row: a scalar there takes a text that reads as its type and a date reads ISO text or milliseconds. Deeper a scalar keeps its JSON type and a date is text.
    pub fn decode_server<'a>(&'a self, value: Option<&Value>, top: bool, enclosing: &mut Enclosing<'a>) -> Result<Value, String> {
        let Some(value) = value.filter(|value| !value.is_null()) else {
            return if value.is_some() && self.nullable { Ok(Value::Null) }
            else if let Some(default) = &self.default { Ok(default.clone()) }
            else if self.nullable || self.optional { Ok(Value::Null) }
            else { Err(format!("{} is required", self.name)) };
        };
        let wrong = || format!("{} has a value of another type", self.name);
        match &self.kind {
            FieldKind::Scalar { kind } if top => read_scalar(value, *kind).ok_or_else(wrong),
            FieldKind::Scalar { kind } if !has_type(*kind, value) => Err(wrong()),
            FieldKind::Scalar { kind: ScalarKind::Date } => read_scalar(value, ScalarKind::Str).ok_or_else(wrong),
            FieldKind::Scalar { kind } => read_scalar(value, *kind).ok_or_else(wrong),
            FieldKind::Enum { .. } => read_enum(value).map(Value::String).ok_or_else(wrong),
            FieldKind::Raw if self.parse_json => read_json_text(value).ok_or_else(wrong),
            FieldKind::Raw => read_raw(value).ok_or_else(wrong),
            FieldKind::Object if self.app_type.is_none() => value.is_object().then(|| value.clone()).ok_or_else(wrong),
            FieldKind::Array if self.app_type.is_none() => value.is_array().then(|| value.clone()).ok_or_else(wrong),
            FieldKind::Object => Ok(Value::Object(self.decode_members(value.as_object().ok_or_else(wrong)?, enclosing)?)),
            FieldKind::Array => {
                let declares = !self.fields.is_empty();
                if declares { enclosing.push(self); }
                let items = value.as_array().ok_or_else(wrong).and_then(|items| items.iter().map(|item| Ok(Value::Object(self.decode_members(item.as_object().ok_or_else(wrong)?, enclosing)?))).collect::<Result<Vec<_>, String>>());
                if declares { enclosing.pop(); }
                items.map(Value::Array)
            }
            FieldKind::Union => self.decode_variant(value, enclosing),
            FieldKind::UnionArray => value.as_array().ok_or_else(wrong)?.iter().map(|item| self.decode_variant(item, enclosing)).collect::<Result<Vec<_>, String>>().map(Value::Array),
            FieldKind::List { element } => read_array(value, |item| match (top, element) {
                (true, kind) => read_scalar(item, *kind),
                (false, kind) if !has_type(*kind, item) => None,
                (false, ScalarKind::Date) => read_scalar(item, ScalarKind::Str),
                (false, kind) => read_scalar(item, *kind),
            }).map(Value::Array).ok_or_else(wrong),
            FieldKind::File => super::field::is_file_descriptor(value).then(|| value.clone()).ok_or_else(wrong),
        }
    }

    /// The members of an object or of an array element. A recursive array names the enclosing array of its type and takes its members.
    fn decode_members<'a>(&'a self, source: &Map<String, Value>, enclosing: &mut Enclosing<'a>) -> Result<Map<String, Value>, String> {
        let recursive = if self.fields.is_empty() && self.kind == FieldKind::Array { enclosing.iter().rev().copied().find(|owner| owner.app_type == self.app_type) } else { None };
        decode_each(recursive.unwrap_or(self).fields.iter(), source, enclosing)
    }

    /// A union value: the variant its `__typename` names decodes the shared and its own members, a name outside the declaration keeps only itself.
    fn decode_variant<'a>(&'a self, value: &Value, enclosing: &mut Enclosing<'a>) -> Result<Value, String> {
        let wrong = || format!("{} has a value of another type", self.name);
        let source = value.as_object().ok_or_else(wrong)?;
        let name = source.get(TYPENAME).and_then(Value::as_str).ok_or_else(wrong)?;
        let mut kept = Map::from_iter([(TYPENAME.to_string(), Value::String(name.to_string()))]);
        if let Some(variant) = self.fields.iter().find(|variant| variant.from.as_deref() == Some(name)) {
            kept.extend(decode_each(self.shared.iter().chain(&variant.fields), source, enclosing)?);
        }
        Ok(Value::Object(kept))
    }
}

fn decode_each<'a>(members: impl Iterator<Item = &'a FieldSpec>, source: &Map<String, Value>, enclosing: &mut Enclosing<'a>) -> Result<Map<String, Value>, String> {
    members.map(|member| {
        let key = member.from.as_deref().unwrap_or(&member.name);
        let found = read_path(source, key).or_else(|| source.get(&member.name));
        Ok((key.to_string(), member.decode_server(found, false, enclosing)?))
    }).collect()
}

/// The JSON type a scalar takes from the server below the row: a text is no number and no bool, an id is a text or an integer of 64 bits.
fn has_type(kind: ScalarKind, value: &Value) -> bool {
    match kind {
        ScalarKind::Str | ScalarKind::Date => value.is_string(),
        ScalarKind::Num => value.is_number(),
        ScalarKind::Int => value.as_i64().is_some(),
        ScalarKind::Bool => value.is_boolean(),
        ScalarKind::Id => value.is_string() || value.as_i64().is_some(),
    }
}
