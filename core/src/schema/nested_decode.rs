//! The decoding of a nested value that comes from the server: one function for every path that lands a row (data.md P-DATA-63).

use serde_json::{Map, Value};

use super::{FieldKind, FieldSpec};
use crate::value::{read_enum, read_scalar, ScalarKind};

impl FieldSpec {
    /// The stored value of a member for what the server sent, or why the row is refused. An absent or null member takes its default or null by its declaration, a member of another type is an error.
    pub fn decode_nested(&self, value: Option<&Value>) -> Result<Value, String> {
        let Some(value) = value.filter(|value| !value.is_null()) else {
            return if value.is_some() && self.nullable { Ok(Value::Null) }
            else if let Some(default) = &self.default { Ok(default.clone()) }
            else if self.nullable || self.optional { Ok(Value::Null) }
            else { Err(format!("{} is required", self.name)) };
        };
        let wrong = || format!("{} has a value of another type", self.name);
        match &self.kind {
            FieldKind::Scalar { kind } if !has_type(*kind, value) => Err(wrong()),
            FieldKind::Scalar { kind: ScalarKind::Date } => read_scalar(value, ScalarKind::Str).ok_or_else(wrong),
            FieldKind::Scalar { kind } => read_scalar(value, *kind).ok_or_else(wrong),
            FieldKind::Enum { .. } => read_enum(value).map(Value::String).ok_or_else(wrong),
            FieldKind::Object => Ok(Value::Object(self.decode_members(value.as_object().ok_or_else(wrong)?)?)),
            FieldKind::Array => value.as_array().ok_or_else(wrong)?.iter().map(|item| Ok(Value::Object(self.decode_members(item.as_object().ok_or_else(wrong)?)?))).collect::<Result<Vec<_>, String>>().map(Value::Array),
            _ => self.decode(value).ok_or_else(wrong),
        }
    }

    fn decode_members(&self, source: &Map<String, Value>) -> Result<Map<String, Value>, String> {
        self.fields.iter().map(|member| Ok((member.name.clone(), member.decode_nested(source.get(&member.name))?))).collect()
    }

    /// Whether the member holds nested members that `decode_nested` checks.
    pub fn is_nested(&self) -> bool { matches!(self.kind, FieldKind::Object | FieldKind::Array) }
}

/// The JSON type a scalar takes from the server: a text is no number and no bool, an id is a text or an integer of 64 bits.
fn has_type(kind: ScalarKind, value: &Value) -> bool {
    match kind {
        ScalarKind::Str | ScalarKind::Date => value.is_string(),
        ScalarKind::Num => value.is_number(),
        ScalarKind::Int => value.as_i64().is_some(),
        ScalarKind::Bool => value.is_boolean(),
        ScalarKind::Id => value.is_string() || value.as_i64().is_some(),
    }
}
