#[cfg(test)]
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use time::macros::format_description;
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

use crate::identity::{RowId, ListKey};

mod fractional;
mod numeric;

pub(crate) use numeric::{compare_numbers, Numeric};

pub use fractional::{fractional_between, fractional_keys_between, ORDER_KEY_LIMIT};

pub(crate) const ORDER_ALPHABET: &str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const INT_LIMIT: f64 = 9_223_372_036_854_775_808.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarKind {
    Str,
    Num,
    Int,
    Bool,
    Date,
    Id,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScalarError {
    #[error("missing value for {context}")]
    MissingValue { context: String },
    #[error("rejected value for {context}")]
    RejectedValue { context: String },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RelationParamsError {
    #[error("unknown relation param {0}")]
    UnknownField(String),
    #[error(transparent)]
    Scalar(#[from] ScalarError),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ListKeyError {
    #[error("model and relation name must not be empty")]
    MissingSegment,
    #[error(transparent)]
    Params(#[from] RelationParamsError),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OrderKeyError {
    #[error("fractional order bounds are invalid")]
    InvalidBounds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    Desc,
    AscNullsLast,
    DescNullsFirst,
}

impl SortDirection {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "asc" => Some(SortDirection::Asc),
            "desc" => Some(SortDirection::Desc),
            "asc_nulls_last" => Some(SortDirection::AscNullsLast),
            "desc_nulls_first" => Some(SortDirection::DescNullsFirst),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            SortDirection::Asc => "asc",
            SortDirection::Desc => "desc",
            SortDirection::AscNullsLast => "asc_nulls_last",
            SortDirection::DescNullsFirst => "desc_nulls_first",
        }
    }
}

pub fn read_str(value: &Value) -> Option<String> {
    value.as_str().map(str::to_string)
}

pub fn read_identifier(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => (!text.is_empty()).then(|| text.clone()),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                return Some(integer.to_string());
            }
            if let Some(unsigned) = number.as_u64() {
                return Some(unsigned.to_string());
            }
            let float = number.as_f64()?;
            (float.is_finite() && float.fract() == 0.0).then(|| float.to_string())
        }
        _ => None,
    }
}

pub fn read_id(value: &Value) -> Option<RowId> {
    read_identifier(value).and_then(|text| RowId::new(&text).ok())
}

pub fn read_num(value: &Value) -> Option<f64> {
    let number = match value {
        Value::Number(number) => number.as_f64()?,
        Value::String(text) if !text.is_empty() => text.parse::<f64>().ok()?,
        _ => return None,
    };
    number.is_finite().then_some(if number == 0.0 { 0.0 } else { number })
}

pub fn read_int(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                return Some(integer);
            }
        }
        Value::String(text) => {
            if let Ok(integer) = text.parse::<i64>() {
                return Some(integer);
            }
        }
        _ => {}
    }
    let number = read_num(value)?;
    (number.fract() == 0.0 && (-INT_LIMIT..INT_LIMIT).contains(&number)).then_some(number as i64)
}

pub fn read_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(flag) => Some(*flag),
        Value::String(text) => match text.as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

pub fn read_date(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(canonical_date(text)),
        Value::Number(number) => iso_from_milliseconds(number.as_f64()?),
        _ => None,
    }
}

pub fn read_enum(value: &Value) -> Option<String> {
    value.as_str().map(str::to_string)
}

pub fn read_raw(value: &Value) -> Option<Value> {
    (!value.is_null()).then(|| value.clone())
}

pub(crate) fn read_json_text(value: &Value) -> Option<Value> {
    serde_json::from_str(value.as_str()?).ok()
}

pub fn read_array<T>(value: &Value, element: impl Fn(&Value) -> Option<T>) -> Option<Vec<T>> {
    value.as_array().map(|items| items.iter().filter_map(element).collect())
}

pub fn read_scalar(value: &Value, kind: ScalarKind) -> Option<Value> {
    match kind {
        ScalarKind::Str => read_str(value).map(Value::String),
        ScalarKind::Num => read_num(value).and_then(Number::from_f64).map(Value::Number),
        ScalarKind::Int => read_int(value).map(Value::from),
        ScalarKind::Bool => read_bool(value).map(Value::Bool),
        ScalarKind::Date => read_date(value).map(Value::String),
        ScalarKind::Id => read_id(value).map(|id| Value::String(id.as_str().to_string())),
    }
}

pub fn require<T>(
    value: Option<&Value>,
    context: &str,
    read: impl Fn(&Value) -> Option<T>,
) -> Result<T, ScalarError> {
    let missing = || ScalarError::MissingValue { context: context.to_string() };
    let value = value.filter(|value| !value.is_null()).ok_or_else(missing)?;
    read(value).ok_or_else(|| ScalarError::RejectedValue { context: context.to_string() })
}

fn canonical_iso(moment: OffsetDateTime) -> Option<String> {
    moment.to_offset(UtcOffset::UTC).format(format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z")).ok()
}

fn canonical_date(text: &str) -> String {
    OffsetDateTime::parse(text, &Rfc3339).ok().and_then(canonical_iso).unwrap_or_else(|| text.to_string())
}

fn iso_from_milliseconds(milliseconds: f64) -> Option<String> {
    if !milliseconds.is_finite() {
        return None;
    }
    let nanoseconds = (milliseconds.floor() as i128).checked_mul(1_000_000)?;
    canonical_iso(OffsetDateTime::from_unix_timestamp_nanos(nanoseconds).ok()?)
}

#[cfg(test)]
fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => compare_numbers(left, right) == Some(Ordering::Equal),
        _ => left == right,
    }
}

#[cfg(test)]
fn is_missing(value: Option<&Value>) -> bool {
    value.is_none_or(Value::is_null)
}

#[cfg(test)]
pub fn null_equals(value: Option<&Value>, expected: Option<&Value>) -> bool {
    match (is_missing(value), is_missing(expected)) {
        (true, true) => true,
        (false, false) => values_equal(value.unwrap(), expected.unwrap()),
        _ => false,
    }
}

#[cfg(test)]
pub fn null_not_equals(value: Option<&Value>, expected: Option<&Value>) -> bool {
    !null_equals(value, expected)
}

#[cfg(test)]
pub fn null_is_in(value: Option<&Value>, values: &[Value]) -> bool {
    match value.filter(|value| !value.is_null()) {
        Some(value) => values.iter().any(|candidate| values_equal(candidate, value)),
        None => false,
    }
}

#[cfg(test)]
pub fn null_is_not_in(value: Option<&Value>, values: &[Value]) -> bool {
    match value.filter(|value| !value.is_null()) {
        Some(value) => !values.iter().any(|candidate| values_equal(candidate, value)),
        None => true,
    }
}

#[cfg(test)]
pub fn null_compare(value: Option<&Value>, bound: &Value, keeps_greater: bool, or_equal: bool) -> bool {
    let Some(value) = value else { return false };
    let ordering = match (value, bound) {
        (Value::Number(value), Value::Number(bound)) => compare_numbers(value, bound),
        (Value::String(value), Value::String(bound)) => Some(value.cmp(bound)),
        _ => None,
    };
    match ordering {
        None => false,
        Some(Ordering::Equal) => or_equal,
        Some(ordering) => (ordering == Ordering::Greater) == keeps_greater,
    }
}

pub fn relation_param_text(value: &Value) -> String {
    let tag = match value {
        Value::Null => "n",
        Value::Bool(_) => "b",
        Value::Number(_) => "d",
        Value::String(_) => "s",
        Value::Array(_) => "a",
        Value::Object(_) => "o",
    };
    format!("{tag}:{value}")
}

pub fn normalize_relation_params(
    values: &Map<String, Value>,
    kinds: &BTreeMap<String, ScalarKind>,
) -> Result<BTreeMap<String, String>, RelationParamsError> {
    let mut result = BTreeMap::new();
    for (name, value) in values {
        let kind = kinds.get(name).ok_or_else(|| RelationParamsError::UnknownField(name.clone()))?;
        if value.is_null() {
            result.insert(name.clone(), relation_param_text(value));
            continue;
        }
        let value = Some(value);
        let normalized = match kind {
            ScalarKind::Id => Value::String(require(value, name, read_id)?.as_str().to_string()),
            ScalarKind::Int => Value::from(require(value, name, read_int)?),
            ScalarKind::Num => Value::from(require(value, name, read_num)?),
            ScalarKind::Bool => Value::Bool(require(value, name, read_bool)?),
            ScalarKind::Date => Value::String(require(value, name, read_date)?),
            ScalarKind::Str => Value::String(require(value, name, read_str)?),
        };
        result.insert(name.clone(), relation_param_text(&normalized));
    }
    Ok(result)
}

pub fn duplicates<T: Eq + Hash + Clone>(values: &[T]) -> Vec<T> {
    let mut counts: HashMap<&T, usize> = HashMap::new();
    let mut repeated = Vec::new();
    for value in values {
        let count = counts.entry(value).or_insert(0);
        *count += 1;
        if *count == 2 {
            repeated.push(value.clone());
        }
    }
    repeated
}

pub fn relation_scope_key(
    model: &str,
    name: &str,
    params: &BTreeMap<String, String>,
) -> Result<ListKey, ListKeyError> {
    if model.is_empty() || name.is_empty() {
        return Err(ListKeyError::MissingSegment);
    }
    let encoded = serde_json::to_string(params).expect("string map serializes");
    Ok(ListKey::compose(&[model, name, encoded.as_str()]))
}

pub fn relation_scope_key_from_values(
    model: &str,
    name: &str,
    values: &Map<String, Value>,
    kinds: &BTreeMap<String, ScalarKind>,
) -> Result<ListKey, ListKeyError> {
    relation_scope_key(model, name, &normalize_relation_params(values, kinds)?)
}

#[cfg(test)]
mod value_tests;
#[cfg(test)]
mod fractional_tests;

pub fn same_fields(left: &Map<String, Value>, right: &Map<String, Value>) -> bool {
    let present = |row: &Map<String, Value>| row.values().filter(|value| !value.is_null()).count();
    present(left) == present(right) && left.iter().filter(|(_, value)| !value.is_null()).all(|(key, value)| right.get(key) == Some(value))
}
