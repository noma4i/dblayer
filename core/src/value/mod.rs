use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use time::macros::format_description;
use time::OffsetDateTime;

use crate::identity::{RowId, ScopeKey};

const ORDER_ALPHABET: &str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const SIGN_BIT: u64 = 1 << 63;
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
pub enum ScopeKeyError {
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderValue {
    Number(f64),
    Text(String),
    Missing,
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
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => iso_from_milliseconds(number.as_f64()?),
        _ => None,
    }
}

pub fn read_enum(value: &Value, allowed: &[String]) -> Option<String> {
    let text = value.as_str()?;
    allowed.iter().any(|candidate| candidate == text).then(|| text.to_string())
}

pub fn read_raw(value: &Value) -> Option<Value> {
    (!value.is_null()).then(|| value.clone())
}

pub fn read_array<T>(value: &Value, element: impl Fn(&Value) -> Option<T>) -> Option<Vec<T>> {
    value.as_array().map(|items| items.iter().filter_map(element).collect())
}

pub fn read_object<T>(value: &Value, read: impl Fn(&Map<String, Value>) -> Option<T>) -> Option<T> {
    value.as_object().and_then(read)
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

fn iso_from_milliseconds(milliseconds: f64) -> Option<String> {
    if !milliseconds.is_finite() {
        return None;
    }
    let nanoseconds = (milliseconds.floor() as i128).checked_mul(1_000_000)?;
    OffsetDateTime::from_unix_timestamp_nanos(nanoseconds)
        .ok()?
        .format(format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"))
        .ok()
}

fn compare_numbers(left: &Number, right: &Number) -> Option<Ordering> {
    if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
        return Some(left.cmp(&right));
    }
    if let (Some(left), Some(right)) = (left.as_u64(), right.as_u64()) {
        return Some(left.cmp(&right));
    }
    left.as_f64()?.partial_cmp(&right.as_f64()?)
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => compare_numbers(left, right) == Some(Ordering::Equal),
        _ => left == right,
    }
}

fn is_missing(value: Option<&Value>) -> bool {
    value.is_none_or(Value::is_null)
}

pub fn null_equals(value: Option<&Value>, expected: Option<&Value>) -> bool {
    match (is_missing(value), is_missing(expected)) {
        (true, true) => true,
        (false, false) => values_equal(value.unwrap(), expected.unwrap()),
        _ => false,
    }
}

pub fn null_not_equals(value: Option<&Value>, expected: Option<&Value>) -> bool {
    !null_equals(value, expected)
}

pub fn null_is_in(value: Option<&Value>, values: &[Value]) -> bool {
    match value.filter(|value| !value.is_null()) {
        Some(value) => values.iter().any(|candidate| values_equal(candidate, value)),
        None => false,
    }
}

pub fn null_is_not_in(value: Option<&Value>, values: &[Value]) -> bool {
    match value.filter(|value| !value.is_null()) {
        Some(value) => !values.iter().any(|candidate| values_equal(candidate, value)),
        None => true,
    }
}

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

fn hex_byte(byte: u8) -> String {
    format!("{byte:02x}")
}

fn encoded_number(number: f64, descending: bool) -> String {
    if number.is_nan() {
        return (if descending { "0" } else { "1" }).to_string();
    }
    let mut bits = number.to_bits();
    bits = if bits & SIGN_BIT == 0 { bits ^ SIGN_BIT } else { !bits };
    if descending {
        bits = !bits;
    }
    format!("{}{bits:016x}", if descending { '1' } else { '0' })
}

fn encoded_text(text: &str, descending: bool) -> String {
    let mut result = String::from(if descending { "1" } else { "0" });
    for byte in text.bytes() {
        result.push_str(&hex_byte(if descending { !byte } else { byte }));
    }
    result.push_str(&if descending { hex_byte(!b'.') } else { ".".to_string() });
    result
}

pub fn order_key_encoded(values: &[OrderValue], directions: &[SortDirection], id: &RowId) -> String {
    let mut result = String::new();
    for (index, value) in values.iter().enumerate() {
        let descending = directions.get(index) == Some(&SortDirection::Desc);
        match value {
            OrderValue::Number(number) => result.push_str(&encoded_number(*number, descending)),
            OrderValue::Text(text) => result.push_str(&encoded_text(text, descending)),
            OrderValue::Missing => result.push('2'),
        }
    }
    result.push('.');
    for byte in id.as_str().bytes() {
        result.push_str(&hex_byte(byte));
    }
    result
}

pub fn fractional_between(lower: Option<&str>, upper: Option<&str>) -> Result<String, OrderKeyError> {
    let alphabet = ORDER_ALPHABET.as_bytes();
    let lower = lower.unwrap_or("").as_bytes();
    let upper = upper.unwrap_or("").as_bytes();
    let mut prefix: Vec<u8> = Vec::new();
    let mut index = 0;
    loop {
        let lower_byte = lower.get(index).copied().unwrap_or(alphabet[0]);
        let upper_byte = upper.get(index).copied().unwrap_or(alphabet[alphabet.len() - 1]);
        if lower_byte == upper_byte {
            prefix.push(lower_byte);
            index += 1;
            continue;
        }
        let position = |byte: u8| alphabet.iter().position(|candidate| *candidate == byte);
        let (Some(lower_index), Some(upper_index)) = (position(lower_byte), position(upper_byte)) else {
            return Err(OrderKeyError::InvalidBounds);
        };
        if lower_index >= upper_index {
            return Err(OrderKeyError::InvalidBounds);
        }
        if upper_index - lower_index > 1 {
            prefix.push(alphabet[(lower_index + upper_index) / 2]);
        } else {
            prefix.push(lower_byte);
            if index + 1 < lower.len() {
                prefix.extend_from_slice(&lower[index + 1..]);
            }
            prefix.push(alphabet[alphabet.len() / 2]);
        }
        return Ok(String::from_utf8(prefix).expect("alphabet is ascii"));
    }
}

/// Scope-key text of a relation parameter that was passed as null (for example the unfiltered feed).
const NULL_PARAM: &str = "\u{0}null";

pub fn normalize_relation_params(
    values: &Map<String, Value>,
    kinds: &BTreeMap<String, ScalarKind>,
) -> Result<BTreeMap<String, String>, RelationParamsError> {
    let mut result = BTreeMap::new();
    for (name, value) in values {
        let kind = kinds.get(name).ok_or_else(|| RelationParamsError::UnknownField(name.clone()))?;
        if value.is_null() {
            result.insert(name.clone(), NULL_PARAM.to_string());
            continue;
        }
        let value = Some(value);
        let text = match kind {
            ScalarKind::Id => require(value, name, read_id)?.as_str().to_string(),
            ScalarKind::Int => require(value, name, read_int)?.to_string(),
            ScalarKind::Num => require(value, name, read_num)?.to_string(),
            ScalarKind::Bool => require(value, name, read_bool)?.to_string(),
            ScalarKind::Date => require(value, name, read_date)?,
            ScalarKind::Str => require(value, name, read_str)?,
        };
        result.insert(name.clone(), text);
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
) -> Result<ScopeKey, ScopeKeyError> {
    if model.is_empty() || name.is_empty() {
        return Err(ScopeKeyError::MissingSegment);
    }
    let encoded = serde_json::to_string(params).expect("string map serializes");
    Ok(ScopeKey::compose(&[model, name, encoded.as_str()]))
}

pub fn relation_scope_key_from_values(
    model: &str,
    name: &str,
    values: &Map<String, Value>,
    kinds: &BTreeMap<String, ScalarKind>,
) -> Result<ScopeKey, ScopeKeyError> {
    relation_scope_key(model, name, &normalize_relation_params(values, kinds)?)
}

#[cfg(test)]
mod value_tests;
