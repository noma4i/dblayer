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

pub fn normalize_relation_params(
    values: &Map<String, Value>,
    kinds: &BTreeMap<String, ScalarKind>,
) -> Result<BTreeMap<String, String>, RelationParamsError> {
    let mut result = BTreeMap::new();
    for (name, value) in values {
        let kind = kinds.get(name).ok_or_else(|| RelationParamsError::UnknownField(name.clone()))?;
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
mod tests {
    use super::*;
    use serde_json::json;

    fn row_id(value: &str) -> RowId {
        RowId::new(value).unwrap()
    }

    fn object(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn scalar_kinds_convert_supported_values_only() {
        assert_eq!(read_str(&json!("a")), Some("a".to_string()));
        assert_eq!(read_str(&json!(2)), None);
        assert_eq!(read_str(&Value::Null), None);
        assert_eq!(read_num(&json!(1.5)), Some(1.5));
        assert_eq!(read_num(&json!("2.5")), Some(2.5));
        assert_eq!(read_num(&json!("")), None);
        assert_eq!(read_num(&json!(true)), None);
        assert_eq!(read_int(&json!(3)), Some(3));
        assert_eq!(read_int(&json!(3.9)), None);
        assert_eq!(read_int(&json!("7")), Some(7));
        assert_eq!(read_int(&json!("x")), None);
        assert_eq!(read_bool(&json!(false)), Some(false));
        assert_eq!(read_bool(&json!("true")), Some(true));
        assert_eq!(read_bool(&json!("yes")), None);
        assert_eq!(read_bool(&json!(1)), None);
        assert_eq!(read_date(&json!("2021-05-05T10:00:00Z")), Some("2021-05-05T10:00:00Z".to_string()));
        assert_eq!(read_date(&json!(0)), Some("1970-01-01T00:00:00.000Z".to_string()));
        assert_eq!(read_date(&json!(true)), None);
    }

    #[test]
    fn number_edge_cases_normalize() {
        assert_eq!(read_num(&json!(-0.0)).map(f64::is_sign_negative), Some(false));
        assert_eq!(read_num(&json!("inf")), None);
        assert_eq!(read_num(&json!("NaN")), None);
        assert_eq!(read_int(&json!(9_007_199_254_740_993_i64)), Some(9_007_199_254_740_993));
        assert_eq!(read_int(&json!(1e30)), None);
        assert_eq!(read_date(&json!(1_700_000_000_123_i64)), Some("2023-11-14T22:13:20.123Z".to_string()));
    }

    #[test]
    fn identifier_accepts_non_empty_strings_and_integral_numbers() {
        assert_eq!(read_identifier(&json!("abc")), Some("abc".to_string()));
        assert_eq!(read_identifier(&json!("")), None);
        assert_eq!(read_identifier(&json!(7)), Some("7".to_string()));
        assert_eq!(read_identifier(&json!(-12)), Some("-12".to_string()));
        assert_eq!(read_identifier(&json!(7.0)), Some("7".to_string()));
        assert_eq!(read_identifier(&json!(7.5)), None);
        assert_eq!(read_identifier(&json!(true)), None);
        assert_eq!(read_identifier(&Value::Null), None);
    }

    #[test]
    fn required_values_report_missing_and_rejected() {
        assert_eq!(require(None, "chatId", read_id), Err(ScalarError::MissingValue { context: "chatId".into() }));
        assert_eq!(
            require(Some(&Value::Null), "chatId", read_id),
            Err(ScalarError::MissingValue { context: "chatId".into() })
        );
        assert_eq!(
            require(Some(&json!("")), "chatId", read_id),
            Err(ScalarError::RejectedValue { context: "chatId".into() })
        );
        assert_eq!(
            require(Some(&json!(7.5)), "chatId", read_id),
            Err(ScalarError::RejectedValue { context: "chatId".into() })
        );
        assert_eq!(
            require(Some(&json!("yes")), "flag", read_bool),
            Err(ScalarError::RejectedValue { context: "flag".into() })
        );
    }

    #[test]
    fn numeric_values_normalize() {
        assert_eq!(require(Some(&json!(7)), "chatId", read_id).unwrap(), row_id("7"));
        assert_eq!(require(Some(&json!("42")), "count", read_int).unwrap(), 42);
    }

    #[test]
    fn enum_and_raw_values_convert_supported_values_only() {
        let allowed = vec!["image".to_string(), "video".to_string()];
        assert_eq!(read_enum(&json!("image"), &allowed), Some("image".to_string()));
        assert_eq!(read_enum(&json!("audio"), &allowed), None);
        assert_eq!(read_enum(&Value::Null, &allowed), None);
        assert_eq!(read_raw(&json!("a")), Some(json!("a")));
        assert_eq!(read_raw(&json!([])), Some(json!([])));
        assert_eq!(read_raw(&Value::Null), None);
    }

    #[test]
    fn array_and_object_read_only_their_own_container() {
        let list = json!(["1", "x", "2"]);
        let record = json!({"id": "7"});
        assert_eq!(read_array(&list, read_int), Some(vec![1, 2]));
        assert_eq!(read_array(&json!([]), read_int), Some(vec![]));
        assert_eq!(read_array(&json!("a"), read_int), None);
        assert_eq!(read_array(&Value::Null, read_int), None);
        assert_eq!(read_object(&record, |map| map.get("id").and_then(read_str)), Some("7".to_string()));
        assert_eq!(read_object(&json!([]), |_| Some("x")), None);
    }

    #[test]
    fn json_values_decode_every_kind_and_reject_broken_body() {
        let body = r#"{"s":"text","n":1.5,"b":true,"nothing":null,"list":[1,2],"nested":{"key":"value"}}"#;
        let value: Value = serde_json::from_str(body).unwrap();
        assert_eq!(value["s"], json!("text"));
        assert_eq!(value["n"], json!(1.5));
        assert_eq!(value["b"], json!(true));
        assert_eq!(value["nothing"], Value::Null);
        assert_eq!(value["list"], json!([1, 2]));
        assert_eq!(value["nested"], json!({"key": "value"}));
        assert!(serde_json::from_str::<Value>("not json").is_err());
    }

    #[test]
    fn json_values_round_trip_with_sorted_keys() {
        let value = json!({"b": 2, "a": "x"});
        let text = serde_json::to_string(&value).unwrap();
        assert!(text.find('a').unwrap() < text.find('b').unwrap());
        for sample in [value, Value::Null, json!(true), json!([1, null])] {
            let round: Value = serde_json::from_str(&serde_json::to_string(&sample).unwrap()).unwrap();
            assert_eq!(round, sample);
        }
    }

    #[test]
    fn scalar_kind_decodes_into_json_values() {
        assert_eq!(read_scalar(&json!("42"), ScalarKind::Int), Some(json!(42)));
        assert_eq!(read_scalar(&json!("abc"), ScalarKind::Int), None);
        assert_eq!(read_scalar(&json!(2), ScalarKind::Num), Some(json!(2.0)));
        assert_eq!(read_scalar(&json!(""), ScalarKind::Id), None);
        assert_eq!(read_scalar(&json!(7), ScalarKind::Id), Some(json!("7")));
        assert_eq!(read_scalar(&json!("true"), ScalarKind::Bool), Some(json!(true)));
        assert_eq!(read_scalar(&json!("x"), ScalarKind::Str), Some(json!("x")));
        let date = read_scalar(&json!(1_700_000_000_000_i64), ScalarKind::Date).unwrap();
        assert!(date.as_str().unwrap().starts_with("2023-"));
    }

    #[test]
    fn null_semantics_treat_absent_and_null_as_missing() {
        let list = vec![json!("a")];
        let a = json!("a");
        let b = json!("b");
        assert!(null_equals(None, None));
        assert!(null_equals(Some(&Value::Null), None));
        assert!(!null_equals(Some(&a), None));
        assert!(null_equals(Some(&a), Some(&a)));
        assert!(null_not_equals(Some(&a), Some(&b)));
        assert!(!null_not_equals(None, None));
        assert!(!null_is_in(None, &list));
        assert!(null_is_in(Some(&a), &list));
        assert!(!null_is_in(Some(&Value::Null), &list));
        assert!(null_is_not_in(None, &list));
        assert!(!null_is_not_in(Some(&a), &list));
        assert!(null_is_not_in(Some(&b), &list));
        assert!(null_is_not_in(None, &[]));
    }

    #[test]
    fn null_semantics_compare_numbers_strings_and_absent_values() {
        let five = json!(5);
        let three = json!(3);
        assert!(null_compare(Some(&five), &three, true, false));
        assert!(!null_compare(Some(&five), &five, true, false));
        assert!(null_compare(Some(&five), &five, true, true));
        assert!(!null_compare(None, &five, true, false));
        assert!(null_compare(Some(&json!("b")), &json!("a"), true, false));
        assert!(null_compare(Some(&three), &five, false, false));
        assert!(!null_compare(Some(&json!("b")), &five, true, false));
        assert!(null_equals(Some(&json!(1)), Some(&json!(1.0))));
    }

    #[test]
    fn order_key_numbers_and_strings_keep_order_per_direction() {
        let id = row_id("row");
        let values = [-10.0, -1.0, 0.0, 1.0, 10.0];
        let ascending: Vec<String> =
            values.iter().map(|number| order_key_encoded(&[OrderValue::Number(*number)], &[SortDirection::Asc], &id)).collect();
        let descending: Vec<String> =
            values.iter().map(|number| order_key_encoded(&[OrderValue::Number(*number)], &[SortDirection::Desc], &id)).collect();
        let strings: Vec<String> = ["a", "ab", "b"]
            .iter()
            .map(|text| order_key_encoded(&[OrderValue::Text(text.to_string())], &[SortDirection::Asc], &id))
            .collect();
        assert!(ascending.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(descending.windows(2).all(|pair| pair[0] > pair[1]));
        assert!(strings[0] < strings[1]);
        assert!(strings[1] < strings[2]);
    }

    #[test]
    fn order_key_missing_sorts_last_in_both_directions_and_nan_is_largest() {
        let id = row_id("row");
        let key = |value: OrderValue, direction: SortDirection| order_key_encoded(&[value], &[direction], &id);
        let number = key(OrderValue::Number(1.0), SortDirection::Asc);
        let nan = key(OrderValue::Number(f64::NAN), SortDirection::Asc);
        let missing = key(OrderValue::Missing, SortDirection::Asc);
        assert!(number < nan);
        assert!(nan < missing);
        assert!(key(OrderValue::Number(1.0), SortDirection::Desc) < key(OrderValue::Missing, SortDirection::Desc));
    }

    #[test]
    fn order_key_tie_break_uses_id_and_fields_compare_lexicographically() {
        let id = row_id("row");
        let with_id = |value: &str| order_key_encoded(&[OrderValue::Number(1.0)], &[SortDirection::Asc], &row_id(value));
        let pair = |number: f64, text: &str| {
            order_key_encoded(
                &[OrderValue::Number(number), OrderValue::Text(text.to_string())],
                &[SortDirection::Asc, SortDirection::Asc],
                &id,
            )
        };
        assert_ne!(with_id("1"), with_id("2"));
        assert!(with_id("1") < with_id("2"));
        assert!(pair(1.0, "a") < pair(1.0, "b"));
        assert!(pair(1.0, "z") < pair(2.0, "a"));
    }

    #[test]
    fn order_key_encoded_keys_follow_field_order() {
        let first = row_id("1");
        let second = row_id("2");
        let key = |number: f64, direction: SortDirection, id: &RowId| order_key_encoded(&[OrderValue::Number(number)], &[direction], id);
        assert!(key(1.0, SortDirection::Asc, &first) < key(2.0, SortDirection::Asc, &first));
        assert!(key(2.0, SortDirection::Desc, &first) < key(1.0, SortDirection::Desc, &first));
        assert!(key(1.0, SortDirection::Asc, &first) < key(1.0, SortDirection::Asc, &second));
    }

    #[test]
    fn fractional_keys_between_nils_and_open_ends_are_ordered() {
        let key = fractional_between(None, None).unwrap();
        let before = fractional_between(None, Some("V")).unwrap();
        let after = fractional_between(Some("V"), None).unwrap();
        assert!(!key.is_empty());
        assert!(!key.ends_with('0'));
        assert!(before.as_str() < "V");
        assert!(after.as_str() > "V");
    }

    #[test]
    fn fractional_random_inserts_stay_strictly_between_and_sorted() {
        let mut lower: Option<String> = None;
        let mut upper: Option<String> = None;
        let mut state = 17_u32;
        for _ in 0..20 {
            let key = fractional_between(lower.as_deref(), upper.as_deref()).unwrap();
            if let Some(lower) = &lower {
                assert!(key > *lower);
            }
            if let Some(upper) = &upper {
                assert!(key < *upper);
            }
            state = (state * 73 + 19) % 997;
            if state % 2 == 0 {
                lower = Some(key);
            } else {
                upper = Some(key);
            }
        }

        let mut keys: Vec<String> = Vec::new();
        state = 37;
        for _ in 0..100 {
            state = (state * 73 + 19) % 997;
            let index = state as usize % (keys.len() + 1);
            let lower_key = if index == 0 { None } else { Some(keys[index - 1].as_str()) };
            let upper_key = if index == keys.len() { None } else { Some(keys[index].as_str()) };
            let key = fractional_between(lower_key, upper_key).unwrap();
            keys.insert(index, key);
        }
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(sorted, keys);
    }

    #[test]
    fn fractional_five_thousand_inserts_in_one_gap_stay_unique_and_never_end_with_zero() {
        let mut upper: Option<String> = None;
        let mut keys: Vec<String> = Vec::new();
        for _ in 0..5000 {
            let key = fractional_between(None, upper.as_deref()).unwrap();
            if let Some(previous) = keys.last() {
                assert!(key < *previous);
            }
            assert!(!key.ends_with('0'));
            upper = Some(key.clone());
            keys.push(key);
        }
        assert_eq!(keys.iter().collect::<std::collections::HashSet<_>>().len(), 5000);
    }

    #[test]
    fn fractional_inserts_between_fixed_bounds_stay_between() {
        let initial = fractional_between(None, None).unwrap();
        let upper = fractional_between(Some(&initial), None).unwrap();
        let mut lower = initial;
        let mut keys = Vec::new();
        for _ in 0..5000 {
            let key = fractional_between(Some(&lower), Some(&upper)).unwrap();
            assert!(lower < key);
            assert!(key < upper);
            keys.push(key.clone());
            lower = key;
        }
        assert_eq!(keys.iter().collect::<std::collections::HashSet<_>>().len(), 5000);
    }

    #[test]
    fn fractional_append_keys_stay_monotonic_and_never_end_with_zero() {
        let mut lower: Option<String> = None;
        let mut keys: Vec<String> = Vec::new();
        for _ in 0..200 {
            let key = fractional_between(lower.as_deref(), None).unwrap();
            lower = Some(key.clone());
            keys.push(key);
        }
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(keys.iter().collect::<std::collections::HashSet<_>>().len(), 200);
        assert!(keys.iter().all(|key| !key.is_empty() && !key.ends_with('0')));
    }

    #[test]
    fn fractional_rejects_inverted_bounds() {
        assert_eq!(fractional_between(Some("b"), Some("a")), Err(OrderKeyError::InvalidBounds));
        assert_eq!(fractional_between(Some("!"), Some("a")), Err(OrderKeyError::InvalidBounds));
    }

    #[test]
    fn relation_params_normalize_to_strings() {
        let kinds = BTreeMap::from([("chatId".to_string(), ScalarKind::Id)]);
        let from_string = normalize_relation_params(&object(json!({"chatId": "1"})), &kinds).unwrap();
        let from_number = normalize_relation_params(&object(json!({"chatId": 1})), &kinds).unwrap();
        assert_eq!(from_string, BTreeMap::from([("chatId".to_string(), "1".to_string())]));
        assert_eq!(from_number, from_string);
        let count = BTreeMap::from([("count".to_string(), ScalarKind::Int)]);
        assert_eq!(normalize_relation_params(&object(json!({"count": "42"})), &count).unwrap()["count"], "42");
        let flag = BTreeMap::from([("flag".to_string(), ScalarKind::Bool)]);
        assert_eq!(normalize_relation_params(&object(json!({"flag": true})), &flag).unwrap()["flag"], "true");
        let text = BTreeMap::from([("q".to_string(), ScalarKind::Str), ("at".to_string(), ScalarKind::Date)]);
        let both = normalize_relation_params(&object(json!({"q": "hi", "at": "2021-01-01T00:00:00Z"})), &text).unwrap();
        assert_eq!(both["q"], "hi");
        assert_eq!(both["at"], "2021-01-01T00:00:00Z");
    }

    #[test]
    fn relation_params_report_unknown_field_and_rejected_id() {
        let kinds = BTreeMap::from([("chatId".to_string(), ScalarKind::Id)]);
        assert_eq!(
            normalize_relation_params(&object(json!({"authorId": "1"})), &kinds),
            Err(RelationParamsError::UnknownField("authorId".into()))
        );
        assert_eq!(
            normalize_relation_params(&object(json!({"chatId": ""})), &kinds),
            Err(RelationParamsError::Scalar(ScalarError::RejectedValue { context: "chatId".into() }))
        );
    }

    #[test]
    fn duplicates_return_values_in_first_repeat_order() {
        let values: Vec<String> = ["a", "b", "a", "c", "b"].iter().map(|text| text.to_string()).collect();
        assert_eq!(duplicates(&values), vec!["a".to_string(), "b".to_string()]);
        assert_eq!(duplicates(&["a", "a", "a"]), vec!["a"]);
        assert!(duplicates(&["a", "b"]).is_empty());
    }

    #[test]
    fn relation_scope_keys_match_spec() {
        let relation = relation_scope_key("message", "thread", &BTreeMap::from([("chatId".to_string(), "42".to_string())])).unwrap();
        let sorted = relation_scope_key(
            "message",
            "thread",
            &BTreeMap::from([("b".to_string(), "2".to_string()), ("a".to_string(), "1".to_string())]),
        )
        .unwrap();
        let empty = relation_scope_key("message", "thread", &BTreeMap::new()).unwrap();
        assert_eq!(relation.as_str(), "7:message6:thread15:{\"chatId\":\"42\"}");
        assert_eq!(relation.decompose().unwrap(), vec!["message", "thread", "{\"chatId\":\"42\"}"]);
        assert_eq!(sorted.decompose().unwrap(), vec!["message", "thread", "{\"a\":\"1\",\"b\":\"2\"}"]);
        assert_eq!(empty.decompose().unwrap(), vec!["message", "thread", "{}"]);
    }

    #[test]
    fn relation_scope_keys_reject_empty_segments_and_unknown_fields() {
        assert_eq!(relation_scope_key("", "thread", &BTreeMap::new()), Err(ScopeKeyError::MissingSegment));
        assert_eq!(relation_scope_key("message", "", &BTreeMap::new()), Err(ScopeKeyError::MissingSegment));
        let kinds = BTreeMap::from([("chatId".to_string(), ScalarKind::Id)]);
        assert_eq!(
            relation_scope_key_from_values("message", "thread", &object(json!({"authorId": "1"})), &kinds),
            Err(ScopeKeyError::Params(RelationParamsError::UnknownField("authorId".into())))
        );
    }

    #[test]
    fn normalized_relation_values_produce_equal_keys() {
        let kinds = BTreeMap::from([("chatId".to_string(), ScalarKind::Id)]);
        let from_string = relation_scope_key_from_values("message", "thread", &object(json!({"chatId": "42"})), &kinds).unwrap();
        let from_number = relation_scope_key_from_values("message", "thread", &object(json!({"chatId": 42})), &kinds).unwrap();
        let empty_values = relation_scope_key_from_values("message", "thread", &Map::new(), &BTreeMap::new()).unwrap();
        let empty_params = relation_scope_key("message", "thread", &BTreeMap::new()).unwrap();
        assert_eq!(from_string, from_number);
        assert_eq!(empty_values, empty_params);
    }
}
