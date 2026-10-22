use serde_json::Value;

use super::CloudValue;

impl CloudValue {
    pub fn of(value: &Value) -> Self {
        match value {
            Value::Null => CloudValue::Null,
            Value::Bool(value) => CloudValue::Flag { value: *value },
            Value::Number(number) => match number.as_i64() {
                Some(value) => CloudValue::Whole { value },
                None => CloudValue::Fraction { value: number.as_f64().unwrap_or_default() },
            },
            Value::String(value) => CloudValue::Text { value: value.clone() },
            tree => CloudValue::Tree { json: tree.to_string() },
        }
    }

    pub fn value(&self) -> Option<Value> {
        Some(match self {
            CloudValue::Null => Value::Null,
            CloudValue::Flag { value } => Value::Bool(*value),
            CloudValue::Whole { value } => Value::from(*value),
            CloudValue::Fraction { value } => serde_json::Number::from_f64(*value).map_or(Value::Null, Value::Number),
            CloudValue::Text { value } => Value::String(value.clone()),
            CloudValue::Tree { json } => serde_json::from_str(json).ok()?,
        })
    }
}
