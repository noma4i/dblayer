use serde_json::{Map, Value};

use crate::identity::RowId;
use crate::schema::Sort;
use crate::value::{order_key_encoded, OrderValue, SortDirection};

pub fn order_values(sort: &Sort, row: &Map<String, Value>) -> Vec<OrderValue> {
    match sort {
        Sort::ServerOrder => Vec::new(),
        Sort::Fields(fields) => fields.iter().map(|field| order_value(row.get(&field.column))).collect(),
    }
}

fn order_value(value: Option<&Value>) -> OrderValue {
    match value {
        Some(Value::Number(number)) => number.as_f64().map_or(OrderValue::Missing, OrderValue::Number),
        Some(Value::String(text)) => OrderValue::Text(text.clone()),
        Some(Value::Bool(flag)) => OrderValue::Number(f64::from(u8::from(*flag))),
        _ => OrderValue::Missing,
    }
}

pub fn directions(sort: &Sort) -> Vec<SortDirection> {
    match sort {
        Sort::ServerOrder => Vec::new(),
        Sort::Fields(fields) => fields.iter().map(|field| field.direction).collect(),
    }
}

pub fn order_key(sort: &Sort, row: &Map<String, Value>, id: &RowId) -> Option<String> {
    match sort {
        Sort::ServerOrder => None,
        Sort::Fields(_) => Some(order_key_encoded(&order_values(sort, row), &directions(sort), id)),
    }
}
