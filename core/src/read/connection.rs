use serde_json::Value;

use crate::schema::CursorSpec;
use crate::store::PageInfo;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LandingFault {
    PayloadNotObject,
    MissingPageInfo,
    CursorMissing,
}

pub fn connection_nodes(connection: &Value) -> Vec<Value> {
    let Some(fields) = connection.as_object() else { return Vec::new() };
    if let Some(nodes) = fields.get("nodes").and_then(Value::as_array) {
        return nodes.iter().filter(|node| !node.is_null()).cloned().collect();
    }
    let Some(edges) = fields.get("edges").and_then(Value::as_array) else { return Vec::new() };
    edges.iter().filter_map(|edge| edge.get("node")).filter(|node| !node.is_null()).cloned().collect()
}

pub fn connection_page_info(connection: &Value, cursor: Option<&CursorSpec>) -> PageInfo {
    let info = connection.get("pageInfo");
    PageInfo {
        has_next_page: info.and_then(|info| info.get("hasNextPage")).and_then(Value::as_bool).unwrap_or(false),
        end_cursor: cursor_text(connection, info, cursor),
        total: connection.get("totalCount").and_then(Value::as_i64),
    }
}

fn cursor_text(connection: &Value, info: Option<&Value>, cursor: Option<&CursorSpec>) -> Option<String> {
    let value = match cursor {
        Some(spec) => spec.from.split('.').try_fold(connection, |value, segment| value.get(segment))?,
        None => info?.get("endCursor")?,
    };
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) if cursor.is_some() => Some(number.to_string()),
        _ => None,
    }
}

pub fn landing_fault(connection: &Value, cursor: Option<&CursorSpec>) -> Option<LandingFault> {
    let Some(fields) = connection.as_object() else { return Some(LandingFault::PayloadNotObject) };
    let Some(info) = fields.get("pageInfo").and_then(Value::as_object) else { return Some(LandingFault::MissingPageInfo) };
    if info.get("hasNextPage") != Some(&Value::Bool(true)) {
        return None;
    }
    match cursor_text(connection, Some(&Value::Object(info.clone())), cursor) {
        Some(text) if !text.is_empty() => None,
        _ => Some(LandingFault::CursorMissing),
    }
}
