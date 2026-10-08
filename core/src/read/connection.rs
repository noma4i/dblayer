use serde_json::Value;

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

pub fn connection_page_info(connection: &Value) -> PageInfo {
    let info = connection.get("pageInfo");
    PageInfo {
        has_next_page: info.and_then(|info| info.get("hasNextPage")).and_then(Value::as_bool).unwrap_or(false),
        end_cursor: info.and_then(|info| info.get("endCursor")).and_then(Value::as_str).map(str::to_string),
    }
}

pub fn connection_total_count(connection: &Value) -> Option<i64> {
    let count = connection.get("totalCount")?.as_f64()?;
    count.is_finite().then(|| count.trunc() as i64)
}

pub fn landing_fault(connection: &Value) -> Option<LandingFault> {
    let Some(fields) = connection.as_object() else { return Some(LandingFault::PayloadNotObject) };
    let Some(info) = fields.get("pageInfo").and_then(Value::as_object) else { return Some(LandingFault::MissingPageInfo) };
    if info.get("hasNextPage") != Some(&Value::Bool(true)) {
        return None;
    }
    match info.get("endCursor") {
        None | Some(Value::Null) => Some(LandingFault::CursorMissing),
        Some(Value::String(cursor)) if cursor.is_empty() => Some(LandingFault::CursorMissing),
        _ => None,
    }
}

pub fn is_landable(connection: &Value) -> bool {
    landing_fault(connection).is_none()
}
