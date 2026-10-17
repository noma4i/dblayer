use serde_json::{Map, Value};

use crate::read::{connection_page_info, connection_rows, landing_fault};
use crate::schema::RemoteShape;
use crate::store::PageInfo;

use super::error::SyncError;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Extracted {
    pub nodes: Vec<Map<String, Value>>,
    pub page_info: Option<PageInfo>,
}

pub(crate) fn root_value<'a>(data: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(data, |value, segment| value.get(segment))
}

pub(crate) fn extract(data: &Value, root_path: &str, shape: &RemoteShape, total_from: Option<&str>) -> Result<Extracted, SyncError> {
    let root = root_value(data, root_path);
    let malformed = |reason: &str| SyncError::Malformed(format!("{root_path}: {reason}"));
    match shape {
        RemoteShape::Single => {
            let nodes = root.and_then(Value::as_object).cloned().into_iter().collect();
            Ok(Extracted { nodes, page_info: None })
        }
        RemoteShape::List { .. } => {
            let items = root.and_then(Value::as_array).ok_or_else(|| malformed("expected a list"))?;
            Ok(Extracted { nodes: objects(items), page_info: None })
        }
        RemoteShape::Connection { cursor, .. } => {
            let cursor = cursor.as_ref();
            let connection = root.ok_or_else(|| malformed("expected a connection"))?;
            if let Some(fault) = landing_fault(connection, cursor) {
                return Err(malformed(&format!("{fault:?}")));
            }
            let mut page_info = connection_page_info(connection, cursor);
            if let Some(path) = total_from {
                page_info.total = root_value(data, path).and_then(Value::as_i64);
            }
            Ok(Extracted { nodes: objects(&connection_rows(connection)), page_info: Some(page_info) })
        }
    }
}

fn objects(items: &[Value]) -> Vec<Map<String, Value>> {
    items.iter().filter_map(Value::as_object).cloned().collect()
}
