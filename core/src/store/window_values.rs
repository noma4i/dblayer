use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::commit::{EdgeValue, EdgeWrite, ScopeRef};
use crate::store::DbError;
use crate::owners::read::windows::WindowKey;
use crate::owners::store::RowValue;
use crate::schema::{FieldKind, FieldSpec};
use crate::value::{read_id, ScalarKind};

pub(crate) struct WindowPage<'a> {
    pub key: &'a WindowKey,
    pub page: u32,
    pub replace_all: bool,
    pub cursor: Option<&'a str>,
    pub roots: &'a [RowValue],
    pub related: &'a [(String, RowValue)],
    pub total: Option<i64>,
    pub base_revision: i64,
}

pub(crate) fn row_values(row: &RowValue) -> Result<Map<String, Value>, DbError> {
    match serde_json::from_str(&row.values).map_err(|error| DbError::Internal(error.to_string()))? {
        Value::Object(map) => Ok(map),
        _ => Err(DbError::Internal("the values of a row are not an object".into())),
    }
}

fn edge_value(kind: &FieldKind, value: &Value) -> Option<EdgeValue> {
    match kind {
        FieldKind::Scalar { kind: ScalarKind::Int } => value.as_i64().map(EdgeValue::Whole),
        FieldKind::Scalar { kind: ScalarKind::Num } => value.as_f64().map(EdgeValue::Fraction),
        FieldKind::Scalar { kind: ScalarKind::Bool } => value.as_bool().map(|flag| EdgeValue::Whole(i64::from(flag))),
        FieldKind::Scalar { .. } | FieldKind::Enum { .. } => value.as_str().map(|text| EdgeValue::Text(text.to_string())),
        _ => None,
    }
}

pub(crate) fn edge_writes(scope: &ScopeRef, fields: &[FieldSpec], roots: &[Map<String, Value>], edges: &BTreeMap<String, Map<String, Value>>) -> Vec<EdgeWrite> {
    if fields.is_empty() { return Vec::new(); }
    roots.iter().filter_map(|root| root.get("id").and_then(read_id)).map(|row| {
        let edge = edges.get(row.as_str());
        let values = fields.iter().filter_map(|field| {
            let value = field.decode(&field.read(edge?)?)?;
            Some((field.name.clone(), edge_value(&field.kind, &value)?))
        }).collect();
        EdgeWrite { scope: scope.clone(), row, values }
    }).collect()
}
