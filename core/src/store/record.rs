use std::mem::size_of;
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::identity::RowId;
use crate::syncstate::SyncInfo;

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub id: RowId,
    pub revision: i64,
    pub data: Map<String, Value>,
    pub sync: SyncInfo,
}

pub type Row = Arc<Record>;

impl Record {
    pub fn is_local(&self) -> bool {
        self.id.is_temp()
    }

    pub fn get(&self, field: &str) -> Option<&Value> {
        self.data.get(field)
    }

    pub fn text(&self, field: &str) -> Option<&str> {
        self.data.get(field).and_then(Value::as_str)
    }

    #[cfg(test)]
    pub fn json(&self) -> String {
        Value::Object(self.data.clone()).to_string()
    }
}

fn object_bytes(data: &Map<String, Value>) -> usize {
    data.iter().map(|(key, value)| size_of::<(String, Value)>() + 4 * size_of::<usize>() + key.capacity() + value_bytes(value)).sum()
}

fn value_bytes(value: &Value) -> usize {
    match value {
        Value::String(text) => text.capacity(),
        Value::Array(items) => items.capacity() * size_of::<Value>() + items.iter().map(value_bytes).sum::<usize>(),
        Value::Object(data) => object_bytes(data),
        _ => 0,
    }
}

/// The memory a row of the model holds, as the platform reports it.
pub(crate) fn row_bytes(model: &str, row: &Record) -> usize {
    let strings = |values: &Vec<String>| values.capacity() * size_of::<String>() + values.iter().map(String::capacity).sum::<usize>();
    size_of::<Record>() + size_of::<(String, String)>() + usize::BITS as usize
        + model.len() + 2 * row.id.as_str().len() + object_bytes(&row.data)
        + strings(&row.sync.actions) + strings(&row.sync.dirty_fields)
        + row.sync.error.as_ref().map_or(0, |error| error.message.capacity())
        + row.sync.uploads.iter().map(|(action, upload)| action.capacity() + upload.error.as_ref().map_or(0, |error| error.message.capacity())).sum::<usize>()
}
