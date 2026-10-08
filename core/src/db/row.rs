use std::sync::Arc;

use serde_json::{Map, Value};

use crate::identity::RowId;

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub id: RowId,
    pub revision: i64,
    pub data: Map<String, Value>,
}

pub type Row = Arc<Record>;

impl Record {
    pub fn get(&self, field: &str) -> Option<&Value> {
        self.data.get(field)
    }

    pub fn text(&self, field: &str) -> Option<&str> {
        self.data.get(field).and_then(Value::as_str)
    }

    pub fn json(&self) -> String {
        Value::Object(self.data.clone()).to_string()
    }
}
