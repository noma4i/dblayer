use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One input of a trace: a call of the public API or an input of the transport mock. Arguments are named JSON values.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Input {
    #[serde(rename = "in")]
    pub name: String,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub args: Map<String, Value>,
}

impl Input {
    pub fn new(name: &str) -> Self {
        Self { name: name.into(), args: Map::new() }
    }

    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.args.insert(key.into(), value.into());
        self
    }

    pub fn text(&self, key: &str) -> Option<&str> {
        self.args.get(key).and_then(Value::as_str)
    }

    pub fn number(&self, key: &str) -> Option<u64> {
        self.args.get(key).and_then(Value::as_u64)
    }

    pub fn flag(&self, key: &str) -> Option<bool> {
        self.args.get(key).and_then(Value::as_bool)
    }
}

impl fmt::Display for Input {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(out, "{}", self.name)?;
        if !self.args.is_empty() {
            write!(out, "{}", Value::Object(self.args.clone()))?;
        }
        Ok(())
    }
}

/// One observable value after an input: a snapshot, an event, `Lost`, an outcome of a command or an error.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Observation {
    #[serde(rename = "o")]
    pub kind: String,
    #[serde(default, rename = "v", skip_serializing_if = "Value::is_null")]
    pub value: Value,
}

impl Observation {
    pub fn new(kind: &str, value: Value) -> Self {
        Self { kind: kind.into(), value }
    }

    pub fn bare(kind: &str) -> Self {
        Self { kind: kind.into(), value: Value::Null }
    }
}

impl fmt::Display for Observation {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.value.is_null() { write!(out, "{}", self.kind) } else { write!(out, "{} {}", self.kind, self.value) }
    }
}

/// An input with the observations expected right after it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Step {
    #[serde(flatten)]
    pub input: Input,
    #[serde(default, rename = "out")]
    pub observed: Vec<Observation>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Trace {
    pub name: String,
    pub steps: Vec<Step>,
}

impl Trace {
    pub fn inputs(&self) -> Vec<Input> {
        self.steps.iter().map(|step| step.input.clone()).collect()
    }
}

pub fn list(items: &[Observation]) -> String {
    if items.is_empty() { "[]".into() } else { format!("[{}]", items.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")) }
}
