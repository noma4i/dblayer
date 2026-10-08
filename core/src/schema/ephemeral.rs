use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::pacing::parse_duration;

use crate::value::duplicates;

use super::{DeclarationError, FieldKind, FieldSpec, ModelSpec, is_reserved_name};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EphemeralSpec {
    #[serde(flatten)]
    pub field: FieldSpec,
    #[serde(default)]
    pub reset_after: Option<String>,
}

impl EphemeralSpec {
    pub fn new(field: FieldSpec) -> Self {
        Self { field, reset_after: None }
    }

    pub fn resetting_after(self, window: &str) -> Self {
        Self { reset_after: Some(window.to_string()), ..self }
    }

    pub fn reset_window(&self) -> Option<Duration> {
        self.reset_after.as_deref().and_then(parse_duration)
    }

    pub fn initial(&self) -> Value {
        self.field.default.clone().unwrap_or(Value::Null)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Storage {
    #[default]
    Disk,
    Memory,
}

impl ModelSpec {
    pub fn is_memory(&self) -> bool {
        self.storage == Storage::Memory
    }

    pub fn expire_window(&self) -> Option<Duration> {
        self.expire_after.as_deref().and_then(parse_duration)
    }

    pub fn ephemeral_field(&self, name: &str) -> Option<&EphemeralSpec> {
        self.ephemeral.iter().find(|spec| spec.field.name == name)
    }
}

fn positive_window(text: &str) -> Result<(), DeclarationError> {
    match parse_duration(text) {
        Some(window) if !window.is_zero() => Ok(()),
        _ => Err(DeclarationError::InvalidDuration(text.to_string())),
    }
}

impl ModelSpec {
    pub(super) fn check_ephemeral(&self) -> Result<(), DeclarationError> {
        for spec in &self.ephemeral {
            let field = &spec.field;
            if field.name.is_empty() {
                return Err(DeclarationError::EmptyFieldName);
            }
            if is_reserved_name(&field.name) {
                return Err(DeclarationError::ReservedFieldName(field.name.clone()));
            }
            if self.field(&field.name).is_some() {
                return Err(DeclarationError::DuplicateField(field.name.clone()));
            }
            if field.indexed {
                return Err(DeclarationError::EphemeralIndexed(field.name.clone()));
            }
            if field.kind == FieldKind::File {
                return Err(DeclarationError::EphemeralFile(field.name.clone()));
            }
            if field.default.is_none() && spec.reset_after.is_some() {
                return Err(DeclarationError::ResetWithoutDefault(field.name.clone()));
            }
            if field.default.is_none() && !field.nullable {
                return Err(DeclarationError::EphemeralNeedsDefault(field.name.clone()));
            }
            if let Some(window) = &spec.reset_after {
                positive_window(window)?;
            }
        }
        let names: Vec<&str> = self.ephemeral.iter().map(|spec| spec.field.name.as_str()).collect();
        if let Some(name) = duplicates(&names).first() {
            return Err(DeclarationError::DuplicateField(name.to_string()));
        }
        let indexed = self.composite_indexes.iter().flatten().find(|name| self.ephemeral_field(name).is_some());
        match indexed {
            Some(name) => Err(DeclarationError::EphemeralIndexed(name.clone())),
            None => Ok(()),
        }
    }

    pub(super) fn check_storage(&self) -> Result<(), DeclarationError> {
        if let Some(window) = &self.expire_after {
            if !self.is_memory() {
                return Err(DeclarationError::ExpireNeedsMemory);
            }
            positive_window(window)?;
        }
        if !self.is_memory() {
            return Ok(());
        }
        if let Some(field) = self.fields.iter().find(|field| field.kind == FieldKind::File) {
            return Err(DeclarationError::MemoryHasFile(field.name.clone()));
        }
        match self.actions.first() {
            Some(action) => Err(DeclarationError::MemoryHasActions(action.name.clone())),
            None => Ok(()),
        }
    }
}
