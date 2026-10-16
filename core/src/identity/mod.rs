use std::fmt;

pub(crate) mod touches;

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentityError {
    #[error("row id must not be blank")]
    MissingId,
}

#[derive(Debug, Clone, Eq, PartialOrd, Ord)]
pub struct RowId(String);

impl PartialEq for RowId {
    fn eq(&self, other: &Self) -> bool {
        touches::touch(1);
        self.0 == other.0
    }
}

impl std::hash::Hash for RowId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        touches::touch(1);
        self.0.hash(state);
    }
}

impl RowId {
    pub fn new(value: &str) -> Result<Self, IdentityError> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(IdentityError::MissingId);
        }
        Ok(Self(trimmed.to_string()))
    }

    pub fn from_integer(value: i64) -> Self {
        Self(value.to_string())
    }

    pub fn generate_temp(prefix: Option<&str>) -> Self {
        let prefix = prefix.map(|text| format!("{text}-")).unwrap_or_default();
        Self(format!("temp-{prefix}{}", uuid::Uuid::new_v4().hyphenated().to_string().to_uppercase()))
    }

    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().hyphenated().to_string().to_uppercase())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_temp(&self) -> bool {
        self.0.starts_with("temp-")
    }
}

impl fmt::Display for RowId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for RowId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for RowId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RowIdVisitor;

        impl Visitor<'_> for RowIdVisitor {
            type Value = RowId;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string or integer row id")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<RowId, E> {
                RowId::new(value).map_err(|_| E::custom("RowId must not be blank"))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<RowId, E> {
                Ok(RowId::from_integer(value))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<RowId, E> {
                Ok(RowId(value.to_string()))
            }
        }

        deserializer.deserialize_any(RowIdVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ModelKey(String);

impl ModelKey {
    pub fn new(value: &str) -> Self {
        Self(value.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ModelKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RowRef {
    pub model: ModelKey,
    pub id: RowId,
}

impl RowRef {
    pub fn new(model: ModelKey, id: RowId) -> Self {
        Self { model, id }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ScopeKey(String);

impl ScopeKey {
    pub fn from_raw(value: &str) -> Self {
        Self(value.to_string())
    }

    pub fn compose<S: AsRef<str>>(segments: &[S]) -> Self {
        let mut value = String::new();
        for segment in segments {
            let segment = segment.as_ref();
            value.push_str(&segment.encode_utf16().count().to_string());
            value.push(':');
            value.push_str(segment);
        }
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[cfg(test)]
    pub fn decompose(&self) -> Option<Vec<String>> {
        let mut segments = Vec::new();
        let mut rest = self.0.as_str();
        while !rest.is_empty() {
            let (length_text, tail) = rest.split_once(':')?;
            if length_text.is_empty() || !length_text.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            let units: usize = length_text.parse().ok()?;
            let mut consumed_units = 0;
            let mut end = 0;
            for (index, character) in tail.char_indices() {
                if consumed_units == units {
                    break;
                }
                consumed_units += character.len_utf16();
                end = index + character.len_utf8();
            }
            if consumed_units != units {
                return None;
            }
            segments.push(tail[..end].to_string());
            rest = &tail[end..];
        }
        Some(segments)
    }
}

impl fmt::Display for ScopeKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Generation(u64);

impl Generation {
    pub const INITIAL: Generation = Generation(0);

    pub fn value(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Generation {
        Generation(self.0 + 1)
    }
}

#[cfg(test)]
mod identity_tests;
