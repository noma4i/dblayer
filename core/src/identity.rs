use std::fmt;

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentityError {
    #[error("row id must not be blank")]
    MissingId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RowId(String);

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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowRef {
    pub model: ModelKey,
    pub id: RowId,
}

impl RowRef {
    pub fn new(model: ModelKey, id: RowId) -> Self {
        Self { model, id }
    }

    pub fn action_key_prefix(&self) -> String {
        format!("{}:", self.model.as_str())
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

    pub fn find(model: &str, id: &RowId) -> Self {
        Self::compose(&[model, "id", id.as_str()])
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

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
mod tests {
    use super::*;

    #[test]
    fn row_id_normalizes_text_and_integers_and_rejects_blank() {
        assert_eq!(RowId::new("abc").unwrap().as_str(), "abc");
        assert_eq!(RowId::new("  abc \n").unwrap().as_str(), "abc");
        assert_eq!(RowId::from_integer(42).as_str(), "42");
        assert_eq!(RowId::new(""), Err(IdentityError::MissingId));
        assert_eq!(RowId::new("   "), Err(IdentityError::MissingId));
    }

    #[test]
    fn temp_ids_are_recognised_and_unique() {
        let plain = RowId::generate_temp(None);
        let prefixed = RowId::generate_temp(Some("message"));
        assert!(plain.is_temp());
        assert!(plain.as_str().starts_with("temp-"));
        assert!(prefixed.as_str().starts_with("temp-message-"));
        assert_ne!(plain, RowId::generate_temp(None));
        assert!(!RowId::new("server-1").unwrap().is_temp());
        assert!(RowId::new("temp-123").unwrap().is_temp());
    }

    #[test]
    fn row_id_serializes_as_string_and_deserializes_string_or_integer_but_not_blank() {
        let id = RowId::new("m1").unwrap();
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"m1\"");
        assert_eq!(serde_json::from_str::<RowId>("\"m1\"").unwrap().as_str(), "m1");
        assert_eq!(serde_json::from_str::<RowId>("42").unwrap().as_str(), "42");
        assert!(serde_json::from_str::<RowId>("\"   \"").is_err());
        assert!(serde_json::from_str::<RowId>("1.5").is_err());
        assert!(serde_json::from_str::<RowId>("true").is_err());
    }

    #[test]
    fn scope_key_encodes_segments_with_utf16_length() {
        assert_eq!(ScopeKey::compose(&["chats", "42"]).as_str(), "5:chats2:42");
        assert_eq!(ScopeKey::compose(&[""]).as_str(), "0:");
        assert_eq!(ScopeKey::compose::<&str>(&[]).as_str(), "");
        assert_eq!(ScopeKey::compose(&["🚀"]).as_str(), "2:🚀");
    }

    #[test]
    fn scope_key_round_trips_any_content() {
        let cases: Vec<Vec<&str>> = vec![vec!["chats", "42"], vec!["a:b"], vec!["3:xyz"], vec!["🚀"], vec![], vec![""]];
        for segments in cases {
            let key = ScopeKey::compose(&segments);
            assert_eq!(key.decompose(), Some(segments.iter().map(|s| s.to_string()).collect()));
        }
    }

    #[test]
    fn scope_key_distinguishes_ambiguous_splits() {
        assert_ne!(ScopeKey::compose(&["a", "bc"]), ScopeKey::compose(&["a:b", "c"]));
    }

    #[test]
    fn scope_key_rejects_malformed_text() {
        for text in ["x", "5:ab", "-1:a", "a:b", "2"] {
            assert_eq!(ScopeKey::from_raw(text).decompose(), None, "{text}");
        }
    }

    #[test]
    fn find_scope_uses_model_id_and_row_id() {
        let id = RowId::new("7").unwrap();
        assert_eq!(ScopeKey::find("moments", &id), ScopeKey::compose(&["moments", "id", "7"]));
    }

    #[test]
    fn generation_orders_and_advances() {
        assert_eq!(Generation::INITIAL.value(), 0);
        assert_eq!(Generation::INITIAL.next().value(), 1);
        assert!(Generation::INITIAL < Generation::INITIAL.next());
    }

    #[test]
    fn row_ref_prefixes_action_keys_with_model() {
        let reference = RowRef::new(ModelKey::new("moments"), RowId::new("1").unwrap());
        assert_eq!(reference.action_key_prefix(), "moments:");
    }
}
