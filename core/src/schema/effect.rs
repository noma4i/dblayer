use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::ModelSpec;
use crate::identity::ListKey;
use crate::value::{normalize_relation_params, relation_scope_key, ListKeyError, RelationParamsError, ScalarKind};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectSource {
    Owner(String),
    Row { model: String, #[serde(default, skip_serializing_if = "Option::is_none")] row: Option<String>, field: String },
}

impl ModelSpec {
    pub(crate) fn selection_values(&self, relation: &str, params: &Map<String, Value>) -> Result<Map<String, Value>, SelectionKeyError> {
        let declared = self.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| SelectionKeyError::UnknownRelation(relation.to_string()))?;
        let mut values = params.clone();
        for (name, value) in &declared.text_defaults {
            values.entry(name.clone()).or_insert_with(|| Value::String(value.clone()));
        }
        Ok(values)
    }

    pub fn selection_key(&self, relation: &str, params: &Map<String, Value>) -> Result<ListKey, SelectionKeyError> {
        let params = self.selection_values(relation, params)?;
        let declared = self.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| SelectionKeyError::UnknownRelation(relation.to_string()))?;
        let kinds = self.param_kinds(declared)?;
        let mut scope_params = Map::new();
        for name in kinds.keys() {
            scope_params.insert(name.clone(), params.get(name).ok_or_else(|| SelectionKeyError::Param(format!("missing param {name}")))?.clone());
        }
        let normalized = normalize_relation_params(&scope_params, &kinds)?;
        Ok(relation_scope_key(self.key.as_str(), relation, &normalized)?)
    }

    pub fn partial_selection_params(&self, relation: &str, params: &Map<String, Value>) -> Result<BTreeMap<String, String>, SelectionKeyError> {
        let declared = self.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| SelectionKeyError::UnknownRelation(relation.to_string()))?;
        let kinds: BTreeMap<String, ScalarKind> = self.param_kinds(declared)?.into_iter().filter(|(name, _)| params.contains_key(name)).collect();
        Ok(normalize_relation_params(params, &kinds)?)
    }

    fn param_kinds(&self, declared: &super::RelationSpec) -> Result<BTreeMap<String, ScalarKind>, SelectionKeyError> {
        let mut kinds = BTreeMap::new();
        for column in &declared.by_columns {
            kinds.insert(column.clone(), self.column_scalar_kind(column).ok_or_else(|| SelectionKeyError::Param(format!("{column} is not a scalar or enum field")))?);
        }
        for param in &declared.text_params {
            kinds.insert(param.clone(), declared.scalar_types.get(param).copied().unwrap_or(ScalarKind::Str));
        }
        Ok(kinds)
    }

    pub fn selection_params(&self, relation: &str) -> Vec<String> {
        self.relations.iter().find(|candidate| candidate.name == relation).map(|declared| declared.by_columns.iter().chain(&declared.text_params).cloned().collect()).unwrap_or_default()
    }

    pub fn singleton_relation(&self) -> Option<&super::RelationSpec> {
        let mut found = self.relations.iter().filter(|relation| relation.is_singleton());
        let relation = found.next()?;
        found.next().is_none().then_some(relation)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SelectionKeyError {
    #[error("unknown relation {0}")]
    UnknownRelation(String),
    #[error("{0}")]
    Param(String),
    #[error(transparent)]
    Normalize(#[from] RelationParamsError),
    #[error(transparent)]
    Key(#[from] ListKeyError),
}
