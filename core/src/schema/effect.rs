use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{DeclarationError, FieldKind, ModelSpec};
use crate::identity::ListKey;
use crate::value::{normalize_relation_params, relation_scope_key, ListKeyError, RelationParamsError, ScalarKind};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectSource {
    Literal(Value),
    Arg(String),
    Owner(String),
    Row { model: String, #[serde(default, skip_serializing_if = "Option::is_none")] row: Option<String>, field: String },
    Plus(String),
    Minus(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Selection {
    pub model: String,
    pub scope: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub with: BTreeMap<String, EffectSource>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ActionEffect {
    Add(Selection),
    Remove(Selection),
    Create { model: String, values: BTreeMap<String, EffectSource> },
    Change { model: String, #[serde(default, skip_serializing_if = "Option::is_none")] row: Option<String>, values: BTreeMap<String, EffectSource> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectFault {
    UnknownModel(String),
    UnknownScope { model: String, scope: String },
    ScopeParameter { scope: String, parameter: String },
    UnboundParameter { scope: String, parameter: String },
    PartialBinding(String),
    RowsParameter(String),
    RowsModel(String),
    UnknownArg(String),
    UnknownField { model: String, field: String },
    NotNumeric(String),
    CreatedTwice(String),
    NotSingleton(String),
    PlusOutsideChange,
}

impl ModelSpec {
    pub fn selection_key(&self, relation: &str, params: &Map<String, Value>) -> Result<ListKey, SelectionKeyError> {
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

fn arg_is_id(kind: &str) -> bool {
    matches!(kind.trim_end_matches('?'), "id" | "ids")
}

fn arg_is_number(kind: &str) -> bool {
    matches!(kind.trim_end_matches('?'), "int" | "long" | "num")
}

fn source_args(source: &EffectSource) -> Option<&str> {
    match source {
        EffectSource::Arg(name) | EffectSource::Plus(name) | EffectSource::Minus(name) => Some(name),
        _ => None,
    }
}

fn check_source(owner: &ModelSpec, by_key: &BTreeMap<&str, &ModelSpec>, args: &BTreeMap<String, String>, source: &EffectSource) -> Result<(), EffectFault> {
    if let Some(name) = source_args(source) {
        if !args.contains_key(name) {
            return Err(EffectFault::UnknownArg(name.to_string()));
        }
    }
    match source {
        EffectSource::Owner(field) if field != "id" && owner.field(field).is_none() => Err(EffectFault::UnknownField { model: owner.key.to_string(), field: field.clone() }),
        EffectSource::Row { model, row, field } => {
            let target = by_key.get(model.as_str()).ok_or_else(|| EffectFault::UnknownModel(model.clone()))?;
            if field != "id" && target.field(field).is_none() {
                return Err(EffectFault::UnknownField { model: model.clone(), field: field.clone() });
            }
            match row {
                Some(name) if !args.get(name).is_some_and(|kind| arg_is_id(kind)) => Err(EffectFault::RowsParameter(name.clone())),
                None if target.singleton_relation().is_none() => Err(EffectFault::NotSingleton(model.clone())),
                _ => Ok(()),
            }
        }
        _ => Ok(()),
    }
}

fn check_selection(owner: &ModelSpec, by_key: &BTreeMap<&str, &ModelSpec>, args: &BTreeMap<String, String>, selection: &Selection, adds: bool) -> Result<(), EffectFault> {
    let target = by_key.get(selection.model.as_str()).ok_or_else(|| EffectFault::UnknownModel(selection.model.clone()))?;
    let declared = target.relations.iter().find(|relation| relation.name == selection.scope && relation.remote.is_some());
    let declared = declared.ok_or_else(|| EffectFault::UnknownScope { model: selection.model.clone(), scope: selection.scope.clone() })?;
    match &selection.rows {
        Some(name) if !args.get(name).is_some_and(|kind| arg_is_id(kind)) => return Err(EffectFault::RowsParameter(name.clone())),
        None if selection.model != owner.key.as_str() => return Err(EffectFault::RowsModel(selection.scope.clone())),
        _ => {}
    }
    let params: BTreeSet<&String> = declared.by_columns.iter().chain(&declared.text_params).collect();
    for (name, source) in &selection.with {
        if !params.contains(name) {
            return Err(EffectFault::ScopeParameter { scope: selection.scope.clone(), parameter: name.clone() });
        }
        check_source(owner, by_key, args, source)?;
    }
    let mut unbound = params.iter().filter(|name| !selection.with.contains_key(**name));
    if adds {
        return match unbound.find(|name| !declared.text_defaults.contains_key(**name)) {
            Some(name) => Err(EffectFault::UnboundParameter { scope: selection.scope.clone(), parameter: (*name).clone() }),
            None => Ok(()),
        };
    }
    if !selection.with.is_empty() && unbound.next().is_some() {
        return Err(EffectFault::PartialBinding(selection.scope.clone()));
    }
    Ok(())
}

pub(super) fn check_effects(owner: &ModelSpec, by_key: &BTreeMap<&str, &ModelSpec>) -> Result<(), DeclarationError> {
    for action in &owner.actions {
        let fault = |fault| DeclarationError::Effect { action: action.name.clone(), fault };
        let mut created = BTreeSet::new();
        for effect in &action.effects {
            match effect {
                ActionEffect::Add(selection) => check_selection(owner, by_key, &action.args, selection, true).map_err(fault)?,
                ActionEffect::Remove(selection) => check_selection(owner, by_key, &action.args, selection, false).map_err(fault)?,
                ActionEffect::Create { model, values } => {
                    let target = by_key.get(model.as_str()).ok_or_else(|| fault(EffectFault::UnknownModel(model.clone())))?;
                    if !created.insert(model.clone()) {
                        return Err(fault(EffectFault::CreatedTwice(model.clone())));
                    }
                    for (name, source) in values {
                        if target.field(name).is_none() {
                            return Err(fault(EffectFault::UnknownField { model: model.clone(), field: name.clone() }));
                        }
                        if matches!(source, EffectSource::Plus(_) | EffectSource::Minus(_)) {
                            return Err(fault(EffectFault::PlusOutsideChange));
                        }
                        check_source(owner, by_key, &action.args, source).map_err(fault)?;
                    }
                }
                ActionEffect::Change { model, row, values } => {
                    let target = by_key.get(model.as_str()).ok_or_else(|| fault(EffectFault::UnknownModel(model.clone())))?;
                    match row {
                        Some(name) if !action.args.get(name).is_some_and(|kind| arg_is_id(kind)) => return Err(fault(EffectFault::RowsParameter(name.clone()))),
                        None if target.singleton_relation().is_none() => return Err(fault(EffectFault::NotSingleton(model.clone()))),
                        _ => {}
                    }
                    for (name, source) in values {
                        let declared = target.field(name).ok_or_else(|| fault(EffectFault::UnknownField { model: model.clone(), field: name.clone() }))?;
                        let numeric = matches!(declared.kind, FieldKind::Scalar { kind: ScalarKind::Int | ScalarKind::Num });
                        if matches!(source, EffectSource::Plus(_) | EffectSource::Minus(_)) && !(numeric && source_args(source).and_then(|arg| action.args.get(arg)).is_some_and(|kind| arg_is_number(kind))) {
                            return Err(fault(EffectFault::NotNumeric(name.clone())));
                        }
                        check_source(owner, by_key, &action.args, source).map_err(fault)?;
                    }
                }
            }
        }
    }
    Ok(())
}
