//! Owner `Outbox`: the optimistic effects of an action, planned from what the base shows (`PlanView`). Pure: the same view and call give the same effects.

use serde_json::{Map, Value};

use crate::identity::ListKey;
use crate::schema::keyed::{include_flagged, raise_to, Flagged};
use crate::schema::{EffectSource, ModelSpec, ScopeEffect, ScopeEffectKind};
use crate::value::{fractional_between, read_id};

type Row = Map<String, Value>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanError(pub String);

impl std::fmt::Display for PlanError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str(&self.0) }
}

/// What the plan reads from the base.
pub trait PlanView {
    fn row(&self, model: &str, id: &str) -> Result<Option<Row>, PlanError>;
    /// The order key of `id` in the list, `None` when it is not a member.
    fn member_key(&self, model: &str, list: &str, id: &str) -> Result<Option<String>, PlanError>;
    fn last_key(&self, model: &str, list: &str) -> Result<Option<String>, PlanError>;
    /// The id of the single row of a singleton model.
    fn singleton(&self, model: &str) -> Result<Option<String>, PlanError>;
    /// The rows that go with `(model, id)` when it is removed: the row itself and what depends on it.
    fn removed_with(&self, model: &str, id: &str) -> Result<Vec<(String, String)>, PlanError>;
}

/// A change of a list: `order_key` is set for a row that joins now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListChange {
    Join { model: String, list: ListKey, id: String, before: Option<String>, order_key: Option<String> },
    Leave { model: String, list: ListKey, id: String, before: Option<String> },
}

fn effect_ids(effect: &ScopeEffect, args: &Row) -> Result<Vec<String>, PlanError> {
    let mut ids: Vec<String> = match args.get(&effect.rows) {
        Some(Value::Array(items)) => items.iter().filter_map(|item| item.as_str().map(str::to_string)).collect(),
        Some(Value::String(one)) => vec![one.clone()],
        _ => return Err(PlanError(format!("the argument {} of the scope effect is missing", effect.rows))),
    };
    let mut seen = std::collections::HashSet::new();
    ids.retain(|id| seen.insert(id.clone()));
    Ok(ids)
}

/// The rows of `effect.rows` join or leave the list of every order of the effect, each once. A row that is not in the base joins nothing.
pub fn scope_changes(view: &dyn PlanView, target: &ModelSpec, effect: &ScopeEffect, owner_id: &str, args: &Row) -> Result<Vec<ListChange>, PlanError> {
    let ids = effect_ids(effect, args)?;
    let mut changes = Vec::new();
    for order in &effect.orders {
        let mut params = Map::new();
        for (name, source) in &effect.with {
            let value = if source == "id" { Value::String(owner_id.to_string()) } else { args.get(source).cloned().ok_or_else(|| PlanError(format!("the argument {source} of the scope effect is missing")))? };
            params.insert(name.clone(), value);
        }
        params.insert(effect.order_param.clone(), Value::String(order.clone()));
        let list = target.selection_key(&effect.scope, &params).map_err(|error| PlanError(error.to_string()))?;
        let mut last = view.last_key(&effect.model, list.as_str())?;
        for id in &ids {
            let before = view.member_key(&effect.model, list.as_str(), id)?;
            changes.push(match effect.kind {
                ScopeEffectKind::Join => {
                    let order_key = if before.is_none() && view.row(&effect.model, id)?.is_some() {
                        let key = fractional_between(last.as_deref(), None).map_err(|error| PlanError(error.to_string()))?;
                        last = Some(key.clone());
                        Some(key)
                    } else {
                        None
                    };
                    ListChange::Join { model: effect.model.clone(), list: list.clone(), id: id.clone(), before, order_key }
                }
                ScopeEffectKind::Leave => ListChange::Leave { model: effect.model.clone(), list: list.clone(), id: id.clone(), before },
            });
        }
    }
    Ok(changes)
}

#[cfg(test)]
/// The rows an action that removes its receiver tombstones: the receiver and what depends on it. The lists drop a tombstoned row, the rollback gives it back in place.
pub fn receiver_tombstones(view: &dyn PlanView, model: &str, id: &str) -> Result<Vec<(String, String)>, PlanError> { view.removed_with(model, id) }

fn effect_row(view: &dyn PlanView, args: &Row, model: &str, row: Option<&str>) -> Result<Option<String>, PlanError> {
    match row {
        Some(name) => Ok(args.get(name).and_then(read_id).map(|id| id.to_string())),
        None => view.singleton(model),
    }
}

fn source_value(view: &dyn PlanView, merged: &Row, owner: (&str, &str), args: &Row, source: &EffectSource) -> Result<Option<Value>, PlanError> {
    let present = |row: &Row, field: &str| row.get(field).filter(|value| !value.is_null()).cloned();
    Ok(match source {
        EffectSource::Owner(field) if field == "id" => Some(Value::String(owner.1.to_string())),
        EffectSource::Owner(field) => present(merged, field),
        EffectSource::Row { model, row, field } => {
            let Some(id) = effect_row(view, args, model, row.as_deref())? else { return Ok(None) };
            if model == owner.0 && id == owner.1 {
                return Ok(if field == "id" { Some(Value::String(id)) } else { present(merged, field) });
            }
            let found = view.row(model, &id)?;
            if field == "id" { found.map(|_| Value::String(id)) } else { found.and_then(|row| present(&row, field)) }
        }
    })
}

/// The receiver after the action: the patch, then the keyed elements, then the flagged values (`include` puts an id at the head of a list, `at_least` lifts a number).
/// The keyed elements and the flagged values an action puts after its patch.
pub struct Fields<'a> {
    pub keyed: &'a [(String, Row)],
    pub flagged: &'a [(String, String, Flagged)],
}

pub fn optimistic_row(spec: &ModelSpec, view: &dyn PlanView, (current, patch): (&Row, &Row), Fields { keyed, flagged }: Fields<'_>, args: &Row, owner: (&str, &str)) -> Result<Row, PlanError> {
    let mut merged = current.clone();
    merged.extend(patch.clone());
    for (field, op) in keyed {
        let Some(declared) = spec.field(field) else { continue };
        let target_row = |model: &str, id: &str| view.row(model, id);
        if let Some(next) = crate::schema::keyed::apply(declared, current.get(field), op, args, target_row)? {
            merged.insert(field.clone(), next);
        }
    }
    for (field, when, kind) in flagged {
        let Some(flag) = args.get(when).and_then(Value::as_bool) else { continue };
        let next = match kind {
            Flagged::Include(source) => {
                let Some(value) = source_value(view, &merged, owner, args, source)? else { continue };
                include_flagged(merged.get(field), value, flag)
            }
            Flagged::AtLeast(_) if !flag => continue,
            Flagged::AtLeast(min) => raise_to(merged.get(field), *min),
        };
        merged.insert(field.clone(), next);
    }
    Ok(merged)
}
