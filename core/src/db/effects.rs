use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::commit::{ScopeOp, ScopeRef};
use crate::identity::{ListKey, ModelKey, RowId, RowRef};
use crate::schema::{ActionEffect, EffectSource, ModelSpec, Selection};
use crate::store::Reader;
use crate::value::{fractional_between, read_id};

use super::error::DbError;
use super::local_write::{is_resident, local_scope_ops, reject_invalid_values, reject_unknown_fields};
use super::open::Db;
use super::prior::PriorRow;

type Row = Map<String, Value>;

pub(crate) struct Edits {
    rows: BTreeMap<(String, String), (Row, Row)>,
}

impl Edits {
    pub(crate) fn of_owner(model: &str, id: &RowId, current: Row, merged: Row) -> Self {
        Self { rows: BTreeMap::from([((model.to_string(), id.to_string()), (current, merged))]) }
    }

    pub(crate) fn into_rows(self) -> impl Iterator<Item = ((String, String), (Row, Row))> {
        self.rows.into_iter()
    }

    fn row_mut(&mut self, reader: &Reader<'_>, model: &str, id: &str) -> Result<Option<&mut Row>, DbError> {
        let key = (model.to_string(), id.to_string());
        if !self.rows.contains_key(&key) {
            let Some(current) = reader.point(model, id)? else { return Ok(None) };
            self.rows.insert(key.clone(), (current.clone(), current));
        }
        Ok(self.rows.get_mut(&key).map(|(_, merged)| merged))
    }
}

#[derive(Default)]
pub(crate) struct Planned {
    pub created: Vec<(ModelKey, Row)>,
    pub scope_ops: Vec<ScopeOp>,
    pub prior: Vec<PriorRow>,
}

pub(crate) struct Call<'a> {
    pub owner: &'a RowRef,
    pub args: &'a Map<String, Value>,
    pub now_ms: i64,
}

impl Db {
    pub(crate) fn plan_effects(&self, reader: &Reader<'_>, edits: &mut Edits, call: &Call<'_>, effects: &[ActionEffect]) -> Result<Planned, DbError> {
        let mut planned = Planned::default();
        let mut fronts: BTreeMap<(String, String), Option<String>> = BTreeMap::new();
        for effect in effects {
            match effect {
                ActionEffect::Add(selection) => self.add_members(reader, edits, call, selection, &mut planned, &mut fronts)?,
                ActionEffect::Remove(selection) => self.remove_members(reader, edits, call, selection, &mut planned)?,
                ActionEffect::Create { model, values } => self.create_row(reader, edits, call, model, values, &mut planned)?,
                ActionEffect::Change { model, row, values } => self.change_row(reader, edits, call, model, row.as_deref(), values)?,
            }
        }
        Ok(planned)
    }

    fn add_members(&self, reader: &Reader<'_>, edits: &mut Edits, call: &Call<'_>, selection: &Selection, planned: &mut Planned, fronts: &mut BTreeMap<(String, String), Option<String>>) -> Result<(), DbError> {
        let target = self.shared.spec(&selection.model)?;
        let Some(key) = self.selection_key(reader, edits, call, target, selection, true)? else { return Ok(()) };
        let model = ModelKey::new(&selection.model);
        for id in self.selected_rows(call, selection).iter().rev() {
            if reader.scope_has_member(&selection.model, key.as_str(), id.as_str())? || !is_resident(reader, &model, id)? {
                continue;
            }
            let front = match fronts.get(&(selection.model.clone(), key.to_string())) {
                Some(front) => front.clone(),
                None => reader.scope_first_key(&selection.model, key.as_str())?,
            };
            let order_key = fractional_between(None, front.as_deref()).map_err(|_| crate::read::LandingError::InvalidOrderBounds)?;
            fronts.insert((selection.model.clone(), key.to_string()), Some(order_key.clone()));
            planned.scope_ops.push(ScopeOp::Append { scope: ScopeRef::new(model.clone(), key.clone()), row: id.clone(), order_key });
            planned.prior.push(PriorRow::Member { model: selection.model.clone(), id: id.to_string(), scope_key: key.to_string(), before: None });
        }
        Ok(())
    }

    fn remove_members(&self, reader: &Reader<'_>, edits: &mut Edits, call: &Call<'_>, selection: &Selection, planned: &mut Planned) -> Result<(), DbError> {
        let target = self.shared.spec(&selection.model)?;
        let model = ModelKey::new(&selection.model);
        let prefix = ListKey::compose(&[selection.model.as_str(), selection.scope.as_str()]);
        let every_value = selection.with.is_empty() && !target.selection_params(&selection.scope).is_empty();
        let bound = if every_value { None } else { self.selection_key(reader, edits, call, target, selection, false)? };
        if !every_value && bound.is_none() {
            return Ok(());
        }
        for id in self.selected_rows(call, selection) {
            let keys = match &bound {
                Some(key) => vec![key.to_string()],
                None => reader.scopes_of(&selection.model, id.as_str())?.into_iter().filter(|key| key.starts_with(prefix.as_str())).collect(),
            };
            for key in keys {
                let Some(order_key) = reader.scope_member_key(&selection.model, &key, id.as_str())? else { continue };
                planned.scope_ops.push(ScopeOp::Detach { scope: ScopeRef::new(model.clone(), ListKey::from_raw(&key)), row: id.clone() });
                planned.prior.push(PriorRow::Member { model: selection.model.clone(), id: id.to_string(), scope_key: key, before: Some(order_key) });
            }
        }
        Ok(())
    }

    fn create_row(&self, reader: &Reader<'_>, edits: &mut Edits, call: &Call<'_>, model: &str, values: &BTreeMap<String, EffectSource>, planned: &mut Planned) -> Result<(), DbError> {
        let target = self.shared.spec(model)?;
        let mut row = Row::new();
        for (name, source) in values {
            if let Some(value) = self.source_value(reader, edits, call, source)? {
                row.insert(name.clone(), value);
            }
        }
        reject_unknown_fields(target, model, &row)?;
        let id = RowId::generate_temp(None);
        row.insert("id".to_string(), Value::String(id.to_string()));
        reject_invalid_values(target, model, &row)?;
        let data = target.normalize_local(&row, call.now_ms)?;
        planned.scope_ops.extend(local_scope_ops(reader, target, &id, &data)?);
        planned.prior.push(PriorRow::Created { model: model.to_string(), id: id.to_string() });
        planned.created.push((ModelKey::new(model), data));
        Ok(())
    }

    fn change_row(&self, reader: &Reader<'_>, edits: &mut Edits, call: &Call<'_>, model: &str, row: Option<&str>, values: &BTreeMap<String, EffectSource>) -> Result<(), DbError> {
        let Some(id) = self.effect_row(reader, call, model, row)? else { return Ok(()) };
        let mut changes = Row::new();
        for (name, source) in values {
            let next = match source {
                EffectSource::Plus(arg) | EffectSource::Minus(arg) => {
                    let Some(delta) = call.args.get(arg).filter(|value| !value.is_null()) else { continue };
                    let Some(current) = edits.row_mut(reader, model, id.as_str())?.and_then(|row| row.get(name).cloned()).filter(|value| !value.is_null()) else { continue };
                    shifted(&current, delta, matches!(source, EffectSource::Minus(_)))
                }
                other => self.source_value(reader, edits, call, other)?,
            };
            if let Some(next) = next {
                changes.insert(name.clone(), next);
            }
        }
        if let Some(merged) = edits.row_mut(reader, model, id.as_str())? {
            merged.extend(changes);
        }
        Ok(())
    }

    fn selection_key(&self, reader: &Reader<'_>, edits: &mut Edits, call: &Call<'_>, target: &ModelSpec, selection: &Selection, defaults: bool) -> Result<Option<ListKey>, DbError> {
        let declared = target.relations.iter().find(|relation| relation.name == selection.scope);
        let mut params = Map::new();
        for name in target.selection_params(&selection.scope) {
            let value = match selection.with.get(&name) {
                Some(source) => self.source_value(reader, edits, call, source)?,
                None if defaults => declared.and_then(|relation| relation.text_defaults.get(&name)).map(|text| Value::String(text.clone())),
                None => None,
            };
            let Some(value) = value else { return Ok(None) };
            params.insert(name, value);
        }
        Ok(Some(target.selection_key(&selection.scope, &params)?))
    }

    fn selected_rows(&self, call: &Call<'_>, selection: &Selection) -> Vec<RowId> {
        match selection.rows.as_ref().map(|name| call.args.get(name)) {
            None => vec![call.owner.id.clone()],
            Some(Some(Value::Array(items))) => items.iter().filter_map(read_id).collect(),
            Some(Some(value)) => read_id(value).into_iter().collect(),
            Some(None) => Vec::new(),
        }
    }

    fn effect_row(&self, reader: &Reader<'_>, call: &Call<'_>, model: &str, row: Option<&str>) -> Result<Option<RowId>, DbError> {
        if let Some(name) = row {
            return Ok(call.args.get(name).and_then(read_id));
        }
        let Some(relation) = self.shared.spec(model)?.singleton_relation() else { return Ok(None) };
        let key = self.shared.spec(model)?.selection_key(&relation.name, &Map::new())?;
        reader.scope_members(model, key.as_str())?.into_iter().next().map(|(id, _)| Ok(RowId::new(&id)?)).transpose()
    }

    fn source_value(&self, reader: &Reader<'_>, edits: &mut Edits, call: &Call<'_>, source: &EffectSource) -> Result<Option<Value>, DbError> {
        Ok(match source {
            EffectSource::Literal(value) => Some(value.clone()),
            EffectSource::Arg(name) => call.args.get(name).filter(|value| !value.is_null()).cloned(),
            EffectSource::Owner(field) if field == "id" => Some(Value::String(call.owner.id.to_string())),
            EffectSource::Owner(field) => edits.row_mut(reader, call.owner.model.as_str(), call.owner.id.as_str())?.and_then(|row| row.get(field).filter(|value| !value.is_null()).cloned()),
            EffectSource::Row { model, row, field } => {
                let Some(id) = self.effect_row(reader, call, model, row.as_deref())? else { return Ok(None) };
                if field == "id" {
                    Some(Value::String(id.to_string()))
                } else {
                    edits.row_mut(reader, model, id.as_str())?.and_then(|row| row.get(field).filter(|value| !value.is_null()).cloned())
                }
            }
            EffectSource::Plus(_) | EffectSource::Minus(_) => None,
        })
    }
}

fn shifted(current: &Value, delta: &Value, subtract: bool) -> Option<Value> {
    if let (Some(current), Some(delta)) = (current.as_i64(), delta.as_i64()) {
        return Some(Value::from(if subtract { current.checked_sub(delta)? } else { current.checked_add(delta)? }));
    }
    let (current, delta) = (current.as_f64()?, delta.as_f64()?);
    serde_json::Number::from_f64(if subtract { current - delta } else { current + delta }).map(Value::Number)
}
