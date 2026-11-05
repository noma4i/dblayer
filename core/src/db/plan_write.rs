//! The plan of a call of the platform: `create`, `update`, `act` and `destroy` read the base once and give the owner Outbox (or LocalWrites) the effects the call has before the server answers.
//! Nothing is written here. The owner Store writes the effects and reads the prior state for the rollback in the same transaction.

use serde_json::{json, Map, Value};

use crate::commit::{EntityOp, ScopeOp};
use crate::identity::{ModelKey, RowId, RowRef};
use crate::owners::store::{EffectMember, EffectRow, EffectTombstone, EffectTouch, IntentEffects};
use crate::schema::ModelSpec;
use crate::store::Reader;
use crate::syncstate::read_sync;
use crate::value::{read_id, same_fields};

use super::effects::{Call, Edits};
use super::error::DbError;
use super::local_write::{local_scope_ops, plan_scope_effect, reject_invalid_values, reject_unknown_fields};
use super::open::Db;
use super::prior::PriorRow;

type Row = Map<String, Value>;

/// What a call of the platform does: the row it is about, the input the request is built from, and the effects.
#[derive(Debug, Clone)]
pub(crate) struct Planned {
    pub model: String,
    pub row_id: RowId,
    /// The row the receipt shows: the row of the call, or the copy an action made.
    pub shown: RowId,
    /// The data of the row and the arguments of the call: the variables of the mutation are built from it.
    pub input: Value,
    pub effects: IntentEffects,
    /// The call changes nothing (an `update` with the values the row has).
    pub noop: bool,
}

/// The fields of `after` the row `before` does not have yet; a row that is not in the base takes all of them.
fn changed_fields(before: Option<&Row>, after: &Row) -> Row {
    let Some(before) = before else { return after.clone() };
    after.iter().filter(|(name, value)| before.get(*name) != Some(*value)).map(|(name, value)| (name.clone(), (*value).clone())).chain(after.get("id").map(|id| ("id".to_string(), id.clone()))).collect()
}

fn row_effect(model: &str, id: &str, before: Option<&Row>, after: &Row) -> EffectRow {
    EffectRow { model: model.to_string(), row_id: id.to_string(), values: Value::Object(changed_fields(before, after)).to_string() }
}

fn member_effects(effects: &mut IntentEffects, scope_ops: Vec<ScopeOp>) {
    for operation in scope_ops {
        match operation {
            ScopeOp::Append { scope, row, order_key } | ScopeOp::Move { scope, row, order_key } => effects.members.push(EffectMember { model: scope.model.to_string(), list: scope.key.to_string(), row_id: row.to_string(), order_key, leave: false }),
            ScopeOp::Detach { scope, row } => effects.members.push(EffectMember { model: scope.model.to_string(), list: scope.key.to_string(), row_id: row.to_string(), order_key: String::new(), leave: true }),
        }
    }
}

fn touch_effects(effects: &mut IntentEffects, touches: Vec<EntityOp>) {
    for touch in touches {
        if let EntityOp::Touch { model, id } = touch { effects.touches.push(EffectTouch { model: model.to_string(), row_id: id.to_string() }); }
    }
}

impl Db {
    /// A new row. The id is temporary for a server model, a final one for a local model.
    pub(crate) fn plan_create(&self, model: &str, mut values: Row, args: Row) -> Result<Planned, DbError> {
        self.fill_local_sources(model, &mut values)?;
        let spec = self.shared.spec(model)?;
        reject_unknown_fields(spec, model, &values)?;
        let id = match values.get("id").and_then(read_id) {
            Some(id) => id,
            None if spec.is_local() => RowId::generate(),
            None => RowId::generate_temp(None),
        };
        values.insert("id".to_string(), Value::String(id.to_string()));
        reject_invalid_values(spec, model, &values)?;
        self.shared.store.decode_values(model, &mut values, |source, key| source.get(key))?;
        let now_ms = self.shared.store.lock().now_ms();
        let data = spec.normalize_local(&values, now_ms)?;
        let own = RowRef::new(ModelKey::new(model), id.clone());
        let input = Value::Object(data.iter().chain(&args).map(|(name, value)| (name.clone(), value.clone())).collect());
        let effects = self.shared.read(&[model], |reader| {
            if reader.point(model, id.as_str())?.is_some() || !read_sync(reader.connection(), model, &[id.to_string()])?.is_empty() {
                return Err(DbError::AlreadyExists { model: model.to_string(), id: id.to_string() });
            }
            let mut effects = IntentEffects { rows: vec![row_effect(model, id.as_str(), None, &data)], ..Default::default() };
            member_effects(&mut effects, local_scope_ops(reader, spec, &id, &data)?);
            touch_effects(&mut effects, self.touch_ops(reader, spec, &[&data], &own)?);
            Ok(effects)
        })?;
        Ok(Planned { model: model.to_string(), shown: id.clone(), row_id: id, input, effects, noop: false })
    }

    /// New values of a row. A call with the values the row has and no arguments changes nothing.
    pub(crate) fn plan_update(&self, model: &str, id: &RowId, patch: Row, args: Row) -> Result<Planned, DbError> {
        let spec = self.shared.spec(model)?;
        reject_unknown_fields(spec, model, &patch)?;
        reject_invalid_values(spec, model, &patch)?;
        let mut patch = spec.patch(&patch);
        self.shared.store.decode_values(model, &mut patch, |source, key| source.get(key))?;
        patch.remove("id");
        let own = RowRef::new(ModelKey::new(model), id.clone());
        let mut input = patch.clone();
        input.insert("id".to_string(), Value::String(id.to_string()));
        let sends_args = !args.is_empty();
        input.extend(args);
        let effects = self.shared.read(&[model], |reader| {
            let current = reader.point(model, id.as_str())?.ok_or_else(|| DbError::NotFound { model: model.to_string(), id: id.to_string() })?;
            let mut merged = current.clone();
            merged.extend(patch.clone());
            if same_fields(&merged, &current) && !sends_args {
                return Ok(None);
            }
            let mut effects = IntentEffects { rows: vec![row_effect(model, id.as_str(), Some(&current), &merged)], ..Default::default() };
            touch_effects(&mut effects, self.touch_ops(reader, spec, &[&merged, &current], &own)?);
            Ok(Some(effects))
        })?;
        Ok(match effects {
            Some(effects) => Planned { model: model.to_string(), row_id: id.clone(), shown: id.clone(), input: Value::Object(input), effects, noop: false },
            None => Planned { model: model.to_string(), row_id: id.clone(), shown: id.clone(), input: Value::Object(input), effects: IntentEffects::default(), noop: true },
        })
    }

    /// A named action: the edits of the receiver and of the rows its arguments name, the closed effects of the declaration.
    pub(crate) fn plan_act(&self, model: &str, action: &str, id: &RowId, mut patch: Row, args: Row) -> Result<Planned, DbError> {
        let spec = self.shared.spec(model)?;
        crate::schema::keyed::resolve_maps(&mut patch, &args);
        let keyed = crate::schema::keyed::take(&mut patch);
        let flagged = crate::schema::keyed::take_flags(&mut patch);
        reject_unknown_fields(spec, model, &patch)?;
        reject_invalid_values(spec, model, &patch)?;
        let mut patch = spec.patch(&patch);
        self.shared.store.decode_values(model, &mut patch, |source, key| source.get(key))?;
        let own = RowRef::new(ModelKey::new(model), id.clone());
        let mut input = args.clone();
        input.insert("id".to_string(), Value::String(id.to_string()));
        let declared = spec.actions.iter().find(|declared| declared.name == action).cloned();
        let now_ms = self.shared.store.lock().now_ms();
        let mut shown = None;
        let effects = self.shared.read(&[model], |reader| {
            let current = reader.point(model, id.as_str())?.ok_or_else(|| DbError::NotFound { model: model.to_string(), id: id.to_string() })?;
            let mut merged = current.clone();
            merged.extend(patch.clone());
            for (field, op) in &keyed {
                let Some(declared) = spec.field(field) else { continue };
                let target_row = |model: &str, id: &str| Ok::<_, DbError>(reader.point(model, id)?);
                if let Some(next) = crate::schema::keyed::apply(declared, current.get(field), op, &args, target_row)? {
                    merged.insert(field.clone(), next);
                }
            }
            let mut edits = Edits::of_owner(model, id, current, merged);
            self.apply_flagged(reader, &mut edits, &Call { owner: &own, args: &args }, &flagged)?;
            let mut effects = IntentEffects::default();
            let removes = declared.as_ref().is_some_and(|declared| declared.removes_receiver);
            for ((edited_model, edited_id), (before, after)) in edits.into_rows() {
                let is_receiver = edited_model == model && edited_id == id.as_str();
                if (!is_receiver && before == after) || (is_receiver && removes) {
                    continue;
                }
                effects.rows.push(row_effect(&edited_model, &edited_id, Some(&before), &after));
                touch_effects(&mut effects, self.touch_ops(reader, self.shared.spec(&edited_model)?, &[&after, &before], &RowRef::new(ModelKey::new(&edited_model), RowId::new(&edited_id)?))?);
            }
            if let Some(declared) = &declared {
                self.plan_closed_effect(reader, spec, declared, (&own, &args), now_ms, &mut effects, &mut shown)?;
            }
            Ok(effects)
        })?;
        Ok(Planned { model: model.to_string(), row_id: id.clone(), shown: shown.unwrap_or_else(|| id.clone()), input: Value::Object(input), effects, noop: false })
    }

    /// The row and every row that depends on it.
    pub(crate) fn plan_destroy(&self, model: &str, id: &RowId) -> Result<Planned, DbError> {
        let own = RowRef::new(ModelKey::new(model), id.clone());
        let effects = self.shared.read(&[model], |reader| {
            if reader.point(model, id.as_str())?.is_none() {
                return Err(DbError::NotFound { model: model.to_string(), id: id.to_string() });
            }
            let tombstones = self.destroy_targets(reader, &own)?.into_iter().map(|target| EffectTombstone { model: target.model.to_string(), row_id: target.id.to_string() }).collect();
            Ok(IntentEffects { tombstones, ..Default::default() })
        })?;
        Ok(Planned { model: model.to_string(), row_id: id.clone(), shown: id.clone(), input: json!({ "id": id.as_str() }), effects, noop: false })
    }

    fn plan_closed_effect(&self, reader: &Reader<'_>, spec: &ModelSpec, declared: &crate::schema::ActionDeclaration, (own, args): (&RowRef, &Map<String, Value>), now_ms: i64, effects: &mut IntentEffects, shown: &mut Option<RowId>) -> Result<(), DbError> {
        if declared.removes_receiver {
            for target in self.destroy_targets(reader, own)? {
                effects.tombstones.push(EffectTombstone { model: target.model.to_string(), row_id: target.id.to_string() });
            }
        }
        if let Some(copy) = &declared.copies_receiver_to {
            let target = args.get(&copy.arg).cloned().ok_or_else(|| DbError::Internal(format!("the argument {} of the copy is missing", copy.arg)))?;
            let mut values = reader.point(own.model.as_str(), own.id.as_str())?.ok_or_else(|| DbError::NotFound { model: own.model.to_string(), id: own.id.to_string() })?;
            let copy_id = RowId::generate_temp(None);
            values.insert("id".to_string(), Value::String(copy_id.to_string()));
            values.insert(copy.field.clone(), target);
            let data = spec.normalize_local(&values, now_ms)?;
            member_effects(effects, local_scope_ops(reader, spec, &copy_id, &data)?);
            effects.rows.push(row_effect(own.model.as_str(), copy_id.as_str(), None, &data));
            *shown = Some(copy_id);
        }
        let (mut scope_ops, mut joins) = (Vec::new(), Vec::new());
        for effect in &declared.scope_effects {
            plan_scope_effect(reader, self.shared.spec(&effect.model)?, effect, (own, args), &mut joins, &mut scope_ops)?;
        }
        let order_of = |model: &str, list: &str, row: &str| scope_ops.iter().find_map(|operation| match operation {
            ScopeOp::Append { scope, row: appended, order_key } if scope.model.as_str() == model && scope.key.as_str() == list && appended.as_str() == row => Some(order_key.clone()),
            _ => None,
        }).unwrap_or_default();
        for prior in joins {
            match prior {
                PriorRow::Join { model, id, scope_key, .. } => { let order_key = order_of(&model, &scope_key, &id); effects.members.push(EffectMember { model, list: scope_key, row_id: id, order_key, leave: false }) }
                PriorRow::Leave { model, id, scope_key, .. } => effects.members.push(EffectMember { model, list: scope_key, row_id: id, order_key: String::new(), leave: true }),
                _ => {}
            }
        }
        Ok(())
    }
}

/// The request of an action with a mutation: the name of the mutation, its variables, the rule on refusal and whether the call waits for the network.
#[derive(Debug, Clone)]
pub(crate) struct MutationCall {
    pub mutation: String,
    pub variables: Value,
    pub on_failure: crate::schema::OnFailure,
    pub durable: bool,
}

/// What `set_local` writes: the whole row when it is in the base, the value that waits for the row when it is not.
#[derive(Debug, Clone)]
pub(crate) enum LocalPlan {
    Row { row_id: RowId, values: Row },
    Pending { row_id: RowId, path: String, value: Value },
}

impl Db {
    /// The action of `model` that has a mutation, or `None` for a local model and an action without one.
    pub(crate) fn mutation_call(&self, model: &str, action: &str, planned: &Planned, client_mutation_id: &str) -> Result<Option<MutationCall>, DbError> {
        let spec = self.shared.spec(model)?;
        if spec.is_local() { return Ok(None); }
        let Some(declaration) = spec.actions.iter().find(|declared| declared.name == action) else { return Ok(None) };
        let Some(mutation) = declaration.mutation.clone() else { return Ok(None) };
        let is_id = |field: &str| spec.field(field).is_some_and(|field| matches!(field.kind, crate::schema::FieldKind::Scalar { kind: crate::value::ScalarKind::Id }));
        let temp = planned.row_id.is_temp().then_some(&planned.row_id);
        let variables = crate::write::variables::mutation_variables(declaration, &planned.input, temp, &is_id);
        let variables = crate::write::variables::idempotent_variables(declaration, &planned.input, variables, client_mutation_id);
        Ok(Some(MutationCall { mutation, variables, on_failure: declaration.on_failure, durable: declaration.durable || declaration.upload }))
    }

    /// The write of a local field: the value goes on the row, or waits for the row when it is not in the base.
    pub(crate) fn plan_set_local(&self, model: &str, id: &RowId, path: &str, value: Value) -> Result<LocalPlan, DbError> {
        let spec = self.shared.spec(model)?;
        let slot = spec.local_slot(path).ok_or_else(|| DbError::NotLocalField { model: model.to_string(), path: path.to_string() })?;
        let value = match slot.key {
            None if !value.is_null() => match slot.field.fault(&value, path, true, &mut Vec::new()) {
                Some(crate::schema::ValueFault::Unknown(field)) => return Err(DbError::UnknownField { model: model.to_string(), field }),
                Some(crate::schema::ValueFault::Invalid(_)) => return Err(DbError::LocalValue { model: model.to_string(), path: path.to_string() }),
                None => slot.field.decode(&value).ok_or_else(|| DbError::LocalValue { model: model.to_string(), path: path.to_string() })?,
            },
            _ => value,
        };
        let row = self.shared.read(&[model], |reader| Ok(reader.point(model, id.as_str())?))?;
        Ok(match row {
            Some(mut row) => {
                slot.write(&mut row, value);
                LocalPlan::Row { row_id: id.clone(), values: row }
            }
            None => LocalPlan::Pending { row_id: id.clone(), path: path.to_string(), value },
        })
    }
}
