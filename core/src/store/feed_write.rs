use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde_json::{Map, Value};

use crate::store::{BatchPlan, CommitResult, DestroyReason, EntityOp};
use crate::identity::{ModelKey, RowId, RowRef};
use crate::value::read_id;

use super::merge::{unconfirmed_fields, Admission, RowKind};
use super::row_write::RowWrite;
use super::{Reader, StoreError};
use super::window_write::fault;

type EventRows = BTreeMap<(String, String), Option<Map<String, Value>>>;
pub(super) type OverlayWrites = Vec<(String, String, Map<String, Value>)>;


fn landing(writer: &mut RowWrite<'_>, model: &str, payload: &Value, overlay: &mut OverlayWrites) -> Result<Option<CommitResult>, StoreError> {
    let op = payload.get("op").and_then(Value::as_str).ok_or_else(|| StoreError::Constraint("event has no operation".into()))?;
    if op == "reread" { return Ok(None); }
    if op == "invalid" { return Err(StoreError::Constraint(payload.get("reason").and_then(Value::as_str).unwrap_or("invalid event").to_string())); }
    let id = payload.get("id").and_then(Value::as_str).ok_or_else(|| StoreError::Constraint("event has no id".into()))?;
    let id = RowId::new(id).map_err(fault)?;
    if op == "ephemeral" {
        let values = payload.get("values").and_then(Value::as_object).cloned().unwrap_or_default();
        overlay.push((model.to_string(), id.to_string(), values));
        return Ok(None);
    }
    let connection = writer.connection;
    let models = writer.models;
    let reader = Reader::new(connection, models);
    let mut envelope = BatchPlan::from_source(crate::schema::RowSource::Event);
    if op == "delete" {
        let owner = RowRef::new(ModelKey::new(model), id.clone());
        let mut children = Vec::new();
        super::follow_cascade::descendants(writer.models, &reader, &owner, &mut HashSet::from([owner.clone()]), &mut children)?;
        envelope.entity_ops = crate::owners::write::plan_cascade::cascade_destroys(&owner, &children, true).into_iter().map(|row| EntityOp::Destroy { model: row.model, id: row.id, reason: DestroyReason::Server }).collect();
        let keys: BTreeSet<(String, String)> = envelope.entity_ops.iter().map(|op| (op.row_ref().model.to_string(), op.row_ref().id.to_string())).collect();
        for seq in crate::store::operation::row_operation_sequences(writer.connection, &keys)? {
            envelope.operation_transitions.push(crate::store::OperationTransition::Remove { seq, expected: None });
        }
    } else if op == "upsert" {
        let patch = payload.get("row").and_then(Value::as_object).ok_or_else(|| StoreError::Constraint("event has no row".into()))?;
        let Some(row) = writer.admit(model, patch, RowKind::Server, Admission { base: Some(writer.revision), closing: &[] })? else { return Ok(None) };
        for related in payload.get("related").and_then(Value::as_array).into_iter().flatten() {
            let Some(related_model) = related.get("model").and_then(Value::as_str) else { continue };
            let Some(patch) = related.get("row").and_then(Value::as_object) else { continue };
            if let Some(related_id) = patch.get("id").and_then(read_id) {
                if reader.tombstone_seq(related_model, related_id.as_str())?.is_some() && writer.stored(related_model, related_id.as_str())?.is_none() { continue; }
            }
            let Some(row) = writer.admit(related_model, patch, RowKind::Server, Admission { base: Some(writer.revision), closing: &[] })? else { continue };
            if let Some(id) = row.get("id").and_then(read_id) { envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(related_model), id, row }); }
        }
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id: id.clone(), row });
        super::correlate::replace_temp_rows(writer, &mut envelope)?;
        if let Some(values) = payload.get("set").and_then(Value::as_object).filter(|values| !values.is_empty()) { overlay.push((model.to_string(), id.to_string(), values.clone())); }
    } else {
        return Err(StoreError::Constraint(format!("unknown event operation: {op}")));
    }
    let hidden = writer.feed_hidden.as_ref().cloned().unwrap_or_default();
    let result = super::apply::apply_envelope_at(writer.connection, writer.models, &envelope, (writer.now, writer.revision, writer.generation), Some(writer.originals), Some((&mut writer.current, &hidden, writer.event.as_mut().ok_or_else(|| StoreError::Poisoned("a feed event lands without its undo journal".into()))?)))?;
    if !envelope.operation_transitions.is_empty() {
        let hidden = super::apply::hidden_rows(writer.connection, &[])?;
        let reader = Reader::new(writer.connection, writer.models);
        let mut ids = BTreeMap::<String, Vec<String>>::new();
        for (model, id) in writer.current.keys() { ids.entry(model.clone()).or_default().push(id.clone()); }
        let fields = ids.into_iter().map(|(model, ids)| Ok(((model.clone(), Vec::new()), unconfirmed_fields(&reader, &[], &model, &ids)?))).collect::<Result<Vec<_>, StoreError>>()?;
        writer.replace_feed_reads(hidden, fields);
    }
    if op == "delete" { writer.feed_tombstone(model, id.as_str()); }
    Ok(Some(result))
}

/// The events of a batch land in one transaction, each in its own savepoint: a data error of one event (P-DATA-99) undoes only that event and names it in the second list.
pub(super) fn land(writer: &mut RowWrite<'_>, events: &[(String, String)], overlay: &mut OverlayWrites, results: &mut Vec<CommitResult>) -> Result<(Vec<String>, Vec<usize>), StoreError> {
    let parsed = events.iter().map(|(model, text)| Ok((model, serde_json::from_str::<Value>(text)?))).collect::<Result<Vec<_>, StoreError>>()?;
    let mut wanted = BTreeMap::<String, Vec<String>>::new();
    for (model, payload) in &parsed {
        if let Some(id) = payload.get("id").and_then(Value::as_str) { wanted.entry((*model).clone()).or_default().push(id.to_string()); }
        for related in payload.get("related").and_then(Value::as_array).into_iter().flatten() {
            if let (Some(model), Some(id)) = (related.get("model").and_then(Value::as_str), related.get("row").and_then(|row| row.get("id")).and_then(Value::as_str)) {
                wanted.entry(model.to_string()).or_default().push(id.to_string());
            }
        }
    }
    let mut before = EventRows::new();
    for (model, ids) in wanted {
        writer.prefetch(&model, &ids, &[])?;
        for id in ids {
            let key = (model.clone(), id.clone());
            let row = writer.current.get(&key).and_then(|row| row.as_ref());
            before.entry(key).or_insert_with(|| row.map(|row| row.row.clone()));
            if let Ok(row_id) = RowId::new(&id) { writer.originals.entry(RowRef::new(ModelKey::new(&model), row_id)).or_insert_with(|| row.map(|row| row.row.clone())); }
        }
    }
    writer.feed_hidden = Some(super::apply::hidden_rows(writer.connection, &[])?);
    let mut refused = Vec::new();
    for (at, (model, payload)) in parsed.iter().enumerate() {
        writer.connection.execute_batch("SAVEPOINT feed_event")?;
        writer.begin_event(overlay.len());
        match landing(writer, model, payload, overlay) {
            Ok(result) => {
                writer.connection.execute_batch("RELEASE feed_event")?;
                results.extend(result);
            }
            Err(error) if matches!(error.reject_reason(), crate::owners::contract::RejectReason::Invalid(_)) => {
                writer.connection.execute_batch("ROLLBACK TO feed_event; RELEASE feed_event")?;
                writer.refuse_event(overlay);
                refused.push(at);
            }
            Err(error) => return Err(error),
        }
    }
    writer.event = None;
    let mut observed = Vec::with_capacity(parsed.len());
    for (model, mut payload) in parsed {
        if let Some(object) = payload.as_object_mut() {
            if object.get("observe") == Some(&Value::Bool(true)) {
                if let Some(id) = object.get("id").and_then(Value::as_str).map(str::to_string) {
                    object.insert("before".into(), before.get(&(model.clone(), id.clone())).cloned().flatten().map_or(Value::Null, Value::Object));
                    object.insert("after".into(), writer.current.get(&(model.clone(), id.clone())).and_then(|row| row.as_ref()).map_or(Value::Null, |row| Value::Object(row.row.clone())));
                }
            }
        }
        observed.push(payload.to_string());
    }
    Ok((observed, refused))
}
