use serde_json::{Map, Value};

use crate::identity::{ModelKey, RowId};
use crate::store::{BatchPlan, EntityOp};
use crate::schema::keyed::is_blank;

use super::plan::OutboxOperation;
use super::{Reader, StoreError};
use rusqlite::params;

pub(super) struct Correlated {
    pub temp: RowId,
    pub operation: i64,
}

fn candidates(reader: &Reader<'_>, spec: &crate::schema::ModelSpec, model: &str, incoming: &Map<String, Value>) -> Result<Vec<OutboxOperation>, StoreError> {
    let sql = format!(include_str!("sql/correlate/candidates.sql"), crate::store::operation::OPEN_STATUSES);
    let mut statement = reader.connection().prepare_cached(&sql)?;
    let mut entries = Vec::new();
    for declared in spec.actions.iter().filter(|declared| !declared.correlate.is_empty() && declared.correlate.iter().all(|path| crate::schema::read_path(incoming, path).is_some_and(|value| !is_blank(value)))) {
        let seqs = statement.query_map(params![format!("{model}:{}", declared.name)], |row| row.get::<_, i64>(0))?.collect::<Result<Vec<_>, _>>()?;
        for seq in seqs {
            entries.extend(OutboxOperation::read(reader, seq)?);
        }
    }
    entries.sort_by_key(|entry| entry.operation.id.0);
    Ok(entries)
}

fn correlated_temp(writer: &mut super::row_write::RowWrite<'_>, model: &str, incoming: &Map<String, Value>) -> Result<(Option<Correlated>, usize), StoreError> {
    let connection = writer.connection;
    let models = writer.models;
    let reader = Reader::new(connection, models);
    let spec = &models.get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
    let entries = candidates(&reader, spec, model, incoming)?;
    let ids: Vec<String> = entries.iter().filter_map(|entry| entry.operation.temp_id.as_ref().map(ToString::to_string)).collect();
    writer.prefetch(model, &ids, None)?;
    let mut found = Vec::new();
    for entry in &entries {
        let Some(temp) = entry.operation.temp_id.as_ref() else { continue };
        let Some(action) = entry.operation.action_key.strip_prefix(model).and_then(|rest| rest.strip_prefix(':')) else { continue };
        let Some(declared) = spec.actions.iter().find(|declared| declared.name == action && !declared.correlate.is_empty()) else { continue };
        let Some(sent) = writer.stored(model, temp.as_str())?.map(|stored| stored.row) else { continue };
        let same = declared.correlate.iter().all(|path| match (crate::schema::read_path(&sent, path), crate::schema::read_path(incoming, path)) {
            (Some(sent), Some(incoming)) => !is_blank(sent) && sent == incoming,
            _ => false,
        });
        if same {
            found.push(Correlated { temp: temp.clone(), operation: entry.operation.id.0 });
        }
    }
    Ok((if found.len() == 1 { found.pop() } else { None }, entries.len()))
}

/// Each server row of the envelope that carries the key of an open operation replaces the single temporary row of that key: new rows and rows that got their key later, from any source (P-DATA-23).
pub(super) fn replace_temp_rows(writer: &mut super::row_write::RowWrite<'_>, envelope: &mut BatchPlan) -> Result<(), StoreError> {
    let landed: Vec<(ModelKey, RowId, Map<String, Value>)> = envelope.entity_ops.iter().filter_map(|operation| match operation {
        EntityOp::Upsert { model, id, row } => Some((model.clone(), id.clone(), row.clone())),
        _ => None,
    }).collect();
    let mut replaced: Vec<RowId> = Vec::new();
    for (model, id, row) in landed {
        let (found, looked_at) = correlated_temp(writer, model.as_str(), &row)?;
        writer.candidates += looked_at;
        let Some(found) = found.filter(|found| found.temp != id && !replaced.contains(&found.temp)) else { continue };
        let held = writer.stored(model.as_str(), found.temp.as_str())?.map(|stored| stored.row);
        let outbox = crate::store::plan::read_outbox(&Reader::new(writer.connection, writer.models))?;
        super::swap::plan(writer, envelope, &outbox, super::swap::TempReplacement { model: model.as_str(), temp: &found.temp, real: &id }, found.operation, held)?;
        replaced.push(found.temp);
    }
    Ok(())
}
