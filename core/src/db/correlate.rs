use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, EntityOp};
use crate::identity::RowId;
use crate::operation::Operation;
use crate::store::Reader;

use super::error::DbError;
use super::local_write::read_outbox;
use super::open::Db;

fn at<'a>(row: &'a Map<String, Value>, path: &str) -> Option<&'a Value> {
    let mut segments = path.split('.');
    let mut value = row.get(segments.next()?)?;
    for segment in segments {
        value = value.as_object()?.get(segment)?;
    }
    let empty = match value {
        Value::Null => true,
        Value::String(text) => text.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(fields) => fields.is_empty(),
        _ => false,
    };
    (!empty).then_some(value)
}

fn correlation_key(paths: &[String], row: &Map<String, Value>) -> Option<String> {
    let values: Option<Vec<Value>> = paths.iter().map(|path| at(row, path).cloned()).collect();
    values.map(|values| Value::Array(values).to_string())
}

impl Db {
    pub(crate) fn follow_correlations(&self, reader: &Reader<'_>, envelope: &mut CommitEnvelope) -> Result<(), DbError> {
        let mut candidates: HashMap<String, HashMap<String, Vec<(i64, RowId)>>> = HashMap::new();
        let mut swapped: HashSet<RowId> = HashSet::new();
        let mut replacements = Vec::new();
        for op in envelope.entity_ops.clone() {
            let EntityOp::Upsert { model, id, row } = op else { continue };
            let spec = self.shared.spec(model.as_str())?;
            if id.is_temp() || !spec.actions.iter().any(|action| !action.correlate.is_empty()) || reader.point(model.as_str(), id.as_str())?.is_some() {
                continue;
            }
            let mut found: Vec<(i64, RowId)> = Vec::new();
            for action in spec.actions.iter().filter(|action| !action.correlate.is_empty()) {
                let key = Operation::action_key(model.as_str(), &action.name);
                let Some(incoming) = correlation_key(&action.correlate, &row) else { continue };
                if !candidates.contains_key(&key) {
                    let mut indexed: HashMap<String, Vec<(i64, RowId)>> = HashMap::new();
                    let sql = if action.batch.is_some() {
                        "SELECT op.seq, op_row.row_id FROM op JOIN op_row USING (seq) WHERE op.action_key = ?1 AND op.status IN ('pending', 'delivery_unknown') AND op.transport_started = 1"
                    } else {
                        "SELECT seq, temp_id FROM op WHERE action_key = ?1 AND status IN ('pending', 'delivery_unknown') AND transport_started = 1 AND temp_id IS NOT NULL"
                    };
                    let mut statement = reader.connection().prepare_cached(sql)?;
                    let rows = statement.query_map([&key], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))?.collect::<Result<Vec<_>, _>>()?;
                    for (seq, temp) in rows {
                        let Ok(temp) = RowId::new(&temp) else { continue };
                        if !temp.is_temp() { continue; }
                        let Some(stored) = reader.point(model.as_str(), temp.as_str())? else { continue };
                        if let Some(value) = correlation_key(&action.correlate, &stored) {
                            indexed.entry(value).or_default().push((seq, temp));
                        }
                    }
                    candidates.insert(key.clone(), indexed);
                }
                found.extend(candidates.get(&key).and_then(|indexed| indexed.get(&incoming)).into_iter().flatten().filter(|(_, temp)| !swapped.contains(temp)).cloned());
            }
            if let [(own, temp)] = found.as_slice() {
                replacements.push((model, temp.clone(), id, *own));
                swapped.insert(temp.clone());
            }
        }
        if replacements.is_empty() { return Ok(()) }
        let outbox = read_outbox(reader)?;
        for (model, temp, id, own) in replacements {
            self.swap_temp(reader, envelope, &outbox, model.as_str(), &temp, &id, own)?;
        }
        Ok(())
    }
}
