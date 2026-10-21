use std::collections::HashSet;

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, EntityOp, OperationStatus};
use crate::identity::RowId;
use crate::operation::Operation;
use crate::store::Reader;

use super::error::DbError;
use super::local_write::{read_outbox, OutboxOperation};
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

fn same_record(paths: &[String], temp: &Map<String, Value>, landed: &Map<String, Value>) -> bool {
    paths.iter().all(|path| matches!((at(temp, path), at(landed, path)), (Some(left), Some(right)) if left == right))
}

impl Db {
    pub(crate) fn follow_correlations(&self, reader: &Reader<'_>, envelope: &mut CommitEnvelope) -> Result<(), DbError> {
        let mut outbox: Option<Vec<OutboxOperation>> = None;
        let mut swapped: HashSet<RowId> = HashSet::new();
        for op in envelope.entity_ops.clone() {
            let EntityOp::Upsert { model, id, row } = op else { continue };
            let spec = self.shared.spec(model.as_str())?;
            if id.is_temp() || !spec.actions.iter().any(|action| !action.correlate.is_empty()) || reader.point(model.as_str(), id.as_str())?.is_some() {
                continue;
            }
            if outbox.is_none() {
                outbox = Some(read_outbox(reader)?);
            }
            let open = outbox.as_deref().unwrap_or_default();
            let mut found: Vec<(i64, RowId)> = Vec::new();
            for action in spec.actions.iter().filter(|action| !action.correlate.is_empty()) {
                let key = Operation::action_key(model.as_str(), &action.name);
                for entry in open.iter().filter(|entry| entry.operation.action_key == key) {
                    let sent = matches!(entry.operation.status, OperationStatus::Pending | OperationStatus::DeliveryUnknown) && entry.operation.transport_started;
                    if !sent { continue; }
                    let candidates: Vec<_> = if action.batch.is_some() { entry.operation.row_ids.clone() } else { entry.operation.temp_id.clone().into_iter().collect() };
                    for temp in candidates.into_iter().filter(|temp| temp.is_temp() && !swapped.contains(temp)) {
                        let Some(stored) = reader.point(model.as_str(), temp.as_str())? else { continue };
                        if same_record(&action.correlate, &stored, &row) {
                            found.push((entry.operation.id.0, temp));
                        }
                    }
                }
            }
            if let [(own, temp)] = found.as_slice() {
                self.swap_temp(reader, envelope, open, model.as_str(), temp, &id, *own)?;
                swapped.insert(temp.clone());
            }
        }
        Ok(())
    }
}
