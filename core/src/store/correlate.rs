use serde_json::{Map, Value};

use crate::identity::RowId;
use crate::schema::keyed::is_blank;

use super::plan::OutboxOperation;
use super::{Reader, StoreError};

pub(super) struct Correlated {
    pub temp: RowId,
    pub operation: i64,
}

pub(super) fn correlated_temp(reader: &Reader<'_>, outbox: &[OutboxOperation], model: &str, incoming: &Map<String, Value>) -> Result<Option<Correlated>, StoreError> {
    let spec = &reader.models().get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
    let mut found = Vec::new();
    for entry in outbox {
        let Some(temp) = entry.operation.temp_id.as_ref() else { continue };
        let Some(action) = entry.operation.action_key.strip_prefix(model).and_then(|rest| rest.strip_prefix(':')) else { continue };
        let Some(declared) = spec.actions.iter().find(|declared| declared.name == action && !declared.correlate.is_empty()) else { continue };
        let Some(sent) = reader.point(model, temp.as_str())? else { continue };
        let same = declared.correlate.iter().all(|path| match (crate::schema::read_path(&sent, path), crate::schema::read_path(incoming, path)) {
            (Some(sent), Some(incoming)) => !is_blank(sent) && sent == incoming,
            _ => false,
        });
        if same {
            found.push(Correlated { temp: temp.clone(), operation: entry.operation.id.0 });
        }
    }
    Ok(match found.len() {
        1 => found.pop(),
        _ => None,
    })
}
