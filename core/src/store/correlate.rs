use serde_json::{Map, Value};

use crate::identity::RowId;
use crate::schema::keyed::is_blank;

use super::plan::OutboxOperation;
use super::{Reader, StoreError};
use rusqlite::params;

pub(super) struct Correlated {
    pub temp: RowId,
    pub operation: i64,
}

fn candidates(reader: &Reader<'_>, spec: &crate::schema::ModelSpec, model: &str) -> Result<Vec<OutboxOperation>, StoreError> {
    let sql = format!("SELECT seq FROM op WHERE action_key = ?1 AND temp_id IS NOT NULL AND status IN {} ORDER BY seq", crate::operation::OPEN_STATUSES);
    let mut statement = reader.connection().prepare_cached(&sql)?;
    let mut entries = Vec::new();
    for declared in spec.actions.iter().filter(|declared| !declared.correlate.is_empty()) {
        let seqs = statement.query_map(params![format!("{model}:{}", declared.name)], |row| row.get::<_, i64>(0))?.collect::<Result<Vec<_>, _>>()?;
        for seq in seqs {
            entries.extend(OutboxOperation::read(reader, seq)?);
        }
    }
    entries.sort_by_key(|entry| entry.operation.id.0);
    Ok(entries)
}

pub(super) fn correlated_temp(reader: &Reader<'_>, model: &str, incoming: &Map<String, Value>) -> Result<(Option<Correlated>, usize), StoreError> {
    let spec = &reader.models().get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
    let entries = candidates(reader, spec, model)?;
    let mut found = Vec::new();
    for entry in &entries {
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
    Ok((if found.len() == 1 { found.pop() } else { None }, entries.len()))
}
