//! The windows and edges as the owners read them.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::store::EdgeValue;
use crate::schema::FieldKind;
use crate::value::ScalarKind;

use super::db_error::CoreError;
use super::{window_identity, Store};

fn json_fault(error: impl std::fmt::Display) -> CoreError { CoreError::Internal(error.to_string()) }

impl Store {
    /// Fails when the relation of a call does not name a remote window for these params.
    pub(crate) fn check_window(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<(), CoreError> {
        let spec = self.spec(model)?;
        let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| CoreError::Internal(format!("{model} has no relation {relation}")))?;
        let remote = declared.remote.as_ref().ok_or_else(|| CoreError::Internal(format!("{model}.{relation} is no remote relation")))?;
        window_identity(spec, model, relation, remote, params).map(drop)
    }

    /// The saved window of a relation with params, `None` before its first page.
    pub(crate) fn window_saved(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<Option<crate::store::SavedWindow>, CoreError> {
        #[cfg(test)]
        assert!(self.panic_reads.load(std::sync::atomic::Ordering::SeqCst) & super::PANIC_WINDOW_READ == 0, "the window read failed");
        let spec = self.spec(model)?;
        let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| CoreError::Internal(format!("{model} has no relation {relation}")))?;
        let remote = declared.remote.as_ref().ok_or_else(|| CoreError::Internal(format!("{model}.{relation} is no remote relation")))?;
        let (scope, key) = window_identity(spec, model, relation, remote, params)?;
        let owed = match (declared.node, params.get(crate::schema::NODE_PARAM).and_then(Value::as_str)) {
            (Some(crate::schema::NodeRead::One), Some(id)) => self.read(&[model], |reader| super::reread_owed::is_owed(reader.connection(), model, id, relation))?,
            _ => false,
        };
        let (Some(record), Some(freshness)) = (self.window(&key)?, self.window_freshness(&key)?) else {
            return Ok(owed.then(|| crate::store::SavedWindow { landed_at: 0, fresh_until: 0, stale: true, owed, cursor: None, pages: 0, has_rows: false, generation: self.generation().value(), marker: None, members: 0 }));
        };
        let has_rows = self.read(&[model], |reader| reader.scope_last_id(model, scope.key.as_str()))?.is_some();
        let policy = crate::runtime::selected_policy(!has_rows, &remote.stale_time, remote.empty_stale_time.as_deref());
        let lifetime = crate::runtime::parse_duration(policy).map_or(i64::MAX / 4, |window| i64::try_from(window.as_millis()).unwrap_or(i64::MAX / 4));
        let marker = super::windows::read_marker(&self.lock().connection, &key)?;
        Ok(Some(crate::store::SavedWindow { landed_at: freshness.landed_at, fresh_until: freshness.landed_at.saturating_add(lifetime), stale: freshness.stale_since.is_some() || owed, owed, cursor: record.cursor, pages: u32::try_from(record.pages).unwrap_or(u32::MAX), has_rows, generation: u64::try_from(record.generation).unwrap_or(0), marker, members: self.read(&[model], |reader| reader.scope_member_count(model, scope.key.as_str()))? }))
    }

    /// The edge values of the rows of a relation window, by row id.
    pub(crate) fn edge_values(&self, model: &str, relation: &str, params: &Map<String, Value>, ids: &[String]) -> Result<BTreeMap<String, Map<String, Value>>, CoreError> {
        let spec = self.spec(model)?;
        let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| CoreError::UnknownAssociation { model: model.to_string(), name: relation.to_string() })?;
        let scope = spec.selection_key(relation, params)?;
        let wanted: Vec<&str> = ids.iter().map(String::as_str).collect();
        let read = self.read(&[model], |reader| reader.scope_edges(model, scope.as_str(), &wanted))?;
        { let mut inner = self.lock(); inner.counters.edge_read_steps += read.steps; inner.counters.edge_read_scans += read.scans; }
        Ok(read.members.into_iter().map(|(id, stored)| {
            let mut values: Map<String, Value> = declared.edge_fields.iter().map(|field| (field.name.clone(), Value::Null)).collect();
            for (name, value) in stored {
                if let Some(field) = declared.edge_fields.iter().find(|field| field.name == name) {
                    values.insert(name, edge_json(&field.kind, value));
                }
            }
            (id, values)
        }).collect())
    }

    /// Whether the row is in the membership of the scope of a relation with params.
    /// The ids of a `nodes` window that came back as rows in the answer of its request, in the order of `ids`.
    pub(crate) fn nodes_answered(&self, model: &str, key: &str, ids: &[crate::identity::RowId]) -> Result<Vec<crate::identity::RowId>, CoreError> {
        Ok(self.read(&[model], |reader| {
            let mut answered = Vec::new();
            for id in ids {
                if reader.scope_has_member(model, key, id.as_str())? { answered.push(id.clone()); }
            }
            Ok(answered)
        })?)
    }

    pub(crate) fn window_member(&self, model: &str, relation: &str, params: &Map<String, Value>, id: &str) -> Result<bool, CoreError> {
        let scope = self.spec(model)?.selection_key(relation, params).map_err(json_fault)?;
        Ok(self.read(&[model], |reader| reader.scope_has_member(model, scope.as_str(), id))?)
    }
}

fn edge_json(kind: &FieldKind, value: EdgeValue) -> Value {
    match (kind, value) {
        (FieldKind::Scalar { kind: ScalarKind::Bool }, EdgeValue::Whole(whole)) => Value::Bool(whole != 0),
        (_, EdgeValue::Whole(whole)) => Value::from(whole),
        (_, EdgeValue::Fraction(fraction)) => Value::from(fraction),
        (_, EdgeValue::Text(text)) => Value::String(text),
    }
}
