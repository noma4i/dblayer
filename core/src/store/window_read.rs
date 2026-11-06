//! The windows and edges as the owners read them.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::commit::EdgeValue;
use crate::schema::FieldKind;
use crate::value::{read_id, ScalarKind};

use super::db_error::DbError;
use super::{window_identity, Store};

fn json_fault(error: impl std::fmt::Display) -> DbError { DbError::Internal(error.to_string()) }

impl Store {
    /// The stored record of the window of a relation, `None` before its first page.
    pub(crate) fn window_record(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<Option<crate::store::WindowRecord>, DbError> {
        let spec = self.spec(model)?;
        let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| DbError::Internal(format!("{model} has no relation {relation}")))?;
        let remote = declared.remote.as_ref().ok_or_else(|| DbError::Internal(format!("{model}.{relation} is no remote relation")))?;
        let (_, key) = window_identity(spec, model, relation, remote, params)?;
        Ok(self.window(&key)?)
    }

    /// Whether the window landed within the stale time of its relation and nothing marked it stale since (`now_ms` is the clock of the database).
    pub(crate) fn window_is_fresh(&self, model: &str, relation: &str, params: &Map<String, Value>, now_ms: i64) -> Result<bool, DbError> {
        Ok(self.window_age(model, relation, params, now_ms)?.unwrap_or(false))
    }

    /// Whether a window that landed once outlived its stale time or was marked stale; a window that never landed has no answer.
    pub(crate) fn window_expired(&self, model: &str, relation: &str, params: &Map<String, Value>, now_ms: i64) -> Result<bool, DbError> {
        Ok(self.window_age(model, relation, params, now_ms)? == Some(false))
    }

    fn window_age(&self, model: &str, relation: &str, params: &Map<String, Value>, now_ms: i64) -> Result<Option<bool>, DbError> {
        let spec = self.spec(model)?;
        let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| DbError::Internal(format!("{model} has no relation {relation}")))?;
        let remote = declared.remote.as_ref().ok_or_else(|| DbError::Internal(format!("{model}.{relation} is no remote relation")))?;
        let (scope, key) = window_identity(spec, model, relation, remote, params)?;
        let Some(freshness) = self.window_freshness(&key)? else { return Ok(None) };
        let is_empty = self.read(&[model], |reader| reader.scope_last_id(model, scope.key.as_str()))?.is_none();
        let policy = crate::pacing::selected_policy(is_empty, &remote.stale_time, remote.empty_stale_time.as_deref());
        let window = crate::pacing::parse_duration(policy).ok_or_else(|| DbError::Internal(format!("unknown freshness {policy}")))?;
        Ok(Some(freshness.is_fresh(now_ms, i64::try_from(window.as_millis()).unwrap_or(i64::MAX))))
    }

    /// The edge values of the rows of a relation window, by row id.
    pub(crate) fn edge_values(&self, model: &str, relation: &str, params: &Map<String, Value>, ids: &[String]) -> Result<BTreeMap<String, Map<String, Value>>, DbError> {
        let spec = self.spec(model)?;
        let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| DbError::UnknownAssociation { model: model.to_string(), name: relation.to_string() })?;
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
    pub(crate) fn window_member(&self, model: &str, relation: &str, params: &Map<String, Value>, id: &str) -> Result<bool, DbError> {
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
