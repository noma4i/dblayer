//! The landing of the events of the Feed: one commit for the events of a batch. The codec of the Feed (`owners/read/feed_codec.rs`) reads a frame into a payload
//! with the field `op`: `upsert`, `delete`, `ephemeral`, `reread`, `invalid`. This code turns the payloads into rows, tombstones and joins.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{ModelKey, RowId, RowRef};
use crate::lifecycle::authority::Authority;
use crate::schema::{PageDirection, RemoteShape};
use crate::store::{window_key, Reader};
use crate::value::{fractional_between, read_id};

use super::error::DbError;
use super::local_write::plan_fault;
use super::merge::Admission;
use super::open::Db;

struct Upsert {
    model: String,
    id: RowId,
    related: Vec<(String, String)>,
    joins: Vec<(String, Map<String, Value>)>,
}

#[derive(Default)]
struct Parsed {
    patches: BTreeMap<(String, String), Map<String, Value>>,
    upserts: Vec<Upsert>,
    deletes: Vec<(String, RowId)>,
    ephemeral: Vec<(String, RowId, Map<String, Value>)>,
}

fn object(value: Option<&Value>) -> Map<String, Value> { value.and_then(Value::as_object).cloned().unwrap_or_default() }

fn parse(events: &[(String, String)]) -> Result<Parsed, DbError> {
    let mut parsed = Parsed::default();
    for (model, text) in events {
        let payload: Value = serde_json::from_str(text).map_err(|error| DbError::Internal(error.to_string()))?;
        let id = payload.get("id").and_then(Value::as_str).map(RowId::new).transpose().map_err(|error| DbError::Internal(error.to_string()))?;
        match (payload.get("op").and_then(Value::as_str), id) {
            (Some("upsert"), Some(id)) => {
                let mut patch = |model: &str, row: Map<String, Value>| {
                    if let Some(row_id) = row.get("id").and_then(read_id) {
                        parsed.patches.entry((model.to_string(), row_id.to_string())).or_default().extend(row);
                    }
                };
                patch(model, object(payload.get("row")));
                let mut related = Vec::new();
                for entry in payload.get("related").and_then(Value::as_array).into_iter().flatten() {
                    let Some(related_model) = entry.get("model").and_then(Value::as_str) else { continue };
                    patch(related_model, object(entry.get("row")));
                    if let Some(row_id) = entry.get("row").and_then(|row| row.get("id")).and_then(read_id) {
                        related.push((related_model.to_string(), row_id.to_string()));
                    }
                }
                let joins = payload.get("joins").and_then(Value::as_array).into_iter().flatten().filter_map(|join| Some((join.get("relation")?.as_str()?.to_string(), object(join.get("params"))))).collect();
                if let Some(set) = payload.get("set").and_then(Value::as_object).filter(|set| !set.is_empty()) {
                    parsed.ephemeral.push((model.clone(), id.clone(), set.clone()));
                }
                parsed.upserts.push(Upsert { model: model.clone(), id, related, joins });
            }
            (Some("delete"), Some(id)) => parsed.deletes.push((model.clone(), id)),
            (Some("ephemeral"), Some(id)) => parsed.ephemeral.push((model.clone(), id, object(payload.get("values")))),
            (Some("reread"), _) => {}
            (Some("invalid"), _) => return Err(DbError::Internal(payload.get("reason").and_then(Value::as_str).unwrap_or("invalid event").to_string())),
            _ => return Err(DbError::Internal(format!("an event of {model} has no operation"))),
        }
    }
    Ok(parsed)
}

impl Db {
    /// Lands the events of one batch in one commit. A failing event rejects the batch.
    pub(crate) fn land_events(&self, events: &[(String, String)]) -> Result<(), DbError> {
        let parsed = parse(events)?;
        let generation = self.generation();
        self.commit_following(&Authority::Owner, generation, |reader| {
            let mut operations: BTreeMap<(String, String), EntityOp> = BTreeMap::new();
            let mut touches: Vec<EntityOp> = Vec::new();
            let mut joins: Vec<ScopeOp> = Vec::new();
            for upsert in &parsed.upserts {
                let spec = self.shared.spec(&upsert.model)?;
                let key = (upsert.model.clone(), upsert.id.to_string());
                let Some(patch) = parsed.patches.get(&key) else { continue };
                let Some(row) = self.admitted(reader, WriteOrigin::Event, Admission::default(), &upsert.model, patch, &mut 0)? else { continue };
                let stored = reader.point(&upsert.model, upsert.id.as_str())?;
                let own = RowRef::new(ModelKey::new(&upsert.model), upsert.id.clone());
                let seen: Vec<&Map<String, Value>> = std::iter::once(&row).chain(stored.as_ref()).collect();
                touches.extend(self.touch_ops(reader, spec, &seen, &own)?);
                for (related_model, related_id) in &upsert.related {
                    let related_key = (related_model.clone(), related_id.clone());
                    let Some(patch) = parsed.patches.get(&related_key) else { continue };
                    let Some(related_row) = self.admitted(reader, WriteOrigin::Event, Admission::default(), related_model, patch, &mut 0)? else { continue };
                    let Some(id) = related_row.get("id").and_then(read_id) else { continue };
                    operations.insert((related_model.clone(), id.to_string()), EntityOp::Upsert { model: ModelKey::new(related_model), id, row: related_row });
                }
                joins.extend(self.event_joins(reader, &upsert.model, &upsert.id, &row, &upsert.joins)?);
                operations.insert(key, EntityOp::Upsert { model: ModelKey::new(&upsert.model), id: upsert.id.clone(), row });
            }
            for (model, id) in &parsed.deletes {
                let own = RowRef::new(ModelKey::new(model), id.clone());
                for target in self.destroy_targets(reader, &own)? {
                    operations.insert((target.model.to_string(), target.id.to_string()), EntityOp::Destroy { model: target.model, id: target.id, reason: DestroyReason::Server });
                }
            }
            let mut written: BTreeSet<(String, String)> = operations.keys().cloned().collect();
            let touches = touches.into_iter().filter(|touch| {
                let row = touch.row_ref();
                written.insert((row.model.to_string(), row.id.to_string()))
            });
            let mut envelope = CommitEnvelope::new(WriteOrigin::Event, generation);
            envelope.entity_ops = operations.into_values().chain(touches).collect();
            envelope.scope_ops = joins;
            Ok::<_, DbError>(Some(envelope))
        }).map_err(plan_fault)?;
        if !parsed.ephemeral.is_empty() {
            let admitted = self.admit()?;
            for (model, id, values) in parsed.ephemeral {
                self.set_ephemeral(&admitted, &model, &id, values)?;
            }
        }
        Ok(())
    }

    /// The loaded windows a row joins by its columns: the row is appended to the list of each.
    fn event_joins(&self, reader: &Reader<'_>, model: &str, id: &RowId, _row: &Map<String, Value>, wanted: &[(String, Map<String, Value>)]) -> Result<Vec<ScopeOp>, DbError> {
        let spec = self.shared.spec(model)?;
        let mut joins = Vec::new();
        for (relation, params) in wanted {
            let Some(declared) = spec.relations.iter().find(|candidate| candidate.name == *relation) else { continue };
            let Some(remote) = declared.remote.as_ref() else { continue };
            let Ok(scope_key) = spec.selection_key(relation, params) else { continue };
            let direction = match remote.shape {
                RemoteShape::Connection { direction, .. } => direction,
                _ => PageDirection::Forward,
            };
            let key = window_key(scope_key.as_str(), direction);
            let loaded = rusqlite::OptionalExtension::optional(reader.connection().query_row("SELECT 1 FROM query_window WHERE key = ?1", [&key], |row| row.get::<_, i64>(0)))?;
            if loaded.is_none() || reader.scope_has_member(model, scope_key.as_str(), id.as_str())? {
                continue;
            }
            let last = reader.scope_last_key(model, scope_key.as_str())?;
            let order_key = fractional_between(last.as_deref(), None).map_err(|error| DbError::Internal(error.to_string()))?;
            joins.push(ScopeOp::Append { scope: ScopeRef::new(ModelKey::new(model), scope_key), row: id.clone(), order_key });
        }
        Ok(joins)
    }
}
