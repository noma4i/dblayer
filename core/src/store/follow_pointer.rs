use std::collections::{BTreeMap, BTreeSet};

use rusqlite::OptionalExtension;
use serde_json::{Map, Value};

use crate::store::EntityOp;
use crate::identity::{RowId, RowRef};
use crate::schema::{AssociationKind, AssociationSpec, ModelSpec};
use crate::value::{read_id, same_fields};

use super::{ModelInfo, Reader, StoreError};
use super::window_write::fault;

type Row = Map<String, Value>;


fn setting(spec: &ModelSpec) -> impl Iterator<Item = &AssociationSpec> {
    spec.associations.iter().filter(|association| association.kind == AssociationKind::BelongsTo && !association.sets.is_empty())
}

pub(super) fn affected_parents(reader: &Reader<'_>, models: &BTreeMap<String, ModelInfo>, operations: &[EntityOp], rows: &super::apply::Prefetched, parents: &mut BTreeSet<RowRef>) -> Result<(), StoreError> {
    for operation in operations {
        let row = operation.row_ref();
        let spec = &models.get(row.model.as_str()).ok_or_else(|| StoreError::UnknownModel(row.model.to_string()))?.spec;
        if matches!(operation, EntityOp::Upsert { .. }) && spec.fields.iter().any(|field| field.derived) { parents.insert(row.clone()); }
        let stored = match rows.get(&(row.model.to_string(), row.id.to_string())) { Some(stored) => stored.as_ref().map(|stored| stored.row.clone()), None => reader.point(row.model.as_str(), row.id.as_str())? };
        if let Some(stored) = stored { parents_of(spec, &stored, parents)?; }
    }
    Ok(())
}

pub(crate) fn parents_of(spec: &ModelSpec, row: &Row, parents: &mut BTreeSet<RowRef>) -> Result<(), StoreError> {
    for association in setting(spec) {
        let Some(parent) = row.get(&association.foreign_key).and_then(read_id) else { continue };
        parents.insert(RowRef::new(association.target.clone(), RowId::new(parent.as_str()).map_err(fault)?));
    }
    Ok(())
}

/// The parent models whose stored pointer named the destroyed child: its confirmed delete turns their windows stale (P-DATA-122).
pub(crate) fn named_parents(reader: &Reader<'_>, info: &ModelInfo, id: &str, before: &Row) -> Result<Vec<String>, StoreError> {
    let spec = &info.spec;
    let mut named = Vec::new();
    for association in setting(spec) {
        let Some(parent) = before.get(&association.foreign_key).and_then(read_id) else { continue };
        if association.pointer_field().is_none() { continue }
        if reader.point(association.target.as_str(), parent.as_str())?.is_none() { continue }
        let was_named = match first_ranked(reader, &info.table, association, &parent, false)? {
            None => true,
            Some(winner) => destroyed_outranks(reader, &info.table, association, id, before, &winner)?,
        };
        if was_named { named.push(association.target.to_string()); }
    }
    Ok(named)
}

/// The one entry of the derived parent fields for the parents a write touched, the derived rows go through `apply` in the same transaction.
pub(super) fn settle_parents(reader: &Reader<'_>, models: &BTreeMap<String, ModelInfo>, results: &[super::CommitResult], apply: impl FnOnce(super::BatchPlan) -> Result<super::CommitResult, StoreError>, generation: crate::identity::Generation) -> Result<(Option<super::CommitResult>, usize, BTreeSet<String>), StoreError> {
    let parents: BTreeSet<RowRef> = results.iter().flat_map(|result| result.parents.iter().cloned()).collect();
    let (derived, read, mut stale) = derived_parents(reader, models, &parents)?;
    stale.extend(results.iter().flat_map(|result| result.stale.iter().cloned()));
    let written = if derived.is_empty() { None } else { Some(apply(super::BatchPlan { entity_ops: derived, ..super::BatchPlan::from_source(generation, crate::schema::RowSource::Derived) })?) };
    Ok((written, read, stale))
}

fn derived_parents(reader: &Reader<'_>, models: &BTreeMap<String, ModelInfo>, parents: &BTreeSet<RowRef>) -> Result<(Vec<EntityOp>, usize, BTreeSet<String>), StoreError> {
    let mut derived = Vec::new();
    let mut read = 0;
    let mut stale = BTreeSet::new();
    let mut hidden: Option<super::apply::HiddenRows> = None;
    for parent in parents {
        let Some(stored) = reader.stored_row(parent.model.as_str(), parent.id.as_str())?.map(|stored| stored.row) else { continue };
        let mut next = stored.clone();
        for child in models.values() {
            for association in setting(&child.spec).filter(|association| association.target == parent.model) {
                let named = association.pointer_field().and_then(|field| stored.get(field)).and_then(read_id);
                if let Some(id) = named {
                    if reader.tombstone_seq(child.spec.key.as_str(), id.as_str())?.is_some() && reader.stored_row(child.spec.key.as_str(), id.as_str())?.is_none() {
                        let open = match &hidden { Some(open) => open, None => hidden.insert(super::apply::hidden_rows(reader.connection(), &[])?) };
                        if !open.contains(&(child.spec.key.to_string(), id.to_string())) { stale.insert(parent.model.to_string()); }
                    }
                }
                let mut newest = |permanent: bool| -> Result<Option<Row>, StoreError> {
                    let Some(id) = first_ranked(reader, &child.table, association, &parent.id, permanent)? else { return Ok(None) };
                    read += 1;
                    reader.point(child.spec.key.as_str(), id.as_str())
                };
                let first = newest(false)?;
                let predicted = first.as_ref().and_then(|row| row.get("id")).and_then(Value::as_str).is_some_and(crate::identity::is_temp_id);
                let confirmed = if predicted { newest(true)? } else { None };
                let values = association.derived(first.as_ref(), confirmed.as_ref());
                for field in association.sets.keys() {
                    match values.get(field) {
                        Some(value) => next.insert(field.clone(), value.clone()),
                        None => next.remove(field),
                    };
                }
            }
        }
        if !same_fields(&next, &stored) { derived.push(EntityOp::Upsert { model: parent.model.clone(), id: parent.id.clone(), row: next }); }
    }
    Ok((derived, read, stale))
}

pub(crate) fn winner_sql(table: &str, association: &AssociationSpec, permanent: bool) -> String {
    let foreign_key = crate::store::ddl::sql_ident(&crate::store::ddl::column_name(&association.foreign_key));
    let only = if permanent { format!(" AND {}", crate::identity::permanent_row_flag(&crate::store::ddl::sql_ident("id"))) } else { String::new() };
    format!("SELECT id FROM {table} WHERE {foreign_key} = ?1{only} ORDER BY {} LIMIT 1", order_terms(association))
}

pub(crate) fn contest_sql(table: &str, association: &AssociationSpec) -> String {
    let names: String = association.newer.iter().map(|field| format!(", {}", crate::store::ddl::sql_ident(&crate::store::ddl::column_name(field)))).collect();
    let slots: String = (0..association.newer.len()).map(|index| format!(", ?{}", index + 3)).collect();
    format!("SELECT id FROM (SELECT id{names} FROM {table} WHERE id = ?1 UNION ALL SELECT ?2{slots}) ORDER BY {} LIMIT 1", order_terms(association))
}

fn order_terms(association: &AssociationSpec) -> String {
    crate::store::ddl::newer_order(association).into_iter().map(|(term, descending)| if descending { format!("{term} DESC") } else { term }).collect::<Vec<_>>().join(", ")
}

fn first_ranked(reader: &Reader<'_>, table: &str, association: &AssociationSpec, parent_id: &RowId, permanent: bool) -> Result<Option<RowId>, StoreError> {
    let mut statement = reader.connection().prepare_cached(&winner_sql(table, association, permanent))?;
    let id: Option<String> = statement.query_row(rusqlite::params![parent_id.as_str()], |row| row.get(0)).optional()?;
    id.map(|id| RowId::new(&id).map_err(fault)).transpose()
}

fn destroyed_outranks(reader: &Reader<'_>, table: &str, association: &AssociationSpec, id: &str, row: &Row, winner: &RowId) -> Result<bool, StoreError> {
    let mut bound: Vec<rusqlite::types::Value> = vec![winner.as_str().to_string().into(), id.to_string().into()];
    for field in &association.newer {
        bound.push(crate::store::query::binding(field, row.get(field).unwrap_or(&Value::Null)).map_err(fault)?);
    }
    let mut statement = reader.connection().prepare_cached(&contest_sql(table, association))?;
    let first: String = statement.query_row(rusqlite::params_from_iter(bound), |row| row.get(0))?;
    Ok(first == id)
}
